/*
 * IVrixDB software is licensed under the IVrixDB Software License Agreement
 * (the "License"); you may not use this file or the IVrixDB except in
 * compliance with the License. You may obtain a copy of the License at
 *
 *     https://github.com/hefty111/IVrixDB/blob/master/LICENSE.pdf
 *
 * Unless required by applicable law or agreed to in writing, IVrixDB software is provided
 * and distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions
 * and limitations under the License. See the NOTICE file distributed with the IVrixDB Software
 * for additional information regarding copyright ownership.
 */

package org.apache.solr.ivrixdb.core.overseer.state;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.solr.common.cloud.*;
import org.apache.solr.ivrixdb.core.IVrixLocalNode;
import org.apache.solr.ivrixdb.core.overseer.IVrixOverseer;
import org.apache.solr.ivrixdb.core.overseer.state.metadata.*;
import org.slf4j.Logger;

import static org.apache.solr.ivrixdb.core.overseer.utilities.IVrixStateZkUtil.*;
import static org.apache.solr.ivrixdb.core.overseer.state.IVrixBucket.BucketProgressionStatus.*;
import static org.apache.solr.ivrixdb.core.overseer.state.IVrixBucket.BucketAge.*;
import static org.apache.solr.ivrixdb.core.overseer.state.IVrixBucket.BucketAttachmentState.*;
import static org.apache.solr.ivrixdb.utilities.Constants.IVrix.Limitations.State.MAXIMUM_ATTACHED_COLD_BUCKETS_PER_NODE;

/**
 * This object represents an IVrix Bucket. An IVrix Bucket is a logical
 * fragment of an IVrix Index. It possesses a timestamp range, an age state
 * (HOT, WARM, or COLD), and an attachment state (ATTACHED or DETACHED),
 * and progression state (NEUTRAL, CREATING, DELETING, ROLLING_TO_WARM,
 * ROLLING_TO_COLD, ATTACHING, DETACHING, or FAILED). This object is responsible
 * for creating, deleting, and manipulating a bucket through the states listed above.
 *
 * When a bucket is about to begin transitioning to a new state, it marks itself at the end state
 * and modifies the progression state (towards the new end state), then actually transitions to the new state,
 * and then marks the progression state as NEUTRAL.
 *
 * HOT buckets are replicated across the cluster are being actively written into.
 * WARM buckets are replicated across the cluster that cannot be written into.
 * COLD buckets are local and cannot be written into. For this state, a new feature
 * was created so that COLD buckets can be detached/re-attached into Solr for resource management purposes.
 *
 * Even though buckets can be spread across the cluster, they still belong to the node that initially
 * had the leader replica on it. This allows each IVrix Node to manage its own IVrix Buckets.
 *
 * Currently, an IVrix Bucket is created as a Solr Collection with only one shard, a leader replica
 * that resides in the node that the Bucket belongs to, and replication replicas across the cluster
 * according to a provided replication factor.
 *
 * (IVrixBucket implements the fault-tolerant principles expressed in IVrixOverseer
 * {@link IVrixOverseer})
 *
 * @author Ivri Faitelson
 */
/*
 * TODO: BUG FIX + FEATURE -- improve the deletion of an IVrix Bucket. There is a bug that buckets
 *                            that were indexed into before a restart cannot be properly deleted and the system hangs...
 *
 * TODO: create an explicit state where the IVrixBucket is inactive because it does NOT have a TimeBounds,
 *       There is also the case that Bucket does not possess Solr metadata. Implement bucket inactivity state to avoid confusion.
 *
 * TODO: integrate the FAILED progression value into the state-changing functions below
 *       (create, delete, roll-to-warm, roll-to-cold, attach, detach)
 *
 * TODO: Mark age/attachment/solrMetadata in memory before releasing the lock, and then mark age/attachment/solrMetadata in zk state.
 *       This ensures that in the period when the state lock is acquired, only in-memory changes and no I/O operations will occur.
 *
 * TODO: perhaps refactor the following functions to rely on getResidingNodes()?
 *       -- isDetachedOnAllLiveResidingNodes(), isAttachedOnAllLiveResidingNodes(), canBeAttachedWithAtLeastOneQueryableReplica()
 *
 */
public class IVrixBucket {
  private static final Logger log = IVrixOverseer.log;

  public enum BucketAge {
    HOT, WARM, COLD // ORDER MATTERS! The ordinal values are being utilized
  }
  public enum BucketAttachmentState {
    ATTACHED, DETACHED
  }
  public enum BucketProgressionStatus {
    CREATING_SELF,
    DELETING_SELF,
    ROLLING_TO_WARM, // must be marked WARM in state
    ROLLING_TO_COLD, // must be marked COLD in state
    ATTACHING, // must be marked COLD and ATTACHED in state
    DETACHING, // must be marked COLD and DETACHED in state
    NEUTRAL, // default value
    FAILED
  }


  private List<String> cachedNodeNamesForReplicationFromCreation;
  private int cachedReplicationFactorFromCreation;

  private final List<BucketHolder> holdersForAttachedState;
  private BucketProgressionStatus progressionStatus;
  private BucketAttachmentState attachmentState; // NULL if bucket has not been fully created or deleted
  private BucketAge ageState; // NULL if bucket has not been fully created or deleted

  private final String indexName;
  private final String indexerNodeName;
  private final String bucketName;
  private SolrCollectionMetadata completeSolrCollMetadata; // NULL if bucket has not been fully created
  private TimeBounds timeBounds; // NULL if TimeBounds has not been updated since bucket creation

  private IVrixBucket(String indexName, String bucketName, String indexerNodeName,
                      SolrCollectionMetadata solrCollectionMetadata,
                      TimeBounds timeBounds, BucketAge ageState, BucketAttachmentState attachmentState,
                      List<String> cachedNodeNamesForReplicationFromCreation, int cachedReplicationFactorFromCreation) {
    this.indexName = indexName;
    this.bucketName = bucketName;
    this.indexerNodeName = indexerNodeName;
    this.timeBounds = timeBounds;
    this.completeSolrCollMetadata = solrCollectionMetadata;
    this.attachmentState = attachmentState;
    this.ageState = ageState;
    this.progressionStatus = NEUTRAL;
    this.holdersForAttachedState = new LinkedList<>();
    this.cachedNodeNamesForReplicationFromCreation = cachedNodeNamesForReplicationFromCreation;
    this.cachedReplicationFactorFromCreation = cachedReplicationFactorFromCreation;
  }

  /**
   * @return An IVrix Bucket with the requested name that was persisted in the requested Index name.
   *          Loads the persistent properties of the Bucket from Zookeeper, and has
   *          non-persistent properties at default values.
   */
  public static IVrixBucket loadFromZK(String indexName, String bucketName) throws IOException {
    log.info("loading bucket " + bucketName + " for index " + indexName + "...");


    // given as non-null since otherwise bucket would not appear in ZK state
    Map<String, Object> metadataBlob = retrieveBucketBlob(indexName, bucketName);
    // given as non-null (through bucketBlob and through constructor)
    String indexerNodeName = (String)metadataBlob.get(INDEXER_NODE_NAME_KEY);

    Map<String, Object> solrMetadataMap = (Map<String, Object>)metadataBlob.get(SOLR_COLLECTION_METADATA_KEY);
    Map<String, Object> timeBoundsMap = (Map<String, Object>)metadataBlob.get(TIME_BOUNDS_KEY);
    Boolean isDetached = retrieveIsDetachedPropertyValue(indexName, bucketName);
    String ageStateName = retrieveBucketAgePropertyValue(indexName, bucketName);

    return new IVrixBucket(
        indexName, // given
        bucketName, // given
        indexerNodeName, // given
        solrMetadataMap == null ? null : SolrCollectionMetadata.fromMap(bucketName, solrMetadataMap),
        timeBoundsMap == null ? null : TimeBounds.fromMap(timeBoundsMap),
        ageStateName == null ? null : BucketAge.valueOf(ageStateName),
        isDetached == null ? null : (isDetached ? DETACHED : ATTACHED),
        null,
        -1
    );
  }








  /**
   * Creates a new, default, and empty IVrix Bucket without persistence or physical existence
   * That is, it does not exist in IVrixDB's nor Solr's persistent state.
   *
   * @param indexName The name of the IVrix Index that this Bucket belongs to
   * @param bucketName The name to give the Bucket
   * @param indexerNodeName The name of the Node that this Bucket belongs to
   * @param cachedReplicationFactorFromCreation the replication factor decided for creation
   * @param cachedNodeNamesForReplicationFromCreation names of the replication nodes decided for creation.
   *                                                  Used for determining residing nodes when creation is unfinished.
   */
  public IVrixBucket(String indexName, String bucketName, String indexerNodeName,
                     List<String> cachedNodeNamesForReplicationFromCreation, int cachedReplicationFactorFromCreation) {
    this(indexName, bucketName, indexerNodeName,
        null, null,
        null, null,
        cachedNodeNamesForReplicationFromCreation, cachedReplicationFactorFromCreation
    );
  }

  /**
   * Creates existence for self in persistence and physically (that is, in IVrixDB's and Solr's state)
   */
  public void createSelf(ReentrantLock stateLock) throws IOException {
    log.info("creating bucket " + bucketName + " for index " + indexName + "...");
    assert(isProgressionMarkedAs(NEUTRAL) && isAgeMarkedAs(null) && cachedNodeNamesForReplicationFromCreation != null);

    this.createPersistenceStructures();
    this.markAgeStateAs(HOT);
    this.markAttachmentStateAs(ATTACHED);
    this.markProgressionAs(CREATING_SELF);
    stateLock.unlock();
    IVrixLocalNode.getBucketsPhysicalHandler().createBucket(
        bucketName, indexerNodeName, cachedNodeNamesForReplicationFromCreation, cachedReplicationFactorFromCreation
    );
    stateLock.lock();
    this.updateCompleteSolrCollMetadata();
    this.markProgressionAs(NEUTRAL);
    this.cachedNodeNamesForReplicationFromCreation = null;
  }

  /**
   * Deletes self from persistence and physically (that is, from IVrixDB's and Solr's state)
   */
  public void deleteSelf(ReentrantLock stateLock) throws IOException {
    log.info("deleting bucket " + bucketName + " from index " + indexName + "...");
    assert(isProgressionMarkedAs(NEUTRAL) || isProgressionMarkedAs(DELETING_SELF));

    this.markAgeStateAs(null);
    this.markAttachmentStateAs(null);
    this.markProgressionAs(DELETING_SELF);
    stateLock.unlock();
    if (null != IVrixLocalNode.getClusterState().getCollectionOrNull(this.getName())) {
      IVrixLocalNode.getBucketsPhysicalHandler().deleteBucket(this.getName());
    }
    deleteIVrixBucketPath(this);
    stateLock.lock();
  }

  /**
   * Rolls the Bucket to WARM age state
   */
  public void rollToWarm(ReentrantLock stateLock) throws IOException {
    log.info("rolling bucket " + bucketName + " to warm...");
    assert(isProgressionMarkedAs(NEUTRAL) && (isAgeMarkedAs(HOT) || isAgeMarkedAs(WARM)));

    this.markAgeStateAs(WARM);
    this.markProgressionAs(ROLLING_TO_WARM);
    // stateLock.unlock();
    // physicallyRollBucketToWarm(bucket); // NOTE --- THIS DOES NOT EXIST!!! ROLLING TO WARM DOES NOT REQUIRE PHYSICAL SOLR STATE CHANGE!!!!
    // stateLock.lock();
    this.markProgressionAs(NEUTRAL);
  }

  /**
   * Rolls the Bucket to COLD age state. Deletes all replication
   * replicas from Bucket (in both IVrixDB and Solr states).
   */
  public void rollToCold(ReentrantLock stateLock) throws IOException {
    log.info("rolling bucket " + bucketName + " to cold...");
    assert(isProgressionMarkedAs(NEUTRAL) && (isAgeMarkedAs(WARM) || isAgeMarkedAs(COLD)));

    this.markAgeStateAs(COLD);
    this.markProgressionAs(ROLLING_TO_COLD);
    stateLock.unlock();
    IVrixLocalNode.getBucketsPhysicalHandler().prepareWarmBucketForColdRollover(this);
    stateLock.lock();
    this.updateCompleteSolrCollMetadata();
    this.markProgressionAs(NEUTRAL);
  }

  /**
   * Attaches the Bucket, both in IVrixDB state and in Solr's state (i.e, in state and physically)
   */
  public void attach(ReentrantLock stateLock) throws IOException {
    log.info("attaching bucket " + bucketName + "...");
    // assert(isProgressionMarkedAs(NEUTRAL) && (isAttachmentStateMarkedAs(DETACHED) || isAttachmentStateMarkedAs(ATTACHED)));

    this.markAttachmentStateAs(ATTACHED);
    this.markProgressionAs(ATTACHING);
    stateLock.unlock();
    IVrixLocalNode.getBucketsPhysicalHandler().attachBucket(this);
    stateLock.lock();
    this.markProgressionAs(NEUTRAL);
  }

  /**
   * Detaches the Bucket, both in IVrixDB state and in Solr's state (i.e, in state and physically)
   */
  public void detach(ReentrantLock stateLock) throws IOException {
    log.info("detaching bucket " + bucketName + "...");
    // assert(isProgressionMarkedAs(NEUTRAL) && !isAttachedStateBeingHeld() &&
    //     (isAttachmentStateMarkedAs(ATTACHED) || isAttachmentStateMarkedAs(DETACHED)));

    this.markAttachmentStateAs(DETACHED);
    this.markProgressionAs(DETACHING);
    stateLock.unlock();
    IVrixLocalNode.getBucketsPhysicalHandler().detachBucket(this);
    stateLock.lock();
    this.markProgressionAs(NEUTRAL);
  }

  /**
   * Attaches one bucket and detaches a set of buckets, both in IVrixDB state and in Solr's state (i.e, in state and physically)
   */
  public static void attachAndDetach(IVrixBucket bucketToAttach, Set<IVrixBucket> bucketsToDetach, ReentrantLock stateLock) throws IOException {
    log.info("attaching " + bucketToAttach.bucketName + " and detaching " + bucketsToDetach.toString() + "...");
    assert(bucketToAttach.isProgressionMarkedAs(NEUTRAL) && bucketToAttach.isAttachmentStateMarkedAs(DETACHED));
    for (IVrixBucket bucketToDetach : bucketsToDetach)
      assert(bucketToDetach.isProgressionMarkedAs(NEUTRAL) && bucketToDetach.isAttachmentStateMarkedAs(ATTACHED) && !bucketToDetach.isAttachedStateBeingHeld());

    for (IVrixBucket bucketToDetach : bucketsToDetach) {
      bucketToDetach.markAttachmentStateAs(DETACHED);
      bucketToDetach.markProgressionAs(DETACHING);
    }
    bucketToAttach.markAttachmentStateAs(ATTACHED);
    bucketToAttach.markProgressionAs(ATTACHING);

    for (IVrixBucket bucketToDetach : bucketsToDetach)
      bucketToDetach.detach(stateLock);
    bucketToAttach.attach(stateLock);
  }








  /**
   * @return A Map blob containing the main persisted metadata of the Bucket
   */
  public Map<String, Object> getMainPersistenceBlob() {
    Map<String, Object> metadataMap = new HashMap<>();
    metadataMap.put(INDEXER_NODE_NAME_KEY, indexerNodeName);
    metadataMap.put(SOLR_COLLECTION_METADATA_KEY, completeSolrCollMetadata == null ? null : completeSolrCollMetadata.toMap());
    metadataMap.put(TIME_BOUNDS_KEY, timeBounds == null ? null : timeBounds.toMap());
    return metadataMap;
  }

  private void createPersistenceStructures() throws IOException {
    createBucketBlobNode(this);
    createIsDetachedPropertyNode(this);
    createBucketAgePropertyNode(this);
  }

  /**
    * Updates the time bounds of the Bucket in IVrixDB's state. Used during indexing.
    */
  public void updateTimeBounds(TimeBounds timeBounds) throws IOException {
    this.timeBounds = timeBounds;
    updateBucketBlobNode(this);
  }

  private void updateCompleteSolrCollMetadata() throws IOException {
    DocCollection collectionState = IVrixLocalNode.getClusterState().getCollection(bucketName);
    this.completeSolrCollMetadata = SolrCollectionMetadata.copy(collectionState);
    updateBucketBlobNode(this);
  }

  private void markAgeStateAs(BucketAge newAgeState) throws IOException {
    this.ageState = newAgeState;
    updateBucketAgePropertyNode(this);
  }

  private void markAttachmentStateAs(BucketAttachmentState newAttachmentState) throws IOException {
    this.attachmentState = newAttachmentState;
    updateIsDetachedPropertyNode(this);
  }

  private void markProgressionAs(BucketProgressionStatus progressionStatus) {
    this.progressionStatus = progressionStatus;
  }









  /**
   * @return whether the progression of the bucket matches a given progression state.
   */
  public boolean isProgressionMarkedAs(BucketProgressionStatus progressionStatus) {
    return this.progressionStatus == progressionStatus;
  }

  /**
   * @return whether the attachment state of the bucket matches a given attachment state
   */
  public boolean isAttachmentStateMarkedAs(BucketAttachmentState attachmentState) {
    return this.attachmentState == attachmentState;
  }

  /**
   * @return whether the age of the bucket matches a given age state.
   */
  public boolean isAgeMarkedAs(BucketAge ageState) {
    return this.ageState == ageState;
  }

  /**
   * @return Whether the Bucket has stored the objects in the Solr state that correspond to the Bucket
   */
  public boolean doesSolrCollMetadataExist() {
    return completeSolrCollMetadata != null;
  }

  /**
   * @return whether the time bounds was updated since the bucket has been created.
   */
  public boolean doesTimeBoundsExist() {
    return timeBounds != null;
  }

  /**
   * @return whether the bucket's persistable properties are in a valid enough condition so that the bucket's existence is justified
   */
  public boolean isConsideredOKEnoughToExist() {
    return doesSolrCollMetadataExist() && !isAgeMarkedAs(null) && !isAttachmentStateMarkedAs(null);
  }

  /**
   * @return whether the number of replicas that the bucket marked down matches up to a COLD state number of replicas
   */
  public boolean hasColdStateNumberOfReplicas() {
    return completeSolrCollMetadata.getReplicas().size() == 1;
  }








  /**
   * @return Whether the bucket is being held by any attached-state holder
   */
  public boolean isAttachedStateBeingHeld() {
    return holdersForAttachedState.size() > 0;
  }

  /**
   * @return Whether the bucket is being held by a particular attached-state holder
   */
  public boolean isAttachedStateBeingHeldByHolder(String searchJobID, String requestingNodeName) {
    BucketHolder holder = new BucketHolder(searchJobID, requestingNodeName);
    return holdersForAttachedState.contains(holder);
  }

  /**
   * Adds a holder to the list of attached-state holders (if non-existent) that are currently searching the bucket. Used for searching.
   */
  public void addAttachedStateHolderIfNonExistent(String searchJobID, String requestingNodeName) {
    if (!this.isAttachedStateBeingHeldByHolder(searchJobID, requestingNodeName)) {
      holdersForAttachedState.add(new BucketHolder(searchJobID, requestingNodeName));
    }
  }

  /**
   * removes a particular holder from the list of attached-state holders
   */
  public void removeAttachedStateHolder(String searchJobID, String requestingNodeName) {
    BucketHolder holderToRemove = new BucketHolder(searchJobID, requestingNodeName);
    holdersForAttachedState.removeIf(nextHolder -> nextHolder.equals(holderToRemove));
  }

  /**
   * Removes all attached-state holders that were created by a specific IVrix Node from the list of attached-state holders
   */
  public void removeAllAttachedStateHoldersThatCameFromNode(String nodeBaseURL) {
    holdersForAttachedState.removeIf(nextHolder -> nextHolder.nodeName.equals(nodeBaseURL));
  }

  /**
   * Forcibly removes all attached-state holders from the attached-state holders list
   */
  public void forceRemoveAllAttachedStateHolders() {
    holdersForAttachedState.clear();
  }









  private List<String> getResidingNodes(Collection<Slice> shards) {
    Set<String> residingNodes = new HashSet<>();
    for (Slice shard : shards)
      for (Replica replica : shard.getReplicas())
        residingNodes.add(replica.getNodeName());
    return new LinkedList<>(residingNodes);
  }

  /**
   * @return All the IVrix Nodes where the Bucket (logically) resides in
   */
  public List<String> getAllResidingNodes() {
    if (isAgeMarkedAs(COLD)) { // reflect COLD rollover even if not completed
      return Collections.singletonList(indexerNodeName);
    } else if (completeSolrCollMetadata != null) {
      return getResidingNodes(completeSolrCollMetadata.getSlices());
    } else if (cachedNodeNamesForReplicationFromCreation != null) { // reflect creation even if not completed
      List<String> nodesToConsiderResidingOn = new LinkedList<>(cachedNodeNamesForReplicationFromCreation);
      nodesToConsiderResidingOn.add(indexerNodeName);
      return nodesToConsiderResidingOn;
    } else { // reflect deletion even if not completed
      return new LinkedList<>();
    }
  }

  /**
   * @return All the IVrix Nodes where the Bucket (physically and currently) resides in
   */
  public List<String> getAllLiveResidingNodesWithAttachedReplicas() {
    DocCollection collectionState = IVrixLocalNode.getClusterState().getCollectionOrNull(bucketName);
    if (collectionState == null)
      return new LinkedList<>();
    return getResidingNodes(collectionState.getSlices());
  }

  /**
   * @return A list of the IVrix Nodes that an IVrix Bucket resides in that have reached their max-attached COLD limitation.
   */
  public Set<String> getAllResidingNodesThatReachedMaxAttachedCold(IVrixState state, boolean onlyConsiderLiveReplicas) {
    Set<String> nodesThatReachedMaximumAttachedCold = new HashSet<>();
    for (String residingNode : this.getAllResidingNodes())
      if (MAXIMUM_ATTACHED_COLD_BUCKETS_PER_NODE <= state.getNumberOfAttachedColdBucketsInNode(residingNode, onlyConsiderLiveReplicas))
        nodesThatReachedMaximumAttachedCold.add(residingNode);
    return nodesThatReachedMaximumAttachedCold;
  }

  /**
   * @return Whether the Bucket is detached on all of its live residing Nodes
   */
  public boolean isDetachedOnAllLiveResidingNodes() {
    Collection<String> liveNodes = IVrixLocalNode.getClusterState().getLiveNodes();
    for (Slice shardMetadata : completeSolrCollMetadata.getSlices())
      for (Replica replicaMetadata : shardMetadata.getReplicas())
        if (liveNodes.contains(replicaMetadata.getNodeName()) &&
            SolrCollectionMetadata.isReplicaActuallyAttached(bucketName, shardMetadata.getName(), replicaMetadata.getCoreName()))
          return false;
    return true;
  }

  /**
   * @return Whether the Bucket is attached on all of its live residing Nodes
   */
  public boolean isAttachedOnAllLiveResidingNodes() {
    Collection<String> liveNodes = IVrixLocalNode.getClusterState().getLiveNodes();
    for (Slice shardMetadata : completeSolrCollMetadata.getSlices())
      for (Replica replicaMetadata : shardMetadata.getReplicas())
        if (liveNodes.contains(replicaMetadata.getNodeName()) &&
            !SolrCollectionMetadata.isReplicaActuallyAttached(bucketName, shardMetadata.getName(), replicaMetadata.getCoreName()))
          return false;
    return true;
  }

  /**
   * @return Whether the Bucket is attached with at least one replica
   *          that is live and healthy enough to be queried
   */
  public boolean isAttachedWithAtLeastOneQueryableReplica() {
    DocCollection collectionState = IVrixLocalNode.getClusterState().getCollection(bucketName);
    Collection<String> liveSolrNodes = IVrixLocalNode.getClusterState().getLiveNodes();
    for (Replica replica : collectionState.getReplicas())
      if (replica.getState() == Replica.State.ACTIVE && liveSolrNodes.contains(replica.getNodeName()))
        return true;
    return false;
  }

  /**
   * @return Whether the Bucket can be attached with at least one replica
   *          that can be live and healthy enough to be queried
   */
  public boolean canBeAttachedWithAtLeastOneQueryableReplica() {
    Collection<String> liveNodes = IVrixLocalNode.getClusterState().getLiveNodes();
    for (Replica replicaMetadata : completeSolrCollMetadata.getReplicas())
      if (liveNodes.contains(replicaMetadata.getNodeName()))
        return true;
    return false;
  }






  /**
   * @return The name of the IVrix Index that this Bucket belongs to
   */
  public String getIndexName() {
    return indexName;
  }

  /**
   * @return The name of this Bucket
   */
  public String getName() {
    return bucketName;
  }

  /**
   * @return The name of the IVrix Node that this Bucket belongs to
   */
  public String getIndexerNodeName() {
    return indexerNodeName;
  }

  /**
   * @return The Bucket's timestamp range
   *         WARNING!!! -- null is a feasible return value
   */
  public TimeBounds getTimeBounds() {
    return timeBounds;
  }

  /**
   * @return The age state of the Bucket
   */
  public BucketAge getBucketAge() {
    return ageState;
  }

  /**
   * @return The objects in the Solr state that correspond to the Bucket
   */
  public SolrCollectionMetadata getCompleteSolrCollMetadata() {
    return completeSolrCollMetadata;
  }

  /**
   * @return A numerical representation of the Bucket's timestamp range
   */
  public long getLongRepresentationOfTimeBounds() {
    return timeBounds.getLongRepresentation();
  }

  /**
   * @return whether this bucket is older than another
   */
  public boolean isOlderThan(IVrixBucket that) {
    return this.getLongRepresentationOfTimeBounds() < that.getLongRepresentationOfTimeBounds();
  }

  @Override
  public String toString() {
    return bucketName;
  }
}
