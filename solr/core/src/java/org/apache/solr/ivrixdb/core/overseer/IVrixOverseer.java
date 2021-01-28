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

package org.apache.solr.ivrixdb.core.overseer;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.*;
import java.util.concurrent.locks.*;
import java.util.function.Supplier;

import org.apache.solr.ivrixdb.core.IVrixLocalNode;
import org.apache.solr.ivrixdb.core.overseer.election.LeaderElectionCandidate;
import org.apache.solr.ivrixdb.core.overseer.request.IVrixOverseerClient;
import org.apache.solr.ivrixdb.core.overseer.request.IVrixOverseerOperationParams;
import org.apache.solr.ivrixdb.core.overseer.response.OrganizedBucketsForSearch;
import org.apache.solr.ivrixdb.core.overseer.utilities.CandidateIdUtil;
import org.apache.solr.ivrixdb.core.overseer.state.*;
import org.slf4j.*;

import static org.apache.solr.ivrixdb.core.overseer.state.IVrixBucket.BucketAge.*;
import static org.apache.solr.ivrixdb.core.overseer.state.IVrixBucket.BucketAttachmentState.*;
import static org.apache.solr.ivrixdb.core.overseer.state.IVrixBucket.BucketProgressionStatus.*;
import static org.apache.solr.ivrixdb.utilities.Constants.IVrix.Limitations.State.*;

/**
 * The IVrix Overseer is responsible for managing the IVrixDB global state,
 * the IVrix Indexes and their respective IVrix Buckets. It is a thread-safe,
 * fault-tolerant, leader-elected (and re-elected) role that any IVrix Node can take on.
 * The election works through Zookeeper and is a "first-come, first-serve" election.
 * Each request to the Overseer is a new thread, and so the Overseer is designed to be a thread-safe object.
 * Since most operations need to first communicate with it, the Overseer is designed to execute operations concurrently.
 * The IVrix Overseer sends commands over to Solr’s Overseer to change Solr’s state,
 * thereby allowing Solr to manage its own state undisturbed.
 *
 * A small portion of the Overseer’s state is held in-memory and will die with the Overseer,
 * and the rest is persisted into Zookeeper, which is loaded and cached during Overseer boot-up.
 *
 * The Overseer also manages the overall bucket attachment count across all
 * IVrix Indexes at each IVrix Node. It does so at each bucket creation by executing
 * index rollover commands if necessary, and at each bucket hold request (for search)
 * by executing attach/detach commands if necessary.
 *
 * The Overseer implements fault-tolerance by enabling its operations to work around dead nodes
 * and by ensuring consistency and recovery from non-completed operations at three major checkpoints:
 * Overseer boot-up, Overseer handling of Node boot-up and Overseer handling of Node shutdown.
 * It ensures that it can stay consistent and recover at those checkpoints because each Solr-state-changing
 * operation first marks the change in IVrixDB's state at Zookeeper before enacting the operation.
 * The ONLY exception are deletion commands, which first delete from Solr state before
 * deleting from IVrixDB state.
 *
 * Since the Overseer is a leader-elected object and there are dormant Overseer's waiting to be elected as leader
 * for every IVrix Node, requests are sent to the Overseer through the IVrix Overseer Client, which finds the elected
 * leader and sends the request to it. To learn more about the client, please refer to {@link IVrixOverseerClient}.
 *
 * To learn more about IVrix Indexes and IVrix Buckets, please refer to {@link IVrixBucket} and {@link IVrixIndex}.
 * A description of each Operation that the IVrix Overseer can handle can be found at {@link IVrixOverseerOperationParams}.
 *
 * @author Ivri Faitelson
 */
/*
 * TODO: implement fine-grained locking.
 *
 * TODO: POTENTIAL MEMORY CONSISTENCY ISSUE -- when IVrix Overseer election does not match up
 *       with Solr's Overseer election, reading Solr's state after a Solr state changing command
 *       might not return the correct values. Fix this by peering and using Solr's leader election,
 *       rather than creating a separate leader election process.
 */
public class IVrixOverseer extends LeaderElectionCandidate {
  public static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final IVrixState state;
  private final Condition stateChange;
  private final ReentrantLock stateLock;

  private boolean acceptingRequests;
  private final Condition requestsChange;
  private final ReentrantLock requestsLock;


  /**
   * Creates an IVrix Overseer candidate
   */
  public IVrixOverseer() {
    super();
    this.state = new IVrixState();
    this.stateLock = new ReentrantLock();
    this.requestsLock = new ReentrantLock();
    this.stateChange = stateLock.newCondition();
    this.requestsChange = requestsLock.newCondition();
    this.acceptingRequests = false;
  }

  /**
   * Executes an operation against this IVrix Overseer candidate.
   * Waits for this IVrix Overseer candidate to boot up and permit requests before executing.
   * If it is not in mode "LEADER", then it cannot execute the operation.
   */
  public Object execute(IVrixOverseerOperationParams params) throws Exception {
    try {
      requestsLock.lock();
      while (getOperatingMode() == OperatingMode.LEADER && !acceptingRequests)
        requestsChange.await();
      if (getOperatingMode() != OperatingMode.LEADER)
        throw new IllegalAccessException("Cannot execute against a slave or a not-running instance of overseer!");
    } finally {
      requestsLock.unlock();
    }

    Object response = null;
    try {
      switch (params.getOperationType()) {
        case FREE_HOT_BUCKETS_IN_NODE:
          response = executeFreeHotBucketsInNodeOperation(params);
          break;

        case CREATE_IVRIX_INDEX:
          response = executeCreateIVrixIndex(params);
          break;
        case DELETE_IVRIX_INDEX:
          response = executeDeleteIVrixIndex(params);
          break;
        case GET_ALL_IVRIX_INDEXES:
          response = executeGetAllIVrixIndexes(params);
          break;

        case CREATE_BUCKET:
          response = executeCreateBucketOperation(params);
          break;
        case UPDATE_BUCKET_METADATA:
          response = executeUpdateBucketMetadataOperation(params);
          break;
        case ROLL_HOT_BUCKET_TO_WARM:
          response = executeRollHotBucketToWarmOperation(params);
          break;

        case GET_BUCKETS_WITHIN_BOUNDS:
          response = executeGetBucketsForSearchJobOperation(params);
          break;
        case HOLD_BUCKET_FOR_SEARCH:
          response = executeHoldBucketForSearchOperation(params);
          break;
        case RELEASE_BUCKET_HOLD:
          response = executeReleaseBucketHoldOperation(params);
          break;
      }
    } finally {
      if (stateLock.isHeldByCurrentThread()) {
        stateLock.unlock();
      }
    }
    return response;
  }

  /**
   * Changes the mode of the IVrix Overseer candidate.
   * If the mode is "LEADER", then this candidate
   * is the Overseer, and it would be booted up.
   */
  @Override
  protected void operateAs(OperatingMode operatingMode) throws Exception {
    super.operateAs(operatingMode);
    if (operatingMode == OperatingMode.LEADER) {
      requestsLock.lock();
      stateLock.lock();

      log.info("Initializing Overseer...");
      state.loadFromZK();
      this.ensureStateConsistency();
      this.rollAllHotBucketsFromDownedIndexerNodes();
      log.info("Overseer is initialized.");

      this.acceptingRequests = true;
      requestsChange.signalAll();
      stateChange.signalAll();
      requestsLock.unlock();
      stateLock.unlock();
    }
  }

  /**
   * Shuts down the IVrix Overseer candidate.
   * inhibits any requests from executing.
   */
  @Override
  protected void shutdown() {
    super.shutdown();
    requestsLock.lock();
    log.info("Shutting down Overseer...");
    acceptingRequests = false;
    log.info("Overseer is shutdown.");
    requestsChange.signalAll();
    requestsLock.unlock();
  }

  /**
   * Handles a change in the list of IVrix Overseer candidates (IVrix Nodes)
   * @param changeType The type of change in the list (either CREATED or DELETED)
   * @param slaveID The candidate ID that introduced the change in the list
   */
  @Override
  protected void handleSlaveChange(ChangeType changeType, String slaveID) {
    String slaveNodeName = CandidateIdUtil.extractNodeNameFromID(slaveID);
    requestsLock.lock();
    stateLock.lock();
    try {
      if (changeType == ChangeType.CREATED) {
        log.info("Overseer is handling the addition of slave node -- " + slaveNodeName + "...");
        this.ensureStateConsistency();
        log.info("Overseer finished handling the addition of slave node -- " + slaveNodeName + ".");

      } else if (changeType == ChangeType.DELETED) {
        log.info("Overseer is handling the removal of slave node -- " + slaveNodeName + "...");
        this.ensureStateConsistency();
        this.rollAllHotBucketsInIndexerNode(slaveNodeName);
        log.info("Removing any attached-state holders that came from slave node -- " + slaveNodeName + " ...");
        for (IVrixBucket bucket : state)
          bucket.removeAllAttachedStateHoldersThatCameFromNode(slaveNodeName);
        log.info("Overseer finished handling the removal of slave node -- " + slaveNodeName + ".");
      }

    } catch (Exception e) {
      log.info("Overseer failed at handling the boot state change of slave node: " + slaveNodeName +
          ". Will the log the error, and resume tasks: ", e);
      e.printStackTrace();

    } finally {
      stateChange.signalAll();
      requestsLock.unlock();
      stateLock.unlock();
    }
  }




  private Object executeFreeHotBucketsInNodeOperation(IVrixOverseerOperationParams params) throws Exception {
    stateLock.lock();
    this.rollAllHotBucketsInIndexerNode(params.getRequestingNodeName());
    stateChange.signalAll();
    stateLock.unlock();
    return true;
  }




  private Object executeGetBucketsForSearchJobOperation(IVrixOverseerOperationParams params) throws Exception {
    stateLock.lock();
    List<IVrixBucket> buckets = new LinkedList<>();
    for (IVrixBucket bucket : state.getIndex(params.getIndexName()))
      if (bucket.getTimeBounds() != null && bucket.getTimeBounds().overlapsWith(params.getTimeBounds()))
        buckets.add(bucket);
    stateLock.unlock();
    return OrganizedBucketsForSearch.fromBucketsList(buckets);
  }

  private Object executeHoldBucketForSearchOperation(IVrixOverseerOperationParams params) throws Exception {
    stateLock.lock();
    IVrixBucket bucket;

    while (true) {
      // takes care of interleaving from delete
      bucket = state.getBucket(params.getIndexName(), params.getBucketName());
      // takes care of interleaving from index-rollover in create
      bucket.addAttachedStateHolderIfNonExistent(params.getSearchJobId(), params.getRequestingNodeName());

      Set<IVrixBucket> bucketsToDetachIfNecessary = new HashSet<>();
      for (String nodeName : bucket.getAllResidingNodesThatReachedMaxAttachedCold(state, false))
        if (null != this.getColdBucketToDetachInNode(nodeName, false, true))
          bucketsToDetachIfNecessary.add(this.getColdBucketToDetachInNode(nodeName, false, true));

      if (!bucket.canBeAttachedWithAtLeastOneQueryableReplica()) {
        break;
      } else if (bucket.isAttachmentStateMarkedAs(ATTACHED) && !bucket.isProgressionMarkedAs(ATTACHING)) {
        break;
      } else if (bucket.isAttachmentStateMarkedAs(DETACHED) && bucket.isProgressionMarkedAs(NEUTRAL) &&
          bucketsToDetachIfNecessary.size() == bucket.getAllResidingNodesThatReachedMaxAttachedCold(state, false).size()) {
        IVrixBucket.attachAndDetach(bucket, bucketsToDetachIfNecessary, stateLock);
        stateChange.signalAll();
        break;
      } else {
        stateChange.await();
      }
    }
    boolean success = bucket.isAttachedWithAtLeastOneQueryableReplica();
    if (!success) {
      bucket.removeAttachedStateHolder(params.getSearchJobId(), params.getRequestingNodeName());
      if (!bucket.isAttachedStateBeingHeld())
        stateChange.signalAll();
    }
    stateLock.unlock();
    return success;
  }

  private Object executeReleaseBucketHoldOperation(IVrixOverseerOperationParams params) throws Exception {
    stateLock.lock();
    IVrixBucket bucket = state.getBucket(params.getIndexName(), params.getBucketName());
    bucket.removeAttachedStateHolder(params.getSearchJobId(), params.getRequestingNodeName());
    if (!bucket.isAttachedStateBeingHeld())
      stateChange.signalAll();
    stateLock.unlock();
    return true;
  }




  private Object executeCreateBucketOperation(IVrixOverseerOperationParams params) throws Exception {
    ISupplier<List<String>> supplierOfNodesForBucketReplication = () -> {
      List<String> potentialNodes = new LinkedList<>(IVrixLocalNode.getClusterState().getLiveNodes());
      potentialNodes.remove(params.getRequestingNodeName());
      Collections.shuffle(potentialNodes, new Random());
      return potentialNodes.subList(0, Math.min(params.getReplicationFactor()-1, potentialNodes.size()));
    };


    stateLock.lock();
    List<String> nodesForReplication = supplierOfNodesForBucketReplication.get();
    while (true) {
      // takes care of interleaving with indexerNode deletion
      if (!IVrixLocalNode.getClusterState().getLiveNodes().contains(params.getRequestingNodeName()))
        throw new IllegalAccessException("cannot create a HOT bucket on a dead indexer node!");
      // takes care of interleaving with index deletion
      if (state.getIndex(params.getIndexName()).isMarkedDeleted())
        throw new IllegalAccessException("cannot create a HOT bucket for a deleted/deleting index!");
      // takes care of interleaving with replication node deletion
      for (String nodeName : nodesForReplication)
        if (!IVrixLocalNode.getClusterState().getLiveNodes().contains(nodeName))
          nodesForReplication = supplierOfNodesForBucketReplication.get();

      List<String> allNodesForCreation = new LinkedList<>(nodesForReplication);
      allNodesForCreation.add(0, params.getRequestingNodeName());

      IVrixBucket warmBucketToRollToCold = null;
      IVrixBucket coldBucketToDetach = null;
      for (String nodeName : allNodesForCreation) {
        if (MAXIMUM_ATTACHED_HOT_PLUS_WARM_BUCKETS_PER_NODE <= state.getNumberOfAttachedHotAndWarmBucketsInNode(nodeName, false))
          warmBucketToRollToCold = this.getWarmBucketToRollToColdInNode(nodeName);
        if (MAXIMUM_ATTACHED_COLD_BUCKETS_PER_NODE < state.getNumberOfAttachedColdBucketsInNode(nodeName, false))
          if (null != this.getColdBucketToDetachInNode(nodeName, false, false))
            coldBucketToDetach = this.getColdBucketToDetachInNode(nodeName, false, false);
          else
            coldBucketToDetach = this.getColdBucketToDetachInNode(nodeName, true, false); }

      if (warmBucketToRollToCold == null && coldBucketToDetach == null) {
        break;
      } else if (warmBucketToRollToCold != null && warmBucketToRollToCold.isProgressionMarkedAs(NEUTRAL)) {
        warmBucketToRollToCold.rollToCold(stateLock);
        stateChange.signalAll();
      } else if (coldBucketToDetach != null && coldBucketToDetach.isProgressionMarkedAs(NEUTRAL)) {
        coldBucketToDetach.forceRemoveAllAttachedStateHolders();
        coldBucketToDetach.detach(stateLock);
        stateChange.signalAll();
      } else {
        stateChange.await();
      }
    }

    String bucketName = state.getIndex(params.getIndexName()).nextBucketName();
    IVrixBucket bucket = new IVrixBucket(
        params.getIndexName(), bucketName, params.getRequestingNodeName(),
        nodesForReplication, params.getReplicationFactor()
    );
    try {
      state.addBucket(params.getIndexName(), bucket);
      bucket.createSelf(stateLock);
    } catch (Exception e) {
      state.removeBucket(params.getIndexName(), bucketName);
      bucket.deleteSelf(stateLock);
      throw e;
    }
    stateChange.signalAll();
    stateLock.unlock();
    return bucketName;
  }

  private Object executeUpdateBucketMetadataOperation(IVrixOverseerOperationParams params) throws Exception {
    stateLock.lock();
    IVrixBucket bucket = state.getBucket(params.getIndexName(), params.getBucketName());
    bucket.updateTimeBounds(params.getTimeBounds());
    stateLock.unlock();
    return bucket;
  }

  private Object executeRollHotBucketToWarmOperation(IVrixOverseerOperationParams params) throws Exception {
    stateLock.lock();
    while (true) {
      // takes care of interleaving from delete
      IVrixBucket bucket = state.getBucket(params.getIndexName(), params.getBucketName());
      if (!bucket.isAgeMarkedAs(HOT)) {
        break;
      } else if (bucket.isProgressionMarkedAs(NEUTRAL)) {
        bucket.rollToWarm(stateLock);
        stateChange.signalAll();
        break;
      } else {
        stateChange.await();
      }
    }
    stateLock.unlock();
    return true;
  }




  private Object executeGetAllIVrixIndexes(IVrixOverseerOperationParams params) throws Exception {
    stateLock.lock();
    Collection<String> allIndexNames = state.getAllIndexNames();
    stateLock.unlock();
    return allIndexNames;
  }

  private Object executeCreateIVrixIndex(IVrixOverseerOperationParams params) throws Exception {
    IVrixIndex index = new IVrixIndex(params.getIndexName());
    index.createSelf();
    stateLock.lock();
    state.addIndex(params.getIndexName(), index);
    stateLock.unlock();
    return true;
  }

  private Object executeDeleteIVrixIndex(IVrixOverseerOperationParams params) throws Exception {
    stateLock.lock();
    deleteIndex(params.getIndexName());
    stateChange.signalAll();
    stateLock.unlock();
    return true;
  }

  private void deleteIndex(String indexName) throws IOException, InterruptedException {
    IVrixIndex index = state.getIndex(indexName);
    index.markAsDeleted();
    for (String bucketName : state.getIndex(indexName).getAllBucketNames()) {
      IVrixBucket bucket = state.getIndex(indexName).getBucket(bucketName);
      if (bucket != null) {
        while (!bucket.isProgressionMarkedAs(NEUTRAL) && !bucket.isProgressionMarkedAs(DELETING_SELF) && !bucket.isProgressionMarkedAs(FAILED))
          stateChange.await();
        state.removeBucket(indexName, bucketName);
        if (bucket.isAttachmentStateMarkedAs(DETACHED))
          bucket.attach(stateLock);
        bucket.deleteSelf(stateLock);
      }
    }
    state.removeIndex(indexName);
    index.deleteSelf();
  }


























  private void ensureInState(String logMessage,
                             Supplier<IVrixBucket> findBucketWithIssue,
                             IConsumer<IVrixBucket> fixIssueInBucket) throws IOException, InterruptedException {
    log.info(logMessage);
    while (true) {
      IVrixBucket bucketWithIssue = findBucketWithIssue.get();
      if (bucketWithIssue == null) { // i.e, issue does not exist across all buckets
        break;
      } else if (bucketWithIssue.isProgressionMarkedAs(NEUTRAL) || bucketWithIssue.isProgressionMarkedAs(FAILED)) {
        fixIssueInBucket.accept(bucketWithIssue);
      } else {
        stateChange.await();
      }
    }
  }

  private void rollAllHotBucketsFromDownedIndexerNodes() throws IOException, InterruptedException {
    ensureInState("Rolling to warm all hot buckets from nodes that are currently down...",
        () -> {
          for (IVrixBucket bucket : state)
            if (bucket.isAgeMarkedAs(HOT) && !IVrixLocalNode.getClusterState().getLiveNodes().contains(bucket.getIndexerNodeName()))
              return bucket;
          return null;
        },
        bucketToRollToWarm -> bucketToRollToWarm.rollToWarm(stateLock)
    );
  }

  private void rollAllHotBucketsInIndexerNode(String nodeName) throws IOException, InterruptedException {
    ensureInState("Rolling to warm all hot buckets from node -- " + nodeName + "...",
        () -> {
          for (IVrixBucket bucket : state)
            if (bucket.isAgeMarkedAs(HOT) && Objects.equals(bucket.getIndexerNodeName(), nodeName))
              return bucket;
          return null;
        },
        bucketToRollToWarm -> bucketToRollToWarm.rollToWarm(stateLock)
    );
  }

  private void ensureStateConsistency() throws IOException, InterruptedException {
    log.info("Ensuring that IVrixDB state is consistent...");
    log.info("-- ensuring that index deletion operations result in a full deletion...");
    for (String indexName : state.getAllIndexNames()) {
      if (state.getIndex(indexName).isMarkedDeleted())
        this.deleteIndex(indexName);
    }
    ensureInState("-- ensuring that all unfinished deletion or creation operations result in a full deletion...",
        () -> {
          for (IVrixBucket bucket : state)
            if (bucket.isProgressionMarkedAs(NEUTRAL) || bucket.isProgressionMarkedAs(FAILED))
              if (!bucket.isConsideredOKEnoughToExist())
                return bucket;
          return null;
        },
        bucketToDelete -> {
          state.removeBucket(bucketToDelete.getIndexName(), bucketToDelete.getName());
          bucketToDelete.deleteSelf(stateLock);
        }
    );
    ensureInState( "-- ensuring that all unfinished cold-rollover operations result in a finished cold-rollover...",
        () -> {
          for (IVrixBucket bucket : state)
            if (bucket.isProgressionMarkedAs(NEUTRAL) || bucket.isProgressionMarkedAs(FAILED))
              if (bucket.isAgeMarkedAs(COLD) && !bucket.hasColdStateNumberOfReplicas())
                return bucket;
          return null;
        },
        bucketToFinishRollingToCold -> bucketToFinishRollingToCold.rollToCold(stateLock)
    );
    ensureInState( "-- ensuring that all unfinished detach operations result in a full detachment...",
        () -> {
          for (IVrixBucket bucket : state)
            if (bucket.isProgressionMarkedAs(NEUTRAL) || bucket.isProgressionMarkedAs(FAILED))
              if (bucket.isAttachmentStateMarkedAs(DETACHED) && !bucket.isDetachedOnAllLiveResidingNodes())
                return bucket;
          return null;
        },
        bucketToFinishDetaching -> bucketToFinishDetaching.detach(stateLock)
    );
    ensureInState( "-- ensuring that all unfinished attach operations result in a full attachment...",
        () -> {
          for (IVrixBucket bucket : state)
            if (bucket.isProgressionMarkedAs(NEUTRAL) || bucket.isProgressionMarkedAs(FAILED))
              if (bucket.isAttachmentStateMarkedAs(ATTACHED) && !bucket.isAttachedOnAllLiveResidingNodes())
                return bucket;
          return null;
        },
        bucketToFinishAttaching -> bucketToFinishAttaching.attach(stateLock)
    );
    ensureInState( "-- ensuring that the number of attached hot+warm buckets per node does not exceed the limit per node...",
        () -> {
          Set<String> liveNodes = IVrixLocalNode.getClusterState().getLiveNodes();
          for (String liveNode : liveNodes)
            if (MAXIMUM_ATTACHED_HOT_PLUS_WARM_BUCKETS_PER_NODE < state.getNumberOfAttachedHotAndWarmBucketsInNode(liveNode, true) ||
                MAXIMUM_ATTACHED_HOT_PLUS_WARM_BUCKETS_PER_NODE < state.getNumberOfAttachedHotAndWarmBucketsInNode(liveNode, false))
              if (null != this.getWarmBucketToRollToColdInNode(liveNode))
                return this.getWarmBucketToRollToColdInNode(liveNode);
          return null;
        },
        bucketToRollToCold -> bucketToRollToCold.rollToCold(stateLock)
    );
    ensureInState( "-- ensuring that the number of attached cold buckets per node does not exceed the limit per node...",
        () -> {
          Set<String> liveNodes = IVrixLocalNode.getClusterState().getLiveNodes();
          for (String liveNode : liveNodes)
            if (MAXIMUM_ATTACHED_COLD_BUCKETS_PER_NODE < state.getNumberOfAttachedColdBucketsInNode(liveNode, true) ||
                MAXIMUM_ATTACHED_COLD_BUCKETS_PER_NODE < state.getNumberOfAttachedColdBucketsInNode(liveNode, false))
              if (null != this.getColdBucketToDetachInNode(liveNode, true, false))
                return this.getColdBucketToDetachInNode(liveNode, true, false);
          return null;
        },
        bucketToDetach -> {
          bucketToDetach.forceRemoveAllAttachedStateHolders();
          bucketToDetach.detach(stateLock);
        }
    );
  }





















  private IVrixBucket getWarmBucketToRollToColdInNode(String nodeName) {
    IVrixBucket warmBucketToRoll = null;
    for (IVrixBucket bucket : state)
      if (bucket.getAllResidingNodes().contains(nodeName) && bucket.isAgeMarkedAs(WARM))

        if (warmBucketToRoll == null ||
            !bucket.doesTimeBoundsExist() ||
            (bucket.doesTimeBoundsExist() && warmBucketToRoll.doesTimeBoundsExist() && bucket.isOlderThan(warmBucketToRoll)))
          warmBucketToRoll = bucket;

    return warmBucketToRoll;
  }

  private IVrixBucket getColdBucketToDetachInNode(String nodeName, boolean isForced, boolean neutralProgressionOnly) {
    IVrixBucket coldBucketToDetach = null;
    for (IVrixBucket bucket : state)
      if (bucket.getAllResidingNodes().contains(nodeName) &&
          (isForced || !bucket.isAttachedStateBeingHeld()) &&
          (!neutralProgressionOnly || bucket.isProgressionMarkedAs(NEUTRAL)) &&
          bucket.isAgeMarkedAs(COLD) &&
          bucket.isAttachmentStateMarkedAs(ATTACHED))

        if (coldBucketToDetach == null ||
            !bucket.doesTimeBoundsExist() ||
            (bucket.doesTimeBoundsExist() && coldBucketToDetach.doesTimeBoundsExist() && bucket.isOlderThan(coldBucketToDetach)))
          coldBucketToDetach = bucket;

    return coldBucketToDetach;
  }


  @FunctionalInterface
  private interface ISupplier<T> { T get() throws Exception; }
  @FunctionalInterface
  private interface IConsumer<T> { void accept(T t) throws IOException; }
}
