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

import java.io.*;
import java.util.*;

import org.apache.solr.ivrixdb.core.overseer.IVrixOverseer;
import org.apache.solr.ivrixdb.core.overseer.utilities.IVrixStateZkUtil;
import org.slf4j.Logger;

import static org.apache.solr.ivrixdb.core.overseer.state.IVrixBucket.BucketAge.*;
import static org.apache.solr.ivrixdb.core.overseer.state.IVrixBucket.BucketAttachmentState.*;

/**
 * This object is responsible for storing a cache of IVrixDB's overall
 * state IVrixDB's state is composed of IVrix Indexes, which are comprised of
 * IVrix Buckets. Some state is persistable, and some is not.
 *
 * To learn more about IVrix Indexes and IVrix Buckets, please refer to
 * {@link IVrixBucket} and {@link IVrixIndex}.
 *
 * (IVrixState implements the fault-tolerant principles expressed in IVrixOverseer
 * {@link IVrixOverseer}).
 *
 * @author Ivri Faitelson
 */
public class IVrixState implements Iterable<IVrixBucket> {
  private static final Logger log = IVrixOverseer.log;

  private final Map<String, IVrixIndex> internalState = new HashMap<>();

  /**
   * Loads the persistent properties of the state from Zookeeper,
   * and has non-persistent properties at default values
   */
  public void loadFromZK() throws IOException {
    log.info("Loading IVrix state from ZK...");
    IVrixStateZkUtil.ensurePersistentStatePathExists();
    List<String> persistedIndexNames = IVrixStateZkUtil.retrieveAllPersistedIndexes();
    for (String indexName : persistedIndexNames)
      this.addIndex(indexName, IVrixIndex.loadFromZK(indexName));
  }



  /**
   * @return The number of attached HOT and WARM buckets in the given node
   * @param onlyConsiderLiveReplicas This flag decides whether a bucket counts if it has a live replica on the given node
   */
  public int getNumberOfAttachedHotAndWarmBucketsInNode(String nodeName, boolean onlyConsiderLiveReplicas) {
    int numberOfHotAndWarmBucketsAttached = 0;
    for (IVrixBucket bucket : this)
      if ((!onlyConsiderLiveReplicas || bucket.getAllLiveResidingNodesWithAttachedReplicas().contains(nodeName)) &&
          (onlyConsiderLiveReplicas || bucket.getAllResidingNodes().contains(nodeName)) &&
          (bucket.isAgeMarkedAs(HOT) || bucket.isAgeMarkedAs(WARM)) &&
          bucket.isAttachmentStateMarkedAs(ATTACHED))
        numberOfHotAndWarmBucketsAttached += 1;

    return numberOfHotAndWarmBucketsAttached;
  }

  /**
   * @return The number of attached COLD buckets in the given node
   * @param onlyConsiderLiveReplicas This flag decides whether a bucket counts if it has a live replica on the given node
   */
  public int getNumberOfAttachedColdBucketsInNode(String nodeName, boolean onlyConsiderLiveReplicas) {
    int numberOfColdBucketsAttached = 0;
    for (IVrixBucket bucket : this)
      if ((!onlyConsiderLiveReplicas || bucket.getAllLiveResidingNodesWithAttachedReplicas().contains(nodeName)) &&
          (onlyConsiderLiveReplicas || bucket.getAllResidingNodes().contains(nodeName)) &&
          bucket.isAgeMarkedAs(COLD) &&
          bucket.isAttachmentStateMarkedAs(ATTACHED))
        numberOfColdBucketsAttached += 1;

    return numberOfColdBucketsAttached;
  }



  /**
   * Add an IVrix Index to state
   */
  public void addIndex(String indexName, IVrixIndex index) {
    internalState.put(indexName, index);
  }

  /**
   * remove an IVrix Index from state
   */
  public void removeIndex(String indexName) {
    internalState.remove(indexName);
  }

  /**
   * @return get an Ivrix Index with a name matching the given name
   */
  public IVrixIndex getIndex(String indexName) {
    return internalState.get(indexName);
  }

  /**
   * @return The names of all the IVrix Indexes in state
   */
  public Set<String> getAllIndexNames() {
    return new HashSet<>(internalState.keySet());
  }

  /**
   * Adds a bucket to a IVrix Index
   */
  public void addBucket(String indexName, IVrixBucket bucket) {
    internalState.get(indexName).addBucket(bucket);
  }

  /**
   * Removes a bucket from an IVrix Index
   */
  public void removeBucket(String indexName, String bucketName) {
    if (internalState.get(indexName) != null)
      internalState.get(indexName).removeBucket(bucketName);
  }

  /**
   * @return a bucket matching a given name from an Ivrix Index matching a given name
   */
  public IVrixBucket getBucket(String indexName, String bucketName) {
    if (internalState.get(indexName) != null && !internalState.get(indexName).isMarkedDeleted())
      return internalState.get(indexName).getBucket(bucketName);
    return null;
  }

  /**
   * @return An all-buckets iterator across all IVrix Indexes
   */
  @Override
  public Iterator<IVrixBucket> iterator() {
    return new Iterator<>() {
      private final Iterator<IVrixIndex> indexesIterator = internalState.values().iterator();
      private Iterator<IVrixBucket> indexIterator = null;

      @Override
      public boolean hasNext() {
        if (indexesIterator.hasNext() && (indexIterator == null || !indexIterator.hasNext()))
          indexIterator = indexesIterator.next().iterator();
        return indexIterator != null && indexIterator.hasNext();
      }

      @Override
      public IVrixBucket next() {
        return indexIterator.next();
      }
    };
  }
}
