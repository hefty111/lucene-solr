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

import org.apache.solr.ivrixdb.core.overseer.IVrixOverseer;
import org.apache.solr.ivrixdb.core.overseer.utilities.IVrixStateZkUtil;
import static org.apache.solr.ivrixdb.core.overseer.utilities.IVrixStateZkUtil.LAST_GEN_NUM_KEY;
import static org.apache.solr.ivrixdb.core.overseer.utilities.IVrixStateZkUtil.MARKED_DELETED_KEY;

/**
 * This object represents an IVrix Index. An IVrix index is a schema-less and distributed
 * time-series index with data aging capabilities. It is separated into buckets,
 * where each bucket is a slice of the index. The schema of the Index is composed of
 * two fields: a raw log, and the extracted timestamp. Each bucket is the equivalent to
 * a Solr shard, where its age rolls through the states of HOT, WARM, and COLD.
 * As an IVrix index grows, and reaches certain criteria, it will perform bucket rollovers,
 * where the oldest buckets in each age state are rolled to the next state. This occurs when
 * the IVrix Overseer executes a CREATE BUCKET operation.
 *
 * An IVrix Index is a simply a namespace, and therefore only exists in IVrixDB's state.
 * This object is responsible for creating/deleting the Index, for storing and updating its
 * persistable metadata, and for retrieving IVrix Buckets and caching them in-memory.
 * To learn more about IVrix Buckets, please refer to {@link IVrixBucket}.
 *
 * (IVrixIndex implements the fault-tolerant principles expressed in IVrixOverseer {@link IVrixOverseer})
 *
 * @author Ivri Faitelson
 */
public class IVrixIndex implements Iterable<IVrixBucket> {
  private final Map<String, IVrixBucket> bucketMap;
  private final String indexName;

  private long lastGeneratedBucketNumber;
  private boolean markedDeleted;

  /**
   * Creates a new, default, and empty IVrix Index
   * @param indexName The name of this index
   */
  public IVrixIndex(String indexName) {
    this.bucketMap = new HashMap<>();
    this.indexName = indexName;
    this.lastGeneratedBucketNumber = 0;
    this.markedDeleted = false;
  }

  /**
   * @return Loads the persistent properties of an IVrix Index and its respective Buckets from Zookeeper,
   *          and has non-persistent properties at default values
   */
  public static IVrixIndex loadFromZK(String indexName) throws IOException {
    IVrixIndex index = new IVrixIndex(indexName);
    Map<String, Object> metadataBlob = IVrixStateZkUtil.retrieveIndexBlob(indexName);
    List<String> bucketNames = IVrixStateZkUtil.retrieveAllPersistedBucketNames(indexName);

    for (String bucketName : bucketNames)
      index.addBucket(IVrixBucket.loadFromZK(indexName, bucketName));
    index.lastGeneratedBucketNumber = (Long)metadataBlob.get(LAST_GEN_NUM_KEY);
    index.markedDeleted = (Boolean)metadataBlob.get(MARKED_DELETED_KEY);
    return index;
  }



  /**
   * Creates self in IVrixDB Zookeeper state
   */
  public void createSelf() throws IOException {
    IVrixStateZkUtil.createIndexBlobNode(this);
  }

  /**
   * Deletes self from IVrixDB Zookeeper state
   */
  public void deleteSelf() throws IOException {
    IVrixStateZkUtil.deleteIVrixIndexPath(indexName);
  }

  /**
   * Marks self as deleted/deletion-in-progress
   */
  public void markAsDeleted() throws IOException {
    this.markedDeleted = true;
    IVrixStateZkUtil.updateIndexBlobNode(this);
  }

  /**
   * @return a new unique name for a new bucket
   */
  public String nextBucketName() throws IOException {
    this.lastGeneratedBucketNumber += 1;
    IVrixStateZkUtil.updateIndexBlobNode(this);
    return indexName + lastGeneratedBucketNumber;
  }

  /**
   * Adds an IVrix Bucket to this index
   */
  public void addBucket(IVrixBucket bucket) {
    bucketMap.put(bucket.getName(), bucket);
  }

  /**
   * Removes an IVrix Bucket from this index
   */
  public void removeBucket(String bucketName) {
    bucketMap.remove(bucketName);
  }

  /**
   * @return a bucket with the given bucket name
   */
  public IVrixBucket getBucket(String bucketName) {
    return bucketMap.get(bucketName);
  }

  /**
   * @return all the names of all the buckets in this index
   */
  public Set<String> getAllBucketNames() {
    return new HashSet<>(bucketMap.keySet());
  }

  /**
   * @return the name of this index
   */
  public String getIndexName() {
    return indexName;
  }

  /**
   * @return whether this index is marked as deleted/deletion-in-progress
   */
  public boolean isMarkedDeleted() {
    return markedDeleted;
  }


  /**
   * @return A Map blob containing the main persisted metadata of this index
   */
  public Map<String, Object> getMainPersistenceBlob() {
    Map<String, Object> metadataMap = new HashMap<>();
    metadataMap.put(LAST_GEN_NUM_KEY, lastGeneratedBucketNumber);
    metadataMap.put(MARKED_DELETED_KEY, markedDeleted);
    return metadataMap;
  }

  @Override
  public Iterator<IVrixBucket> iterator() {
    return bucketMap.values().iterator();
  }
}
