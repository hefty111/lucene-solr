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

package org.apache.solr.ivrixdb.core.overseer.utilities;

import java.io.*;
import java.util.*;

import org.apache.solr.common.cloud.*;
import org.apache.solr.common.util.Utils;
import org.apache.solr.ivrixdb.core.IVrixLocalNode;
import org.apache.solr.ivrixdb.core.overseer.state.IVrixBucket;
import org.apache.solr.ivrixdb.core.overseer.state.IVrixIndex;
import org.apache.zookeeper.*;

/**
 * This class is a collection of functions that update IVrixDB's persistent
 * state/metadata, which includes IVrix Indexes and IVrix Buckets.
 *
 * @author Ivri Faitelson
 */
public class IVrixStateZkUtil {
  public static final String BUCKET_AGE_KEY = "bucketAge";
  public static final String IS_DETACHED_KEY = "isDetached";
  public static final String TIME_BOUNDS_KEY = "timeBounds";
  public static final String LAST_GEN_NUM_KEY = "lastGenNum";
  public static final String MARKED_DELETED_KEY = "markedDeleted";
  public static final String INDEXER_NODE_NAME_KEY = "indexerNodeName";
  public static final String SOLR_COLLECTION_METADATA_KEY = "solrCollectionMetadata";
  private static final String PERSISTENT_STATE_PATH = "/ivrix_buckets";




  /**
   * Ensures the Zookeeper path for the persistent state exists
   */
  public static void ensurePersistentStatePathExists() throws IOException {
    try {
      IVrixLocalNode.getZkClient().makePath(PERSISTENT_STATE_PATH, false, true);
    } catch (InterruptedException | KeeperException e) {
      throw new IOException(e);
    }
  }

  /**
   * Deletes a path (and all subsequent sub-paths) of an IVrix Index
   */
  public static void deleteIVrixIndexPath(String indexName) throws IOException {
    String ivrixIndexPath = PERSISTENT_STATE_PATH + "/" + indexName;
    try {
      IVrixLocalNode.getZkClient().clean(ivrixIndexPath);
    } catch (InterruptedException | KeeperException e) {
      throw new IOException(e);
    }
  }

  /**
   * Deletes a path (and all subsequent sub-paths) of an IVrix Bucket
   */
  public static void deleteIVrixBucketPath(IVrixBucket bucket) throws IOException {
    String ivrixBucketPath = PERSISTENT_STATE_PATH + "/" + bucket.getIndexName() + "/" + bucket.getName();
    try {
      IVrixLocalNode.getZkClient().clean(ivrixBucketPath);
    } catch (InterruptedException | KeeperException e) {
      throw new IOException(e);
    }
  }



  /**
   * @return A list of the names of all persistent IVrix Indexes
   */
  public static List<String> retrieveAllPersistedIndexes() throws IOException {
    List<String> bucketNames;
    try {
      bucketNames = IVrixLocalNode.getZkClient().getChildren(PERSISTENT_STATE_PATH, null, true);
    } catch (InterruptedException | KeeperException e) {
      throw new IOException(e);
    }
    return bucketNames;
  }

  /**
   * @return A list of the names of all persistent IVrix Buckets for an IVrix Index
   */
  public static List<String> retrieveAllPersistedBucketNames(String indexName) throws IOException {
    List<String> bucketNames;
    try {
      bucketNames = IVrixLocalNode.getZkClient().getChildren(PERSISTENT_STATE_PATH + "/" + indexName, null, true);
    } catch (InterruptedException | KeeperException e) {
      throw new IOException(e);
    }
    return bucketNames;
  }



  /**
   * Creates a ZK node for an IVrix Index and stores its main metadata as a blob.
   * If the node already exists, it will update the ZK node with the current main metadata.
   */
  public static void createIndexBlobNode(IVrixIndex index) throws IOException {
    String indexBlobPath = PERSISTENT_STATE_PATH + "/" + index.getIndexName();
    createBlobNode(indexBlobPath, index.getMainPersistenceBlob());
  }

  /**
   * updates the ZK node of an IVrix Index with the main metadata
   */
  public static void updateIndexBlobNode(IVrixIndex index) throws IOException {
    String indexBlobPath = PERSISTENT_STATE_PATH + "/" + index.getIndexName();
    updateBlobNode(indexBlobPath, index.getMainPersistenceBlob());
  }

  /**
   * @return The main metadata of an IVrix Index
   */
  public static Map<String, Object> retrieveIndexBlob(String indexName) throws IOException {
    String indexBlobPath = PERSISTENT_STATE_PATH + "/" + indexName;
    return retrieveBlob(indexBlobPath);
  }



  /**
   * Creates a ZK node for an IVrix Bucket and stores its main metadata as a blob.
   * If the node already exists, it will update the ZK node with the current main metadata.
   */
  public static void createBucketBlobNode(IVrixBucket bucket) throws IOException {
    String bucketBlobPath = PERSISTENT_STATE_PATH + "/" + bucket.getIndexName() + "/" + bucket.getName();
    createBlobNode(bucketBlobPath, bucket.getMainPersistenceBlob());
  }

  /**
   * updates the ZK node of an IVrix Bucket with the main metadata
   */
  public static void updateBucketBlobNode(IVrixBucket bucket) throws IOException {
    String bucketBlobPath = PERSISTENT_STATE_PATH + "/" + bucket.getIndexName() + "/" + bucket.getName();
    updateBlobNode(bucketBlobPath, bucket.getMainPersistenceBlob());
  }

  /**
   * @return The main metadata of an IVrix Bucket
   */
  public static Map<String, Object> retrieveBucketBlob(String indexName, String bucketName) throws IOException {
    String bucketBlobPath = PERSISTENT_STATE_PATH + "/" + indexName + "/" + bucketName;
    return retrieveBlob(bucketBlobPath);
  }



  /**
   * Creates a bucket age sub-ZK-node for an IVrix Bucket and stores the bucket's age in there.
   * If the node already exists, it will update the ZK node with the current bucket age.
   */
  public static void createBucketAgePropertyNode(IVrixBucket bucket) throws IOException {
    String propertyPath = PERSISTENT_STATE_PATH + "/" + bucket.getIndexName() + "/" + bucket.getName() + "/" + BUCKET_AGE_KEY;
    createPropertyNode(propertyPath, bucket.getBucketAge() == null ? null : bucket.getBucketAge().name());
  }

  /**
   * Updates a bucket age sub-ZK-node for an IVrix Bucket with the current bucket age
   */
  public static void updateBucketAgePropertyNode(IVrixBucket bucket) throws IOException {
    String propertyPath = PERSISTENT_STATE_PATH + "/" + bucket.getIndexName() + "/" + bucket.getName() + "/" + BUCKET_AGE_KEY;
    updatePropertyNode(propertyPath, bucket.getBucketAge() == null ? null : bucket.getBucketAge().name());
  }

  /**
   * @return The bucket age of an IVrix Bucket
   */
  public static String retrieveBucketAgePropertyValue(String indexName, String bucketName) throws IOException {
    String propertyPath = PERSISTENT_STATE_PATH + "/" + indexName + "/" + bucketName + "/" + BUCKET_AGE_KEY;
    return (String) retrieveProperty(propertyPath);
  }



  /**
   * Creates an is-detached sub-ZK-node for an IVrix Bucket and stores the attachment state in there.
   * If the node already exists, it will update the ZK node with the current attachment state.
   */
  public static void createIsDetachedPropertyNode(IVrixBucket bucket) throws IOException {
    String propertyPath = PERSISTENT_STATE_PATH + "/" + bucket.getIndexName() + "/" + bucket.getName() + "/" + IS_DETACHED_KEY;
    createPropertyNode(propertyPath, bucket.isAttachmentStateMarkedAs(IVrixBucket.BucketAttachmentState.DETACHED));
  }

  /**
   * Updates an is-detached sub-ZK-node for an IVrix Bucket with the current attachment state
   */
  public static void updateIsDetachedPropertyNode(IVrixBucket bucket) throws IOException {
    String propertyPath = PERSISTENT_STATE_PATH + "/" + bucket.getIndexName() + "/" + bucket.getName() + "/" + IS_DETACHED_KEY;
    updatePropertyNode(propertyPath, bucket.isAttachmentStateMarkedAs(IVrixBucket.BucketAttachmentState.DETACHED));
  }

  /**
   * @return The attachment (is-detached) state of an IVrix Bucket
   */
  public static Boolean retrieveIsDetachedPropertyValue(String indexName, String bucketName) throws IOException {
    String propertyPath = PERSISTENT_STATE_PATH + "/" + indexName + "/" + bucketName + "/" + IS_DETACHED_KEY;
    return (Boolean) retrieveProperty(propertyPath);
  }





  private static void createPropertyNode(String fullPropertyPath, Object propertyValue) throws IOException {
    try {
      byte[] rawData = serialize(propertyValue);
      IVrixLocalNode.getZkClient().create(fullPropertyPath, rawData, CreateMode.PERSISTENT, true);
    } catch (InterruptedException | KeeperException e) {
      updatePropertyNode(fullPropertyPath, propertyValue);
    }
  }

  private static void updatePropertyNode(String fullPropertyPath, Object propertyValue) throws IOException {
    try {
      byte[] rawData = serialize(propertyValue);
      IVrixLocalNode.getZkClient().setData(fullPropertyPath, rawData, true);
    } catch (KeeperException.NoNodeException e) {
    } catch (InterruptedException | KeeperException e) {
      throw new IOException(e);
    }
  }

  private static Object retrieveProperty(String fullPropertyPath) throws IOException {
    Object propertyValue = null;
    try {
      byte[] rawData = IVrixLocalNode.getZkClient().getData(fullPropertyPath, null, null, true);
      propertyValue = deserialize(rawData);
    } catch (KeeperException.NoNodeException e) {
    } catch (InterruptedException | KeeperException e) {
      throw new IOException(e);
    }
    return propertyValue;
  }





  private static void createBlobNode(String fullBlobPath, Map<String, Object> blob) throws IOException {
    SolrZkClient zkClient = IVrixLocalNode.getZkClient();
    byte[] serializedBlob = serializeBlob(blob);
    try {
      zkClient.create(fullBlobPath, serializedBlob, CreateMode.PERSISTENT, true);
    } catch (InterruptedException | KeeperException e) {
      updateBlobNode(fullBlobPath, blob);
    }
  }

  private static void updateBlobNode(String fullBlobPath, Map<String, Object> blob) throws IOException {
    SolrZkClient zkClient = IVrixLocalNode.getZkClient();
    byte[] serializedBlob = serializeBlob(blob);
    try {
      zkClient.setData(fullBlobPath, serializedBlob, true);
    } catch (KeeperException.NoNodeException e) {
    } catch (InterruptedException | KeeperException e) {
      throw new IOException(e);
    }
  }

  private static Map<String, Object> retrieveBlob(String fullBlobPath) throws IOException {
    Map<String, Object> blob = null;
    try {
      byte[] rawData = IVrixLocalNode.getZkClient().getData(fullBlobPath, null, null, true);
      blob = deserializeBlob(rawData);
    } catch (KeeperException.NoNodeException e) {
    } catch (InterruptedException | KeeperException e) {
      throw new IOException(e);
    }
    return blob;
  }





  private static byte[] serializeBlob(Map<String, Object> metadata) {
    return Utils.toJSON(new ZkNodeProps(metadata));
  }

  private static Map<String, Object> deserializeBlob(byte[] data) {
    return ZkNodeProps.load(data).getProperties();
  }

  private static byte[] serialize(Object obj) throws IOException {
    ByteArrayOutputStream outputByteStream = new ByteArrayOutputStream();
    ObjectOutputStream outputObjectStream = new ObjectOutputStream(outputByteStream);
    outputObjectStream.writeObject(obj);
    return outputByteStream.toByteArray();
  }

  private static Object deserialize(byte[] data) throws IOException {
    ByteArrayInputStream inputByteStream = new ByteArrayInputStream(data);
    ObjectInputStream inputObjectStream = new ObjectInputStream(inputByteStream);
    try {
      return inputObjectStream.readObject();
    } catch (ClassNotFoundException e) {
      throw new IOException(e);
    }
  }
}
