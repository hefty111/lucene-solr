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

package org.apache.solr.ivrixdb.search.stream.search.bucket;

import java.io.IOException;
import java.util.*;

import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.stream.*;
import org.apache.solr.common.cloud.*;
import org.apache.solr.common.params.*;
import org.apache.solr.ivrixdb.core.IVrixLocalNode;
import org.apache.solr.ivrixdb.utilities.SolrObjectMocker;

import static org.apache.solr.common.params.CommonParams.DISTRIB;

/**
 * This stream reads from a physically held bucket.
 * It utilizes CloudSolrStream's method of searching Replicas
 * in order to rely on Solr's collection searching functionality
 * at the SolrCloud environment (including parallelization and workers).
 * It also modifies some of CloudSolrStream's behaviors to be more fault-tolerant.
 *
 * @author Ivri Faitelson
 */
/*
 * TODO -- fault-tolerance on read() needs to be optimized
 */
public class BucketSearchStream extends CloudSolrStream {
  private String baseURLOfBrokenStream = null;
  private Tuple lastTupleSent = null;

  /**
   * @param zkHost The zookeeper host
   * @param bucketName The name of the bucket
   * @param params The given parameters inside the search decorator
   */
  public BucketSearchStream(String zkHost, String bucketName, SolrParams params) throws IOException {
    super(zkHost, bucketName, params);
  }

  /**
   * Since CloudSolrStream's constructStreams() at opening
   * isn't fault-tolerant, re-implementation is necessary.
   */
  protected void constructStreams() throws IOException {
    ModifiableSolrParams mParams = new ModifiableSolrParams(params);
    mParams = adjustParams(mParams);
    mParams.set(DISTRIB, "false");

    List<String> urlsOfReplicasToQuery = getReplicasToQuery();
    for(String shardUrl : urlsOfReplicasToQuery) {
      SolrStream solrStream = new SolrStream(shardUrl, mParams);
      if(streamContext != null) {
        solrStream.setStreamContext(streamContext);
        if (streamContext.isLocal()) {
          solrStream.setDistrib(false);
        }
      }
      solrStream.setFieldMappings(this.fieldMappings);
      solrStreams.add(solrStream);
    }
  }

  /**
   * reads by utilizing CloudSolrStream's read() and implements fault-tolerancy on top of it.
   */
  @Override
  public Tuple read() throws IOException {
    Tuple tuple;
    if (solrStreams.size() != 0) {
      try {
        tuple = super.read();
      } catch (IOException e) {
        this.resetToLastRead();
        tuple = this.read();
      }
    } else {
      tuple = SolrObjectMocker.mockEOFTuple();
    }
    lastTupleSent = tuple;
    return tuple;
  }

  // taken and improved from TupleStream.getShards()
  private List<String> getReplicasToQuery() {
    ClusterState clusterState = IVrixLocalNode.getZkController().getClusterState();
    DocCollection collectionState = clusterState.getCollection(collection);
    Collection<String> liveSolrNodes = clusterState.getLiveNodes();

    List<String> urlsOfReplicasToQuery = new LinkedList<>();
    for (Slice shard : collectionState.getSlices()) {
      List<Replica> replicaShuffler = new ArrayList<>();

      for (Replica replica : shard.getReplicas()) {
        if (replica.getState() == Replica.State.ACTIVE && liveSolrNodes.contains(replica.getNodeName()) &&
            (baseURLOfBrokenStream == null || !replica.getBaseUrl().equals(baseURLOfBrokenStream))) {
          replicaShuffler.add(replica);
        }
      }

      if (replicaShuffler.size() > 0) {
        Collections.shuffle(replicaShuffler, new Random());
        Replica chosenReplica = replicaShuffler.get(0);
        String replicaURL = (new ZkCoreNodeProps(chosenReplica)).getCoreUrl();
        urlsOfReplicasToQuery.add(replicaURL);
      }
    }
    baseURLOfBrokenStream = null;
    return urlsOfReplicasToQuery;
  }

  private void resetToLastRead() throws IOException {
    IVrixLocalNode.log.info("resetting to last read for bucket " + this.collection + " ...");
    for (TupleStream solrStream : solrStreams) {
      try { solrStream.close(); }
      catch (Exception ignored) {}
    }
    baseURLOfBrokenStream = ((SolrStream)solrStreams.get(0)).getBaseUrl();
    Tuple lastTuple = lastTupleSent;
    while (true) {
      try { this.open(); break; }
      catch (IOException ignored) {}
    }
    if (solrStreams.size() != 0 && lastTuple != null) {
      while (true) {
        Tuple tuple = this.read();
        if (tuple.EOF) {
          IVrixLocalNode.log.error("reached EOF when resetting to last read for bucket " + this.collection);
          break;
        } else if (tuple.fields.equals(lastTuple.fields))
          break;
      }
    }
    IVrixLocalNode.log.info("finished resetting to last read for bucket " + this.collection);
  }
}
