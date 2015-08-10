/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.discovery;

import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.index.IndexAction;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.allocation.command.MoveAllocationCommand;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.store.IndicesStoreIntegrationIT;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.disruption.BlockClusterStateProcessing;
import org.elasticsearch.test.disruption.SingleNodeDisruption;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.transport.TransportModule;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportService;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import static java.lang.Thread.sleep;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

/**
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0, transportClientRatio = 0)
public class EndlessIndexingLoopIT extends ESIntegTestCase {

    /**
     * When a primary is relocating from node_1 to node_2, there can be a short time where the old primary is removed from the node
     * already (closed, not deleted) but the new primary is still in POST_RECOVERY.
     * In this case we must make sure node_1 and node_2 do not send an index command back and forth endlessly.
     * <p/>
     * Course of events:
     * 0. primary ([index][0]) relocates from node_1 to node_2
     * 1. node_2 is done recovering, moves its shard to IndexShardState.POST_RECOVERY and sends a message to master that the shard is ShardRoutingState.STARTED
     * Cluster state 1:
     * node_1: [index][0] RELOCATING   (ShardRoutingState), (STARTED from IndexShardState perspective on node_1)
     * node_2: [index][0] INITIALIZING (ShardRoutingState), (at this point already POST_RECOVERY from IndexShardState perspective on node_2)
     * 2. master receives shard started and updates cluster state to:
     * Cluster state 2:
     * node_1: [index][0] no shard
     * node_2: [index][0] STARTED (ShardRoutingState), (at this point still in POST_RECOVERY from IndexShardState perspective on node_2)
     * master sends this to node_1 and node_2
     * 3. node_1 receives the new cluster state and removes its shard because it is not allocated on node_1 anymore
     * 4. index a document
     * At this point node_1 is already on cluster state 2 and does not have the shard anymore so it forwards the request to node_2.
     * But node_2 is behind with cluster state processing, is still on cluster state 1 and therefore has the shard in
     * IndexShardState.POST_RECOVERY and thinks node_1 has the primary. So it will send the request back to node_1.
     * This goes on until either node_2 finally catches up and processes cluster state 2 or both nodes OOM.
     */
    @Test
    public void testIndexOperationNotSentBackAndForthAllTheTime() throws Exception {
        Settings mockTransportSetting = Settings.builder().put(TransportModule.TRANSPORT_SERVICE_TYPE_KEY, MockTransportService.class.getName()).build();
        Future<String> masterNodeFuture = internalCluster().startMasterOnlyNodeAsync(mockTransportSetting);
        Future<String> node1Future = internalCluster().startDataOnlyNodeAsync(mockTransportSetting);
        final String masterNode = masterNodeFuture.get();
        final String node_1 = node1Future.get();

        logger.info("--> creating index [test] with one shard and zero replica");
        assertAcked(prepareCreate("test").setSettings(
                        Settings.builder().put(indexSettings())
                                .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
                                .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0))
                        .addMapping("doc", jsonBuilder().startObject().startObject("doc")
                                .startObject("properties").startObject("text").field("type", "string").endObject().endObject()
                                .endObject().endObject())
        );
        ensureGreen("test");
        logger.info("--> starting one more data node");
        Future<String> node2NameFuture = internalCluster().startDataOnlyNodeAsync(mockTransportSetting);
        final String node_2 = node2NameFuture.get();
        logger.info("--> running cluster_health");
        ClusterHealthResponse clusterHealth = client().admin().cluster().prepareHealth()
                .setWaitForNodes("3")
                .setWaitForRelocatingShards(0)
                .get();
        assertThat(clusterHealth.isTimedOut(), equalTo(false));

        logger.info("--> move shard from node_1 to node_2, and wait for relocation to finish");

        // register Tracer that will signal when relocations starts and ends
        MockTransportService transportServiceNode2 = (MockTransportService) internalCluster().getInstance(TransportService.class, node_2);
        CountDownLatch beginRelocationLatchNode2 = new CountDownLatch(1);
        CountDownLatch endRelocationLatchNode2 = new CountDownLatch(1);
        transportServiceNode2.addTracer(new IndicesStoreIntegrationIT.ReclocationStartEndTracer(logger, beginRelocationLatchNode2, endRelocationLatchNode2));
        // register a Tracer that will count the number of sent indexing requests on node_2
        final AtomicInteger numSentIndexRequests = new AtomicInteger(0);
        transportServiceNode2.addTracer(new MockTransportService.Tracer() {
            @Override
            public void requestSent(DiscoveryNode node, long requestId, String action, TransportRequestOptions options) {
                if (action.equals(IndexAction.NAME)) {
                    numSentIndexRequests.incrementAndGet();
                }
            }
        });

        // node_2 should fall behind with cluster state processing. we start the disruption later when relocation starts
        SingleNodeDisruption disruptionNode2 = new BlockClusterStateProcessing(node_2, getRandom());
        internalCluster().setDisruptionScheme(disruptionNode2);

        logger.info("--> move shard from {} to {}", node_1, node_2);
        internalCluster().client().admin().cluster().prepareReroute().add(new MoveAllocationCommand(new ShardId("test", 0), node_1, node_2)).get();

        logger.info("--> wait for relocation to start");
        beginRelocationLatchNode2.await();
        // start to block cluster state processing for node_2 so that it will be stuck with the cluster state 1 in above description
        disruptionNode2.startDisrupting();

        logger.info("--> wait for relocation to finish");
        endRelocationLatchNode2.await();
        // now node_2 is still on cluster state 1 but will have have the shard moved to POST_RECOVERY
        final Client node1Client = internalCluster().client(node_1);
        final Client node2Client = internalCluster().client(node_2);
        // wait until node_1 actually has removed the shard
        assertBusy(new Runnable() {
            @Override
            public void run() {
                ClusterState clusterState = node1Client.admin().cluster().prepareState().setLocal(true).get().getState();
                // get the node id from the name. TODO: Is there a better way to do this?
                String nodeId = null;
                for (RoutingNode node : clusterState.getRoutingNodes()) {
                    if (node.node().name().equals(node_1)) {
                        nodeId = node.nodeId();
                    }
                }
                assertNotNull(nodeId);
                // check that node_1 actually has removed the shard
                assertFalse(clusterState.getRoutingNodes().routingNodeIter(nodeId).hasNext());
            }
        });

        logger.info("--> cluster state on {} {}", node_1, node1Client.admin().cluster().prepareState().setLocal(true).get().getState().prettyPrint());
        logger.info("--> cluster state on {} {}", node_2, node2Client.admin().cluster().prepareState().setLocal(true).get().getState().prettyPrint());
        logger.info("--> index doc");
        Future<IndexResponse> indexResponseFuture = client(node_2).prepareIndex("test", "doc").setSource("{\"text\":\"a\"}").execute();
        // wait a little and then see how often the indexing request was sent back and forth
        sleep(1000);
        // stop disrupting so that node_2 can finally apply cluster state 2
        logger.info("--> stop disrupting");
        disruptionNode2.stopDisrupting();
        clusterHealth = client().admin().cluster().prepareHealth()
                .setWaitForRelocatingShards(0)
                .get();
        assertThat(clusterHealth.isTimedOut(), equalTo(false));
        indexResponseFuture.get();
        refresh();
        assertThat(client().prepareCount().get().getCount(), equalTo(1l));
        // check that only one indexing request was sent at most
        assertThat(numSentIndexRequests.get(), lessThanOrEqualTo(1));
    }
}