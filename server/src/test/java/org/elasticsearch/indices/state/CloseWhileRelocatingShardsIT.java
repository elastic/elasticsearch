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
package org.elasticsearch.indices.state;

import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.action.admin.cluster.reroute.ClusterRerouteRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.command.AllocationCommands;
import org.elasticsearch.cluster.routing.allocation.command.MoveAllocationCommand;
import org.elasticsearch.cluster.routing.allocation.decider.ConcurrentRebalanceAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.EnableAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.EnableAllocationDecider.Rebalance;
import org.elasticsearch.cluster.routing.allocation.decider.ThrottlingAllocationDecider;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.recovery.PeerRecoverySourceService;
import org.elasticsearch.indices.recovery.StartRecoveryRequest;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.BackgroundIndexer;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.test.transport.StubbableTransport;
import org.elasticsearch.transport.TransportService;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.util.Collections.singletonList;
import static org.elasticsearch.indices.state.CloseIndexIT.assertException;
import static org.elasticsearch.indices.state.CloseIndexIT.assertIndexIsClosed;
import static org.elasticsearch.indices.state.CloseIndexIT.assertIndexIsOpened;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;

@ESIntegTestCase.ClusterScope(minNumDataNodes = 2)
public class CloseWhileRelocatingShardsIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return singletonList(MockTransportService.TestPlugin.class);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        final int maxRecoveries = Integer.MAX_VALUE;
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(ThrottlingAllocationDecider.CLUSTER_ROUTING_ALLOCATION_NODE_CONCURRENT_RECOVERIES_SETTING.getKey(), maxRecoveries)
            .put(ThrottlingAllocationDecider.CLUSTER_ROUTING_ALLOCATION_NODE_INITIAL_PRIMARIES_RECOVERIES_SETTING.getKey(), maxRecoveries)
            .put(ConcurrentRebalanceAllocationDecider.CLUSTER_ROUTING_ALLOCATION_CLUSTER_CONCURRENT_REBALANCE_SETTING.getKey(), -1)
            .build();
    }

    @Override
    protected int maximumNumberOfShards() {
        return 3;
    }

    public void testCloseWhileRelocatingShards() throws Exception {
        final String[] indices = new String[randomIntBetween(3, 5)];
        final Map<String, Long> docsPerIndex = new HashMap<>();
        final Map<String, BackgroundIndexer> indexers = new HashMap<>();

        for (int i = 0; i < indices.length; i++) {
            final String indexName = "index-" + i;
            int nbDocs = 0;
            switch (i) {
                case 0:
                    logger.debug("creating empty index {}", indexName);
                    createIndex(indexName);
                    break;
                case 1:
                    nbDocs = scaledRandomIntBetween(1, 100);
                    logger.debug("creating index {} with {} documents", indexName, nbDocs);
                    createIndex(indexName);
                    indexRandom(randomBoolean(), IntStream.range(0, nbDocs)
                        .mapToObj(n -> client().prepareIndex(indexName).setSource("num", n))
                        .collect(Collectors.toList()));
                    break;
                default:
                    logger.debug("creating index {} with background indexing", indexName);
                    final BackgroundIndexer indexer = new BackgroundIndexer(indexName, "_doc", client(), -1, 1);
                    indexers.put(indexName, indexer);
                    waitForDocs(1, indexer);
            }
            docsPerIndex.put(indexName, (long) nbDocs);
            indices[i] = indexName;
        }

        ensureGreen(indices);
        assertAcked(client().admin().cluster().prepareUpdateSettings()
            .setTransientSettings(Settings.builder()
                .put(EnableAllocationDecider.CLUSTER_ROUTING_REBALANCE_ENABLE_SETTING.getKey(), Rebalance.NONE.toString())));

        final String targetNode = internalCluster().startDataOnlyNode();
        ensureClusterSizeConsistency(); // wait for the master to finish processing join.

        try {
            final ClusterService clusterService = internalCluster().getInstance(ClusterService.class, internalCluster().getMasterName());
            final ClusterState state = clusterService.state();
            final CountDownLatch latch = new CountDownLatch(indices.length);
            final CountDownLatch release = new CountDownLatch(indices.length);

            // relocate one shard for every index to be closed
            final AllocationCommands commands = new AllocationCommands();
            for (final String index : indices) {
                final NumShards numShards = getNumShards(index);
                final int shardId = numShards.numPrimaries == 1 ? 0 : randomIntBetween(0, numShards.numPrimaries - 1);
                final IndexRoutingTable indexRoutingTable = state.routingTable().index(index);

                final ShardRouting primary = indexRoutingTable.shard(shardId).primaryShard();
                assertTrue(primary.started());

                String currentNodeId = primary.currentNodeId();
                if (numShards.numReplicas > 0) {
                    final ShardRouting replica = indexRoutingTable.shard(shardId).replicaShards().iterator().next();
                    assertTrue(replica.started());
                    if (randomBoolean()) {
                        currentNodeId = replica.currentNodeId();
                    }
                }
                commands.add(new MoveAllocationCommand(index, shardId, state.nodes().resolveNode(currentNodeId).getName(), targetNode));
            }

            // Build the list of shards for which recoveries will be blocked
            final Set<ShardId> blockedShards = commands.commands().stream()
                .map(c -> (MoveAllocationCommand) c)
                .map(c -> new ShardId(clusterService.state().metaData().index(c.index()).getIndex(), c.shardId()))
                .collect(Collectors.toSet());
            assertThat(blockedShards, hasSize(indices.length));

            final Set<String> acknowledgedCloses = ConcurrentCollections.newConcurrentSet();
            final Set<String> interruptedRecoveries = ConcurrentCollections.newConcurrentSet();

            // Create a SendRequestBehavior that will block outgoing start recovery request
            final StubbableTransport.SendRequestBehavior sendBehavior = (connection, requestId, action, request, options) -> {
                if (PeerRecoverySourceService.Actions.START_RECOVERY.equals(action)) {
                    final StartRecoveryRequest startRecoveryRequest = ((StartRecoveryRequest) request);
                    if (blockedShards.contains(startRecoveryRequest.shardId())) {
                        logger.debug("blocking recovery of shard {}", startRecoveryRequest.shardId());
                        latch.countDown();
                        try {
                            release.await();
                            logger.debug("releasing recovery of shard {}", startRecoveryRequest.shardId());
                        } catch (final InterruptedException e) {
                            logger.warn(() -> new ParameterizedMessage("exception when releasing recovery of shard {}",
                                startRecoveryRequest.shardId()), e);
                            interruptedRecoveries.add(startRecoveryRequest.shardId().getIndexName());
                            Thread.currentThread().interrupt();
                            return;
                        }
                    }
                }
                connection.sendRequest(requestId, action, request, options);
            };

            final MockTransportService targetTransportService =
                (MockTransportService) internalCluster().getInstance(TransportService.class, targetNode);

            for (DiscoveryNode node : state.getNodes()) {
                if (node.isDataNode() && node.getName().equals(targetNode) == false) {
                    final TransportService sourceTransportService = internalCluster().getInstance(TransportService.class, node.getName());
                    targetTransportService.addSendBehavior(sourceTransportService, sendBehavior);
                }
            }

            assertAcked(client().admin().cluster().reroute(new ClusterRerouteRequest().commands(commands)).get());

            // start index closing threads
            final List<Thread> threads = new ArrayList<>();
            for (final String indexToClose : indices) {
                final Thread thread = new Thread(() -> {
                    try {
                        latch.await();
                    } catch (InterruptedException e) {
                        throw new AssertionError(e);
                    } finally {
                        release.countDown();
                    }
                    // Closing is not always acknowledged when shards are relocating: this is the case when the target shard is initializing
                    // or is catching up operations. In these cases the TransportVerifyShardBeforeCloseAction will detect that the global
                    // and max sequence number don't match and will not ack the close.
                    AcknowledgedResponse closeResponse = client().admin().indices().prepareClose(indexToClose).get();
                    if (closeResponse.isAcknowledged()) {
                        assertTrue("Index closing should not be acknowledged twice", acknowledgedCloses.add(indexToClose));
                    }
                });
                threads.add(thread);
                thread.start();
            }

            latch.countDown();
            for (Thread thread : threads) {
                thread.join();
            }

            for (Map.Entry<String, BackgroundIndexer> entry : indexers.entrySet()) {
                final BackgroundIndexer indexer = entry.getValue();
                indexer.setAssertNoFailuresOnStop(false);
                indexer.stop();

                final String indexName = entry.getKey();
                docsPerIndex.computeIfPresent(indexName, (key, value) -> value + indexer.totalIndexedDocs());

                final Throwable[] failures = indexer.getFailures();
                if (failures != null) {
                    for (Throwable failure : failures) {
                        assertException(failure, indexName);
                    }
                }
            }

            for (String index : indices) {
                if (acknowledgedCloses.contains(index)) {
                    assertIndexIsClosed(index);
                } else {
                    assertIndexIsOpened(index);
                }
            }

            targetTransportService.clearAllRules();

            // If a shard recovery has been interrupted, we expect its index to be closed
            interruptedRecoveries.forEach(CloseIndexIT::assertIndexIsClosed);

            assertThat("Consider that the test failed if no indices were successfully closed", acknowledgedCloses.size(), greaterThan(0));
            assertAcked(client().admin().indices().prepareOpen("index-*"));
            ensureGreen(indices);

            for (String index : acknowledgedCloses) {
                long docsCount = client().prepareSearch(index).setSize(0).setTrackTotalHits(true).get().getHits().getTotalHits().value;
                assertEquals("Expected " + docsPerIndex.get(index) + " docs in index " + index + " but got " + docsCount
                    + " (close acknowledged=" + acknowledgedCloses.contains(index) + ")", (long) docsPerIndex.get(index), docsCount);
            }
        } finally {
            assertAcked(client().admin().cluster().prepareUpdateSettings()
                .setTransientSettings(Settings.builder()
                    .putNull(EnableAllocationDecider.CLUSTER_ROUTING_REBALANCE_ENABLE_SETTING.getKey())));
        }
    }
}
