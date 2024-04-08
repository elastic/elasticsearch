/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.indices.state;

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
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.recovery.PeerRecoverySourceService;
import org.elasticsearch.indices.recovery.StartRecoveryRequest;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.BackgroundIndexer;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.test.transport.StubbableTransport;

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
import static org.elasticsearch.core.Strings.format;
import static org.elasticsearch.indices.state.CloseIndexIT.assertException;
import static org.elasticsearch.indices.state.CloseIndexIT.assertIndexIsClosed;
import static org.elasticsearch.indices.state.CloseIndexIT.assertIndexIsOpened;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;

@ESIntegTestCase.ClusterScope(minNumDataNodes = 2)
public class CloseWhileRelocatingShardsIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return singletonList(MockTransportService.TestPlugin.class);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        final int maxRecoveries = Integer.MAX_VALUE;
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
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
                case 0 -> {
                    logger.debug("creating empty index {}", indexName);
                    createIndex(indexName);
                }
                case 1 -> {
                    nbDocs = scaledRandomIntBetween(1, 100);
                    logger.debug("creating index {} with {} documents", indexName, nbDocs);
                    createIndex(indexName);
                    indexRandom(
                        randomBoolean(),
                        IntStream.range(0, nbDocs).mapToObj(n -> prepareIndex(indexName).setSource("num", n)).toList()
                    );
                }
                default -> {
                    logger.debug("creating index {} with background indexing", indexName);
                    final BackgroundIndexer indexer = new BackgroundIndexer(indexName, client(), -1, 1);
                    indexers.put(indexName, indexer);
                    indexer.setFailureAssertion(t -> assertException(t, indexName));
                    waitForDocs(1, indexer);
                }
            }
            docsPerIndex.put(indexName, (long) nbDocs);
            indices[i] = indexName;
        }

        ensureGreen(TimeValue.timeValueSeconds(60L), indices);
        updateClusterSettings(
            Settings.builder().put(EnableAllocationDecider.CLUSTER_ROUTING_REBALANCE_ENABLE_SETTING.getKey(), Rebalance.NONE.toString())
        );

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
            final Set<ShardId> blockedShards = commands.commands()
                .stream()
                .map(c -> (MoveAllocationCommand) c)
                .map(c -> new ShardId(clusterService.state().metadata().index(c.index()).getIndex(), c.shardId()))
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
                            logger.warn(() -> format("exception when releasing recovery of shard %s", startRecoveryRequest.shardId()), e);
                            interruptedRecoveries.add(startRecoveryRequest.shardId().getIndexName());
                            Thread.currentThread().interrupt();
                            return;
                        }
                    }
                }
                connection.sendRequest(requestId, action, request, options);
            };

            final var targetTransportService = MockTransportService.getInstance(targetNode);

            for (DiscoveryNode node : state.getNodes()) {
                if (node.canContainData() && node.getName().equals(targetNode) == false) {
                    targetTransportService.addSendBehavior(MockTransportService.getInstance(node.getName()), sendBehavior);
                }
            }

            assertAcked(clusterAdmin().reroute(new ClusterRerouteRequest().commands(commands)).get());

            // start index closing threads
            final List<Thread> threads = new ArrayList<>();
            for (final String indexToClose : indices) {
                final Thread thread = new Thread(() -> {
                    try {
                        safeAwait(latch);
                    } finally {
                        release.countDown();
                    }
                    // Closing is not always acknowledged when shards are relocating: this is the case when the target shard is initializing
                    // or is catching up operations. In these cases the TransportVerifyShardBeforeCloseAction will detect that the global
                    // and max sequence number don't match and will not ack the close.
                    AcknowledgedResponse closeResponse = indicesAdmin().prepareClose(indexToClose).get();
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

            // stop indexers first without waiting for stop to not redundantly index on some while waiting for another one to stop
            for (BackgroundIndexer indexer : indexers.values()) {
                indexer.stop();
            }
            for (Map.Entry<String, BackgroundIndexer> entry : indexers.entrySet()) {
                final BackgroundIndexer indexer = entry.getValue();
                indexer.awaitStopped();
                final String indexName = entry.getKey();
                docsPerIndex.computeIfPresent(indexName, (key, value) -> value + indexer.totalIndexedDocs());
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
            assertAcked(indicesAdmin().prepareOpen("index-*"));
            ensureGreen(indices);

            for (String index : acknowledgedCloses) {
                assertResponse(prepareSearch(index).setSize(0).setTrackTotalHits(true), response -> {
                    long docsCount = response.getHits().getTotalHits().value;
                    assertEquals(
                        "Expected "
                            + docsPerIndex.get(index)
                            + " docs in index "
                            + index
                            + " but got "
                            + docsCount
                            + " (close acknowledged="
                            + acknowledgedCloses.contains(index)
                            + ")",
                        (long) docsPerIndex.get(index),
                        docsCount
                    );
                });
            }
        } finally {
            updateClusterSettings(Settings.builder().putNull(EnableAllocationDecider.CLUSTER_ROUTING_REBALANCE_ENABLE_SETTING.getKey()));
        }
    }
}
