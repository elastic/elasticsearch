/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.allocation;

import org.elasticsearch.action.admin.cluster.reroute.ClusterRerouteUtils;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.metadata.IndexReshardingState;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.RoutingNodes;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.IndexBalanceConstraintSettings;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.routing.allocation.allocator.BalancedShardsAllocator;
import org.elasticsearch.cluster.routing.allocation.allocator.BalancerSettings;
import org.elasticsearch.cluster.routing.allocation.allocator.BalancingWeights;
import org.elasticsearch.cluster.routing.allocation.allocator.BalancingWeightsFactory;
import org.elasticsearch.cluster.routing.allocation.allocator.GlobalBalancingWeightsFactory;
import org.elasticsearch.cluster.routing.allocation.allocator.NodeSorters;
import org.elasticsearch.cluster.routing.allocation.allocator.WeightFunction;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.Decision;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.stateless.AbstractStatelessPluginIntegTestCase;
import org.elasticsearch.xpack.stateless.TestUtils;
import org.elasticsearch.xpack.stateless.reshard.ReshardIndexRequest;
import org.elasticsearch.xpack.stateless.reshard.SplitSourceService;
import org.elasticsearch.xpack.stateless.reshard.TransportReshardAction;
import org.elasticsearch.xpack.stateless.reshard.TransportReshardSplitAction;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Predicate;
import java.util.stream.Stream;

import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_REPLICAS;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_SHARDS;
import static org.elasticsearch.test.ClusterServiceUtils.addTemporaryStateListener;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0)
public class StatelessIndexBalanceAllocationDeciderIT extends AbstractStatelessPluginIntegTestCase {

    private static final int HEAVY_WEIGHT = 1000;
    private static final int LIGHT_WEIGHT = 1;
    private TestHarness testHarness;
    static final TestWeightFunction testWeightFunction = new TestWeightFunction();
    static final BalancerSettings balancerSettings = new BalancerSettings(Settings.builder().build());
    static final GlobalBalancingWeightsFactory globalBalancingWeightsFactory = new GlobalBalancingWeightsFactory(balancerSettings);
    static final BalancingWeights balancingWeights = globalBalancingWeightsFactory.create();
    static final Set<String> lightWeightedNodes = new HashSet<>();

    public void testIndexBalanceAllocationDecider() {
        setUpThreeHealthyIndexNodesAndThreeHealthSearchNodes();

        // Disable the index balanced allocation decider.
        updateClusterSettings(Settings.builder().put(IndexBalanceConstraintSettings.INDEX_BALANCE_DECIDER_ENABLED_SETTING.getKey(), false));
        final var clusterSettings = internalCluster().getCurrentMasterNodeInstance(ClusterSettings.class);
        assertFalse(clusterSettings.get(IndexBalanceConstraintSettings.INDEX_BALANCE_DECIDER_ENABLED_SETTING));

        // Override weight function to favour the first node
        // Disable balancer.balance() using a plugin provided decider with canRebalance() always returning false.
        lightWeightedNodes.add(testHarness.firstIndexNode.getId());
        lightWeightedNodes.add(testHarness.firstSearchNode.getId());

        int randomNumberOfShards = randomIntBetween(10, 20);
        String firstNodeHeavilyImbalancedIndex = randomIdentifier();

        // Keep number of replica to 1 so that SameShardAllocationDecider cannot help with balancing.
        createIndex(
            firstNodeHeavilyImbalancedIndex,
            Settings.builder().put(SETTING_NUMBER_OF_SHARDS, randomNumberOfShards).put(SETTING_NUMBER_OF_REPLICAS, 1).build()
        );
        ensureGreen(firstNodeHeavilyImbalancedIndex);

        // This demonstrates BalancedShardAllocator's built-in node/index weightFunction shard balancing
        // and balancer.balance() have been disabled resulting in all primary shards allocated on index node 1
        // and all replicas allocated to search node 1.
        safeAwait(
            ClusterServiceUtils.addMasterTemporaryStateListener(
                getAllocationStateListener(
                    firstNodeHeavilyImbalancedIndex,
                    Map.of(
                        testHarness.firstIndexNode.getId(),
                        randomNumberOfShards,
                        testHarness.secondIndexNode.getId(),
                        0,
                        testHarness.thirdIndexNode.getId(),
                        0,
                        testHarness.firstSearchNode.getId(),
                        randomNumberOfShards,
                        testHarness.secondSearchNode.getId(),
                        0,
                        testHarness.thirdSearchNode.getId(),
                        0
                    )
                )
            )
        );

        // All settings stay the same.
        // Enable the index balanced allocation decider.
        updateClusterSettings(Settings.builder().put(IndexBalanceConstraintSettings.INDEX_BALANCE_DECIDER_ENABLED_SETTING.getKey(), true));

        randomNumberOfShards = randomIntBetween(10, 20);
        String perfectlyBalancedSecondIndex = randomIdentifier();

        // The decider computes shards threshold as follows:
        // threshold = (totalShards + eligibleNodes.size() - 1 + indexBalanceConstraintSettings.getExcessShards()) / eligibleNodes.size();
        // In this test, there are 3 eligible nodes and 0 excess shards allowed.
        // Therefore, the upper threshold is (randomNumberOfShards + 2 ) / 3
        int perfectlyBalanced = (randomNumberOfShards + 2) / 3;

        createIndex(
            perfectlyBalancedSecondIndex,
            Settings.builder().put(SETTING_NUMBER_OF_SHARDS, randomNumberOfShards).put(SETTING_NUMBER_OF_REPLICAS, 1).build()
        );
        ensureGreen(perfectlyBalancedSecondIndex);

        safeAwait(
            ClusterServiceUtils.addMasterTemporaryStateListener(
                getAllocationStateListener(
                    perfectlyBalancedSecondIndex,
                    Map.of(
                        testHarness.firstIndexNode.getId(),
                        perfectlyBalanced,
                        testHarness.secondIndexNode.getId(),
                        perfectlyBalanced,
                        testHarness.thirdIndexNode.getId(),
                        perfectlyBalanced,
                        testHarness.firstSearchNode.getId(),
                        perfectlyBalanced,
                        testHarness.secondSearchNode.getId(),
                        perfectlyBalanced,
                        testHarness.thirdSearchNode.getId(),
                        perfectlyBalanced
                    )
                )
            )
        );

    }

    public void testShardsFromSameIndexAreNeverCoLocatedWhenIndexBalanceDeciderEnabled() {
        setUpThreeHealthyIndexNodesAndThreeHealthSearchNodes();

        // Weight function should push shards to a single index/search node
        lightWeightedNodes.add(randomFrom(testHarness.firstIndexNode, testHarness.secondIndexNode, testHarness.thirdIndexNode).getId());
        lightWeightedNodes.add(randomFrom(testHarness.firstSearchNode, testHarness.secondSearchNode, testHarness.thirdSearchNode).getId());

        // When the decider is disabled, we should allocate the 3 shards to the same node. We check this to ensure that it's
        // the decider that's causing the spread, and the test is still valid
        updateClusterSettings(Settings.builder().put(IndexBalanceConstraintSettings.INDEX_BALANCE_DECIDER_ENABLED_SETTING.getKey(), false));
        createIndexAndEnsureAllocationMatches(allocatedPrimaries -> {
            final long distinctNodes = allocatedPrimaries.stream().map(ShardRouting::currentNodeId).distinct().count();
            // Either no shards are allocated, or they're all allocated to a single node
            return distinctNodes == 0 || distinctNodes == 1;
        });

        // When the decider is enabled, we should allocate the 3 shards to different nodes
        updateClusterSettings(Settings.builder().put(IndexBalanceConstraintSettings.INDEX_BALANCE_DECIDER_ENABLED_SETTING.getKey(), true));
        createIndexAndEnsureAllocationMatches(
            allocatedPrimaries -> allocatedPrimaries.stream().map(ShardRouting::currentNodeId).distinct().count() == allocatedPrimaries
                .size()
        );
    }

    /**
     * Create a 3-shard index and ensure that in every cluster state, a predicate is met for the allocated shards. Returns
     * when the 3 shards are all allocated.
     *
     * @param allocatedShardsPredicate A predicate used to check the state of the allocation, applied to primaries and replicas separately
     */
    private void createIndexAndEnsureAllocationMatches(Predicate<List<ShardRouting>> allocatedShardsPredicate) {
        final String indexName = randomIndexName();
        final var desiredStateListener = addTemporaryStateListener(
            clusterService(),
            state -> assertRoutingMatchesPredicate(state, indexName, allocatedShardsPredicate, true)
                && assertRoutingMatchesPredicate(state, indexName, allocatedShardsPredicate, false),
            TimeValue.THIRTY_SECONDS
        );

        createIndex(indexName, 3, 1);
        safeAwait(desiredStateListener, TimeValue.THIRTY_SECONDS);
    }

    /**
     * Assert that the current shard allocation matches the predicate
     *
     * @param state The cluster state
     * @param indexName The index to check
     * @param allocatedShardsPredicate The predicate to check the shard allocation
     * @param primary Whether to check the primary or replica shard allocation
     * @return True if the predicate is met and all shards are allocated, false otherwise
     */
    private boolean assertRoutingMatchesPredicate(
        ClusterState state,
        String indexName,
        Predicate<List<ShardRouting>> allocatedShardsPredicate,
        boolean primary
    ) {
        final var indexRoutingTables = state.globalRoutingTable().routingTable(ProjectId.DEFAULT);
        final var optionalIndexRoutingTable = indexRoutingTables.indicesRouting()
            .values()
            .stream()
            .filter(ir -> ir.getIndex().getName().equals(indexName))
            .findFirst();
        return optionalIndexRoutingTable.map(indexRoutingTable -> {
            // Index was created, assert there are 3 primaries and they are never co-located
            final var allocatedShards = indexRoutingTable.allShards()
                .map(
                    indexShardRoutingTable -> primary
                        ? indexShardRoutingTable.primaryShard()
                        : indexShardRoutingTable.replicaShards().getFirst()
                )
                .filter(ShardRouting::assignedToNode)
                .toList();
            assertTrue("Unexpected shard allocation:\n" + indexRoutingTable.prettyPrint(), allocatedShardsPredicate.test(allocatedShards));

            // All shards are active
            return indexRoutingTable.allShardsActive();
        }).orElse(false);
    }

    private Predicate<ClusterState> getAllocationStateListener(String indexName, Map<String, Integer> nodesToShards) {
        return clusterState -> {
            var indexRoutingTable = clusterState.routingTable(ProjectId.DEFAULT).index(indexName);
            if (indexRoutingTable == null) {
                return false;
            }
            return checkShardAssignment(clusterState.getRoutingNodes(), indexRoutingTable.getIndex(), nodesToShards);
        };
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        var plugins = new ArrayList<>(super.nodePlugins());
        plugins.remove(TestUtils.StatelessPluginWithTrialLicense.class);
        plugins.add(TestStatelessPlugin.class);
        return plugins;
    }

    @Override
    protected boolean addMockFsRepository() {
        // Use the FS repository because it supports the blob copy that reshard splits rely on
        return false;
    }

    public static class TestWeightFunction extends WeightFunction {
        public TestWeightFunction() {
            super(1, 1, 1, 1);
        }

        @Override
        public float calculateNodeWeightWithIndex(
            BalancedShardsAllocator.Balancer balancer,
            BalancedShardsAllocator.ModelNode node,
            BalancedShardsAllocator.ProjectIndex index
        ) {
            // Deliberately heavily favour node 1 to receive shards
            return lightWeightedNodes.contains(node.getNodeId()) ? LIGHT_WEIGHT : HEAVY_WEIGHT;
        }
    }

    public static class TestStatelessPlugin extends TestUtils.StatelessPluginWithTrialLicense {
        public TestStatelessPlugin(Settings settings) {
            super(settings);
        }

        @Override
        public BalancingWeightsFactory getBalancingWeightsFactory(BalancerSettings balancerSettings, ClusterSettings clusterSettings) {
            return () -> new BalancingWeights() {

                @Override
                public WeightFunction weightFunctionForShard(ShardRouting shard) {
                    return testWeightFunction;
                }

                @Override
                public WeightFunction weightFunctionForNode(RoutingNode node) {
                    return testWeightFunction;
                }

                @Override
                public NodeSorters createNodeSorters(
                    BalancedShardsAllocator.ModelNode[] modelNodes,
                    BalancedShardsAllocator.Balancer balancer
                ) {
                    return balancingWeights.createNodeSorters(modelNodes, balancer);
                }

                @Override
                public boolean diskUsageIgnored() {
                    return balancingWeights.diskUsageIgnored();
                }
            };
        }

        @Override
        public Collection<AllocationDecider> createAllocationDeciders(Settings settings, ClusterSettings clusterSettings) {
            return CollectionUtils.appendToCopy(super.createAllocationDeciders(settings, clusterSettings), new AllocationDecider() {
                @Override
                public Decision canRebalance(RoutingAllocation allocation) {
                    return Decision.NO;
                }
            });
        }
    }

    @TestLogging(
        value = "org.elasticsearch.cluster.routing.allocation.allocator.BalancedShardsAllocator.not_preferred:DEBUG",
        reason = "So we can see why the shards are being moved"
    )
    public void testShardsAreMovedToImproveIndexBalanceOnClusterScaleUp() {
        // Enable the index balance decider
        Settings settings = Settings.builder()
            .put(IndexBalanceConstraintSettings.INDEX_BALANCE_DECIDER_ENABLED_SETTING.getKey(), true)
            .build();

        final var indexNodes = List.of(startMasterAndIndexNode(settings), startMasterAndIndexNode(settings));
        final var searchNodes = startSearchNodes(2);

        final String indexName = randomIdentifier();
        final int numberOfShards = randomIntBetween(10, 20);

        createIndex(indexName, numberOfShards, 1);
        ensureGreen(indexName);

        {
            // The nodes should both have a roughly even spread of the index shards
            final int thresholdForTwoNodes = Math.ceilDiv(numberOfShards, 2);
            final var state = internalCluster().clusterService().state();
            final var index = state.routingTable(ProjectId.DEFAULT).index(indexName).getIndex();
            final var routingNodes = state.getRoutingNodes();
            indexNodes.stream()
                .map(ESIntegTestCase::getNodeId)
                .forEach(
                    nodeId -> assertThat(
                        routingNodes.node(nodeId).numberOfOwningShardsForIndex(index),
                        lessThanOrEqualTo(thresholdForTwoNodes)
                    )
                );
        }

        // Start more index nodes (should trigger a re-route)
        final var expandedIndexNodes = Stream.concat(indexNodes.stream(), startIndexNodes(randomIntBetween(1, 4), settings).stream())
            .toList();
        // The nodes should eventually all have a roughly even spread of the index shards
        assertIndexBalanceIsRestored(numberOfShards, expandedIndexNodes, indexName);

        // Start more search nodes (should trigger a re-route)
        final var expandedSearchNodes = Stream.concat(searchNodes.stream(), startSearchNodes(randomIntBetween(1, 4), settings).stream())
            .toList();
        // The nodes should eventually all have a roughly even spread of the index shards
        assertIndexBalanceIsRestored(numberOfShards, expandedSearchNodes, indexName);
    }

    public void testSplitSourcesAreNotMovedForIndexBalanceOnClusterScaleUpDuringReshard() throws Exception {
        final var settings = Settings.builder()
            .put(IndexBalanceConstraintSettings.INDEX_BALANCE_DECIDER_ENABLED_SETTING.getKey(), true)
            // The unowned documents grace period would otherwise delay the completion of the reshard by minutes
            .put(SplitSourceService.RESHARD_SPLIT_DELETE_UNOWNED_GRACE_PERIOD.getKey(), TimeValue.ZERO)
            .build();
        startMasterOnlyNode(settings);
        final var indexNodes = startIndexNodes(2, settings);

        // Each node gets half of the sources and, once the split starts, half of the targets.
        final int sourceShards = 4;
        final var indexName = randomIndexName();
        createIndex(indexName, sourceShards, 0);
        indexDocs(indexName, between(1, 50));
        flush(indexName);
        ensureGreen(indexName);

        // Stall the start split requests on the split sources, so that the split targets stay in CLONE
        final var stallSplits = new AtomicBoolean(true);
        final var stalledSplits = new ConcurrentLinkedQueue<Runnable>();
        final var allSplitsStalled = new CountDownLatch(sourceShards);
        for (final var indexNode : indexNodes) {
            final var executor = internalCluster().getInstance(ThreadPool.class, indexNode).generic();
            MockTransportService.getInstance(indexNode)
                .addRequestHandlingBehavior(TransportReshardSplitAction.START_SPLIT_ACTION_NAME, (handler, request, channel, task) -> {
                    final Runnable runSplit = () -> executor.execute(() -> {
                        try {
                            handler.messageReceived(request, channel, task);
                        } catch (Exception e) {
                            channel.sendResponse(e);
                        }
                    });
                    synchronized (stalledSplits) {
                        if (stallSplits.get()) {
                            stalledSplits.add(runSplit);
                            allSplitsStalled.countDown();
                            return;
                        }
                    }
                    runSplit.run();
                });
        }

        // Record any split source that starts relocating before all its targets have reached HANDOFF
        final Set<ShardId> movedSplitSources = ConcurrentCollections.newConcurrentSet();
        final ClusterStateListener splitSourceMovesRecorder = event -> {
            final var indexMetadata = event.state().metadata().getProject(ProjectId.DEFAULT).index(indexName);
            if (indexMetadata == null || indexMetadata.getReshardingMetadata() == null) {
                return;
            }
            final var split = indexMetadata.getReshardingMetadata().getSplit();
            final var indexRoutingTable = event.state().routingTable(ProjectId.DEFAULT).index(indexName);
            for (int shard = 0; shard < sourceShards; shard++) {
                final var primary = indexRoutingTable.shard(shard).primaryShard();
                if (primary.relocating()
                    && split.allTargetStatesAtLeast(shard, IndexReshardingState.Split.TargetShardState.HANDOFF) == false) {
                    movedSplitSources.add(primary.shardId());
                }
            }
        };
        final var masterClusterService = internalCluster().getCurrentMasterNodeInstance(ClusterService.class);
        masterClusterService.addListener(splitSourceMovesRecorder);
        try {
            safeGet(client().execute(TransportReshardAction.TYPE, new ReshardIndexRequest(indexName)));
            safeAwait(allSplitsStalled);

            // Scale up the index tier, which halves the threshold, then reroute
            final var expandedIndexNodes = Stream.concat(indexNodes.stream(), startIndexNodes(2, settings).stream()).toList();
            ensureStableCluster(expandedIndexNodes.size() + 1);
            ClusterRerouteUtils.reroute(client());

            // The original nodes hold all the sources and targets, so at least one of them is over the threshold of the expanded tier
            final var stateAfterScaling = masterClusterService.state();
            final var index = stateAfterScaling.routingTable(ProjectId.DEFAULT).index(indexName).getIndex();
            final int expandedThreshold = Math.ceilDiv(2 * sourceShards, expandedIndexNodes.size());
            assertTrue(
                "at least one original index node should be over the index balance threshold of the expanded tier",
                indexNodes.stream()
                    .anyMatch(
                        indexNode -> stateAfterScaling.getRoutingNodes()
                            .node(getNodeId(indexNode))
                            .numberOfOwningShardsForIndex(index) > expandedThreshold
                    )
            );
            assertThat("split sources must not be moved before their targets reach HANDOFF", movedSplitSources, empty());

            // Release the splits and let the reshard complete
            synchronized (stalledSplits) {
                stallSplits.set(false);
            }
            stalledSplits.forEach(Runnable::run);
            awaitClusterState(state -> state.projectState().metadata().index(indexName).getReshardingMetadata() == null);
            ensureGreen(indexName);

            assertThat("split sources must not be moved before their targets reach HANDOFF", movedSplitSources, empty());

            // The reroute that follows the targets' shard-started balances the index across the expanded index tier
            assertIndexBalanceIsRestored(2 * sourceShards, expandedIndexNodes, indexName);
        } finally {
            masterClusterService.removeListener(splitSourceMovesRecorder);
        }
    }

    private void assertIndexBalanceIsRestored(int numberOfShards, List<String> expandedNodeSet, String indexName) {
        // The nodes should eventually all have a roughly even spread of the index shards
        final int shardCountThreshold = Math.ceilDiv(numberOfShards, expandedNodeSet.size());
        awaitClusterState(state -> {
            final var routingNodes = state.getRoutingNodes();
            final var index = state.routingTable(ProjectId.DEFAULT).index(indexName).getIndex();
            logger.info("--> Checking cluster state for index {}", index);
            return expandedNodeSet.stream().map(ESIntegTestCase::getNodeId).allMatch(nodeId -> {
                int shardsOnNode = routingNodes.node(nodeId).numberOfOwningShardsForIndex(index);
                logger.info(
                    "--> Node {} has {} shards (threshold={}, numberOfShards={}, eligibleNodes={})",
                    nodeId,
                    shardsOnNode,
                    shardCountThreshold,
                    numberOfShards,
                    expandedNodeSet.size()
                );
                return shardsOnNode <= shardCountThreshold;
            });
        });
    }

    private void setUpThreeHealthyIndexNodesAndThreeHealthSearchNodes() {
        Settings settings = Settings.builder().build();
        startMasterOnlyNode(settings);
        final var indexNodes = startIndexNodes(3, settings);
        final var searchNodes = startSearchNodes(3);

        final String firstIndexNodeName = indexNodes.get(0);
        final String secondIndexNodeName = indexNodes.get(1);
        final String thirdIndexNodeName = indexNodes.get(2);
        final String firstSearchNodeName = searchNodes.get(0);
        final String secondSearchNodeName = searchNodes.get(1);
        final String thirdSearchNodeName = searchNodes.get(2);

        ensureStableCluster(7);

        final DiscoveryNode firstIndexNode = internalCluster().getInstance(TransportService.class, firstIndexNodeName).getLocalNode();
        final DiscoveryNode secondIndexNode = internalCluster().getInstance(TransportService.class, secondIndexNodeName).getLocalNode();
        final DiscoveryNode thirdIndexNode = internalCluster().getInstance(TransportService.class, thirdIndexNodeName).getLocalNode();
        final DiscoveryNode firstSearchNode = internalCluster().getInstance(TransportService.class, firstSearchNodeName).getLocalNode();
        final DiscoveryNode secondSearchNode = internalCluster().getInstance(TransportService.class, secondSearchNodeName).getLocalNode();
        final DiscoveryNode thirdSearchNode = internalCluster().getInstance(TransportService.class, thirdSearchNodeName).getLocalNode();

        testHarness = new TestHarness(firstIndexNode, secondIndexNode, thirdIndexNode, firstSearchNode, secondSearchNode, thirdSearchNode);
    }

    private boolean checkShardAssignment(RoutingNodes routingNodes, Index index, Map<String, Integer> nodesToShards) {
        for (String nodeId : nodesToShards.keySet()) {
            var numberOfShards = routingNodes.node(nodeId).numberOfOwningShardsForIndex(index);
            if (numberOfShards > nodesToShards.get(nodeId)) {
                return false;
            }
        }
        return true;
    }

    record TestHarness(
        DiscoveryNode firstIndexNode,
        DiscoveryNode secondIndexNode,
        DiscoveryNode thirdIndexNode,
        DiscoveryNode firstSearchNode,
        DiscoveryNode secondSearchNode,
        DiscoveryNode thirdSearchNode
    ) {}

}
