/*
 * ELASTICSEARCH CONFIDENTIAL
 * __________________
 *
 * Copyright Elasticsearch B.V. All rights reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Elasticsearch B.V. and its suppliers, if any.
 * The intellectual and technical concepts contained herein
 * are proprietary to Elasticsearch B.V. and its suppliers and
 * may be covered by U.S. and Foreign Patents, patents in
 * process, and are protected by trade secret or copyright
 * law.  Dissemination of this information or reproduction of
 * this material is strictly forbidden unless prior written
 * permission is obtained from Elasticsearch B.V.
 */

package co.elastic.elasticsearch.stateless.allocation;

import co.elastic.elasticsearch.stateless.AbstractStatelessIntegTestCase;
import co.elastic.elasticsearch.stateless.Stateless;

import org.elasticsearch.cluster.ClusterState;
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
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.index.Index;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.transport.TransportService;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;

import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_REPLICAS;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_SHARDS;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0)
public class StatelessIndexBalanceAllocationDeciderIT extends AbstractStatelessIntegTestCase {

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

        // Index balanced allocation decider disabled.
        final var clusterSettings = internalCluster().getCurrentMasterNodeInstance(ClusterSettings.class);
        assertFalse(clusterSettings.get(IndexBalanceConstraintSettings.INDEX_BALANCE_DECIDER_ENABLED_SETTING));

        // Override weight function to favour the first node
        // Disable balancer.balance() using a plugin provided decider with canRebalance() always returning false.
        // IndexBalanceAllocationDecider is disabled by default.
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
        plugins.remove(Stateless.class);
        plugins.add(StatelessIndexBalanceAllocationDeciderIT.TestStateless.class);
        return plugins;
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
            balancer.getAllocation().setDebugMode(RoutingAllocation.DebugMode.ON);
            // Deliberately heavily favour node 1 to receive shards
            return lightWeightedNodes.contains(node.getNodeId()) ? LIGHT_WEIGHT : HEAVY_WEIGHT;
        }
    }

    public static class TestStateless extends Stateless {
        public TestStateless(Settings settings) {
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
                public Decision canRebalance(ShardRouting shardRouting, RoutingAllocation allocation) {
                    return Decision.NO;
                }
            });
        }
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
