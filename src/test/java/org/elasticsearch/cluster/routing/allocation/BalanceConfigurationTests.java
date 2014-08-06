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

package org.elasticsearch.cluster.routing.allocation;

import com.carrotsearch.hppc.cursors.ObjectCursor;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSetMultimap;
import org.elasticsearch.cluster.*;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.*;
import org.elasticsearch.cluster.routing.allocation.allocator.BalancedShardsAllocator;
import org.elasticsearch.cluster.routing.allocation.allocator.ShardsAllocator;
import org.elasticsearch.cluster.routing.allocation.allocator.ShardsAllocators;
import org.elasticsearch.cluster.routing.allocation.decider.ClusterRebalanceAllocationDecider;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.gateway.none.NoneGatewayAllocator;
import org.elasticsearch.node.settings.NodeSettingsService;
import org.elasticsearch.test.ElasticsearchAllocationTestCase;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Locale;

import static org.elasticsearch.cluster.routing.ShardRoutingState.INITIALIZING;
import static org.elasticsearch.cluster.routing.ShardRoutingState.STARTED;
import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.hamcrest.Matchers.*;

public class BalanceConfigurationTests extends ElasticsearchAllocationTestCase {
    private final ESLogger logger = Loggers.getLogger(BalanceConfigurationTests.class);
    private int numberOfNodes;
    private int numberOfIndices;
    private int[] numberOfShards;
    private int numberOfReplicas;
    private int totalShards;
    private long smallShardLimit;

    private ClusterInfo clusterInfo;

    /**
     * The cluster settles if only total shards per node are used in balancing
     * and the total shards are reasonably balanced.
     */
    @Test
    public void testShardBalance() {
        initRandom();
        clusterBalancesWithTheseSettings(1, 0, 0, 0, new ShardBalanceAssertion());
    }

    /**
     * The cluster settles if only shards per index per node are used in
     * balancing and the shards per index are reasonably balanced.
     */
    @Test
    public void testIndexBalance() {
        initRandom();
        clusterBalancesWithTheseSettings(0, 1, 0, 0, new IndexBalanceAssertion());
    }

    /**
     * The cluster settles if only total primaries per node are used in
     * balancing and the primaries are reasonably balanced.
     */
    @Test
    public void testPrimaryBalance() {
        initRandom();
        clusterBalancesWithTheseSettings(0, 0, 1, 0, new PrimaryBalanceAssertion());
    }

    /**
     * The cluster settles if only similarly sized shards are used in balancing
     * and the sizes are reasonably balanced.
     */
    @Test
    public void testSizeBalance() {
        // If all shards are the same size (0 in this case) then size balance
        // devolves into shard balance.
        initRandom();
        buildZeroSizedClusterInfo();
        clusterBalancesWithTheseSettings(0, 0, 0, 1, new ShardBalanceAssertion());

//        // If all no indexes share the same shard size then size balance
//        // devolves into index balance
//        buildEvenlyDistributedClusterInfo();
//        clusterBalancesWithTheseSettings(0, 0, 0, 1, new IndexBalanceAssertion());
//      ------ Removed because this only works with a small number of indexes -
//             there simply aren't enough cohorts for it to work for a ~100 indexes.

        // But with a random configuration this will balance on average shard
        // size
        initRandom();
        buildRandomlyDistributedClusterInfo();
        clusterBalancesWithTheseSettings(0, 0, 0, 1, new SizeBalanceAssertion());
    }

    @Test
    public void testRandomTotalTerminates() {
        initRandom();
        buildRandomlyDistributedClusterInfo();
        float total, shardBalance, indexBalance, primaryBalance, sizeBalance;
        do {
            shardBalance = getRandom().nextFloat();
            indexBalance = getRandom().nextFloat();
            primaryBalance = getRandom().nextFloat();
            sizeBalance = getRandom().nextFloat();
            total = shardBalance + indexBalance + primaryBalance + sizeBalance;
        } while (total == 0);
        logger.info("rebalancing with thetas=[{}, {}, {}, {}]", shardBalance, indexBalance, primaryBalance, sizeBalance);
        clusterBalancesWithTheseSettings(shardBalance, indexBalance, primaryBalance, sizeBalance, new NoopBalanceAssertion());
    }

    private void clusterBalancesWithTheseSettings(float shardBalance, float indexBalance, float primaryBalance, float sizeBalance, BalanceAssertion assertion) {
        final float balanceThreshold = 1.0f;

        ImmutableSettings.Builder settings = settingsBuilder();
        settings.put(ClusterRebalanceAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ALLOW_REBALANCE, ClusterRebalanceAllocationDecider.ClusterRebalanceType.ALWAYS.toString());
        settings.put(BalancedShardsAllocator.SETTING_INDEX_BALANCE_FACTOR, indexBalance);
        settings.put(BalancedShardsAllocator.SETTING_SHARD_BALANCE_FACTOR, shardBalance);
        settings.put(BalancedShardsAllocator.SETTING_PRIMARY_BALANCE_FACTOR, primaryBalance);
        settings.put(BalancedShardsAllocator.SETTING_SHARD_SIZE_BALANCE_FACTOR, sizeBalance);
        settings.put(BalancedShardsAllocator.SETTING_THRESHOLD, balanceThreshold);
        settings.put(InternalClusterInfoService.SMALL_SHARD_LIMIT_NAME, smallShardLimit);

        AllocationService strategy = createAllocationService(settings.build(), getRandom(), new ClusterInfoService() {
            @Override
            public ClusterInfo getClusterInfo() {
                return clusterInfo;
            }
        });

        ClusterState clusterstate = initCluster(strategy);
        assertion.assertBalanced(clusterstate.getRoutingNodes(), balanceThreshold);

        clusterstate = addNode(clusterstate, strategy);
        assertion.assertBalanced(clusterstate.getRoutingNodes(), balanceThreshold);

        clusterstate = removeNodes(clusterstate, strategy);
        assertion.assertBalanced(clusterstate.getRoutingNodes(), balanceThreshold);
    }

    private void initRandom() {
        numberOfNodes = between(20, 60);
        numberOfIndices = between(1, 100);
        numberOfReplicas = between(1, 2);
        totalShards = 0;
        numberOfShards = new int[numberOfIndices];
        for (int i = 0; i < numberOfIndices; i++) {
            numberOfShards[i] = getRandom().nextBoolean() ? 1 : between(2, 10);
            totalShards += numberOfShards[i];
        }
        smallShardLimit = (long)(ByteSizeValue.parseBytesSizeValue("10KB").bytes() + 
                getRandom().nextDouble() * ByteSizeValue.parseBytesSizeValue("100MB").bytes());
    }

    private ClusterState initCluster(AllocationService strategy) {
        MetaData.Builder metaDataBuilder = MetaData.builder();
        RoutingTable.Builder routingTableBuilder = RoutingTable.builder();

        for (int i = 0; i < numberOfIndices; i++) {
            IndexMetaData.Builder index = IndexMetaData.builder("test" + i).numberOfShards(numberOfShards[i]).numberOfReplicas(numberOfReplicas);
            metaDataBuilder = metaDataBuilder.put(index);
        }

        MetaData metaData = metaDataBuilder.build();

        for (ObjectCursor<IndexMetaData> cursor : metaData.indices().values()) {
            routingTableBuilder.addAsNew(cursor.value);
        }

        RoutingTable routingTable = routingTableBuilder.build();

        logger.info("start {} nodes for {} indexes with {} total shards and {} replicas", numberOfNodes, numberOfIndices, totalShards, numberOfReplicas);
        DiscoveryNodes.Builder nodes = DiscoveryNodes.builder();
        for (int i = 0; i < numberOfNodes; i++) {
            nodes.put(newNode("node" + i));
        }
        ClusterState clusterState = ClusterState.builder(org.elasticsearch.cluster.ClusterName.DEFAULT).nodes(nodes).metaData(metaData).routingTable(routingTable).build();
        routingTable = strategy.reroute(clusterState).routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();
        RoutingNodes routingNodes = clusterState.routingNodes();

        logger.info("restart all the primary shards, replicas will start initializing");
        routingNodes = clusterState.routingNodes();
        routingTable = strategy.applyStartedShards(clusterState, routingNodes.shardsWithState(INITIALIZING)).routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();
        routingNodes = clusterState.routingNodes();

        logger.info("start the replica shards");
        routingNodes = clusterState.routingNodes();
        routingTable = strategy.applyStartedShards(clusterState, routingNodes.shardsWithState(INITIALIZING)).routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();
        routingNodes = clusterState.routingNodes();

        logger.info("complete rebalancing");
        clusterState = rebalance(strategy, routingTable, routingNodes, clusterState);

        return clusterState;
    }

    private ClusterState addNode(ClusterState clusterState, AllocationService strategy) {
        logger.info("now, start 1 more node, check that rebalancing will happen because we set it to always");
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder(clusterState.nodes())
                .put(newNode("node" + numberOfNodes)))
                .build();

        RoutingTable routingTable = strategy.reroute(clusterState).routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();
        RoutingNodes routingNodes = clusterState.routingNodes();

        // move initializing to started
        clusterState = rebalance(strategy, routingTable, routingNodes, clusterState);

        numberOfNodes++;
        return clusterState;
    }

    private ClusterState removeNodes(ClusterState clusterState, AllocationService strategy) {
        logger.info("Removing half the nodes (" + (numberOfNodes + 1) / 2 + ")");
        DiscoveryNodes.Builder nodes = DiscoveryNodes.builder(clusterState.nodes());

        for (int i = numberOfNodes / 2; i <= numberOfNodes; i++) {
            nodes.remove("node" + i);
        }
        numberOfNodes = numberOfNodes / 2;

        clusterState = ClusterState.builder(clusterState).nodes(nodes.build()).build();
        RoutingNodes routingNodes = clusterState.routingNodes();

        logger.info("start all the primary shards, replicas will start initializing");
        RoutingTable routingTable = strategy.applyStartedShards(clusterState, routingNodes.shardsWithState(INITIALIZING)).routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();
        routingNodes = clusterState.routingNodes();

        logger.info("start the replica shards");
        routingTable = strategy.applyStartedShards(clusterState, routingNodes.shardsWithState(INITIALIZING)).routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();
        routingNodes = clusterState.routingNodes();

        logger.info("rebalancing");
        routingTable = strategy.reroute(clusterState).routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();
        routingNodes = clusterState.routingNodes();

        logger.info("complete rebalancing");
        clusterState = rebalance(strategy, routingTable, routingNodes, clusterState);

        return clusterState;
    }

    private ClusterState rebalance(AllocationService strategy, RoutingTable routingTable, RoutingNodes routingNodes, ClusterState clusterState) {
        RoutingTable prev = routingTable;
        int steps = 0;
        while (true) {
            routingTable = strategy.applyStartedShards(clusterState, routingNodes.shardsWithState(INITIALIZING)).routingTable();
            clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();
            routingNodes = clusterState.routingNodes();
            if (routingTable == prev && allActive(routingTable)) {
                logger.info("done after {} steps", steps);
                return clusterState;
            }
            prev = routingTable;
            if (steps != 0 && steps % 10 == 0) {
                logger.info("completed {} steps", steps);
            }
            steps++;
        }
    }

    private boolean allActive(RoutingTable routingTable) {
        for (IndexRoutingTable index: routingTable) {
            for (IndexShardRoutingTable shard: index) {
                for (ShardRouting replica: shard) {
                    if (!replica.started()) {
                        logger.info("Not started");
                        return false;
                    }
                }
            }
        }
        return true;
    }

    private interface BalanceAssertion {
        void assertBalanced(RoutingNodes nodes, float threshold);
    }

    private class ShardBalanceAssertion implements BalanceAssertion {
        @Override
        public void assertBalanced(RoutingNodes nodes, float threshold) {
            final int numShards = totalShards * (numberOfReplicas + 1);
            final float avgNumShards = (float)numShards / (float)numberOfNodes;
            final int min = (int)Math.round(Math.floor(avgNumShards - threshold));
            final int max = (int)Math.round(Math.ceil(avgNumShards + threshold));

            for (RoutingNode node : nodes) {
                assertThat(node.shardsWithState(STARTED).size(),
                        both(greaterThanOrEqualTo(min)).and(lessThanOrEqualTo(max)));
            }
        }
    }

    private class IndexBalanceAssertion implements BalanceAssertion {
        @Override
        public void assertBalanced(RoutingNodes nodes, float threshold) {
            for (String index : nodes.getRoutingTable().indicesRouting().keySet()) {
                int numShards = numberOfShards[Integer.parseInt(index.substring("test".length()))] * (numberOfReplicas + 1);
                float avgNumShards = (float) (numShards) / (float) (numberOfNodes);
                int min = (int)Math.round(Math.floor(avgNumShards - threshold));
                int max = (int)Math.round(Math.ceil(avgNumShards + threshold));

                for (RoutingNode node : nodes) {
                    assertThat(node.shardsWithState(index, STARTED).size(),
                            both(greaterThanOrEqualTo(min)).and(lessThanOrEqualTo(max)));
                }
            }
        }
    }

    private class PrimaryBalanceAssertion implements BalanceAssertion {
        @Override
        public void assertBalanced(RoutingNodes nodes, float threshold) {
            final float avgNumShards = (float)totalShards / (float)numberOfNodes;
            final int min = (int)Math.round(Math.floor(avgNumShards - threshold));
            final int max = (int)Math.round(Math.ceil(avgNumShards + threshold));

            for (RoutingNode node : nodes) {
                int primaries = 0;
                for (ShardRouting shard : node.shardsWithState(STARTED)) {
                    primaries += shard.primary() ? 1 : 0;
                }
                assertThat(primaries, both(greaterThanOrEqualTo(min)).and(lessThanOrEqualTo(max)));
            }
        }
    }

    private class SizeBalanceAssertion implements BalanceAssertion {
        @Override
        public void assertBalanced(RoutingNodes nodes, float threshold) {
            for (int i = 0; i < numberOfIndices; i++) {
                String idx = "test" + i;
                long size = clusterInfo.getIndexToAverageShardSize().get(idx);
                int similarShards = 0;

                long minSize, maxSize;
                if (size < smallShardLimit) {
                    minSize = 0;
                    maxSize = smallShardLimit;
                } else {
                    double log = Math.log10(size);
                    minSize = (long)Math.pow(10, Math.floor(log));
                    maxSize = (long)Math.pow(10, Math.ceil(log));
                }
                for (int j = 0; j < numberOfIndices; j++) {
                    String currentIdx = "test" + j;
                    long currentSize = clusterInfo.getIndexToAverageShardSize().get(currentIdx);
                    if (minSize <= currentSize && currentSize < maxSize) {
                        similarShards += numberOfShards[j];
                    }
                }

                similarShards *= (numberOfReplicas + 1);
                final float avgNumShards = (float)similarShards / (float)numberOfNodes;
                final int min = (int)Math.round(Math.floor(avgNumShards - threshold));
                final int max = (int)Math.round(Math.ceil(avgNumShards + threshold));

                for (RoutingNode node : nodes) {
                    int count = 0;
                    for(MutableShardRouting shard: node) {
                        long currentSize = clusterInfo.getShardSizes().get(InternalClusterInfoService.shardIdentifierFromRouting(shard));
                        if (minSize <= currentSize && currentSize <= maxSize) {
                            count += 1;
                        }
                    }
                    assertThat(count, both(greaterThanOrEqualTo(min)).and(lessThanOrEqualTo(max)));
                }
            }
        }
    }

    /**
     * Assserts nothing about the cluster balance state.
     */
    private class NoopBalanceAssertion implements BalanceAssertion {
        @Override
        public void assertBalanced(RoutingNodes nodes, float threshold) {
        }
    }

    private void buildEvenlyDistributedClusterInfo() {
        long[] sizes = new long[numberOfIndices];
        // Set the sizes far enough appart that they don't end up in the same cohort.
        double nextSize = Math.log10(1024 * 1024 * 1024);
        for (int i = 0; i < numberOfIndices; i++) {
            sizes[i] = (long)Math.pow(10, nextSize);
            nextSize *= 10;
        }
        buildClusterInfo(sizes);
    }

    private void buildZeroSizedClusterInfo() {
        long[] sizes = new long[numberOfIndices];
        Arrays.fill(sizes, 0);
        buildClusterInfo(sizes);
    }

    private void buildRandomlyDistributedClusterInfo() {
        long[] sizes = new long[numberOfIndices];
        for (int i = 0; i < numberOfIndices; i++) {
            // Pick a random size with probabilities similar to those on the
            // Wikimedia cluster (just because I know the probabilites, not
            // because it is special)
            double size;
            if (numberOfShards[i] == 1) {
                // Single shard indices are normally small (10s of MB) or
                // hovering around 2GB.
                if (rarely()) {
                    size = (1 + getRandom().nextInt(30)) * 102.4 * 1024 * 1024;
                } else {
                    size = getRandom().nextDouble() * 10 * 1024 * 1024;
                }
            } else {
                // Multi shard indexes are sometimes huge (around 20GB) but normally
                // about the size of a large of a large single shard index.
                if (rarely()) {
                    // Rarely tens of gigabyte sized
                    size = (long)(1 + getRandom().nextInt(30)) * 1024 * 1024 * 1024;
                } else {
                    // Reasonably commonly in the 100MB to 2GB range
                    size = (1 + getRandom().nextInt(30)) * 102.4 * 1024 * 1024;
                }
            }
            sizes[i] = (long)size;
        }
        buildClusterInfo(sizes);
    }

    private void buildClusterInfo(long[] sizes) {
        ImmutableMap.Builder<String, Long> shardSizes = ImmutableMap.builder();
        ImmutableMap.Builder<String, Long> indexToAverageShardSize = ImmutableMap.builder();
        ImmutableSetMultimap.Builder<Integer, String> logShardSizeToShard = ImmutableSetMultimap.builder();
        for (int i = 0; i < numberOfIndices; i++) {
            indexToAverageShardSize.put("test" + i, sizes[i]);
            for (int s = 0; s < numberOfShards[i]; s++) {
                String sid = String.format(Locale.US, "[test%s][%s]", i, s);
                shardSizes.put(sid + "[p]", sizes[i]);
                shardSizes.put(sid + "[r]", sizes[i]);
                int logSize = sizes[i] <= smallShardLimit ? 0 : (int)Math.floor(Math.log10(sizes[i]));
                logShardSizeToShard.put(logSize, sid + "[p]");
                logShardSizeToShard.put(logSize, sid + "[r]");
            }
        }
        clusterInfo = new ClusterInfo(ImmutableMap.<String, DiskUsage> of(), shardSizes.build(), indexToAverageShardSize.build(),
                logShardSizeToShard.build());
    }

    @Test
    public void testPersistedSettings() {
        ImmutableSettings.Builder settings = settingsBuilder();
        settings.put(BalancedShardsAllocator.SETTING_INDEX_BALANCE_FACTOR, 0.2);
        settings.put(BalancedShardsAllocator.SETTING_SHARD_BALANCE_FACTOR, 0.3);
        settings.put(BalancedShardsAllocator.SETTING_PRIMARY_BALANCE_FACTOR, 0.5);
        settings.put(BalancedShardsAllocator.SETTING_SHARD_SIZE_BALANCE_FACTOR, 0.8);
        settings.put(BalancedShardsAllocator.SETTING_THRESHOLD, 2.0);
        final NodeSettingsService.Listener[] listeners = new NodeSettingsService.Listener[1];
        NodeSettingsService service = new NodeSettingsService(settingsBuilder().build()) {
            @Override
            public void addListener(Listener listener) {
                assertNull("addListener was called twice while only one time was expected", listeners[0]);
                listeners[0] = listener;
            }

        };
        BalancedShardsAllocator allocator = new BalancedShardsAllocator(settings.build(), service);
        assertThat(allocator.getIndexBalance(), equalTo(0.2f));
        assertThat(allocator.getShardBalance(), equalTo(0.3f));
        assertThat(allocator.getPrimaryBalance(), equalTo(0.5f));
        assertThat(allocator.getSizeBalance(), equalTo(0.8f));
        assertThat(allocator.getThreshold(), equalTo(2.0f));

        settings = settingsBuilder();
        settings.put(ClusterRebalanceAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ALLOW_REBALANCE, ClusterRebalanceAllocationDecider.ClusterRebalanceType.ALWAYS.toString());
        listeners[0].onRefreshSettings(settings.build());
        assertThat(allocator.getIndexBalance(), equalTo(0.2f));
        assertThat(allocator.getShardBalance(), equalTo(0.3f));
        assertThat(allocator.getPrimaryBalance(), equalTo(0.5f));
        assertThat(allocator.getSizeBalance(), equalTo(0.8f));
        assertThat(allocator.getThreshold(), equalTo(2.0f));

        settings = settingsBuilder();
        settings.put(BalancedShardsAllocator.SETTING_INDEX_BALANCE_FACTOR, 0.5);
        settings.put(BalancedShardsAllocator.SETTING_SHARD_BALANCE_FACTOR, 0.1);
        settings.put(BalancedShardsAllocator.SETTING_PRIMARY_BALANCE_FACTOR, 0.4);
        settings.put(BalancedShardsAllocator.SETTING_SHARD_SIZE_BALANCE_FACTOR, 0.01);
        settings.put(BalancedShardsAllocator.SETTING_THRESHOLD, 3.0);
        listeners[0].onRefreshSettings(settings.build());
        assertThat(allocator.getIndexBalance(), equalTo(0.5f));
        assertThat(allocator.getShardBalance(), equalTo(0.1f));
        assertThat(allocator.getPrimaryBalance(), equalTo(0.4f));
        assertThat(allocator.getSizeBalance(), equalTo(0.01f));
        assertThat(allocator.getThreshold(), equalTo(3.0f));
    }

    @Test
    public void testNoRebalanceOnPrimaryOverload() {
        ImmutableSettings.Builder settings = settingsBuilder();
        AllocationService strategy = new AllocationService(settings.build(), randomAllocationDeciders(settings.build(),
                new NodeSettingsService(ImmutableSettings.Builder.EMPTY_SETTINGS), getRandom()), new ShardsAllocators(settings.build(),
                new NoneGatewayAllocator(), new ShardsAllocator() {

            @Override
            public boolean rebalance(RoutingAllocation allocation) {
                return false;
            }

            @Override
            public boolean move(MutableShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
                return false;
            }

            @Override
            public void applyStartedShards(StartedRerouteAllocation allocation) {


            }

            @Override
            public void applyFailedShards(FailedRerouteAllocation allocation) {
            }

            /*
             *  // this allocator tries to rebuild this scenario where a rebalance is
             *  // triggered solely by the primary overload on node [1] where a shard
             *  // is rebalanced to node 0
                routing_nodes:
                -----node_id[0][V]
                --------[test][0], node[0], [R], s[STARTED]
                --------[test][4], node[0], [R], s[STARTED]
                -----node_id[1][V]
                --------[test][0], node[1], [P], s[STARTED]
                --------[test][1], node[1], [P], s[STARTED]
                --------[test][3], node[1], [R], s[STARTED]
                -----node_id[2][V]
                --------[test][1], node[2], [R], s[STARTED]
                --------[test][2], node[2], [R], s[STARTED]
                --------[test][4], node[2], [P], s[STARTED]
                -----node_id[3][V]
                --------[test][2], node[3], [P], s[STARTED]
                --------[test][3], node[3], [P], s[STARTED]
                ---- unassigned
             */
            @Override
            public boolean allocateUnassigned(RoutingAllocation allocation) {
                RoutingNodes.UnassignedShards unassigned = allocation.routingNodes().unassigned();
                boolean changed = !unassigned.isEmpty();
                for (MutableShardRouting sr : unassigned) {
                    switch (sr.id()) {
                        case 0:
                            if (sr.primary()) {
                                allocation.routingNodes().assign(sr, "node1");
                            } else {
                                allocation.routingNodes().assign(sr, "node0");
                            }
                            break;
                        case 1:
                            if (sr.primary()) {
                                allocation.routingNodes().assign(sr, "node1");
                            } else {
                                allocation.routingNodes().assign(sr, "node2");
                            }
                            break;
                        case 2:
                            if (sr.primary()) {
                                allocation.routingNodes().assign(sr, "node3");
                            } else {
                                allocation.routingNodes().assign(sr, "node2");
                            }
                            break;
                        case 3:
                            if (sr.primary()) {
                                allocation.routingNodes().assign(sr, "node3");
                            } else {
                                allocation.routingNodes().assign(sr, "node1");
                            }
                            break;
                        case 4:
                            if (sr.primary()) {
                                allocation.routingNodes().assign(sr, "node2");
                            } else {
                                allocation.routingNodes().assign(sr, "node0");
                            }
                            break;
                    }

                }
                unassigned.clear();
                return changed;
            }
        }), ClusterInfoService.EMPTY);
        MetaData.Builder metaDataBuilder = MetaData.builder();
        RoutingTable.Builder routingTableBuilder = RoutingTable.builder();
        IndexMetaData.Builder indexMeta = IndexMetaData.builder("test").numberOfShards(5).numberOfReplicas(1);
        metaDataBuilder = metaDataBuilder.put(indexMeta);
        MetaData metaData = metaDataBuilder.build();
        for (ObjectCursor<IndexMetaData> cursor : metaData.indices().values()) {
            routingTableBuilder.addAsNew(cursor.value);
        }
        RoutingTable routingTable = routingTableBuilder.build();
        DiscoveryNodes.Builder nodes = DiscoveryNodes.builder();
        for (int i = 0; i < 4; i++) {
            DiscoveryNode node = newNode("node" + i);
            nodes.put(node);
        }

        ClusterState clusterState = ClusterState.builder(org.elasticsearch.cluster.ClusterName.DEFAULT).nodes(nodes).metaData(metaData).routingTable(routingTable).build();
        routingTable = strategy.reroute(clusterState).routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();
        RoutingNodes routingNodes = clusterState.routingNodes();

        for (RoutingNode routingNode : routingNodes) {
            for (MutableShardRouting mutableShardRouting : routingNode) {
                assertThat(mutableShardRouting.state(), equalTo(ShardRoutingState.INITIALIZING));
            }
        }
        strategy = createAllocationService(settings.build());

        logger.info("use the new allocator and check if it moves shards");
        routingNodes = clusterState.routingNodes();
        routingTable = strategy.applyStartedShards(clusterState, routingNodes.shardsWithState(INITIALIZING)).routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();
        routingNodes = clusterState.routingNodes();
        for (RoutingNode routingNode : routingNodes) {
            for (MutableShardRouting mutableShardRouting : routingNode) {
                assertThat(mutableShardRouting.state(), equalTo(ShardRoutingState.STARTED));
            }
        }

        logger.info("start the replica shards");
        routingTable = strategy.applyStartedShards(clusterState, routingNodes.shardsWithState(INITIALIZING)).routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();
        routingNodes = clusterState.routingNodes();

        for (RoutingNode routingNode : routingNodes) {
            for (MutableShardRouting mutableShardRouting : routingNode) {
                assertThat(mutableShardRouting.state(), equalTo(ShardRoutingState.STARTED));
            }
        }

        logger.info("rebalancing");
        routingTable = strategy.reroute(clusterState).routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();
        routingNodes = clusterState.routingNodes();

        for (RoutingNode routingNode : routingNodes) {
            for (MutableShardRouting mutableShardRouting : routingNode) {
                assertThat(mutableShardRouting.state(), equalTo(ShardRoutingState.STARTED));
            }
        }

    }

    @Before
    public void resetClusterInfo() {
        clusterInfo = ClusterInfoService.EMPTY.getClusterInfo();
    }
}
