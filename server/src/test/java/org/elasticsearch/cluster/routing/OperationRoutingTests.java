/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.cluster.routing;

import org.elasticsearch.action.support.replication.ClusterStateCreationUtils;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.node.ResponseCollectorService;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.object.HasToString.hasToString;

public class OperationRoutingTests extends ESTestCase {
    public void testPreferNodes() throws InterruptedException, IOException {
        TestThreadPool threadPool = null;
        ClusterService clusterService = null;
        try {
            threadPool = new TestThreadPool("testPreferNodes");
            clusterService = ClusterServiceUtils.createClusterService(threadPool);
            final String indexName = "test";
            ClusterServiceUtils.setState(clusterService, ClusterStateCreationUtils.stateWithActivePrimary(indexName, true, randomInt(8)));
            final Index index = clusterService.state().metadata().index(indexName).getIndex();
            final List<ShardRouting> shards = clusterService.state().getRoutingNodes().assignedShards(new ShardId(index, 0));
            final int count = randomIntBetween(1, shards.size());
            int position = 0;
            final List<String> nodes = new ArrayList<>();
            final List<ShardRouting> expected = new ArrayList<>();
            for (int i = 0; i < count; i++) {
                if (randomBoolean() && shards.get(position).initializing() == false) {
                    nodes.add(shards.get(position).currentNodeId());
                    expected.add(shards.get(position));
                    position++;
                } else {
                    nodes.add("missing_" + i);
                }
            }
            final ShardIterator it = new OperationRouting(
                Settings.EMPTY,
                new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
            ).getShards(clusterService.state(), indexName, 0, "_prefer_nodes:" + String.join(",", nodes));
            final List<ShardRouting> all = new ArrayList<>();
            ShardRouting shard;
            while ((shard = it.nextOrNull()) != null) {
                all.add(shard);
            }
            final Set<ShardRouting> preferred = new HashSet<>();
            preferred.addAll(all.subList(0, expected.size()));
            // the preferred shards should be at the front of the list
            assertThat(preferred, containsInAnyOrder(expected.toArray()));
            // verify all the shards are there
            assertThat(all.size(), equalTo(shards.size()));
        } finally {
            IOUtils.close(clusterService);
            terminate(threadPool);
        }
    }

    public void testPreferCombine() throws InterruptedException, IOException {
        TestThreadPool threadPool = null;
        ClusterService clusterService = null;
        try {
            threadPool = new TestThreadPool("testPreferCombine");
            clusterService = ClusterServiceUtils.createClusterService(threadPool);
            final String indexName = "test";
            ClusterServiceUtils.setState(clusterService, ClusterStateCreationUtils.stateWithActivePrimary(indexName, true, randomInt(8)));
            final Index index = clusterService.state().metadata().index(indexName).getIndex();
            final List<ShardRouting> shards = clusterService.state().getRoutingNodes().assignedShards(new ShardId(index, 0));
            final ClusterState state = clusterService.state();

            Function<String, List<ShardRouting>> func = prefer -> {
                final ShardIterator it = new OperationRouting(
                    Settings.EMPTY,
                    new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
                ).getShards(state, indexName, 0, prefer);
                final List<ShardRouting> all = new ArrayList<>();
                ShardRouting shard;
                while (it != null && (shard = it.nextOrNull()) != null) {
                    all.add(shard);
                }
                return all;
            };

            // combine _shards with custom_string
            final int numRepeated = 4;
            for (int i = 0; i < numRepeated; i++) {
                String custom_string = "a" + randomAlphaOfLength(10); // not start with _

                List<ShardRouting> prefer_custom = func.apply(custom_string);
                List<ShardRouting> prefer_custom_shard = func.apply("_shards:0|" + custom_string);
                List<ShardRouting> prefer_custom_othershard = func.apply("_shards:1|" + custom_string);

                assertThat(prefer_custom.size(), equalTo(shards.size()));
                assertThat(prefer_custom_shard.size(), equalTo(shards.size()));
                assertThat(prefer_custom_othershard.size(), equalTo(0));
                assertThat(prefer_custom, equalTo(prefer_custom_shard)); // same order
            }

            // combine _shards with _local
            String local = "_local";
            List<ShardRouting> prefer_shard_local = func.apply("_shards:0|" + local);
            assertThat(prefer_shard_local.size(), equalTo(shards.size()));

            // combine _shards with failed_string (start with _)
            String failed_string = "_xyz";
            final IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> func.apply("_shards:0|" + failed_string));
            assertThat(e, hasToString(containsString("no Preference for [" + failed_string + "]")));

        } finally {
            IOUtils.close(clusterService);
            terminate(threadPool);
        }
    }

    public void testFairSessionIdPreferences() throws InterruptedException, IOException {
        // Ensure that a user session is re-routed back to same nodes for
        // subsequent searches and that the nodes are selected fairly i.e.
        // given identically sorted lists of nodes across all shard IDs
        // each shard ID doesn't pick the same node.
        final int numIndices = randomIntBetween(1, 3);
        final int numShards = randomIntBetween(2, 10);
        final int numReplicas = randomIntBetween(1, 3);
        final String[] indexNames = new String[numIndices];
        for (int i = 0; i < numIndices; i++) {
            indexNames[i] = "test" + i;
        }
        ClusterState state = ClusterStateCreationUtils.stateWithAssignedPrimariesAndReplicas(indexNames, numShards, numReplicas);
        final int numRepeatedSearches = 4;
        List<ShardRouting> sessionsfirstSearch = null;
        OperationRouting opRouting = new OperationRouting(
            Settings.EMPTY,
            new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
        );
        String sessionKey = randomAlphaOfLength(10);
        for (int i = 0; i < numRepeatedSearches; i++) {
            List<ShardRouting> searchedShards = new ArrayList<>(numShards);
            Set<String> selectedNodes = new HashSet<>(numShards);
            final GroupShardsIterator<ShardIterator> groupIterator = opRouting.searchShards(state, indexNames, null, sessionKey);

            assertThat("One group per index shard", groupIterator.size(), equalTo(numIndices * numShards));
            for (ShardIterator shardIterator : groupIterator) {
                assertThat(shardIterator.size(), equalTo(numReplicas + 1));

                ShardRouting firstChoice = shardIterator.nextOrNull();
                assertNotNull(firstChoice);
                ShardRouting duelFirst = duelGetShards(state, firstChoice.shardId(), sessionKey).nextOrNull();
                assertThat("Regression test failure", duelFirst, equalTo(firstChoice));

                searchedShards.add(firstChoice);
                selectedNodes.add(firstChoice.currentNodeId());
            }
            if (sessionsfirstSearch == null) {
                sessionsfirstSearch = searchedShards;
            } else {
                assertThat("Sessions must reuse same replica choices", searchedShards, equalTo(sessionsfirstSearch));
            }

            // 2 is the bare minimum number of nodes we can reliably expect from
            // randomized tests in my experiments over thousands of iterations.
            // Ideally we would test for greater levels of machine utilisation
            // given a configuration with many nodes but the nature of hash
            // collisions means we can't always rely on optimal node usage in
            // all cases.
            assertThat("Search should use more than one of the nodes", selectedNodes.size(), greaterThan(1));
        }
    }

    // Regression test for the routing logic - implements same hashing logic
    private ShardIterator duelGetShards(ClusterState clusterState, ShardId shardId, String sessionId) {
        final IndexShardRoutingTable indexShard = clusterState.getRoutingTable().shardRoutingTable(shardId.getIndexName(), shardId.getId());
        int routingHash = Murmur3HashFunction.hash(sessionId);
        routingHash = 31 * routingHash + indexShard.shardId.hashCode();
        return indexShard.activeInitializingShardsIt(routingHash);
    }

    public void testThatOnlyNodesSupportNodeIds() throws InterruptedException, IOException {
        TestThreadPool threadPool = null;
        ClusterService clusterService = null;
        try {
            threadPool = new TestThreadPool("testThatOnlyNodesSupportNodeIds");
            clusterService = ClusterServiceUtils.createClusterService(threadPool);
            final String indexName = "test";
            ClusterServiceUtils.setState(clusterService, ClusterStateCreationUtils.stateWithActivePrimary(indexName, true, randomInt(8)));
            final Index index = clusterService.state().metadata().index(indexName).getIndex();
            final List<ShardRouting> shards = clusterService.state().getRoutingNodes().assignedShards(new ShardId(index, 0));
            final int count = randomIntBetween(1, shards.size());
            int position = 0;
            final List<String> nodes = new ArrayList<>();
            final List<ShardRouting> expected = new ArrayList<>();
            for (int i = 0; i < count; i++) {
                if (randomBoolean() && shards.get(position).initializing() == false) {
                    nodes.add(shards.get(position).currentNodeId());
                    expected.add(shards.get(position));
                    position++;
                } else {
                    nodes.add("missing_" + i);
                }
            }
            if (expected.size() > 0) {
                final ShardIterator it = new OperationRouting(
                    Settings.EMPTY,
                    new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
                ).getShards(clusterService.state(), indexName, 0, "_only_nodes:" + String.join(",", nodes));
                final List<ShardRouting> only = new ArrayList<>();
                ShardRouting shard;
                while ((shard = it.nextOrNull()) != null) {
                    only.add(shard);
                }
                assertThat(new HashSet<>(only), equalTo(new HashSet<>(expected)));
            } else {
                final ClusterService cs = clusterService;
                final IllegalArgumentException e = expectThrows(
                    IllegalArgumentException.class,
                    () -> new OperationRouting(
                        Settings.EMPTY,
                        new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
                    ).getShards(cs.state(), indexName, 0, "_only_nodes:" + String.join(",", nodes))
                );
                if (nodes.size() == 1) {
                    assertThat(
                        e,
                        hasToString(
                            containsString("no data nodes with criteria [" + String.join(",", nodes) + "] found for shard: [test][0]")
                        )
                    );
                } else {
                    assertThat(
                        e,
                        hasToString(
                            containsString("no data nodes with criterion [" + String.join(",", nodes) + "] found for shard: [test][0]")
                        )
                    );
                }
            }
        } finally {
            IOUtils.close(clusterService);
            terminate(threadPool);
        }
    }

    public void testARSRanking() throws Exception {
        final int numIndices = 1;
        final int numShards = 1;
        final int numReplicas = 2;
        final String[] indexNames = new String[numIndices];
        for (int i = 0; i < numIndices; i++) {
            indexNames[i] = "test" + i;
        }
        ClusterState state = ClusterStateCreationUtils.stateWithAssignedPrimariesAndReplicas(indexNames, numShards, numReplicas);
        OperationRouting opRouting = new OperationRouting(
            Settings.EMPTY,
            new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
        );
        opRouting.setUseAdaptiveReplicaSelection(true);
        List<ShardRouting> searchedShards = new ArrayList<>(numShards);
        TestThreadPool threadPool = new TestThreadPool("test");
        ClusterService clusterService = ClusterServiceUtils.createClusterService(threadPool);
        ResponseCollectorService collector = new ResponseCollectorService(clusterService);
        GroupShardsIterator<ShardIterator> groupIterator = opRouting.searchShards(
            state,
            indexNames,
            null,
            null,
            collector,
            new HashMap<>()
        );

        assertThat("One group per index shard", groupIterator.size(), equalTo(numIndices * numShards));

        // Test that the shards use a round-robin pattern when there are no stats
        assertThat(groupIterator.get(0).size(), equalTo(numReplicas + 1));
        ShardRouting firstChoice = groupIterator.get(0).nextOrNull();
        assertNotNull(firstChoice);
        searchedShards.add(firstChoice);

        groupIterator = opRouting.searchShards(state, indexNames, null, null, collector, new HashMap<>());

        assertThat(groupIterator.size(), equalTo(numIndices * numShards));
        ShardRouting secondChoice = groupIterator.get(0).nextOrNull();
        assertNotNull(secondChoice);
        searchedShards.add(secondChoice);

        groupIterator = opRouting.searchShards(state, indexNames, null, null, collector, new HashMap<>());

        assertThat(groupIterator.size(), equalTo(numIndices * numShards));
        ShardRouting thirdChoice = groupIterator.get(0).nextOrNull();
        assertNotNull(thirdChoice);
        searchedShards.add(thirdChoice);

        // All three shards should have been separate, because there are no stats yet so they're all ranked equally.
        assertThat(searchedShards.size(), equalTo(3));

        // Now let's start adding node metrics, since that will affect which node is chosen
        collector.addNodeStatistics("node_0", 2, TimeValue.timeValueMillis(200).nanos(), TimeValue.timeValueMillis(150).nanos());
        collector.addNodeStatistics("node_1", 1, TimeValue.timeValueMillis(150).nanos(), TimeValue.timeValueMillis(50).nanos());
        collector.addNodeStatistics("node_2", 1, TimeValue.timeValueMillis(200).nanos(), TimeValue.timeValueMillis(200).nanos());

        groupIterator = opRouting.searchShards(state, indexNames, null, null, collector, new HashMap<>());
        ShardRouting shardChoice = groupIterator.get(0).nextOrNull();
        // node 1 should be the lowest ranked node to start
        assertThat(shardChoice.currentNodeId(), equalTo("node_1"));

        // node 1 starts getting more loaded...
        collector.addNodeStatistics("node_1", 1, TimeValue.timeValueMillis(200).nanos(), TimeValue.timeValueMillis(100).nanos());
        groupIterator = opRouting.searchShards(state, indexNames, null, null, collector, new HashMap<>());
        shardChoice = groupIterator.get(0).nextOrNull();
        assertThat(shardChoice.currentNodeId(), equalTo("node_1"));

        // and more loaded...
        collector.addNodeStatistics("node_1", 2, TimeValue.timeValueMillis(220).nanos(), TimeValue.timeValueMillis(120).nanos());
        groupIterator = opRouting.searchShards(state, indexNames, null, null, collector, new HashMap<>());
        shardChoice = groupIterator.get(0).nextOrNull();
        assertThat(shardChoice.currentNodeId(), equalTo("node_1"));

        // and even more
        collector.addNodeStatistics("node_1", 3, TimeValue.timeValueMillis(250).nanos(), TimeValue.timeValueMillis(150).nanos());
        groupIterator = opRouting.searchShards(state, indexNames, null, null, collector, new HashMap<>());
        shardChoice = groupIterator.get(0).nextOrNull();
        // finally, node 0 is chosen instead
        assertThat(shardChoice.currentNodeId(), equalTo("node_0"));

        IOUtils.close(clusterService);
        terminate(threadPool);
    }

    public void testARSStatsAdjustment() throws Exception {
        int numIndices = 1;
        int numShards = 1;
        int numReplicas = 1;
        String[] indexNames = new String[numIndices];
        for (int i = 0; i < numIndices; i++) {
            indexNames[i] = "test" + i;
        }

        ClusterState state = ClusterStateCreationUtils.stateWithAssignedPrimariesAndReplicas(indexNames, numShards, numReplicas);
        OperationRouting opRouting = new OperationRouting(
            Settings.EMPTY,
            new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
        );
        opRouting.setUseAdaptiveReplicaSelection(true);
        TestThreadPool threadPool = new TestThreadPool("test");
        ClusterService clusterService = ClusterServiceUtils.createClusterService(threadPool);

        ResponseCollectorService collector = new ResponseCollectorService(clusterService);
        GroupShardsIterator<ShardIterator> groupIterator = opRouting.searchShards(
            state,
            indexNames,
            null,
            null,
            collector,
            new HashMap<>()
        );
        assertThat("One group per index shard", groupIterator.size(), equalTo(numIndices * numShards));

        // We have two nodes, where the second has more load
        collector.addNodeStatistics("node_0", 1, TimeValue.timeValueMillis(50).nanos(), TimeValue.timeValueMillis(40).nanos());
        collector.addNodeStatistics("node_1", 2, TimeValue.timeValueMillis(100).nanos(), TimeValue.timeValueMillis(60).nanos());

        // Check the first node is usually selected, if it's stats don't change much
        for (int i = 0; i < 10; i++) {
            groupIterator = opRouting.searchShards(state, indexNames, null, null, collector, new HashMap<>());
            ShardRouting shardChoice = groupIterator.get(0).nextOrNull();
            assertThat(shardChoice.currentNodeId(), equalTo("node_0"));

            int responseTime = 50 + randomInt(5);
            int serviceTime = 40 + randomInt(5);
            collector.addNodeStatistics(
                "node_0",
                1,
                TimeValue.timeValueMillis(responseTime).nanos(),
                TimeValue.timeValueMillis(serviceTime).nanos()
            );
        }

        // Check that we try the second when the first node slows down more
        collector.addNodeStatistics("node_0", 2, TimeValue.timeValueMillis(60).nanos(), TimeValue.timeValueMillis(50).nanos());
        groupIterator = opRouting.searchShards(state, indexNames, null, null, collector, new HashMap<>());
        ShardRouting shardChoice = groupIterator.get(0).nextOrNull();
        assertThat(shardChoice.currentNodeId(), equalTo("node_1"));

        IOUtils.close(clusterService);
        terminate(threadPool);
    }

    public void testARSOutstandingRequestTracking() throws Exception {
        int numIndices = 1;
        int numShards = 2;
        int numReplicas = 1;
        String[] indexNames = new String[numIndices];
        for (int i = 0; i < numIndices; i++) {
            indexNames[i] = "test" + i;
        }

        ClusterState state = ClusterStateCreationUtils.stateWithAssignedPrimariesAndReplicas(indexNames, numShards, numReplicas);
        OperationRouting opRouting = new OperationRouting(
            Settings.EMPTY,
            new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
        );
        opRouting.setUseAdaptiveReplicaSelection(true);
        TestThreadPool threadPool = new TestThreadPool("test");
        ClusterService clusterService = ClusterServiceUtils.createClusterService(threadPool);

        ResponseCollectorService collector = new ResponseCollectorService(clusterService);

        // We have two nodes with very similar statistics
        collector.addNodeStatistics("node_0", 1, TimeValue.timeValueMillis(50).nanos(), TimeValue.timeValueMillis(40).nanos());
        collector.addNodeStatistics("node_1", 1, TimeValue.timeValueMillis(51).nanos(), TimeValue.timeValueMillis(40).nanos());
        Map<String, Long> outstandingRequests = new HashMap<>();

        // Check that we choose to search over both nodes
        GroupShardsIterator<ShardIterator> groupIterator = opRouting.searchShards(
            state,
            indexNames,
            null,
            null,
            collector,
            outstandingRequests
        );

        Set<String> nodeIds = new HashSet<>();
        nodeIds.add(groupIterator.get(0).nextOrNull().currentNodeId());
        nodeIds.add(groupIterator.get(1).nextOrNull().currentNodeId());
        assertThat(nodeIds, equalTo(Set.of("node_0", "node_1")));
        assertThat(outstandingRequests.get("node_0"), equalTo(1L));
        assertThat(outstandingRequests.get("node_1"), equalTo(1L));

        // The first node becomes much more loaded
        collector.addNodeStatistics("node_0", 6, TimeValue.timeValueMillis(300).nanos(), TimeValue.timeValueMillis(200).nanos());
        outstandingRequests = new HashMap<>();

        // Check that we always choose the second node
        groupIterator = opRouting.searchShards(state, indexNames, null, null, collector, outstandingRequests);

        nodeIds = new HashSet<>();
        nodeIds.add(groupIterator.get(0).nextOrNull().currentNodeId());
        nodeIds.add(groupIterator.get(1).nextOrNull().currentNodeId());
        assertThat(nodeIds, equalTo(Set.of("node_1")));
        assertThat(outstandingRequests.get("node_1"), equalTo(2L));

        IOUtils.close(clusterService);
        terminate(threadPool);
    }

}
