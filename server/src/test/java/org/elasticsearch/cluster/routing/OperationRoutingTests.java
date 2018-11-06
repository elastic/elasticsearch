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
package org.elasticsearch.cluster.routing;

import org.elasticsearch.Version;
import org.elasticsearch.action.support.replication.ClusterStateCreationUtils;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.node.ResponseCollectorService;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.object.HasToString.hasToString;

public class OperationRoutingTests extends ESTestCase{

    public void testGenerateShardId() {
        int[][] possibleValues = new int[][] {
            {8,4,2}, {20, 10, 2}, {36, 12, 3}, {15,5,1}
        };
        for (int i = 0; i < 10; i++) {
            int[] shardSplits = randomFrom(possibleValues);
            assertEquals(shardSplits[0], (shardSplits[0] / shardSplits[1]) * shardSplits[1]);
            assertEquals(shardSplits[1], (shardSplits[1] / shardSplits[2]) * shardSplits[2]);
            IndexMetaData metaData = IndexMetaData.builder("test").settings(settings(Version.CURRENT)).numberOfShards(shardSplits[0])
                .numberOfReplicas(1).build();
            String term = randomAlphaOfLength(10);
            final int shard = OperationRouting.generateShardId(metaData, term, null);
            IndexMetaData shrunk = IndexMetaData.builder("test").settings(settings(Version.CURRENT)).numberOfShards(shardSplits[1])
                .numberOfReplicas(1)
                .setRoutingNumShards(shardSplits[0]).build();
            int shrunkShard = OperationRouting.generateShardId(shrunk, term, null);

            Set<ShardId> shardIds = IndexMetaData.selectShrinkShards(shrunkShard, metaData, shrunk.getNumberOfShards());
            assertEquals(1, shardIds.stream().filter((sid) -> sid.id() == shard).count());

            shrunk = IndexMetaData.builder("test").settings(settings(Version.CURRENT)).numberOfShards(shardSplits[2]).numberOfReplicas(1)
                .setRoutingNumShards(shardSplits[0]).build();
            shrunkShard = OperationRouting.generateShardId(shrunk, term, null);
            shardIds = IndexMetaData.selectShrinkShards(shrunkShard, metaData, shrunk.getNumberOfShards());
            assertEquals(Arrays.toString(shardSplits), 1, shardIds.stream().filter((sid) -> sid.id() == shard).count());
        }
    }

    public void testGenerateShardIdSplit() {
        int[][] possibleValues = new int[][] {
            {2,4,8}, {2, 10, 20}, {3, 12, 36}, {1,5,15}
        };
        for (int i = 0; i < 10; i++) {
            int[] shardSplits = randomFrom(possibleValues);
            assertEquals(shardSplits[0], (shardSplits[0] * shardSplits[1]) / shardSplits[1]);
            assertEquals(shardSplits[1], (shardSplits[1] * shardSplits[2]) / shardSplits[2]);
            IndexMetaData metaData = IndexMetaData.builder("test").settings(settings(Version.CURRENT)).numberOfShards(shardSplits[0])
                .numberOfReplicas(1).setRoutingNumShards(shardSplits[2]).build();
            String term = randomAlphaOfLength(10);
            final int shard = OperationRouting.generateShardId(metaData, term, null);
            IndexMetaData split = IndexMetaData.builder("test").settings(settings(Version.CURRENT)).numberOfShards(shardSplits[1])
                .numberOfReplicas(1)
                .setRoutingNumShards(shardSplits[2]).build();
            int shrunkShard = OperationRouting.generateShardId(split, term, null);

            ShardId shardId = IndexMetaData.selectSplitShard(shrunkShard, metaData, split.getNumberOfShards());
            assertNotNull(shardId);
            assertEquals(shard, shardId.getId());

            split = IndexMetaData.builder("test").settings(settings(Version.CURRENT)).numberOfShards(shardSplits[2]).numberOfReplicas(1)
                .setRoutingNumShards(shardSplits[2]).build();
            shrunkShard = OperationRouting.generateShardId(split, term, null);
            shardId = IndexMetaData.selectSplitShard(shrunkShard, metaData, split.getNumberOfShards());
            assertNotNull(shardId);
            assertEquals(shard, shardId.getId());
        }
    }

    public void testPartitionedIndex() {
        // make sure the same routing value always has each _id fall within the configured partition size
        for (int shards = 1; shards < 5; shards++) {
            for (int partitionSize = 1; partitionSize == 1 || partitionSize < shards; partitionSize++) {
                IndexMetaData metaData = IndexMetaData.builder("test")
                    .settings(settings(Version.CURRENT))
                    .numberOfShards(shards)
                    .routingPartitionSize(partitionSize)
                    .numberOfReplicas(1)
                    .build();

                for (int i = 0; i < 20; i++) {
                    String routing = randomUnicodeOfLengthBetween(1, 50);
                    Set<Integer> shardSet = new HashSet<>();

                    for (int k = 0; k < 150; k++) {
                        String id = randomUnicodeOfLengthBetween(1, 50);

                        shardSet.add(OperationRouting.generateShardId(metaData, id, routing));
                    }

                    assertEquals(partitionSize, shardSet.size());
                }
            }
        }
    }

    public void testPartitionedIndexShrunk() {
        Map<String, Map<String, Integer>> routingIdToShard = new HashMap<>();

        Map<String, Integer> routingA = new HashMap<>();
        routingA.put("a_0", 1);
        routingA.put("a_1", 2);
        routingA.put("a_2", 2);
        routingA.put("a_3", 2);
        routingA.put("a_4", 1);
        routingA.put("a_5", 2);
        routingIdToShard.put("a", routingA);

        Map<String, Integer> routingB = new HashMap<>();
        routingB.put("b_0", 0);
        routingB.put("b_1", 0);
        routingB.put("b_2", 0);
        routingB.put("b_3", 0);
        routingB.put("b_4", 3);
        routingB.put("b_5", 3);
        routingIdToShard.put("b", routingB);

        Map<String, Integer> routingC = new HashMap<>();
        routingC.put("c_0", 1);
        routingC.put("c_1", 1);
        routingC.put("c_2", 0);
        routingC.put("c_3", 0);
        routingC.put("c_4", 0);
        routingC.put("c_5", 1);
        routingIdToShard.put("c", routingC);

        Map<String, Integer> routingD = new HashMap<>();
        routingD.put("d_0", 2);
        routingD.put("d_1", 2);
        routingD.put("d_2", 3);
        routingD.put("d_3", 3);
        routingD.put("d_4", 3);
        routingD.put("d_5", 3);
        routingIdToShard.put("d", routingD);

        IndexMetaData metaData = IndexMetaData.builder("test")
                .settings(settings(Version.CURRENT))
                .setRoutingNumShards(8)
                .numberOfShards(4)
                .routingPartitionSize(3)
                .numberOfReplicas(1)
                .build();

        for (Map.Entry<String, Map<String, Integer>> routingIdEntry : routingIdToShard.entrySet()) {
            String routing = routingIdEntry.getKey();

            for (Map.Entry<String, Integer> idEntry : routingIdEntry.getValue().entrySet()) {
                String id = idEntry.getKey();
                int shard = idEntry.getValue();

                assertEquals(shard, OperationRouting.generateShardId(metaData, id, routing));
            }
        }
    }

    public void testPartitionedIndexBWC() {
        Map<String, Map<String, Integer>> routingIdToShard = new HashMap<>();

        Map<String, Integer> routingA = new HashMap<>();
        routingA.put("a_0", 3);
        routingA.put("a_1", 2);
        routingA.put("a_2", 2);
        routingA.put("a_3", 3);
        routingIdToShard.put("a", routingA);

        Map<String, Integer> routingB = new HashMap<>();
        routingB.put("b_0", 5);
        routingB.put("b_1", 0);
        routingB.put("b_2", 0);
        routingB.put("b_3", 0);
        routingIdToShard.put("b", routingB);

        Map<String, Integer> routingC = new HashMap<>();
        routingC.put("c_0", 4);
        routingC.put("c_1", 4);
        routingC.put("c_2", 3);
        routingC.put("c_3", 4);
        routingIdToShard.put("c", routingC);

        Map<String, Integer> routingD = new HashMap<>();
        routingD.put("d_0", 3);
        routingD.put("d_1", 4);
        routingD.put("d_2", 4);
        routingD.put("d_3", 4);
        routingIdToShard.put("d", routingD);

        IndexMetaData metaData = IndexMetaData.builder("test")
                .settings(settings(Version.CURRENT))
                .numberOfShards(6)
                .routingPartitionSize(2)
                .numberOfReplicas(1)
                .build();

        for (Map.Entry<String, Map<String, Integer>> routingIdEntry : routingIdToShard.entrySet()) {
            String routing = routingIdEntry.getKey();

            for (Map.Entry<String, Integer> idEntry : routingIdEntry.getValue().entrySet()) {
                String id = idEntry.getKey();
                int shard = idEntry.getValue();

                assertEquals(shard, OperationRouting.generateShardId(metaData, id, routing));
            }
        }
    }

    /**
     * Ensures that all changes to the hash-function / shard selection are BWC
     */
    public void testBWC() {
        Map<String, Integer> termToShard = new TreeMap<>();
        termToShard.put("sEERfFzPSI", 1);
        termToShard.put("cNRiIrjzYd", 7);
        termToShard.put("BgfLBXUyWT", 5);
        termToShard.put("cnepjZhQnb", 3);
        termToShard.put("OKCmuYkeCK", 6);
        termToShard.put("OutXGRQUja", 5);
        termToShard.put("yCdyocKWou", 1);
        termToShard.put("KXuNWWNgVj", 2);
        termToShard.put("DGJOYrpESx", 4);
        termToShard.put("upLDybdTGs", 5);
        termToShard.put("yhZhzCPQby", 1);
        termToShard.put("EyCVeiCouA", 1);
        termToShard.put("tFyVdQauWR", 6);
        termToShard.put("nyeRYDnDQr", 6);
        termToShard.put("hswhrppvDH", 0);
        termToShard.put("BSiWvDOsNE", 5);
        termToShard.put("YHicpFBSaY", 1);
        termToShard.put("EquPtdKaBZ", 4);
        termToShard.put("rSjLZHCDfT", 5);
        termToShard.put("qoZALVcite", 7);
        termToShard.put("yDCCPVBiCm", 7);
        termToShard.put("ngizYtQgGK", 5);
        termToShard.put("FYQRIBcNqz", 0);
        termToShard.put("EBzEDAPODe", 2);
        termToShard.put("YePigbXgKb", 1);
        termToShard.put("PeGJjomyik", 3);
        termToShard.put("cyQIvDmyYD", 7);
        termToShard.put("yIEfZrYfRk", 5);
        termToShard.put("kblouyFUbu", 7);
        termToShard.put("xvIGbRiGJF", 3);
        termToShard.put("KWimwsREPf", 4);
        termToShard.put("wsNavvIcdk", 7);
        termToShard.put("xkWaPcCmpT", 0);
        termToShard.put("FKKTOnJMDy", 7);
        termToShard.put("RuLzobYixn", 2);
        termToShard.put("mFohLeFRvF", 4);
        termToShard.put("aAMXnamRJg", 7);
        termToShard.put("zKBMYJDmBI", 0);
        termToShard.put("ElSVuJQQuw", 7);
        termToShard.put("pezPtTQAAm", 7);
        termToShard.put("zBjjNEjAex", 2);
        termToShard.put("PGgHcLNPYX", 7);
        termToShard.put("hOkpeQqTDF", 3);
        termToShard.put("chZXraUPBH", 7);
        termToShard.put("FAIcSmmNXq", 5);
        termToShard.put("EZmDicyayC", 0);
        termToShard.put("GRIueBeIyL", 7);
        termToShard.put("qCChjGZYLp", 3);
        termToShard.put("IsSZQwwnUT", 3);
        termToShard.put("MGlxLFyyCK", 3);
        termToShard.put("YmscwrKSpB", 0);
        termToShard.put("czSljcjMop", 5);
        termToShard.put("XhfGWwNlng", 1);
        termToShard.put("cWpKJjlzgj", 7);
        termToShard.put("eDzIfMKbvk", 1);
        termToShard.put("WFFWYBfnTb", 0);
        termToShard.put("oDdHJxGxja", 7);
        termToShard.put("PDOQQqgIKE", 1);
        termToShard.put("bGEIEBLATe", 6);
        termToShard.put("xpRkJPWVpu", 2);
        termToShard.put("kTwZnPEeIi", 2);
        termToShard.put("DifcuqSsKk", 1);
        termToShard.put("CEmLmljpXe", 5);
        termToShard.put("cuNKtLtyJQ", 7);
        termToShard.put("yNjiAnxAmt", 5);
        termToShard.put("bVDJDCeaFm", 2);
        termToShard.put("vdnUhGLFtl", 0);
        termToShard.put("LnqSYezXbr", 5);
        termToShard.put("EzHgydDCSR", 3);
        termToShard.put("ZSKjhJlcpn", 1);
        termToShard.put("WRjUoZwtUz", 3);
        termToShard.put("RiBbcCdIgk", 4);
        termToShard.put("yizTqyjuDn", 4);
        termToShard.put("QnFjcpcZUT", 4);
        termToShard.put("agYhXYUUpl", 7);
        termToShard.put("UOjiTugjNC", 7);
        termToShard.put("nICGuWTdfV", 0);
        termToShard.put("NrnSmcnUVF", 2);
        termToShard.put("ZSzFcbpDqP", 3);
        termToShard.put("YOhahLSzzE", 5);
        termToShard.put("iWswCilUaT", 1);
        termToShard.put("zXAamKsRwj", 2);
        termToShard.put("aqGsrUPHFq", 5);
        termToShard.put("eDItImYWTS", 1);
        termToShard.put("JAYDZMRcpW", 4);
        termToShard.put("lmvAaEPflK", 7);
        termToShard.put("IKuOwPjKCx", 5);
        termToShard.put("schsINzlYB", 1);
        termToShard.put("OqbFNxrKrF", 2);
        termToShard.put("QrklDfvEJU", 6);
        termToShard.put("VLxKRKdLbx", 4);
        termToShard.put("imoydNTZhV", 1);
        termToShard.put("uFZyTyOMRO", 4);
        termToShard.put("nVAZVMPNNx", 3);
        termToShard.put("rPIdESYaAO", 5);
        termToShard.put("nbZWPWJsIM", 0);
        termToShard.put("wRZXPSoEgd", 3);
        termToShard.put("nGzpgwsSBc", 4);
        termToShard.put("AITyyoyLLs", 4);
        IndexMetaData metaData = IndexMetaData.builder("test").settings(settings(Version.CURRENT)).numberOfShards(8)
            .numberOfReplicas(1).build();
        for (Map.Entry<String, Integer> entry : termToShard.entrySet()) {
            String key = entry.getKey();
            int shard = randomBoolean() ?
                OperationRouting.generateShardId(metaData, key, null) : OperationRouting.generateShardId(metaData, "foobar", key);
            assertEquals(shard, entry.getValue().intValue());
        }
    }

    public void testPreferNodes() throws InterruptedException, IOException {
        TestThreadPool threadPool = null;
        ClusterService clusterService = null;
        try {
            threadPool = new TestThreadPool("testPreferNodes");
            clusterService = ClusterServiceUtils.createClusterService(threadPool);
            final String indexName = "test";
            ClusterServiceUtils.setState(clusterService, ClusterStateCreationUtils.stateWithActivePrimary(indexName, true, randomInt(8)));
            final Index index = clusterService.state().metaData().index(indexName).getIndex();
            final List<ShardRouting> shards = clusterService.state().getRoutingNodes().assignedShards(new ShardId(index, 0));
            final int count = randomIntBetween(1, shards.size());
            int position = 0;
            final List<String> nodes = new ArrayList<>();
            final List<ShardRouting> expected = new ArrayList<>();
            for (int i = 0; i < count; i++) {
                if (randomBoolean() && !shards.get(position).initializing()) {
                    nodes.add(shards.get(position).currentNodeId());
                    expected.add(shards.get(position));
                    position++;
                } else {
                    nodes.add("missing_" + i);
                }
            }
            final ShardIterator it =
                    new OperationRouting(Settings.EMPTY, new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS))
                            .getShards(clusterService.state(), indexName, 0, "_prefer_nodes:" + String.join(",", nodes));
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
        OperationRouting opRouting = new OperationRouting(Settings.EMPTY,
                new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS));
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
            final Index index = clusterService.state().metaData().index(indexName).getIndex();
            final List<ShardRouting> shards = clusterService.state().getRoutingNodes().assignedShards(new ShardId(index, 0));
            final int count = randomIntBetween(1, shards.size());
            int position = 0;
            final List<String> nodes = new ArrayList<>();
            final List<ShardRouting> expected = new ArrayList<>();
            for (int i = 0; i < count; i++) {
                if (randomBoolean() && !shards.get(position).initializing()) {
                    nodes.add(shards.get(position).currentNodeId());
                    expected.add(shards.get(position));
                    position++;
                } else {
                    nodes.add("missing_" + i);
                }
            }
            if (expected.size() > 0) {
                final ShardIterator it =
                    new OperationRouting(Settings.EMPTY, new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS))
                        .getShards(clusterService.state(), indexName, 0, "_only_nodes:" + String.join(",", nodes));
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
                            new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS))
                        .getShards(cs.state(), indexName, 0, "_only_nodes:" + String.join(",", nodes)));
                if (nodes.size() == 1) {
                    assertThat(
                        e,
                        hasToString(containsString(
                            "no data nodes with criteria [" + String.join(",", nodes) + "] found for shard: [test][0]")));
                } else {
                    assertThat(
                        e,
                        hasToString(containsString(
                            "no data nodes with criterion [" + String.join(",", nodes) + "] found for shard: [test][0]")));
                }
            }
        } finally {
            IOUtils.close(clusterService);
            terminate(threadPool);
        }
    }

    public void testAdaptiveReplicaSelection() throws Exception {
        final int numIndices = 1;
        final int numShards = 1;
        final int numReplicas = 2;
        final String[] indexNames = new String[numIndices];
        for (int i = 0; i < numIndices; i++) {
            indexNames[i] = "test" + i;
        }
        ClusterState state = ClusterStateCreationUtils.stateWithAssignedPrimariesAndReplicas(indexNames, numShards, numReplicas);
        OperationRouting opRouting = new OperationRouting(Settings.EMPTY,
                new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS));
        opRouting.setUseAdaptiveReplicaSelection(true);
        List<ShardRouting> searchedShards = new ArrayList<>(numShards);
        Set<String> selectedNodes = new HashSet<>(numShards);
        TestThreadPool threadPool = new TestThreadPool("testThatOnlyNodesSupportNodeIds");
        ClusterService clusterService = ClusterServiceUtils.createClusterService(threadPool);
        ResponseCollectorService collector = new ResponseCollectorService(clusterService);
        Map<String, Long> outstandingRequests = new HashMap<>();
        GroupShardsIterator<ShardIterator> groupIterator = opRouting.searchShards(state,
                indexNames, null, null, collector, outstandingRequests);

        assertThat("One group per index shard", groupIterator.size(), equalTo(numIndices * numShards));

        // Test that the shards use a round-robin pattern when there are no stats
        assertThat(groupIterator.get(0).size(), equalTo(numReplicas + 1));
        ShardRouting firstChoice = groupIterator.get(0).nextOrNull();
        assertNotNull(firstChoice);
        searchedShards.add(firstChoice);
        selectedNodes.add(firstChoice.currentNodeId());

        groupIterator = opRouting.searchShards(state, indexNames, null, null, collector, outstandingRequests);

        assertThat(groupIterator.size(), equalTo(numIndices * numShards));
        ShardRouting secondChoice = groupIterator.get(0).nextOrNull();
        assertNotNull(secondChoice);
        searchedShards.add(secondChoice);
        selectedNodes.add(secondChoice.currentNodeId());

        groupIterator = opRouting.searchShards(state, indexNames, null, null, collector, outstandingRequests);

        assertThat(groupIterator.size(), equalTo(numIndices * numShards));
        ShardRouting thirdChoice = groupIterator.get(0).nextOrNull();
        assertNotNull(thirdChoice);
        searchedShards.add(thirdChoice);
        selectedNodes.add(thirdChoice.currentNodeId());

        // All three shards should have been separate, because there are no stats yet so they're all ranked equally.
        assertThat(searchedShards.size(), equalTo(3));

        // Now let's start adding node metrics, since that will affect which node is chosen
        collector.addNodeStatistics("node_0", 2, TimeValue.timeValueMillis(200).nanos(), TimeValue.timeValueMillis(150).nanos());
        collector.addNodeStatistics("node_1", 1, TimeValue.timeValueMillis(100).nanos(), TimeValue.timeValueMillis(50).nanos());
        collector.addNodeStatistics("node_2", 1, TimeValue.timeValueMillis(200).nanos(), TimeValue.timeValueMillis(200).nanos());
        outstandingRequests.put("node_0", 1L);
        outstandingRequests.put("node_1", 1L);
        outstandingRequests.put("node_2", 1L);

        groupIterator = opRouting.searchShards(state, indexNames, null, null, collector, outstandingRequests);
        ShardRouting shardChoice = groupIterator.get(0).nextOrNull();
        // node 1 should be the lowest ranked node to start
        assertThat(shardChoice.currentNodeId(), equalTo("node_1"));

        // node 1 starts getting more loaded...
        collector.addNodeStatistics("node_1", 2, TimeValue.timeValueMillis(200).nanos(), TimeValue.timeValueMillis(150).nanos());
        groupIterator = opRouting.searchShards(state, indexNames, null, null, collector, outstandingRequests);
        shardChoice = groupIterator.get(0).nextOrNull();
        assertThat(shardChoice.currentNodeId(), equalTo("node_1"));

        // and more loaded...
        collector.addNodeStatistics("node_1", 3, TimeValue.timeValueMillis(250).nanos(), TimeValue.timeValueMillis(200).nanos());
        groupIterator = opRouting.searchShards(state, indexNames, null, null, collector, outstandingRequests);
        shardChoice = groupIterator.get(0).nextOrNull();
        assertThat(shardChoice.currentNodeId(), equalTo("node_1"));

        // and even more
        collector.addNodeStatistics("node_1", 4, TimeValue.timeValueMillis(300).nanos(), TimeValue.timeValueMillis(250).nanos());
        groupIterator = opRouting.searchShards(state, indexNames, null, null, collector, outstandingRequests);
        shardChoice = groupIterator.get(0).nextOrNull();
        // finally, node 2 is chosen instead
        assertThat(shardChoice.currentNodeId(), equalTo("node_2"));

        IOUtils.close(clusterService);
        terminate(threadPool);
    }

}
