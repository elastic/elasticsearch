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

import org.apache.lucene.util.IOUtils;
import org.elasticsearch.Version;
import org.elasticsearch.action.support.replication.ClusterStateCreationUtils;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.routing.allocation.decider.AwarenessAllocationDecider;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.object.HasToString.hasToString;
import static org.hamcrest.Matchers.equalTo;

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
            String term = randomAsciiOfLength(10);
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

}
