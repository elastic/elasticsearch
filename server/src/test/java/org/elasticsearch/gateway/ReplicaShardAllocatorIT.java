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

package org.elasticsearch.gateway;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.decider.EnableAllocationDecider;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.MergePolicyConfig;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.InternalSettingsPlugin;
import org.elasticsearch.test.InternalTestCluster;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.stream.IntStream;

import static java.util.stream.Collectors.toList;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0)
public class ReplicaShardAllocatorIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(InternalSettingsPlugin.class);
    }

    public void testRecentPrimaryData() throws Exception {
        final String indexName = "test";
        internalCluster().startMasterOnlyNode();
        String primary = internalCluster().startDataOnlyNode();

        assertAcked(client().admin().indices().prepareCreate(indexName)
            .setSettings(Settings.builder()
                .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 2)
                .put(EnableAllocationDecider.INDEX_ROUTING_REBALANCE_ENABLE_SETTING.getKey(), "none")
                // disable merges to keep segments the same
                .put(MergePolicyConfig.INDEX_MERGE_ENABLED, "false"))
            .get());

        String replica1 = internalCluster().startDataOnlyNode();

        indexRandom(randomBoolean(), randomBoolean(), randomBoolean(), IntStream.range(0, randomIntBetween(100, 200))
            .mapToObj(n -> client().prepareIndex(indexName, "_doc").setSource("num", n)).collect(toList()));

        flush(indexName);

        final String initialReplica = allocatedToReplica(indexName);

        String replicaWithFileOverlap = internalCluster().startDataOnlyNode();
        ensureGreen();

        // this extra node ensures allocation deciders say yes. But due to delayed allocation, it will not be used until we are
        // done.
        internalCluster().startDataOnlyNode();

        internalCluster().restartNode(replicaWithFileOverlap, new InternalTestCluster.RestartCallback() {
            @Override
            public Settings onNodeStopped(String nodeName) throws Exception {
                // disabled below for now, since it makes test succeed. Problem is that when a node leaves the cluster,
                // GatewayAllocator.applyFailedShards is not called. Likewise, when TransportWriteAction marks stale, it does not call
                // it because it is no longer in the routing table anyway.
                // without below the test triggers the cache issue half the time (since it picks one of the nodes with matching seqno
                // randomly).

                // ensure replicaWithFileOverlap is outdated seqno-wise compared to primary
//                indexRandom(randomBoolean(), randomBoolean(), randomBoolean(), IntStream.range(0, randomIntBetween(1, 5))
//                    .mapToObj(n -> client().prepareIndex(indexName, "_doc").setSource("num", n)).collect(toList()));

                // this should fix above, but cannot do, since it picks wrong node to start (data folder issue).
                // clear cache, since this is not done when replica1 dies below (bug?).
//                internalCluster().restartNode(primary, new InternalTestCluster.RestartCallback());

                internalCluster().restartNode(replica1, new InternalTestCluster.RestartCallback() {
                    @Override
                    public Settings onNodeStopped(String nodeName) throws Exception {
                        assertAcked(client().admin().cluster().prepareUpdateSettings().setTransientSettings(Settings.builder()
                            .put(EnableAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ENABLE_SETTING.getKey(), "primaries")).get());

                        assertAcked(client().admin().indices().prepareUpdateSettings(indexName)
                            .setSettings(Settings.builder()
                                .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 1))
                            .get());

                        // todo: can we wait for first reroute being done, including async-fetching and subsequent processing?
                        Thread.sleep(100);

                        // invalidate primary data compared to cache on master.
                        indexRandom(randomBoolean(), randomBoolean(), randomBoolean(), IntStream.range(0, randomIntBetween(1, 5))
                            .mapToObj(n -> client().prepareIndex(indexName, "_doc").setSource("num", n)).collect(toList()));

                        return super.onNodeStopped(nodeName);
                    }
                });
                return super.onNodeStopped(nodeName);
            }
        });

        assertThat(client().admin().cluster().prepareHealth(indexName).get().getStatus(), is(ClusterHealthStatus.YELLOW));

        logger.info("--> Re-enabling allocation");
        assertAcked(client().admin().cluster().prepareUpdateSettings().setTransientSettings(Settings.builder()
            .put(EnableAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ENABLE_SETTING.getKey(), (String) null)).get());
        ensureGreen();

        String after = allocatedToReplica(indexName);
        logger.info("--> Now allocated to {}, was {}", after, initialReplica);
        assertThat(after, not(equalTo(initialReplica)));
    }

    private String allocatedToReplica(String indexName) {
        final ClusterState clusterState = client().admin().cluster().prepareState().get().getState();
        List<String> replicas =
            clusterState.routingTable().allShards(indexName).stream()
                .filter(r -> r.primary() == false).filter(ShardRouting::started).map(ShardRouting::currentNodeId).collect(toList());
        assertEquals(1, replicas.size());
        return replicas.get(0);
    }
}
