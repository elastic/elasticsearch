/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.indices.shards;

import org.apache.lucene.index.CorruptIndexException;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.store.MockFSIndexStore;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoTimeout;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST)
public class IndicesShardStoreRequestIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(MockFSIndexStore.TestPlugin.class);
    }

    private static IndicesShardStoresResponse execute(IndicesShardStoresRequest request) {
        return client().execute(TransportIndicesShardStoresAction.TYPE, request).actionGet(10, TimeUnit.SECONDS);
    }

    public void testEmpty() {
        ensureGreen();
        IndicesShardStoresResponse rsp = execute(new IndicesShardStoresRequest());
        assertThat(rsp.getStoreStatuses().size(), equalTo(0));
    }

    public void testBasic() throws Exception {
        String index = "test";
        internalCluster().ensureAtLeastNumDataNodes(2);
        assertAcked(prepareCreate(index).setSettings(indexSettings(2, 1)));
        indexRandomData(index);
        ensureGreen(index);

        // no unallocated shards
        IndicesShardStoresResponse response = execute(new IndicesShardStoresRequest(index));
        assertThat(response.getStoreStatuses().size(), equalTo(0));

        // all shards
        response = execute(new IndicesShardStoresRequest(index).shardStatuses("all"));
        assertThat(response.getStoreStatuses().containsKey(index), equalTo(true));
        Map<Integer, List<IndicesShardStoresResponse.StoreStatus>> shardStores = response.getStoreStatuses().get(index);
        assertThat(shardStores.size(), equalTo(2));
        for (Map.Entry<Integer, List<IndicesShardStoresResponse.StoreStatus>> shardStoreStatuses : shardStores.entrySet()) {
            for (IndicesShardStoresResponse.StoreStatus storeStatus : shardStoreStatuses.getValue()) {
                assertThat(storeStatus.getAllocationId(), notNullValue());
                assertThat(storeStatus.getNode(), notNullValue());
                assertThat(storeStatus.getStoreException(), nullValue());
            }
        }

        // default with unassigned shards
        ensureGreen(index);
        logger.info("--> disable allocation");
        disableAllocation(index);
        logger.info("--> stop random node");
        int num = clusterAdmin().prepareState().get().getState().nodes().getSize();
        internalCluster().stopNode(internalCluster().getNodeNameThat(new IndexNodePredicate(index)));
        assertNoTimeout(clusterAdmin().prepareHealth().setWaitForNodes("" + (num - 1)));
        ClusterState clusterState = clusterAdmin().prepareState().get().getState();
        List<ShardRouting> unassignedShards = clusterState.routingTable().index(index).shardsWithState(ShardRoutingState.UNASSIGNED);
        response = execute(new IndicesShardStoresRequest(index));
        assertThat(response.getStoreStatuses().containsKey(index), equalTo(true));
        Map<Integer, List<IndicesShardStoresResponse.StoreStatus>> shardStoresStatuses = response.getStoreStatuses().get(index);
        assertThat(shardStoresStatuses.size(), equalTo(unassignedShards.size()));
        for (Map.Entry<Integer, List<IndicesShardStoresResponse.StoreStatus>> storesStatus : shardStoresStatuses.entrySet()) {
            assertThat("must report for one store", storesStatus.getValue().size(), equalTo(1));
            assertThat(
                "reported store should be primary",
                storesStatus.getValue().get(0).getAllocationStatus(),
                equalTo(IndicesShardStoresResponse.StoreStatus.AllocationStatus.PRIMARY)
            );
        }
        logger.info("--> enable allocation");
        enableAllocation(index);
    }

    public void testIndices() throws Exception {
        String index1 = "test1";
        String index2 = "test2";
        internalCluster().ensureAtLeastNumDataNodes(2);
        for (final var index : List.of(index1, index2)) {
            final var settings = Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 2);
            if (randomBoolean()) {
                settings.put(IndexMetadata.SETTING_INDEX_HIDDEN, randomBoolean());
            }
            assertAcked(prepareCreate(index).setSettings(settings));
        }
        indexRandomData(index1);
        indexRandomData(index2);
        ensureGreen();
        IndicesShardStoresResponse response = execute(new IndicesShardStoresRequest(new String[] {}).shardStatuses("all"));
        Map<String, Map<Integer, List<IndicesShardStoresResponse.StoreStatus>>> shardStatuses = response.getStoreStatuses();
        assertThat(shardStatuses.containsKey(index1), equalTo(true));
        assertThat(shardStatuses.containsKey(index2), equalTo(true));
        assertThat(shardStatuses.get(index1).size(), equalTo(2));
        assertThat(shardStatuses.get(index2).size(), equalTo(2));

        // ensure index filtering works
        response = execute(new IndicesShardStoresRequest(index1).shardStatuses("all"));
        shardStatuses = response.getStoreStatuses();
        assertThat(shardStatuses.containsKey(index1), equalTo(true));
        assertThat(shardStatuses.containsKey(index2), equalTo(false));
        assertThat(shardStatuses.get(index1).size(), equalTo(2));
    }

    public void testCorruptedShards() throws Exception {
        String index = "test";
        internalCluster().ensureAtLeastNumDataNodes(2);
        assertAcked(
            prepareCreate(index).setSettings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, "5")
                    .put(MockFSIndexStore.INDEX_CHECK_INDEX_ON_CLOSE_SETTING.getKey(), false)
            )
        );

        indexRandomData(index);
        ensureGreen(index);

        logger.info("--> disable allocation");
        disableAllocation(index);

        logger.info("--> corrupt random shard copies");
        Map<Integer, Set<String>> corruptedShardIDMap = new HashMap<>();
        Index idx = resolveIndex(index);
        for (String node : internalCluster().nodesInclude(index)) {
            IndicesService indexServices = internalCluster().getInstance(IndicesService.class, node);
            IndexService indexShards = indexServices.indexServiceSafe(idx);
            for (Integer shardId : indexShards.shardIds()) {
                IndexShard shard = indexShards.getShard(shardId);
                if (randomBoolean()) {
                    logger.debug("--> failing shard [{}] on node [{}]", shardId, node);
                    shard.failShard("test", new CorruptIndexException("test corrupted", ""));
                    logger.debug("--> failed shard [{}] on node [{}]", shardId, node);
                    Set<String> nodes = corruptedShardIDMap.get(shardId);
                    if (nodes == null) {
                        nodes = new HashSet<>();
                    }
                    nodes.add(node);
                    corruptedShardIDMap.put(shardId, nodes);
                }
            }
        }

        assertBusy(() -> { // IndicesClusterStateService#failAndRemoveShard() called asynchronously but we need it to have completed here.
            IndicesShardStoresResponse rsp = execute(new IndicesShardStoresRequest(index).shardStatuses("all"));
            Map<Integer, List<IndicesShardStoresResponse.StoreStatus>> shardStatuses = rsp.getStoreStatuses().get(index);
            assertNotNull(shardStatuses);
            assertThat(shardStatuses.size(), greaterThan(0));
            for (Map.Entry<Integer, List<IndicesShardStoresResponse.StoreStatus>> shardStatus : shardStatuses.entrySet()) {
                for (IndicesShardStoresResponse.StoreStatus status : shardStatus.getValue()) {
                    if (corruptedShardIDMap.containsKey(shardStatus.getKey())
                        && corruptedShardIDMap.get(shardStatus.getKey()).contains(status.getNode().getName())) {
                        assertThat(
                            "shard [" + shardStatus.getKey() + "] is failed on node [" + status.getNode().getName() + "]",
                            status.getStoreException(),
                            notNullValue()
                        );
                    } else {
                        assertNull(
                            "shard [" + shardStatus.getKey() + "] is not failed on node [" + status.getNode().getName() + "]",
                            status.getStoreException()
                        );
                    }
                }
            }
        });
        logger.info("--> enable allocation");
        enableAllocation(index);
    }

    private void indexRandomData(String index) throws InterruptedException {
        int numDocs = scaledRandomIntBetween(10, 20);
        IndexRequestBuilder[] builders = new IndexRequestBuilder[numDocs];
        for (int i = 0; i < builders.length; i++) {
            builders[i] = prepareIndex(index).setSource("field", "value");
        }
        indexRandom(true, builders);
        indicesAdmin().prepareFlush().setForce(true).get();
    }

    private static final class IndexNodePredicate implements Predicate<Settings> {
        private final Set<String> nodesWithShard;

        IndexNodePredicate(String index) {
            this.nodesWithShard = findNodesWithShard(index);
        }

        @Override
        public boolean test(Settings settings) {
            return nodesWithShard.contains(settings.get("node.name"));
        }

        private Set<String> findNodesWithShard(String index) {
            ClusterState state = clusterAdmin().prepareState().get().getState();
            IndexRoutingTable indexRoutingTable = state.routingTable().index(index);
            List<ShardRouting> startedShards = indexRoutingTable.shardsWithState(ShardRoutingState.STARTED);
            Set<String> nodesNamesWithShard = new HashSet<>();
            for (ShardRouting startedShard : startedShards) {
                nodesNamesWithShard.add(state.nodes().get(startedShard.currentNodeId()).getName());
            }
            return nodesNamesWithShard;
        }
    }
}
