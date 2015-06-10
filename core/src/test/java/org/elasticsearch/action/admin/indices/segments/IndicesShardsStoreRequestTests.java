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

package org.elasticsearch.action.admin.indices.segments;

import com.carrotsearch.hppc.cursors.IntObjectCursor;

import com.carrotsearch.hppc.cursors.ObjectCursor;
import com.google.common.base.Predicate;
import org.elasticsearch.action.admin.indices.shards.IndicesShardsStoresRequest.ShardState;
import org.elasticsearch.action.admin.indices.shards.IndicesShardsStoresResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.client.Requests;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.routing.*;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.ImmutableOpenIntMap;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.*;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.elasticsearch.test.store.MockFSDirectoryService;
import org.junit.Test;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutionException;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAllSuccessful;
import static org.hamcrest.Matchers.*;

@ElasticsearchIntegrationTest.ClusterScope(scope = ElasticsearchIntegrationTest.Scope.TEST)
public class IndicesShardsStoreRequestTests extends ElasticsearchIntegrationTest {

    private void indexRandomData(String index) throws ExecutionException, InterruptedException {
        int numDocs = scaledRandomIntBetween(100, 1000);
        IndexRequestBuilder[] builders = new IndexRequestBuilder[numDocs];
        for (int i = 0; i < builders.length; i++) {
            builders[i] = client().prepareIndex(index, "type").setSource("field", "value");
        }
        indexRandom(true, builders);
        assertAllSuccessful(client().admin().indices().prepareFlush().setForce(true).setWaitIfOngoing(true).execute().actionGet());
        ensureGreen(index);
    }

    @Test
    public void testEmpty() {
        ensureGreen();
        IndicesShardsStoresResponse rsp = client().admin().indices().prepareShardStores().get();
        assertThat(rsp.getShardStatuses().size(), equalTo(0));
    }

    @Test
    public void testBasic() throws Exception {
        String index = "test";
        internalCluster().ensureAtLeastNumDataNodes(2);
        assertAcked(prepareCreate(index).setSettings(Settings.builder()
                        .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, "2")
                        .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, "1")
        ));
        ensureGreen(index);
        indexRandomData(index);

        // no unallocated shards
        IndicesShardsStoresResponse response = client().admin().indices().prepareShardStores(index).get();
        assertThat(response.getShardStatuses().size(), equalTo(0));

        response = client().admin().indices().shardsStores(Requests.indicesShardsStoresRequest(index).shardState(ShardState.ALL)).get();
        assertThat(response.getShardStatuses().containsKey(index), equalTo(true));
        ImmutableOpenIntMap<List<IndicesShardsStoresResponse.ShardStoreStatus>> shardStores = response.getShardStatuses().get(index);
        assertThat(shardStores.values().size(), equalTo(2));
        for (ObjectCursor<List<IndicesShardsStoresResponse.ShardStoreStatus>> shardStoreStatuses : shardStores.values()) {
            for (IndicesShardsStoresResponse.ShardStoreStatus shardStoreStatus : shardStoreStatuses.value) {
                assertThat(shardStoreStatus.getVersion(), greaterThan(-1l));
                assertThat(shardStoreStatus.getNode(), notNullValue());
                assertThat(shardStoreStatus.getStoreException(), nullValue());
            }
        }
        ensureGreen(index);
        logger.info("--> disable allocation");
        disableAllocation(index);
        logger.info("--> stop random node");
        internalCluster().stopRandomNode(new IndexNonMasterNodePredicate(index, internalCluster().getMasterName()));
        ensureYellow(index);
        response = client().admin().indices().shardsStores(Requests.indicesShardsStoresRequest(index).shardState(ShardState.UNALLOCATED)).get();
        assertThat(response.getShardStatuses().containsKey(index), equalTo(true));
        ImmutableOpenIntMap<List<IndicesShardsStoresResponse.ShardStoreStatus>> shardStoresStatuses = response.getShardStatuses().get(index);
        assertThat(shardStoresStatuses.size(), equalTo(2));
        logger.info("--> enable allocation");
        enableAllocation(index);
    }

    @Test
    public void testIndices() throws Exception {
        String index1 = "test1";
        String index2 = "test2";
        internalCluster().ensureAtLeastNumDataNodes(2);
        assertAcked(prepareCreate(index1).setSettings(Settings.builder()
                        .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, "2")
        ));
        assertAcked(prepareCreate(index2).setSettings(Settings.builder()
                        .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, "2")
        ));
        ensureGreen();
        indexRandomData(index1);
        indexRandomData(index2);
        IndicesShardsStoresResponse response = client().admin().indices().shardsStores(Requests.indicesShardsStoresRequest().shardState(ShardState.ALL)).get();
        ImmutableOpenMap<String, ImmutableOpenIntMap<List<IndicesShardsStoresResponse.ShardStoreStatus>>> shardStatuses = response.getShardStatuses();
        assertThat(shardStatuses.containsKey(index1), equalTo(true));
        assertThat(shardStatuses.containsKey(index2), equalTo(true));
        assertThat(shardStatuses.get(index1).size(), equalTo(2));
        assertThat(shardStatuses.get(index2).size(), equalTo(2));

        // ensure index filtering works
        response = client().admin().indices().shardsStores(Requests.indicesShardsStoresRequest(index1).shardState(ShardState.ALL)).get();
        shardStatuses = response.getShardStatuses();
        assertThat(shardStatuses.containsKey(index1), equalTo(true));
        assertThat(shardStatuses.containsKey(index2), equalTo(false));
        assertThat(shardStatuses.get(index1).size(), equalTo(2));
    }

    @Test
    public void testCorruptedShards() throws Exception {
        String index = "test";
        internalCluster().ensureAtLeastNumDataNodes(2);
        assertAcked(prepareCreate(index).setSettings(Settings.builder()
                        .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, "2")
                        .put(MockFSDirectoryService.CHECK_INDEX_ON_CLOSE, false)
        ));
        ensureGreen();
        indexRandomData(index);

        logger.info("--> disable allocation");
        disableAllocation(index);

        logger.info("--> corrupt all shards");
        Set<Integer> corruptedShardIDs = new HashSet<>();
        for (String node : internalCluster().nodesInclude(index)) {
            IndicesService indexServices = internalCluster().getInstance(IndicesService.class, node);
            IndexService indexShards = indexServices.indexServiceSafe(index);
            for (Integer shardId : indexShards.shardIds()) {
                IndexShard shard = indexShards.shardSafe(shardId);
                shard.failShard("test", new IOException("test corrupted"));
                corruptedShardIDs.add(shardId);
            }
        }

        logger.info("--> full cluster restart");
        internalCluster().fullRestart();

        IndicesShardsStoresResponse rsp = client().admin().indices().prepareShardStores(index).get();
        ImmutableOpenIntMap<List<IndicesShardsStoresResponse.ShardStoreStatus>> shardStatuses = rsp.getShardStatuses().get(index);
        assertNotNull(shardStatuses);
        assertThat(shardStatuses.size(), greaterThan(0));
        for (IntObjectCursor<List<IndicesShardsStoresResponse.ShardStoreStatus>> shardStatus : shardStatuses) {
            assertThat(shardStatus.key, isIn(corruptedShardIDs));
            for (IndicesShardsStoresResponse.ShardStoreStatus status : shardStatus.value) {
                assertThat(status.getVersion(), equalTo(-1l));
                assertThat(status.getStoreException(), notNullValue());
            }
        }
        logger.info("--> enable allocation");
        enableAllocation(index);
    }

    @Test
    public void testUnassignedShards() throws Exception {
        final String index = "test";
        internalCluster().ensureAtLeastNumDataNodes(2);
        assertAcked(prepareCreate(index).setSettings(Settings.builder()
                        .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, "2")
                        .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, "1")
        ));
        ensureGreen();
        indexRandomData(index);

        logger.info("--> disable allocation");
        disableAllocation(index);
        logger.info("--> stop random node");
        internalCluster().stopRandomNode(new IndexNonMasterNodePredicate(index, internalCluster().getMasterName()));
        ensureYellow(index);

        RoutingNodes.UnassignedShards unassigned = clusterService().state().routingNodes().unassigned();
        assertThat(unassigned.size(), greaterThan(0));
        IndicesShardsStoresResponse response = client().admin().indices().prepareShardStores(index).get();
        ImmutableOpenMap<String, ImmutableOpenIntMap<List<IndicesShardsStoresResponse.ShardStoreStatus>>> statuses = response.getShardStatuses();
        for (MutableShardRouting shardRouting : unassigned) {
            assertTrue(statuses.containsKey(shardRouting.getIndex()));
            ImmutableOpenIntMap<List<IndicesShardsStoresResponse.ShardStoreStatus>> listMap = statuses.get(shardRouting.getIndex());
            assertTrue(listMap.containsKey(shardRouting.id()));
        }
        ensureYellow();
        logger.info("--> enable allocation");
        enableAllocation(index);
    }

    @Test
    public void testSerialization() throws Exception {
        String index = "test";
        internalCluster().ensureAtMostNumDataNodes(2);
        assertAcked(prepareCreate(index).setSettings(Settings.builder()
                        .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, "2")
        ));
        ensureGreen();
        indexRandomData(index);
        IndicesShardsStoresResponse response = client().admin().indices().shardsStores(Requests.indicesShardsStoresRequest().shardState(ShardState.ALL)).get();
        XContentBuilder contentBuilder = XContentFactory.jsonBuilder();
        contentBuilder.startObject();
        response.toXContent(contentBuilder, ToXContent.EMPTY_PARAMS);
        contentBuilder.endObject();
        BytesReference bytes = contentBuilder.bytes();
        final XContentParser parser = XContentFactory.xContent(XContentType.JSON).createParser(bytes);
        Map<String, Object> map = parser.mapAndClose();
        Map<String, Object> indices = (Map<String, Object>) map.get("indices");
        assertThat(indices.containsKey("test"), equalTo(true));
        Map<String, Object> shards = ((Map<String, Object>) ((Map<String, Object>) indices.get("test")).get("shards"));
        assertThat(shards.size(), equalTo(2));
        for (String shardId : shards.keySet()) {
            HashMap shardStoresStatus = (HashMap) shards.get(shardId);
            assertThat(shardStoresStatus.containsKey("primary_allocated"), equalTo(true));
            assertThat(shardStoresStatus.containsKey("stores"), equalTo(true));
            List stores = (ArrayList) shardStoresStatus.get("stores");
            for (Object store : stores) {
                HashMap storeInfo = ((HashMap) store);
                assertThat(storeInfo.containsKey("node"), equalTo(true));
                assertThat(storeInfo.containsKey("version"), equalTo(true));
            }
        }
    }

    private void disableAllocation(String index) {
        client().admin().indices().prepareUpdateSettings(index).setSettings(Settings.builder().put(
                "index.routing.allocation.enable", "none"
        )).get();
    }

    private void enableAllocation(String index) {
        client().admin().indices().prepareUpdateSettings(index).setSettings(Settings.builder().put(
                "index.routing.allocation.enable", "all"
        )).get();
    }

    private final static class IndexNonMasterNodePredicate implements Predicate<Settings> {
        private final String masterName;
        private final Set<String> nodesWithShard;

        public IndexNonMasterNodePredicate(String index, String masterName) {
            this.masterName = masterName;
            this.nodesWithShard = findNodesWithShard(index);
        }

        @Override
        public boolean apply(Settings settings) {
            String currentNodeName = settings.get("name");
            return !currentNodeName.equals(masterName) && nodesWithShard.contains(currentNodeName);
        }

        private Set<String> findNodesWithShard(String index) {
            ClusterState state = client().admin().cluster().prepareState().get().getState();
            IndexRoutingTable indexRoutingTable = state.routingTable().index(index);
            List<ShardRouting> startedShards = indexRoutingTable.shardsWithState(ShardRoutingState.STARTED);
            Set<String> nodesWithShard = new HashSet<>();
            for (ShardRouting startedShard : startedShards) {
                nodesWithShard.add(state.nodes().get(startedShard.currentNodeId()).getName());
            }
            return nodesWithShard;
        }
    }
}
