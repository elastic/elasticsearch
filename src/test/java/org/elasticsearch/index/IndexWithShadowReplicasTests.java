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

package org.elasticsearch.index;

import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotResponse;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.RoutingNodes;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShadowIndexShard;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.snapshots.SnapshotState;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.elasticsearch.test.InternalTestCluster;
import org.junit.Test;

import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static com.google.common.collect.Lists.newArrayList;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.*;
import static org.hamcrest.Matchers.*;

/**
 * Tests for indices that use shadow replicas and a shared filesystem
 */
@ElasticsearchIntegrationTest.ClusterScope(scope = ElasticsearchIntegrationTest.Scope.TEST, numDataNodes = 0)
public class IndexWithShadowReplicasTests extends ElasticsearchIntegrationTest {

    /**
     * Tests the case where we create an index without shadow replicas, snapshot it and then restore into
     * an index with shadow replicas enabled.
     */
    public void testRestoreToShadow() throws ExecutionException, InterruptedException {
        Settings nodeSettings = ImmutableSettings.builder()
                .put("node.add_id_to_custom_path", false)
                .put("node.enable_custom_paths", true)
                .build();

        internalCluster().startNodesAsync(3, nodeSettings).get();
        final Path dataPath = createTempDir();
        Settings idxSettings = ImmutableSettings.builder()
                .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0).build();
        assertAcked(prepareCreate("foo").setSettings(idxSettings));
        ensureGreen();
        final int numDocs = randomIntBetween(10, 100);
        for (int i = 0; i < numDocs; i++) {
            client().prepareIndex("foo", "doc", ""+i).setSource("foo", "bar").get();
        }
        assertNoFailures(client().admin().indices().prepareFlush().setForce(true).setWaitIfOngoing(true).execute().actionGet());

        assertAcked(client().admin().cluster().preparePutRepository("test-repo")
                .setType("fs").setSettings(ImmutableSettings.settingsBuilder()
                        .put("location", createTempDir())));
        CreateSnapshotResponse createSnapshotResponse = client().admin().cluster().prepareCreateSnapshot("test-repo", "test-snap").setWaitForCompletion(true).setIndices("foo").get();
        assertThat(createSnapshotResponse.getSnapshotInfo().successfulShards(), greaterThan(0));
        assertThat(createSnapshotResponse.getSnapshotInfo().successfulShards(), equalTo(createSnapshotResponse.getSnapshotInfo().totalShards()));
        assertThat(client().admin().cluster().prepareGetSnapshots("test-repo").setSnapshots("test-snap").get().getSnapshots().get(0).state(), equalTo(SnapshotState.SUCCESS));

        Settings shadowSettings = ImmutableSettings.builder()
                .put(IndexMetaData.SETTING_DATA_PATH, dataPath.toAbsolutePath().toString())
                .put(IndexMetaData.SETTING_SHADOW_REPLICAS, true)
                .put(IndexMetaData.SETTING_SHARED_FILESYSTEM, true)
                .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 2).build();

        logger.info("--> restore the index into shadow replica index");
        RestoreSnapshotResponse restoreSnapshotResponse = client().admin().cluster().prepareRestoreSnapshot("test-repo", "test-snap")
                .setIndexSettings(shadowSettings).setWaitForCompletion(true)
                .setRenamePattern("(.+)").setRenameReplacement("$1-copy")
                .execute().actionGet();
        assertThat(restoreSnapshotResponse.getRestoreInfo().totalShards(), greaterThan(0));
        ensureGreen();
        refresh();

        for (IndicesService service : internalCluster().getDataNodeInstances(IndicesService.class)) {
            if (service.hasIndex("foo-copy")) {
                IndexShard shard = service.indexServiceSafe("foo-copy").shard(0);
                if (shard.routingEntry().primary()) {
                    assertFalse(shard instanceof ShadowIndexShard);
                } else {
                    assertTrue(shard instanceof ShadowIndexShard);
                }
            }
        }
        logger.info("--> performing query");
        SearchResponse resp = client().prepareSearch("foo-copy").setQuery(matchAllQuery()).get();
        assertHitCount(resp, numDocs);

    }

    @Test
    public void testIndexWithFewDocuments() throws Exception {
        Settings nodeSettings = ImmutableSettings.builder()
                .put("node.add_id_to_custom_path", false)
                .put("node.enable_custom_paths", true)
                .build();

        internalCluster().startNodesAsync(3, nodeSettings).get();
        final String IDX = "test";
        final Path dataPath = createTempDir();

        Settings idxSettings = ImmutableSettings.builder()
                .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 2)
                .put(IndexMetaData.SETTING_DATA_PATH, dataPath.toAbsolutePath().toString())
                .put(IndexMetaData.SETTING_SHADOW_REPLICAS, true)
                .put(IndexMetaData.SETTING_SHARED_FILESYSTEM, true)
                .build();

        prepareCreate(IDX).setSettings(idxSettings).addMapping("doc", "foo", "type=string").get();
        ensureGreen(IDX);

        // So basically, the primary should fail and the replica will need to
        // replay the translog, this is what this tests
        client().prepareIndex(IDX, "doc", "1").setSource("foo", "bar").get();
        client().prepareIndex(IDX, "doc", "2").setSource("foo", "bar").get();

        // Check that we can get doc 1 and 2, because we are doing realtime
        // gets and getting from the primary
        GetResponse gResp1 = client().prepareGet(IDX, "doc", "1").setRealtime(true).setFields("foo").get();
        GetResponse gResp2 = client().prepareGet(IDX, "doc", "2").setRealtime(true).setFields("foo").get();
        assertThat(gResp1.getField("foo").getValue().toString(), equalTo("bar"));
        assertThat(gResp2.getField("foo").getValue().toString(), equalTo("bar"));

        flushAndRefresh(IDX);
        client().prepareIndex(IDX, "doc", "3").setSource("foo", "bar").get();
        client().prepareIndex(IDX, "doc", "4").setSource("foo", "bar").get();
        refresh();

        // Check that we can get doc 1 and 2 without realtime
        gResp1 = client().prepareGet(IDX, "doc", "1").setRealtime(false).setFields("foo").get();
        gResp2 = client().prepareGet(IDX, "doc", "2").setRealtime(false).setFields("foo").get();
        assertThat(gResp1.getField("foo").getValue().toString(), equalTo("bar"));
        assertThat(gResp2.getField("foo").getValue().toString(), equalTo("bar"));

        logger.info("--> restarting all nodes");
        if (randomBoolean()) {
            logger.info("--> rolling restart");
            internalCluster().rollingRestart();
        } else {
            logger.info("--> full restart");
            internalCluster().fullRestart();
        }

        client().admin().cluster().prepareHealth().setWaitForNodes("3").get();
        ensureGreen(IDX);
        flushAndRefresh(IDX);

        logger.info("--> performing query");
        SearchResponse resp = client().prepareSearch(IDX).setQuery(matchAllQuery()).get();
        assertHitCount(resp, 4);

        logger.info("--> deleting index");
        assertAcked(client().admin().indices().prepareDelete(IDX));
    }

    @Test
    public void testReplicaToPrimaryPromotion() throws Exception {
        Settings nodeSettings = ImmutableSettings.builder()
                .put("node.add_id_to_custom_path", false)
                .put("node.enable_custom_paths", true)
                .build();

        String node1 = internalCluster().startNode(nodeSettings);
        Path dataPath = createTempDir();
        String IDX = "test";

        Settings idxSettings = ImmutableSettings.builder()
                .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 1)
                .put(IndexMetaData.SETTING_DATA_PATH, dataPath.toAbsolutePath().toString())
                .put(IndexMetaData.SETTING_SHADOW_REPLICAS, true)
                .put(IndexMetaData.SETTING_SHARED_FILESYSTEM, true)
                .build();

        prepareCreate(IDX).setSettings(idxSettings).addMapping("doc", "foo", "type=string").get();
        ensureYellow(IDX);
        client().prepareIndex(IDX, "doc", "1").setSource("foo", "bar").get();
        client().prepareIndex(IDX, "doc", "2").setSource("foo", "bar").get();

        GetResponse gResp1 = client().prepareGet(IDX, "doc", "1").setFields("foo").get();
        GetResponse gResp2 = client().prepareGet(IDX, "doc", "2").setFields("foo").get();
        assertTrue(gResp1.isExists());
        assertTrue(gResp2.isExists());
        assertThat(gResp1.getField("foo").getValue().toString(), equalTo("bar"));
        assertThat(gResp2.getField("foo").getValue().toString(), equalTo("bar"));

        // Node1 has the primary, now node2 has the replica
        String node2 = internalCluster().startNode(nodeSettings);
        ensureGreen(IDX);
        client().admin().cluster().prepareHealth().setWaitForNodes("2").get();
        flushAndRefresh(IDX);

        logger.info("--> stopping node1 [{}]", node1);
        internalCluster().stopRandomNode(InternalTestCluster.nameFilter(node1));
        ensureYellow(IDX);

        logger.info("--> performing query");
        SearchResponse resp = client().prepareSearch(IDX).setQuery(matchAllQuery()).get();
        assertHitCount(resp, 2);

        gResp1 = client().prepareGet(IDX, "doc", "1").setFields("foo").get();
        gResp2 = client().prepareGet(IDX, "doc", "2").setFields("foo").get();
        assertTrue(gResp1.isExists());
        assertTrue(gResp2.toString(), gResp2.isExists());
        assertThat(gResp1.getField("foo").getValue().toString(), equalTo("bar"));
        assertThat(gResp2.getField("foo").getValue().toString(), equalTo("bar"));

        client().prepareIndex(IDX, "doc", "1").setSource("foo", "foobar").get();
        client().prepareIndex(IDX, "doc", "2").setSource("foo", "foobar").get();
        gResp1 = client().prepareGet(IDX, "doc", "1").setFields("foo").get();
        gResp2 = client().prepareGet(IDX, "doc", "2").setFields("foo").get();
        assertTrue(gResp1.isExists());
        assertTrue(gResp2.toString(), gResp2.isExists());
        assertThat(gResp1.getField("foo").getValue().toString(), equalTo("foobar"));
        assertThat(gResp2.getField("foo").getValue().toString(), equalTo("foobar"));
    }

    @Test
    public void testPrimaryRelocation() throws Exception {
        Settings nodeSettings = ImmutableSettings.builder()
                .put("node.add_id_to_custom_path", false)
                .put("node.enable_custom_paths", true)
                .build();

        String node1 = internalCluster().startNode(nodeSettings);
        Path dataPath = createTempDir();
        String IDX = "test";

        Settings idxSettings = ImmutableSettings.builder()
                .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 1)
                .put(IndexMetaData.SETTING_DATA_PATH, dataPath.toAbsolutePath().toString())
                .put(IndexMetaData.SETTING_SHADOW_REPLICAS, true)
                .put(IndexMetaData.SETTING_SHARED_FILESYSTEM, true)
                .build();

        prepareCreate(IDX).setSettings(idxSettings).addMapping("doc", "foo", "type=string").get();
        ensureYellow(IDX);
        client().prepareIndex(IDX, "doc", "1").setSource("foo", "bar").get();
        client().prepareIndex(IDX, "doc", "2").setSource("foo", "bar").get();

        GetResponse gResp1 = client().prepareGet(IDX, "doc", "1").setFields("foo").get();
        GetResponse gResp2 = client().prepareGet(IDX, "doc", "2").setFields("foo").get();
        assertTrue(gResp1.isExists());
        assertTrue(gResp2.isExists());
        assertThat(gResp1.getField("foo").getValue().toString(), equalTo("bar"));
        assertThat(gResp2.getField("foo").getValue().toString(), equalTo("bar"));

        // Node1 has the primary, now node2 has the replica
        String node2 = internalCluster().startNode(nodeSettings);
        ensureGreen(IDX);
        client().admin().cluster().prepareHealth().setWaitForNodes("2").get();
        flushAndRefresh(IDX);

        // now prevent primary from being allocated on node 1 move to node_3
        String node3 = internalCluster().startNode(nodeSettings);
        Settings build = ImmutableSettings.builder().put("index.routing.allocation.exclude._name", node1).build();
        client().admin().indices().prepareUpdateSettings(IDX).setSettings(build).execute().actionGet();

        ensureGreen(IDX);
        logger.info("--> performing query");
        SearchResponse resp = client().prepareSearch(IDX).setQuery(matchAllQuery()).get();
        assertHitCount(resp, 2);

        gResp1 = client().prepareGet(IDX, "doc", "1").setFields("foo").get();
        gResp2 = client().prepareGet(IDX, "doc", "2").setFields("foo").get();
        assertTrue(gResp1.isExists());
        assertTrue(gResp2.toString(), gResp2.isExists());
        assertThat(gResp1.getField("foo").getValue().toString(), equalTo("bar"));
        assertThat(gResp2.getField("foo").getValue().toString(), equalTo("bar"));

        client().prepareIndex(IDX, "doc", "3").setSource("foo", "bar").get();
        client().prepareIndex(IDX, "doc", "4").setSource("foo", "bar").get();
        gResp1 = client().prepareGet(IDX, "doc", "3").setPreference("_primary").setFields("foo").get();
        gResp2 = client().prepareGet(IDX, "doc", "4").setPreference("_primary").setFields("foo").get();
        assertTrue(gResp1.isExists());
        assertTrue(gResp2.isExists());
        assertThat(gResp1.getField("foo").getValue().toString(), equalTo("bar"));
        assertThat(gResp2.getField("foo").getValue().toString(), equalTo("bar"));
    }

    @Test
    public void testIndexWithShadowReplicasCleansUp() throws Exception {
        Settings nodeSettings = ImmutableSettings.builder()
                .put("node.add_id_to_custom_path", false)
                .put("node.enable_custom_paths", true)
                .build();

        int nodeCount = randomIntBetween(2, 5);
        internalCluster().startNodesAsync(nodeCount, nodeSettings).get();
        Path dataPath = createTempDir();
        String IDX = "test";

        Settings idxSettings = ImmutableSettings.builder()
                .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, randomIntBetween(1, nodeCount - 1))
                .put(IndexMetaData.SETTING_DATA_PATH, dataPath.toAbsolutePath().toString())
                .put(IndexMetaData.SETTING_SHADOW_REPLICAS, true)
                .put(IndexMetaData.SETTING_SHARED_FILESYSTEM, true)
                .build();

        prepareCreate(IDX).setSettings(idxSettings).addMapping("doc", "foo", "type=string").get();
        ensureGreen(IDX);
        client().prepareIndex(IDX, "doc", "1").setSource("foo", "bar").get();
        client().prepareIndex(IDX, "doc", "2").setSource("foo", "bar").get();
        flushAndRefresh(IDX);

        GetResponse gResp1 = client().prepareGet(IDX, "doc", "1").setFields("foo").get();
        GetResponse gResp2 = client().prepareGet(IDX, "doc", "2").setFields("foo").get();
        assertThat(gResp1.getField("foo").getValue().toString(), equalTo("bar"));
        assertThat(gResp2.getField("foo").getValue().toString(), equalTo("bar"));

        logger.info("--> performing query");
        SearchResponse resp = client().prepareSearch(IDX).setQuery(matchAllQuery()).get();
        assertHitCount(resp, 2);

        assertAcked(client().admin().indices().prepareDelete(IDX));

        assertPathHasBeenCleared(dataPath);
    }

    /**
     * Tests that shadow replicas can be "naturally" rebalanced and relocated
     * around the cluster. By "naturally" I mean without using the reroute API
     * @throws Exception
     */
    @Test
    public void testShadowReplicaNaturalRelocation() throws Exception {
        Settings nodeSettings = ImmutableSettings.builder()
                .put("node.add_id_to_custom_path", false)
                .put("node.enable_custom_paths", true)
                .build();

        internalCluster().startNodesAsync(2, nodeSettings).get();
        Path dataPath = createTempDir();
        String IDX = "test";

        Settings idxSettings = ImmutableSettings.builder()
                .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 5)
                .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 1)
                .put(IndexMetaData.SETTING_DATA_PATH, dataPath.toAbsolutePath().toString())
                .put(IndexMetaData.SETTING_SHADOW_REPLICAS, true)
                .put(IndexMetaData.SETTING_SHARED_FILESYSTEM, true)
                .build();

        prepareCreate(IDX).setSettings(idxSettings).addMapping("doc", "foo", "type=string").get();
        ensureGreen(IDX);

        int docCount = randomIntBetween(10, 100);
        List<IndexRequestBuilder> builders = newArrayList();
        for (int i = 0; i < docCount; i++) {
            builders.add(client().prepareIndex(IDX, "doc", i + "").setSource("foo", "bar"));
        }
        indexRandom(true, true, true, builders);
        flushAndRefresh(IDX);

        // start a third node, with 5 shards each on the other nodes, they
        // should relocate some to the third node
        final String node3 = internalCluster().startNode(nodeSettings);

        assertBusy(new Runnable() {
            @Override
            public void run() {
                client().admin().cluster().prepareHealth().setWaitForNodes("3").get();
                ClusterStateResponse resp = client().admin().cluster().prepareState().get();
                RoutingNodes nodes = resp.getState().getRoutingNodes();
                for (RoutingNode node : nodes) {
                    logger.info("--> node has {} shards (needs at least 2)", node.numberOfOwningShards());
                    assertThat("at least 2 shards on node", node.numberOfOwningShards(), greaterThanOrEqualTo(2));
                }
            }
        });
        ensureYellow(IDX);

        logger.info("--> performing query");
        SearchResponse resp = client().prepareSearch(IDX).setQuery(matchAllQuery()).get();
        assertHitCount(resp, docCount);

        assertAcked(client().admin().indices().prepareDelete(IDX));

        assertPathHasBeenCleared(dataPath);
    }

    @Test
    public void testShadowReplicasUsingFieldData() throws Exception {
        Settings nodeSettings = ImmutableSettings.builder()
                .put("node.add_id_to_custom_path", false)
                .put("node.enable_custom_paths", true)
                .build();

        internalCluster().startNodesAsync(3, nodeSettings).get();
        Path dataPath = createTempDir();
        String IDX = "test";

        Settings idxSettings = ImmutableSettings.builder()
                .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 2)
                .put(IndexMetaData.SETTING_DATA_PATH, dataPath.toAbsolutePath().toString())
                .put(IndexMetaData.SETTING_SHADOW_REPLICAS, true)
                .put(IndexMetaData.SETTING_SHARED_FILESYSTEM, true)
                .build();

        prepareCreate(IDX).setSettings(idxSettings).addMapping("doc", "foo", "type=string,index=not_analyzed").get();
        ensureGreen(IDX);

        client().prepareIndex(IDX, "doc", "1").setSource("foo", "foo").get();
        client().prepareIndex(IDX, "doc", "2").setSource("foo", "bar").get();
        client().prepareIndex(IDX, "doc", "3").setSource("foo", "baz").get();
        client().prepareIndex(IDX, "doc", "4").setSource("foo", "eggplant").get();
        flushAndRefresh(IDX);

        SearchResponse resp = client().prepareSearch(IDX).setQuery(matchAllQuery()).addFieldDataField("foo").addSort("foo", SortOrder.ASC).get();
        assertHitCount(resp, 4);
        assertOrderedSearchHits(resp, "2", "3", "4", "1");
        SearchHit[] hits = resp.getHits().hits();
        assertThat(hits[0].field("foo").getValue().toString(), equalTo("bar"));
        assertThat(hits[1].field("foo").getValue().toString(), equalTo("baz"));
        assertThat(hits[2].field("foo").getValue().toString(), equalTo("eggplant"));
        assertThat(hits[3].field("foo").getValue().toString(), equalTo("foo"));
    }
}
