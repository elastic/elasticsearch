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
package org.elasticsearch.indices;

import org.apache.lucene.store.LockObtainFailedException;
import org.elasticsearch.Version;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.AliasAction;
import org.elasticsearch.cluster.metadata.IndexGraveyard;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.io.FileSystemUtils;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.gateway.GatewayMetaState;
import org.elasticsearch.gateway.LocalAllocateDangledIndices;
import org.elasticsearch.gateway.MetaStateService;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardPath;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.test.IndexSettingsModule;

import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.action.support.WriteRequest.RefreshPolicy.IMMEDIATE;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

public class IndicesServiceTests extends ESSingleNodeTestCase {

    public IndicesService getIndicesService() {
        return getInstanceFromNode(IndicesService.class);
    }

    public NodeEnvironment getNodeEnvironment() {
        return getInstanceFromNode(NodeEnvironment.class);
    }

    @Override
    protected boolean resetNodeAfterTest() {
        return true;
    }

    public void testCanDeleteIndexContent() throws IOException {
        final IndicesService indicesService = getIndicesService();
        IndexSettings idxSettings = IndexSettingsModule.newIndexSettings("test", Settings.builder()
                .put(IndexMetaData.SETTING_SHADOW_REPLICAS, true)
                .put(IndexMetaData.SETTING_DATA_PATH, "/foo/bar")
                .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, randomIntBetween(1, 4))
                .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, randomIntBetween(0, 3))
                .build());
        assertFalse("shard on shared filesystem", indicesService.canDeleteIndexContents(idxSettings.getIndex(), idxSettings));

        final IndexMetaData.Builder newIndexMetaData = IndexMetaData.builder(idxSettings.getIndexMetaData());
        newIndexMetaData.state(IndexMetaData.State.CLOSE);
        idxSettings = IndexSettingsModule.newIndexSettings(newIndexMetaData.build());
        assertTrue("shard on shared filesystem, but closed, so it should be deletable",
            indicesService.canDeleteIndexContents(idxSettings.getIndex(), idxSettings));
    }

    public void testCanDeleteShardContent() {
        IndicesService indicesService = getIndicesService();
        IndexMetaData meta = IndexMetaData.builder("test").settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(
                1).build();
        IndexSettings indexSettings = IndexSettingsModule.newIndexSettings("test", meta.getSettings());
        ShardId shardId = new ShardId(meta.getIndex(), 0);
        assertFalse("no shard location", indicesService.canDeleteShardContent(shardId, indexSettings));
        IndexService test = createIndex("test");
        shardId = new ShardId(test.index(), 0);
        assertTrue(test.hasShard(0));
        assertFalse("shard is allocated", indicesService.canDeleteShardContent(shardId, test.getIndexSettings()));
        test.removeShard(0, "boom");
        assertTrue("shard is removed", indicesService.canDeleteShardContent(shardId, test.getIndexSettings()));
        ShardId notAllocated = new ShardId(test.index(), 100);
        assertFalse("shard that was never on this node should NOT be deletable",
            indicesService.canDeleteShardContent(notAllocated, test.getIndexSettings()));
    }

    public void testDeleteIndexStore() throws Exception {
        IndicesService indicesService = getIndicesService();
        IndexService test = createIndex("test");
        ClusterService clusterService = getInstanceFromNode(ClusterService.class);
        IndexMetaData firstMetaData = clusterService.state().metaData().index("test");
        assertTrue(test.hasShard(0));

        try {
            indicesService.deleteIndexStore("boom", firstMetaData, clusterService.state());
            fail();
        } catch (IllegalStateException ex) {
            // all good
        }

        GatewayMetaState gwMetaState = getInstanceFromNode(GatewayMetaState.class);
        MetaData meta = gwMetaState.loadMetaState();
        assertNotNull(meta);
        assertNotNull(meta.index("test"));
        assertAcked(client().admin().indices().prepareDelete("test"));

        meta = gwMetaState.loadMetaState();
        assertNotNull(meta);
        assertNull(meta.index("test"));


        test = createIndex("test");
        client().prepareIndex("test", "type", "1").setSource("field", "value").setRefreshPolicy(IMMEDIATE).get();
        client().admin().indices().prepareFlush("test").get();
        assertHitCount(client().prepareSearch("test").get(), 1);
        IndexMetaData secondMetaData = clusterService.state().metaData().index("test");
        assertAcked(client().admin().indices().prepareClose("test"));
        ShardPath path = ShardPath.loadShardPath(logger, getNodeEnvironment(), new ShardId(test.index(), 0), test.getIndexSettings());
        assertTrue(path.exists());

        try {
            indicesService.deleteIndexStore("boom", secondMetaData, clusterService.state());
            fail();
        } catch (IllegalStateException ex) {
            // all good
        }

        assertTrue(path.exists());

        // now delete the old one and make sure we resolve against the name
        try {
            indicesService.deleteIndexStore("boom", firstMetaData, clusterService.state());
            fail();
        } catch (IllegalStateException ex) {
            // all good
        }
        assertAcked(client().admin().indices().prepareOpen("test"));
        ensureGreen("test");
    }

    public void testPendingTasks() throws Exception {
        IndicesService indicesService = getIndicesService();
        IndexService test = createIndex("test");

        assertTrue(test.hasShard(0));
        ShardPath path = test.getShardOrNull(0).shardPath();
        assertTrue(test.getShardOrNull(0).routingEntry().started());
        ShardPath shardPath = ShardPath.loadShardPath(logger, getNodeEnvironment(), new ShardId(test.index(), 0), test.getIndexSettings());
        assertEquals(shardPath, path);
        try {
            indicesService.processPendingDeletes(test.index(), test.getIndexSettings(), new TimeValue(0, TimeUnit.MILLISECONDS));
            fail("can't get lock");
        } catch (LockObtainFailedException ex) {

        }
        assertTrue(path.exists());

        int numPending = 1;
        if (randomBoolean()) {
            indicesService.addPendingDelete(new ShardId(test.index(), 0), test.getIndexSettings());
        } else {
            if (randomBoolean()) {
                numPending++;
                indicesService.addPendingDelete(new ShardId(test.index(), 0), test.getIndexSettings());
            }
            indicesService.addPendingDelete(test.index(), test.getIndexSettings());
        }
        assertAcked(client().admin().indices().prepareClose("test"));
        assertTrue(path.exists());

        assertEquals(indicesService.numPendingDeletes(test.index()), numPending);
        assertTrue(indicesService.hasUncompletedPendingDeletes());

        // shard lock released... we can now delete
        indicesService.processPendingDeletes(test.index(), test.getIndexSettings(), new TimeValue(0, TimeUnit.MILLISECONDS));
        assertEquals(indicesService.numPendingDeletes(test.index()), 0);
        assertFalse(indicesService.hasUncompletedPendingDeletes());
        assertFalse(path.exists());

        if (randomBoolean()) {
            indicesService.addPendingDelete(new ShardId(test.index(), 0), test.getIndexSettings());
            indicesService.addPendingDelete(new ShardId(test.index(), 1), test.getIndexSettings());
            indicesService.addPendingDelete(new ShardId("bogus", "_na_", 1), test.getIndexSettings());
            assertEquals(indicesService.numPendingDeletes(test.index()), 2);
            assertTrue(indicesService.hasUncompletedPendingDeletes());
            // shard lock released... we can now delete
            indicesService.processPendingDeletes(test.index(), test.getIndexSettings(), new TimeValue(0, TimeUnit.MILLISECONDS));
            assertEquals(indicesService.numPendingDeletes(test.index()), 0);
            assertTrue(indicesService.hasUncompletedPendingDeletes()); // "bogus" index has not been removed
        }
        assertAcked(client().admin().indices().prepareOpen("test"));

    }

    public void testVerifyIfIndexContentDeleted() throws Exception {
        final Index index = new Index("test", UUIDs.randomBase64UUID());
        final IndicesService indicesService = getIndicesService();
        final NodeEnvironment nodeEnv = getNodeEnvironment();
        final MetaStateService metaStateService = getInstanceFromNode(MetaStateService.class);

        final ClusterService clusterService = getInstanceFromNode(ClusterService.class);
        final Settings idxSettings = Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT)
                                                        .put(IndexMetaData.SETTING_INDEX_UUID, index.getUUID())
                                                        .build();
        final IndexMetaData indexMetaData = new IndexMetaData.Builder(index.getName())
                                                             .settings(idxSettings)
                                                             .numberOfShards(1)
                                                             .numberOfReplicas(0)
                                                             .build();
        metaStateService.writeIndex("test index being created", indexMetaData);
        final MetaData metaData = MetaData.builder(clusterService.state().metaData()).put(indexMetaData, true).build();
        final ClusterState csWithIndex = new ClusterState.Builder(clusterService.state()).metaData(metaData).build();
        try {
            indicesService.verifyIndexIsDeleted(index, csWithIndex);
            fail("Should not be able to delete index contents when the index is part of the cluster state.");
        } catch (IllegalStateException e) {
            assertThat(e.getMessage(), containsString("Cannot delete index"));
        }

        final ClusterState withoutIndex = new ClusterState.Builder(csWithIndex)
                                                          .metaData(MetaData.builder(csWithIndex.metaData()).remove(index.getName()))
                                                          .build();
        indicesService.verifyIndexIsDeleted(index, withoutIndex);
        assertFalse("index files should be deleted", FileSystemUtils.exists(nodeEnv.indexPaths(index)));
    }

    public void testDanglingIndicesWithAliasConflict() throws Exception {
        final String indexName = "test-idx1";
        final String alias = "test-alias";
        final IndicesService indicesService = getIndicesService();
        final ClusterService clusterService = getInstanceFromNode(ClusterService.class);
        final IndexService test = createIndex(indexName);

        // create the alias for the index
        AliasAction action = new AliasAction(AliasAction.Type.ADD, indexName, alias);
        IndicesAliasesRequest request = new IndicesAliasesRequest().addAliasAction(action);
        client().admin().indices().aliases(request).actionGet();
        final ClusterState originalState = clusterService.state();

        // try to import a dangling index with the same name as the alias, it should fail
        final LocalAllocateDangledIndices dangling = getInstanceFromNode(LocalAllocateDangledIndices.class);
        final Settings idxSettings = Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT)
                                                       .put(IndexMetaData.SETTING_INDEX_UUID, UUIDs.randomBase64UUID())
                                                       .build();
        final IndexMetaData indexMetaData = new IndexMetaData.Builder(alias)
                                                             .settings(idxSettings)
                                                             .numberOfShards(1)
                                                             .numberOfReplicas(0)
                                                             .build();
        DanglingListener listener = new DanglingListener();
        dangling.allocateDangled(Arrays.asList(indexMetaData), listener);
        listener.latch.await();
        assertThat(clusterService.state(), equalTo(originalState));

        // remove the alias
        action = new AliasAction(AliasAction.Type.REMOVE, indexName, alias);
        request = new IndicesAliasesRequest().addAliasAction(action);
        client().admin().indices().aliases(request).actionGet();

        // now try importing a dangling index with the same name as the alias, it should succeed.
        listener = new DanglingListener();
        dangling.allocateDangled(Arrays.asList(indexMetaData), listener);
        listener.latch.await();
        assertThat(clusterService.state(), not(originalState));
        assertNotNull(clusterService.state().getMetaData().index(alias));

        // cleanup
        indicesService.deleteIndex(test.index(), "finished with test");
    }

    /**
     * This test checks an edge case where, if a node had an index (lets call it A with UUID 1), then
     * deleted it (so a tombstone entry for A will exist in the cluster state), then created
     * a new index A with UUID 2, then shutdown, when the node comes back online, it will look at the
     * tombstones for deletions, and it should proceed with trying to delete A with UUID 1 and not
     * throw any errors that the index still exists in the cluster state.  This is a case of ensuring
     * that tombstones that have the same name as current valid indices don't cause confusion by
     * trying to delete an index that exists.
     * See https://github.com/elastic/elasticsearch/issues/18054
     */
    public void testIndexAndTombstoneWithSameNameOnStartup() throws Exception {
        final String indexName = "test";
        final Index index = new Index(indexName, UUIDs.randomBase64UUID());
        final IndicesService indicesService = getIndicesService();
        final Settings idxSettings = Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT)
                                         .put(IndexMetaData.SETTING_INDEX_UUID, index.getUUID())
                                         .build();
        final IndexMetaData indexMetaData = new IndexMetaData.Builder(index.getName())
                                                .settings(idxSettings)
                                                .numberOfShards(1)
                                                .numberOfReplicas(0)
                                                .build();
        final Index tombstonedIndex = new Index(indexName, UUIDs.randomBase64UUID());
        final IndexGraveyard graveyard = IndexGraveyard.builder().addTombstone(tombstonedIndex).build();
        final MetaData metaData = MetaData.builder().put(indexMetaData, true).indexGraveyard(graveyard).build();
        final ClusterState clusterState = new ClusterState.Builder(new ClusterName("testCluster")).metaData(metaData).build();
        // if all goes well, this won't throw an exception, otherwise, it will throw an IllegalStateException
        indicesService.verifyIndexIsDeleted(tombstonedIndex, clusterState);
    }

    private static class DanglingListener implements LocalAllocateDangledIndices.Listener {
        final CountDownLatch latch = new CountDownLatch(1);

        @Override
        public void onResponse(LocalAllocateDangledIndices.AllocateDangledResponse response) {
            latch.countDown();
        }

        @Override
        public void onFailure(Throwable e) {
            latch.countDown();
        }
    }

}
