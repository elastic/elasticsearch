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
package org.elasticsearch.index.shard;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.LockObtainFailedException;
import org.apache.lucene.util.Constants;
import org.apache.lucene.util.IOUtils;
import org.elasticsearch.Version;
import org.elasticsearch.action.admin.indices.stats.CommonStats;
import org.elasticsearch.action.admin.indices.stats.CommonStatsFlags;
import org.elasticsearch.action.admin.indices.stats.IndexStats;
import org.elasticsearch.action.admin.indices.stats.ShardStats;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.ClusterInfoService;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.InternalClusterInfoService;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.env.ShardLock;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.indexing.IndexingOperationListener;
import org.elasticsearch.index.indexing.ShardIndexingService;
import org.elasticsearch.index.mapper.Mapping;
import org.elasticsearch.index.mapper.ParseContext;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.mapper.internal.UidFieldMapper;
import org.elasticsearch.index.query.QueryParsingException;
import org.elasticsearch.index.settings.IndexSettingsService;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.index.translog.TranslogConfig;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.test.DummyShardLock;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.test.VersionUtils;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.elasticsearch.cluster.metadata.IndexMetaData.EMPTY_PARAMS;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_REPLICAS;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_SHARDS;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_VERSION_CREATED;
import static org.elasticsearch.common.settings.Settings.settingsBuilder;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.hamcrest.Matchers.equalTo;

/**
 * Simple unit-test IndexShard related operations.
 */
public class IndexShardTests extends ESSingleNodeTestCase {

    public void testFlushOnDeleteSetting() throws Exception {
        boolean initValue = randomBoolean();
        createIndex("test", settingsBuilder().put(IndexShard.INDEX_FLUSH_ON_CLOSE, initValue).build());
        ensureGreen();
        IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        IndexService test = indicesService.indexService("test");
        IndexShard shard = test.shard(0);
        assertEquals(initValue, shard.isFlushOnClose());
        final boolean newValue = !initValue;
        assertAcked(client().admin().indices().prepareUpdateSettings("test").setSettings(settingsBuilder().put(IndexShard.INDEX_FLUSH_ON_CLOSE, newValue).build()));
        assertEquals(newValue, shard.isFlushOnClose());

        try {
            assertAcked(client().admin().indices().prepareUpdateSettings("test").setSettings(settingsBuilder().put(IndexShard.INDEX_FLUSH_ON_CLOSE, "FOOBAR").build()));
            fail("exception expected");
        } catch (IllegalArgumentException ex) {

        }
        assertEquals(newValue, shard.isFlushOnClose());

    }

    public void testWriteShardState() throws Exception {
        try (NodeEnvironment env = newNodeEnvironment()) {
            ShardId id = new ShardId("foo", 1);
            long version = between(1, Integer.MAX_VALUE / 2);
            boolean primary = randomBoolean();
            ShardStateMetaData state1 = new ShardStateMetaData(version, primary, "foo");
            write(state1, env.availableShardPaths(id));
            ShardStateMetaData shardStateMetaData = load(logger, env.availableShardPaths(id));
            assertEquals(shardStateMetaData, state1);

            ShardStateMetaData state2 = new ShardStateMetaData(version, primary, "foo");
            write(state2, env.availableShardPaths(id));
            shardStateMetaData = load(logger, env.availableShardPaths(id));
            assertEquals(shardStateMetaData, state1);

            ShardStateMetaData state3 = new ShardStateMetaData(version + 1, primary, "foo");
            write(state3, env.availableShardPaths(id));
            shardStateMetaData = load(logger, env.availableShardPaths(id));
            assertEquals(shardStateMetaData, state3);
            assertEquals("foo", state3.indexUUID);
        }
    }

    @Test
    public void testLockTryingToDelete() throws Exception {
        createIndex("test");
        ensureGreen();
        NodeEnvironment env = getInstanceFromNode(NodeEnvironment.class);
        Path[] shardPaths = env.availableShardPaths(new ShardId("test", 0));
        logger.info("--> paths: [{}]", (Object)shardPaths);
        // Should not be able to acquire the lock because it's already open
        try {
            NodeEnvironment.acquireFSLockForPaths(Settings.EMPTY, shardPaths);
            fail("should not have been able to acquire the lock");
        } catch (LockObtainFailedException e) {
            assertTrue("msg: " + e.getMessage(), e.getMessage().contains("unable to acquire write.lock"));
        }
        // Test without the regular shard lock to assume we can acquire it
        // (worst case, meaning that the shard lock could be acquired and
        // we're green to delete the shard's directory)
        ShardLock sLock = new DummyShardLock(new ShardId("test", 0));
        try {
            env.deleteShardDirectoryUnderLock(sLock, Settings.builder().build());
            fail("should not have been able to delete the directory");
        } catch (LockObtainFailedException e) {
            assertTrue("msg: " + e.getMessage(), e.getMessage().contains("unable to acquire write.lock"));
        }
    }

    public void testPersistenceStateMetadataPersistence() throws Exception {
        createIndex("test");
        ensureGreen();
        IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        NodeEnvironment env = getInstanceFromNode(NodeEnvironment.class);
        IndexService test = indicesService.indexService("test");
        IndexShard shard = test.shard(0);
        ShardStateMetaData shardStateMetaData = load(logger, env.availableShardPaths(shard.shardId));
        assertEquals(getShardStateMetadata(shard), shardStateMetaData);
        ShardRouting routing = new ShardRouting(shard.shardRouting, shard.shardRouting.version() + 1);
        shard.updateRoutingEntry(routing, true);

        shardStateMetaData = load(logger, env.availableShardPaths(shard.shardId));
        assertEquals(shardStateMetaData, getShardStateMetadata(shard));
        assertEquals(shardStateMetaData, new ShardStateMetaData(routing.version(), routing.primary(), shard.indexSettings.get(IndexMetaData.SETTING_INDEX_UUID)));

        routing = new ShardRouting(shard.shardRouting, shard.shardRouting.version() + 1);
        shard.updateRoutingEntry(routing, true);
        shardStateMetaData = load(logger, env.availableShardPaths(shard.shardId));
        assertEquals(shardStateMetaData, getShardStateMetadata(shard));
        assertEquals(shardStateMetaData, new ShardStateMetaData(routing.version(), routing.primary(), shard.indexSettings.get(IndexMetaData.SETTING_INDEX_UUID)));

        routing = new ShardRouting(shard.shardRouting, shard.shardRouting.version() + 1);
        shard.updateRoutingEntry(routing, true);
        shardStateMetaData = load(logger, env.availableShardPaths(shard.shardId));
        assertEquals(shardStateMetaData, getShardStateMetadata(shard));
        assertEquals(shardStateMetaData, new ShardStateMetaData(routing.version(), routing.primary(), shard.indexSettings.get(IndexMetaData.SETTING_INDEX_UUID)));

        // test if we still write it even if the shard is not active
        ShardRouting inactiveRouting = TestShardRouting.newShardRouting(shard.shardRouting.index(), shard.shardRouting.shardId().id(), shard.shardRouting.currentNodeId(), null, null, true, ShardRoutingState.INITIALIZING, shard.shardRouting.version() + 1);
        shard.persistMetadata(inactiveRouting, shard.shardRouting);
        shardStateMetaData = load(logger, env.availableShardPaths(shard.shardId));
        assertEquals("inactive shard state shouldn't be persisted", shardStateMetaData, getShardStateMetadata(shard));
        assertEquals("inactive shard state shouldn't be persisted", shardStateMetaData, new ShardStateMetaData(routing.version(), routing.primary(), shard.indexSettings.get(IndexMetaData.SETTING_INDEX_UUID)));


        shard.updateRoutingEntry(new ShardRouting(shard.shardRouting, shard.shardRouting.version() + 1), false);
        shardStateMetaData = load(logger, env.availableShardPaths(shard.shardId));
        assertFalse("shard state persisted despite of persist=false", shardStateMetaData.equals(getShardStateMetadata(shard)));
        assertEquals("shard state persisted despite of persist=false", shardStateMetaData, new ShardStateMetaData(routing.version(), routing.primary(), shard.indexSettings.get(IndexMetaData.SETTING_INDEX_UUID)));


        routing = new ShardRouting(shard.shardRouting, shard.shardRouting.version() + 1);
        shard.updateRoutingEntry(routing, true);
        shardStateMetaData = load(logger, env.availableShardPaths(shard.shardId));
        assertEquals(shardStateMetaData, getShardStateMetadata(shard));
        assertEquals(shardStateMetaData, new ShardStateMetaData(routing.version(), routing.primary(), shard.indexSettings.get(IndexMetaData.SETTING_INDEX_UUID)));
    }

    public void testDeleteShardState() throws IOException {
        createIndex("test");
        ensureGreen();
        IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        NodeEnvironment env = getInstanceFromNode(NodeEnvironment.class);
        IndexService test = indicesService.indexService("test");
        IndexShard shard = test.shard(0);
        try {
            shard.deleteShardState();
            fail("shard is active metadata delete must fail");
        } catch (IllegalStateException ex) {
            // fine - only delete if non-active
        }

        ShardRouting routing = shard.routingEntry();
        ShardStateMetaData shardStateMetaData = load(logger, env.availableShardPaths(shard.shardId));
        assertEquals(shardStateMetaData, getShardStateMetadata(shard));

        routing = TestShardRouting.newShardRouting(shard.shardId.index().getName(), shard.shardId.id(), routing.currentNodeId(), null, routing.primary(), ShardRoutingState.INITIALIZING, shard.shardRouting.allocationId(), shard.shardRouting.version() + 1);
        shard.updateRoutingEntry(routing, true);
        shard.deleteShardState();

        assertNull("no shard state expected after delete on initializing", load(logger, env.availableShardPaths(shard.shardId)));


    }

    public void testFailShard() throws Exception {
        createIndex("test");
        ensureGreen();
        IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        NodeEnvironment env = getInstanceFromNode(NodeEnvironment.class);
        IndexService test = indicesService.indexService("test");
        IndexShard shard = test.shard(0);
        // fail shard
        shard.failShard("test shard fail", new CorruptIndexException("", ""));
        // check state file still exists
        ShardStateMetaData shardStateMetaData = load(logger, env.availableShardPaths(shard.shardId));
        assertEquals(shardStateMetaData, getShardStateMetadata(shard));
        ShardPath shardPath = ShardPath.loadShardPath(logger, env, shard.shardId(), test.getIndexSettings());
        assertNotNull(shardPath);
        // but index can't be opened for a failed shard
        assertThat("store index should be corrupted", Store.canOpenIndex(logger, shardPath.resolveIndex()), equalTo(false));
    }

    ShardStateMetaData getShardStateMetadata(IndexShard shard) {
        ShardRouting shardRouting = shard.routingEntry();
        if (shardRouting == null) {
            return null;
        } else {
            return new ShardStateMetaData(shardRouting.version(), shardRouting.primary(), shard.indexSettings.get(IndexMetaData.SETTING_INDEX_UUID));
        }
    }

    public void testShardStateMetaHashCodeEquals() {
        ShardStateMetaData meta = new ShardStateMetaData(randomLong(), randomBoolean(), randomRealisticUnicodeOfCodepointLengthBetween(1, 10));

        assertEquals(meta, new ShardStateMetaData(meta.version, meta.primary, meta.indexUUID));
        assertEquals(meta.hashCode(), new ShardStateMetaData(meta.version, meta.primary, meta.indexUUID).hashCode());

        assertFalse(meta.equals(new ShardStateMetaData(meta.version, !meta.primary, meta.indexUUID)));
        assertFalse(meta.equals(new ShardStateMetaData(meta.version + 1, meta.primary, meta.indexUUID)));
        assertFalse(meta.equals(new ShardStateMetaData(meta.version, !meta.primary, meta.indexUUID + "foo")));
        Set<Integer> hashCodes = new HashSet<>();
        for (int i = 0; i < 30; i++) { // just a sanity check that we impl hashcode
            meta = new ShardStateMetaData(randomLong(), randomBoolean(), randomRealisticUnicodeOfCodepointLengthBetween(1, 10));
            hashCodes.add(meta.hashCode());
        }
        assertTrue("more than one unique hashcode expected but got: " + hashCodes.size(), hashCodes.size() > 1);

    }

    @Test
    public void testDeleteIndexDecreasesCounter() throws InterruptedException, ExecutionException, IOException {
        assertAcked(client().admin().indices().prepareCreate("test").setSettings(Settings.builder().put("index.number_of_shards", 1).put("index.number_of_replicas", 0)).get());
        ensureGreen("test");
        IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        IndexService indexService = indicesService.indexServiceSafe("test");
        IndexShard indexShard = indexService.shard(0);
        client().admin().indices().prepareDelete("test").get();
        assertThat(indexShard.getOperationsCount(), equalTo(0));
        try {
            indexShard.incrementOperationCounter();
            fail("we should not be able to increment anymore");
        } catch (IndexShardClosedException e) {
            // expected
        }
    }

    @Test
    public void testIndexShardCounter() throws InterruptedException, ExecutionException, IOException {
        assertAcked(client().admin().indices().prepareCreate("test").setSettings(Settings.builder().put("index.number_of_shards", 1).put("index.number_of_replicas", 0)).get());
        ensureGreen("test");
        IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        IndexService indexService = indicesService.indexServiceSafe("test");
        IndexShard indexShard = indexService.shard(0);
        assertEquals(0, indexShard.getOperationsCount());
        indexShard.incrementOperationCounter();
        assertEquals(1, indexShard.getOperationsCount());
        indexShard.incrementOperationCounter();
        assertEquals(2, indexShard.getOperationsCount());
        indexShard.decrementOperationCounter();
        indexShard.decrementOperationCounter();
        assertEquals(0, indexShard.getOperationsCount());
    }

    @Test
    public void testMarkAsInactiveTriggersSyncedFlush() throws Exception {
        assertAcked(client().admin().indices().prepareCreate("test")
                .setSettings(SETTING_NUMBER_OF_SHARDS, 1, SETTING_NUMBER_OF_REPLICAS, 0));
        client().prepareIndex("test", "test").setSource("{}").get();
        ensureGreen("test");
        IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        indicesService.indexService("test").shard(0).markAsInactive();
        assertBusy(new Runnable() { // should be very very quick
            @Override
            public void run() {
                IndexStats indexStats = client().admin().indices().prepareStats("test").clear().get().getIndex("test");
                assertNotNull(indexStats.getShards()[0].getCommitStats().getUserData().get(Engine.SYNC_COMMIT_ID));
            }
        });
        IndexStats indexStats = client().admin().indices().prepareStats("test").get().getIndex("test");
        assertNotNull(indexStats.getShards()[0].getCommitStats().getUserData().get(Engine.SYNC_COMMIT_ID));
    }

    public static ShardStateMetaData load(ESLogger logger, Path... shardPaths) throws IOException {
        return ShardStateMetaData.FORMAT.loadLatestState(logger, shardPaths);
    }

    public static void write(ShardStateMetaData shardStateMetaData,
                             Path... shardPaths) throws IOException {
        ShardStateMetaData.FORMAT.write(shardStateMetaData, shardStateMetaData.version, shardPaths);
    }

    public void testDurableFlagHasEffect() {
        createIndex("test");
        ensureGreen();
        client().prepareIndex("test", "bar", "1").setSource("{}").get();
        IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        IndexService test = indicesService.indexService("test");
        IndexShard shard = test.shard(0);
        setDurability(shard, Translog.Durabilty.REQUEST);
        assertFalse(shard.engine().getTranslog().syncNeeded());
        setDurability(shard, Translog.Durabilty.ASYNC);
        client().prepareIndex("test", "bar", "2").setSource("{}").get();
        assertTrue(shard.engine().getTranslog().syncNeeded());
        setDurability(shard, Translog.Durabilty.REQUEST);
        client().prepareDelete("test", "bar", "1").get();
        assertFalse(shard.engine().getTranslog().syncNeeded());

        setDurability(shard, Translog.Durabilty.ASYNC);
        client().prepareDelete("test", "bar", "2").get();
        assertTrue(shard.engine().getTranslog().syncNeeded());
        setDurability(shard, Translog.Durabilty.REQUEST);
        assertNoFailures(client().prepareBulk()
                .add(client().prepareIndex("test", "bar", "3").setSource("{}"))
                .add(client().prepareDelete("test", "bar", "1")).get());
        assertFalse(shard.engine().getTranslog().syncNeeded());

        setDurability(shard, Translog.Durabilty.ASYNC);
        assertNoFailures(client().prepareBulk()
                .add(client().prepareIndex("test", "bar", "4").setSource("{}"))
                .add(client().prepareDelete("test", "bar", "3")).get());
        setDurability(shard, Translog.Durabilty.REQUEST);
        assertTrue(shard.engine().getTranslog().syncNeeded());
    }

    private void setDurability(IndexShard shard, Translog.Durabilty durabilty) {
        client().admin().indices().prepareUpdateSettings(shard.shardId.getIndex()).setSettings(settingsBuilder().put(TranslogConfig.INDEX_TRANSLOG_DURABILITY, durabilty.name()).build()).get();
        assertEquals(durabilty, shard.getTranslogDurability());
    }

    public void testDeleteByQueryBWC() {
        Version version = VersionUtils.randomVersion(random());
        assertAcked(client().admin().indices().prepareCreate("test")
                .setSettings(SETTING_NUMBER_OF_SHARDS, 1, SETTING_NUMBER_OF_REPLICAS, 0, IndexMetaData.SETTING_VERSION_CREATED, version.id));
        ensureGreen("test");
        client().prepareIndex("test", "person").setSource("{ \"user\" : \"kimchy\" }").get();

        IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        IndexService test = indicesService.indexService("test");
        IndexShard shard = test.shard(0);
        int numDocs = 1;
        shard.state = IndexShardState.RECOVERING;
        try {
            shard.recoveryState().getTranslog().totalOperations(1);
            shard.engine().config().getTranslogRecoveryPerformer().performRecoveryOperation(shard.engine(), new Translog.DeleteByQuery(new Engine.DeleteByQuery(null, new BytesArray("{\"term\" : { \"user\" : \"kimchy\" }}"), null, null, null, Engine.Operation.Origin.RECOVERY, 0, "person")), false);
            assertTrue(version.onOrBefore(Version.V_1_0_0_Beta2));
            numDocs = 0;
        } catch (QueryParsingException ex) {
            assertTrue(version.after(Version.V_1_0_0_Beta2));
        } finally {
            shard.state = IndexShardState.STARTED;
        }
        shard.engine().refresh("foo");

        try (Engine.Searcher searcher = shard.engine().acquireSearcher("foo")) {
            assertEquals(numDocs, searcher.reader().numDocs());
        }
    }

    public void testMinimumCompatVersion() {
        Version versionCreated = VersionUtils.randomVersion(random());
        assertAcked(client().admin().indices().prepareCreate("test")
                .setSettings(SETTING_NUMBER_OF_SHARDS, 1, SETTING_NUMBER_OF_REPLICAS, 0, SETTING_VERSION_CREATED, versionCreated.id));
        client().prepareIndex("test", "test").setSource("{}").get();
        ensureGreen("test");
        IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        IndexShard test = indicesService.indexService("test").shard(0);
        assertEquals(versionCreated.luceneVersion, test.minimumCompatibleVersion());
        client().prepareIndex("test", "test").setSource("{}").get();
        assertEquals(versionCreated.luceneVersion, test.minimumCompatibleVersion());
        test.engine().flush();
        assertEquals(Version.CURRENT.luceneVersion, test.minimumCompatibleVersion());
    }

    public void testUpdatePriority() {
        assertAcked(client().admin().indices().prepareCreate("test")
                .setSettings(IndexMetaData.SETTING_PRIORITY, 200));
        IndexSettingsService indexSettingsService = getInstanceFromNode(IndicesService.class).indexService("test").settingsService();
        assertEquals(200, indexSettingsService.getSettings().getAsInt(IndexMetaData.SETTING_PRIORITY, 0).intValue());
        client().admin().indices().prepareUpdateSettings("test").setSettings(Settings.builder().put(IndexMetaData.SETTING_PRIORITY, 400).build()).get();
        assertEquals(400, indexSettingsService.getSettings().getAsInt(IndexMetaData.SETTING_PRIORITY, 0).intValue());
    }

    public void testRecoverIntoLeftover() throws IOException {
        createIndex("test");
        ensureGreen("test");
        client().prepareIndex("test", "bar", "1").setSource("{}").setRefresh(true).get();
        client().admin().indices().prepareFlush("test").get();
        SearchResponse response = client().prepareSearch("test").get();
        assertHitCount(response, 1l);
        IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        IndexService test = indicesService.indexService("test");
        IndexShard shard = test.shard(0);
        ShardPath shardPath = shard.shardPath();
        Path dataPath = shardPath.getDataPath();
        client().admin().indices().prepareClose("test").get();
        Path tempDir = createTempDir();
        Files.move(dataPath, tempDir.resolve("test"));
        client().admin().indices().prepareDelete("test").get();
        Files.createDirectories(dataPath.getParent());
        Files.move(tempDir.resolve("test"), dataPath);
        createIndex("test");
        ensureGreen("test");
        response = client().prepareSearch("test").get();
        assertHitCount(response, 0l);
    }

    public void testIndexDirIsDeletedWhenShardRemoved() throws Exception {
        Environment env = getInstanceFromNode(Environment.class);
        Path idxPath = env.sharedDataFile().resolve(randomAsciiOfLength(10));
        logger.info("--> idxPath: [{}]", idxPath);
        Settings idxSettings = Settings.builder()
                .put(IndexMetaData.SETTING_DATA_PATH, idxPath)
                .build();
        createIndex("test", idxSettings);
        ensureGreen("test");
        client().prepareIndex("test", "bar", "1").setSource("{}").setRefresh(true).get();
        SearchResponse response = client().prepareSearch("test").get();
        assertHitCount(response, 1l);
        client().admin().indices().prepareDelete("test").get();
        assertPathHasBeenCleared(idxPath);
    }

    public void testExpectedShardSizeIsPresent() throws InterruptedException {
        assertAcked(client().admin().indices().prepareCreate("test")
                .setSettings(SETTING_NUMBER_OF_SHARDS, 1, SETTING_NUMBER_OF_REPLICAS, 0));
        for (int i = 0; i < 50; i++) {
            client().prepareIndex("test", "test").setSource("{}").get();
        }
        ensureGreen("test");
        InternalClusterInfoService clusterInfoService = (InternalClusterInfoService) getInstanceFromNode(ClusterInfoService.class);
        clusterInfoService.refresh();
        ClusterState state = getInstanceFromNode(ClusterService.class).state();
        Long test = clusterInfoService.getClusterInfo().getShardSize(state.getRoutingTable().index("test").getShards().get(0).primaryShard());
        assertNotNull(test);
        assertTrue(test > 0);
    }

    public void testIndexCanChangeCustomDataPath() throws Exception {
        Environment env = getInstanceFromNode(Environment.class);
        Path idxPath = env.sharedDataFile().resolve(randomAsciiOfLength(10));
        final String INDEX = "idx";
        Path startDir = idxPath.resolve("start-" + randomAsciiOfLength(10));
        Path endDir = idxPath.resolve("end-" + randomAsciiOfLength(10));
        logger.info("--> start dir: [{}]", startDir.toAbsolutePath().toString());
        logger.info("-->   end dir: [{}]", endDir.toAbsolutePath().toString());
        // temp dirs are automatically created, but the end dir is what
        // startDir is going to be renamed as, so it needs to be deleted
        // otherwise we get all sorts of errors about the directory
        // already existing
        IOUtils.rm(endDir);

        Settings sb = Settings.builder()
                .put(IndexMetaData.SETTING_DATA_PATH, startDir.toAbsolutePath().toString())
                .build();
        Settings sb2 = Settings.builder()
                .put(IndexMetaData.SETTING_DATA_PATH, endDir.toAbsolutePath().toString())
                .build();

        logger.info("--> creating an index with data_path [{}]", startDir.toAbsolutePath().toString());
        createIndex(INDEX, sb);
        ensureGreen(INDEX);
        client().prepareIndex(INDEX, "bar", "1").setSource("{}").setRefresh(true).get();

        SearchResponse resp = client().prepareSearch(INDEX).setQuery(matchAllQuery()).get();
        assertThat("found the hit", resp.getHits().getTotalHits(), equalTo(1L));

        logger.info("--> closing the index [{}]", INDEX);
        client().admin().indices().prepareClose(INDEX).get();
        logger.info("--> index closed, re-opening...");
        client().admin().indices().prepareOpen(INDEX).get();
        logger.info("--> index re-opened");
        ensureGreen(INDEX);

        resp = client().prepareSearch(INDEX).setQuery(matchAllQuery()).get();
        assertThat("found the hit", resp.getHits().getTotalHits(), equalTo(1L));

        // Now, try closing and changing the settings

        logger.info("--> closing the index [{}]", INDEX);
        client().admin().indices().prepareClose(INDEX).get();

        logger.info("--> moving data on disk [{}] to [{}]", startDir.getFileName(), endDir.getFileName());
        assert Files.exists(endDir) == false : "end directory should not exist!";
        Files.move(startDir, endDir, StandardCopyOption.REPLACE_EXISTING);

        logger.info("--> updating settings...");
        client().admin().indices().prepareUpdateSettings(INDEX)
                .setSettings(sb2)
                .setIndicesOptions(IndicesOptions.fromOptions(true, false, true, true))
                .get();

        assert Files.exists(startDir) == false : "start dir shouldn't exist";

        logger.info("--> settings updated and files moved, re-opening index");
        client().admin().indices().prepareOpen(INDEX).get();
        logger.info("--> index re-opened");
        ensureGreen(INDEX);

        resp = client().prepareSearch(INDEX).setQuery(matchAllQuery()).get();
        assertThat("found the hit", resp.getHits().getTotalHits(), equalTo(1L));

        assertAcked(client().admin().indices().prepareDelete(INDEX));
        assertPathHasBeenCleared(startDir.toAbsolutePath().toString());
        assertPathHasBeenCleared(endDir.toAbsolutePath().toString());
    }

    public void testShardStats() throws IOException {
        createIndex("test");
        ensureGreen();
        IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        IndexService test = indicesService.indexService("test");
        IndexShard shard = test.shard(0);
        ShardStats stats = new ShardStats(shard.routingEntry(), shard.shardPath(), new CommonStats(shard, new CommonStatsFlags()), shard.commitStats());
        assertEquals(shard.shardPath().getRootDataPath().toString(), stats.getDataPath());
        assertEquals(shard.shardPath().getRootStatePath().toString(), stats.getStatePath());
        assertEquals(shard.shardPath().isCustomDataPath(), stats.isCustomDataPath());

        if (randomBoolean() || true) { // try to serialize it to ensure values survive the serialization
            BytesStreamOutput out = new BytesStreamOutput();
            stats.writeTo(out);
            StreamInput in = StreamInput.wrap(out.bytes());
            stats = ShardStats.readShardStats(in);
        }
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        stats.toXContent(builder, EMPTY_PARAMS);
        builder.endObject();
        String xContent = builder.string();
        StringBuilder expectedSubSequence = new StringBuilder("\"shard_path\":{\"state_path\":\"");
        expectedSubSequence.append(shard.shardPath().getRootStatePath().toString());
        expectedSubSequence.append("\",\"data_path\":\"");
        expectedSubSequence.append(shard.shardPath().getRootDataPath().toString());
        expectedSubSequence.append("\",\"is_custom_data_path\":").append(shard.shardPath().isCustomDataPath()).append("}");
        assumeFalse("Some path weirdness on windows", Constants.WINDOWS);
        assertTrue(xContent.contains(expectedSubSequence));
    }

    private ParsedDocument testParsedDocument(String uid, String id, String type, String routing, long timestamp, long ttl, ParseContext.Document document, BytesReference source, Mapping mappingUpdate) {
        Field uidField = new Field("_uid", uid, UidFieldMapper.Defaults.FIELD_TYPE);
        Field versionField = new NumericDocValuesField("_version", 0);
        document.add(uidField);
        document.add(versionField);
        return new ParsedDocument(uidField, versionField, id, type, routing, timestamp, ttl, Arrays.asList(document), source, mappingUpdate);
    }

    public void testPreIndex() throws IOException {
        createIndex("testpreindex");
        ensureGreen();
        IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        IndexService test = indicesService.indexService("testpreindex");
        IndexShard shard = test.shard(0);
        ShardIndexingService shardIndexingService = shard.indexingService();
        final AtomicBoolean preIndexCalled = new AtomicBoolean(false);

        shardIndexingService.addListener(new IndexingOperationListener() {
            @Override
            public Engine.Index preIndex(Engine.Index index) {
                preIndexCalled.set(true);
                return super.preIndex(index);
            }
        });

        ParsedDocument doc = testParsedDocument("1", "1", "test", null, -1, -1, new ParseContext.Document(), new BytesArray(new byte[]{1}), null);
        Engine.Index index = new Engine.Index(new Term("_uid", "1"), doc);
        shard.index(index);
        assertTrue(preIndexCalled.get());
    }

    public void testPostIndex() throws IOException {
        createIndex("testpostindex");
        ensureGreen();
        IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        IndexService test = indicesService.indexService("testpostindex");
        IndexShard shard = test.shard(0);
        ShardIndexingService shardIndexingService = shard.indexingService();
        final AtomicBoolean postIndexCalled = new AtomicBoolean(false);

        shardIndexingService.addListener(new IndexingOperationListener() {
            @Override
            public void postIndex(Engine.Index index) {
                postIndexCalled.set(true);
                super.postIndex(index);
            }
        });

        ParsedDocument doc = testParsedDocument("1", "1", "test", null, -1, -1, new ParseContext.Document(), new BytesArray(new byte[]{1}), null);
        Engine.Index index = new Engine.Index(new Term("_uid", "1"), doc);
        shard.index(index);
        assertTrue(postIndexCalled.get());
    }

    public void testPostIndexWithException() throws IOException {
        createIndex("testpostindexwithexception");
        ensureGreen();
        IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        IndexService test = indicesService.indexService("testpostindexwithexception");
        IndexShard shard = test.shard(0);
        ShardIndexingService shardIndexingService = shard.indexingService();

        shard.close("Unexpected close", true);
        shard.state = IndexShardState.STARTED; // It will generate exception

        final AtomicBoolean postIndexWithExceptionCalled = new AtomicBoolean(false);

        shardIndexingService.addListener(new IndexingOperationListener() {
            @Override
            public void postIndex(Engine.Index index, Throwable ex) {
                assertNotNull(ex);
                postIndexWithExceptionCalled.set(true);
                super.postIndex(index, ex);
            }
        });

        ParsedDocument doc = testParsedDocument("1", "1", "test", null, -1, -1, new ParseContext.Document(), new BytesArray(new byte[]{1}), null);
        Engine.Index index = new Engine.Index(new Term("_uid", "1"), doc);

        try {
            shard.index(index);
            fail();
        }catch (IllegalIndexShardStateException e){

        }

        assertTrue(postIndexWithExceptionCalled.get());
    }

}
