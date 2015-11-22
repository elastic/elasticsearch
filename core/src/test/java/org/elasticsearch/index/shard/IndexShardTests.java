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
import org.apache.lucene.index.*;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.LockObtainFailedException;
import org.apache.lucene.util.Constants;
import org.apache.lucene.util.IOUtils;
import org.elasticsearch.Version;
import org.elasticsearch.action.admin.indices.flush.FlushRequest;
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
import org.elasticsearch.cluster.metadata.SnapshotId;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.*;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.DummyTransportAddress;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.env.ShardLock;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.NodeServicesProvider;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.EngineException;
import org.elasticsearch.index.fielddata.FieldDataStats;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.flush.FlushStats;
import org.elasticsearch.index.indexing.IndexingOperationListener;
import org.elasticsearch.index.indexing.ShardIndexingService;
import org.elasticsearch.index.mapper.*;
import org.elasticsearch.index.mapper.internal.UidFieldMapper;
import org.elasticsearch.index.snapshots.IndexShardRepository;
import org.elasticsearch.index.snapshots.IndexShardSnapshotStatus;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.index.translog.TranslogConfig;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.recovery.RecoveryState;
import org.elasticsearch.test.DummyShardLock;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.test.IndexSettingsModule;
import org.elasticsearch.test.FieldMaskingReader;
import org.elasticsearch.test.VersionUtils;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.elasticsearch.cluster.metadata.IndexMetaData.*;
import static org.elasticsearch.common.settings.Settings.settingsBuilder;
import static org.elasticsearch.common.xcontent.ToXContent.EMPTY_PARAMS;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.*;
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
        IndexShard shard = test.getShardOrNull(0);
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

    public void testLockTryingToDelete() throws Exception {
        createIndex("test");
        ensureGreen();
        NodeEnvironment env = getInstanceFromNode(NodeEnvironment.class);
        Path[] shardPaths = env.availableShardPaths(new ShardId("test", 0));
        logger.info("--> paths: [{}]", (Object)shardPaths);
        // Should not be able to acquire the lock because it's already open
        try {
            NodeEnvironment.acquireFSLockForPaths(IndexSettingsModule.newIndexSettings("test", Settings.EMPTY), shardPaths);
            fail("should not have been able to acquire the lock");
        } catch (LockObtainFailedException e) {
            assertTrue("msg: " + e.getMessage(), e.getMessage().contains("unable to acquire write.lock"));
        }
        // Test without the regular shard lock to assume we can acquire it
        // (worst case, meaning that the shard lock could be acquired and
        // we're green to delete the shard's directory)
        ShardLock sLock = new DummyShardLock(new ShardId("test", 0));
        try {
            env.deleteShardDirectoryUnderLock(sLock, IndexSettingsModule.newIndexSettings("test", Settings.EMPTY));
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
        IndexShard shard = test.getShardOrNull(0);
        ShardStateMetaData shardStateMetaData = load(logger, env.availableShardPaths(shard.shardId));
        assertEquals(getShardStateMetadata(shard), shardStateMetaData);
        ShardRouting routing = new ShardRouting(shard.shardRouting, shard.shardRouting.version() + 1);
        shard.updateRoutingEntry(routing, true);

        shardStateMetaData = load(logger, env.availableShardPaths(shard.shardId));
        assertEquals(shardStateMetaData, getShardStateMetadata(shard));
        assertEquals(shardStateMetaData, new ShardStateMetaData(routing.version(), routing.primary(), shard.indexSettings().getUUID()));

        routing = new ShardRouting(shard.shardRouting, shard.shardRouting.version() + 1);
        shard.updateRoutingEntry(routing, true);
        shardStateMetaData = load(logger, env.availableShardPaths(shard.shardId));
        assertEquals(shardStateMetaData, getShardStateMetadata(shard));
        assertEquals(shardStateMetaData, new ShardStateMetaData(routing.version(), routing.primary(), shard.indexSettings().getUUID()));

        routing = new ShardRouting(shard.shardRouting, shard.shardRouting.version() + 1);
        shard.updateRoutingEntry(routing, true);
        shardStateMetaData = load(logger, env.availableShardPaths(shard.shardId));
        assertEquals(shardStateMetaData, getShardStateMetadata(shard));
        assertEquals(shardStateMetaData, new ShardStateMetaData(routing.version(), routing.primary(), shard.indexSettings().getUUID()));

        // test if we still write it even if the shard is not active
        ShardRouting inactiveRouting = TestShardRouting.newShardRouting(shard.shardRouting.index(), shard.shardRouting.shardId().id(), shard.shardRouting.currentNodeId(), null, null, true, ShardRoutingState.INITIALIZING, shard.shardRouting.version() + 1);
        shard.persistMetadata(inactiveRouting, shard.shardRouting);
        shardStateMetaData = load(logger, env.availableShardPaths(shard.shardId));
        assertEquals("inactive shard state shouldn't be persisted", shardStateMetaData, getShardStateMetadata(shard));
        assertEquals("inactive shard state shouldn't be persisted", shardStateMetaData, new ShardStateMetaData(routing.version(), routing.primary(), shard.indexSettings().getUUID()));


        shard.updateRoutingEntry(new ShardRouting(shard.shardRouting, shard.shardRouting.version() + 1), false);
        shardStateMetaData = load(logger, env.availableShardPaths(shard.shardId));
        assertFalse("shard state persisted despite of persist=false", shardStateMetaData.equals(getShardStateMetadata(shard)));
        assertEquals("shard state persisted despite of persist=false", shardStateMetaData, new ShardStateMetaData(routing.version(), routing.primary(), shard.indexSettings().getUUID()));


        routing = new ShardRouting(shard.shardRouting, shard.shardRouting.version() + 1);
        shard.updateRoutingEntry(routing, true);
        shardStateMetaData = load(logger, env.availableShardPaths(shard.shardId));
        assertEquals(shardStateMetaData, getShardStateMetadata(shard));
        assertEquals(shardStateMetaData, new ShardStateMetaData(routing.version(), routing.primary(), shard.indexSettings().getUUID()));
    }

    public void testDeleteShardState() throws IOException {
        createIndex("test");
        ensureGreen();
        IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        NodeEnvironment env = getInstanceFromNode(NodeEnvironment.class);
        IndexService test = indicesService.indexService("test");
        IndexShard shard = test.getShardOrNull(0);
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
        IndexShard shard = test.getShardOrNull(0);
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
            return new ShardStateMetaData(shardRouting.version(), shardRouting.primary(), shard.indexSettings().getUUID());
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

    public void testDeleteIndexDecreasesCounter() throws InterruptedException, ExecutionException, IOException {
        assertAcked(client().admin().indices().prepareCreate("test").setSettings(Settings.builder().put("index.number_of_shards", 1).put("index.number_of_replicas", 0)).get());
        ensureGreen("test");
        IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        IndexService indexService = indicesService.indexServiceSafe("test");
        IndexShard indexShard = indexService.getShardOrNull(0);
        client().admin().indices().prepareDelete("test").get();
        assertThat(indexShard.getOperationsCount(), equalTo(0));
        try {
            indexShard.incrementOperationCounter();
            fail("we should not be able to increment anymore");
        } catch (IndexShardClosedException e) {
            // expected
        }
    }

    public void testIndexShardCounter() throws InterruptedException, ExecutionException, IOException {
        assertAcked(client().admin().indices().prepareCreate("test").setSettings(Settings.builder().put("index.number_of_shards", 1).put("index.number_of_replicas", 0)).get());
        ensureGreen("test");
        IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        IndexService indexService = indicesService.indexServiceSafe("test");
        IndexShard indexShard = indexService.getShardOrNull(0);
        assertEquals(0, indexShard.getOperationsCount());
        indexShard.incrementOperationCounter();
        assertEquals(1, indexShard.getOperationsCount());
        indexShard.incrementOperationCounter();
        assertEquals(2, indexShard.getOperationsCount());
        indexShard.decrementOperationCounter();
        indexShard.decrementOperationCounter();
        assertEquals(0, indexShard.getOperationsCount());
    }

    public void testMarkAsInactiveTriggersSyncedFlush() throws Exception {
        assertAcked(client().admin().indices().prepareCreate("test")
                .setSettings(SETTING_NUMBER_OF_SHARDS, 1, SETTING_NUMBER_OF_REPLICAS, 0));
        client().prepareIndex("test", "test").setSource("{}").get();
        ensureGreen("test");
        IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        Boolean result = indicesService.indexService("test").getShardOrNull(0).checkIdle(0);
        assertEquals(Boolean.TRUE, result);
        assertBusy(() -> {
            IndexStats indexStats = client().admin().indices().prepareStats("test").clear().get().getIndex("test");
            assertNotNull(indexStats.getShards()[0].getCommitStats().getUserData().get(Engine.SYNC_COMMIT_ID));
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
        IndexShard shard = test.getShardOrNull(0);
        setDurability(shard, Translog.Durabilty.REQUEST);
        assertFalse(shard.getEngine().getTranslog().syncNeeded());
        setDurability(shard, Translog.Durabilty.ASYNC);
        client().prepareIndex("test", "bar", "2").setSource("{}").get();
        assertTrue(shard.getEngine().getTranslog().syncNeeded());
        setDurability(shard, Translog.Durabilty.REQUEST);
        client().prepareDelete("test", "bar", "1").get();
        assertFalse(shard.getEngine().getTranslog().syncNeeded());

        setDurability(shard, Translog.Durabilty.ASYNC);
        client().prepareDelete("test", "bar", "2").get();
        assertTrue(shard.getEngine().getTranslog().syncNeeded());
        setDurability(shard, Translog.Durabilty.REQUEST);
        assertNoFailures(client().prepareBulk()
                .add(client().prepareIndex("test", "bar", "3").setSource("{}"))
                .add(client().prepareDelete("test", "bar", "1")).get());
        assertFalse(shard.getEngine().getTranslog().syncNeeded());

        setDurability(shard, Translog.Durabilty.ASYNC);
        assertNoFailures(client().prepareBulk()
                .add(client().prepareIndex("test", "bar", "4").setSource("{}"))
                .add(client().prepareDelete("test", "bar", "3")).get());
        setDurability(shard, Translog.Durabilty.REQUEST);
        assertTrue(shard.getEngine().getTranslog().syncNeeded());
    }

    private void setDurability(IndexShard shard, Translog.Durabilty durabilty) {
        client().admin().indices().prepareUpdateSettings(shard.shardId.getIndex()).setSettings(settingsBuilder().put(TranslogConfig.INDEX_TRANSLOG_DURABILITY, durabilty.name()).build()).get();
        assertEquals(durabilty, shard.getTranslogDurability());
    }

    public void testMinimumCompatVersion() {
        Version versionCreated = VersionUtils.randomVersion(random());
        assertAcked(client().admin().indices().prepareCreate("test")
                .setSettings(SETTING_NUMBER_OF_SHARDS, 1, SETTING_NUMBER_OF_REPLICAS, 0, SETTING_VERSION_CREATED, versionCreated.id));
        client().prepareIndex("test", "test").setSource("{}").get();
        ensureGreen("test");
        IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        IndexShard test = indicesService.indexService("test").getShardOrNull(0);
        assertEquals(versionCreated.luceneVersion, test.minimumCompatibleVersion());
        client().prepareIndex("test", "test").setSource("{}").get();
        assertEquals(versionCreated.luceneVersion, test.minimumCompatibleVersion());
        test.getEngine().flush();
        assertEquals(Version.CURRENT.luceneVersion, test.minimumCompatibleVersion());
    }

    public void testUpdatePriority() {
        assertAcked(client().admin().indices().prepareCreate("test")
                .setSettings(IndexMetaData.SETTING_PRIORITY, 200));
        IndexService indexService = getInstanceFromNode(IndicesService.class).indexService("test");
        assertEquals(200, indexService.getIndexSettings().getSettings().getAsInt(IndexMetaData.SETTING_PRIORITY, 0).intValue());
        client().admin().indices().prepareUpdateSettings("test").setSettings(Settings.builder().put(IndexMetaData.SETTING_PRIORITY, 400).build()).get();
        assertEquals(400, indexService.getIndexSettings().getSettings().getAsInt(IndexMetaData.SETTING_PRIORITY, 0).intValue());
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
        IndexShard shard = test.getShardOrNull(0);
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
        IndexShard shard = test.getShardOrNull(0);
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
        IndexShard shard = test.getShardOrNull(0);
        ShardIndexingService shardIndexingService = shard.indexingService();
        final AtomicBoolean preIndexCalled = new AtomicBoolean(false);

        shardIndexingService.addListener(new IndexingOperationListener() {
            @Override
            public Engine.Index preIndex(Engine.Index operation) {
                preIndexCalled.set(true);
                return super.preIndex(operation);
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
        IndexShard shard = test.getShardOrNull(0);
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
        IndexShard shard = test.getShardOrNull(0);
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

    public void testMaybeFlush() throws Exception {
        createIndex("test", settingsBuilder().put(TranslogConfig.INDEX_TRANSLOG_DURABILITY, Translog.Durabilty.REQUEST).build());
        ensureGreen();
        IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        IndexService test = indicesService.indexService("test");
        IndexShard shard = test.getShardOrNull(0);
        assertFalse(shard.shouldFlush());
        client().admin().indices().prepareUpdateSettings("test").setSettings(settingsBuilder().put(IndexShard.INDEX_TRANSLOG_FLUSH_THRESHOLD_OPS, 1).build()).get();
        client().prepareIndex("test", "test", "0").setSource("{}").setRefresh(randomBoolean()).get();
        assertFalse(shard.shouldFlush());
        ParsedDocument doc = testParsedDocument("1", "1", "test", null, -1, -1, new ParseContext.Document(), new BytesArray(new byte[]{1}), null);
        Engine.Index index = new Engine.Index(new Term("_uid", "1"), doc);
        shard.index(index);
        assertTrue(shard.shouldFlush());
        assertEquals(2, shard.getEngine().getTranslog().totalOperations());
        client().prepareIndex("test", "test", "2").setSource("{}").setRefresh(randomBoolean()).get();
        assertBusy(() -> { // this is async
            assertFalse(shard.shouldFlush());
        });
        assertEquals(0, shard.getEngine().getTranslog().totalOperations());
        shard.getEngine().getTranslog().sync();
        long size = shard.getEngine().getTranslog().sizeInBytes();
        logger.info("--> current translog size: [{}] num_ops [{}] generation [{}]", shard.getEngine().getTranslog().sizeInBytes(), shard.getEngine().getTranslog().totalOperations(), shard.getEngine().getTranslog().getGeneration());
        client().admin().indices().prepareUpdateSettings("test").setSettings(settingsBuilder().put(IndexShard.INDEX_TRANSLOG_FLUSH_THRESHOLD_OPS, 1000)
                .put(IndexShard.INDEX_TRANSLOG_FLUSH_THRESHOLD_SIZE, new ByteSizeValue(size, ByteSizeUnit.BYTES))
                .build()).get();
        client().prepareDelete("test", "test", "2").get();
        logger.info("--> translog size after delete: [{}] num_ops [{}] generation [{}]", shard.getEngine().getTranslog().sizeInBytes(), shard.getEngine().getTranslog().totalOperations(), shard.getEngine().getTranslog().getGeneration());
        assertBusy(() -> { // this is async
            logger.info("--> translog size on iter  : [{}] num_ops [{}] generation [{}]", shard.getEngine().getTranslog().sizeInBytes(), shard.getEngine().getTranslog().totalOperations(), shard.getEngine().getTranslog().getGeneration());
            assertFalse(shard.shouldFlush());
        });
        assertEquals(0, shard.getEngine().getTranslog().totalOperations());
    }

    public void testStressMaybeFlush() throws Exception {
        createIndex("test");
        ensureGreen();
        IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        IndexService test = indicesService.indexService("test");
        final IndexShard shard = test.getShardOrNull(0);
        assertFalse(shard.shouldFlush());
        client().admin().indices().prepareUpdateSettings("test").setSettings(settingsBuilder().put(IndexShard.INDEX_TRANSLOG_FLUSH_THRESHOLD_OPS, 1).build()).get();
        client().prepareIndex("test", "test", "0").setSource("{}").setRefresh(randomBoolean()).get();
        assertFalse(shard.shouldFlush());
        final AtomicBoolean running = new AtomicBoolean(true);
        final int numThreads = randomIntBetween(2, 4);
        Thread[] threads = new Thread[numThreads];
        CyclicBarrier barrier = new CyclicBarrier(numThreads + 1);
        for (int i = 0; i < threads.length; i++) {
            threads[i] = new Thread() {
                @Override
                public void run() {
                    try {
                        barrier.await();
                    } catch (InterruptedException | BrokenBarrierException e) {
                        throw new RuntimeException(e);
                    }
                    while (running.get()) {
                        shard.maybeFlush();
                    }
                }
            };
            threads[i].start();
        }
        barrier.await();
        FlushStats flushStats = shard.flushStats();
        long total = flushStats.getTotal();
        client().prepareIndex("test", "test", "1").setSource("{}").get();
        assertBusy(() -> {
            assertEquals(total + 1, shard.flushStats().getTotal());
        });
        running.set(false);
        for (int i = 0; i < threads.length; i++) {
            threads[i].join();
        }
        assertEquals(total + 1, shard.flushStats().getTotal());
    }

    public void testRecoverFromStore() throws IOException {
        createIndex("test");
        ensureGreen();
        IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        IndexService test = indicesService.indexService("test");
        final IndexShard shard = test.getShardOrNull(0);

        client().prepareIndex("test", "test", "0").setSource("{}").setRefresh(randomBoolean()).get();
        if (randomBoolean()) {
            client().admin().indices().prepareFlush().get();
        }
        ShardRouting routing = new ShardRouting(shard.routingEntry());
        test.removeShard(0, "b/c simon says so");
        ShardRoutingHelper.reinit(routing);
        IndexShard newShard = test.createShard(0, routing);
        newShard.updateRoutingEntry(routing, false);
        DiscoveryNode localNode = new DiscoveryNode("foo", DummyTransportAddress.INSTANCE, Version.CURRENT);
        newShard.markAsRecovering("store", new RecoveryState(newShard.shardId(), routing.primary(), RecoveryState.Type.STORE, localNode, localNode));
        assertTrue(newShard.recoverFromStore(localNode));
        routing = new ShardRouting(routing);
        ShardRoutingHelper.moveToStarted(routing);
        newShard.updateRoutingEntry(routing, true);
        SearchResponse response = client().prepareSearch().get();
        assertHitCount(response, 1);
    }

    public void testFailIfIndexNotPresentInRecoverFromStore() throws IOException {
        createIndex("test");
        ensureGreen();
        IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        DiscoveryNode localNode = new DiscoveryNode("foo", DummyTransportAddress.INSTANCE, Version.CURRENT);
        IndexService test = indicesService.indexService("test");
        final IndexShard shard = test.getShardOrNull(0);

        client().prepareIndex("test", "test", "0").setSource("{}").setRefresh(randomBoolean()).get();
        if (randomBoolean()) {
            client().admin().indices().prepareFlush().get();
        }
        final ShardRouting origRouting = shard.routingEntry();
        ShardRouting routing = new ShardRouting(origRouting);
        Store store = shard.store();
        store.incRef();
        test.removeShard(0, "b/c simon says so");
        Lucene.cleanLuceneIndex(store.directory());
        store.decRef();
        ShardRoutingHelper.reinit(routing);
        IndexShard newShard = test.createShard(0, routing);
        newShard.updateRoutingEntry(routing, false);
        newShard.markAsRecovering("store", new RecoveryState(newShard.shardId(), routing.primary(), RecoveryState.Type.STORE, localNode, localNode));
        try {
            newShard.recoverFromStore(localNode);
            fail("index not there!");
        } catch (IndexShardRecoveryException ex) {
            assertTrue(ex.getMessage().contains("failed to fetch index version after copying it over"));
        }

        ShardRoutingHelper.moveToUnassigned(routing, new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, "because I say so"));
        ShardRoutingHelper.initialize(routing, origRouting.currentNodeId());
        assertTrue("it's already recovering, we should ignore new ones", newShard.ignoreRecoveryAttempt());
        try {
            newShard.markAsRecovering("store", new RecoveryState(newShard.shardId(), routing.primary(), RecoveryState.Type.STORE, localNode, localNode));
            fail("we are already recovering, can't mark again");
        } catch (IllegalIndexShardStateException e) {
            // OK!
        }
        test.removeShard(0, "I broken it");
        newShard = test.createShard(0, routing);
        newShard.updateRoutingEntry(routing, false);
        newShard.markAsRecovering("store", new RecoveryState(newShard.shardId(), routing.primary(), RecoveryState.Type.STORE, localNode, localNode));
        assertTrue("recover even if there is nothing to recover", newShard.recoverFromStore(localNode));

        routing = new ShardRouting(routing);
        ShardRoutingHelper.moveToStarted(routing);
        newShard.updateRoutingEntry(routing, true);
        SearchResponse response = client().prepareSearch().get();
        assertHitCount(response, 0);
        client().prepareIndex("test", "test", "0").setSource("{}").setRefresh(true).get();
        assertHitCount(client().prepareSearch().get(), 1);
    }

    public void testRestoreShard() throws IOException {
        createIndex("test");
        createIndex("test_target");
        ensureGreen();
        IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        IndexService test = indicesService.indexService("test");
        IndexService test_target = indicesService.indexService("test_target");
        final IndexShard test_shard = test.getShardOrNull(0);

        client().prepareIndex("test", "test", "0").setSource("{}").setRefresh(randomBoolean()).get();
        client().prepareIndex("test_target", "test", "1").setSource("{}").setRefresh(true).get();
        assertHitCount(client().prepareSearch("test_target").get(), 1);
        assertSearchHits(client().prepareSearch("test_target").get(), "1");
        client().admin().indices().prepareFlush("test").get(); // only flush test
        final ShardRouting origRouting = test_target.getShardOrNull(0).routingEntry();
        ShardRouting routing = new ShardRouting(origRouting);
        ShardRoutingHelper.reinit(routing);
        routing = ShardRoutingHelper.newWithRestoreSource(routing, new RestoreSource(new SnapshotId("foo", "bar"), Version.CURRENT, "test"));
        test_target.removeShard(0, "just do it man!");
        final IndexShard test_target_shard = test_target.createShard(0, routing);
        Store sourceStore = test_shard.store();
        Store targetStore = test_target_shard.store();

        test_target_shard.updateRoutingEntry(routing, false);
        DiscoveryNode localNode = new DiscoveryNode("foo", DummyTransportAddress.INSTANCE, Version.CURRENT);
        test_target_shard.markAsRecovering("store", new RecoveryState(routing.shardId(), routing.primary(), RecoveryState.Type.SNAPSHOT, routing.restoreSource(), localNode));
        assertTrue(test_target_shard.restoreFromRepository(new IndexShardRepository() {
            @Override
            public void snapshot(SnapshotId snapshotId, ShardId shardId, IndexCommit snapshotIndexCommit, IndexShardSnapshotStatus snapshotStatus) {
            }

            @Override
            public void restore(SnapshotId snapshotId, Version version, ShardId shardId, ShardId snapshotShardId, RecoveryState recoveryState) {
                try {
                    Lucene.cleanLuceneIndex(targetStore.directory());
                    for (String file : sourceStore.directory().listAll()) {
                        if (file.equals("write.lock") || file.startsWith("extra")) {
                            continue;
                        }
                        targetStore.directory().copyFrom(sourceStore.directory(), file, file, IOContext.DEFAULT);
                    }
                } catch (Exception ex) {
                    throw new RuntimeException(ex);
                }
            }

            @Override
            public IndexShardSnapshotStatus snapshotStatus(SnapshotId snapshotId, Version version, ShardId shardId) {
                return null;
            }

            @Override
            public void verify(String verificationToken) {
            }
        }, localNode));

        routing = new ShardRouting(routing);
        ShardRoutingHelper.moveToStarted(routing);
        test_target_shard.updateRoutingEntry(routing, true);
        assertHitCount(client().prepareSearch("test_target").get(), 1);
        assertSearchHits(client().prepareSearch("test_target").get(), "0");
    }

    public void testSearcherWrapperIsUsed() throws IOException {
        createIndex("test");
        ensureGreen();
        IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        IndexService indexService = indicesService.indexService("test");
        IndexShard shard = indexService.getShardOrNull(0);
        client().prepareIndex("test", "test", "0").setSource("{\"foo\" : \"bar\"}").setRefresh(true).get();
        client().prepareIndex("test", "test", "1").setSource("{\"foobar\" : \"bar\"}").setRefresh(true).get();

        Engine.GetResult getResult = shard.get(new Engine.Get(false, new Term(UidFieldMapper.NAME, Uid.createUid("test", "1"))));
        assertTrue(getResult.exists());
        assertNotNull(getResult.searcher());
        getResult.release();
        try (Engine.Searcher searcher = shard.acquireSearcher("test")) {
            TopDocs search = searcher.searcher().search(new TermQuery(new Term("foo", "bar")), 10);
            assertEquals(search.totalHits, 1);
            search = searcher.searcher().search(new TermQuery(new Term("foobar", "bar")), 10);
            assertEquals(search.totalHits, 1);
        }
        IndexSearcherWrapper wrapper = new IndexSearcherWrapper() {
            @Override
            public DirectoryReader wrap(DirectoryReader reader) throws IOException {
                return new FieldMaskingReader("foo", reader);
            }

            @Override
            public IndexSearcher wrap(IndexSearcher searcher) throws EngineException {
                return searcher;
            }
        };

        IndexShard newShard = reinitWithWrapper(indexService, shard, wrapper);
        try {
            try (Engine.Searcher searcher = newShard.acquireSearcher("test")) {
                TopDocs search = searcher.searcher().search(new TermQuery(new Term("foo", "bar")), 10);
                assertEquals(search.totalHits, 0);
                search = searcher.searcher().search(new TermQuery(new Term("foobar", "bar")), 10);
                assertEquals(search.totalHits, 1);
            }
            getResult = newShard.get(new Engine.Get(false, new Term(UidFieldMapper.NAME, Uid.createUid("test", "1"))));
            assertTrue(getResult.exists());
            assertNotNull(getResult.searcher()); // make sure get uses the wrapped reader
            assertTrue(getResult.searcher().reader() instanceof FieldMaskingReader);
            getResult.release();
        } finally {
            newShard.close("just do it", randomBoolean());
        }
    }

    public void testSearcherWrapperWorksWithGlobaOrdinals() throws IOException {
        createIndex("test");
        ensureGreen();
        IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        IndexService indexService = indicesService.indexService("test");
        IndexShard shard = indexService.getShardOrNull(0);
        client().prepareIndex("test", "test", "0").setSource("{\"foo\" : \"bar\"}").setRefresh(true).get();
        client().prepareIndex("test", "test", "1").setSource("{\"foobar\" : \"bar\"}").setRefresh(true).get();

        IndexSearcherWrapper wrapper = new IndexSearcherWrapper() {
            @Override
            public DirectoryReader wrap(DirectoryReader reader) throws IOException {
                return new FieldMaskingReader("foo", reader);
            }

            @Override
            public IndexSearcher wrap(IndexSearcher searcher) throws EngineException {
                return searcher;
            }
        };

        IndexShard newShard = reinitWithWrapper(indexService, shard, wrapper);
        try {
            // test global ordinals are evicted
            MappedFieldType foo = newShard.mapperService().indexName("foo");
            IndexFieldData.Global ifd = shard.indexFieldDataService().getForField(foo);
            FieldDataStats before = shard.fieldData().stats("foo");
            assertThat(before.getMemorySizeInBytes(), equalTo(0l));
            FieldDataStats after = null;
            try (Engine.Searcher searcher = newShard.acquireSearcher("test")) {
                assumeTrue("we have to have more than one segment", searcher.getDirectoryReader().leaves().size() > 1);
                IndexFieldData indexFieldData = ifd.loadGlobal(searcher.getDirectoryReader());
                after = shard.fieldData().stats("foo");
                assertEquals(after.getEvictions(), before.getEvictions());
                // If a field doesn't exist an empty IndexFieldData is returned and that isn't cached:
                assertThat(after.getMemorySizeInBytes(), equalTo(0l));
            }
            assertEquals(shard.fieldData().stats("foo").getEvictions(), before.getEvictions());
            assertEquals(shard.fieldData().stats("foo").getMemorySizeInBytes(), after.getMemorySizeInBytes());
            newShard.flush(new FlushRequest().force(true).waitIfOngoing(true));
            newShard.refresh("test");
            assertEquals(shard.fieldData().stats("foo").getMemorySizeInBytes(), before.getMemorySizeInBytes());
            assertEquals(shard.fieldData().stats("foo").getEvictions(), before.getEvictions());
        } finally {
            newShard.close("just do it", randomBoolean());
        }
    }

    public void testSearchIsReleaseIfWrapperFails() throws IOException {
        createIndex("test");
        ensureGreen();
        IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        IndexService indexService = indicesService.indexService("test");
        IndexShard shard = indexService.getShardOrNull(0);
        client().prepareIndex("test", "test", "0").setSource("{\"foo\" : \"bar\"}").setRefresh(true).get();
        IndexSearcherWrapper wrapper = new IndexSearcherWrapper() {
            @Override
            public DirectoryReader wrap(DirectoryReader reader) throws IOException {
                throw new RuntimeException("boom");
            }

            public IndexSearcher wrap(IndexSearcher searcher) throws EngineException {
                return searcher;
            }
        };

        IndexShard newShard = reinitWithWrapper(indexService, shard, wrapper);
        try {
            newShard.acquireSearcher("test");
            fail("exception expected");
        } catch (RuntimeException ex) {
            //
        } finally {
            newShard.close("just do it", randomBoolean());
        }
        // test will fail due to unclosed searchers if the searcher is not released
    }

    private final IndexShard reinitWithWrapper(IndexService indexService, IndexShard shard, IndexSearcherWrapper wrapper) throws IOException {
        ShardRouting routing = new ShardRouting(shard.routingEntry());
        shard.close("simon says", true);
        NodeServicesProvider indexServices = indexService.getIndexServices();
        IndexShard newShard = new IndexShard(shard.shardId(), indexService.getIndexSettings(), shard.shardPath(), shard.store(), indexService.cache(), indexService.mapperService(), indexService.similarityService(), indexService.fieldData(), shard.getEngineFactory(), indexService.getIndexEventListener(), wrapper, indexServices);
        ShardRoutingHelper.reinit(routing);
        newShard.updateRoutingEntry(routing, false);
        DiscoveryNode localNode = new DiscoveryNode("foo", DummyTransportAddress.INSTANCE, Version.CURRENT);
        newShard.markAsRecovering("store", new RecoveryState(newShard.shardId(), routing.primary(), RecoveryState.Type.STORE, localNode, localNode));
        assertTrue(newShard.recoverFromStore(localNode));
        routing = new ShardRouting(routing);
        ShardRoutingHelper.moveToStarted(routing);
        newShard.updateRoutingEntry(routing, true);
        return newShard;
    }

}
