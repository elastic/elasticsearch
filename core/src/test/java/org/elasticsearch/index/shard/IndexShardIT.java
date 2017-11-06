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
import org.apache.lucene.store.LockObtainFailedException;
import org.apache.lucene.util.IOUtils;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.Version;
import org.elasticsearch.action.admin.indices.stats.IndexStats;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.ClusterInfoService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.InternalClusterInfoService;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.CheckedRunnable;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.lucene.uid.Versions;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.env.ShardLock;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.flush.FlushStats;
import org.elasticsearch.index.mapper.IdFieldMapper;
import org.elasticsearch.index.mapper.Mapping;
import org.elasticsearch.index.mapper.ParseContext;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.mapper.SeqNoFieldMapper;
import org.elasticsearch.index.mapper.SourceToParse;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.recovery.RecoveryState;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.DummyShardLock;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.test.IndexSettingsModule;
import org.elasticsearch.test.InternalSettingsPlugin;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static org.elasticsearch.action.support.WriteRequest.RefreshPolicy.IMMEDIATE;
import static org.elasticsearch.action.support.WriteRequest.RefreshPolicy.NONE;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_REPLICAS;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_SHARDS;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.hamcrest.Matchers.equalTo;

public class IndexShardIT extends ESSingleNodeTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return pluginList(InternalSettingsPlugin.class);
    }

    private ParsedDocument testParsedDocument(String id, String type, String routing, long seqNo,
                                              ParseContext.Document document, BytesReference source, XContentType xContentType,
                                              Mapping mappingUpdate) {
        Field uidField = new Field("_id", id, IdFieldMapper.Defaults.FIELD_TYPE);
        Field versionField = new NumericDocValuesField("_version", 0);
        SeqNoFieldMapper.SequenceIDFields seqID = SeqNoFieldMapper.SequenceIDFields.emptySeqID();
        document.add(uidField);
        document.add(versionField);
        document.add(seqID.seqNo);
        document.add(seqID.seqNoDocValue);
        document.add(seqID.primaryTerm);
        return new ParsedDocument(versionField, seqID, id, type, routing,
                Collections.singletonList(document), source, xContentType, mappingUpdate);
    }

    public void testLockTryingToDelete() throws Exception {
        createIndex("test");
        ensureGreen();
        NodeEnvironment env = getInstanceFromNode(NodeEnvironment.class);

        ClusterService cs = getInstanceFromNode(ClusterService.class);
        final Index index = cs.state().metaData().index("test").getIndex();
        Path[] shardPaths = env.availableShardPaths(new ShardId(index, 0));
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
        ShardLock sLock = new DummyShardLock(new ShardId(index, 0));
        try {
            env.deleteShardDirectoryUnderLock(sLock, IndexSettingsModule.newIndexSettings("test", Settings.EMPTY));
            fail("should not have been able to delete the directory");
        } catch (LockObtainFailedException e) {
            assertTrue("msg: " + e.getMessage(), e.getMessage().contains("unable to acquire write.lock"));
        }
    }

    public void testMarkAsInactiveTriggersSyncedFlush() throws Exception {
        assertAcked(client().admin().indices().prepareCreate("test")
            .setSettings(Settings.builder().put(SETTING_NUMBER_OF_SHARDS, 1).put(SETTING_NUMBER_OF_REPLICAS, 0)));
        client().prepareIndex("test", "test").setSource("{}", XContentType.JSON).get();
        ensureGreen("test");
        IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        indicesService.indexService(resolveIndex("test")).getShardOrNull(0).checkIdle(0);
        assertBusy(() -> {
                IndexStats indexStats = client().admin().indices().prepareStats("test").clear().get().getIndex("test");
                assertNotNull(indexStats.getShards()[0].getCommitStats().getUserData().get(Engine.SYNC_COMMIT_ID));
                indicesService.indexService(resolveIndex("test")).getShardOrNull(0).checkIdle(0);
            }
        );
        IndexStats indexStats = client().admin().indices().prepareStats("test").get().getIndex("test");
        assertNotNull(indexStats.getShards()[0].getCommitStats().getUserData().get(Engine.SYNC_COMMIT_ID));
    }

    public void testDurableFlagHasEffect() throws Exception {
        createIndex("test");
        ensureGreen();
        client().prepareIndex("test", "bar", "1").setSource("{}", XContentType.JSON).get();
        IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        IndexService test = indicesService.indexService(resolveIndex("test"));
        IndexShard shard = test.getShardOrNull(0);
        Translog translog = ShardUtilsTests.getShardEngine(shard).getTranslog();
        Predicate<Translog> needsSync = (tlog) -> {
            // we can't use tlog.needsSync() here since it also takes the global checkpoint into account
            // we explicitly want to check here if our durability checks are taken into account so we only
            // check if we are synced upto the current write location
            Translog.Location lastWriteLocation = tlog.getLastWriteLocation();
            try {
                // the lastWriteLocaltion has a Integer.MAX_VALUE size so we have to create a new one
                return tlog.ensureSynced(new Translog.Location(lastWriteLocation.generation, lastWriteLocation.translogLocation, 0));
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        };
        setDurability(shard, Translog.Durability.REQUEST);
        assertFalse(needsSync.test(translog));
        setDurability(shard, Translog.Durability.ASYNC);
        client().prepareIndex("test", "bar", "2").setSource("{}", XContentType.JSON).get();
        assertTrue(needsSync.test(translog));
        setDurability(shard, Translog.Durability.REQUEST);
        client().prepareDelete("test", "bar", "1").get();
        assertFalse(needsSync.test(translog));

        setDurability(shard, Translog.Durability.ASYNC);
        client().prepareDelete("test", "bar", "2").get();
        assertTrue(translog.syncNeeded());
        setDurability(shard, Translog.Durability.REQUEST);
        assertNoFailures(client().prepareBulk()
            .add(client().prepareIndex("test", "bar", "3").setSource("{}", XContentType.JSON))
            .add(client().prepareDelete("test", "bar", "1")).get());
        assertFalse(needsSync.test(translog));

        setDurability(shard, Translog.Durability.ASYNC);
        assertNoFailures(client().prepareBulk()
            .add(client().prepareIndex("test", "bar", "4").setSource("{}", XContentType.JSON))
            .add(client().prepareDelete("test", "bar", "3")).get());
        setDurability(shard, Translog.Durability.REQUEST);
        assertTrue(needsSync.test(translog));
    }

    private void setDurability(IndexShard shard, Translog.Durability durability) {
        client().admin().indices().prepareUpdateSettings(shard.shardId().getIndexName()).setSettings(
            Settings.builder().put(IndexSettings.INDEX_TRANSLOG_DURABILITY_SETTING.getKey(), durability.name()).build()).get();
        assertEquals(durability, shard.getTranslogDurability());
    }

    public void testUpdatePriority() {
        assertAcked(client().admin().indices().prepareCreate("test")
            .setSettings(Settings.builder().put(IndexMetaData.SETTING_PRIORITY, 200)));
        IndexService indexService = getInstanceFromNode(IndicesService.class).indexService(resolveIndex("test"));
        assertEquals(200, indexService.getIndexSettings().getSettings().getAsInt(IndexMetaData.SETTING_PRIORITY, 0).intValue());
        client().admin().indices().prepareUpdateSettings("test").setSettings(Settings.builder().put(IndexMetaData.SETTING_PRIORITY, 400)
            .build()).get();
        assertEquals(400, indexService.getIndexSettings().getSettings().getAsInt(IndexMetaData.SETTING_PRIORITY, 0).intValue());
    }

    public void testIndexDirIsDeletedWhenShardRemoved() throws Exception {
        Environment env = getInstanceFromNode(Environment.class);
        Path idxPath = env.sharedDataFile().resolve(randomAlphaOfLength(10));
        logger.info("--> idxPath: [{}]", idxPath);
        Settings idxSettings = Settings.builder()
            .put(IndexMetaData.SETTING_DATA_PATH, idxPath)
            .build();
        createIndex("test", idxSettings);
        ensureGreen("test");
        client().prepareIndex("test", "bar", "1").setSource("{}", XContentType.JSON).setRefreshPolicy(IMMEDIATE).get();
        SearchResponse response = client().prepareSearch("test").get();
        assertHitCount(response, 1L);
        client().admin().indices().prepareDelete("test").get();
        assertAllIndicesRemovedAndDeletionCompleted(Collections.singleton(getInstanceFromNode(IndicesService.class)));
        assertPathHasBeenCleared(idxPath);
    }

    public void testExpectedShardSizeIsPresent() throws InterruptedException {
        assertAcked(client().admin().indices().prepareCreate("test")
            .setSettings(Settings.builder().put(SETTING_NUMBER_OF_SHARDS, 1).put(SETTING_NUMBER_OF_REPLICAS, 0)));
        for (int i = 0; i < 50; i++) {
            client().prepareIndex("test", "test").setSource("{}", XContentType.JSON).get();
        }
        ensureGreen("test");
        InternalClusterInfoService clusterInfoService = (InternalClusterInfoService) getInstanceFromNode(ClusterInfoService.class);
        clusterInfoService.refresh();
        ClusterState state = getInstanceFromNode(ClusterService.class).state();
        Long test = clusterInfoService.getClusterInfo().getShardSize(state.getRoutingTable().index("test")
            .getShards().get(0).primaryShard());
        assertNotNull(test);
        assertTrue(test > 0);
    }

    public void testIndexCanChangeCustomDataPath() throws Exception {
        Environment env = getInstanceFromNode(Environment.class);
        Path idxPath = env.sharedDataFile().resolve(randomAlphaOfLength(10));
        final String INDEX = "idx";
        Path startDir = idxPath.resolve("start-" + randomAlphaOfLength(10));
        Path endDir = idxPath.resolve("end-" + randomAlphaOfLength(10));
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
        client().prepareIndex(INDEX, "bar", "1").setSource("{}", XContentType.JSON).setRefreshPolicy(IMMEDIATE).get();

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
        assertAllIndicesRemovedAndDeletionCompleted(Collections.singleton(getInstanceFromNode(IndicesService.class)));
        assertPathHasBeenCleared(startDir.toAbsolutePath());
        assertPathHasBeenCleared(endDir.toAbsolutePath());
    }

    public void testMaybeFlush() throws Exception {
        createIndex("test", Settings.builder().put(IndexSettings.INDEX_TRANSLOG_DURABILITY_SETTING.getKey(), Translog.Durability.REQUEST)
            .build());
        ensureGreen();
        IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        IndexService test = indicesService.indexService(resolveIndex("test"));
        IndexShard shard = test.getShardOrNull(0);
        assertFalse(shard.shouldFlush());
        client().admin().indices().prepareUpdateSettings("test").setSettings(Settings.builder()
            .put(IndexSettings.INDEX_TRANSLOG_FLUSH_THRESHOLD_SIZE_SETTING.getKey(),
                new ByteSizeValue(117 /* size of the operation + header&footer*/, ByteSizeUnit.BYTES)).build()).get();
        client().prepareIndex("test", "test", "0")
            .setSource("{}", XContentType.JSON).setRefreshPolicy(randomBoolean() ? IMMEDIATE : NONE).get();
        assertFalse(shard.shouldFlush());
        shard.applyIndexOperationOnPrimary(Versions.MATCH_ANY, VersionType.INTERNAL,
            SourceToParse.source("test", "test", "1", new BytesArray("{}"), XContentType.JSON),
            IndexRequest.UNSET_AUTO_GENERATED_TIMESTAMP, false, update -> {});
        assertTrue(shard.shouldFlush());
        final Translog translog = shard.getEngine().getTranslog();
        assertEquals(2, translog.uncommittedOperations());
        client().prepareIndex("test", "test", "2").setSource("{}", XContentType.JSON)
            .setRefreshPolicy(randomBoolean() ? IMMEDIATE : NONE).get();
        assertBusy(() -> { // this is async
            assertFalse(shard.shouldFlush());
        });
        assertEquals(0, translog.uncommittedOperations());
        translog.sync();
        long size = translog.uncommittedSizeInBytes();
        logger.info("--> current translog size: [{}] num_ops [{}] generation [{}]", translog.uncommittedSizeInBytes(),
            translog.uncommittedOperations(), translog.getGeneration());
        client().admin().indices().prepareUpdateSettings("test").setSettings(Settings.builder().put(
            IndexSettings.INDEX_TRANSLOG_FLUSH_THRESHOLD_SIZE_SETTING.getKey(), new ByteSizeValue(size, ByteSizeUnit.BYTES))
            .build()).get();
        client().prepareDelete("test", "test", "2").get();
        logger.info("--> translog size after delete: [{}] num_ops [{}] generation [{}]", translog.uncommittedSizeInBytes(),
            translog.uncommittedOperations(), translog.getGeneration());
        assertBusy(() -> { // this is async
            logger.info("--> translog size on iter  : [{}] num_ops [{}] generation [{}]", translog.uncommittedSizeInBytes(),
                translog.uncommittedOperations(), translog.getGeneration());
            assertFalse(shard.shouldFlush());
        });
        assertEquals(0, translog.uncommittedOperations());
    }

    public void testMaybeRollTranslogGeneration() throws Exception {
        final int generationThreshold = randomIntBetween(64, 512);
        final Settings settings =
                Settings
                        .builder()
                        .put("index.number_of_shards", 1)
                        .put("index.translog.generation_threshold_size", generationThreshold + "b")
                        .build();
        createIndex("test", settings);
        ensureGreen("test");
        final IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        final IndexService test = indicesService.indexService(resolveIndex("test"));
        final IndexShard shard = test.getShardOrNull(0);
        int rolls = 0;
        final Translog translog = shard.getEngine().getTranslog();
        final long generation = translog.currentFileGeneration();
        final int numberOfDocuments = randomIntBetween(32, 128);
        for (int i = 0; i < numberOfDocuments; i++) {
            assertThat(translog.currentFileGeneration(), equalTo(generation + rolls));
            final Engine.IndexResult result = shard.applyIndexOperationOnPrimary(Versions.MATCH_ANY, VersionType.INTERNAL,
                SourceToParse.source("test", "test", "1", new BytesArray("{}"), XContentType.JSON),
                IndexRequest.UNSET_AUTO_GENERATED_TIMESTAMP, false, update -> {});
            final Translog.Location location = result.getTranslogLocation();
            shard.afterWriteOperation();
            if (location.translogLocation + location.size > generationThreshold) {
                // wait until the roll completes
                assertBusy(() -> assertFalse(shard.shouldRollTranslogGeneration()));
                rolls++;
                assertThat(translog.currentFileGeneration(), equalTo(generation + rolls));
            }
        }
    }

    public void testStressMaybeFlushOrRollTranslogGeneration() throws Exception {
        createIndex("test");
        ensureGreen();
        IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        IndexService test = indicesService.indexService(resolveIndex("test"));
        final IndexShard shard = test.getShardOrNull(0);
        assertFalse(shard.shouldFlush());
        final String key;
        final boolean flush = randomBoolean();
        if (flush) {
            key = "index.translog.flush_threshold_size";
        } else {
            key = "index.translog.generation_threshold_size";
        }
        // size of the operation plus header and footer
        final Settings settings = Settings.builder().put(key, "117b").build();
        client().admin().indices().prepareUpdateSettings("test").setSettings(settings).get();
        client().prepareIndex("test", "test", "0")
                .setSource("{}", XContentType.JSON)
                .setRefreshPolicy(randomBoolean() ? IMMEDIATE : NONE)
                .get();
        assertFalse(shard.shouldFlush());
        final AtomicBoolean running = new AtomicBoolean(true);
        final int numThreads = randomIntBetween(2, 4);
        final Thread[] threads = new Thread[numThreads];
        final CyclicBarrier barrier = new CyclicBarrier(numThreads + 1);
        for (int i = 0; i < threads.length; i++) {
            threads[i] = new Thread(() -> {
                try {
                    barrier.await();
                } catch (final InterruptedException | BrokenBarrierException e) {
                    throw new RuntimeException(e);
                }
                while (running.get()) {
                    shard.afterWriteOperation();
                }
            });
            threads[i].start();
        }
        barrier.await();
        final CheckedRunnable<Exception> check;
        if (flush) {
            final FlushStats flushStats = shard.flushStats();
            final long total = flushStats.getTotal();
            client().prepareIndex("test", "test", "1").setSource("{}", XContentType.JSON).get();
            check = () -> assertEquals(total + 1, shard.flushStats().getTotal());
        } else {
            final long generation = shard.getEngine().getTranslog().currentFileGeneration();
            client().prepareIndex("test", "test", "1").setSource("{}", XContentType.JSON).get();
            check = () -> assertEquals(
                    generation + 1,
                    shard.getEngine().getTranslog().currentFileGeneration());
        }
        assertBusy(check);
        running.set(false);
        for (int i = 0; i < threads.length; i++) {
            threads[i].join();
        }
        check.run();
    }

    public void testShardHasMemoryBufferOnTranslogRecover() throws Throwable {
        createIndex("test");
        ensureGreen();
        IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        IndexService indexService = indicesService.indexService(resolveIndex("test"));
        IndexShard shard = indexService.getShardOrNull(0);
        client().prepareIndex("test", "test", "0").setSource("{\"foo\" : \"bar\"}", XContentType.JSON).get();
        client().prepareDelete("test", "test", "0").get();
        client().prepareIndex("test", "test", "1").setSource("{\"foo\" : \"bar\"}", XContentType.JSON).setRefreshPolicy(IMMEDIATE).get();

        IndexSearcherWrapper wrapper = new IndexSearcherWrapper() {};
        shard.close("simon says", false);
        AtomicReference<IndexShard> shardRef = new AtomicReference<>();
        List<Exception> failures = new ArrayList<>();
        IndexingOperationListener listener = new IndexingOperationListener() {

            @Override
            public void postIndex(ShardId shardId, Engine.Index index, Engine.IndexResult result) {
                try {
                    assertNotNull(shardRef.get());
                    // this is all IMC needs to do - check current memory and refresh
                    assertTrue(shardRef.get().getIndexBufferRAMBytesUsed() > 0);
                    shardRef.get().refresh("test");
                } catch (Exception e) {
                    failures.add(e);
                    throw e;
                }
            }


            @Override
            public void postDelete(ShardId shardId, Engine.Delete delete, Engine.DeleteResult result) {
                try {
                    assertNotNull(shardRef.get());
                    // this is all IMC needs to do - check current memory and refresh
                    assertTrue(shardRef.get().getIndexBufferRAMBytesUsed() > 0);
                    shardRef.get().refresh("test");
                } catch (Exception e) {
                    failures.add(e);
                    throw e;
                }
            }
        };
        final IndexShard newShard = newIndexShard(indexService, shard, wrapper, listener);
        shardRef.set(newShard);
        recoverShard(newShard);

        try {
            ExceptionsHelper.rethrowAndSuppress(failures);
        } finally {
            newShard.close("just do it", randomBoolean());
        }
    }


    public static final IndexShard recoverShard(IndexShard newShard) throws IOException {
        DiscoveryNode localNode = new DiscoveryNode("foo", buildNewFakeTransportAddress(), emptyMap(), emptySet(), Version.CURRENT);
        newShard.markAsRecovering("store", new RecoveryState(newShard.routingEntry(), localNode, null));
        assertTrue(newShard.recoverFromStore());
        IndexShardTestCase.updateRoutingEntry(newShard, newShard.routingEntry().moveToStarted());
        return newShard;
    }

    public static final IndexShard newIndexShard(IndexService indexService, IndexShard shard, IndexSearcherWrapper wrapper,
                                                 IndexingOperationListener... listeners) throws IOException {
        ShardRouting initializingShardRouting = getInitializingShardRouting(shard.routingEntry());
        IndexShard newShard = new IndexShard(initializingShardRouting, indexService.getIndexSettings(), shard.shardPath(),
            shard.store(), indexService.getIndexSortSupplier(), indexService.cache(), indexService.mapperService(), indexService.similarityService(),
            shard.getEngineFactory(), indexService.getIndexEventListener(), wrapper,
            indexService.getThreadPool(), indexService.getBigArrays(), null, Collections.emptyList(), Arrays.asList(listeners), () -> {});
        return newShard;
    }

    private static ShardRouting getInitializingShardRouting(ShardRouting existingShardRouting) {
        ShardRouting shardRouting = TestShardRouting.newShardRouting(existingShardRouting.shardId(),
            existingShardRouting.currentNodeId(), null, existingShardRouting.primary(), ShardRoutingState.INITIALIZING,
            existingShardRouting.allocationId());
        shardRouting = shardRouting.updateUnassigned(new UnassignedInfo(UnassignedInfo.Reason.INDEX_REOPENED, "fake recovery"),
            RecoverySource.StoreRecoverySource.EXISTING_STORE_INSTANCE);
        return shardRouting;
    }
}
