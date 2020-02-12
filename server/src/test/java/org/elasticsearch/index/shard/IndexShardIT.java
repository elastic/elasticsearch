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

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.store.LockObtainFailedException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.node.stats.NodeStats;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
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
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.CheckedFunction;
import org.elasticsearch.common.CheckedRunnable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.lucene.uid.Versions;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.env.ShardLock;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.engine.CommitStats;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.NoOpEngine;
import org.elasticsearch.index.engine.SegmentsStats;
import org.elasticsearch.index.flush.FlushStats;
import org.elasticsearch.index.mapper.SourceToParse;
import org.elasticsearch.index.seqno.RetentionLeaseSyncer;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.translog.TestTranslog;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.index.translog.TranslogStats;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.indices.breaker.CircuitBreakerStats;
import org.elasticsearch.indices.recovery.RecoveryState;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.test.DummyShardLock;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.test.IndexSettingsModule;
import org.elasticsearch.test.InternalSettingsPlugin;
import org.junit.Assert;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;
import java.util.stream.Stream;

import static com.carrotsearch.randomizedtesting.RandomizedTest.randomAsciiLettersOfLength;
import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static org.elasticsearch.action.support.WriteRequest.RefreshPolicy.IMMEDIATE;
import static org.elasticsearch.action.support.WriteRequest.RefreshPolicy.NONE;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_REPLICAS;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_SHARDS;
import static org.elasticsearch.cluster.routing.TestShardRouting.newShardRouting;
import static org.elasticsearch.index.seqno.SequenceNumbers.NO_OPS_PERFORMED;
import static org.elasticsearch.index.seqno.SequenceNumbers.UNASSIGNED_SEQ_NO;
import static org.elasticsearch.index.shard.IndexShardTestCase.getTranslog;
import static org.elasticsearch.index.shard.IndexShardTestCase.recoverFromStore;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.either;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.notNullValue;

public class IndexShardIT extends ESSingleNodeTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return pluginList(InternalSettingsPlugin.class);
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

    public void testDurableFlagHasEffect() throws Exception {
        createIndex("test");
        ensureGreen();
        client().prepareIndex("test").setId("1").setSource("{}", XContentType.JSON).get();
        IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        IndexService test = indicesService.indexService(resolveIndex("test"));
        IndexShard shard = test.getShardOrNull(0);
        Translog translog = getTranslog(shard);
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
        client().prepareIndex("test").setId("2").setSource("{}", XContentType.JSON).get();
        assertTrue(needsSync.test(translog));
        setDurability(shard, Translog.Durability.REQUEST);
        client().prepareDelete("test", "1").get();
        assertFalse(needsSync.test(translog));

        setDurability(shard, Translog.Durability.ASYNC);
        client().prepareDelete("test", "2").get();
        assertTrue(translog.syncNeeded());
        setDurability(shard, Translog.Durability.REQUEST);
        assertNoFailures(client().prepareBulk()
            .add(client().prepareIndex("test").setId("3").setSource("{}", XContentType.JSON))
            .add(client().prepareDelete("test", "1")).get());
        assertFalse(needsSync.test(translog));

        setDurability(shard, Translog.Durability.ASYNC);
        assertNoFailures(client().prepareBulk()
            .add(client().prepareIndex("test").setId("4").setSource("{}", XContentType.JSON))
            .add(client().prepareDelete("test", "3")).get());
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
        assertEquals(200,
            indexService.getIndexSettings().getSettings().getAsInt(IndexMetaData.SETTING_PRIORITY, 0).intValue());
        client().admin().indices().prepareUpdateSettings("test")
            .setSettings(Settings.builder().put(IndexMetaData.SETTING_PRIORITY, 400)
            .build()).get();
        assertEquals(400,
            indexService.getIndexSettings().getSettings().getAsInt(IndexMetaData.SETTING_PRIORITY, 0).intValue());
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
        client().prepareIndex("test").setId("1")
            .setSource("{}", XContentType.JSON).setRefreshPolicy(IMMEDIATE).get();
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
            client().prepareIndex("test").setSource("{}", XContentType.JSON).get();
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
        final String index = "test-custom-data-path";
        final Path sharedDataPath = getInstanceFromNode(Environment.class).sharedDataFile().resolve(randomAsciiLettersOfLength(10));
        final Path indexDataPath = sharedDataPath.resolve("start-" + randomAsciiLettersOfLength(10));

        logger.info("--> creating index [{}] with data_path [{}]", index, indexDataPath);
        createIndex(index, Settings.builder().put(IndexMetaData.SETTING_DATA_PATH, indexDataPath.toAbsolutePath().toString()).build());
        client().prepareIndex(index).setId("1").setSource("foo", "bar").setRefreshPolicy(IMMEDIATE).get();
        ensureGreen(index);

        assertHitCount(client().prepareSearch(index).setSize(0).get(), 1L);

        logger.info("--> closing the index [{}]", index);
        assertAcked(client().admin().indices().prepareClose(index));
        logger.info("--> index closed, re-opening...");
        assertAcked(client().admin().indices().prepareOpen(index));
        logger.info("--> index re-opened");
        ensureGreen(index);

        assertHitCount(client().prepareSearch(index).setSize(0).get(), 1L);

        // Now, try closing and changing the settings
        logger.info("--> closing the index [{}] before updating data_path", index);
        assertAcked(client().admin().indices().prepareClose(index));

        final Path newIndexDataPath = sharedDataPath.resolve("end-" + randomAlphaOfLength(10));
        IOUtils.rm(newIndexDataPath);

        logger.info("--> copying data on disk from [{}] to [{}]", indexDataPath, newIndexDataPath);
        assert Files.exists(newIndexDataPath) == false : "new index data path directory should not exist!";
        try (Stream<Path> stream = Files.walk(indexDataPath)) {
            stream.forEach(path -> {
                try {
                    if (path.endsWith(".lock") == false) {
                        Files.copy(path, newIndexDataPath.resolve(indexDataPath.relativize(path)));
                    }
                } catch (final Exception e) {
                    logger.error("Failed to copy data path directory", e);
                    fail();
                }
            });
        }

        logger.info("--> updating data_path to [{}] for index [{}]", newIndexDataPath, index);
        assertAcked(client().admin().indices().prepareUpdateSettings(index)
            .setSettings(Settings.builder().put(IndexMetaData.SETTING_DATA_PATH, newIndexDataPath.toAbsolutePath().toString()).build())
            .setIndicesOptions(IndicesOptions.fromOptions(true, false, true, true)));

        logger.info("--> settings updated and files moved, re-opening index");
        assertAcked(client().admin().indices().prepareOpen(index));
        logger.info("--> index re-opened");
        ensureGreen(index);

        assertHitCount(client().prepareSearch(index).setSize(0).get(), 1L);

        assertAcked(client().admin().indices().prepareDelete(index));
        assertAllIndicesRemovedAndDeletionCompleted(Collections.singleton(getInstanceFromNode(IndicesService.class)));
        assertPathHasBeenCleared(newIndexDataPath.toAbsolutePath());
    }

    public void testMaybeFlush() throws Exception {
        createIndex("test", Settings.builder().put(IndexSettings.INDEX_TRANSLOG_DURABILITY_SETTING.getKey(), Translog.Durability.REQUEST)
            .build());
        ensureGreen();
        IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        IndexService test = indicesService.indexService(resolveIndex("test"));
        IndexShard shard = test.getShardOrNull(0);
        assertFalse(shard.shouldPeriodicallyFlush());
        client().admin().indices().prepareUpdateSettings("test").setSettings(Settings.builder()
            .put(IndexSettings.INDEX_TRANSLOG_FLUSH_THRESHOLD_SIZE_SETTING.getKey(),
                new ByteSizeValue(135 /* size of the operation + one generation header&footer*/, ByteSizeUnit.BYTES)).build()).get();
        client().prepareIndex("test").setId("0")
            .setSource("{}", XContentType.JSON).setRefreshPolicy(randomBoolean() ? IMMEDIATE : NONE).get();
        assertFalse(shard.shouldPeriodicallyFlush());
        shard.applyIndexOperationOnPrimary(Versions.MATCH_ANY, VersionType.INTERNAL,
            new SourceToParse("test", "1", new BytesArray("{}"), XContentType.JSON),
            SequenceNumbers.UNASSIGNED_SEQ_NO, 0, IndexRequest.UNSET_AUTO_GENERATED_TIMESTAMP, false);
        assertTrue(shard.shouldPeriodicallyFlush());
        final Translog translog = getTranslog(shard);
        assertEquals(2, translog.stats().getUncommittedOperations());
        client().prepareIndex("test").setId("2").setSource("{}", XContentType.JSON)
            .setRefreshPolicy(randomBoolean() ? IMMEDIATE : NONE).get();
        assertBusy(() -> { // this is async
            assertFalse(shard.shouldPeriodicallyFlush());
            assertThat(shard.flushStats().getPeriodic(), greaterThan(0L));
        });
        assertEquals(0, translog.stats().getUncommittedOperations());
        translog.sync();
        long size = Math.max(translog.stats().getUncommittedSizeInBytes(), Translog.DEFAULT_HEADER_SIZE_IN_BYTES + 1);
        logger.info("--> current translog size: [{}] num_ops [{}] generation [{}]",
            translog.stats().getUncommittedSizeInBytes(), translog.stats().getUncommittedOperations(), translog.getGeneration());
        client().admin().indices().prepareUpdateSettings("test").setSettings(Settings.builder().put(
            IndexSettings.INDEX_TRANSLOG_FLUSH_THRESHOLD_SIZE_SETTING.getKey(), new ByteSizeValue(size, ByteSizeUnit.BYTES))
            .build()).get();
        client().prepareDelete("test", "2").get();
        logger.info("--> translog size after delete: [{}] num_ops [{}] generation [{}]",
            translog.stats().getUncommittedSizeInBytes(), translog.stats().getUncommittedOperations(), translog.getGeneration());
        assertBusy(() -> { // this is async
            final TranslogStats translogStats = translog.stats();
            final CommitStats commitStats = shard.commitStats();
            final FlushStats flushStats = shard.flushStats();
            logger.info("--> translog stats [{}] gen [{}] commit_stats [{}] flush_stats [{}/{}]",
                Strings.toString(translogStats), translog.getGeneration().translogFileGeneration,
                commitStats.getUserData(), flushStats.getPeriodic(), flushStats.getTotal());
            assertFalse(shard.shouldPeriodicallyFlush());
        });
        assertEquals(0, translog.stats().getUncommittedOperations());
    }

    public void testMaybeRollTranslogGeneration() throws Exception {
        final int generationThreshold = randomIntBetween(64, 512);
        final Settings settings =
                Settings
                        .builder()
                        .put("index.number_of_shards", 1)
                        .put("index.translog.generation_threshold_size", generationThreshold + "b")
                        .build();
        createIndex("test", settings, "test");
        ensureGreen("test");
        final IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        final IndexService test = indicesService.indexService(resolveIndex("test"));
        final IndexShard shard = test.getShardOrNull(0);
        int rolls = 0;
        final Translog translog = getTranslog(shard);
        final long generation = translog.currentFileGeneration();
        final int numberOfDocuments = randomIntBetween(32, 128);
        for (int i = 0; i < numberOfDocuments; i++) {
            assertThat(translog.currentFileGeneration(), equalTo(generation + rolls));
            final Engine.IndexResult result = shard.applyIndexOperationOnPrimary(Versions.MATCH_ANY, VersionType.INTERNAL,
                new SourceToParse("test", "1", new BytesArray("{}"), XContentType.JSON),
                SequenceNumbers.UNASSIGNED_SEQ_NO, 0, IndexRequest.UNSET_AUTO_GENERATED_TIMESTAMP, false);
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
        assertFalse(shard.shouldPeriodicallyFlush());
        final boolean flush = randomBoolean();
        final Settings settings;
        if (flush) {
            // size of the operation plus the overhead of one generation.
            settings = Settings.builder().put("index.translog.flush_threshold_size", "125b").build();
        } else {
            // size of the operation plus header and footer
            settings = Settings.builder().put("index.translog.generation_threshold_size", "117b").build();
        }
        client().admin().indices().prepareUpdateSettings("test").setSettings(settings).get();
        client().prepareIndex("test").setId("0")
                .setSource("{}", XContentType.JSON)
                .setRefreshPolicy(randomBoolean() ? IMMEDIATE : NONE)
                .get();
        assertFalse(shard.shouldPeriodicallyFlush());
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
            final FlushStats initialStats = shard.flushStats();
            client().prepareIndex("test").setId("1").setSource("{}", XContentType.JSON).get();
            check = () -> {
                assertFalse(shard.shouldPeriodicallyFlush());
                final FlushStats currentStats = shard.flushStats();
                String msg = String.format(Locale.ROOT, "flush stats: total=[%d vs %d], periodic=[%d vs %d]",
                    initialStats.getTotal(), currentStats.getTotal(), initialStats.getPeriodic(), currentStats.getPeriodic());
                assertThat(msg, currentStats.getPeriodic(),
                    either(equalTo(initialStats.getPeriodic() + 1)).or(equalTo(initialStats.getPeriodic() + 2)));
                assertThat(msg, currentStats.getTotal(),
                    either(equalTo(initialStats.getTotal() + 1)).or(equalTo(initialStats.getTotal() + 2)));
            };
        } else {
            final long generation = getTranslog(shard).currentFileGeneration();
            client().prepareIndex("test").setId("1").setSource("{}", XContentType.JSON).get();
            check = () -> {
                assertFalse(shard.shouldRollTranslogGeneration());
                assertEquals(generation + 1, getTranslog(shard).currentFileGeneration());
            };
        }
        assertBusy(check);
        running.set(false);
        for (int i = 0; i < threads.length; i++) {
            threads[i].join();
        }
        check.run();
    }

    public void testFlushStats() throws Exception {
        final IndexService indexService = createIndex("test");
        ensureGreen();
        Settings settings = Settings.builder().put("index.translog.flush_threshold_size", "" + between(200, 300) + "b").build();
        client().admin().indices().prepareUpdateSettings("test").setSettings(settings).get();
        final int numDocs = between(10, 100);
        for (int i = 0; i < numDocs; i++) {
            client().prepareIndex("test").setId(Integer.toString(i)).setSource("{}", XContentType.JSON).get();
        }
        // A flush stats may include the new total count but the old period count - assert eventually.
        assertBusy(() -> {
            final FlushStats flushStats = client().admin().indices().prepareStats("test").clear().setFlush(true).get().getTotal().flush;
            assertThat(flushStats.getPeriodic(), allOf(equalTo(flushStats.getTotal()), greaterThan(0L)));
        });
        assertBusy(() -> assertThat(indexService.getShard(0).shouldPeriodicallyFlush(), equalTo(false)));
        settings = Settings.builder().put("index.translog.flush_threshold_size", (String) null).build();
        client().admin().indices().prepareUpdateSettings("test").setSettings(settings).get();

        client().prepareIndex("test").setId(UUIDs.randomBase64UUID()).setSource("{}", XContentType.JSON).get();
        client().admin().indices().prepareFlush("test").setForce(randomBoolean()).setWaitIfOngoing(true).get();
        final FlushStats flushStats = client().admin().indices().prepareStats("test").clear().setFlush(true).get().getTotal().flush;
        assertThat(flushStats.getTotal(), greaterThan(flushStats.getPeriodic()));
    }

    public void testShardHasMemoryBufferOnTranslogRecover() throws Throwable {
        createIndex("test");
        ensureGreen();
        IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        IndexService indexService = indicesService.indexService(resolveIndex("test"));
        IndexShard shard = indexService.getShardOrNull(0);
        client().prepareIndex("test").setId("0").setSource("{\"foo\" : \"bar\"}", XContentType.JSON).get();
        client().prepareDelete("test", "0").get();
        client().prepareIndex("test").setId("1").setSource("{\"foo\" : \"bar\"}", XContentType.JSON)
            .setRefreshPolicy(IMMEDIATE).get();

        CheckedFunction<DirectoryReader, DirectoryReader, IOException> wrapper = directoryReader -> directoryReader;
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
        final IndexShard newShard = newIndexShard(indexService, shard, wrapper, getInstanceFromNode(CircuitBreakerService.class), listener);
        shardRef.set(newShard);
        recoverShard(newShard);

        try {
            ExceptionsHelper.rethrowAndSuppress(failures);
        } finally {
            newShard.close("just do it", randomBoolean());
        }
    }

    /** Check that the accounting breaker correctly matches the segments API for memory usage */
    private void checkAccountingBreaker() {
        CircuitBreakerService breakerService = getInstanceFromNode(CircuitBreakerService.class);
        CircuitBreaker acctBreaker = breakerService.getBreaker(CircuitBreaker.ACCOUNTING);
        long usedMem = acctBreaker.getUsed();
        assertThat(usedMem, greaterThan(0L));
        NodesStatsResponse response = client().admin().cluster().prepareNodesStats().setIndices(true).setBreaker(true).get();
        NodeStats stats = response.getNodes().get(0);
        assertNotNull(stats);
        SegmentsStats segmentsStats = stats.getIndices().getSegments();
        CircuitBreakerStats breakerStats = stats.getBreaker().getStats(CircuitBreaker.ACCOUNTING);
        assertEquals(usedMem, segmentsStats.getMemoryInBytes());
        assertEquals(usedMem, breakerStats.getEstimated());
    }

    public void testCircuitBreakerIncrementedByIndexShard() throws Exception {
        client().admin().cluster().prepareUpdateSettings()
                .setTransientSettings(Settings.builder().put("network.breaker.inflight_requests.overhead", 0.0)).get();

        // Generate a couple of segments
        client().prepareIndex("test").setId("1")
            .setSource("{\"foo\":\"" + randomAlphaOfLength(100) + "\"}", XContentType.JSON)
            .setRefreshPolicy(IMMEDIATE).get();
        // Use routing so 2 documents are guaranteed to be on the same shard
        String routing = randomAlphaOfLength(5);
        client().prepareIndex("test").setId("2")
            .setSource("{\"foo\":\"" + randomAlphaOfLength(100) + "\"}", XContentType.JSON)
            .setRefreshPolicy(IMMEDIATE).setRouting(routing).get();
        client().prepareIndex("test").setId("3")
            .setSource("{\"foo\":\"" + randomAlphaOfLength(100) + "\"}", XContentType.JSON)
            .setRefreshPolicy(IMMEDIATE).setRouting(routing).get();

        checkAccountingBreaker();
        // Test that force merging causes the breaker to be correctly adjusted
        logger.info("--> force merging to a single segment");
        client().admin().indices().prepareForceMerge("test").setMaxNumSegments(1).setFlush(randomBoolean()).get();
        client().admin().indices().prepareRefresh().get();
        checkAccountingBreaker();

        client().admin().cluster().prepareUpdateSettings()
                .setTransientSettings(Settings.builder().put("indices.breaker.total.limit", "1kb")).get();

        // Test that we're now above the parent limit due to the segments
        Exception e = expectThrows(Exception.class,
                () -> client().prepareSearch("test")
                    .addAggregation(AggregationBuilders.terms("foo_terms").field("foo.keyword")).get());
        logger.info("--> got an expected exception", e);
        assertThat(e.getCause(), notNullValue());
        assertThat(e.getCause().getMessage(), containsString("[parent] Data too large, data for [<agg [foo_terms]>]"));

        client().admin().cluster().prepareUpdateSettings()
                .setTransientSettings(Settings.builder()
                        .putNull("indices.breaker.total.limit")
                        .putNull("network.breaker.inflight_requests.overhead")).get();

        // Test that deleting the index causes the breaker to correctly be decremented
        logger.info("--> deleting index");
        client().admin().indices().prepareDelete("test").get();

        // Accounting breaker should now be 0
        CircuitBreakerService breakerService = getInstanceFromNode(CircuitBreakerService.class);
        CircuitBreaker acctBreaker = breakerService.getBreaker(CircuitBreaker.ACCOUNTING);
        assertThat(acctBreaker.getUsed(), equalTo(0L));
    }

    public static final IndexShard recoverShard(IndexShard newShard) throws IOException {
        DiscoveryNode localNode = new DiscoveryNode("foo", buildNewFakeTransportAddress(), emptyMap(), emptySet(), Version.CURRENT);
        newShard.markAsRecovering("store", new RecoveryState(newShard.routingEntry(), localNode, null));
        recoverFromStore(newShard);
        IndexShardTestCase.updateRoutingEntry(newShard, newShard.routingEntry().moveToStarted());
        return newShard;
    }

    public static final IndexShard newIndexShard(
        final IndexService indexService,
        final IndexShard shard, CheckedFunction<DirectoryReader, DirectoryReader, IOException> wrapper,
        final CircuitBreakerService cbs,
        final IndexingOperationListener... listeners) throws IOException {
        ShardRouting initializingShardRouting = getInitializingShardRouting(shard.routingEntry());
        return new IndexShard(
                initializingShardRouting,
                indexService.getIndexSettings(),
                shard.shardPath(),
                shard.store(),
                indexService.getIndexSortSupplier(),
                indexService.cache(),
                indexService.mapperService(),
                indexService.similarityService(),
                shard.getEngineFactory(),
                indexService.getIndexEventListener(),
                wrapper,
                indexService.getThreadPool(),
                indexService.getBigArrays(),
                null,
                Collections.emptyList(),
                Arrays.asList(listeners),
                () -> {},
                RetentionLeaseSyncer.EMPTY,
                cbs);
    }

    private static ShardRouting getInitializingShardRouting(ShardRouting existingShardRouting) {
        ShardRouting shardRouting = newShardRouting(existingShardRouting.shardId(),
            existingShardRouting.currentNodeId(), null, existingShardRouting.primary(), ShardRoutingState.INITIALIZING,
            existingShardRouting.allocationId());
        shardRouting = shardRouting.updateUnassigned(new UnassignedInfo(UnassignedInfo.Reason.INDEX_REOPENED, "fake recovery"),
            RecoverySource.ExistingStoreRecoverySource.INSTANCE);
        return shardRouting;
    }

    public void testGlobalCheckpointListeners() throws Exception {
        createIndex("test", Settings.builder()
            .put("index.number_of_shards", 1)
            .put("index.number_of_replicas", 0).build());
        ensureGreen();
        final IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        final IndexService test = indicesService.indexService(resolveIndex("test"));
        final IndexShard shard = test.getShardOrNull(0);
        final int numberOfUpdates = randomIntBetween(1, 128);
        for (int i = 0; i < numberOfUpdates; i++) {
            final int index = i;
            final AtomicLong globalCheckpoint = new AtomicLong();
            shard.addGlobalCheckpointListener(
                    i,
                    (g, e) -> {
                        assertThat(g, greaterThanOrEqualTo(NO_OPS_PERFORMED));
                        assertNull(e);
                        globalCheckpoint.set(g);
                    },
                    null);
            client().prepareIndex("test").setId(Integer.toString(i)).setSource("{}", XContentType.JSON).get();
            assertBusy(() -> assertThat(globalCheckpoint.get(), equalTo((long) index)));
            // adding a listener expecting a lower global checkpoint should fire immediately
            final AtomicLong immediateGlobalCheckpint = new AtomicLong();
            shard.addGlobalCheckpointListener(
                    randomLongBetween(0, i),
                    (g, e) -> {
                        assertThat(g, greaterThanOrEqualTo(NO_OPS_PERFORMED));
                        assertNull(e);
                        immediateGlobalCheckpint.set(g);
                    },
                    null);
            assertBusy(() -> assertThat(immediateGlobalCheckpint.get(), equalTo((long) index)));
        }
        final AtomicBoolean invoked = new AtomicBoolean();
        shard.addGlobalCheckpointListener(
                numberOfUpdates,
                (g, e) -> {
                    invoked.set(true);
                    assertThat(g, equalTo(UNASSIGNED_SEQ_NO));
                    assertThat(e, instanceOf(IndexShardClosedException.class));
                    assertThat(((IndexShardClosedException)e).getShardId(), equalTo(shard.shardId()));
                },
                null);
        shard.close("closed", randomBoolean());
        assertBusy(() -> assertTrue(invoked.get()));
    }

    public void testGlobalCheckpointListenerTimeout() throws InterruptedException {
        createIndex("test", Settings.builder()
            .put("index.number_of_shards", 1)
            .put("index.number_of_replicas", 0).build());
        ensureGreen();
        final IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        final IndexService test = indicesService.indexService(resolveIndex("test"));
        final IndexShard shard = test.getShardOrNull(0);
        final AtomicBoolean notified = new AtomicBoolean();
        final CountDownLatch latch = new CountDownLatch(1);
        final TimeValue timeout = TimeValue.timeValueMillis(randomIntBetween(1, 50));
        shard.addGlobalCheckpointListener(
                0,
                (g, e) -> {
                    try {
                        notified.set(true);
                        assertThat(g, equalTo(UNASSIGNED_SEQ_NO));
                        assertNotNull(e);
                        assertThat(e, instanceOf(TimeoutException.class));
                        assertThat(e.getMessage(), equalTo(timeout.getStringRep()));
                    } finally {
                        latch.countDown();
                    }
                },
                timeout);
        latch.await();
        assertTrue(notified.get());
    }

    public void testInvalidateIndicesRequestCacheWhenRollbackEngine() throws Exception {
        createIndex("test", Settings.builder()
            .put("index.number_of_shards", 1)
            .put("index.number_of_replicas", 0)
            .put("index.refresh_interval", -1).build());
        ensureGreen();
        final IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        final IndexShard shard = indicesService.getShardOrNull(new ShardId(resolveIndex("test"), 0));
        final SearchRequest countRequest = new SearchRequest("test").source(new SearchSourceBuilder().size(0));
        final long numDocs = between(10, 20);
        for (int i = 0; i < numDocs; i++) {
            client().prepareIndex("test").setId(Integer.toString(i)).setSource("{}", XContentType.JSON).get();
            if (randomBoolean()) {
                shard.refresh("test");
            }
        }
        shard.refresh("test");
        assertThat(client().search(countRequest).actionGet().getHits().getTotalHits().value, equalTo(numDocs));
        assertThat(shard.getLocalCheckpoint(), equalTo(shard.seqNoStats().getMaxSeqNo()));

        final CountDownLatch engineResetLatch = new CountDownLatch(1);
        shard.acquireAllPrimaryOperationsPermits(ActionListener.wrap(r -> {
            try {
                shard.resetEngineToGlobalCheckpoint();
            } finally {
                r.close();
                engineResetLatch.countDown();
            }
        }, Assert::assertNotNull), TimeValue.timeValueMinutes(1L));
        engineResetLatch.await();

        final long moreDocs = between(10, 20);
        for (int i = 0; i < moreDocs; i++) {
            client().prepareIndex("test").setId(Long.toString(i + numDocs)).setSource("{}", XContentType.JSON).get();
            if (randomBoolean()) {
                shard.refresh("test");
            }
        }
        shard.refresh("test");
        try (Engine.Searcher searcher = shard.acquireSearcher("test")) {
            assertThat("numDocs=" + numDocs + " moreDocs=" + moreDocs,
                (long) searcher.getIndexReader().numDocs(), equalTo(numDocs + moreDocs));
        }
        assertThat("numDocs=" + numDocs + " moreDocs=" + moreDocs,
            client().search(countRequest).actionGet().getHits().getTotalHits().value, equalTo(numDocs + moreDocs));
    }

    public void testShardChangesWithDefaultDocType() throws Exception {
        Settings settings = Settings.builder()
            .put("index.number_of_shards", 1)
            .put("index.number_of_replicas", 0)
            .put("index.translog.flush_threshold_size", "512mb") // do not flush
            .put("index.soft_deletes.enabled", true).build();
        IndexService indexService = createIndex("index", settings, "user_doc", "title", "type=keyword");
        int numOps = between(1, 10);
        for (int i = 0; i < numOps; i++) {
            if (randomBoolean()) {
                client().prepareIndex("index").setId(randomFrom("1", "2"))
                    .setSource("{}", XContentType.JSON).get();
            } else {
                client().prepareDelete("index", randomFrom("1", "2")).get();
            }
        }
        IndexShard shard = indexService.getShard(0);
        try (Translog.Snapshot luceneSnapshot = shard.newChangesSnapshot("test", 0, numOps - 1, true);
             Translog.Snapshot translogSnapshot = getTranslog(shard).newSnapshot()) {
            List<Translog.Operation> opsFromLucene = TestTranslog.drainSnapshot(luceneSnapshot, true);
            List<Translog.Operation> opsFromTranslog = TestTranslog.drainSnapshot(translogSnapshot, true);
            assertThat(opsFromLucene, equalTo(opsFromTranslog));
        }
    }

    /**
     * Test that the {@link org.elasticsearch.index.engine.NoOpEngine} takes precedence over other
     * engine factories if the index is closed.
     */
    public void testNoOpEngineFactoryTakesPrecedence() {
        final String indexName = "closed-index";
        createIndex(indexName, Settings.builder().put("index.number_of_shards", 1).put("index.number_of_replicas", 0).build());
        ensureGreen();

        assertAcked(client().admin().indices().prepareClose(indexName));

        final ClusterService clusterService = getInstanceFromNode(ClusterService.class);
        final ClusterState clusterState = clusterService.state();

        final IndexMetaData indexMetaData = clusterState.metaData().index(indexName);
        final IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        final IndexService indexService = indicesService.indexServiceSafe(indexMetaData.getIndex());

        for (IndexShard indexShard : indexService) {
            assertThat(indexShard.getEngine(), instanceOf(NoOpEngine.class));
        }
    }

    /**
     * Asserts that there are no files in the specified path
     */
    private void assertPathHasBeenCleared(Path path) {
        logger.info("--> checking that [{}] has been cleared", path);
        int count = 0;
        StringBuilder sb = new StringBuilder();
        sb.append("[");
        if (Files.exists(path)) {
            try (DirectoryStream<Path> stream = Files.newDirectoryStream(path)) {
                for (Path file : stream) {
                    // Skip files added by Lucene's ExtraFS
                    if (file.getFileName().toString().startsWith("extra")) {
                        continue;
                    }
                    logger.info("--> found file: [{}]", file.toAbsolutePath().toString());
                    if (Files.isDirectory(file)) {
                        assertPathHasBeenCleared(file);
                    } else if (Files.isRegularFile(file)) {
                        count++;
                        sb.append(file.toAbsolutePath().toString());
                        sb.append("\n");
                    }
                }
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
        sb.append("]");
        assertThat(count + " files exist that should have been cleaned:\n" + sb.toString(), count, equalTo(0));
    }

    private static void assertAllIndicesRemovedAndDeletionCompleted(Iterable<IndicesService> indicesServices) throws Exception {
        for (IndicesService indicesService : indicesServices) {
            assertBusy(() -> assertFalse(indicesService.iterator().hasNext()), 1, TimeUnit.MINUTES);
            assertBusy(() -> assertFalse(indicesService.hasUncompletedPendingDeletes()), 1, TimeUnit.MINUTES);
        }
    }
}
