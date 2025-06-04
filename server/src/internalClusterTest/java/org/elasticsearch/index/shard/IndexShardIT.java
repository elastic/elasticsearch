/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.index.shard;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.store.LockObtainFailedException;
import org.elasticsearch.exception.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.ClusterInfoService;
import org.elasticsearch.cluster.ClusterInfoServiceUtils;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.InternalClusterInfoService;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.lucene.uid.Versions;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.CheckedFunction;
import org.elasticsearch.core.CheckedRunnable;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.env.ShardLock;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.engine.CommitStats;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.NoOpEngine;
import org.elasticsearch.index.flush.FlushStats;
import org.elasticsearch.index.mapper.MapperMetrics;
import org.elasticsearch.index.mapper.SourceToParse;
import org.elasticsearch.index.seqno.RetentionLeaseSyncer;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.translog.TestTranslog;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.index.translog.TranslogStats;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.indices.recovery.RecoveryState;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.test.DummyShardLock;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.test.IndexSettingsModule;
import org.elasticsearch.test.InternalSettingsPlugin;
import org.elasticsearch.xcontent.XContentType;
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
import java.util.Optional;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;
import java.util.stream.Stream;

import static com.carrotsearch.randomizedtesting.RandomizedTest.randomAsciiLettersOfLength;
import static java.util.Collections.emptySet;
import static org.elasticsearch.action.support.WriteRequest.RefreshPolicy.IMMEDIATE;
import static org.elasticsearch.action.support.WriteRequest.RefreshPolicy.NONE;
import static org.elasticsearch.cluster.routing.TestShardRouting.shardRoutingBuilder;
import static org.elasticsearch.index.shard.IndexShardTestCase.closeShardNoCheck;
import static org.elasticsearch.index.shard.IndexShardTestCase.getTranslog;
import static org.elasticsearch.index.shard.IndexShardTestCase.recoverFromStore;
import static org.elasticsearch.test.LambdaMatchers.falseWith;
import static org.elasticsearch.test.LambdaMatchers.trueWith;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.either;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.instanceOf;

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
        final Index index = cs.state().metadata().getProject().index("test").getIndex();
        Path[] shardPaths = env.availableShardPaths(new ShardId(index, 0));
        logger.info("--> paths: [{}]", (Object) shardPaths);
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
        final ShardLock sLock = new DummyShardLock(new ShardId(index, 0));
        final IndexSettings indexSettings = IndexSettingsModule.newIndexSettings("test", Settings.EMPTY);

        final LockObtainFailedException exception = expectThrows(
            LockObtainFailedException.class,
            () -> env.deleteShardDirectoryUnderLock(sLock, indexSettings, indexPaths -> {
                assert false : "should not be called " + indexPaths;
            })
        );
        assertThat(exception.getMessage(), exception.getMessage(), containsString("unable to acquire write.lock"));
    }

    public void testDurableFlagHasEffect() {
        createIndex("test");
        ensureGreen();
        prepareIndex("test").setId("1").setSource("{}", XContentType.JSON).get();
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
                return tlog.ensureSynced(
                    new Translog.Location(lastWriteLocation.generation(), lastWriteLocation.translogLocation(), 0),
                    SequenceNumbers.UNASSIGNED_SEQ_NO
                );
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        };
        setDurability(shard, Translog.Durability.REQUEST);
        assertThat(needsSync, falseWith(translog));
        setDurability(shard, Translog.Durability.ASYNC);
        prepareIndex("test").setId("2").setSource("{}", XContentType.JSON).get();
        assertThat(needsSync, trueWith(translog));
        setDurability(shard, Translog.Durability.REQUEST);
        client().prepareDelete("test", "1").get();
        assertThat(needsSync, falseWith(translog));

        setDurability(shard, Translog.Durability.ASYNC);
        client().prepareDelete("test", "2").get();
        assertTrue(translog.syncNeeded());
        setDurability(shard, Translog.Durability.REQUEST);
        assertNoFailures(
            client().prepareBulk()
                .add(prepareIndex("test").setId("3").setSource("{}", XContentType.JSON))
                .add(client().prepareDelete("test", "1"))
                .get()
        );
        assertThat(needsSync, falseWith(translog));

        setDurability(shard, Translog.Durability.ASYNC);
        assertNoFailures(
            client().prepareBulk()
                .add(prepareIndex("test").setId("4").setSource("{}", XContentType.JSON))
                .add(client().prepareDelete("test", "3"))
                .get()
        );
        setDurability(shard, Translog.Durability.REQUEST);
        assertThat(needsSync, trueWith(translog));
    }

    private void setDurability(IndexShard shard, Translog.Durability durability) {
        indicesAdmin().prepareUpdateSettings(shard.shardId().getIndexName())
            .setSettings(Settings.builder().put(IndexSettings.INDEX_TRANSLOG_DURABILITY_SETTING.getKey(), durability.name()).build())
            .get();
        assertEquals(durability, shard.getTranslogDurability());
    }

    public void testUpdatePriority() {
        assertAcked(indicesAdmin().prepareCreate("test").setSettings(Settings.builder().put(IndexMetadata.SETTING_PRIORITY, 200)));
        IndexService indexService = getInstanceFromNode(IndicesService.class).indexService(resolveIndex("test"));
        assertEquals(200, indexService.getIndexSettings().getSettings().getAsInt(IndexMetadata.SETTING_PRIORITY, 0).intValue());
        indicesAdmin().prepareUpdateSettings("test").setSettings(Settings.builder().put(IndexMetadata.SETTING_PRIORITY, 400).build()).get();
        assertEquals(400, indexService.getIndexSettings().getSettings().getAsInt(IndexMetadata.SETTING_PRIORITY, 0).intValue());
    }

    public void testIndexDirIsDeletedWhenShardRemoved() throws Exception {
        Environment env = getInstanceFromNode(Environment.class);
        Path idxPath = env.sharedDataDir().resolve(randomAlphaOfLength(10));
        logger.info("--> idxPath: [{}]", idxPath);
        Settings idxSettings = Settings.builder().put(IndexMetadata.SETTING_DATA_PATH, idxPath).build();
        createIndex("test", idxSettings);
        ensureGreen("test");
        prepareIndex("test").setId("1").setSource("{}", XContentType.JSON).setRefreshPolicy(IMMEDIATE).get();
        assertHitCount(client().prepareSearch("test"), 1L);
        indicesAdmin().prepareDelete("test").get();
        awaitIndexShardCloseAsyncTasks();
        assertAllIndicesRemovedAndDeletionCompleted(Collections.singleton(getInstanceFromNode(IndicesService.class)));
        assertPathHasBeenCleared(idxPath);
    }

    public void testExpectedShardSizeIsPresent() throws InterruptedException {
        assertAcked(indicesAdmin().prepareCreate("test").setSettings(indexSettings(1, 0)));
        for (int i = 0; i < 50; i++) {
            prepareIndex("test").setSource("{}", XContentType.JSON).get();
        }
        ensureGreen("test");
        InternalClusterInfoService clusterInfoService = (InternalClusterInfoService) getInstanceFromNode(ClusterInfoService.class);
        ClusterInfoServiceUtils.refresh(clusterInfoService);
        ClusterState state = getInstanceFromNode(ClusterService.class).state();
        ShardRouting shardRouting = state.getRoutingTable().index("test").shard(0).primaryShard();
        Long test = clusterInfoService.getClusterInfo().getShardSize(shardRouting);
        assertNotNull(test);
        assertTrue(test > 0);

        Optional<Long> dataSetSize = clusterInfoService.getClusterInfo().getShardDataSetSize(shardRouting.shardId());
        assertTrue(dataSetSize.isPresent());
        assertThat(dataSetSize.get(), greaterThan(0L));
    }

    public void testIndexCanChangeCustomDataPath() throws Exception {
        final String index = "test-custom-data-path";
        final Path sharedDataPath = getInstanceFromNode(Environment.class).sharedDataDir().resolve(randomAsciiLettersOfLength(10));
        final Path indexDataPath = sharedDataPath.resolve("start-" + randomAsciiLettersOfLength(10));

        logger.info("--> creating index [{}] with data_path [{}]", index, indexDataPath);
        createIndex(index, Settings.builder().put(IndexMetadata.SETTING_DATA_PATH, indexDataPath.toAbsolutePath().toString()).build());
        prepareIndex(index).setId("1").setSource("foo", "bar").setRefreshPolicy(IMMEDIATE).get();
        ensureGreen(index);

        assertHitCount(client().prepareSearch(index).setSize(0), 1L);

        logger.info("--> closing the index [{}]", index);
        assertAcked(indicesAdmin().prepareClose(index));
        logger.info("--> index closed, re-opening...");
        assertAcked(indicesAdmin().prepareOpen(index));
        logger.info("--> index re-opened");
        ensureGreen(index);

        assertHitCount(client().prepareSearch(index).setSize(0), 1L);

        // Now, try closing and changing the settings
        logger.info("--> closing the index [{}] before updating data_path", index);
        assertAcked(indicesAdmin().prepareClose(index));
        awaitIndexShardCloseAsyncTasks();

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
        assertAcked(
            indicesAdmin().prepareUpdateSettings(index)
                .setSettings(Settings.builder().put(IndexMetadata.SETTING_DATA_PATH, newIndexDataPath.toAbsolutePath().toString()).build())
                .setIndicesOptions(IndicesOptions.fromOptions(true, false, true, true))
        );

        logger.info("--> settings updated and files moved, re-opening index");
        assertAcked(indicesAdmin().prepareOpen(index));
        logger.info("--> index re-opened");
        ensureGreen(index);

        assertHitCount(client().prepareSearch(index).setSize(0), 1L);

        assertAcked(indicesAdmin().prepareDelete(index));
        awaitIndexShardCloseAsyncTasks();
        assertAllIndicesRemovedAndDeletionCompleted(Collections.singleton(getInstanceFromNode(IndicesService.class)));
        assertPathHasBeenCleared(newIndexDataPath.toAbsolutePath());
    }

    public void testMaybeFlush() throws Exception {
        createIndex(
            "test",
            Settings.builder().put(IndexSettings.INDEX_TRANSLOG_DURABILITY_SETTING.getKey(), Translog.Durability.REQUEST).build()
        );
        ensureGreen();
        IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        IndexService test = indicesService.indexService(resolveIndex("test"));
        IndexShard shard = test.getShardOrNull(0);
        assertFalse(shard.shouldPeriodicallyFlush());
        indicesAdmin().prepareUpdateSettings("test")
            .setSettings(
                Settings.builder()
                    .put(
                        IndexSettings.INDEX_TRANSLOG_FLUSH_THRESHOLD_SIZE_SETTING.getKey(),
                        ByteSizeValue.of(135 /* size of the operation + one generation header&footer*/, ByteSizeUnit.BYTES)
                    )
                    .build()
            )
            .get();
        prepareIndex("test").setId("0").setSource("{}", XContentType.JSON).setRefreshPolicy(randomBoolean() ? IMMEDIATE : NONE).get();
        assertFalse(shard.shouldPeriodicallyFlush());
        shard.applyIndexOperationOnPrimary(
            Versions.MATCH_ANY,
            VersionType.INTERNAL,
            new SourceToParse("1", new BytesArray("{}"), XContentType.JSON),
            SequenceNumbers.UNASSIGNED_SEQ_NO,
            0,
            IndexRequest.UNSET_AUTO_GENERATED_TIMESTAMP,
            false
        );
        assertTrue(shard.shouldPeriodicallyFlush());
        final Translog translog = getTranslog(shard);
        assertEquals(2, translog.stats().getUncommittedOperations());
        assertThat(shard.flushStats().getTotal(), equalTo(0L));
        prepareIndex("test").setId("2").setSource("{}", XContentType.JSON).setRefreshPolicy(randomBoolean() ? IMMEDIATE : NONE).get();
        assertThat(shard.getLastKnownGlobalCheckpoint(), equalTo(2L));
        assertBusy(() -> { // this is async
            assertFalse(shard.shouldPeriodicallyFlush());
            assertThat(shard.flushStats().getPeriodic(), equalTo(1L));
            assertThat(shard.flushStats().getTotal(), equalTo(1L));
        });
        shard.sync();
        assertThat(shard.getLastSyncedGlobalCheckpoint(), equalTo(2L));
        assertThat("last commit [" + shard.commitStats().getUserData() + "]", translog.stats().getUncommittedOperations(), equalTo(0));
        long size = Math.max(translog.stats().getUncommittedSizeInBytes(), Translog.DEFAULT_HEADER_SIZE_IN_BYTES + 1);
        logger.info(
            "--> current translog size: [{}] num_ops [{}] generation [{}]",
            translog.stats().getUncommittedSizeInBytes(),
            translog.stats().getUncommittedOperations(),
            translog.getGeneration()
        );
        indicesAdmin().prepareUpdateSettings("test")
            .setSettings(
                Settings.builder()
                    .put(IndexSettings.INDEX_TRANSLOG_FLUSH_THRESHOLD_SIZE_SETTING.getKey(), ByteSizeValue.of(size, ByteSizeUnit.BYTES))
                    .build()
            )
            .get();
        client().prepareDelete("test", "2").get();
        logger.info(
            "--> translog size after delete: [{}] num_ops [{}] generation [{}]",
            translog.stats().getUncommittedSizeInBytes(),
            translog.stats().getUncommittedOperations(),
            translog.getGeneration()
        );
        assertBusy(() -> { // this is async
            final TranslogStats translogStats = translog.stats();
            final CommitStats commitStats = shard.commitStats();
            final FlushStats flushStats = shard.flushStats();
            logger.info(
                "--> translog stats [{}] gen [{}] commit_stats [{}] flush_stats [{}/{}]",
                Strings.toString(translogStats),
                translog.getGeneration().translogFileGeneration(),
                commitStats.getUserData(),
                flushStats.getPeriodic(),
                flushStats.getTotal()
            );
            assertFalse(shard.shouldPeriodicallyFlush());
        });
        shard.sync();
        assertEquals(0, translog.stats().getUncommittedOperations());
    }

    public void testMaybeRollTranslogGeneration() throws Exception {
        final int generationThreshold = randomIntBetween(64, 512);
        final Settings settings = Settings.builder()
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
            final Engine.IndexResult result = shard.applyIndexOperationOnPrimary(
                Versions.MATCH_ANY,
                VersionType.INTERNAL,
                new SourceToParse("1", new BytesArray("{}"), XContentType.JSON),
                SequenceNumbers.UNASSIGNED_SEQ_NO,
                0,
                IndexRequest.UNSET_AUTO_GENERATED_TIMESTAMP,
                false
            );
            final Translog.Location location = result.getTranslogLocation();
            shard.afterWriteOperation();
            if (location.translogLocation() + location.size() > generationThreshold) {
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
        indicesAdmin().prepareUpdateSettings("test").setSettings(settings).get();
        prepareIndex("test").setId("0").setSource("{}", XContentType.JSON).setRefreshPolicy(randomBoolean() ? IMMEDIATE : NONE).get();
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
            prepareIndex("test").setId("1").setSource("{}", XContentType.JSON).get();
            check = () -> {
                assertFalse(shard.shouldPeriodicallyFlush());
                final FlushStats currentStats = shard.flushStats();
                String msg = String.format(
                    Locale.ROOT,
                    "flush stats: total=[%d vs %d], periodic=[%d vs %d]",
                    initialStats.getTotal(),
                    currentStats.getTotal(),
                    initialStats.getPeriodic(),
                    currentStats.getPeriodic()
                );
                assertThat(
                    msg,
                    currentStats.getPeriodic(),
                    either(equalTo(initialStats.getPeriodic() + 1)).or(equalTo(initialStats.getPeriodic() + 2))
                );
                assertThat(
                    msg,
                    currentStats.getTotal(),
                    either(equalTo(initialStats.getTotal() + 1)).or(equalTo(initialStats.getTotal() + 2))
                );
            };
        } else {
            final long generation = getTranslog(shard).currentFileGeneration();
            prepareIndex("test").setId("1").setSource("{}", XContentType.JSON).get();
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
        indicesAdmin().prepareUpdateSettings("test").setSettings(settings).get();
        final int numDocs = between(10, 100);
        for (int i = 0; i < numDocs; i++) {
            prepareIndex("test").setId(Integer.toString(i)).setSource("{}", XContentType.JSON).get();
        }
        // A flush stats may include the new total count but the old period count - assert eventually.
        assertBusy(() -> {
            final FlushStats flushStats = indicesAdmin().prepareStats("test").clear().setFlush(true).get().getTotal().flush;
            assertThat(flushStats.getPeriodic(), allOf(equalTo(flushStats.getTotal()), greaterThan(0L)));
        });
        assertBusy(() -> assertThat(indexService.getShard(0).shouldPeriodicallyFlush(), equalTo(false)));
        settings = Settings.builder().put("index.translog.flush_threshold_size", (String) null).build();
        indicesAdmin().prepareUpdateSettings("test").setSettings(settings).get();

        prepareIndex("test").setId(UUIDs.randomBase64UUID()).setSource("{}", XContentType.JSON).get();
        indicesAdmin().prepareFlush("test").setForce(randomBoolean()).setWaitIfOngoing(true).get();
        final FlushStats flushStats = indicesAdmin().prepareStats("test").clear().setFlush(true).get().getTotal().flush;
        assertThat(flushStats.getTotal(), greaterThan(flushStats.getPeriodic()));
    }

    public void testShardHasMemoryBufferOnTranslogRecover() throws Throwable {
        createIndex("test");
        ensureGreen();
        IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        IndexService indexService = indicesService.indexService(resolveIndex("test"));
        IndexShard shard = indexService.getShardOrNull(0);
        prepareIndex("test").setId("0").setSource("{\"foo\" : \"bar\"}", XContentType.JSON).get();
        client().prepareDelete("test", "0").get();
        prepareIndex("test").setId("1").setSource("{\"foo\" : \"bar\"}", XContentType.JSON).setRefreshPolicy(IMMEDIATE).get();

        CheckedFunction<DirectoryReader, DirectoryReader, IOException> wrapper = directoryReader -> directoryReader;
        closeShardNoCheck(shard);
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
            closeShardNoCheck(newShard, randomBoolean());
        }
    }

    public static final IndexShard recoverShard(IndexShard newShard) throws IOException {
        DiscoveryNode localNode = DiscoveryNodeUtils.builder("foo").roles(emptySet()).build();
        newShard.markAsRecovering("store", new RecoveryState(newShard.routingEntry(), localNode, null));
        recoverFromStore(newShard);
        IndexShardTestCase.updateRoutingEntry(
            newShard,
            newShard.routingEntry().moveToStarted(ShardRouting.UNAVAILABLE_EXPECTED_SHARD_SIZE)
        );
        return newShard;
    }

    public static final IndexShard newIndexShard(
        final IndexService indexService,
        final IndexShard shard,
        CheckedFunction<DirectoryReader, DirectoryReader, IOException> wrapper,
        final CircuitBreakerService cbs,
        final IndexingOperationListener... listeners
    ) throws IOException {
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
            indexService.getThreadPoolMergeExecutorService(),
            indexService.getBigArrays(),
            null,
            Collections.emptyList(),
            Arrays.asList(listeners),
            IndexShardTestCase.NOOP_GCP_SYNCER,
            RetentionLeaseSyncer.EMPTY,
            cbs,
            IndexModule.DEFAULT_SNAPSHOT_COMMIT_SUPPLIER,
            System::nanoTime,
            null,
            MapperMetrics.NOOP,
            new IndexingStatsSettings(ClusterSettings.createBuiltInClusterSettings())
        );
    }

    private static ShardRouting getInitializingShardRouting(ShardRouting existingShardRouting) {
        ShardRouting shardRouting = shardRoutingBuilder(
            existingShardRouting.shardId(),
            existingShardRouting.currentNodeId(),
            existingShardRouting.primary(),
            ShardRoutingState.INITIALIZING
        ).withAllocationId(existingShardRouting.allocationId()).build();
        shardRouting = shardRouting.updateUnassigned(
            new UnassignedInfo(UnassignedInfo.Reason.INDEX_REOPENED, "fake recovery"),
            RecoverySource.ExistingStoreRecoverySource.INSTANCE
        );
        return shardRouting;
    }

    public void testInvalidateIndicesRequestCacheWhenRollbackEngine() throws Exception {
        createIndex("test", indexSettings(1, 0).put("index.refresh_interval", -1).build());
        ensureGreen();
        final IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        final IndexShard shard = indicesService.getShardOrNull(new ShardId(resolveIndex("test"), 0));
        final SearchRequest countRequest = new SearchRequest("test").source(new SearchSourceBuilder().size(0));
        final long numDocs = between(10, 20);
        for (int i = 0; i < numDocs; i++) {
            prepareIndex("test").setId(Integer.toString(i)).setSource("{}", XContentType.JSON).get();
            if (randomBoolean()) {
                shard.refresh("test");
            }
        }
        shard.refresh("test");
        assertHitCount(client().search(countRequest), numDocs);
        assertThat(shard.getLocalCheckpoint(), equalTo(shard.seqNoStats().getMaxSeqNo()));

        final CountDownLatch engineResetLatch = new CountDownLatch(1);
        shard.acquireAllPrimaryOperationsPermits(ActionListener.wrap(r -> {
            try {
                shard.rollbackEngineToGlobalCheckpoint();
            } finally {
                r.close();
                engineResetLatch.countDown();
            }
        }, Assert::assertNotNull), TimeValue.timeValueMinutes(1L));
        engineResetLatch.await();

        final long moreDocs = between(10, 20);
        for (int i = 0; i < moreDocs; i++) {
            prepareIndex("test").setId(Long.toString(i + numDocs)).setSource("{}", XContentType.JSON).get();
            if (randomBoolean()) {
                shard.refresh("test");
            }
        }
        shard.refresh("test");
        try (Engine.Searcher searcher = shard.acquireSearcher("test")) {
            assertThat(
                "numDocs=" + numDocs + " moreDocs=" + moreDocs,
                (long) searcher.getIndexReader().numDocs(),
                equalTo(numDocs + moreDocs)
            );
        }
        assertHitCount(client().search(countRequest), numDocs + moreDocs);
    }

    public void testShardChangesWithDefaultDocType() throws Exception {
        Settings settings = indexSettings(1, 0).put("index.translog.flush_threshold_size", "512mb") // do not flush
            .put("index.soft_deletes.enabled", true)
            .build();
        IndexService indexService = createIndex("index", settings, "user_doc", "title", "type=keyword");
        int numOps = between(1, 10);
        for (int i = 0; i < numOps; i++) {
            if (randomBoolean()) {
                prepareIndex("index").setId(randomFrom("1", "2")).setSource("{}", XContentType.JSON).get();
            } else {
                client().prepareDelete("index", randomFrom("1", "2")).get();
            }
        }
        IndexShard shard = indexService.getShard(0);
        try (
            Translog.Snapshot luceneSnapshot = shard.newChangesSnapshot(
                "test",
                0,
                numOps - 1,
                true,
                randomBoolean(),
                randomBoolean(),
                randomLongBetween(1, ByteSizeValue.ofMb(32).getBytes())
            );
            Translog.Snapshot translogSnapshot = getTranslog(shard).newSnapshot()
        ) {
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
        createIndex(indexName, indexSettings(1, 0).build());
        ensureGreen();

        assertAcked(indicesAdmin().prepareClose(indexName));

        final ClusterService clusterService = getInstanceFromNode(ClusterService.class);
        final ClusterState clusterState = clusterService.state();

        final IndexMetadata indexMetadata = clusterState.metadata().getProject().index(indexName);
        final IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        final IndexService indexService = indicesService.indexServiceSafe(indexMetadata.getIndex());

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
