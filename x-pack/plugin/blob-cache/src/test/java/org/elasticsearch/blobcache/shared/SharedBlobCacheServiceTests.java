/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.blobcache.shared;

import org.apache.lucene.store.AlreadyClosedException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.GroupedActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.blobcache.BlobCacheMetrics;
import org.elasticsearch.blobcache.BlobCacheUtils;
import org.elasticsearch.blobcache.common.ByteRange;
import org.elasticsearch.blobcache.common.SparseFileTracker;
import org.elasticsearch.blobcache.shared.SharedBlobCacheService.CacheFileRegion;
import org.elasticsearch.blobcache.shared.SharedBlobCacheService.RangeMissingHandler;
import org.elasticsearch.blobcache.shared.SharedBlobCacheService.SourceInputStreamFactory;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsException;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.RatioValue;
import org.elasticsearch.common.unit.RelativeByteSizeValue;
import org.elasticsearch.common.util.concurrent.DeterministicTaskQueue;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.StoppableExecutorServiceWrapper;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.core.CheckedRunnable;
import org.elasticsearch.core.Predicates;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.node.NodeRoleSettings;
import org.elasticsearch.telemetry.InstrumentType;
import org.elasticsearch.telemetry.Measurement;
import org.elasticsearch.telemetry.RecordingMeterRegistry;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.io.InputStream;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.IntConsumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.elasticsearch.blobcache.BlobCacheMetrics.BLOB_CACHE_COUNT_OF_EVICTED_REGIONS_TOTAL;
import static org.elasticsearch.blobcache.BlobCacheMetrics.BLOB_CACHE_EVICTION_SCANNED_ENTRIES;
import static org.elasticsearch.blobcache.BlobCacheMetrics.BLOB_CACHE_EVICTION_SCAN_TIME;
import static org.elasticsearch.blobcache.BlobCacheMetrics.BLOB_CACHE_LOCK_ACQUIRE_TIME;
import static org.elasticsearch.blobcache.BlobCacheMetrics.EvictionScanMode.AllFrequencies;
import static org.elasticsearch.blobcache.BlobCacheMetrics.EvictionScanMode.LowestFrequency;
import static org.elasticsearch.blobcache.BlobCacheMetrics.EvictionScanOutcome.Evicted;
import static org.elasticsearch.blobcache.BlobCacheMetrics.EvictionScanOutcome.Free;
import static org.elasticsearch.blobcache.BlobCacheMetrics.EvictionScanOutcome.None;
import static org.elasticsearch.blobcache.BlobCacheMetrics.LOCK_ACQUIRE_SITE_ATTRIBUTE_KEY;
import static org.elasticsearch.blobcache.BlobCacheMetrics.LockAcquireSite.CacheMissEviction;
import static org.elasticsearch.blobcache.BlobCacheMetrics.LockAcquireSite.Decay;
import static org.elasticsearch.blobcache.BlobCacheMetrics.LockAcquireSite.Demote;
import static org.elasticsearch.blobcache.BlobCacheMetrics.LockAcquireSite.ForceEvict;
import static org.elasticsearch.blobcache.BlobCacheMetrics.LockAcquireSite.LowestFrequencyEviction;
import static org.elasticsearch.blobcache.BlobCacheMetrics.LockAcquireSite.Promote;
import static org.elasticsearch.blobcache.BlobCacheMetrics.LockAcquireSite.SlotAssignment;
import static org.elasticsearch.node.Node.NODE_NAME_SETTING;
import static org.elasticsearch.telemetry.InstrumentType.DOUBLE_HISTOGRAM;
import static org.elasticsearch.telemetry.InstrumentType.LONG_HISTOGRAM;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;

public class SharedBlobCacheServiceTests extends ESTestCase {

    private static long size(long numPages) {
        return numPages * SharedBytes.PAGE_SIZE;
    }

    private static <E extends Exception> void completeWith(ActionListener<Void> listener, CheckedRunnable<E> runnable) {
        ActionListener.completeWith(listener, () -> {
            runnable.run();
            return null;
        });
    }

    public void testBasicEviction() throws IOException {
        Settings settings = Settings.builder()
            .put(NODE_NAME_SETTING.getKey(), "node")
            .put(SharedBlobCacheService.SHARED_CACHE_SIZE_SETTING.getKey(), ByteSizeValue.ofBytes(size(500)).getStringRep())
            .put(SharedBlobCacheService.SHARED_CACHE_REGION_SIZE_SETTING.getKey(), ByteSizeValue.ofBytes(size(100)).getStringRep())
            .put(SharedBlobCacheService.SHARED_CACHE_INITIAL_DECAYS_SETTING.getKey(), 0)
            .put("path.home", createTempDir())
            .build();
        final DeterministicTaskQueue taskQueue = new DeterministicTaskQueue();
        RecordingMeterRegistry recordingMeterRegistry = new RecordingMeterRegistry();
        BlobCacheMetrics metrics = new BlobCacheMetrics(recordingMeterRegistry);
        try (
            NodeEnvironment environment = new NodeEnvironment(settings, TestEnvironment.newEnvironment(settings));
            var cacheService = new SharedBlobCacheService<TestCacheKey>(
                environment,
                settings,
                taskQueue.getThreadPool(),
                taskQueue.getThreadPool().executor(ThreadPool.Names.GENERIC),
                metrics
            )
        ) {
            final var cacheKey = generateCacheKey();
            assertEquals(5, cacheService.freeRegionCount());
            final var region0 = cacheService.get(cacheKey, size(250), 0);
            assertEquals(size(100), region0.tracker.getLength());
            assertEquals(4, cacheService.freeRegionCount());
            final var region1 = cacheService.get(cacheKey, size(250), 1);
            assertEquals(size(100), region1.tracker.getLength());
            assertEquals(3, cacheService.freeRegionCount());
            final var region2 = cacheService.get(cacheKey, size(250), 2);
            assertEquals(size(50), region2.tracker.getLength());
            assertEquals(2, cacheService.freeRegionCount());

            synchronized (cacheService) {
                assertTrue(tryEvict(region1));
            }
            assertEquals(3, cacheService.freeRegionCount());
            // one eviction should be reflected in the telemetry for total count of evicted regions
            assertThat(
                recordingMeterRegistry.getRecorder()
                    .getMeasurements(InstrumentType.LONG_COUNTER, BLOB_CACHE_COUNT_OF_EVICTED_REGIONS_TOTAL)
                    .size(),
                is(1)
            );
            synchronized (cacheService) {
                assertFalse(tryEvict(region1));
            }

            assertEquals(3, cacheService.freeRegionCount());
            final var bytesReadFuture = new PlainActionFuture<Integer>();
            region0.populateAndRead(
                ByteRange.of(0L, 1L),
                ByteRange.of(0L, 1L),
                (channel, channelPos, relativePos, length) -> 1,
                (channel, channelPos, streamFactory, relativePos, length, progressUpdater, completionListener) -> completeWith(
                    completionListener,
                    () -> progressUpdater.accept(length)
                ),
                taskQueue.getThreadPool().generic(),
                bytesReadFuture
            );
            synchronized (cacheService) {
                assertFalse(tryEvict(region0));
            }
            assertEquals(3, cacheService.freeRegionCount());
            assertFalse(bytesReadFuture.isDone());
            taskQueue.runAllRunnableTasks();
            synchronized (cacheService) {
                assertTrue(tryEvict(region0));
            }
            assertEquals(4, cacheService.freeRegionCount());
            synchronized (cacheService) {
                assertTrue(tryEvict(region2));
            }
            assertEquals(5, cacheService.freeRegionCount());
            // another 2 evictions should bump our total evictions telemetry at 3
            assertThat(
                recordingMeterRegistry.getRecorder()
                    .getMeasurements(InstrumentType.LONG_COUNTER, BLOB_CACHE_COUNT_OF_EVICTED_REGIONS_TOTAL)
                    .size(),
                is(3)
            );

            assertTrue(bytesReadFuture.isDone());
            assertEquals(Integer.valueOf(1), bytesReadFuture.actionGet());
        }
    }

    public void testTimestampIsStampedOnCacheEntryAndSetOnce() throws IOException {
        Settings settings = Settings.builder()
            .put(NODE_NAME_SETTING.getKey(), "node")
            .put(SharedBlobCacheService.SHARED_CACHE_SIZE_SETTING.getKey(), ByteSizeValue.ofBytes(size(500)))
            .put(SharedBlobCacheService.SHARED_CACHE_REGION_SIZE_SETTING.getKey(), ByteSizeValue.ofBytes(size(100)))
            .put(SharedBlobCacheService.SHARED_CACHE_INITIAL_DECAYS_SETTING.getKey(), 0)
            .put("path.home", createTempDir())
            .build();
        final DeterministicTaskQueue taskQueue = new DeterministicTaskQueue();
        try (
            NodeEnvironment environment = new NodeEnvironment(settings, TestEnvironment.newEnvironment(settings));
            var cacheService = new SharedBlobCacheService<TestCacheKey>(
                environment,
                settings,
                taskQueue.getThreadPool(),
                taskQueue.getThreadPool().executor(ThreadPool.Names.GENERIC),
                new BlobCacheMetrics(new RecordingMeterRegistry())
            )
        ) {
            final var cacheKey = generateCacheKey();
            final long timestamp = randomLongBetween(1, Long.MAX_VALUE - 1);

            // a region created with an explicit timestamp is stamped with it
            final var region0 = cacheService.get(cacheKey, size(250), 0, timestamp);
            assertEquals(timestamp, region0.timestampMillis());

            // the timestamp is set-once: fetching the same region again with a different timestamp keeps the original
            final var region0Again = cacheService.get(cacheKey, size(250), 0, timestamp + 1);
            assertSame(region0, region0Again);
            assertEquals(timestamp, region0Again.timestampMillis());

            // a region created without a timestamp defaults to UNKNOWN_TIMESTAMP
            final var region1 = cacheService.get(cacheKey, size(250), 1);
            assertEquals(SharedBlobCacheService.UNKNOWN_TIMESTAMP, region1.timestampMillis());
        }
    }

    public void testFetchOverloadsStampTimestamp() throws Exception {
        final long cacheSize = size(500L);
        final long regionSize = size(100L);
        Settings settings = Settings.builder()
            .put(NODE_NAME_SETTING.getKey(), "node")
            .put(SharedBlobCacheService.SHARED_CACHE_SIZE_SETTING.getKey(), ByteSizeValue.ofBytes(cacheSize))
            .put(SharedBlobCacheService.SHARED_CACHE_REGION_SIZE_SETTING.getKey(), ByteSizeValue.ofBytes(regionSize))
            .put(SharedBlobCacheService.SHARED_CACHE_INITIAL_DECAYS_SETTING.getKey(), 0)
            .put("path.home", createTempDir())
            .build();

        final var threadPool = new TestThreadPool("test");
        final var bulkExecutor = new StoppableExecutorServiceWrapper(threadPool.generic());

        // a writer that simply reports the requested bytes as populated
        final RangeMissingHandler writer = (
            channel,
            channelPos,
            streamFactory,
            relativePos,
            length,
            progressUpdater,
            completionListener) -> completeWith(completionListener, () -> progressUpdater.accept(length));

        try (
            NodeEnvironment environment = new NodeEnvironment(settings, TestEnvironment.newEnvironment(settings));
            var cacheService = new SharedBlobCacheService<>(
                environment,
                settings,
                threadPool,
                threadPool.executor(ThreadPool.Names.GENERIC),
                BlobCacheMetrics.NOOP
            )
        ) {
            {
                final var cacheKey = generateCacheKey();
                final long ts = randomLongBetween(1, Long.MAX_VALUE - 1);
                final PlainActionFuture<Boolean> future = new PlainActionFuture<>();
                cacheService.fetchRegion(cacheKey, 0, regionSize, writer, bulkExecutor, true, ts, future);
                assertThat(future.get(10, TimeUnit.SECONDS), is(true));
                assertEquals(ts, cacheService.get(cacheKey, regionSize, 0).timestampMillis());
            }
            {
                final var cacheKey = generateCacheKey();
                final long ts = randomLongBetween(1, Long.MAX_VALUE - 1);
                final var range = ByteRange.of(0, regionSize);
                final PlainActionFuture<Boolean> future = new PlainActionFuture<>();
                cacheService.fetchRange(cacheKey, 0, range, regionSize, writer, bulkExecutor, true, ts, future);
                assertThat(future.get(10, TimeUnit.SECONDS), is(true));
                assertEquals(ts, cacheService.get(cacheKey, regionSize, 0).timestampMillis());
            }
            {
                final var cacheKey = generateCacheKey();
                final long ts = randomLongBetween(1, Long.MAX_VALUE - 1);
                final PlainActionFuture<Boolean> future = new PlainActionFuture<>();
                cacheService.maybeFetchRegion(cacheKey, 0, regionSize, writer, bulkExecutor, ts, future);
                assertThat(future.get(10, TimeUnit.SECONDS), is(true));
                assertEquals(ts, cacheService.get(cacheKey, regionSize, 0).timestampMillis());
            }
            {
                final var cacheKey = generateCacheKey();
                final long ts = randomLongBetween(1, Long.MAX_VALUE - 1);
                final var range = ByteRange.of(0, regionSize);
                final PlainActionFuture<Boolean> future = new PlainActionFuture<>();
                cacheService.maybeFetchRange(cacheKey, 0, range, regionSize, writer, bulkExecutor, ts, future);
                assertThat(future.get(10, TimeUnit.SECONDS), is(true));
                assertEquals(ts, cacheService.get(cacheKey, regionSize, 0).timestampMillis());
            }
        } finally {
            TestThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
        }
    }

    public void testTimestampSetOnceAcrossFetchOverloads() throws Exception {
        final long cacheSize = size(500L);
        final long regionSize = size(100L);
        Settings settings = Settings.builder()
            .put(NODE_NAME_SETTING.getKey(), "node")
            .put(SharedBlobCacheService.SHARED_CACHE_SIZE_SETTING.getKey(), ByteSizeValue.ofBytes(cacheSize))
            .put(SharedBlobCacheService.SHARED_CACHE_REGION_SIZE_SETTING.getKey(), ByteSizeValue.ofBytes(regionSize))
            .put(SharedBlobCacheService.SHARED_CACHE_INITIAL_DECAYS_SETTING.getKey(), 0)
            .put("path.home", createTempDir())
            .build();

        final var threadPool = new TestThreadPool("test");
        final var bulkExecutor = new StoppableExecutorServiceWrapper(threadPool.generic());

        final RangeMissingHandler writer = (
            channel,
            channelPos,
            streamFactory,
            relativePos,
            length,
            progressUpdater,
            completionListener) -> completeWith(completionListener, () -> progressUpdater.accept(length));

        try (
            NodeEnvironment environment = new NodeEnvironment(settings, TestEnvironment.newEnvironment(settings));
            var cacheService = new SharedBlobCacheService<>(
                environment,
                settings,
                threadPool,
                threadPool.executor(ThreadPool.Names.GENERIC),
                BlobCacheMetrics.NOOP
            )
        ) {
            final var cacheKey = generateCacheKey();
            final long firstTimestamp = randomLongBetween(1, Long.MAX_VALUE - 2);
            final long secondTimestamp = firstTimestamp + 1;

            // first population path to create region 0 wins the stamp
            final PlainActionFuture<Boolean> firstFuture = new PlainActionFuture<>();
            cacheService.fetchRegion(cacheKey, 0, regionSize, writer, bulkExecutor, true, firstTimestamp, firstFuture);
            assertThat(firstFuture.get(10, TimeUnit.SECONDS), is(true));

            // a later population path through a different overload carries a different timestamp, but the stamp is set-once
            final PlainActionFuture<Boolean> secondFuture = new PlainActionFuture<>();
            cacheService.maybeFetchRange(
                cacheKey,
                0,
                ByteRange.of(0, regionSize),
                regionSize,
                writer,
                bulkExecutor,
                secondTimestamp,
                secondFuture
            );
            secondFuture.get(10, TimeUnit.SECONDS);

            assertEquals(firstTimestamp, cacheService.get(cacheKey, regionSize, 0).timestampMillis());
        } finally {
            TestThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
        }
    }

    public void testGetCacheFileStampsTimestampOnRead() throws Exception {
        Settings settings = Settings.builder()
            .put(NODE_NAME_SETTING.getKey(), "node")
            .put(SharedBlobCacheService.SHARED_CACHE_SIZE_SETTING.getKey(), ByteSizeValue.ofBytes(size(500)))
            .put(SharedBlobCacheService.SHARED_CACHE_REGION_SIZE_SETTING.getKey(), ByteSizeValue.ofBytes(size(100)))
            .put(SharedBlobCacheService.SHARED_CACHE_INITIAL_DECAYS_SETTING.getKey(), 0)
            .put("path.home", createTempDir())
            .build();
        final DeterministicTaskQueue taskQueue = new DeterministicTaskQueue();
        final ExecutorService ioExecutor = Executors.newCachedThreadPool();
        try (
            NodeEnvironment environment = new NodeEnvironment(settings, TestEnvironment.newEnvironment(settings));
            var cacheService = new SharedBlobCacheService<TestCacheKey>(
                environment,
                settings,
                taskQueue.getThreadPool(),
                ioExecutor,
                new BlobCacheMetrics(new RecordingMeterRegistry())
            )
        ) {
            final var cacheKey = generateCacheKey();
            final long ts = randomLongBetween(1, Long.MAX_VALUE - 1);
            final Path tempFile = createTempFile("test", "other");
            final ByteBuffer writeBuffer = ByteBuffer.allocate(SharedBytes.PAGE_SIZE);

            // the timestamp passed to getCacheFile is stamped on the region populated by the CacheFile read
            final SharedBlobCacheService<TestCacheKey>.CacheFile cacheFile = cacheService.getCacheFile(
                cacheKey,
                1L,
                SharedBlobCacheService.CacheMissHandler.NOOP,
                ts
            );

            final int bytesRead = cacheFile.populateAndRead(
                ByteRange.of(0L, 1L),
                ByteRange.of(0L, 1L),
                (channel, pos, relativePos, len) -> len,
                (channel, channelPos, streamFactory, relativePos, len, progressUpdater, completionListener) -> {
                    try (var in = Files.newInputStream(tempFile)) {
                        SharedBytes.copyToCacheFileAligned(channel, in, channelPos, progressUpdater, writeBuffer.clear());
                    }
                    ActionListener.completeWith(completionListener, () -> null);
                },
                tempFile.toAbsolutePath().toString()
            );
            assertThat(bytesRead, is(1));

            assertEquals(ts, cacheService.get(cacheKey, 1L, 0).timestampMillis());
        } finally {
            ThreadPool.terminate(ioExecutor, 10, TimeUnit.SECONDS);
        }
    }

    public void testCacheMissOnPopulate() throws Exception {
        Settings settings = Settings.builder()
            .put(NODE_NAME_SETTING.getKey(), "node")
            .put(SharedBlobCacheService.SHARED_CACHE_SIZE_SETTING.getKey(), ByteSizeValue.ofBytes(size(50)).getStringRep())
            .put(SharedBlobCacheService.SHARED_CACHE_REGION_SIZE_SETTING.getKey(), ByteSizeValue.ofBytes(size(10)).getStringRep())
            .put(SharedBlobCacheService.SHARED_CACHE_INITIAL_DECAYS_SETTING.getKey(), 0)
            .put("path.home", createTempDir())
            .build();
        final DeterministicTaskQueue taskQueue = new DeterministicTaskQueue();
        RecordingMeterRegistry recordingMeterRegistry = new RecordingMeterRegistry();
        BlobCacheMetrics metrics = new BlobCacheMetrics(recordingMeterRegistry);
        ExecutorService ioExecutor = Executors.newCachedThreadPool();
        try (
            NodeEnvironment environment = new NodeEnvironment(settings, TestEnvironment.newEnvironment(settings));
            var cacheService = new SharedBlobCacheService<TestCacheKey>(
                environment,
                settings,
                taskQueue.getThreadPool(),
                ioExecutor,
                metrics
            )
        ) {
            ByteRange rangeRead = ByteRange.of(0L, 1L);
            ByteRange rangeWrite = ByteRange.of(0L, 1L);
            Path tempFile = createTempFile("test", "other");
            String resourceDescription = tempFile.toAbsolutePath().toString();
            final var cacheKey = generateCacheKey();
            SharedBlobCacheService<TestCacheKey>.CacheFile cacheFile = cacheService.getCacheFile(
                cacheKey,
                1L,
                SharedBlobCacheService.CacheMissHandler.NOOP
            );

            ByteBuffer writeBuffer = ByteBuffer.allocate(SharedBytes.PAGE_SIZE);

            final int bytesRead = cacheFile.populateAndRead(
                rangeRead,
                rangeWrite,
                (channel, pos, relativePos, len) -> len,
                (channel, channelPos, streamFactory, relativePos, len, progressUpdater, completionListener) -> {
                    try (var in = Files.newInputStream(tempFile)) {
                        SharedBytes.copyToCacheFileAligned(channel, in, channelPos, progressUpdater, writeBuffer.clear());
                    }
                    ActionListener.completeWith(completionListener, () -> null);
                },
                resourceDescription
            );
            assertThat(bytesRead, is(1));
            List<Measurement> measurements = recordingMeterRegistry.getRecorder()
                .getMeasurements(InstrumentType.LONG_COUNTER, "es.blob_cache.miss_that_triggered_read.total");
            Measurement first = measurements.getFirst();
            assertThat(first.attributes().get("file_extension"), is("other"));
            assertThat(first.value(), is(1L));

            Path tempFile2 = createTempFile("test", "cfs");
            resourceDescription = tempFile2.toAbsolutePath().toString();
            cacheFile = cacheService.getCacheFile(generateCacheKey(), 1L, SharedBlobCacheService.CacheMissHandler.NOOP);

            ByteBuffer writeBuffer2 = ByteBuffer.allocate(SharedBytes.PAGE_SIZE);

            final int bytesRead2 = cacheFile.populateAndRead(
                rangeRead,
                rangeWrite,
                (channel, pos, relativePos, len) -> len,
                (channel, channelPos, streamFactory, relativePos, len, progressUpdater, completionListener) -> {
                    try (var in = Files.newInputStream(tempFile2)) {
                        SharedBytes.copyToCacheFileAligned(channel, in, channelPos, progressUpdater, writeBuffer2.clear());
                    }
                    ActionListener.completeWith(completionListener, () -> null);
                },
                resourceDescription
            );
            assertThat(bytesRead2, is(1));

            measurements = recordingMeterRegistry.getRecorder()
                .getMeasurements(InstrumentType.LONG_COUNTER, "es.blob_cache.miss_that_triggered_read.total");
            Measurement measurement = measurements.get(1);
            assertThat(measurement.attributes().get("file_extension"), is("cfs"));
            assertThat(measurement.value(), is(1L));
        }
        ioExecutor.shutdown();
    }

    private static boolean tryEvict(CacheFileRegion<TestCacheKey> region1) {
        if (randomBoolean()) {
            return region1.tryEvict();
        } else {
            boolean result = region1.tryEvictNoDecRef();
            if (result) {
                region1.decRef();
            }
            return result;
        }
    }

    public void testAutoEviction() throws IOException {
        Settings settings = Settings.builder()
            .put(NODE_NAME_SETTING.getKey(), "node")
            .put(SharedBlobCacheService.SHARED_CACHE_SIZE_SETTING.getKey(), ByteSizeValue.ofBytes(size(200)).getStringRep())
            .put(SharedBlobCacheService.SHARED_CACHE_REGION_SIZE_SETTING.getKey(), ByteSizeValue.ofBytes(size(100)).getStringRep())
            .put(SharedBlobCacheService.SHARED_CACHE_INITIAL_DECAYS_SETTING.getKey(), 0)
            .put("path.home", createTempDir())
            .build();
        final DeterministicTaskQueue taskQueue = new DeterministicTaskQueue();
        try (
            NodeEnvironment environment = new NodeEnvironment(settings, TestEnvironment.newEnvironment(settings));
            var cacheService = new SharedBlobCacheService<TestCacheKey>(
                environment,
                settings,
                taskQueue.getThreadPool(),
                taskQueue.getThreadPool().executor(ThreadPool.Names.GENERIC),
                BlobCacheMetrics.NOOP
            )
        ) {
            final var cacheKey = generateCacheKey();
            assertEquals(2, cacheService.freeRegionCount());
            final var region0 = cacheService.get(cacheKey, size(250), 0);
            assertEquals(size(100), region0.tracker.getLength());
            assertEquals(1, cacheService.freeRegionCount());
            final var region1 = cacheService.get(cacheKey, size(250), 1);
            assertEquals(size(100), region1.tracker.getLength());
            assertEquals(0, cacheService.freeRegionCount());
            assertFalse(region0.isEvicted());
            assertFalse(region1.isEvicted());

            // acquire region 2, which should evict region 0 (oldest)
            final var region2 = cacheService.get(cacheKey, size(250), 2);
            assertEquals(size(50), region2.tracker.getLength());
            assertEquals(0, cacheService.freeRegionCount());
            assertTrue(region0.isEvicted());
            assertFalse(region1.isEvicted());

            // explicitly evict region 1
            synchronized (cacheService) {
                assertTrue(tryEvict(region1));
            }
            assertEquals(1, cacheService.freeRegionCount());
        }
    }

    public void testForceEviction() throws IOException {
        Settings settings = Settings.builder()
            .put(NODE_NAME_SETTING.getKey(), "node")
            .put(SharedBlobCacheService.SHARED_CACHE_SIZE_SETTING.getKey(), ByteSizeValue.ofBytes(size(500)).getStringRep())
            .put(SharedBlobCacheService.SHARED_CACHE_REGION_SIZE_SETTING.getKey(), ByteSizeValue.ofBytes(size(100)).getStringRep())
            .put(SharedBlobCacheService.SHARED_CACHE_INITIAL_DECAYS_SETTING.getKey(), 0)
            .put("path.home", createTempDir())
            .build();
        final DeterministicTaskQueue taskQueue = new DeterministicTaskQueue();
        try (
            NodeEnvironment environment = new NodeEnvironment(settings, TestEnvironment.newEnvironment(settings));
            var cacheService = new SharedBlobCacheService<>(
                environment,
                settings,
                taskQueue.getThreadPool(),
                taskQueue.getThreadPool().executor(ThreadPool.Names.GENERIC),
                BlobCacheMetrics.NOOP
            )
        ) {
            final var cacheKey1 = generateCacheKey();
            final var cacheKey2 = generateCacheKey();
            assertEquals(5, cacheService.freeRegionCount());
            final var region0 = cacheService.get(cacheKey1, size(250), 0);
            assertEquals(4, cacheService.freeRegionCount());
            final var region1 = cacheService.get(cacheKey2, size(250), 1);
            assertEquals(3, cacheService.freeRegionCount());
            assertFalse(region0.isEvicted());
            assertFalse(region1.isEvicted());
            if (randomBoolean()) {
                cacheService.removeFromCache(cacheKey1);
            } else {
                cacheService.forceEvict(cacheKey1::equals);
            }
            assertTrue(region0.isEvicted());
            assertFalse(region1.isEvicted());
            assertEquals(4, cacheService.freeRegionCount());
        }
    }

    public void testForceEvictResponse() throws IOException {
        Settings settings = Settings.builder()
            .put(NODE_NAME_SETTING.getKey(), "node")
            .put(SharedBlobCacheService.SHARED_CACHE_SIZE_SETTING.getKey(), ByteSizeValue.ofBytes(size(500)).getStringRep())
            .put(SharedBlobCacheService.SHARED_CACHE_REGION_SIZE_SETTING.getKey(), ByteSizeValue.ofBytes(size(100)).getStringRep())
            .put(SharedBlobCacheService.SHARED_CACHE_INITIAL_DECAYS_SETTING.getKey(), 0)
            .put("path.home", createTempDir())
            .build();
        final DeterministicTaskQueue taskQueue = new DeterministicTaskQueue();
        try (
            NodeEnvironment environment = new NodeEnvironment(settings, TestEnvironment.newEnvironment(settings));
            var cacheService = new SharedBlobCacheService<>(
                environment,
                settings,
                taskQueue.getThreadPool(),
                taskQueue.getThreadPool().executor(ThreadPool.Names.GENERIC),
                BlobCacheMetrics.NOOP
            )
        ) {
            final var cacheKey1 = generateCacheKey();
            final var cacheKey2 = generateCacheKey();
            assertEquals(5, cacheService.freeRegionCount());
            final var region0 = cacheService.get(cacheKey1, size(250), 0);
            assertEquals(4, cacheService.freeRegionCount());
            final var region1 = cacheService.get(cacheKey2, size(250), 1);
            assertEquals(3, cacheService.freeRegionCount());
            assertFalse(region0.isEvicted());
            assertFalse(region1.isEvicted());

            if (randomBoolean()) {
                assertEquals(1, cacheService.forceEvict(cacheKey1.shardId(), cK -> cK == cacheKey1));
            } else {
                assertEquals(1, cacheService.forceEvict(cK -> cK == cacheKey1));
            }
            assertEquals(1, cacheService.forceEvict(e -> true));
        }
    }

    public void testAsynchronousEviction() throws Exception {
        Settings settings = Settings.builder()
            .put(NODE_NAME_SETTING.getKey(), "node")
            .put(SharedBlobCacheService.SHARED_CACHE_SIZE_SETTING.getKey(), ByteSizeValue.ofBytes(size(500)).getStringRep())
            .put(SharedBlobCacheService.SHARED_CACHE_REGION_SIZE_SETTING.getKey(), ByteSizeValue.ofBytes(size(100)).getStringRep())
            .put(SharedBlobCacheService.SHARED_CACHE_INITIAL_DECAYS_SETTING.getKey(), 0)
            .put("path.home", createTempDir())
            .build();
        final DeterministicTaskQueue taskQueue = new DeterministicTaskQueue();
        try (
            NodeEnvironment environment = new NodeEnvironment(settings, TestEnvironment.newEnvironment(settings));
            var cacheService = new SharedBlobCacheService<>(
                environment,
                settings,
                taskQueue.getThreadPool(),
                taskQueue.getThreadPool().executor(ThreadPool.Names.GENERIC),
                BlobCacheMetrics.NOOP
            )
        ) {
            final var cacheKey1 = generateCacheKey();
            final var cacheKey2 = generateCacheKey();
            assertEquals(5, cacheService.freeRegionCount());
            final var region0 = cacheService.get(cacheKey1, size(250), 0);
            assertEquals(4, cacheService.freeRegionCount());
            final var region1 = cacheService.get(cacheKey2, size(250), 1);
            assertEquals(3, cacheService.freeRegionCount());
            assertFalse(region0.isEvicted());
            assertFalse(region1.isEvicted());
            cacheService.forceEvictAsync(ck -> ck == cacheKey1);
            assertFalse(region0.isEvicted());
            assertFalse(region1.isEvicted());
            // run the async task
            taskQueue.runAllRunnableTasks();
            assertTrue(region0.isEvicted());
            assertFalse(region1.isEvicted());
            assertEquals(4, cacheService.freeRegionCount());
        }
    }

    public void testDemoteAll() throws Exception {
        final boolean async = randomBoolean();
        Settings settings = Settings.builder()
            .put(NODE_NAME_SETTING.getKey(), "node")
            .put(SharedBlobCacheService.SHARED_CACHE_SIZE_SETTING.getKey(), ByteSizeValue.ofBytes(size(500)).getStringRep())
            .put(SharedBlobCacheService.SHARED_CACHE_REGION_SIZE_SETTING.getKey(), ByteSizeValue.ofBytes(size(100)).getStringRep())
            .put(SharedBlobCacheService.SHARED_CACHE_INITIAL_DECAYS_SETTING.getKey(), 0)
            .put("path.home", createTempDir())
            .build();
        final DeterministicTaskQueue taskQueue = new DeterministicTaskQueue();
        try (
            NodeEnvironment environment = new NodeEnvironment(settings, TestEnvironment.newEnvironment(settings));
            var cacheService = new SharedBlobCacheService<>(
                environment,
                settings,
                taskQueue.getThreadPool(),
                taskQueue.getThreadPool().executor(ThreadPool.Names.GENERIC),
                BlobCacheMetrics.NOOP
            )
        ) {
            final ShardId shard1 = randomShardId();
            final ShardId shard2 = randomShardId();
            final var cacheKey1 = randomTestCacheKey(shard1);
            final var cacheKey2 = randomTestCacheKey(shard2);

            final var region0 = cacheService.get(cacheKey1, size(250), 0);
            final var region1 = cacheService.get(cacheKey1, size(250), 1);
            final var region2 = cacheService.get(cacheKey2, size(250), 0);

            assertEquals(1, cacheService.getFreq(region0));
            assertEquals(1, cacheService.getFreq(region1));
            assertThat(cacheService.countCachedRegionsByFreq(key -> key.shardId().equals(shard1)), equalTo(Map.of(1, 2)));

            if (async) {
                cacheService.demoteAllAsync(shard1, id -> id.equals(shard1));
                assertThat(cacheService.countCachedRegionsByFreq(key -> key.shardId().equals(shard1)), equalTo(Map.of(1, 2)));
                taskQueue.runAllRunnableTasks();
            } else {
                assertEquals(2, cacheService.demoteAll(shard1));
            }

            assertThat(cacheService.countCachedRegionsByFreq(key -> key.shardId().equals(shard1)), equalTo(Map.of(0, 2)));
            assertEquals(0, cacheService.getFreq(region0));
            assertEquals(0, cacheService.getFreq(region1));
            assertEquals(1, cacheService.getFreq(region2));

            assertEquals(0, cacheService.demoteAll(shard1));
            assertEquals(0, cacheService.demoteAll(randomShardId()));
        }
    }

    /// Verifies that {@link SharedBlobCacheService#demoteAll} moves demoted regions to the freq-0 head
    /// so they are evicted before other freq-0 entries.
    public void testDemoteAllMovesRegionsToFrontForEviction() throws IOException {
        Settings settings = Settings.builder()
            .put(NODE_NAME_SETTING.getKey(), "node")
            .put(SharedBlobCacheService.SHARED_CACHE_SIZE_SETTING.getKey(), ByteSizeValue.ofBytes(size(300)).getStringRep())
            .put(SharedBlobCacheService.SHARED_CACHE_REGION_SIZE_SETTING.getKey(), ByteSizeValue.ofBytes(size(100)).getStringRep())
            .put(SharedBlobCacheService.SHARED_CACHE_INITIAL_DECAYS_SETTING.getKey(), 0)
            .put("path.home", createTempDir())
            .build();
        final DeterministicTaskQueue taskQueue = new DeterministicTaskQueue();
        try (
            NodeEnvironment environment = new NodeEnvironment(settings, TestEnvironment.newEnvironment(settings));
            var cacheService = new SharedBlobCacheService<>(
                environment,
                settings,
                taskQueue.getThreadPool(),
                taskQueue.getThreadPool().executor(ThreadPool.Names.GENERIC),
                BlobCacheMetrics.NOOP
            )
        ) {
            final ShardId protectedShard = randomShardId();
            final ShardId victimShard = randomShardId();
            final var protectedKey = randomTestCacheKey(protectedShard);
            final var victimKey = randomTestCacheKey(victimShard);

            final var protectedRegion0 = cacheService.get(protectedKey, size(250), 0);
            final var protectedRegion1 = cacheService.get(protectedKey, size(250), 1);
            assertThat(cacheService.freeRegionCount(), equalTo(1));

            cacheService.computeDecay();
            taskQueue.runAllRunnableTasks();
            assertThat(cacheService.countCachedRegionsByFreq(key -> key.shardId().equals(protectedShard)), equalTo(Map.of(0, 2)));
            assertEquals(0, cacheService.getFreq(protectedRegion0));
            assertEquals(0, cacheService.getFreq(protectedRegion1));

            final var victimRegion0 = cacheService.get(victimKey, size(250), 0);
            assertThat(cacheService.freeRegionCount(), equalTo(0));
            assertEquals(1, cacheService.getFreq(victimRegion0));

            assertEquals(1, cacheService.demoteAll(victimShard));
            assertEquals(0, cacheService.getFreq(victimRegion0));
            assertThat(cacheService.countCachedRegionsByFreq(key -> true), equalTo(Map.of(0, 3)));

            assertThat(cacheService.maybeEvictLeastUsed(randomTestCacheKey(randomShardId()), size(250), 0), is(true));
            assertTrue(victimRegion0.isEvicted());
            assertFalse(protectedRegion0.isEvicted());
            assertFalse(protectedRegion1.isEvicted());
        }
    }

    public void testCountCachedRegionsByShardId() throws IOException {
        final int numShards = randomIntBetween(1, 10);
        final Map<ShardId, Integer> regionCountPerShard = new HashMap<>();
        final Map<ShardId, TestCacheKey> cacheKeyPerShard = new HashMap<>();
        int totalRegions = 0;
        for (int s = 0; s < numShards; s++) {
            final ShardId shardId = randomShardId();
            final int numRegions = randomIntBetween(1, 10);
            cacheKeyPerShard.put(shardId, randomTestCacheKey(shardId));
            regionCountPerShard.put(shardId, numRegions);
            totalRegions += numRegions;
        }

        Settings settings = Settings.builder()
            .put(NODE_NAME_SETTING.getKey(), "node")
            .put(
                SharedBlobCacheService.SHARED_CACHE_SIZE_SETTING.getKey(),
                ByteSizeValue.ofBytes(size(100L * randomIntBetween(totalRegions, totalRegions * 2)))
            )
            .put(SharedBlobCacheService.SHARED_CACHE_REGION_SIZE_SETTING.getKey(), ByteSizeValue.ofBytes(size(100)).getStringRep())
            .put(SharedBlobCacheService.SHARED_CACHE_INITIAL_DECAYS_SETTING.getKey(), 0)
            .put("path.home", createTempDir())
            .build();
        final DeterministicTaskQueue taskQueue = new DeterministicTaskQueue();
        try (
            NodeEnvironment environment = new NodeEnvironment(settings, TestEnvironment.newEnvironment(settings));
            var cacheService = new SharedBlobCacheService<>(
                environment,
                settings,
                taskQueue.getThreadPool(),
                taskQueue.getThreadPool().executor(ThreadPool.Names.GENERIC),
                BlobCacheMetrics.NOOP
            )
        ) {
            for (var entry : regionCountPerShard.entrySet()) {
                final TestCacheKey cacheKey = cacheKeyPerShard.get(entry.getKey());
                final long blobLength = size(100L * entry.getValue());
                for (int r = 0; r < entry.getValue(); r++) {
                    cacheService.get(cacheKey, blobLength, r);
                }
            }

            for (var entry : regionCountPerShard.entrySet()) {
                final ShardId shardId = entry.getKey();
                final int expectedRegions = entry.getValue();
                assertThat(cacheService.countCachedRegions(shardId, (key, region) -> true), equalTo((long) expectedRegions));
                assertThat(cacheService.countCachedRegions(shardId, (key, region) -> region == 0), equalTo(1L));
                assertThat(
                    cacheService.countCachedRegions(shardId, (key, region) -> key.equals(cacheKeyPerShard.get(shardId))),
                    equalTo((long) expectedRegions)
                );
            }
            assertThat(cacheService.countCachedRegions(randomShardId(), (key, region) -> true), equalTo(0L));
        }
    }

    public void testForceEvictByShardIdAndRegionPredicate() throws IOException {
        Settings settings = Settings.builder()
            .put(NODE_NAME_SETTING.getKey(), "node")
            .put(SharedBlobCacheService.SHARED_CACHE_SIZE_SETTING.getKey(), ByteSizeValue.ofBytes(size(500)).getStringRep())
            .put(SharedBlobCacheService.SHARED_CACHE_REGION_SIZE_SETTING.getKey(), ByteSizeValue.ofBytes(size(100)).getStringRep())
            .put(SharedBlobCacheService.SHARED_CACHE_INITIAL_DECAYS_SETTING.getKey(), 0)
            .put("path.home", createTempDir())
            .build();
        final DeterministicTaskQueue taskQueue = new DeterministicTaskQueue();
        try (
            NodeEnvironment environment = new NodeEnvironment(settings, TestEnvironment.newEnvironment(settings));
            var cacheService = new SharedBlobCacheService<>(
                environment,
                settings,
                taskQueue.getThreadPool(),
                taskQueue.getThreadPool().executor(ThreadPool.Names.GENERIC),
                BlobCacheMetrics.NOOP
            )
        ) {
            final ShardId shard1 = randomShardId();
            final ShardId shard2 = randomShardId();
            final var cacheKey1 = randomTestCacheKey(shard1);
            final var cacheKey2 = randomTestCacheKey(shard2);

            // populate regions: 2 for shard1, 1 for shard2
            final var region0 = cacheService.get(cacheKey1, size(250), 0);
            final var region1 = cacheService.get(cacheKey1, size(250), 1);
            final var region2 = cacheService.get(cacheKey2, size(250), 0);
            assertEquals(2, cacheService.freeRegionCount());

            // evict only region 0 of shard1
            assertEquals(1, cacheService.forceEvict(shard1, (key, region) -> region == 0));
            assertTrue(region0.isEvicted());
            assertFalse(region1.isEvicted());
            assertFalse(region2.isEvicted());
            assertEquals(3, cacheService.freeRegionCount());

            // evict remaining shard1 regions
            assertEquals(1, cacheService.forceEvict(shard1, (key, region) -> true));
            assertTrue(region1.isEvicted());
            assertFalse(region2.isEvicted());
            assertEquals(4, cacheService.freeRegionCount());

            // evict with a predicate that matches no region
            assertEquals(0, cacheService.forceEvict(shard2, (key, region) -> region == 99));
            assertFalse(region2.isEvicted());

            // evict shard2 by file key
            assertEquals(1, cacheService.forceEvict(shard2, (key, region) -> key.equals(cacheKey2)));
            assertTrue(region2.isEvicted());
            assertEquals(5, cacheService.freeRegionCount());
        }
    }

    public void testSubmitAsyncEviction() throws Exception {
        Settings settings = Settings.builder()
            .put(NODE_NAME_SETTING.getKey(), "node")
            .put(SharedBlobCacheService.SHARED_CACHE_SIZE_SETTING.getKey(), ByteSizeValue.ofBytes(size(500)).getStringRep())
            .put(SharedBlobCacheService.SHARED_CACHE_REGION_SIZE_SETTING.getKey(), ByteSizeValue.ofBytes(size(100)).getStringRep())
            .put(SharedBlobCacheService.SHARED_CACHE_INITIAL_DECAYS_SETTING.getKey(), 0)
            .put("path.home", createTempDir())
            .build();
        final DeterministicTaskQueue taskQueue = new DeterministicTaskQueue();
        try (
            NodeEnvironment environment = new NodeEnvironment(settings, TestEnvironment.newEnvironment(settings));
            var cacheService = new SharedBlobCacheService<>(
                environment,
                settings,
                taskQueue.getThreadPool(),
                taskQueue.getThreadPool().executor(ThreadPool.Names.GENERIC),
                BlobCacheMetrics.NOOP
            )
        ) {
            final ShardId shard1 = randomShardId();
            final var cacheKey1 = randomTestCacheKey(shard1);

            final var region0 = cacheService.get(cacheKey1, size(250), 0);
            final var region1 = cacheService.get(cacheKey1, size(250), 1);
            assertFalse(region0.isEvicted());
            assertFalse(region1.isEvicted());

            AtomicBoolean taskExecuted = new AtomicBoolean(false);
            cacheService.submitAsyncEviction(() -> {
                taskExecuted.set(true);
                cacheService.forceEvict(shard1, (key, region) -> region == 0);
            });

            assertFalse(taskExecuted.get());
            assertFalse(region0.isEvicted());

            taskQueue.runAllRunnableTasks();

            assertTrue(taskExecuted.get());
            assertTrue(region0.isEvicted());
            assertFalse(region1.isEvicted());
            assertEquals(4, cacheService.freeRegionCount());
        }
    }

    public void testDecay() throws IOException {
        RecordingMeterRegistry recordingMeterRegistry = new RecordingMeterRegistry();
        BlobCacheMetrics metrics = new BlobCacheMetrics(recordingMeterRegistry);
        // we have 8 regions
        Settings settings = Settings.builder()
            .put(NODE_NAME_SETTING.getKey(), "node")
            .put(SharedBlobCacheService.SHARED_CACHE_SIZE_SETTING.getKey(), ByteSizeValue.ofBytes(size(400)).getStringRep())
            .put(SharedBlobCacheService.SHARED_CACHE_REGION_SIZE_SETTING.getKey(), ByteSizeValue.ofBytes(size(100)).getStringRep())
            .put(SharedBlobCacheService.SHARED_CACHE_INITIAL_DECAYS_SETTING.getKey(), 0)
            .put("path.home", createTempDir())
            .build();
        final DeterministicTaskQueue taskQueue = new DeterministicTaskQueue();
        try (
            NodeEnvironment environment = new NodeEnvironment(settings, TestEnvironment.newEnvironment(settings));
            var cacheService = new SharedBlobCacheService<>(
                environment,
                settings,
                taskQueue.getThreadPool(),
                taskQueue.getThreadPool().executor(ThreadPool.Names.GENERIC),
                metrics
            )
        ) {
            assertEquals(4, cacheService.freeRegionCount());

            final var cacheKey1 = generateCacheKey();
            final var cacheKey2 = generateCacheKey();
            final var cacheKey3 = generateCacheKey();
            final var evictKey = generateCacheKey();
            // add a region that we can evict when provoking first decay
            cacheService.get(evictKey, size(250), 0);
            assertEquals(3, cacheService.freeRegionCount());
            final var region0 = cacheService.get(cacheKey1, size(250), 0);
            assertEquals(2, cacheService.freeRegionCount());
            final var region1 = cacheService.get(cacheKey2, size(250), 1);
            assertEquals(1, cacheService.freeRegionCount());
            final var region2 = cacheService.get(cacheKey3, size(250), 1);
            assertEquals(0, cacheService.freeRegionCount());

            assertEquals(1, cacheService.getFreq(region0));
            assertEquals(1, cacheService.getFreq(region1));
            assertEquals(1, cacheService.getFreq(region2));
            AtomicLong expectedEpoch = new AtomicLong();
            Runnable triggerDecay = () -> {
                assertThat(taskQueue.hasRunnableTasks(), is(false));
                cacheService.get(generateCacheKey(), size(250), 0);
                assertThat(taskQueue.hasRunnableTasks(), is(true));
                taskQueue.runAllRunnableTasks();
                assertThat(cacheService.epoch(), equalTo(expectedEpoch.incrementAndGet()));
                long epochs = recordedEpochs(recordingMeterRegistry);
                assertEquals(cacheService.epoch(), epochs);
            };

            triggerDecay.run();

            cacheService.get(cacheKey1, size(250), 0);
            cacheService.get(cacheKey2, size(250), 1);
            cacheService.get(cacheKey3, size(250), 1);

            triggerDecay.run();

            final var region0Again = cacheService.get(cacheKey1, size(250), 0);
            assertSame(region0Again, region0);
            assertEquals(3, cacheService.getFreq(region0));
            assertEquals(1, cacheService.getFreq(region1));
            assertEquals(1, cacheService.getFreq(region2));

            triggerDecay.run();

            cacheService.get(cacheKey1, size(250), 0);
            assertEquals(4, cacheService.getFreq(region0));
            cacheService.get(cacheKey1, size(250), 0);
            assertEquals(4, cacheService.getFreq(region0));
            assertEquals(0, cacheService.getFreq(region1));
            assertEquals(0, cacheService.getFreq(region2));

            // ensure no freq=0 entries
            cacheService.get(cacheKey2, size(250), 1);
            cacheService.get(cacheKey3, size(250), 1);
            assertEquals(2, cacheService.getFreq(region1));
            assertEquals(2, cacheService.getFreq(region2));

            triggerDecay.run();

            assertEquals(3, cacheService.getFreq(region0));
            assertEquals(1, cacheService.getFreq(region1));
            assertEquals(1, cacheService.getFreq(region2));

            triggerDecay.run();
            assertEquals(2, cacheService.getFreq(region0));
            assertEquals(0, cacheService.getFreq(region1));
            assertEquals(0, cacheService.getFreq(region2));

            // ensure no freq=0 entries
            cacheService.get(cacheKey2, size(250), 1);
            cacheService.get(cacheKey3, size(250), 1);
            assertEquals(2, cacheService.getFreq(region1));
            assertEquals(2, cacheService.getFreq(region2));

            triggerDecay.run();
            assertEquals(1, cacheService.getFreq(region0));
            assertEquals(1, cacheService.getFreq(region1));
            assertEquals(1, cacheService.getFreq(region2));

            triggerDecay.run();
            assertEquals(0, cacheService.getFreq(region0));
            assertEquals(0, cacheService.getFreq(region1));
            assertEquals(0, cacheService.getFreq(region2));
        }
    }

    /**
     * Verifies that the blob cache free list is partitioned into {@code initial_decays} regions and that
     * a decay is imposed as specified: every {@code numRegions / initial_decays} polls from the initial
     * free list schedule a decay.
     */
    public void testInitialDecaysPartitionsFreeList() throws IOException {
        RecordingMeterRegistry recordingMeterRegistry = new RecordingMeterRegistry();
        BlobCacheMetrics metrics = new BlobCacheMetrics(recordingMeterRegistry);
        final int numRegions = between(10, 100);
        final int initialDecays = between(1, 10);
        int initialDecayPollCount = Math.max(numRegions / initialDecays, 1);
        // Decay triggers when (initialFreeRegions after decrement) % initialDecayPollCount == 0, i.e. at 0, initialDecayPollCount,
        // 2*initialDecayPollCount, ...
        long expectedEpochs = 1 + (numRegions - 1) / initialDecayPollCount;
        Settings settings = Settings.builder()
            .put(NODE_NAME_SETTING.getKey(), "node")
            .put(SharedBlobCacheService.SHARED_CACHE_SIZE_SETTING.getKey(), ByteSizeValue.ofBytes(size(numRegions)).getStringRep())
            .put(SharedBlobCacheService.SHARED_CACHE_REGION_SIZE_SETTING.getKey(), ByteSizeValue.ofBytes(size(1)).getStringRep())
            .put(SharedBlobCacheService.SHARED_CACHE_INITIAL_DECAYS_SETTING.getKey(), initialDecays)
            .put("path.home", createTempDir())
            .build();
        final DeterministicTaskQueue taskQueue = new DeterministicTaskQueue();
        try (
            NodeEnvironment environment = new NodeEnvironment(settings, TestEnvironment.newEnvironment(settings));
            var cacheService = new SharedBlobCacheService<TestCacheKey>(
                environment,
                settings,
                taskQueue.getThreadPool(),
                taskQueue.getThreadPool().executor(ThreadPool.Names.GENERIC),
                metrics
            )
        ) {
            assertThat(cacheService.epoch(), equalTo(0L));
            assertThat(cacheService.freeRegionCount(), equalTo(numRegions));
            long fileLength = size(numRegions + 10);
            // Allocate all regions from the initial free list, running the task queue after each allocation
            // so each scheduled decay runs before the next trigger (epoch advances and next spawn can run).
            // Decay is triggered when (initialFreeRegions after decrement) % initialDecayPollCount == 0.
            for (int i = 0; i < numRegions; i++) {
                long epochBefore = cacheService.epoch();
                cacheService.get(generateCacheKey(), fileLength, 0);
                taskQueue.runAllRunnableTasks();
                long epochAfter = cacheService.epoch();
                boolean decayExpected = (numRegions - (i + 1)) % initialDecayPollCount == 0;
                if (decayExpected) {
                    assertThat(
                        "epoch should advance on decay at poll "
                            + (i + 1)
                            + "/"
                            + numRegions
                            + " (numRegions="
                            + numRegions
                            + ", initialDecays="
                            + initialDecays
                            + ")",
                        epochAfter,
                        equalTo(epochBefore + 1L)
                    );
                } else {
                    assertThat(
                        "epoch should not advance when no decay at poll " + (i + 1) + "/" + numRegions,
                        epochAfter,
                        equalTo(epochBefore)
                    );
                }
            }
            assertThat(
                "total epoch advances should match expected (numRegions=" + numRegions + ", initialDecays=" + initialDecays + ")",
                cacheService.epoch(),
                equalTo(expectedEpochs)
            );
            assertThat(recordedEpochs(recordingMeterRegistry), equalTo(expectedEpochs));
        }
    }

    /**
     * With initial_decays=0 no decay is imposed when consuming the initial free list; epoch stays 0 until eviction triggers decay.
     */
    public void testInitialDecaysZeroDisablesFreeListDecay() throws IOException {
        BlobCacheMetrics metrics = new BlobCacheMetrics(new RecordingMeterRegistry());
        final int numRegions = between(10, 100);
        Settings settings = Settings.builder()
            .put(NODE_NAME_SETTING.getKey(), "node")
            .put(SharedBlobCacheService.SHARED_CACHE_SIZE_SETTING.getKey(), ByteSizeValue.ofBytes(size(numRegions)).getStringRep())
            .put(SharedBlobCacheService.SHARED_CACHE_REGION_SIZE_SETTING.getKey(), ByteSizeValue.ofBytes(size(1)).getStringRep())
            .put(SharedBlobCacheService.SHARED_CACHE_INITIAL_DECAYS_SETTING.getKey(), 0)
            .put("path.home", createTempDir())
            .build();
        final DeterministicTaskQueue taskQueue = new DeterministicTaskQueue();
        try (
            NodeEnvironment environment = new NodeEnvironment(settings, TestEnvironment.newEnvironment(settings));
            var cacheService = new SharedBlobCacheService<TestCacheKey>(
                environment,
                settings,
                taskQueue.getThreadPool(),
                taskQueue.getThreadPool().executor(ThreadPool.Names.GENERIC),
                metrics
            )
        ) {
            assertThat(cacheService.epoch(), equalTo(0L));
            long fileLength = size(numRegions + 10);
            for (int i = 0; i < numRegions; i++) {
                cacheService.get(generateCacheKey(), fileLength, 0);
            }
            taskQueue.runAllRunnableTasks();
            assertThat(
                "no decay should be scheduled when consuming initial free list with initial_decays=0",
                cacheService.epoch(),
                equalTo(0L)
            );
        }
    }

    /**
     * When freq0 is below threshold and the freelist is non-empty, allocation uses a free region
     * and does not schedule decay.
     */
    public void testNoDecayWhenFreelistNonEmptyAndFreq0BelowThreshold() throws IOException {
        final int numRegions = between(20, 100);
        final int threshold = Math.max(1, (int) (numRegions * 0.05));
        Settings settings = Settings.builder()
            .put(NODE_NAME_SETTING.getKey(), "node")
            .put(SharedBlobCacheService.SHARED_CACHE_SIZE_SETTING.getKey(), ByteSizeValue.ofBytes(size(numRegions)).getStringRep())
            .put(SharedBlobCacheService.SHARED_CACHE_REGION_SIZE_SETTING.getKey(), ByteSizeValue.ofBytes(size(1)).getStringRep())
            .put(SharedBlobCacheService.SHARED_CACHE_INITIAL_DECAYS_SETTING.getKey(), 0)
            .put("path.home", createTempDir())
            .build();
        long fileLength = size(numRegions + 10);
        DeterministicTaskQueue taskQueue = new DeterministicTaskQueue();
        try (
            NodeEnvironment env = new NodeEnvironment(settings, TestEnvironment.newEnvironment(settings));
            var cache = new SharedBlobCacheService<TestCacheKey>(
                env,
                settings,
                taskQueue.getThreadPool(),
                taskQueue.getThreadPool().executor(ThreadPool.Names.GENERIC),
                BlobCacheMetrics.NOOP
            )
        ) {
            var keys = new ArrayList<TestCacheKey>(numRegions);
            var regions = new ArrayList<CacheFileRegion<TestCacheKey>>(numRegions);
            for (int i = 0; i < numRegions; i++) {
                var key = generateCacheKey();
                keys.add(key);
                regions.add(cache.get(key, fileLength, 0));
            }
            cache.get(generateCacheKey(), fileLength, 0);
            taskQueue.runAllRunnableTasks();
            for (int i = 0; i < numRegions - 1; i++) {
                cache.get(keys.get(i), fileLength, 0);
            }
            long freq0Count = regions.stream().filter(r -> r.isEvicted() == false).filter(r -> cache.getFreq(r) == 0).count();
            assertThat("freq0 count must be below threshold", freq0Count, lessThan((long) threshold));

            var soleFreq0Region = cache.get(keys.get(numRegions - 1), fileLength, 0);
            synchronized (cache) {
                assertTrue(tryEvict(soleFreq0Region));
            }
            taskQueue.runAllRunnableTasks();
            assertThat(cache.freeRegionCount(), equalTo(1));
            long epochBefore = cache.epoch();
            cache.get(generateCacheKey(), fileLength, 0);
            taskQueue.runAllRunnableTasks();
            assertThat("no decay when freelist is non-empty even though freq0 is below threshold", cache.epoch(), equalTo(epochBefore));
        }
    }

    /**
     * When freq0 is below threshold and the freelist is empty, allocation that triggers eviction
     * does schedule decay.
     */
    public void testDecayWhenFreelistEmptyAndFreq0BelowThreshold() throws IOException {
        final int numRegions = between(20, 100);
        final int threshold = Math.max(1, (int) (numRegions * 0.05));
        Settings settings = Settings.builder()
            .put(NODE_NAME_SETTING.getKey(), "node")
            .put(SharedBlobCacheService.SHARED_CACHE_SIZE_SETTING.getKey(), ByteSizeValue.ofBytes(size(numRegions)).getStringRep())
            .put(SharedBlobCacheService.SHARED_CACHE_REGION_SIZE_SETTING.getKey(), ByteSizeValue.ofBytes(size(1)).getStringRep())
            .put(SharedBlobCacheService.SHARED_CACHE_INITIAL_DECAYS_SETTING.getKey(), 0)
            .put("path.home", createTempDir())
            .build();
        long fileLength = size(numRegions + 10);
        DeterministicTaskQueue taskQueue = new DeterministicTaskQueue();
        try (
            NodeEnvironment env = new NodeEnvironment(settings, TestEnvironment.newEnvironment(settings));
            var cache = new SharedBlobCacheService<TestCacheKey>(
                env,
                settings,
                taskQueue.getThreadPool(),
                taskQueue.getThreadPool().executor(ThreadPool.Names.GENERIC),
                BlobCacheMetrics.NOOP
            )
        ) {
            var keys = new ArrayList<TestCacheKey>(numRegions);
            var regions = new ArrayList<CacheFileRegion<TestCacheKey>>(numRegions);
            for (int i = 0; i < numRegions; i++) {
                var key = generateCacheKey();
                keys.add(key);
                regions.add(cache.get(key, fileLength, 0));
            }
            cache.get(generateCacheKey(), fileLength, 0);
            taskQueue.runAllRunnableTasks();
            for (int i = 0; i < numRegions - 1; i++) {
                cache.get(keys.get(i), fileLength, 0);
            }
            long freq0Count = regions.stream().filter(r -> r.isEvicted() == false).filter(r -> cache.getFreq(r) == 0).count();
            assertThat("freq0 below threshold", freq0Count, lessThan((long) threshold));
            assertThat(cache.freeRegionCount(), equalTo(0));

            long epochBefore = cache.epoch();
            cache.get(generateCacheKey(), fileLength, 0);
            taskQueue.runAllRunnableTasks();
            assertThat(
                "decay is provoked when freelist is empty and freq0 is below threshold (5% of numRegions)",
                cache.epoch(),
                equalTo(epochBefore + 1L)
            );
        }
    }

    /**
     * When freq0 is at or above threshold (5% of numRegions), allocation that triggers eviction
     * does not schedule decay (right time, not sooner).
     */
    public void testNoDecayWhenFreq0AtOrAboveThreshold() throws IOException {
        final int numRegions = between(20, 100);
        final int threshold = Math.max(1, (int) (numRegions * 0.05));
        Settings settings = Settings.builder()
            .put(NODE_NAME_SETTING.getKey(), "node")
            .put(SharedBlobCacheService.SHARED_CACHE_SIZE_SETTING.getKey(), ByteSizeValue.ofBytes(size(numRegions)).getStringRep())
            .put(SharedBlobCacheService.SHARED_CACHE_REGION_SIZE_SETTING.getKey(), ByteSizeValue.ofBytes(size(1)).getStringRep())
            .put(SharedBlobCacheService.SHARED_CACHE_INITIAL_DECAYS_SETTING.getKey(), 0)
            .put("path.home", createTempDir())
            .build();
        long fileLength = size(numRegions + 10);
        DeterministicTaskQueue taskQueue = new DeterministicTaskQueue();
        try (
            NodeEnvironment env = new NodeEnvironment(settings, TestEnvironment.newEnvironment(settings));
            var cache = new SharedBlobCacheService<TestCacheKey>(
                env,
                settings,
                taskQueue.getThreadPool(),
                taskQueue.getThreadPool().executor(ThreadPool.Names.GENERIC),
                BlobCacheMetrics.NOOP
            )
        ) {
            var keys = new ArrayList<TestCacheKey>(numRegions);
            var regions = new ArrayList<CacheFileRegion<TestCacheKey>>(numRegions);
            for (int i = 0; i < numRegions; i++) {
                var key = generateCacheKey();
                keys.add(key);
                regions.add(cache.get(key, fileLength, 0));
            }
            cache.get(generateCacheKey(), fileLength, 0);
            taskQueue.runAllRunnableTasks();
            int promoted = 0;
            for (int i = 0; i < numRegions && promoted < numRegions - threshold - 1; i++) {
                if (regions.get(i).isEvicted() == false) {
                    cache.get(keys.get(i), fileLength, 0);
                    promoted++;
                }
            }
            long freq0Count = regions.stream().filter(r -> r.isEvicted() == false).filter(r -> cache.getFreq(r) == 0).count();
            assertThat("freq0 must be at or above threshold so decay is not triggered", freq0Count, greaterThanOrEqualTo((long) threshold));
            assertThat(cache.freeRegionCount(), equalTo(0));

            long epochBefore = cache.epoch();
            cache.get(generateCacheKey(), fileLength, 0);
            taskQueue.runAllRunnableTasks();
            assertThat(
                "decay is not provoked when freq0 is at or above threshold (5% of numRegions), even with empty freelist",
                cache.epoch(),
                equalTo(epochBefore)
            );
        }
    }

    private static long recordedEpochs(RecordingMeterRegistry recordingMeterRegistry) {
        long epochs = recordingMeterRegistry.getRecorder()
            .getMeasurements(InstrumentType.LONG_COUNTER, "es.blob_cache.epoch.total")
            .stream()
            .mapToLong(Measurement::getLong)
            .sum();
        return epochs;
    }

    /**
     * Test when many objects need to decay, in particular useful to measure how long the decay task takes.
     * For 1M objects (with no assertions) it took 26ms locally.
     */
    public void testMassiveDecay() throws IOException {
        RecordingMeterRegistry recordingMeterRegistry = new RecordingMeterRegistry();
        BlobCacheMetrics metrics = new BlobCacheMetrics(recordingMeterRegistry);
        int regions = 1024; // to measure decay time, increase to 1024*1024 and disable assertions.
        Settings settings = Settings.builder()
            .put(NODE_NAME_SETTING.getKey(), "node")
            .put(SharedBlobCacheService.SHARED_CACHE_SIZE_SETTING.getKey(), ByteSizeValue.ofBytes(size(regions)).getStringRep())
            .put(SharedBlobCacheService.SHARED_CACHE_REGION_SIZE_SETTING.getKey(), ByteSizeValue.ofBytes(size(1)).getStringRep())
            .put(SharedBlobCacheService.SHARED_CACHE_INITIAL_DECAYS_SETTING.getKey(), 0)
            .put("path.home", createTempDir())
            .build();
        final DeterministicTaskQueue taskQueue = new DeterministicTaskQueue();
        try (
            NodeEnvironment environment = new NodeEnvironment(settings, TestEnvironment.newEnvironment(settings));
            var cacheService = new SharedBlobCacheService<>(
                environment,
                settings,
                taskQueue.getThreadPool(),
                taskQueue.getThreadPool().executor(ThreadPool.Names.GENERIC),
                metrics
            )
        ) {
            Runnable decay = () -> {
                assertThat(taskQueue.hasRunnableTasks(), is(true));
                long before = System.currentTimeMillis();
                taskQueue.runAllRunnableTasks();
                long after = System.currentTimeMillis();
                logger.debug("took {} ms", (after - before));
            };
            long fileLength = size(regions + 100);
            TestCacheKey cacheKey = generateCacheKey();
            for (int i = 0; i < regions; ++i) {
                cacheService.get(cacheKey, fileLength, i);
                if (Integer.bitCount(i) == 1) {
                    logger.debug("did {} gets", i);
                }
            }
            assertThat(taskQueue.hasRunnableTasks(), is(false));
            cacheService.get(cacheKey, fileLength, regions);
            decay.run();
            int maxRounds = 5;
            for (int round = 2; round <= maxRounds; ++round) {
                for (int i = round; i < regions + round; ++i) {
                    cacheService.get(cacheKey, fileLength, i);
                    if (Integer.bitCount(i) == 1) {
                        logger.debug("did {} gets", i);
                    }
                }
                decay.run();
            }

            Map<Integer, Integer> freqs = new HashMap<>();
            for (int i = maxRounds; i < regions + maxRounds; ++i) {
                int freq = cacheService.getFreq(cacheService.get(cacheKey, fileLength, i)) - 2;
                freqs.compute(freq, (k, v) -> v == null ? 1 : v + 1);
                if (Integer.bitCount(i) == 1) {
                    logger.debug("did {} gets", i);
                }
            }
            assertThat(freqs.get(4), equalTo(regions - maxRounds + 1));

            long epochs = recordedEpochs(recordingMeterRegistry);
            assertEquals(cacheService.epoch(), epochs);
        }
    }

    /**
     * Exercise SharedBlobCacheService#get in multiple threads to trigger any assertion errors.
     * @throws IOException
     */
    public void testGetMultiThreaded() throws IOException {
        final int threads = between(2, 10);
        final int regionCount = between(1, 20);
        final boolean incRef = randomBoolean();
        // if we have enough regions, a get should always have a result (except for explicit evict interference)
        // if we incRef, we risk the eviction racing against that, leading to no available region, so allow
        // the already closed exception in that case.
        final boolean allowAlreadyClosed = regionCount < threads || incRef;

        logger.info("{} {} {}", threads, regionCount, allowAlreadyClosed);
        Settings settings = Settings.builder()
            .put(NODE_NAME_SETTING.getKey(), "node")
            .put(SharedBlobCacheService.SHARED_CACHE_SIZE_SETTING.getKey(), ByteSizeValue.ofBytes(size(regionCount * 100L)).getStringRep())
            .put(SharedBlobCacheService.SHARED_CACHE_REGION_SIZE_SETTING.getKey(), ByteSizeValue.ofBytes(size(100)).getStringRep())
            .put(SharedBlobCacheService.SHARED_CACHE_INITIAL_DECAYS_SETTING.getKey(), 0)
            .put(SharedBlobCacheService.SHARED_CACHE_MIN_TIME_DELTA_SETTING.getKey(), randomFrom("0", "1ms", "10s"))
            .put("path.home", createTempDir())
            .build();
        long fileLength = size(500);
        ThreadPool threadPool = new TestThreadPool("testGetMultiThreaded");
        ShardId shardId = randomShardId();
        Set<TestCacheKey> files = randomSet(1, 10, () -> randomTestCacheKey(shardId));
        try (
            NodeEnvironment environment = new NodeEnvironment(settings, TestEnvironment.newEnvironment(settings));
            var cacheService = new SharedBlobCacheService<TestCacheKey>(
                environment,
                settings,
                threadPool,
                threadPool.executor(ThreadPool.Names.GENERIC),
                BlobCacheMetrics.NOOP
            )
        ) {
            CyclicBarrier ready = new CyclicBarrier(threads);
            List<Thread> threadList = IntStream.range(0, threads).mapToObj(no -> {
                int iterations = between(100, 500);
                TestCacheKey[] cacheKeys = IntStream.range(0, iterations)
                    .mapToObj(ignore -> randomFrom(files))
                    .toArray(TestCacheKey[]::new);
                int[] regions = IntStream.range(0, iterations).map(ignore -> between(0, 4)).toArray();
                int[] yield = IntStream.range(0, iterations).map(ignore -> between(0, 9)).toArray();
                int[] evict = IntStream.range(0, iterations).map(ignore -> between(0, 199)).toArray();
                return new Thread(() -> {
                    try {
                        ready.await();
                        for (int i = 0; i < iterations; ++i) {
                            try {
                                CacheFileRegion<TestCacheKey> cacheFileRegion;
                                try {
                                    cacheFileRegion = cacheService.get(cacheKeys[i], fileLength, regions[i]);
                                } catch (AlreadyClosedException e) {
                                    assert allowAlreadyClosed || e.getMessage().equals("evicted during free region allocation") : e;
                                    throw e;
                                }
                                assertTrue(cacheFileRegion.testOnlyNonVolatileIO() != null || cacheFileRegion.isEvicted());
                                if (incRef && cacheFileRegion.tryIncRef()) {
                                    if (yield[i] == 0) {
                                        Thread.yield();
                                    }
                                    cacheFileRegion.decRef();
                                }
                                if (evict[i] == 0) {
                                    cacheService.forceEvict(x -> true);
                                } else if (evict[i] == 1) {
                                    cacheService.forceEvict(shardId, x -> true);
                                }
                            } catch (AlreadyClosedException e) {
                                // ignore
                            }
                        }
                    } catch (InterruptedException | BrokenBarrierException e) {
                        assert false;
                        throw new RuntimeException(e);
                    }
                });
            }).toList();
            threadList.forEach(Thread::start);
            threadList.forEach(thread -> {
                try {
                    thread.join();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(e);
                }
            });
        } finally {
            threadPool.shutdownNow();
        }
    }

    private static ShardId randomShardId() {
        return new ShardId(randomAlphaOfLength(10), randomUUID(), between(0, 5));
    }

    public void testCacheSizeRejectedOnNonFrozenNodes() {
        String cacheSize = randomBoolean()
            ? ByteSizeValue.ofBytes(size(500)).getStringRep()
            : (new RatioValue(between(1, 100))).formatNoTrailingZerosPercent();
        final Settings settings = Settings.builder()
            .put(SharedBlobCacheService.SHARED_CACHE_SIZE_SETTING.getKey(), cacheSize)
            .put(SharedBlobCacheService.SHARED_CACHE_REGION_SIZE_SETTING.getKey(), ByteSizeValue.ofBytes(size(100)).getStringRep())
            .putList(NodeRoleSettings.NODE_ROLES_SETTING.getKey(), DiscoveryNodeRole.DATA_HOT_NODE_ROLE.roleName())
            .build();
        final IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> SharedBlobCacheService.SHARED_CACHE_SIZE_SETTING.get(settings)
        );
        assertThat(e.getCause(), notNullValue());
        assertThat(e.getCause(), instanceOf(SettingsException.class));
        assertThat(
            e.getCause().getMessage(),
            is(
                "Setting ["
                    + SharedBlobCacheService.SHARED_CACHE_SIZE_SETTING.getKey()
                    + "] to be positive ["
                    + cacheSize
                    + "] is only permitted on nodes with the data_frozen, search, or indexing role. Roles are [data_hot]"
            )
        );
    }

    public void testMultipleDataPathsRejectedOnFrozenNodes() {
        final Settings settings = Settings.builder()
            .put(SharedBlobCacheService.SHARED_CACHE_SIZE_SETTING.getKey(), ByteSizeValue.ofBytes(size(500)).getStringRep())
            .putList(NodeRoleSettings.NODE_ROLES_SETTING.getKey(), DiscoveryNodeRole.DATA_FROZEN_NODE_ROLE.roleName())
            .putList(Environment.PATH_DATA_SETTING.getKey(), List.of("a", "b"))
            .build();
        final IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> SharedBlobCacheService.SHARED_CACHE_SIZE_SETTING.get(settings)
        );
        assertThat(e.getCause(), notNullValue());
        assertThat(e.getCause(), instanceOf(SettingsException.class));
        assertThat(
            e.getCause().getMessage(),
            is(
                "setting ["
                    + SharedBlobCacheService.SHARED_CACHE_SIZE_SETTING.getKey()
                    + "="
                    + ByteSizeValue.ofBytes(size(500)).getStringRep()
                    + "] is not permitted on nodes with multiple data paths [a,b]"
            )
        );
    }

    public void testDedicateFrozenCacheSizeDefaults() {
        final Settings settings = Settings.builder()
            .putList(NodeRoleSettings.NODE_ROLES_SETTING.getKey(), DiscoveryNodeRole.DATA_FROZEN_NODE_ROLE.roleName())
            .build();

        RelativeByteSizeValue relativeCacheSize = SharedBlobCacheService.SHARED_CACHE_SIZE_SETTING.get(settings);
        assertThat(relativeCacheSize.isAbsolute(), is(false));
        assertThat(relativeCacheSize.isNonZeroSize(), is(true));
        assertThat(relativeCacheSize.calculateValue(ByteSizeValue.ofBytes(10000), null), equalTo(ByteSizeValue.ofBytes(9000)));
        assertThat(SharedBlobCacheService.SHARED_CACHE_SIZE_MAX_HEADROOM_SETTING.get(settings), equalTo(ByteSizeValue.ofGb(100)));
    }

    public void testNotDedicatedFrozenCacheSizeDefaults() {
        final Settings settings = Settings.builder()
            .putList(
                NodeRoleSettings.NODE_ROLES_SETTING.getKey(),
                Sets.union(
                    Set.of(
                        randomFrom(
                            DiscoveryNodeRole.DATA_HOT_NODE_ROLE,
                            DiscoveryNodeRole.DATA_COLD_NODE_ROLE,
                            DiscoveryNodeRole.DATA_WARM_NODE_ROLE,
                            DiscoveryNodeRole.DATA_CONTENT_NODE_ROLE
                        )
                    ),
                    new HashSet<>(
                        randomSubsetOf(
                            between(0, 3),
                            DiscoveryNodeRole.DATA_FROZEN_NODE_ROLE,
                            DiscoveryNodeRole.INGEST_ROLE,
                            DiscoveryNodeRole.MASTER_ROLE
                        )
                    )
                ).stream().map(DiscoveryNodeRole::roleName).collect(Collectors.toList())
            )
            .build();

        RelativeByteSizeValue relativeCacheSize = SharedBlobCacheService.SHARED_CACHE_SIZE_SETTING.get(settings);
        assertThat(relativeCacheSize.isNonZeroSize(), is(false));
        assertThat(relativeCacheSize.isAbsolute(), is(true));
        assertThat(relativeCacheSize.getAbsolute(), equalTo(ByteSizeValue.ZERO));
        assertThat(SharedBlobCacheService.SHARED_CACHE_SIZE_MAX_HEADROOM_SETTING.get(settings), equalTo(ByteSizeValue.ofBytes(-1)));
    }

    public void testSearchOrIndexNodeCacheSizeDefaults() {
        final Settings settings = Settings.builder()
            .putList(
                NodeRoleSettings.NODE_ROLES_SETTING.getKey(),
                randomFrom(DiscoveryNodeRole.SEARCH_ROLE, DiscoveryNodeRole.INDEX_ROLE).roleName()
            )
            .build();

        RelativeByteSizeValue relativeCacheSize = SharedBlobCacheService.SHARED_CACHE_SIZE_SETTING.get(settings);
        assertThat(relativeCacheSize.isAbsolute(), is(false));
        assertThat(relativeCacheSize.isNonZeroSize(), is(true));
        assertThat(relativeCacheSize.calculateValue(ByteSizeValue.ofBytes(10000), null), equalTo(ByteSizeValue.ofBytes(9000)));
        assertThat(SharedBlobCacheService.SHARED_CACHE_SIZE_MAX_HEADROOM_SETTING.get(settings), equalTo(ByteSizeValue.ofGb(100)));
    }

    public void testMaxHeadroomRejectedForAbsoluteCacheSize() {
        String cacheSize = ByteSizeValue.ofBytes(size(500)).getStringRep();
        final Settings settings = Settings.builder()
            .put(SharedBlobCacheService.SHARED_CACHE_SIZE_SETTING.getKey(), cacheSize)
            .put(SharedBlobCacheService.SHARED_CACHE_SIZE_MAX_HEADROOM_SETTING.getKey(), ByteSizeValue.ofBytes(size(100)).getStringRep())
            .putList(NodeRoleSettings.NODE_ROLES_SETTING.getKey(), DiscoveryNodeRole.DATA_FROZEN_NODE_ROLE.roleName())
            .build();
        final IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> SharedBlobCacheService.SHARED_CACHE_SIZE_MAX_HEADROOM_SETTING.get(settings)
        );
        assertThat(e.getCause(), notNullValue());
        assertThat(e.getCause(), instanceOf(SettingsException.class));
        assertThat(
            e.getCause().getMessage(),
            is(
                "setting ["
                    + SharedBlobCacheService.SHARED_CACHE_SIZE_MAX_HEADROOM_SETTING.getKey()
                    + "] cannot be specified for absolute ["
                    + SharedBlobCacheService.SHARED_CACHE_SIZE_SETTING.getKey()
                    + "="
                    + cacheSize
                    + "]"
            )
        );
    }

    public void testCalculateCacheSize() {
        long smallSize = 10000;
        long largeSize = ByteSizeValue.ofTb(10).getBytes();
        assertThat(SharedBlobCacheService.calculateCacheSize(Settings.EMPTY, smallSize), equalTo(0L));
        final Settings settings = Settings.builder()
            .putList(NodeRoleSettings.NODE_ROLES_SETTING.getKey(), DiscoveryNodeRole.DATA_FROZEN_NODE_ROLE.roleName())
            .build();
        assertThat(SharedBlobCacheService.calculateCacheSize(settings, smallSize), equalTo(9000L));
        assertThat(SharedBlobCacheService.calculateCacheSize(settings, largeSize), equalTo(largeSize - ByteSizeValue.ofGb(100).getBytes()));
    }

    private static TestCacheKey generateCacheKey() {
        return randomTestCacheKey(randomShardId());
    }

    public void testCacheSizeChanges() throws IOException {
        ByteSizeValue val1 = ByteSizeValue.of(randomIntBetween(1, 5), ByteSizeUnit.MB);
        Settings settings = Settings.builder()
            .put(NODE_NAME_SETTING.getKey(), "node")
            .put(SharedBlobCacheService.SHARED_CACHE_SIZE_SETTING.getKey(), val1.getStringRep())
            .put(SharedBlobCacheService.SHARED_CACHE_REGION_SIZE_SETTING.getKey(), ByteSizeValue.ofBytes(size(100)).getStringRep())
            .put(SharedBlobCacheService.SHARED_CACHE_INITIAL_DECAYS_SETTING.getKey(), 0)
            .put("path.home", createTempDir())
            .build();
        final DeterministicTaskQueue taskQueue = new DeterministicTaskQueue();
        try (
            NodeEnvironment environment = new NodeEnvironment(settings, TestEnvironment.newEnvironment(settings));
            SharedBlobCacheService<?> cacheService = new SharedBlobCacheService<>(
                environment,
                settings,
                taskQueue.getThreadPool(),
                taskQueue.getThreadPool().executor(ThreadPool.Names.GENERIC),
                BlobCacheMetrics.NOOP
            )
        ) {
            assertEquals(val1.getBytes(), cacheService.getStats().size());
        }

        ByteSizeValue val2 = ByteSizeValue.of(randomIntBetween(1, 5), ByteSizeUnit.MB);
        settings = Settings.builder()
            .put(settings)
            .put(SharedBlobCacheService.SHARED_CACHE_SIZE_SETTING.getKey(), val2.getStringRep())
            .build();
        try (
            NodeEnvironment environment = new NodeEnvironment(settings, TestEnvironment.newEnvironment(settings));
            SharedBlobCacheService<?> cacheService = new SharedBlobCacheService<>(
                environment,
                settings,
                taskQueue.getThreadPool(),
                taskQueue.getThreadPool().executor(ThreadPool.Names.GENERIC),
                BlobCacheMetrics.NOOP
            )
        ) {
            assertEquals(val2.getBytes(), cacheService.getStats().size());
        }
    }

    public void testMaybeEvictLeastUsed() throws Exception {
        final int numRegions = 10;
        final long regionSize = size(1L);
        Settings settings = Settings.builder()
            .put(NODE_NAME_SETTING.getKey(), "node")
            .put(SharedBlobCacheService.SHARED_CACHE_SIZE_SETTING.getKey(), ByteSizeValue.ofBytes(size(numRegions)).getStringRep())
            .put(SharedBlobCacheService.SHARED_CACHE_REGION_SIZE_SETTING.getKey(), ByteSizeValue.ofBytes(regionSize).getStringRep())
            .put(SharedBlobCacheService.SHARED_CACHE_INITIAL_DECAYS_SETTING.getKey(), 0)
            .put("path.home", createTempDir())
            .build();

        final DeterministicTaskQueue taskQueue = new DeterministicTaskQueue();
        try (
            NodeEnvironment environment = new NodeEnvironment(settings, TestEnvironment.newEnvironment(settings));
            var cacheService = new SharedBlobCacheService<TestCacheKey>(
                environment,
                settings,
                taskQueue.getThreadPool(),
                taskQueue.getThreadPool().executor(ThreadPool.Names.GENERIC),
                BlobCacheMetrics.NOOP
            )
        ) {
            final Map<TestCacheKey, CacheFileRegion<TestCacheKey>> cacheEntries = new HashMap<>();

            assertThat("All regions are free", cacheService.freeRegionCount(), equalTo(numRegions));
            assertThat("Cache has no entries", cacheService.maybeEvictLeastUsed(generateCacheKey(), regionSize, 0), is(false));

            // use all regions in cache
            for (int i = 0; i < numRegions; i++) {
                final var cacheKey = generateCacheKey();
                var entry = cacheService.get(cacheKey, regionSize, 0);
                entry.populate(
                    ByteRange.of(0L, regionSize),
                    (channel, channelPos, streamFactory, relativePos, length, progressUpdater, completionListener) -> completeWith(
                        completionListener,
                        () -> progressUpdater.accept(length)
                    ),
                    taskQueue.getThreadPool().generic(),
                    ActionListener.noop()
                );
                assertThat(cacheService.getFreq(entry), equalTo(1));
                cacheEntries.put(cacheKey, entry);
            }

            assertThat("Expected all regions to be used", cacheService.freeRegionCount(), equalTo(0));
            assertThat(
                "Expected no entries old enough to be evicted",
                cacheService.maybeEvictLeastUsed(generateCacheKey(), regionSize, 0),
                is(false)
            );

            taskQueue.runAllRunnableTasks();

            assertThat("Expected all regions to be used", cacheService.freeRegionCount(), equalTo(0));
            assertThat(
                "Expected no entries old enough to be evicted",
                cacheService.maybeEvictLeastUsed(generateCacheKey(), regionSize, 0),
                is(false)
            );

            cacheService.maybeScheduleDecayAndNewEpoch();
            taskQueue.runAllRunnableTasks();

            cacheEntries.keySet().forEach(key -> cacheService.get(key, regionSize, 0));
            cacheService.maybeScheduleDecayAndNewEpoch();
            taskQueue.runAllRunnableTasks();

            // touch some random cache entries
            var usedCacheKeys = Set.copyOf(randomSubsetOf(cacheEntries.keySet()));
            usedCacheKeys.forEach(key -> cacheService.get(key, regionSize, 0));

            cacheEntries.forEach(
                (key, entry) -> assertThat(cacheService.getFreq(entry), usedCacheKeys.contains(key) ? equalTo(3) : equalTo(1))
            );

            assertThat("Expected all regions to be used", cacheService.freeRegionCount(), equalTo(0));
            assertThat(
                "Expected no entries old enough to be evicted",
                cacheService.maybeEvictLeastUsed(generateCacheKey(), regionSize, 0),
                is(false)
            );

            cacheService.maybeScheduleDecayAndNewEpoch();
            taskQueue.runAllRunnableTasks();

            assertThat("Expected all regions to be used", cacheService.freeRegionCount(), equalTo(0));
            cacheEntries.forEach(
                (key, entry) -> assertThat(cacheService.getFreq(entry), usedCacheKeys.contains(key) ? equalTo(2) : equalTo(0))
            );

            var zeroFrequencyCacheEntries = cacheEntries.size() - usedCacheKeys.size();
            for (int i = 0; i < zeroFrequencyCacheEntries; i++) {
                assertThat(cacheService.freeRegionCount(), equalTo(i));
                assertThat(
                    "Expected at least one entry old enough to be evicted",
                    cacheService.maybeEvictLeastUsed(generateCacheKey(), regionSize, 0),
                    is(true)
                );
                assertThat(cacheService.freeRegionCount(), equalTo(i + 1));
            }

            assertThat(
                "Expected no more entries old enough to be evicted",
                cacheService.maybeEvictLeastUsed(generateCacheKey(), regionSize, 0),
                is(false)
            );
            assertThat(cacheService.freeRegionCount(), equalTo(zeroFrequencyCacheEntries));
        }
    }

    /**
     * Drives the lowest-frequency eviction scanner `SharedBlobCacheService#maybeEvictLeastUsed` directly and asserts that
     * each invocation records the right {@code mode}, {@code outcome} and {@code entriesScanned}. The clock advances by a fixed amount
     * on every read, and the scanner reads it exactly twice per call (start + end), so the recorded scan time is deterministic.
     */
    public void testEvictionScanMetricsLowestFrequency() throws Exception {
        final int numRegions = 10;
        final long regionSize = size(1L);
        final long freqScanTimeTakenMicros = randomLongBetween(1, 10_000);
        final long freqScanTimeTakenNanos = TimeUnit.MICROSECONDS.toNanos(freqScanTimeTakenMicros);
        final AtomicLong clock = new AtomicLong();
        Settings settings = Settings.builder()
            .put(NODE_NAME_SETTING.getKey(), "node")
            .put(SharedBlobCacheService.SHARED_CACHE_SIZE_SETTING.getKey(), ByteSizeValue.ofBytes(size(numRegions)))
            .put(SharedBlobCacheService.SHARED_CACHE_REGION_SIZE_SETTING.getKey(), ByteSizeValue.ofBytes(regionSize))
            .put(SharedBlobCacheService.SHARED_CACHE_INITIAL_DECAYS_SETTING.getKey(), 0)
            .put("path.home", createTempDir())
            .build();

        final DeterministicTaskQueue taskQueue = new DeterministicTaskQueue();
        final RecordingMeterRegistry recording = new RecordingMeterRegistry();
        try (
            NodeEnvironment environment = new NodeEnvironment(settings, TestEnvironment.newEnvironment(settings));
            var cacheService = new SharedBlobCacheService<TestCacheKey>(
                environment,
                settings,
                taskQueue.getThreadPool(),
                taskQueue.getThreadPool().executor(ThreadPool.Names.GENERIC),
                new BlobCacheMetrics(recording),
                () -> clock.addAndGet(freqScanTimeTakenNanos),
                new DefaultEvictionPolicy<>()
            )
        ) {
            // fill the cache: every entry lands at frequency 1, leaving the lowest-frequency (0) list empty
            for (int i = 0; i < numRegions; i++) {
                var entry = cacheService.get(generateCacheKey(), regionSize, 0);
                entry.populate(
                    ByteRange.of(0L, regionSize),
                    (channel, channelPos, streamFactory, relativePos, length, progressUpdater, completionListener) -> completeWith(
                        completionListener,
                        () -> progressUpdater.accept(length)
                    ),
                    taskQueue.getThreadPool().generic(),
                    ActionListener.noop()
                );
                assertThat(cacheService.getFreq(entry), equalTo(1));
            }
            taskQueue.runAllRunnableTasks();
            assertThat(cacheService.freeRegionCount(), equalTo(0));

            // the lowest-frequency list is empty, so the scan walks nothing and frees nothing
            assertThat(cacheService.maybeEvictLeastUsed(generateCacheKey(), regionSize, 0), is(false));

            var none = evictionScanMeasurements(recording, LONG_HISTOGRAM, BLOB_CACHE_EVICTION_SCANNED_ENTRIES, LowestFrequency, None);
            assertThat(none, hasSize(1));
            assertThat(none.get(0).getLong(), is(0L));
            var noneTime = evictionScanMeasurements(recording, DOUBLE_HISTOGRAM, BLOB_CACHE_EVICTION_SCAN_TIME, LowestFrequency, None);
            assertThat(noneTime, hasSize(1));
            assertThat(noneTime.get(0).getDouble(), is((double) freqScanTimeTakenMicros));

            // a decay moves every entry down to frequency 0, making them eligible for the lowest-frequency scan
            cacheService.maybeScheduleDecayAndNewEpoch();
            taskQueue.runAllRunnableTasks();

            // each call evicts the head of the lowest-frequency list
            for (int i = 0; i < numRegions; i++) {
                assertThat(cacheService.maybeEvictLeastUsed(generateCacheKey(), regionSize, 0), is(true));
            }

            var evicted = evictionScanMeasurements(
                recording,
                LONG_HISTOGRAM,
                BLOB_CACHE_EVICTION_SCANNED_ENTRIES,
                LowestFrequency,
                Evicted
            );
            assertThat(evicted, hasSize(numRegions));
            for (Measurement measurement : evicted) {
                assertThat(measurement.getLong(), is(1L));
            }
            var evictedTime = evictionScanMeasurements(
                recording,
                DOUBLE_HISTOGRAM,
                BLOB_CACHE_EVICTION_SCAN_TIME,
                LowestFrequency,
                Evicted
            );
            assertThat(evictedTime, hasSize(numRegions));
            for (Measurement measurement : evictedTime) {
                assertThat(measurement.getDouble(), is((double) freqScanTimeTakenMicros));
            }

            // every eviction scan reached through this path is a lowest-frequency scan
            for (Measurement measurement : recording.getRecorder().getMeasurements(LONG_HISTOGRAM, BLOB_CACHE_EVICTION_SCANNED_ENTRIES)) {
                assertThat(measurement.attributes().get(BlobCacheMetrics.EVICTION_SCAN_MODE_ATTRIBUTE_KEY), is(LowestFrequency.name()));
            }
        }
    }

    /**
     * Drives the all-frequencies eviction scanner ({@code maybeEvictAndTake}, reached via {@link SharedBlobCacheService#get} when no free
     * region is available) and asserts the {@code mode}, {@code outcome} and {@code entriesScanned} for both an evicting policy and a
     * policy that never evicts (forcing the scan to walk every cached entry before failing the allocation).
     */
    public void testEvictionScanMetricsAllFrequencies() throws Exception {
        final int numRegions = randomIntBetween(4, 20);
        final long regionSize = size(1L);
        Settings settings = Settings.builder()
            .put(NODE_NAME_SETTING.getKey(), "node")
            .put(SharedBlobCacheService.SHARED_CACHE_SIZE_SETTING.getKey(), ByteSizeValue.ofBytes(size(numRegions)).getStringRep())
            .put(SharedBlobCacheService.SHARED_CACHE_REGION_SIZE_SETTING.getKey(), ByteSizeValue.ofBytes(regionSize).getStringRep())
            .put(SharedBlobCacheService.SHARED_CACHE_INITIAL_DECAYS_SETTING.getKey(), 0)
            .put("path.home", createTempDir())
            .build();
        final DeterministicTaskQueue taskQueue = new DeterministicTaskQueue();

        // scenario 1 (Evicted): the default policy evicts the head of the lowest non-empty frequency bucket
        final RecordingMeterRegistry recordingEvicted = new RecordingMeterRegistry();
        try (
            NodeEnvironment environment = new NodeEnvironment(settings, TestEnvironment.newEnvironment(settings));
            var cacheService = new SharedBlobCacheService<TestCacheKey>(
                environment,
                settings,
                taskQueue.getThreadPool(),
                taskQueue.getThreadPool().executor(ThreadPool.Names.GENERIC),
                new BlobCacheMetrics(recordingEvicted)
            )
        ) {
            // fill the cache: every entry lands at frequency 1, leaving the lowest-frequency (0) list empty
            for (int i = 0; i < numRegions; i++) {
                cacheService.get(generateCacheKey(), regionSize, 0);
            }
            assertThat(cacheService.freeRegionCount(), equalTo(0));

            // get() of a new key has no free region: the scan walks frequency 0 (empty) then evicts the frequency-1 head
            cacheService.get(generateCacheKey(), regionSize, 0);

            var evicted = evictionScanMeasurements(
                recordingEvicted,
                LONG_HISTOGRAM,
                BLOB_CACHE_EVICTION_SCANNED_ENTRIES,
                AllFrequencies,
                Evicted
            );
            assertThat(evicted, hasSize(1));
            assertThat(evicted.get(0).getLong(), is(1L));
            assertThat(
                evictionScanMeasurements(recordingEvicted, DOUBLE_HISTOGRAM, BLOB_CACHE_EVICTION_SCAN_TIME, AllFrequencies, Evicted),
                hasSize(1)
            );
            assertThat(
                evictionScanMeasurementsByMode(recordingEvicted, LONG_HISTOGRAM, BLOB_CACHE_EVICTION_SCANNED_ENTRIES, LowestFrequency),
                empty()
            );
            assertThat(
                evictionScanMeasurementsByMode(recordingEvicted, DOUBLE_HISTOGRAM, BLOB_CACHE_EVICTION_SCAN_TIME, LowestFrequency),
                empty()
            );
            assertThat(recordingEvicted.getRecorder().getMeasurements(LONG_HISTOGRAM, BLOB_CACHE_EVICTION_SCANNED_ENTRIES), hasSize(1));
            assertThat(recordingEvicted.getRecorder().getMeasurements(DOUBLE_HISTOGRAM, BLOB_CACHE_EVICTION_SCAN_TIME), hasSize(1));
        }

        // scenario 2 (None): a policy that never evicts walks every cached entry across every frequency bucket and frees nothing
        final RecordingMeterRegistry recordingNone = new RecordingMeterRegistry();
        final EvictionPolicy<TestCacheKey> neverEvict = new EvictionPolicy<>() {
            @Override
            public Predicate<CacheRegion<TestCacheKey>> createPredicate(CacheRegion<TestCacheKey> incoming) {
                return Predicates.never();
            }

            @Override
            public void onCached(CacheRegion<TestCacheKey> region) {}

            @Override
            public void onEvicted(CacheRegion<TestCacheKey> region) {}
        };
        try (
            NodeEnvironment environment = new NodeEnvironment(settings, TestEnvironment.newEnvironment(settings));
            var cacheService = new SharedBlobCacheService<TestCacheKey>(
                environment,
                settings,
                taskQueue.getThreadPool(),
                taskQueue.getThreadPool().executor(ThreadPool.Names.GENERIC),
                new BlobCacheMetrics(recordingNone),
                neverEvict
            )
        ) {
            for (int i = 0; i < numRegions; i++) {
                cacheService.get(generateCacheKey(), regionSize, 0);
            }
            assertThat(cacheService.freeRegionCount(), equalTo(0));

            // no entry is evictable: the scan walks every cached entry once across all frequency buckets and the allocation fails
            expectThrows(AlreadyClosedException.class, () -> cacheService.get(generateCacheKey(), regionSize, 0));

            var none = evictionScanMeasurements(recordingNone, LONG_HISTOGRAM, BLOB_CACHE_EVICTION_SCANNED_ENTRIES, AllFrequencies, None);
            assertThat(none, hasSize(1));
            assertThat(none.get(0).getLong(), is((long) numRegions));
            assertThat(
                evictionScanMeasurements(recordingNone, DOUBLE_HISTOGRAM, BLOB_CACHE_EVICTION_SCAN_TIME, AllFrequencies, None),
                hasSize(1)
            );
            assertThat(
                evictionScanMeasurementsByMode(recordingNone, LONG_HISTOGRAM, BLOB_CACHE_EVICTION_SCANNED_ENTRIES, LowestFrequency),
                empty()
            );
            assertThat(
                evictionScanMeasurementsByMode(recordingNone, DOUBLE_HISTOGRAM, BLOB_CACHE_EVICTION_SCAN_TIME, LowestFrequency),
                empty()
            );
            assertThat(recordingNone.getRecorder().getMeasurements(LONG_HISTOGRAM, BLOB_CACHE_EVICTION_SCANNED_ENTRIES), hasSize(1));
            assertThat(recordingNone.getRecorder().getMeasurements(DOUBLE_HISTOGRAM, BLOB_CACHE_EVICTION_SCAN_TIME), hasSize(1));
        }
    }

    /// Drives the all-frequency scanner to the rarely-hit `Free` outcome and asserts the recorded `mode`, `outcome` and `entriesScanned`.
    ///
    /// The `Free` outcome fires when a region appears in `freeRegions` *during* the scan's poll, rather than being produced by the
    /// scan's own eviction. To trigger it deterministically, we install a policy that never evicts, but when its eviction predicate is
    /// created force-evicts a victim that has been parked in a higher frequency bucket. `forceEvict` bypasses the eviction predicate (so
    /// the side effect fires exactly once) and re-enters the same reentrant monitor already held by the in-flight scan, freeing one region
    /// into `freeRegions` which the scan's next poll then picks up.
    ///
    /// The victim must live in a *different* frequency bucket than the one being scanned: `maybeEvictAndTakeForFrequency` walks its
    /// bucket's linked list in place, so force-evicting an entry from that same bucket would unlink the cursor (or a later node)
    /// mid-traversal, making `entriesScanned` and the outcome non-deterministic with no exception to flag it. We therefore fill the
    /// cache, decay everything to frequency 0, then promote the victim to frequency 2 via a cache hit, so `forceEvict` only mutates
    /// `freqs[2]` and the freq-0 walk of the remaining `numRegions - 1` entries stays intact.
    public void testEvictionScanMetricsFreeOutcome() throws Exception {
        final int numRegions = randomIntBetween(4, 20);
        final long regionSize = size(1L);
        final long freqScanTimeTakenMicros = randomLongBetween(1, 10_000);
        final long freqScanTimeTakenNanos = TimeUnit.MICROSECONDS.toNanos(freqScanTimeTakenMicros);
        final AtomicLong clock = new AtomicLong();
        Settings settings = Settings.builder()
            .put(NODE_NAME_SETTING.getKey(), "node")
            .put(SharedBlobCacheService.SHARED_CACHE_SIZE_SETTING.getKey(), ByteSizeValue.ofBytes(size(numRegions)))
            .put(SharedBlobCacheService.SHARED_CACHE_REGION_SIZE_SETTING.getKey(), ByteSizeValue.ofBytes(regionSize))
            .put(SharedBlobCacheService.SHARED_CACHE_INITIAL_DECAYS_SETTING.getKey(), 0)
            .put("path.home", createTempDir())
            .build();

        final DeterministicTaskQueue taskQueue = new DeterministicTaskQueue();
        final RecordingMeterRegistry recording = new RecordingMeterRegistry();
        final AtomicReference<SharedBlobCacheService<TestCacheKey>> serviceRef = new AtomicReference<>();
        final TestCacheKey victimKey = generateCacheKey();

        // never evicts, but when its eviction predicate is created force-evicts the victim
        final EvictionPolicy<TestCacheKey> freeingPolicy = new EvictionPolicy<>() {
            final AtomicBoolean forcedOnce = new AtomicBoolean(false);

            @Override
            public Predicate<CacheRegion<TestCacheKey>> createPredicate(CacheRegion<TestCacheKey> incoming) {
                if (forcedOnce.compareAndSet(false, true)) {
                    serviceRef.get().forceEvict(victimKey::equals);
                }
                return Predicates.never();
            }

            @Override
            public void onCached(CacheRegion<TestCacheKey> region) {}

            @Override
            public void onEvicted(CacheRegion<TestCacheKey> region) {}
        };

        try (
            NodeEnvironment environment = new NodeEnvironment(settings, TestEnvironment.newEnvironment(settings));
            var cacheService = new SharedBlobCacheService<>(
                environment,
                settings,
                taskQueue.getThreadPool(),
                taskQueue.getThreadPool().executor(ThreadPool.Names.GENERIC),
                new BlobCacheMetrics(recording),
                () -> clock.addAndGet(freqScanTimeTakenNanos),
                freeingPolicy
            )
        ) {
            serviceRef.set(cacheService);

            // fill the cache: the victim plus numRegions - 1 other keys, all landing at frequency 1
            cacheService.get(victimKey, regionSize, 0);
            for (int i = 0; i < numRegions - 1; i++) {
                cacheService.get(generateCacheKey(), regionSize, 0);
            }
            assertThat(cacheService.freeRegionCount(), equalTo(0));

            // decay moves every entry to frequency 0 and advances the epoch, so the victim can be promoted on its next access
            cacheService.maybeScheduleDecayAndNewEpoch();
            taskQueue.runAllRunnableTasks();

            // a cache hit promotes the victim to frequency 2, parking it in a bucket the freq-0 scan never walks
            var victimEntry = cacheService.get(victimKey, regionSize, 0);
            assertThat(cacheService.getFreq(victimEntry), equalTo(2));

            // the cache is still full, so the only way a region can land in freeRegions mid-scan is the in-scan force-evict below
            assertThat(cacheService.freeRegionCount(), equalTo(0));

            // get() of a new key has no free region: the freq-0 scan walks all numRegions - 1 entries (predicate false), the first of
            // which force-evicts the freq-2 victim; the freq-1 poll then picks up that freed region, giving the Free outcome
            cacheService.get(generateCacheKey(), regionSize, 0);

            var scanned = evictionScanMeasurements(recording, LONG_HISTOGRAM, BLOB_CACHE_EVICTION_SCANNED_ENTRIES, AllFrequencies, Free);
            assertThat(scanned, hasSize(1));
            assertThat(scanned.get(0).getLong(), is((long) numRegions - 1));

            var time = evictionScanMeasurements(recording, DOUBLE_HISTOGRAM, BLOB_CACHE_EVICTION_SCAN_TIME, AllFrequencies, Free);
            assertThat(time, hasSize(1));
            // inside of scan window we capture lock-acquisition timing for force-evicting, which is 2 clock ticks, plus the delta of 1 here
            assertThat(time.get(0).getDouble(), is(3 * (double) freqScanTimeTakenMicros));

            // exactly one scan was recorded in the whole test, and none of it on the lowest-frequency path
            assertThat(recording.getRecorder().getMeasurements(LONG_HISTOGRAM, BLOB_CACHE_EVICTION_SCANNED_ENTRIES), hasSize(1));
            assertThat(recording.getRecorder().getMeasurements(DOUBLE_HISTOGRAM, BLOB_CACHE_EVICTION_SCAN_TIME), hasSize(1));
            assertThat(
                evictionScanMeasurementsByMode(recording, LONG_HISTOGRAM, BLOB_CACHE_EVICTION_SCANNED_ENTRIES, LowestFrequency),
                empty()
            );
            assertThat(
                evictionScanMeasurementsByMode(recording, DOUBLE_HISTOGRAM, BLOB_CACHE_EVICTION_SCAN_TIME, LowestFrequency),
                empty()
            );
            assertThat(cacheService.countCachedRegions(victimKey::equals), equalTo(0L));
        }
    }

    /// Drives the lowest-frequency scanner and asserts that `entriesScanned` counts the protected (non-evictable) entries it walks past
    /// before reaching an evictable one, landing strictly between 1 and `numRegions`.
    ///
    /// We fill the cache, protect the first `skip` inserted keys via the policy, then decay everything to frequency 0 (which preserves
    /// insertion order, so the protected keys sit at the head). A single scan then walks those `skip` protected head entries (counted
    /// but skipped), evicts the `(skip+1)`-th, and stops, so `entriesScanned == skip + 1`.
    public void testEvictionScanMetricsSkipsNonEvictableEntries() throws Exception {
        final int numRegions = randomIntBetween(4, 20);
        final int skip = randomIntBetween(1, numRegions - 1);
        final long regionSize = size(1L);
        final long freqScanTimeTakenMicros = randomLongBetween(1, 10_000);
        final long freqScanTimeTakenNanos = TimeUnit.MICROSECONDS.toNanos(freqScanTimeTakenMicros);
        final AtomicLong clock = new AtomicLong();
        final Settings settings = Settings.builder()
            .put(NODE_NAME_SETTING.getKey(), "node")
            .put(SharedBlobCacheService.SHARED_CACHE_SIZE_SETTING.getKey(), ByteSizeValue.ofBytes(size(numRegions)))
            .put(SharedBlobCacheService.SHARED_CACHE_REGION_SIZE_SETTING.getKey(), ByteSizeValue.ofBytes(regionSize))
            .put(SharedBlobCacheService.SHARED_CACHE_INITIAL_DECAYS_SETTING.getKey(), 0)
            .put("path.home", createTempDir())
            .build();

        final DeterministicTaskQueue taskQueue = new DeterministicTaskQueue();
        final RecordingMeterRegistry recording = new RecordingMeterRegistry();
        // Protection is keyed, not positional: eviction physically removes head entries, so protecting "the first N still present" would
        // shift the target as scans proceed. A fixed key set keeps the `(skip+1)`-th inserted key as the deterministic victim.
        final Set<TestCacheKey> protectedKeys = new HashSet<>();
        final EvictionPolicy<TestCacheKey> protectFirstSkip = new EvictionPolicy<>() {
            @Override
            public Predicate<CacheRegion<TestCacheKey>> createPredicate(CacheRegion<TestCacheKey> incoming) {
                return region -> protectedKeys.contains(region.key()) == false;
            }

            @Override
            public void onCached(CacheRegion<TestCacheKey> region) {}

            @Override
            public void onEvicted(CacheRegion<TestCacheKey> region) {}
        };

        try (
            NodeEnvironment environment = new NodeEnvironment(settings, TestEnvironment.newEnvironment(settings));
            var cacheService = new SharedBlobCacheService<>(
                environment,
                settings,
                taskQueue.getThreadPool(),
                taskQueue.getThreadPool().executor(ThreadPool.Names.GENERIC),
                new BlobCacheMetrics(recording),
                () -> clock.addAndGet(freqScanTimeTakenNanos),
                protectFirstSkip
            )
        ) {
            // fill the cache in insertion order; every entry lands at frequency 1
            final List<TestCacheKey> keys = new ArrayList<>();
            for (int i = 0; i < numRegions; i++) {
                final var key = generateCacheKey();
                var entry = cacheService.get(key, regionSize, 0);
                entry.populate(
                    ByteRange.of(0L, regionSize),
                    (channel, channelPos, streamFactory, relativePos, length, progressUpdater, completionListener) -> completeWith(
                        completionListener,
                        () -> progressUpdater.accept(length)
                    ),
                    taskQueue.getThreadPool().generic(),
                    ActionListener.noop()
                );
                assertThat(cacheService.getFreq(entry), equalTo(1));
                keys.add(key);
            }
            taskQueue.runAllRunnableTasks();
            assertThat(cacheService.freeRegionCount(), equalTo(0));

            // protect the first skip inserted keys: they sit at the head once decayed and are walked-but-skipped
            protectedKeys.addAll(keys.subList(0, skip));

            // decay moves every entry to frequency 0, preserving insertion order (so protected keys stay at the head)
            cacheService.maybeScheduleDecayAndNewEpoch();
            taskQueue.runAllRunnableTasks();

            // one scan: walks the skip protected head entries (counted), then evicts the (skip+1)-th and stops
            assertThat(cacheService.maybeEvictLeastUsed(generateCacheKey(), regionSize, 0), is(true));

            var evicted = evictionScanMeasurements(
                recording,
                LONG_HISTOGRAM,
                BLOB_CACHE_EVICTION_SCANNED_ENTRIES,
                LowestFrequency,
                Evicted
            );
            assertThat(evicted, hasSize(1));
            assertThat(evicted.get(0).getLong(), is((long) skip + 1));

            var evictedTime = evictionScanMeasurements(
                recording,
                DOUBLE_HISTOGRAM,
                BLOB_CACHE_EVICTION_SCAN_TIME,
                LowestFrequency,
                Evicted
            );
            assertThat(evictedTime, hasSize(1));
            assertThat(evictedTime.get(0).getDouble(), is((double) freqScanTimeTakenMicros));

            assertThat(recording.getRecorder().getMeasurements(LONG_HISTOGRAM, BLOB_CACHE_EVICTION_SCANNED_ENTRIES), hasSize(1));
            assertThat(recording.getRecorder().getMeasurements(DOUBLE_HISTOGRAM, BLOB_CACHE_EVICTION_SCAN_TIME), hasSize(1));
            assertThat(
                evictionScanMeasurementsByMode(recording, LONG_HISTOGRAM, BLOB_CACHE_EVICTION_SCANNED_ENTRIES, AllFrequencies),
                empty()
            );
            assertThat(evictionScanMeasurementsByMode(recording, DOUBLE_HISTOGRAM, BLOB_CACHE_EVICTION_SCAN_TIME, AllFrequencies), empty());
        }
    }

    /// Drives the all-frequencies scanner and asserts that `entriesScanned` accumulates across two frequency buckets in a single scan.
    ///
    /// We place `freq0Regions` protected entries at frequency 0 (filled then decayed) and `freq1Regions` entries at frequency 1
    /// (filled afterwards, no decay), protecting all of the freq-0 entries plus the first `skipFreq1Regions` of the freq-1 entries.
    /// A single scan then walks every protected freq-0 entry, finds nothing freed at the freq-1 boundary,
    /// skips the `skipFreq1Regions` protected freq-1 entries, and evicts the first eligible one,
    /// so `entriesScanned == freq0Regions + skipFreq1Regions + 1` (strictly greater than 1, spanning both buckets).
    /// As in the lowest-frequency variant, protection is keyed so the victim stays put as the scan proceeds.
    public void testEvictionScanMetricsSkipsAcrossFrequencyBuckets() throws Exception {
        final int numRegions = randomIntBetween(6, 30);
        final int freq0Regions = randomIntBetween(1, numRegions - 2);
        final int freq1Regions = numRegions - freq0Regions; // always >= 2
        // protected prefix within freq 1. victim is freq1Keys.get(skipFreq1Regions)
        final int skipFreq1Regions = randomIntBetween(1, freq1Regions - 1);
        final long regionSize = size(1L);
        final long freqScanTimeTakenMicros = randomLongBetween(1, 10_000);
        final long freqScanTimeTakenNanos = TimeUnit.MICROSECONDS.toNanos(freqScanTimeTakenMicros);
        final AtomicLong clock = new AtomicLong();
        Settings settings = Settings.builder()
            .put(NODE_NAME_SETTING.getKey(), "node")
            .put(SharedBlobCacheService.SHARED_CACHE_SIZE_SETTING.getKey(), ByteSizeValue.ofBytes(size(numRegions)))
            .put(SharedBlobCacheService.SHARED_CACHE_REGION_SIZE_SETTING.getKey(), ByteSizeValue.ofBytes(regionSize))
            .put(SharedBlobCacheService.SHARED_CACHE_INITIAL_DECAYS_SETTING.getKey(), 0)
            .put("path.home", createTempDir())
            .build();

        final DeterministicTaskQueue taskQueue = new DeterministicTaskQueue();
        final RecordingMeterRegistry recording = new RecordingMeterRegistry();
        final Set<TestCacheKey> protectedKeys = new HashSet<>();
        final EvictionPolicy<TestCacheKey> protectByKey = new EvictionPolicy<>() {
            @Override
            public Predicate<CacheRegion<TestCacheKey>> createPredicate(CacheRegion<TestCacheKey> incoming) {
                return region -> protectedKeys.contains(region.key()) == false;
            }

            @Override
            public void onCached(CacheRegion<TestCacheKey> region) {}

            @Override
            public void onEvicted(CacheRegion<TestCacheKey> region) {}
        };

        try (
            NodeEnvironment environment = new NodeEnvironment(settings, TestEnvironment.newEnvironment(settings));
            var cacheService = new SharedBlobCacheService<TestCacheKey>(
                environment,
                settings,
                taskQueue.getThreadPool(),
                taskQueue.getThreadPool().executor(ThreadPool.Names.GENERIC),
                new BlobCacheMetrics(recording),
                () -> clock.addAndGet(freqScanTimeTakenNanos),
                protectByKey
            )
        ) {
            // fill the freq-0 set at frequency 1, leaving freq1Regions free slots
            final List<TestCacheKey> freq0Keys = new ArrayList<>();
            for (int i = 0; i < freq0Regions; i++) {
                final var key = generateCacheKey();
                cacheService.get(key, regionSize, 0);
                freq0Keys.add(key);
            }

            // decay moves the freq-0 set down to frequency 0
            cacheService.maybeScheduleDecayAndNewEpoch();
            taskQueue.runAllRunnableTasks();

            // fill the freq-1 set at frequency 1, filling the remaining slots
            final List<TestCacheKey> freq1Keys = new ArrayList<>();
            for (int i = 0; i < freq1Regions; i++) {
                final var key = generateCacheKey();
                cacheService.get(key, regionSize, 0);
                freq1Keys.add(key);
            }
            assertThat(cacheService.freeRegionCount(), equalTo(0));
            final TestCacheKey victim = freq1Keys.get(skipFreq1Regions);

            // protect the whole freq-0 set plus the first N of the freq-1 set; the victim is the first eligible entry reached
            protectedKeys.addAll(freq0Keys);
            protectedKeys.addAll(freq1Keys.subList(0, skipFreq1Regions));

            // get() of a new key has no free region: the freq-0 scan walks all freq0Regions protected entries; the free-region poll at
            // the freq-1 boundary returns nothing (the cache is full and the scan has freed nothing, so this is not the Free path);
            // then the freq-1 scan skips the `skipFreq1Regions` protected entries and evicts freq1Keys.get(skipFreq1Regions)
            cacheService.get(generateCacheKey(), regionSize, 0);

            var evicted = evictionScanMeasurements(recording, LONG_HISTOGRAM, BLOB_CACHE_EVICTION_SCANNED_ENTRIES, AllFrequencies, Evicted);
            assertThat(evicted, hasSize(1));
            assertThat(evicted.get(0).getLong(), is((long) freq0Regions + skipFreq1Regions + 1));

            var evictedTime = evictionScanMeasurements(recording, DOUBLE_HISTOGRAM, BLOB_CACHE_EVICTION_SCAN_TIME, AllFrequencies, Evicted);
            assertThat(evictedTime, hasSize(1));
            assertThat(evictedTime.get(0).getDouble(), is((double) freqScanTimeTakenMicros));

            assertThat(recording.getRecorder().getMeasurements(LONG_HISTOGRAM, BLOB_CACHE_EVICTION_SCANNED_ENTRIES), hasSize(1));
            assertThat(recording.getRecorder().getMeasurements(DOUBLE_HISTOGRAM, BLOB_CACHE_EVICTION_SCAN_TIME), hasSize(1));
            assertThat(
                evictionScanMeasurementsByMode(recording, LONG_HISTOGRAM, BLOB_CACHE_EVICTION_SCANNED_ENTRIES, LowestFrequency),
                empty()
            );
            assertThat(
                evictionScanMeasurementsByMode(recording, DOUBLE_HISTOGRAM, BLOB_CACHE_EVICTION_SCAN_TIME, LowestFrequency),
                empty()
            );
            assertThat(cacheService.countCachedRegions(victim::equals), equalTo(0L));
        }
    }

    /// Drives a single [SharedBlobCacheService#get] of a fresh key into a cache with a free slot and asserts the resulting
    /// [BlobCacheMetrics.LockAcquireSite#SlotAssignment] sample. No eviction is needed, so the cache-miss eviction site is untouched.
    public void testLockAcquireMetricsSlotAssignment() throws Exception {
        runLockAcquireMetricsTest(ctx -> {
            ctx.cacheService().get(generateCacheKey(), ctx.regionSize(), 0);

            assertLockAcquireSamples(ctx.recording(), SlotAssignment, 1, ctx.clockStepMicros());
            // a free slot was available, so no eviction victim had to be scanned for
            assertThat(lockAcquireMeasurements(ctx.recording(), CacheMissEviction), empty());
        });
    }

    /// Fills the cache, then drives an evicting [SharedBlobCacheService#get] on a brand-new key with zero free regions: the
    /// cache-miss path scans for a victim (one [BlobCacheMetrics.LockAcquireSite#CacheMissEviction]) then installs the incoming
    /// region ([BlobCacheMetrics.LockAcquireSite#SlotAssignment]). The fill produces one {@code SlotAssignment} per region.
    public void testLockAcquireMetricsCacheMissEviction() throws Exception {
        runLockAcquireMetricsTest(ctx -> {
            for (int i = 0; i < ctx.numRegions(); i++) {
                ctx.cacheService().get(generateCacheKey(), ctx.regionSize(), 0);
            }
            assertThat(ctx.cacheService().freeRegionCount(), equalTo(0));
            // filling used free slots only, so no eviction has happened yet
            assertThat(lockAcquireMeasurements(ctx.recording(), CacheMissEviction), empty());
            // one SlotAssignment per fill, before any eviction
            assertLockAcquireSamples(ctx.recording(), SlotAssignment, ctx.numRegions(), ctx.clockStepMicros());

            // reset so the only SlotAssignment we assert after this is the post-eviction install
            ctx.recording().getRecorder().resetCalls();

            // brand-new key with no free region: initChunk -> maybeEvictAndTake (CacheMissEviction) -> assignToSlot (SlotAssignment)
            ctx.cacheService().get(generateCacheKey(), ctx.regionSize(), 0);

            assertLockAcquireSamples(ctx.recording(), CacheMissEviction, 1, ctx.clockStepMicros());
            assertLockAcquireSamples(ctx.recording(), SlotAssignment, 1, ctx.clockStepMicros());
        });
    }

    /// Drives a cache hit after the epoch has advanced past the entry's last-accessed epoch, exercising the
    /// [BlobCacheMetrics.LockAcquireSite#Promote] site. The decay task that advances the epoch records exactly one
    /// [BlobCacheMetrics.LockAcquireSite#Decay] sample, which is filtered out of the promote assertion.
    public void testLockAcquireMetricsPromote() throws Exception {
        runLockAcquireMetricsTest(ctx -> {
            final var key = generateCacheKey();
            ctx.cacheService().get(key, ctx.regionSize(), 0);

            // advance the epoch so the next access to key promotes it; this decay task records one Decay sample
            ctx.cacheService().maybeScheduleDecayAndNewEpoch();
            ctx.taskQueue().runAllRunnableTasks();
            assertLockAcquireSamples(ctx.recording(), Decay, 1, ctx.clockStepMicros());

            ctx.cacheService().get(key, ctx.regionSize(), 0);
            assertLockAcquireSamples(ctx.recording(), Promote, 1, ctx.clockStepMicros());
        });
    }

    /// Drives the best-effort prefetch scanner `SharedBlobCacheService::maybeEvictLeastUsed` on an empty freq-0 list: the lock is
    /// taken unconditionally, so a single [BlobCacheMetrics.LockAcquireSite#LowestFrequencyEviction] sample is recorded even though nothing
    /// is evicted.
    public void testLockAcquireMetricsPrefetchEviction() throws Exception {
        runLockAcquireMetricsTest(ctx -> {
            assertThat(ctx.cacheService().maybeEvictLeastUsed(generateCacheKey(), ctx.regionSize(), 0), is(false));

            assertLockAcquireSamples(ctx.recording(), LowestFrequencyEviction, 1, ctx.clockStepMicros());
        });
    }

    /// Drives the whole-key-mapping bulk eviction [SharedBlobCacheService#forceEvict(Predicate)] and asserts the
    /// [BlobCacheMetrics.LockAcquireSite#ForceEvict] site. A no-match call takes no lock and records nothing.
    public void testLockAcquireMetricsForceEvictByKey() throws Exception {
        runLockAcquireMetricsTest(ctx -> {
            final var key = generateCacheKey();
            ctx.cacheService().get(key, ctx.regionSize(), 0);

            // no matching entries: the lock is never taken
            assertThat(ctx.cacheService().forceEvict(k -> false), equalTo(0));
            assertThat(lockAcquireMeasurements(ctx.recording(), ForceEvict), empty());

            assertThat(ctx.cacheService().forceEvict(key::equals), equalTo(1));
            assertLockAcquireSamples(ctx.recording(), ForceEvict, 1, ctx.clockStepMicros());
        });
    }

    /// Asserts the async whole-key-mapping eviction path [SharedBlobCacheService#forceEvictAsync] funnels into the same
    /// [BlobCacheMetrics.LockAcquireSite#ForceEvict] site, recording exactly one sample once the queued task runs.
    public void testLockAcquireMetricsForceEvictByKeyAsync() throws Exception {
        runLockAcquireMetricsTest(ctx -> {
            final var key = generateCacheKey();
            ctx.cacheService().get(key, ctx.regionSize(), 0);

            ctx.cacheService().forceEvictAsync(key::equals);
            // nothing recorded until the queued task runs
            assertThat(lockAcquireMeasurements(ctx.recording(), ForceEvict), empty());

            ctx.taskQueue().runAllRunnableTasks();
            assertLockAcquireSamples(ctx.recording(), ForceEvict, 1, ctx.clockStepMicros());
        });
    }

    /// Drives the shard-scoped bulk eviction [SharedBlobCacheService#forceEvict(ShardId, Predicate)] and asserts the
    /// [BlobCacheMetrics.LockAcquireSite#ForceEvict] site. A no-match {@code BiPredicate} call takes no lock and records
    /// nothing.
    public void testLockAcquireMetricsForceEvictByShard() throws Exception {
        runLockAcquireMetricsTest(ctx -> {
            final ShardId shard = randomShardId();
            final var key = randomTestCacheKey(shard);
            ctx.cacheService().get(key, ctx.regionSize(), 0);

            // no matching regions for the shard: the lock is never taken
            assertThat(ctx.cacheService().forceEvict(shard, (k, region) -> false), equalTo(0));
            assertThat(lockAcquireMeasurements(ctx.recording(), ForceEvict), empty());

            assertThat(ctx.cacheService().forceEvict(shard, key::equals), equalTo(1));
            assertLockAcquireSamples(ctx.recording(), ForceEvict, 1, ctx.clockStepMicros());
        });
    }

    /// Drives [SharedBlobCacheService#demoteAll] on a shard holding a freq>0 region and asserts the
    /// [BlobCacheMetrics.LockAcquireSite#Demote] site. Demoting an unknown shard matches nothing, takes no lock, and records nothing.
    public void testLockAcquireMetricsDemote() throws Exception {
        runLockAcquireMetricsTest(ctx -> {
            final ShardId shard = randomShardId();
            final var key = randomTestCacheKey(shard);
            ctx.cacheService().get(key, ctx.regionSize(), 0); // lands at frequency 1

            // unknown shard, no matching entries: the lock is never taken
            assertThat(ctx.cacheService().demoteAll(randomShardId()), equalTo(0));
            assertThat(lockAcquireMeasurements(ctx.recording(), Demote), empty());

            assertThat(ctx.cacheService().demoteAll(shard), equalTo(1));
            assertLockAcquireSamples(ctx.recording(), Demote, 1, ctx.clockStepMicros());
        });
    }

    /// Drives the background LFU decay directly [SharedBlobCacheService#computeDecay] and asserts the single
    /// [BlobCacheMetrics.LockAcquireSite#Decay] sample it records unconditionally.
    public void testLockAcquireMetricsDecay() throws Exception {
        runLockAcquireMetricsTest(ctx -> {
            ctx.cacheService().computeDecay();

            assertLockAcquireSamples(ctx.recording(), Decay, 1, ctx.clockStepMicros());
        });
    }

    private static List<Measurement> evictionScanMeasurements(
        RecordingMeterRegistry recording,
        InstrumentType instrumentType,
        String histogramName,
        BlobCacheMetrics.EvictionScanMode mode,
        BlobCacheMetrics.EvictionScanOutcome outcome
    ) {
        return recording.getRecorder()
            .getMeasurements(instrumentType, histogramName)
            .stream()
            .filter(m -> mode.name().equals(m.attributes().get(BlobCacheMetrics.EVICTION_SCAN_MODE_ATTRIBUTE_KEY)))
            .filter(m -> outcome.name().equals(m.attributes().get(BlobCacheMetrics.EVICTION_SCAN_OUTCOME_ATTRIBUTE_KEY)))
            .toList();
    }

    private static List<Measurement> evictionScanMeasurementsByMode(
        RecordingMeterRegistry recording,
        InstrumentType instrumentType,
        String histogramName,
        BlobCacheMetrics.EvictionScanMode mode
    ) {
        return recording.getRecorder()
            .getMeasurements(instrumentType, histogramName)
            .stream()
            .filter(m -> mode.name().equals(m.attributes().get(BlobCacheMetrics.EVICTION_SCAN_MODE_ATTRIBUTE_KEY)))
            .toList();
    }

    private static List<Measurement> lockAcquireMeasurements(
        final RecordingMeterRegistry recording,
        final BlobCacheMetrics.LockAcquireSite site
    ) {
        return recording.getRecorder()
            .getMeasurements(DOUBLE_HISTOGRAM, BLOB_CACHE_LOCK_ACQUIRE_TIME)
            .stream()
            .filter(m -> site.name().equals(m.attributes().get(LOCK_ACQUIRE_SITE_ATTRIBUTE_KEY)))
            .toList();
    }

    /// Asserts that exactly {@code expectedCount} lock-acquire samples were recorded for {@code site}, each carrying only the
    /// {@code es_lock_acquire_site} attribute set to {@code site} and the deterministic-clock value {@code clockStepMicros}.
    private static void assertLockAcquireSamples(
        final RecordingMeterRegistry recording,
        final BlobCacheMetrics.LockAcquireSite site,
        final int expectedCount,
        final long clockStepMicros
    ) {
        final var samples = lockAcquireMeasurements(recording, site);
        assertThat(samples, hasSize(expectedCount));
        for (Measurement sample : samples) {
            assertThat(sample.getDouble(), is((double) clockStepMicros));
            assertThat(sample.attributes().keySet(), hasSize(1));
            assertThat(sample.attributes(), hasKey(LOCK_ACQUIRE_SITE_ATTRIBUTE_KEY));
            assertThat(sample.attributes().get(LOCK_ACQUIRE_SITE_ATTRIBUTE_KEY), is(site.name()));
        }
    }

    /// Bundles the objects created by [#runLockAcquireMetricsTest] so a test body can drive the cache and assert against the
    /// recorded lock-acquire samples without repeating the shared setup.
    private record LockAcquireMetricsTestContext(
        SharedBlobCacheService<TestCacheKey> cacheService,
        RecordingMeterRegistry recording,
        DeterministicTaskQueue taskQueue,
        int numRegions,
        long regionSize,
        long clockStepMicros
    ) {
        private LockAcquireMetricsTestContext {
            Stream.of(cacheService, recording, taskQueue).forEach(Objects::requireNonNull);
            assertThat(numRegions, greaterThan(0));
            assertThat(regionSize, greaterThan(0L));
            assertThat(clockStepMicros, greaterThan(0L));
        }
    }

    /// Builds the settings, environment and cache shared by every {@code testLockAcquireMetrics*} test (a fixed number of
    /// single-region slots, a deterministic clock that advances a random step per read, and a [DefaultEvictionPolicy]) and runs
    /// {@code body} against the resulting [LockAcquireMetricsTestContext].
    private void runLockAcquireMetricsTest(CheckedConsumer<LockAcquireMetricsTestContext, Exception> body) throws Exception {
        final int numRegions = randomIntBetween(2, 10);
        final long regionSize = size(1L);
        final long clockStepMicros = randomLongBetween(1, 10_000);
        final long clockStepNanos = TimeUnit.MICROSECONDS.toNanos(clockStepMicros);
        final AtomicLong clock = new AtomicLong();
        final Settings settings = lockAcquireCacheSettings(numRegions, regionSize);
        final DeterministicTaskQueue taskQueue = new DeterministicTaskQueue();
        final RecordingMeterRegistry recording = new RecordingMeterRegistry();
        try (
            NodeEnvironment environment = new NodeEnvironment(settings, TestEnvironment.newEnvironment(settings));
            var cacheService = new SharedBlobCacheService<TestCacheKey>(
                environment,
                settings,
                taskQueue.getThreadPool(),
                taskQueue.getThreadPool().executor(ThreadPool.Names.GENERIC),
                new BlobCacheMetrics(recording),
                () -> clock.addAndGet(clockStepNanos),
                new DefaultEvictionPolicy<>()
            )
        ) {
            body.accept(new LockAcquireMetricsTestContext(cacheService, recording, taskQueue, numRegions, regionSize, clockStepMicros));
        }
    }

    private Settings lockAcquireCacheSettings(final long numRegions, final long regionSize) {
        return Settings.builder()
            .put(NODE_NAME_SETTING.getKey(), "node")
            .put(SharedBlobCacheService.SHARED_CACHE_SIZE_SETTING.getKey(), ByteSizeValue.ofBytes(size(numRegions)))
            .put(SharedBlobCacheService.SHARED_CACHE_REGION_SIZE_SETTING.getKey(), ByteSizeValue.ofBytes(regionSize))
            // disable the initial-decay scheduling so it does not fire extra Decay tasks and muddy the per-site sample counts
            .put(SharedBlobCacheService.SHARED_CACHE_INITIAL_DECAYS_SETTING.getKey(), 0)
            .put("path.home", createTempDir())
            .build();
    }

    public void testMaybeFetchRegion() throws Exception {
        final long cacheSize = size(500L);
        final long regionSize = size(100L);
        Settings settings = Settings.builder()
            .put(NODE_NAME_SETTING.getKey(), "node")
            .put(SharedBlobCacheService.SHARED_CACHE_SIZE_SETTING.getKey(), ByteSizeValue.ofBytes(cacheSize).getStringRep())
            .put(SharedBlobCacheService.SHARED_CACHE_REGION_SIZE_SETTING.getKey(), ByteSizeValue.ofBytes(regionSize).getStringRep())
            .put(SharedBlobCacheService.SHARED_CACHE_INITIAL_DECAYS_SETTING.getKey(), 0)
            .put("path.home", createTempDir())
            .build();

        final var bulkTaskCount = new AtomicInteger(0);
        final var threadPool = new TestThreadPool("test");
        final var bulkExecutor = new StoppableExecutorServiceWrapper(threadPool.generic()) {
            @Override
            public void execute(Runnable command) {
                super.execute(command);
                bulkTaskCount.incrementAndGet();
            }
        };

        try (
            NodeEnvironment environment = new NodeEnvironment(settings, TestEnvironment.newEnvironment(settings));
            var cacheService = new SharedBlobCacheService<>(
                environment,
                settings,
                threadPool,
                threadPool.executor(ThreadPool.Names.GENERIC),
                BlobCacheMetrics.NOOP
            )
        ) {
            {
                // fetch a single region
                final var cacheKey = generateCacheKey();
                assertEquals(5, cacheService.freeRegionCount());
                final long blobLength = size(250); // 3 regions
                AtomicLong bytesRead = new AtomicLong(0L);
                final PlainActionFuture<Boolean> future = new PlainActionFuture<>();
                cacheService.maybeFetchRegion(
                    cacheKey,
                    0,
                    blobLength,
                    (channel, channelPos, streamFactory, relativePos, length, progressUpdater, completionListener) -> completeWith(
                        completionListener,
                        () -> {
                            assert streamFactory == null : streamFactory;
                            bytesRead.addAndGet(length);
                            progressUpdater.accept(length);
                        }
                    ),
                    bulkExecutor,
                    future
                );

                var fetched = future.get(10, TimeUnit.SECONDS);
                assertThat("Region has been fetched", fetched, is(true));
                assertEquals(regionSize, bytesRead.get());
                assertEquals(4, cacheService.freeRegionCount());
                assertEquals(1, bulkTaskCount.get());
            }
            {
                // fetch multiple regions to used all the cache
                final int remainingFreeRegions = cacheService.freeRegionCount();
                assertEquals(4, cacheService.freeRegionCount());

                final var cacheKey = generateCacheKey();
                final long blobLength = regionSize * remainingFreeRegions;
                AtomicLong bytesRead = new AtomicLong(0L);

                final PlainActionFuture<Collection<Boolean>> future = new PlainActionFuture<>();
                final var listener = new GroupedActionListener<>(remainingFreeRegions, future);
                for (int region = 0; region < remainingFreeRegions; region++) {
                    cacheService.maybeFetchRegion(
                        cacheKey,
                        region,
                        blobLength,
                        (channel, channelPos, streamFactory, relativePos, length, progressUpdater, completionListener) -> completeWith(
                            completionListener,
                            () -> {
                                assert streamFactory == null : streamFactory;
                                bytesRead.addAndGet(length);
                                progressUpdater.accept(length);
                            }
                        ),
                        bulkExecutor,
                        listener
                    );
                }

                var results = future.get(10, TimeUnit.SECONDS);
                assertThat(results.stream().allMatch(result -> result), is(true));
                assertEquals(blobLength, bytesRead.get());
                assertEquals(0, cacheService.freeRegionCount());
                assertEquals(1 + remainingFreeRegions, bulkTaskCount.get());
            }
            {
                // cache fully used, no entry old enough to be evicted
                assertEquals(0, cacheService.freeRegionCount());
                final var cacheKey = generateCacheKey();
                final PlainActionFuture<Boolean> future = new PlainActionFuture<>();
                final int region = randomIntBetween(0, 10);
                cacheService.maybeFetchRegion(
                    cacheKey,
                    region,
                    regionSize * (region + 1),
                    (channel, channelPos, streamFactory, relativePos, length, progressUpdater, completionListener) -> completeWith(
                        completionListener,
                        () -> {
                            throw new AssertionError("should not be executed");
                        }
                    ),
                    bulkExecutor,
                    future
                );
                assertThat("Listener is immediately completed", future.isDone(), is(true));
                assertThat("Region already exists in cache", future.get(), is(false));
            }
            {
                cacheService.computeDecay();

                // fetch one more region should evict an old cache entry
                final var cacheKey = generateCacheKey();
                assertEquals(0, cacheService.freeRegionCount());
                long blobLength = randomLongBetween(1L, regionSize);
                AtomicLong bytesRead = new AtomicLong(0L);
                final PlainActionFuture<Boolean> future = new PlainActionFuture<>();
                cacheService.maybeFetchRegion(
                    cacheKey,
                    0,
                    blobLength,
                    (channel, channelPos, ignore, relativePos, length, progressUpdater, completionListener) -> completeWith(
                        completionListener,
                        () -> {
                            assert ignore == null : ignore;
                            bytesRead.addAndGet(length);
                            progressUpdater.accept(length);
                        }
                    ),
                    bulkExecutor,
                    future
                );

                var fetched = future.get(10, TimeUnit.SECONDS);
                assertThat("Region has been fetched", fetched, is(true));
                assertEquals(blobLength, bytesRead.get());
                assertEquals(0, cacheService.freeRegionCount());
            }
        }

        threadPool.shutdown();
    }

    public void testFetchRegion() throws Exception {
        final long cacheSize = size(500L);
        final long regionSize = size(100L);
        Settings settings = Settings.builder()
            .put(NODE_NAME_SETTING.getKey(), "node")
            .put(SharedBlobCacheService.SHARED_CACHE_SIZE_SETTING.getKey(), ByteSizeValue.ofBytes(cacheSize).getStringRep())
            .put(SharedBlobCacheService.SHARED_CACHE_REGION_SIZE_SETTING.getKey(), ByteSizeValue.ofBytes(regionSize).getStringRep())
            .put(SharedBlobCacheService.SHARED_CACHE_INITIAL_DECAYS_SETTING.getKey(), 0)
            .put("path.home", createTempDir())
            .build();

        final var threadPool = new TestThreadPool("test");

        try (
            NodeEnvironment environment = new NodeEnvironment(settings, TestEnvironment.newEnvironment(settings));
            var cacheService = new SharedBlobCacheService<>(
                environment,
                settings,
                threadPool,
                threadPool.executor(ThreadPool.Names.GENERIC),
                BlobCacheMetrics.NOOP
            )
        ) {
            {
                // fetch a single region
                final var cacheKey = generateCacheKey();
                assertEquals(5, cacheService.freeRegionCount());
                final long blobLength = size(250); // 3 regions
                final AtomicLong bytesRead = new AtomicLong(0L);
                final PlainActionFuture<Boolean> future = new PlainActionFuture<>();
                final var bulkTaskCount = new AtomicInteger(0);
                final var executionFinishedLatch = new CountDownLatch(1);
                cacheService.fetchRegion(
                    cacheKey,
                    0,
                    blobLength,
                    (channel, channelPos, streamFactory, relativePos, length, progressUpdater, completionListener) -> completeWith(
                        completionListener,
                        () -> {
                            assert streamFactory == null : streamFactory;
                            bytesRead.addAndGet(length);
                            progressUpdater.accept(length);
                        }
                    ),
                    bulkExecutor(threadPool, bulkTaskCount, executionFinishedLatch),
                    true,
                    future
                );

                var fetched = future.get(10, TimeUnit.SECONDS);
                safeAwait(executionFinishedLatch);
                assertThat("Region has been fetched", fetched, is(true));
                assertEquals(regionSize, bytesRead.get());
                assertEquals(4, cacheService.freeRegionCount());
                assertEquals(1, bulkTaskCount.get());
            }
            {
                // fetch multiple regions to used all the cache
                final int remainingFreeRegions = cacheService.freeRegionCount();
                assertEquals(4, cacheService.freeRegionCount());

                final var cacheKey = generateCacheKey();
                final long blobLength = regionSize * remainingFreeRegions;
                final AtomicLong bytesRead = new AtomicLong(0L);

                final PlainActionFuture<Collection<Boolean>> future = new PlainActionFuture<>();
                final var listener = new GroupedActionListener<>(remainingFreeRegions, future);
                final var bulkTaskCount = new AtomicInteger(0);
                final var executionFinishedLatch = new CountDownLatch(remainingFreeRegions);
                for (int region = 0; region < remainingFreeRegions; region++) {
                    cacheService.fetchRegion(
                        cacheKey,
                        region,
                        blobLength,
                        (channel, channelPos, streamFactory, relativePos, length, progressUpdater, completionListener) -> completeWith(
                            completionListener,
                            () -> {
                                assert streamFactory == null : streamFactory;
                                bytesRead.addAndGet(length);
                                progressUpdater.accept(length);
                            }
                        ),
                        bulkExecutor(threadPool, bulkTaskCount, executionFinishedLatch),
                        true,
                        listener
                    );
                }

                var results = future.get(10, TimeUnit.SECONDS);
                safeAwait(executionFinishedLatch);
                assertThat(results.stream().allMatch(result -> result), is(true));
                assertEquals(blobLength, bytesRead.get());
                assertEquals(0, cacheService.freeRegionCount());
                assertEquals(remainingFreeRegions, bulkTaskCount.get());
            }
            {
                // cache fully used, no entry old enough to be evicted and force=false should not evict entries
                assertEquals(0, cacheService.freeRegionCount());
                final var cacheKey = generateCacheKey();
                final PlainActionFuture<Boolean> future = new PlainActionFuture<>();
                cacheService.fetchRegion(
                    cacheKey,
                    0,
                    regionSize,
                    (channel, channelPos, streamFactory, relativePos, length, progressUpdater, completionListener) -> completeWith(
                        completionListener,
                        () -> {
                            throw new AssertionError("should not be executed");
                        }
                    ),
                    threadPool.generic(),
                    false,
                    future
                );
                assertThat("Listener is immediately completed", future.isDone(), is(true));
                assertThat("Region already exists in cache", future.get(), is(false));
            }
            {
                // cache fully used, but force=true, so the cache should evict regions to make space for the requested regions
                assertEquals(0, cacheService.freeRegionCount());
                final AtomicLong bytesRead = new AtomicLong(0L);
                final var cacheKey = generateCacheKey();
                final PlainActionFuture<Collection<Boolean>> future = new PlainActionFuture<>();
                final var regionsToFetch = randomIntBetween(1, (int) (cacheSize / regionSize));
                final var listener = new GroupedActionListener<>(regionsToFetch, future);
                final long blobLength = regionsToFetch * regionSize;
                final var bulkTaskCount = new AtomicInteger(0);
                final var executionFinishedLatch = new CountDownLatch(regionsToFetch);
                for (int region = 0; region < regionsToFetch; region++) {
                    cacheService.fetchRegion(
                        cacheKey,
                        region,
                        blobLength,
                        (channel, channelPos, streamFactory, relativePos, length, progressUpdater, completionListener) -> completeWith(
                            completionListener,
                            () -> {
                                assert streamFactory == null : streamFactory;
                                bytesRead.addAndGet(length);
                                progressUpdater.accept(length);
                            }
                        ),
                        bulkExecutor(threadPool, bulkTaskCount, executionFinishedLatch),
                        true,
                        listener
                    );
                }

                var results = future.get(10, TimeUnit.SECONDS);
                safeAwait(executionFinishedLatch);
                assertThat(results.stream().allMatch(result -> result), is(true));
                assertEquals(blobLength, bytesRead.get());
                assertEquals(0, cacheService.freeRegionCount());
                assertEquals(regionsToFetch, bulkTaskCount.get());
            }
            {
                final var bulkTaskCount = new AtomicInteger(0);
                cacheService.computeDecay();

                // We explicitly called computeDecay, meaning that some regions must have been demoted to level 0,
                // therefore there should be enough room to fetch the requested range regardless of the force flag.
                final var cacheKey = generateCacheKey();
                assertEquals(0, cacheService.freeRegionCount());
                final long blobLength = randomLongBetween(1L, regionSize);
                final AtomicLong bytesRead = new AtomicLong(0L);
                final PlainActionFuture<Boolean> future = new PlainActionFuture<>();
                final var executionFinishedLatch = new CountDownLatch(1);
                cacheService.fetchRegion(
                    cacheKey,
                    0,
                    blobLength,
                    (channel, channelPos, ignore, relativePos, length, progressUpdater, completionListener) -> completeWith(
                        completionListener,
                        () -> {
                            assert ignore == null : ignore;
                            bytesRead.addAndGet(length);
                            progressUpdater.accept(length);
                        }
                    ),
                    bulkExecutor(threadPool, bulkTaskCount, executionFinishedLatch),
                    randomBoolean(),
                    future
                );

                var fetched = future.get(10, TimeUnit.SECONDS);
                safeAwait(executionFinishedLatch);
                assertThat("Region has been fetched", fetched, is(true));
                assertEquals(blobLength, bytesRead.get());
                assertEquals(0, cacheService.freeRegionCount());
            }
        } finally {
            TestThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
        }
    }

    private static StoppableExecutorServiceWrapper bulkExecutor(
        final TestThreadPool threadPool,
        final AtomicInteger bulkTaskCount,
        final CountDownLatch executionFinishedLatch
    ) {
        return new StoppableExecutorServiceWrapper(threadPool.generic()) {
            @Override
            public void execute(Runnable command) {
                super.execute(() -> {
                    command.run();
                    executionFinishedLatch.countDown();
                });
                bulkTaskCount.incrementAndGet();
            }
        };
    }

    public void testMaybeFetchRange() throws Exception {
        final long cacheSize = size(500L);
        final long regionSize = size(100L);
        Settings settings = Settings.builder()
            .put(NODE_NAME_SETTING.getKey(), "node")
            .put(SharedBlobCacheService.SHARED_CACHE_SIZE_SETTING.getKey(), ByteSizeValue.ofBytes(cacheSize).getStringRep())
            .put(SharedBlobCacheService.SHARED_CACHE_REGION_SIZE_SETTING.getKey(), ByteSizeValue.ofBytes(regionSize).getStringRep())
            .put(SharedBlobCacheService.SHARED_CACHE_INITIAL_DECAYS_SETTING.getKey(), 0)
            .put("path.home", createTempDir())
            .build();

        final var bulkTaskCount = new AtomicInteger(0);
        final var threadPool = new TestThreadPool("test");
        final var bulkExecutor = new StoppableExecutorServiceWrapper(threadPool.generic()) {
            @Override
            public void execute(Runnable command) {
                super.execute(command);
                bulkTaskCount.incrementAndGet();
            }
        };

        try (
            NodeEnvironment environment = new NodeEnvironment(settings, TestEnvironment.newEnvironment(settings));
            var cacheService = new SharedBlobCacheService<>(
                environment,
                settings,
                threadPool,
                threadPool.executor(ThreadPool.Names.GENERIC),
                BlobCacheMetrics.NOOP
            )
        ) {
            {
                // fetch a random range in a random region of the blob
                final var cacheKey = generateCacheKey();
                assertEquals(5, cacheService.freeRegionCount());

                // blobLength is 1024000 bytes and requires 3 regions
                final long blobLength = size(250);
                final var regions = List.of(
                    // region 0: 0-409600
                    ByteRange.of(cacheService.getRegionStart(0), cacheService.getRegionEnd(0)),
                    // region 1: 409600-819200
                    ByteRange.of(cacheService.getRegionStart(1), cacheService.getRegionEnd(1)),
                    // region 2: 819200-1228800
                    ByteRange.of(cacheService.getRegionStart(2), cacheService.getRegionEnd(2))
                );

                long pos = randomLongBetween(0, blobLength - 1L);
                long len = randomLongBetween(1, blobLength - pos);
                var range = ByteRange.of(pos, pos + len);
                var region = between(0, regions.size() - 1);
                var regionRange = cacheService.mapSubRangeToRegion(range, region);

                var bytesCopied = new AtomicLong(0L);
                var future = new PlainActionFuture<Boolean>();
                cacheService.maybeFetchRange(
                    cacheKey,
                    region,
                    range,
                    blobLength,
                    (channel, channelPos, streamFactory, relativePos, length, progressUpdater, completionListener) -> completeWith(
                        completionListener,
                        () -> {
                            assertThat(range.start() + relativePos, equalTo(cacheService.getRegionStart(region) + regionRange.start()));
                            assertThat(channelPos, equalTo(Math.toIntExact(regionRange.start())));
                            assertThat(length, equalTo(Math.toIntExact(regionRange.length())));
                            bytesCopied.addAndGet(length);
                        }
                    ),
                    bulkExecutor,
                    future
                );
                var fetched = future.get(10, TimeUnit.SECONDS);

                assertThat(regionRange.length(), equalTo(bytesCopied.get()));
                if (regionRange.isEmpty()) {
                    assertThat(fetched, is(false));
                    assertEquals(5, cacheService.freeRegionCount());
                    assertEquals(0, bulkTaskCount.get());
                } else {
                    assertThat(fetched, is(true));
                    assertEquals(4, cacheService.freeRegionCount());
                    assertEquals(1, bulkTaskCount.get());
                }
            }
            {
                // fetch multiple ranges to use all the cache
                final int remainingFreeRegions = cacheService.freeRegionCount();
                assertThat(remainingFreeRegions, greaterThanOrEqualTo(4));
                bulkTaskCount.set(0);

                final var cacheKey = generateCacheKey();
                final long blobLength = regionSize * remainingFreeRegions;
                AtomicLong bytesCopied = new AtomicLong(0L);

                final PlainActionFuture<Collection<Boolean>> future = new PlainActionFuture<>();
                final var listener = new GroupedActionListener<>(remainingFreeRegions, future);
                for (int region = 0; region < remainingFreeRegions; region++) {
                    cacheService.maybeFetchRange(
                        cacheKey,
                        region,
                        ByteRange.of(0L, blobLength),
                        blobLength,
                        (channel, channelPos, streamFactory, relativePos, length, progressUpdater, completionListener) -> completeWith(
                            completionListener,
                            () -> bytesCopied.addAndGet(length)
                        ),
                        bulkExecutor,
                        listener
                    );
                }

                var results = future.get(10, TimeUnit.SECONDS);
                assertThat(results.stream().allMatch(result -> result), is(true));
                assertEquals(blobLength, bytesCopied.get());
                assertEquals(0, cacheService.freeRegionCount());
                assertEquals(remainingFreeRegions, bulkTaskCount.get());
            }
            {
                // cache fully used, no entry old enough to be evicted
                assertEquals(0, cacheService.freeRegionCount());
                final var cacheKey = generateCacheKey();
                final var blobLength = randomLongBetween(1L, regionSize);
                final PlainActionFuture<Boolean> future = new PlainActionFuture<>();
                cacheService.maybeFetchRange(
                    cacheKey,
                    0, // first region since blobLength fits in the size of a region
                    ByteRange.of(0L, blobLength),
                    blobLength,
                    (channel, channelPos, streamFactory, relativePos, length, progressUpdater, completionListener) -> completeWith(
                        completionListener,
                        () -> {
                            throw new AssertionError("should not be executed");
                        }
                    ),
                    bulkExecutor,
                    future
                );
                assertThat("Listener is immediately completed", future.isDone(), is(true));
                assertThat("Region already exists in cache", future.get(), is(false));
            }
            {
                cacheService.computeDecay();

                // fetch one more range should evict an old cache entry
                final var cacheKey = generateCacheKey();
                assertEquals(0, cacheService.freeRegionCount());
                long blobLength = randomLongBetween(1L, regionSize);
                AtomicLong bytesCopied = new AtomicLong(0L);
                final PlainActionFuture<Boolean> future = new PlainActionFuture<>();
                cacheService.maybeFetchRange(
                    cacheKey,
                    0,
                    ByteRange.of(0L, blobLength),
                    blobLength,
                    (channel, channelPos, streamFactory, relativePos, length, progressUpdater, completionListener) -> completeWith(
                        completionListener,
                        () -> bytesCopied.addAndGet(length)
                    ),
                    bulkExecutor,
                    future
                );

                var fetched = future.get(10, TimeUnit.SECONDS);
                assertThat("Region has been fetched", fetched, is(true));
                assertEquals(blobLength, bytesCopied.get());
                assertEquals(0, cacheService.freeRegionCount());
            }
        }
        threadPool.shutdown();
    }

    public void testFetchRange() throws Exception {
        final long cacheSize = size(500L);
        final long regionSize = size(100L);
        Settings settings = Settings.builder()
            .put(NODE_NAME_SETTING.getKey(), "node")
            .put(SharedBlobCacheService.SHARED_CACHE_SIZE_SETTING.getKey(), ByteSizeValue.ofBytes(cacheSize).getStringRep())
            .put(SharedBlobCacheService.SHARED_CACHE_REGION_SIZE_SETTING.getKey(), ByteSizeValue.ofBytes(regionSize).getStringRep())
            .put(SharedBlobCacheService.SHARED_CACHE_INITIAL_DECAYS_SETTING.getKey(), 0)
            .put("path.home", createTempDir())
            .build();

        final var bulkTaskCount = new AtomicInteger(0);
        final var threadPool = new TestThreadPool("test");
        final var bulkExecutor = new StoppableExecutorServiceWrapper(threadPool.generic()) {
            @Override
            public void execute(Runnable command) {
                super.execute(command);
                bulkTaskCount.incrementAndGet();
            }
        };

        try (
            NodeEnvironment environment = new NodeEnvironment(settings, TestEnvironment.newEnvironment(settings));
            var cacheService = new SharedBlobCacheService<>(
                environment,
                settings,
                threadPool,
                threadPool.executor(ThreadPool.Names.GENERIC),
                BlobCacheMetrics.NOOP
            )
        ) {
            {
                // fetch a random range in a random region of the blob
                final var cacheKey = generateCacheKey();
                assertEquals(5, cacheService.freeRegionCount());

                // blobLength is 1024000 bytes and requires 3 regions
                final long blobLength = size(250);
                final var regions = List.of(
                    // region 0: 0-409600
                    ByteRange.of(cacheService.getRegionStart(0), cacheService.getRegionEnd(0)),
                    // region 1: 409600-819200
                    ByteRange.of(cacheService.getRegionStart(1), cacheService.getRegionEnd(1)),
                    // region 2: 819200-1228800
                    ByteRange.of(cacheService.getRegionStart(2), cacheService.getRegionEnd(2))
                );

                long pos = randomLongBetween(0, blobLength - 1L);
                long len = randomLongBetween(1, blobLength - pos);
                var range = ByteRange.of(pos, pos + len);
                var region = between(0, regions.size() - 1);
                var regionRange = cacheService.mapSubRangeToRegion(range, region);

                var bytesCopied = new AtomicLong(0L);
                var future = new PlainActionFuture<Boolean>();
                cacheService.maybeFetchRange(
                    cacheKey,
                    region,
                    range,
                    blobLength,
                    (channel, channelPos, streamFactory, relativePos, length, progressUpdater, completionListener) -> completeWith(
                        completionListener,
                        () -> {
                            assertThat(range.start() + relativePos, equalTo(cacheService.getRegionStart(region) + regionRange.start()));
                            assertThat(channelPos, equalTo(Math.toIntExact(regionRange.start())));
                            assertThat(length, equalTo(Math.toIntExact(regionRange.length())));
                            bytesCopied.addAndGet(length);
                        }
                    ),
                    bulkExecutor,
                    future
                );
                var fetched = future.get(10, TimeUnit.SECONDS);

                assertThat(regionRange.length(), equalTo(bytesCopied.get()));
                if (regionRange.isEmpty()) {
                    assertThat(fetched, is(false));
                    assertEquals(5, cacheService.freeRegionCount());
                    assertEquals(0, bulkTaskCount.get());
                } else {
                    assertThat(fetched, is(true));
                    assertEquals(4, cacheService.freeRegionCount());
                    assertEquals(1, bulkTaskCount.get());
                }
            }
            {
                // fetch multiple ranges to use all the cache
                final int remainingFreeRegions = cacheService.freeRegionCount();
                assertThat(remainingFreeRegions, greaterThanOrEqualTo(4));
                bulkTaskCount.set(0);

                final var cacheKey = generateCacheKey();
                final long blobLength = regionSize * remainingFreeRegions;
                AtomicLong bytesCopied = new AtomicLong(0L);

                final PlainActionFuture<Collection<Boolean>> future = new PlainActionFuture<>();
                final var listener = new GroupedActionListener<>(remainingFreeRegions, future);
                for (int region = 0; region < remainingFreeRegions; region++) {
                    cacheService.fetchRange(
                        cacheKey,
                        region,
                        ByteRange.of(0L, blobLength),
                        blobLength,
                        (channel, channelPos, streamFactory, relativePos, length, progressUpdater, completionListener) -> completeWith(
                            completionListener,
                            () -> bytesCopied.addAndGet(length)
                        ),
                        bulkExecutor,
                        true,
                        listener
                    );
                }

                var results = future.get(10, TimeUnit.SECONDS);
                assertThat(results.stream().allMatch(result -> result), is(true));
                assertEquals(blobLength, bytesCopied.get());
                assertEquals(0, cacheService.freeRegionCount());
                assertEquals(remainingFreeRegions, bulkTaskCount.get());
            }
            {
                // cache fully used, no entry old enough to be evicted and force=false
                assertEquals(0, cacheService.freeRegionCount());
                final var cacheKey = generateCacheKey();
                final var blobLength = randomLongBetween(1L, regionSize);
                final PlainActionFuture<Boolean> future = new PlainActionFuture<>();
                cacheService.fetchRange(
                    cacheKey,
                    0, // first region since blobLength fits in the size of a region
                    ByteRange.of(0L, blobLength),
                    blobLength,
                    (channel, channelPos, streamFactory, relativePos, length, progressUpdater, completionListener) -> completeWith(
                        completionListener,
                        () -> {
                            throw new AssertionError("should not be executed");
                        }
                    ),
                    bulkExecutor,
                    false,
                    future
                );
                assertThat("Listener is immediately completed", future.isDone(), is(true));
                assertThat("Region already exists in cache", future.get(), is(false));
            }
            {
                // cache fully used, since force=true the range should be populated
                final var cacheKey = generateCacheKey();
                assertEquals(0, cacheService.freeRegionCount());
                long blobLength = randomLongBetween(1L, regionSize);
                AtomicLong bytesCopied = new AtomicLong(0L);
                final PlainActionFuture<Boolean> future = new PlainActionFuture<>();
                cacheService.fetchRange(
                    cacheKey,
                    0,
                    ByteRange.of(0L, blobLength),
                    blobLength,
                    (channel, channelPos, streamFactory, relativePos, length, progressUpdater, completionListener) -> completeWith(
                        completionListener,
                        () -> bytesCopied.addAndGet(length)
                    ),
                    bulkExecutor,
                    true,
                    future
                );

                var fetched = future.get(10, TimeUnit.SECONDS);
                assertThat("Region has been fetched", fetched, is(true));
                assertEquals(blobLength, bytesCopied.get());
                assertEquals(0, cacheService.freeRegionCount());
            }
            {
                cacheService.computeDecay();

                // We explicitly called computeDecay, meaning that some regions must have been demoted to level 0,
                // therefore there should be enough room to fetch the requested range regardless of the force flag.
                final var cacheKey = generateCacheKey();
                assertEquals(0, cacheService.freeRegionCount());
                long blobLength = randomLongBetween(1L, regionSize);
                AtomicLong bytesCopied = new AtomicLong(0L);
                final PlainActionFuture<Boolean> future = new PlainActionFuture<>();
                cacheService.fetchRange(
                    cacheKey,
                    0,
                    ByteRange.of(0L, blobLength),
                    blobLength,
                    (channel, channelPos, streamFactory, relativePos, length, progressUpdater, completionListener) -> completeWith(
                        completionListener,
                        () -> bytesCopied.addAndGet(length)
                    ),
                    bulkExecutor,
                    randomBoolean(),
                    future
                );

                var fetched = future.get(10, TimeUnit.SECONDS);
                assertThat("Region has been fetched", fetched, is(true));
                assertEquals(blobLength, bytesCopied.get());
                assertEquals(0, cacheService.freeRegionCount());
            }
        } finally {
            TestThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
        }
    }

    public void testPopulate() throws Exception {
        final long regionSize = size(1L);
        Settings settings = Settings.builder()
            .put(NODE_NAME_SETTING.getKey(), "node")
            .put(SharedBlobCacheService.SHARED_CACHE_SIZE_SETTING.getKey(), ByteSizeValue.ofBytes(size(100)).getStringRep())
            .put(SharedBlobCacheService.SHARED_CACHE_REGION_SIZE_SETTING.getKey(), ByteSizeValue.ofBytes(regionSize).getStringRep())
            .put(SharedBlobCacheService.SHARED_CACHE_INITIAL_DECAYS_SETTING.getKey(), 0)
            .put("path.home", createTempDir())
            .build();

        final DeterministicTaskQueue taskQueue = new DeterministicTaskQueue();
        try (
            NodeEnvironment environment = new NodeEnvironment(settings, TestEnvironment.newEnvironment(settings));
            var cacheService = new SharedBlobCacheService<>(
                environment,
                settings,
                taskQueue.getThreadPool(),
                taskQueue.getThreadPool().executor(ThreadPool.Names.GENERIC),
                BlobCacheMetrics.NOOP
            )
        ) {
            final var cacheKey = generateCacheKey();
            final var blobLength = size(12L);

            // start populating the first region
            var entry = cacheService.get(cacheKey, blobLength, 0);
            AtomicLong bytesWritten = new AtomicLong(0L);
            final PlainActionFuture<Boolean> future1 = new PlainActionFuture<>();
            entry.populate(
                ByteRange.of(0, regionSize - 1),
                (channel, channelPos, streamFactory, relativePos, length, progressUpdater, completionListener) -> completeWith(
                    completionListener,
                    () -> {
                        bytesWritten.addAndGet(length);
                        progressUpdater.accept(length);
                    }
                ),
                taskQueue.getThreadPool().generic(),
                future1
            );

            assertThat(future1.isDone(), is(false));
            assertThat(taskQueue.hasRunnableTasks(), is(true));
            assertTrue(entry.tracker.waitForRangeIfPending(ByteRange.of(0, regionSize - 1), ActionListener.noop()));

            // start populating the second region
            entry = cacheService.get(cacheKey, blobLength, 1);
            final PlainActionFuture<Boolean> future2 = new PlainActionFuture<>();
            entry.populate(
                ByteRange.of(0, regionSize - 1),
                (channel, channelPos, streamFactory, relativePos, length, progressUpdater, completionListener) -> completeWith(
                    completionListener,
                    () -> {
                        bytesWritten.addAndGet(length);
                        progressUpdater.accept(length);
                    }
                ),
                taskQueue.getThreadPool().generic(),
                future2
            );

            assertTrue(entry.tracker.waitForRangeIfPending(ByteRange.of(0, regionSize - 1), ActionListener.noop()));

            // start populating again the first region; async notified
            entry = cacheService.get(cacheKey, blobLength, 0);
            final PlainActionFuture<Boolean> future3 = new PlainActionFuture<>();
            entry.populate(
                ByteRange.of(0, regionSize - 1),
                (channel, channelPos, streamFactory, relativePos, length, progressUpdater, completionListener) -> completeWith(
                    completionListener,
                    () -> {
                        bytesWritten.addAndGet(length);
                        progressUpdater.accept(length);
                    }
                ),
                taskQueue.getThreadPool().generic(),
                future3
            );

            assertThat(future3.isDone(), is(false));
            taskQueue.runAllRunnableTasks();
            assertThat(future1.isDone(), is(true));
            assertThat(future3.isDone(), is(true));

            var written1 = future1.get(10L, TimeUnit.SECONDS);
            var written3 = future3.get(10L, TimeUnit.SECONDS);
            // one and only one wrote it
            assertThat(written1 ^ written3, is(true));

            var written = future2.get(10L, TimeUnit.SECONDS);
            assertThat(future2.isDone(), is(true));
            assertThat(written, is(true));
        }
    }

    /**
     * Two populate calls for the same range before the executor runs both queue a task. waitForRange registers
     * listeners immediately (before any executor task runs), so both callers see unclaimed gaps and queue work.
     * claim() is called inside each executor task, and only the first caller to claim gets the gaps to fill.
     */
    public void testPopulateConcurrentSameRange() throws Exception {
        final long regionSize = size(1L);
        Settings settings = Settings.builder()
            .put(NODE_NAME_SETTING.getKey(), "node")
            .put(SharedBlobCacheService.SHARED_CACHE_SIZE_SETTING.getKey(), ByteSizeValue.ofBytes(size(100)).getStringRep())
            .put(SharedBlobCacheService.SHARED_CACHE_REGION_SIZE_SETTING.getKey(), ByteSizeValue.ofBytes(regionSize).getStringRep())
            .put(SharedBlobCacheService.SHARED_CACHE_INITIAL_DECAYS_SETTING.getKey(), 0)
            .put("path.home", createTempDir())
            .build();

        final DeterministicTaskQueue taskQueue = new DeterministicTaskQueue();
        try (
            NodeEnvironment environment = new NodeEnvironment(settings, TestEnvironment.newEnvironment(settings));
            var cacheService = new SharedBlobCacheService<>(
                environment,
                settings,
                taskQueue.getThreadPool(),
                taskQueue.getThreadPool().executor(ThreadPool.Names.GENERIC),
                BlobCacheMetrics.NOOP
            )
        ) {
            final var cacheKey = generateCacheKey();
            final var blobLength = size(12L);
            final var entry = cacheService.get(cacheKey, blobLength, 0);
            final AtomicLong bytesWritten = new AtomicLong(0L);
            final RangeMissingHandler writer = (
                channel,
                channelPos,
                streamFactory,
                relativePos,
                length,
                progressUpdater,
                completionListener) -> completeWith(completionListener, () -> {
                    bytesWritten.addAndGet(length);
                    progressUpdater.accept(length);
                });

            // Two populate calls for the same range before the executor runs
            final PlainActionFuture<Boolean> future1 = new PlainActionFuture<>();
            entry.populate(ByteRange.of(0, regionSize - 1), writer, taskQueue.getThreadPool().generic(), future1);
            final PlainActionFuture<Boolean> future2 = new PlainActionFuture<>();
            entry.populate(ByteRange.of(0, regionSize - 1), writer, taskQueue.getThreadPool().generic(), future2);

            // Both calls registered listeners immediately; neither future is done yet
            assertThat(future1.isDone(), is(false));
            assertThat(future2.isDone(), is(false));

            taskQueue.runAllRunnableTasks();

            // Exactly one caller claimed the gaps and wrote the data; the other got an empty claim
            assertThat(future1.get(10L, TimeUnit.SECONDS) || future2.get(10L, TimeUnit.SECONDS), is(true));
            assertThat(future1.get(10L, TimeUnit.SECONDS) && future2.get(10L, TimeUnit.SECONDS), is(false));
            assertThat(bytesWritten.get(), equalTo(regionSize - 1));
        }
    }

    private void assertThatNonPositiveRecoveryRangeSizeRejected(Setting<ByteSizeValue> setting) {
        final String value = randomFrom(ByteSizeValue.MINUS_ONE, ByteSizeValue.ZERO).getStringRep();
        final Settings settings = Settings.builder()
            .put(SharedBlobCacheService.SHARED_CACHE_SIZE_SETTING.getKey(), ByteSizeValue.ofBytes(size(100)).getStringRep())
            .putList(NodeRoleSettings.NODE_ROLES_SETTING.getKey(), DiscoveryNodeRole.DATA_FROZEN_NODE_ROLE.roleName())
            .put(setting.getKey(), value)
            .build();
        final IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> setting.get(settings));
        assertThat(e.getCause(), notNullValue());
        assertThat(e.getCause(), instanceOf(SettingsException.class));
        assertThat(e.getCause().getMessage(), is("setting [" + setting.getKey() + "] must be greater than zero"));
    }

    public void testNonPositiveRegionSizeRejected() {
        assertThatNonPositiveRecoveryRangeSizeRejected(SharedBlobCacheService.SHARED_CACHE_REGION_SIZE_SETTING);
    }

    public void testNonPositiveRangeSizeRejected() {
        assertThatNonPositiveRecoveryRangeSizeRejected(SharedBlobCacheService.SHARED_CACHE_RANGE_SIZE_SETTING);
    }

    public void testNonPositiveRecoveryRangeSizeRejected() {
        assertThatNonPositiveRecoveryRangeSizeRejected(SharedBlobCacheService.SHARED_CACHE_RECOVERY_RANGE_SIZE_SETTING);
    }

    public void testUseFullRegionSize() throws IOException {
        final long regionSize = size(randomIntBetween(1, 100));
        final long cacheSize = regionSize * randomIntBetween(1, 10);

        Settings settings = Settings.builder()
            .put(NODE_NAME_SETTING.getKey(), "node")
            .put(SharedBlobCacheService.SHARED_CACHE_REGION_SIZE_SETTING.getKey(), ByteSizeValue.ofBytes(regionSize).getStringRep())
            .put(SharedBlobCacheService.SHARED_CACHE_SIZE_SETTING.getKey(), ByteSizeValue.ofBytes(cacheSize).getStringRep())
            .put(SharedBlobCacheService.SHARED_CACHE_INITIAL_DECAYS_SETTING.getKey(), 0)
            .put("path.home", createTempDir())
            .build();
        final DeterministicTaskQueue taskQueue = new DeterministicTaskQueue();
        try (
            NodeEnvironment environment = new NodeEnvironment(settings, TestEnvironment.newEnvironment(settings));
            var cacheService = new SharedBlobCacheService<>(
                environment,
                settings,
                taskQueue.getThreadPool(),
                taskQueue.getThreadPool().executor(ThreadPool.Names.GENERIC),
                BlobCacheMetrics.NOOP,
                new DefaultEvictionPolicy<>()
            ) {
                @Override
                protected int computeCacheFileRegionSize(long fileLength, int region) {
                    // use full region
                    return super.getRegionSize();
                }
            }
        ) {
            final var cacheKey = generateCacheKey();
            final var blobLength = randomLongBetween(1L, cacheSize);

            int regions = Math.toIntExact(blobLength / regionSize);
            regions += (blobLength % regionSize == 0 ? 0 : 1);
            assertThat(
                cacheService.computeCacheFileRegionSize(blobLength, randomFrom(regions)),
                equalTo(BlobCacheUtils.toIntBytes(regionSize))
            );
            for (int region = 0; region < regions; region++) {
                var cacheFileRegion = cacheService.get(cacheKey, blobLength, region);
                assertThat(cacheFileRegion.tracker.getLength(), equalTo(regionSize));
            }
        }
    }

    public void testUsageSharedSourceInputStreamFactoryInCachePopulation() throws Exception {
        final long regionSizeInBytes = size(100);
        final Settings settings = Settings.builder()
            .put(NODE_NAME_SETTING.getKey(), "node")
            .put(SharedBlobCacheService.SHARED_CACHE_SIZE_SETTING.getKey(), ByteSizeValue.ofBytes(size(200)).getStringRep())
            .put(SharedBlobCacheService.SHARED_CACHE_REGION_SIZE_SETTING.getKey(), ByteSizeValue.ofBytes(regionSizeInBytes).getStringRep())
            .put(SharedBlobCacheService.SHARED_CACHE_INITIAL_DECAYS_SETTING.getKey(), 0)
            .put("path.home", createTempDir())
            .build();
        final ThreadPool threadPool = new TestThreadPool("test");
        try (
            NodeEnvironment environment = new NodeEnvironment(settings, TestEnvironment.newEnvironment(settings));
            var cacheService = new SharedBlobCacheService<>(
                environment,
                settings,
                threadPool,
                threadPool.executor(ThreadPool.Names.GENERIC),
                BlobCacheMetrics.NOOP
            )
        ) {
            final var cacheKey = generateCacheKey();
            assertEquals(2, cacheService.freeRegionCount());
            final var region = cacheService.get(cacheKey, size(250), 0);
            assertEquals(regionSizeInBytes, region.tracker.getLength());

            // Read disjoint ranges to create holes in the region
            final long interval = regionSizeInBytes / between(5, 20);
            for (var start = interval; start < regionSizeInBytes - 2 * SharedBytes.PAGE_SIZE; start += interval) {
                final var range = ByteRange.of(start, start + SharedBytes.PAGE_SIZE);
                final PlainActionFuture<Integer> future = new PlainActionFuture<>();
                region.populateAndRead(
                    range,
                    range,
                    (channel, channelPos, relativePos, length) -> length,
                    (channel, channelPos, streamFactory, relativePos, length, progressUpdater, completionListener) -> completeWith(
                        completionListener,
                        () -> progressUpdater.accept(length)
                    ),
                    EsExecutors.DIRECT_EXECUTOR_SERVICE,
                    future
                );
                safeGet(future);
            }

            // Read the entire region with a shared source input stream and we want to ensure the following behaviours
            // 1. fillCacheRange is invoked as many times as the number of holes/gaps
            // 2. fillCacheRange is invoked single threaded with the gap order
            // 3. The shared streamFactory is passed to each invocation
            // 4. The factory is closed at the end
            final int numberGaps = region.tracker.getCompletedRanges().size() + 1;
            final var invocationCounter = new AtomicInteger();
            final var factoryClosed = new AtomicBoolean(false);
            final var dummyStreamFactory = new SourceInputStreamFactory() {
                @Override
                public void create(int relativePos, ActionListener<InputStream> listener) {
                    listener.onResponse(null);
                }

                @Override
                public void close() {
                    factoryClosed.set(true);
                }
            };

            final var rangeMissingHandler = new RangeMissingHandler() {
                final AtomicReference<Thread> invocationThread = new AtomicReference<>();
                final AtomicInteger position = new AtomicInteger(-1);

                @Override
                public SourceInputStreamFactory sharedInputStreamFactory(List<SparseFileTracker.Gap> gaps) {
                    return dummyStreamFactory;
                }

                @Override
                public void fillCacheRange(
                    SharedBytes.IO channel,
                    int channelPos,
                    SourceInputStreamFactory streamFactory,
                    int relativePos,
                    int length,
                    IntConsumer progressUpdater,
                    ActionListener<Void> completion
                ) throws IOException {
                    completeWith(completion, () -> {
                        if (invocationCounter.incrementAndGet() == 1) {
                            final Thread witness = invocationThread.compareAndExchange(null, Thread.currentThread());
                            assertThat(witness, nullValue());
                        } else {
                            assertThat(invocationThread.get(), sameInstance(Thread.currentThread()));
                        }
                        assertThat(streamFactory, sameInstance(dummyStreamFactory));
                        assertThat(position.getAndSet(relativePos), lessThan(relativePos));
                        progressUpdater.accept(length);
                    });
                }
            };

            final var range = ByteRange.of(0, regionSizeInBytes);
            if (randomBoolean()) {
                final PlainActionFuture<Integer> future = new PlainActionFuture<>();
                region.populateAndRead(
                    range,
                    range,
                    (channel, channelPos, relativePos, length) -> length,
                    rangeMissingHandler,
                    threadPool.generic(),
                    future
                );
                assertThat(safeGet(future).longValue(), equalTo(regionSizeInBytes));
            } else {
                final PlainActionFuture<Boolean> future = new PlainActionFuture<>();
                region.populate(range, rangeMissingHandler, threadPool.generic(), future);
                assertThat(safeGet(future), equalTo(true));
            }
            assertThat(invocationCounter.get(), equalTo(numberGaps));
            assertThat(region.tracker.checkAvailable(regionSizeInBytes), is(true));
            assertBusy(() -> assertThat(factoryClosed.get(), is(true)));
        } finally {
            threadPool.shutdown();
        }
    }

    // Verifies that withMemorySegmentSlice returns false before data is populated, and provides
    // a readable memory segment with correct content after population. Single region of size(10), file size(8).
    public void testWithByteBufferSlice() throws Exception {
        final int regionSize = (int) size(10);
        final long fileLength = size(8); // fits in a single region
        Settings settings = Settings.builder()
            .put(NODE_NAME_SETTING.getKey(), "node")
            .put(SharedBlobCacheService.SHARED_CACHE_SIZE_SETTING.getKey(), ByteSizeValue.ofBytes(size(50)).getStringRep())
            .put(SharedBlobCacheService.SHARED_CACHE_REGION_SIZE_SETTING.getKey(), ByteSizeValue.ofBytes(regionSize).getStringRep())
            .put(SharedBlobCacheService.SHARED_CACHE_MMAP.getKey(), true)
            .put("path.home", createTempDir())
            .build();
        final DeterministicTaskQueue taskQueue = new DeterministicTaskQueue();
        ExecutorService ioExecutor = Executors.newCachedThreadPool();
        try (
            NodeEnvironment environment = new NodeEnvironment(settings, TestEnvironment.newEnvironment(settings));
            var cacheService = new SharedBlobCacheService<TestCacheKey>(
                environment,
                settings,
                taskQueue.getThreadPool(),
                ioExecutor,
                BlobCacheMetrics.NOOP
            )
        ) {
            final var cacheKey = generateCacheKey();
            SharedBlobCacheService<TestCacheKey>.CacheFile cacheFile = cacheService.getCacheFile(
                cacheKey,
                fileLength,
                SharedBlobCacheService.CacheMissHandler.NOOP
            );

            // before populating, withMemorySegmentSlice should return false (data not available)
            assertFalse(cacheFile.withMemorySegmentSlice(0, 100, slice -> fail("should not be invoked")));

            // populate the cache with known data
            byte[] testData = randomByteArrayOfLength((int) fileLength);
            ByteBuffer writeBuffer = ByteBuffer.allocate(SharedBytes.PAGE_SIZE);
            final int bytesRead = cacheFile.populateAndRead(
                ByteRange.of(0L, fileLength),
                ByteRange.of(0L, fileLength),
                (channel, pos, relativePos, len) -> len,
                (channel, channelPos, streamFactory, relativePos, len, progressUpdater, completionListener) -> {
                    SharedBytes.copyToCacheFileAligned(
                        channel,
                        new java.io.ByteArrayInputStream(testData, relativePos, len),
                        channelPos,
                        relativePos,
                        len,
                        progressUpdater,
                        writeBuffer.clear()
                    );
                    ActionListener.completeWith(completionListener, () -> null);
                },
                "test"
            );
            assertThat(bytesRead, equalTo((int) fileLength));

            // now withMemorySegmentSlice should provide a valid slice
            int sliceOffset = randomIntBetween(0, (int) fileLength / 2);
            int sliceLength = randomIntBetween(1, (int) fileLength - sliceOffset);
            boolean available = cacheFile.withMemorySegmentSlice(sliceOffset, sliceLength, slice -> {
                assertTrue(slice.isReadOnly());
                assertEquals(sliceLength, (int) slice.byteSize());
                byte[] sliceData = new byte[sliceLength];
                MemorySegment.copy(slice, ValueLayout.JAVA_BYTE, 0, sliceData, 0, sliceLength);
                for (int i = 0; i < sliceLength; i++) {
                    assertEquals(testData[sliceOffset + i], sliceData[i]);
                }
            });
            assertTrue(available);
        }
        ioExecutor.shutdown();
    }

    // Verifies that the memory segment ref held during the callback prevents the region from being
    // evicted. 2 regions of size(10), file size(8); eviction pressure is applied inside the callback.
    public void testWithByteBufferSlicePreventsEviction() throws Exception {
        final int regionSize = (int) size(10);
        Settings settings = Settings.builder()
            .put(NODE_NAME_SETTING.getKey(), "node")
            .put(SharedBlobCacheService.SHARED_CACHE_SIZE_SETTING.getKey(), ByteSizeValue.ofBytes(size(20)).getStringRep())
            .put(SharedBlobCacheService.SHARED_CACHE_REGION_SIZE_SETTING.getKey(), ByteSizeValue.ofBytes(regionSize).getStringRep())
            .put(SharedBlobCacheService.SHARED_CACHE_MMAP.getKey(), true)
            .put("path.home", createTempDir())
            .build();
        final DeterministicTaskQueue taskQueue = new DeterministicTaskQueue();
        ExecutorService ioExecutor = Executors.newCachedThreadPool();
        try (
            NodeEnvironment environment = new NodeEnvironment(settings, TestEnvironment.newEnvironment(settings));
            var cacheService = new SharedBlobCacheService<TestCacheKey>(
                environment,
                settings,
                taskQueue.getThreadPool(),
                ioExecutor,
                BlobCacheMetrics.NOOP
            )
        ) {
            assertEquals(2, cacheService.freeRegionCount());

            // populate region 0 with known data for cacheKey1
            final long fileLength = size(8); // fits in one region
            final var cacheKey1 = generateCacheKey();
            SharedBlobCacheService<TestCacheKey>.CacheFile cacheFile1 = cacheService.getCacheFile(
                cacheKey1,
                fileLength,
                SharedBlobCacheService.CacheMissHandler.NOOP
            );
            byte[] testData = randomByteArrayOfLength((int) fileLength);
            ByteBuffer writeBuffer = ByteBuffer.allocate(SharedBytes.PAGE_SIZE);
            cacheFile1.populateAndRead(
                ByteRange.of(0L, fileLength),
                ByteRange.of(0L, fileLength),
                (channel, pos, relativePos, len) -> len,
                (channel, channelPos, streamFactory, relativePos, len, progressUpdater, completionListener) -> {
                    SharedBytes.copyToCacheFileAligned(
                        channel,
                        new java.io.ByteArrayInputStream(testData, relativePos, len),
                        channelPos,
                        relativePos,
                        len,
                        progressUpdater,
                        writeBuffer.clear()
                    );
                    ActionListener.completeWith(completionListener, () -> null);
                },
                "test"
            );

            // inside the callback, the ref is held — eviction should not reclaim the region
            boolean available = cacheFile1.withMemorySegmentSlice(0, (int) fileLength, slice -> {
                // fill the remaining region with a different key, using up all free regions
                final var cacheKey2 = generateCacheKey();
                cacheService.get(cacheKey2, fileLength, 0);

                // now all regions are used; requesting yet another key triggers eviction pressure
                final var cacheKey3 = generateCacheKey();
                cacheService.get(cacheKey3, fileLength, 0);
                taskQueue.runAllRunnableTasks();

                // the memory segment should still contain the original data (region not evicted while ref held)
                byte[] readBack = new byte[(int) fileLength];
                MemorySegment.copy(slice, ValueLayout.JAVA_BYTE, 0, readBack, 0, (int) fileLength);
                assertArrayEquals(testData, readBack);
            });
            assertTrue(available);

        }
        ioExecutor.shutdown();
    }

    // Verifies that withMemorySegmentSlice returns false and the callback is not invoked after a
    // region has been evicted. 2 regions of size(10), file size(8); eviction forced by cache pressure.
    public void testWithByteBufferSliceReturnsFalseAfterEviction() throws Exception {
        final int regionSize = (int) size(10);
        Settings settings = Settings.builder()
            .put(NODE_NAME_SETTING.getKey(), "node")
            .put(SharedBlobCacheService.SHARED_CACHE_SIZE_SETTING.getKey(), ByteSizeValue.ofBytes(size(20)).getStringRep())
            .put(SharedBlobCacheService.SHARED_CACHE_REGION_SIZE_SETTING.getKey(), ByteSizeValue.ofBytes(regionSize).getStringRep())
            .put(SharedBlobCacheService.SHARED_CACHE_MMAP.getKey(), true)
            .put("path.home", createTempDir())
            .build();
        final DeterministicTaskQueue taskQueue = new DeterministicTaskQueue();
        try (
            NodeEnvironment environment = new NodeEnvironment(settings, TestEnvironment.newEnvironment(settings));
            var cacheService = new SharedBlobCacheService<TestCacheKey>(
                environment,
                settings,
                taskQueue.getThreadPool(),
                EsExecutors.DIRECT_EXECUTOR_SERVICE,
                BlobCacheMetrics.NOOP
            )
        ) {
            assertEquals(2, cacheService.freeRegionCount());

            final long fileLength = size(8); // fits in one region
            final var cacheKey1 = generateCacheKey();
            SharedBlobCacheService<TestCacheKey>.CacheFile cacheFile1 = cacheService.getCacheFile(
                cacheKey1,
                fileLength,
                SharedBlobCacheService.CacheMissHandler.NOOP
            );

            // populate the region
            byte[] testData = randomByteArrayOfLength((int) fileLength);
            ByteBuffer writeBuffer = ByteBuffer.allocate(SharedBytes.PAGE_SIZE);
            cacheFile1.populateAndRead(
                ByteRange.of(0L, fileLength),
                ByteRange.of(0L, fileLength),
                (channel, pos, relativePos, len) -> len,
                (channel, channelPos, streamFactory, relativePos, len, progressUpdater, completionListener) -> {
                    SharedBytes.copyToCacheFileAligned(
                        channel,
                        new java.io.ByteArrayInputStream(testData, relativePos, len),
                        channelPos,
                        relativePos,
                        len,
                        progressUpdater,
                        writeBuffer.clear()
                    );
                    ActionListener.completeWith(completionListener, () -> null);
                },
                "test"
            );

            // confirm the slice is accessible before eviction
            assertTrue(cacheFile1.withMemorySegmentSlice(0, (int) fileLength, slice -> {}));

            // fill the second region, then request a third key to force eviction of cacheKey1's region
            cacheService.get(generateCacheKey(), fileLength, 0);
            cacheService.get(generateCacheKey(), fileLength, 0);
            taskQueue.runAllRunnableTasks();

            // after eviction the action must not be invoked and the method must return false
            boolean available = cacheFile1.withMemorySegmentSlice(
                0,
                (int) fileLength,
                slice -> { fail("action should not be invoked after eviction"); }
            );
            assertFalse(available);
        }
    }

    // Verifies that withMemorySegmentSlice returns false when the requested range spans multiple
    // regions. Regions of size(10), file size(25) spanning 3 regions; slice straddles the boundary.
    public void testWithByteBufferSliceCrossRegionReturnsFalse() throws Exception {
        final int regionSize = (int) size(10);
        final long fileLength = size(25); // spans 3 regions
        Settings settings = Settings.builder()
            .put(NODE_NAME_SETTING.getKey(), "node")
            .put(SharedBlobCacheService.SHARED_CACHE_SIZE_SETTING.getKey(), ByteSizeValue.ofBytes(size(50)).getStringRep())
            .put(SharedBlobCacheService.SHARED_CACHE_REGION_SIZE_SETTING.getKey(), ByteSizeValue.ofBytes(regionSize).getStringRep())
            .put(SharedBlobCacheService.SHARED_CACHE_MMAP.getKey(), true)
            .put("path.home", createTempDir())
            .build();
        final DeterministicTaskQueue taskQueue = new DeterministicTaskQueue();
        try (
            NodeEnvironment environment = new NodeEnvironment(settings, TestEnvironment.newEnvironment(settings));
            var cacheService = new SharedBlobCacheService<TestCacheKey>(
                environment,
                settings,
                taskQueue.getThreadPool(),
                taskQueue.getThreadPool().executor(ThreadPool.Names.GENERIC),
                BlobCacheMetrics.NOOP
            )
        ) {
            final var cacheKey = generateCacheKey();
            SharedBlobCacheService<TestCacheKey>.CacheFile cacheFile = cacheService.getCacheFile(
                cacheKey,
                fileLength,
                SharedBlobCacheService.CacheMissHandler.NOOP
            );

            // request a slice that spans the region boundary (region 0 -> region 1)
            // region 0 covers [0, regionSize), region 1 covers [regionSize, 2*regionSize)
            int crossBoundaryOffset = regionSize - 100;
            int crossBoundaryLength = 200; // crosses into region 1
            boolean available = cacheFile.withMemorySegmentSlice(crossBoundaryOffset, crossBoundaryLength, slice -> {
                fail("action should not be invoked for cross-region slice");
            });
            assertFalse(available);
        }
    }

    // Verifies that withMemorySegmentSlice returns false when mmap is disabled, even after the
    // region has been fully populated. Single region of size(10), file size(8), mmap=false.
    public void testWithByteBufferSliceNoMmapReturnsFalse() throws Exception {
        final int regionSize = (int) size(10);
        final long fileLength = size(8);
        Settings settings = Settings.builder()
            .put(NODE_NAME_SETTING.getKey(), "node")
            .put(SharedBlobCacheService.SHARED_CACHE_SIZE_SETTING.getKey(), ByteSizeValue.ofBytes(size(50)).getStringRep())
            .put(SharedBlobCacheService.SHARED_CACHE_REGION_SIZE_SETTING.getKey(), ByteSizeValue.ofBytes(regionSize).getStringRep())
            .put(SharedBlobCacheService.SHARED_CACHE_MMAP.getKey(), false)
            .put("path.home", createTempDir())
            .build();
        final DeterministicTaskQueue taskQueue = new DeterministicTaskQueue();
        ExecutorService ioExecutor = Executors.newCachedThreadPool();
        try (
            NodeEnvironment environment = new NodeEnvironment(settings, TestEnvironment.newEnvironment(settings));
            var cacheService = new SharedBlobCacheService<TestCacheKey>(
                environment,
                settings,
                taskQueue.getThreadPool(),
                ioExecutor,
                BlobCacheMetrics.NOOP
            )
        ) {
            final var cacheKey = generateCacheKey();
            SharedBlobCacheService<TestCacheKey>.CacheFile cacheFile = cacheService.getCacheFile(
                cacheKey,
                fileLength,
                SharedBlobCacheService.CacheMissHandler.NOOP
            );

            // populate the cache
            byte[] testData = randomByteArrayOfLength((int) fileLength);
            ByteBuffer writeBuffer = ByteBuffer.allocate(SharedBytes.PAGE_SIZE);
            cacheFile.populateAndRead(
                ByteRange.of(0L, fileLength),
                ByteRange.of(0L, fileLength),
                (channel, pos, relativePos, len) -> len,
                (channel, channelPos, streamFactory, relativePos, len, progressUpdater, completionListener) -> {
                    SharedBytes.copyToCacheFileAligned(
                        channel,
                        new java.io.ByteArrayInputStream(testData, relativePos, len),
                        channelPos,
                        relativePos,
                        len,
                        progressUpdater,
                        writeBuffer.clear()
                    );
                    ActionListener.completeWith(completionListener, () -> null);
                },
                "test"
            );

            // without mmap, withMemorySegmentSlice should return false even with data populated
            boolean available = cacheFile.withMemorySegmentSlice(
                0,
                100,
                slice -> { fail("action should not be invoked when mmap is not enabled"); }
            );
            assertFalse(available);
        }
        ioExecutor.shutdown();
    }

    // Verifies that withMemorySegmentSlices resolves multiple ranges within a single region
    // and across regions, returning the correct data for each slice.
    public void testWithByteBufferSlices() throws Exception {
        final int regionSize = (int) size(10);
        final long fileLength = size(25); // spans 3 regions
        Settings settings = Settings.builder()
            .put(NODE_NAME_SETTING.getKey(), "node")
            .put(SharedBlobCacheService.SHARED_CACHE_SIZE_SETTING.getKey(), ByteSizeValue.ofBytes(size(200)).getStringRep())
            .put(SharedBlobCacheService.SHARED_CACHE_REGION_SIZE_SETTING.getKey(), ByteSizeValue.ofBytes(regionSize).getStringRep())
            .put(SharedBlobCacheService.SHARED_CACHE_MMAP.getKey(), true)
            .put("path.home", createTempDir())
            .build();
        final DeterministicTaskQueue taskQueue = new DeterministicTaskQueue();
        ExecutorService ioExecutor = EsExecutors.DIRECT_EXECUTOR_SERVICE;
        try (
            NodeEnvironment environment = new NodeEnvironment(settings, TestEnvironment.newEnvironment(settings));
            var cacheService = new SharedBlobCacheService<TestCacheKey>(
                environment,
                settings,
                taskQueue.getThreadPool(),
                ioExecutor,
                BlobCacheMetrics.NOOP
            )
        ) {
            final var cacheKey = generateCacheKey();
            SharedBlobCacheService<TestCacheKey>.CacheFile cacheFile = cacheService.getCacheFile(
                cacheKey,
                fileLength,
                SharedBlobCacheService.CacheMissHandler.NOOP
            );

            // before populating, withSliceAddresses should return false
            long[] offsets = { 0, (long) regionSize + 10, (long) regionSize * 2 + 5 };
            int sliceLen = 50;
            MemorySegment addrsOut = MemorySegment.ofArray(new long[3]);
            assertFalse(cacheFile.withSliceAddresses(offsets, sliceLen, 3, addrsOut, addrs -> fail("should not be invoked")));

            // populate all regions
            byte[] testData = randomByteArrayOfLength((int) fileLength);
            ByteBuffer writeBuffer = ByteBuffer.allocate(SharedBytes.PAGE_SIZE);
            cacheFile.populateAndRead(
                ByteRange.of(0L, fileLength),
                ByteRange.of(0L, fileLength),
                (channel, pos, relativePos, len) -> len,
                (channel, channelPos, streamFactory, relativePos, len, progressUpdater, completionListener) -> {
                    SharedBytes.copyToCacheFileAligned(
                        channel,
                        new java.io.ByteArrayInputStream(testData, relativePos, len),
                        channelPos,
                        relativePos,
                        len,
                        progressUpdater,
                        writeBuffer.clear()
                    );
                    ActionListener.completeWith(completionListener, () -> null);
                },
                "test"
            );

            // now withSliceAddresses should succeed for slices within regions
            boolean available = cacheFile.withSliceAddresses(offsets, sliceLen, 3, addrsOut, addrs -> {
                for (int i = 0; i < 3; i++) {
                    long addr = addrs.getAtIndex(ValueLayout.JAVA_LONG, i);
                    assertNotEquals(0L, addr);
                    MemorySegment slice = MemorySegment.ofAddress(addr).reinterpret(sliceLen);
                    byte[] sliceData = new byte[sliceLen];
                    MemorySegment.copy(slice, ValueLayout.JAVA_BYTE, 0, sliceData, 0, sliceLen);
                    for (int j = 0; j < sliceLen; j++) {
                        assertEquals(testData[(int) offsets[i] + j], sliceData[j]);
                    }
                }
            });
            assertTrue(available);
        }
    }

    // Verifies that withMemorySegmentSlices correctly handles multiple slices from the same region,
    // only acquiring one ref-count for deduplication.
    public void testWithByteBufferSlicesSameRegion() throws Exception {
        final int regionSize = (int) size(10);
        final long fileLength = size(8); // fits in a single region
        Settings settings = Settings.builder()
            .put(NODE_NAME_SETTING.getKey(), "node")
            .put(SharedBlobCacheService.SHARED_CACHE_SIZE_SETTING.getKey(), ByteSizeValue.ofBytes(size(50)).getStringRep())
            .put(SharedBlobCacheService.SHARED_CACHE_REGION_SIZE_SETTING.getKey(), ByteSizeValue.ofBytes(regionSize).getStringRep())
            .put(SharedBlobCacheService.SHARED_CACHE_MMAP.getKey(), true)
            .put("path.home", createTempDir())
            .build();
        final DeterministicTaskQueue taskQueue = new DeterministicTaskQueue();
        ExecutorService ioExecutor = EsExecutors.DIRECT_EXECUTOR_SERVICE;
        try (
            NodeEnvironment environment = new NodeEnvironment(settings, TestEnvironment.newEnvironment(settings));
            var cacheService = new SharedBlobCacheService<TestCacheKey>(
                environment,
                settings,
                taskQueue.getThreadPool(),
                ioExecutor,
                BlobCacheMetrics.NOOP
            )
        ) {
            final var cacheKey = generateCacheKey();
            SharedBlobCacheService<TestCacheKey>.CacheFile cacheFile = cacheService.getCacheFile(
                cacheKey,
                fileLength,
                SharedBlobCacheService.CacheMissHandler.NOOP
            );

            byte[] testData = randomByteArrayOfLength((int) fileLength);
            ByteBuffer writeBuffer = ByteBuffer.allocate(SharedBytes.PAGE_SIZE);
            cacheFile.populateAndRead(
                ByteRange.of(0L, fileLength),
                ByteRange.of(0L, fileLength),
                (channel, pos, relativePos, len) -> len,
                (channel, channelPos, streamFactory, relativePos, len, progressUpdater, completionListener) -> {
                    SharedBytes.copyToCacheFileAligned(
                        channel,
                        new java.io.ByteArrayInputStream(testData, relativePos, len),
                        channelPos,
                        relativePos,
                        len,
                        progressUpdater,
                        writeBuffer.clear()
                    );
                    ActionListener.completeWith(completionListener, () -> null);
                },
                "test"
            );

            // multiple slices all within the same region
            int sliceLen = 20;
            long[] offsets = { 0, 30, 60, 100 };
            int count = offsets.length;
            MemorySegment addrsOut = MemorySegment.ofArray(new long[count]);
            boolean available = cacheFile.withSliceAddresses(offsets, sliceLen, count, addrsOut, addrs -> {
                for (int i = 0; i < count; i++) {
                    long addr = addrs.getAtIndex(ValueLayout.JAVA_LONG, i);
                    assertNotEquals(0L, addr);
                    MemorySegment slice = MemorySegment.ofAddress(addr).reinterpret(sliceLen);
                    byte[] sliceData = new byte[sliceLen];
                    MemorySegment.copy(slice, ValueLayout.JAVA_BYTE, 0, sliceData, 0, sliceLen);
                    for (int j = 0; j < sliceLen; j++) {
                        assertEquals(testData[(int) offsets[i] + j], sliceData[j]);
                    }
                }
            });
            assertTrue(available);
        }
    }

    // Verifies that withMemorySegmentSlices returns false when any range crosses a region boundary,
    // even when other ranges are valid. Regions of size(10), file size(25) spanning 3 regions.
    public void testWithByteBufferSlicesCrossRegionReturnsFalse() throws Exception {
        final int regionSize = (int) size(10);
        final long fileLength = size(25); // spans 3 regions
        Settings settings = Settings.builder()
            .put(NODE_NAME_SETTING.getKey(), "node")
            .put(SharedBlobCacheService.SHARED_CACHE_SIZE_SETTING.getKey(), ByteSizeValue.ofBytes(size(200)).getStringRep())
            .put(SharedBlobCacheService.SHARED_CACHE_REGION_SIZE_SETTING.getKey(), ByteSizeValue.ofBytes(regionSize).getStringRep())
            .put(SharedBlobCacheService.SHARED_CACHE_MMAP.getKey(), true)
            .put("path.home", createTempDir())
            .build();
        final DeterministicTaskQueue taskQueue = new DeterministicTaskQueue();
        ExecutorService ioExecutor = EsExecutors.DIRECT_EXECUTOR_SERVICE;
        try (
            NodeEnvironment environment = new NodeEnvironment(settings, TestEnvironment.newEnvironment(settings));
            var cacheService = new SharedBlobCacheService<TestCacheKey>(
                environment,
                settings,
                taskQueue.getThreadPool(),
                ioExecutor,
                BlobCacheMetrics.NOOP
            )
        ) {
            final var cacheKey = generateCacheKey();
            SharedBlobCacheService<TestCacheKey>.CacheFile cacheFile = cacheService.getCacheFile(
                cacheKey,
                fileLength,
                SharedBlobCacheService.CacheMissHandler.NOOP
            );

            byte[] testData = randomByteArrayOfLength((int) fileLength);
            ByteBuffer writeBuffer = ByteBuffer.allocate(SharedBytes.PAGE_SIZE);
            cacheFile.populateAndRead(
                ByteRange.of(0L, fileLength),
                ByteRange.of(0L, fileLength),
                (channel, pos, relativePos, len) -> len,
                (channel, channelPos, streamFactory, relativePos, len, progressUpdater, completionListener) -> {
                    SharedBytes.copyToCacheFileAligned(
                        channel,
                        new java.io.ByteArrayInputStream(testData, relativePos, len),
                        channelPos,
                        relativePos,
                        len,
                        progressUpdater,
                        writeBuffer.clear()
                    );
                    ActionListener.completeWith(completionListener, () -> null);
                },
                "test"
            );

            int sliceLen = 200;
            int crossBoundaryOffset = regionSize - 100; // straddles region 0 -> region 1
            long[] offsets = { 10, crossBoundaryOffset, (long) regionSize * 2 + 5 };
            MemorySegment addrsOut = MemorySegment.ofArray(new long[3]);
            boolean available = cacheFile.withSliceAddresses(offsets, sliceLen, 3, addrsOut, addrs -> {
                fail("action should not be invoked when a range crosses a region boundary");
            });
            assertFalse(available);
        }
    }

    // Verifies that withMemorySegmentSlices returns false when mmap is disabled.
    public void testWithByteBufferSlicesNoMmapReturnsFalse() throws Exception {
        final int regionSize = (int) size(10);
        final long fileLength = size(8);
        Settings settings = Settings.builder()
            .put(NODE_NAME_SETTING.getKey(), "node")
            .put(SharedBlobCacheService.SHARED_CACHE_SIZE_SETTING.getKey(), ByteSizeValue.ofBytes(size(50)).getStringRep())
            .put(SharedBlobCacheService.SHARED_CACHE_REGION_SIZE_SETTING.getKey(), ByteSizeValue.ofBytes(regionSize).getStringRep())
            .put(SharedBlobCacheService.SHARED_CACHE_MMAP.getKey(), false)
            .put("path.home", createTempDir())
            .build();
        final DeterministicTaskQueue taskQueue = new DeterministicTaskQueue();
        ExecutorService ioExecutor = EsExecutors.DIRECT_EXECUTOR_SERVICE;
        try (
            NodeEnvironment environment = new NodeEnvironment(settings, TestEnvironment.newEnvironment(settings));
            var cacheService = new SharedBlobCacheService<TestCacheKey>(
                environment,
                settings,
                taskQueue.getThreadPool(),
                ioExecutor,
                BlobCacheMetrics.NOOP
            )
        ) {
            final var cacheKey = generateCacheKey();
            SharedBlobCacheService<TestCacheKey>.CacheFile cacheFile = cacheService.getCacheFile(
                cacheKey,
                fileLength,
                SharedBlobCacheService.CacheMissHandler.NOOP
            );

            byte[] testData = randomByteArrayOfLength((int) fileLength);
            ByteBuffer writeBuffer = ByteBuffer.allocate(SharedBytes.PAGE_SIZE);
            cacheFile.populateAndRead(
                ByteRange.of(0L, fileLength),
                ByteRange.of(0L, fileLength),
                (channel, pos, relativePos, len) -> len,
                (channel, channelPos, streamFactory, relativePos, len, progressUpdater, completionListener) -> {
                    SharedBytes.copyToCacheFileAligned(
                        channel,
                        new java.io.ByteArrayInputStream(testData, relativePos, len),
                        channelPos,
                        relativePos,
                        len,
                        progressUpdater,
                        writeBuffer.clear()
                    );
                    ActionListener.completeWith(completionListener, () -> null);
                },
                "test"
            );

            long[] offsets = { 0, 50 };
            MemorySegment addrsOut = MemorySegment.ofArray(new long[2]);
            assertFalse(cacheFile.withSliceAddresses(offsets, 20, 2, addrsOut, addrs -> fail("should not be invoked")));
        }
    }

    public void testWithByteBufferSlicesPartialPopulationReleasesRefs() throws Exception {
        final int regionSize = (int) size(10);
        final long fileLength = size(25); // spans 3 regions
        Settings settings = Settings.builder()
            .put(NODE_NAME_SETTING.getKey(), "node")
            .put(SharedBlobCacheService.SHARED_CACHE_SIZE_SETTING.getKey(), ByteSizeValue.ofBytes(size(200)).getStringRep())
            .put(SharedBlobCacheService.SHARED_CACHE_REGION_SIZE_SETTING.getKey(), ByteSizeValue.ofBytes(regionSize).getStringRep())
            .put(SharedBlobCacheService.SHARED_CACHE_MMAP.getKey(), true)
            .put("path.home", createTempDir())
            .build();
        final DeterministicTaskQueue taskQueue = new DeterministicTaskQueue();
        ExecutorService ioExecutor = EsExecutors.DIRECT_EXECUTOR_SERVICE;
        try (
            NodeEnvironment environment = new NodeEnvironment(settings, TestEnvironment.newEnvironment(settings));
            var cacheService = new SharedBlobCacheService<TestCacheKey>(
                environment,
                settings,
                taskQueue.getThreadPool(),
                ioExecutor,
                BlobCacheMetrics.NOOP
            )
        ) {
            final var cacheKey = generateCacheKey();
            SharedBlobCacheService<TestCacheKey>.CacheFile cacheFile = cacheService.getCacheFile(
                cacheKey,
                fileLength,
                SharedBlobCacheService.CacheMissHandler.NOOP
            );

            // populate only region 0, leaving regions 1 and 2 unpopulated
            byte[] testData = randomByteArrayOfLength(regionSize);
            ByteBuffer writeBuffer = ByteBuffer.allocate(SharedBytes.PAGE_SIZE);
            cacheFile.populateAndRead(
                ByteRange.of(0L, regionSize),
                ByteRange.of(0L, regionSize),
                (channel, pos, relativePos, len) -> len,
                (channel, channelPos, streamFactory, relativePos, len, progressUpdater, completionListener) -> {
                    SharedBytes.copyToCacheFileAligned(
                        channel,
                        new java.io.ByteArrayInputStream(testData, relativePos, len),
                        channelPos,
                        relativePos,
                        len,
                        progressUpdater,
                        writeBuffer.clear()
                    );
                    ActionListener.completeWith(completionListener, () -> null);
                },
                "test"
            );

            // request slices in region 0 (populated) and region 1 (not populated);
            // the loop acquires a ref on region 0, then hits the population check failure on region 1
            var region0 = cacheService.get(cacheKey, fileLength, 0);
            long[] offsets = { 50, (long) regionSize + 10 };
            int sliceLen = 50;
            MemorySegment addrsOut = MemorySegment.ofArray(new long[2]);
            assertFalse(cacheFile.withSliceAddresses(offsets, sliceLen, 2, addrsOut, addrs -> fail("should not be invoked")));

            // region 0's ref should have been released by the finally block
            synchronized (cacheService) {
                assertTrue("region 0 should be evictable after mid-loop failure released its ref", tryEvict(region0));
            }
        }
    }

    public void testWithByteBufferSlicesReleasesRefsOnException() throws Exception {
        final int regionSize = (int) size(10);
        final long fileLength = size(8); // fits in a single region
        Settings settings = Settings.builder()
            .put(NODE_NAME_SETTING.getKey(), "node")
            .put(SharedBlobCacheService.SHARED_CACHE_SIZE_SETTING.getKey(), ByteSizeValue.ofBytes(size(50)).getStringRep())
            .put(SharedBlobCacheService.SHARED_CACHE_REGION_SIZE_SETTING.getKey(), ByteSizeValue.ofBytes(regionSize).getStringRep())
            .put(SharedBlobCacheService.SHARED_CACHE_MMAP.getKey(), true)
            .put("path.home", createTempDir())
            .build();
        final DeterministicTaskQueue taskQueue = new DeterministicTaskQueue();
        ExecutorService ioExecutor = EsExecutors.DIRECT_EXECUTOR_SERVICE;
        try (
            NodeEnvironment environment = new NodeEnvironment(settings, TestEnvironment.newEnvironment(settings));
            var cacheService = new SharedBlobCacheService<TestCacheKey>(
                environment,
                settings,
                taskQueue.getThreadPool(),
                ioExecutor,
                BlobCacheMetrics.NOOP
            )
        ) {
            final var cacheKey = generateCacheKey();
            SharedBlobCacheService<TestCacheKey>.CacheFile cacheFile = cacheService.getCacheFile(
                cacheKey,
                fileLength,
                SharedBlobCacheService.CacheMissHandler.NOOP
            );

            byte[] testData = randomByteArrayOfLength((int) fileLength);
            ByteBuffer writeBuffer = ByteBuffer.allocate(SharedBytes.PAGE_SIZE);
            cacheFile.populateAndRead(
                ByteRange.of(0L, fileLength),
                ByteRange.of(0L, fileLength),
                (channel, pos, relativePos, len) -> len,
                (channel, channelPos, streamFactory, relativePos, len, progressUpdater, completionListener) -> {
                    SharedBytes.copyToCacheFileAligned(
                        channel,
                        new java.io.ByteArrayInputStream(testData, relativePos, len),
                        channelPos,
                        relativePos,
                        len,
                        progressUpdater,
                        writeBuffer.clear()
                    );
                    ActionListener.completeWith(completionListener, () -> null);
                },
                "test"
            );

            var region = cacheService.get(cacheKey, fileLength, 0);
            int freeBeforeCall = cacheService.freeRegionCount();

            long[] offsets = { 0, 50 };
            MemorySegment addrsOut = MemorySegment.ofArray(new long[2]);
            IOException thrown = expectThrows(IOException.class, () -> cacheFile.withSliceAddresses(offsets, 20, 2, addrsOut, addrs -> {
                throw new IOException("test exception");
            }));
            assertEquals("test exception", thrown.getMessage());

            assertEquals(freeBeforeCall, cacheService.freeRegionCount());

            synchronized (cacheService) {
                assertTrue("region should be evictable after refs released by finally block", tryEvict(region));
            }
        }
    }

    public void testGetIfPresentDoesNotAllocateRegionWhenAbsent() throws Exception {
        final long regionSize = size(10);
        final long fileLength = size(randomIntBetween(5, 19));
        Settings settings = Settings.builder()
            .put(NODE_NAME_SETTING.getKey(), "node")
            .put(SharedBlobCacheService.SHARED_CACHE_SIZE_SETTING.getKey(), ByteSizeValue.ofBytes(size(50)).getStringRep())
            .put(SharedBlobCacheService.SHARED_CACHE_REGION_SIZE_SETTING.getKey(), ByteSizeValue.ofBytes(regionSize).getStringRep())
            .put(SharedBlobCacheService.SHARED_CACHE_MMAP.getKey(), randomBoolean())
            .put("path.home", createTempDir())
            .build();
        final DeterministicTaskQueue taskQueue = new DeterministicTaskQueue();
        try (
            NodeEnvironment environment = new NodeEnvironment(settings, TestEnvironment.newEnvironment(settings));
            var cacheService = new SharedBlobCacheService<TestCacheKey>(
                environment,
                settings,
                taskQueue.getThreadPool(),
                EsExecutors.DIRECT_EXECUTOR_SERVICE,
                BlobCacheMetrics.NOOP
            )
        ) {
            final var cacheFile = cacheService.getCacheFile(generateCacheKey(), fileLength, SharedBlobCacheService.CacheMissHandler.NOOP);
            final int initialFreeRegions = cacheService.freeRegionCount();

            assertFalse(cacheFile.tryRead(ByteBuffer.allocate(100), 0));
            assertThat(cacheService.freeRegionCount(), equalTo(initialFreeRegions));

            assertFalse(cacheFile.tryPrefetch(0, fileLength));
            assertThat(cacheService.freeRegionCount(), equalTo(initialFreeRegions));

            assertFalse(cacheFile.withMemorySegmentSlice(0, 100, slice -> fail("should not be invoked")));
            assertThat(cacheService.freeRegionCount(), equalTo(initialFreeRegions));

            MemorySegment addrsOut1 = MemorySegment.ofArray(new long[1]);
            assertFalse(cacheFile.withSliceAddresses(new long[] { 0L }, 100, 1, addrsOut1, addrs -> fail("should not be invoked")));
            assertThat(cacheService.freeRegionCount(), equalTo(initialFreeRegions));
        }
    }

    public void testGetIfPresentFindsPopulatedEntry() throws Exception {
        final long regionSize = size(10);
        final long fileLength = size(randomIntBetween(5, 19));
        final boolean mmapEnabled = randomBoolean();
        Settings settings = Settings.builder()
            .put(NODE_NAME_SETTING.getKey(), "node")
            .put(SharedBlobCacheService.SHARED_CACHE_SIZE_SETTING.getKey(), ByteSizeValue.ofBytes(size(50)).getStringRep())
            .put(SharedBlobCacheService.SHARED_CACHE_REGION_SIZE_SETTING.getKey(), ByteSizeValue.ofBytes(regionSize).getStringRep())
            .put(SharedBlobCacheService.SHARED_CACHE_MMAP.getKey(), mmapEnabled)
            .put("path.home", createTempDir())
            .build();
        final DeterministicTaskQueue taskQueue = new DeterministicTaskQueue();
        try (
            NodeEnvironment environment = new NodeEnvironment(settings, TestEnvironment.newEnvironment(settings));
            var cacheService = new SharedBlobCacheService<TestCacheKey>(
                environment,
                settings,
                taskQueue.getThreadPool(),
                EsExecutors.DIRECT_EXECUTOR_SERVICE,
                BlobCacheMetrics.NOOP
            )
        ) {
            final var cacheKey = generateCacheKey();
            final var cacheFile = cacheService.getCacheFile(cacheKey, fileLength, SharedBlobCacheService.CacheMissHandler.NOOP);

            final byte[] testData = randomByteArrayOfLength((int) fileLength);
            final ByteBuffer writeBuffer = ByteBuffer.allocate(SharedBytes.PAGE_SIZE);
            cacheFile.populateAndRead(
                ByteRange.of(0L, fileLength),
                ByteRange.of(0L, fileLength),
                (channel, pos, relativePos, len) -> len,
                (channel, channelPos, streamFactory, relativePos, len, progressUpdater, completionListener) -> {
                    SharedBytes.copyToCacheFileAligned(
                        channel,
                        new java.io.ByteArrayInputStream(testData, relativePos, len),
                        channelPos,
                        relativePos,
                        len,
                        progressUpdater,
                        writeBuffer.clear()
                    );
                    ActionListener.completeWith(completionListener, () -> null);
                },
                "test"
            );

            final int singleRegionBound = (int) Math.min(regionSize, fileLength);
            final int readOffset = randomIntBetween(0, singleRegionBound / 2);
            final int readLength = randomIntBetween(1, singleRegionBound - readOffset);
            final byte[] expected = Arrays.copyOfRange(testData, readOffset, readOffset + readLength);
            final byte[] actual = new byte[readLength];

            assertTrue(cacheFile.tryRead(ByteBuffer.wrap(actual), readOffset));
            assertArrayEquals(expected, actual);

            if (mmapEnabled) {
                Arrays.fill(actual, (byte) 0);
                final boolean sliceAvailable = cacheFile.withMemorySegmentSlice(readOffset, readLength, slice -> {
                    assertTrue(slice.isReadOnly());
                    MemorySegment.copy(slice, ValueLayout.JAVA_BYTE, 0, actual, 0, actual.length);
                });
                assertTrue(sliceAvailable);
                assertArrayEquals(expected, actual);

                Arrays.fill(actual, (byte) 0);
                MemorySegment addrsOut = MemorySegment.ofArray(new long[1]);
                final boolean slicesAvailable = cacheFile.withSliceAddresses(new long[] { readOffset }, readLength, 1, addrsOut, addrs -> {
                    long addr = addrs.getAtIndex(ValueLayout.JAVA_LONG, 0);
                    assertNotEquals(0L, addr);
                    MemorySegment slice = MemorySegment.ofAddress(addr).reinterpret(actual.length);
                    MemorySegment.copy(slice, ValueLayout.JAVA_BYTE, 0, actual, 0, actual.length);
                });
                assertTrue(slicesAvailable);
                assertArrayEquals(expected, actual);
            }
        }
    }

    // Verify that madvise can be applied on the read path (cache hit) even when the region was
    // populated by a warming/prefetch service that did not call madvise. This simulates the pattern
    // where CacheFileReader.doRead calls channel.madvise(advice) in the RangeAvailableHandler.
    public void testMadviseAppliedOnReadPathForWarmedRegion() throws Exception {
        final long regionSize = size(1L);
        Settings settings = Settings.builder()
            .put(NODE_NAME_SETTING.getKey(), "node")
            .put(SharedBlobCacheService.SHARED_CACHE_SIZE_SETTING.getKey(), ByteSizeValue.ofBytes(size(100)).getStringRep())
            .put(SharedBlobCacheService.SHARED_CACHE_REGION_SIZE_SETTING.getKey(), ByteSizeValue.ofBytes(regionSize).getStringRep())
            .put(SharedBlobCacheService.SHARED_CACHE_INITIAL_DECAYS_SETTING.getKey(), 0)
            .put(SharedBlobCacheService.SHARED_CACHE_MMAP.getKey(), true)
            .put("path.home", createTempDir())
            .build();

        final DeterministicTaskQueue taskQueue = new DeterministicTaskQueue();
        try (
            NodeEnvironment environment = new NodeEnvironment(settings, TestEnvironment.newEnvironment(settings));
            var cacheService = new SharedBlobCacheService<>(
                environment,
                settings,
                taskQueue.getThreadPool(),
                taskQueue.getThreadPool().executor(ThreadPool.Names.GENERIC),
                BlobCacheMetrics.NOOP
            )
        ) {
            final var cacheKey = generateCacheKey();
            final var blobLength = regionSize;

            // Step 1: populate the region (simulates warming — no madvise applied)
            var entry = cacheService.get(cacheKey, blobLength, 0);
            final PlainActionFuture<Boolean> populateFuture = new PlainActionFuture<>();
            entry.populate(
                ByteRange.of(0, regionSize),
                (channel, channelPos, streamFactory, relativePos, length, progressUpdater, completionListener) -> completeWith(
                    completionListener,
                    () -> progressUpdater.accept(length)
                ),
                taskQueue.getThreadPool().generic(),
                populateFuture
            );
            taskQueue.runAllRunnableTasks();
            assertTrue(populateFuture.get(10, TimeUnit.SECONDS));

            // Step 2: read from cache with madvise in the reader callback (simulates CacheFileReader.doRead).
            // The reader callback asserts the channel starts at MADV_NORMAL then applies MADV_RANDOM.
            final var cacheFile = cacheService.getCacheFile(cacheKey, blobLength, SharedBlobCacheService.CacheMissHandler.NOOP);
            int bytesRead = cacheFile.populateAndRead(
                ByteRange.of(0, regionSize),
                ByteRange.of(0, regionSize),
                (channel, channelPos, relativePos, length) -> {
                    assertThat(channel.currentAdvice(), equalTo(SharedBytes.MADV_NORMAL));
                    channel.madvise(SharedBytes.MADV_RANDOM);
                    assertThat(channel.currentAdvice(), equalTo(SharedBytes.MADV_RANDOM));
                    return length;
                },
                (channel, channelPos, streamFactory, relativePos, length, progressUpdater, completionListener) -> completeWith(
                    completionListener,
                    () -> progressUpdater.accept(length)
                ),
                cacheFile.toString()
            );
            assertThat(bytesRead, equalTo(Math.toIntExact(regionSize)));
        }
    }

    // Verify that stale advice from a previous tenant is overwritten when a new tenant reads
    // the reused region. This covers the case where a region was MADV_RANDOM for shard A's .vec
    // file, gets evicted, then reused for shard B's .doc file with MADV_NORMAL.
    public void testStaleAdviceOverwrittenOnRegionReuse() throws Exception {
        final long regionSize = size(1L);
        Settings settings = Settings.builder()
            .put(NODE_NAME_SETTING.getKey(), "node")
            .put(SharedBlobCacheService.SHARED_CACHE_SIZE_SETTING.getKey(), ByteSizeValue.ofBytes(regionSize).getStringRep())
            .put(SharedBlobCacheService.SHARED_CACHE_REGION_SIZE_SETTING.getKey(), ByteSizeValue.ofBytes(regionSize).getStringRep())
            .put(SharedBlobCacheService.SHARED_CACHE_INITIAL_DECAYS_SETTING.getKey(), 0)
            .put(SharedBlobCacheService.SHARED_CACHE_MMAP.getKey(), true)
            .put("path.home", createTempDir())
            .build();

        final DeterministicTaskQueue taskQueue = new DeterministicTaskQueue();
        try (
            NodeEnvironment environment = new NodeEnvironment(settings, TestEnvironment.newEnvironment(settings));
            var cacheService = new SharedBlobCacheService<>(
                environment,
                settings,
                taskQueue.getThreadPool(),
                taskQueue.getThreadPool().executor(ThreadPool.Names.GENERIC),
                BlobCacheMetrics.NOOP
            )
        ) {
            // Step 1: populate a region and set MADV_RANDOM via the fill handler (simulates .vec file)
            final var vecKey = generateCacheKey();
            var entry = cacheService.get(vecKey, regionSize, 0);
            final PlainActionFuture<Boolean> populateFuture1 = new PlainActionFuture<>();
            entry.populate(
                ByteRange.of(0, regionSize),
                (channel, channelPos, streamFactory, relativePos, length, progressUpdater, completionListener) -> {
                    channel.madvise(SharedBytes.MADV_RANDOM);
                    completeWith(completionListener, () -> progressUpdater.accept(length));
                },
                taskQueue.getThreadPool().generic(),
                populateFuture1
            );
            taskQueue.runAllRunnableTasks();
            assertTrue(populateFuture1.get(10, TimeUnit.SECONDS));

            // Step 2: evict the region by triggering decay and allocating a new key
            cacheService.computeDecay();
            final var docKey = generateCacheKey();
            var newEntry = cacheService.get(docKey, regionSize, 0);

            // Step 3: populate the reused region (simulates warming for .doc file — no madvise)
            final PlainActionFuture<Boolean> populateFuture2 = new PlainActionFuture<>();
            newEntry.populate(
                ByteRange.of(0, regionSize),
                (channel, channelPos, streamFactory, relativePos, length, progressUpdater, completionListener) -> completeWith(
                    completionListener,
                    () -> progressUpdater.accept(length)
                ),
                taskQueue.getThreadPool().generic(),
                populateFuture2
            );
            taskQueue.runAllRunnableTasks();
            assertTrue(populateFuture2.get(10, TimeUnit.SECONDS));

            // Step 4: read with MADV_NORMAL in the reader callback (simulates CacheFileReader.doRead for .doc).
            // The channel should still carry stale MADV_RANDOM; the reader overwrites it with MADV_NORMAL.
            final var cacheFile = cacheService.getCacheFile(docKey, regionSize, SharedBlobCacheService.CacheMissHandler.NOOP);
            cacheFile.populateAndRead(
                ByteRange.of(0, regionSize),
                ByteRange.of(0, regionSize),
                (channel, channelPos, relativePos, length) -> {
                    assertThat(channel.currentAdvice(), equalTo(SharedBytes.MADV_RANDOM));
                    channel.madvise(SharedBytes.MADV_NORMAL);
                    assertThat(channel.currentAdvice(), equalTo(SharedBytes.MADV_NORMAL));
                    return length;
                },
                (channel, channelPos, streamFactory, relativePos, length, progressUpdater, completionListener) -> completeWith(
                    completionListener,
                    () -> progressUpdater.accept(length)
                ),
                cacheFile.toString()
            );
        }
    }

    // Verify that CacheFile.tryRead applies the supplied madvise advice.
    // This covers the fast-path used by CacheFileReader.tryRead for single-region cache hits.
    public void testMadviseAppliedOnTryReadFastPath() throws Exception {
        final long regionSize = size(1L);
        Settings settings = Settings.builder()
            .put(NODE_NAME_SETTING.getKey(), "node")
            .put(SharedBlobCacheService.SHARED_CACHE_SIZE_SETTING.getKey(), ByteSizeValue.ofBytes(size(100)).getStringRep())
            .put(SharedBlobCacheService.SHARED_CACHE_REGION_SIZE_SETTING.getKey(), ByteSizeValue.ofBytes(regionSize).getStringRep())
            .put(SharedBlobCacheService.SHARED_CACHE_INITIAL_DECAYS_SETTING.getKey(), 0)
            .put(SharedBlobCacheService.SHARED_CACHE_MMAP.getKey(), true)
            .put("path.home", createTempDir())
            .build();

        final DeterministicTaskQueue taskQueue = new DeterministicTaskQueue();
        try (
            NodeEnvironment environment = new NodeEnvironment(settings, TestEnvironment.newEnvironment(settings));
            var cacheService = new SharedBlobCacheService<>(
                environment,
                settings,
                taskQueue.getThreadPool(),
                taskQueue.getThreadPool().executor(ThreadPool.Names.GENERIC),
                BlobCacheMetrics.NOOP
            )
        ) {
            final var cacheKey = generateCacheKey();
            final var blobLength = regionSize;

            // Step 1: populate the region (simulates warming — no madvise applied)
            var entry = cacheService.get(cacheKey, blobLength, 0);
            final PlainActionFuture<Boolean> populateFuture = new PlainActionFuture<>();
            entry.populate(
                ByteRange.of(0, regionSize),
                (channel, channelPos, streamFactory, relativePos, length, progressUpdater, completionListener) -> completeWith(
                    completionListener,
                    () -> progressUpdater.accept(length)
                ),
                taskQueue.getThreadPool().generic(),
                populateFuture
            );
            taskQueue.runAllRunnableTasks();
            assertTrue(populateFuture.get(10, TimeUnit.SECONDS));

            // Step 2: use the tryRead fast path with MADV_RANDOM
            final var cacheFile = cacheService.getCacheFile(cacheKey, blobLength, SharedBlobCacheService.CacheMissHandler.NOOP);
            ByteBuffer buf = ByteBuffer.allocate(Math.toIntExact(regionSize));
            boolean success = cacheFile.tryRead(buf, 0, SharedBytes.MADV_RANDOM);
            assertTrue(success);

            // Step 3: verify the advice was applied by reading again via populateAndRead
            // and inspecting the channel's current advice
            cacheFile.populateAndRead(
                ByteRange.of(0, regionSize),
                ByteRange.of(0, regionSize),
                (channel, channelPos, relativePos, length) -> {
                    assertThat(channel.currentAdvice(), equalTo(SharedBytes.MADV_RANDOM));
                    return length;
                },
                (channel, channelPos, streamFactory, relativePos, length, progressUpdater, completionListener) -> completeWith(
                    completionListener,
                    () -> progressUpdater.accept(length)
                ),
                cacheFile.toString()
            );
        }
    }

    private record TestCacheKey(ShardId shardId, String file) implements SharedBlobCacheService.KeyBase {}

    private static TestCacheKey randomTestCacheKey(ShardId shardId) {
        return new TestCacheKey(shardId, randomAlphaOfLength(5));
    }

}
