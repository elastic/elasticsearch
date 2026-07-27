/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.cache.reader;

import org.elasticsearch.ResourceAlreadyUploadedException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.blobcache.BlobCacheMetrics;
import org.elasticsearch.blobcache.CachePopulationSource;
import org.elasticsearch.blobcache.shared.SharedBlobCacheService;
import org.elasticsearch.blobcache.shared.SharedBytes;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.telemetry.InstrumentType;
import org.elasticsearch.telemetry.Measurement;
import org.elasticsearch.telemetry.RecordingMeterRegistry;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.searchablesnapshots.cache.common.TestUtils;
import org.elasticsearch.xpack.stateless.StatelessPlugin;
import org.elasticsearch.xpack.stateless.cache.StatelessSharedBlobCacheService;
import org.elasticsearch.xpack.stateless.lucene.FileCacheKey;
import org.junit.After;
import org.junit.Before;

import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.elasticsearch.xpack.stateless.TestUtils.newCacheService;
import static org.elasticsearch.xpack.stateless.commits.BlobLocationTestUtils.createBlobFileRanges;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

public class CacheFileReaderTests extends ESTestCase {

    private static final int REGION_PAGES = 10;
    private static final int BLOB_LENGTH = REGION_PAGES * SharedBytes.PAGE_SIZE;
    private static final ByteSizeValue REGION_SIZE = ByteSizeValue.ofBytes(BLOB_LENGTH);

    private ThreadPool threadPool;

    @Before
    public void startThreadPool() throws Exception {
        threadPool = new TestThreadPool("CacheFileReaderTests", StatelessPlugin.statelessExecutorBuilders(Settings.EMPTY, randomBoolean()));
    }

    @After
    public void stopThreadPool() throws Exception {
        assertTrue(terminate(threadPool));
    }

    public void testTryPrefetchFetches() throws Exception {
        assumeTrue("object store prefetch feature is disabled", CacheFileReader.OBJECT_STORE_PREFETCH_FEATURE_FLAG.isEnabled());
        Settings settings = nodeSettings();
        RecordingMeterRegistry meterRegistry = new RecordingMeterRegistry();
        BlobCacheMetrics metrics = new BlobCacheMetrics(meterRegistry);

        try (
            NodeEnvironment env = new NodeEnvironment(settings, TestEnvironment.newEnvironment(settings));
            StatelessSharedBlobCacheService service = newCacheService(env, settings, threadPool)
        ) {
            String fileName = "prefetch-target";
            byte[] blob = randomByteArrayOfLength(BLOB_LENGTH);
            FileCacheKey cacheKey = new FileCacheKey(new ShardId(new Index("idx", "uid"), 0), 1L, fileName);
            AtomicInteger fetchCount = new AtomicInteger();
            CacheBlobReader reader = countingObjectStoreReader(fileName, blob, service.getRangeSize(), fetchCount);
            CacheFileReader cacheFileReader = new CacheFileReader(
                service.getCacheFile(cacheKey, blob.length, SharedBlobCacheService.CacheMissHandler.NOOP),
                reader,
                createBlobFileRanges(1L, 0L, 0, blob.length),
                metrics,
                System::currentTimeMillis
            );

            assertFalse("first call should miss the fast path", cacheFileReader.tryPrefetch(0L, blob.length));
            assertThat("blob store should have served at least one range request", fetchCount.get(), greaterThan(0));
            assertPrefetchMetric(meterRegistry, BlobCacheMetrics.PrefetchResult.Fetched, 1);
            assertPrefetchMetric(meterRegistry, BlobCacheMetrics.PrefetchResult.Failed, 0);
            assertPrefetchMetric(meterRegistry, BlobCacheMetrics.PrefetchResult.AlreadyCached, 0);

            int fetchCountAfterPopulate = fetchCount.get();
            assertTrue("second call should hit the fast path now that the range is cached", cacheFileReader.tryPrefetch(0L, blob.length));
            assertThat("fast path must not re-fetch from the blob store", fetchCount.get(), equalTo(fetchCountAfterPopulate));
            assertPrefetchMetric(meterRegistry, BlobCacheMetrics.PrefetchResult.AlreadyCached, 1);
        }
    }

    public void testTryPrefetchRecordsFailure() throws Exception {
        assumeTrue("object store prefetch feature is disabled", CacheFileReader.OBJECT_STORE_PREFETCH_FEATURE_FLAG.isEnabled());
        Settings settings = nodeSettings();
        RecordingMeterRegistry meterRegistry = new RecordingMeterRegistry();
        BlobCacheMetrics metrics = new BlobCacheMetrics(meterRegistry);

        try (
            NodeEnvironment env = new NodeEnvironment(settings, TestEnvironment.newEnvironment(settings));
            StatelessSharedBlobCacheService service = newCacheService(env, settings, threadPool)
        ) {
            String fileName = "prefetch-failure";
            byte[] blob = randomByteArrayOfLength(BLOB_LENGTH);
            FileCacheKey cacheKey = new FileCacheKey(new ShardId(new Index("idx", "uid"), 0), 1L, fileName);
            CacheBlobReader reader = new ObjectStoreCacheBlobReader(
                TestUtils.singleBlobContainer(fileName, blob),
                fileName,
                service.getRangeSize(),
                EsExecutors.DIRECT_EXECUTOR_SERVICE
            ) {
                @Override
                public void getRangeInputStream(long position, int length, ActionListener<InputStream> listener) {
                    listener.onFailure(new java.io.IOException("simulated blob fetch failure"));
                }
            };
            CacheFileReader cacheFileReader = new CacheFileReader(
                service.getCacheFile(cacheKey, blob.length, SharedBlobCacheService.CacheMissHandler.NOOP),
                reader,
                createBlobFileRanges(1L, 0L, 0, blob.length),
                metrics,
                System::currentTimeMillis
            );

            assertFalse(cacheFileReader.tryPrefetch(0L, blob.length));
            assertPrefetchMetric(meterRegistry, BlobCacheMetrics.PrefetchResult.Failed, 1);
            assertPrefetchMetric(meterRegistry, BlobCacheMetrics.PrefetchResult.Fetched, 0);
            assertPrefetchMetric(meterRegistry, BlobCacheMetrics.PrefetchResult.AlreadyCached, 0);
        }
    }

    public void testTryPrefetchWithOversizedFileLength() throws Exception {
        assumeTrue("object store prefetch feature is disabled", CacheFileReader.OBJECT_STORE_PREFETCH_FEATURE_FLAG.isEnabled());
        Settings settings = nodeSettings();
        RecordingMeterRegistry meterRegistry = new RecordingMeterRegistry();
        BlobCacheMetrics metrics = new BlobCacheMetrics(meterRegistry);

        try (
            NodeEnvironment env = new NodeEnvironment(settings, TestEnvironment.newEnvironment(settings));
            StatelessSharedBlobCacheService service = newCacheService(env, settings, threadPool)
        ) {
            String fileName = "prefetch-oversized-length";
            byte[] blob = randomByteArrayOfLength(BLOB_LENGTH);
            FileCacheKey cacheKey = new FileCacheKey(new ShardId(new Index("idx", "uid"), 0), 1L, fileName);
            AtomicInteger fetchCount = new AtomicInteger();
            CacheBlobReader reader = countingObjectStoreReader(fileName, blob, service.getRangeSize(), fetchCount);
            CacheFileReader cacheFileReader = new CacheFileReader(
                service.getCacheFile(cacheKey, blob.length, SharedBlobCacheService.CacheMissHandler.NOOP),
                reader,
                createBlobFileRanges(1L, 0L, 0, blob.length),
                metrics,
                System::currentTimeMillis
            );

            long oversizedLength = (long) blob.length * 1024L;
            assertFalse(
                "oversized prefetch must not hit the fast path on the first call",
                cacheFileReader.tryPrefetch(0L, oversizedLength)
            );
            assertThat("oversized prefetch must still trigger a fetch from the blob store", fetchCount.get(), greaterThan(0));
            assertPrefetchMetric(meterRegistry, BlobCacheMetrics.PrefetchResult.Fetched, 1);
            assertPrefetchMetric(meterRegistry, BlobCacheMetrics.PrefetchResult.Failed, 0);
            assertPrefetchMetric(meterRegistry, BlobCacheMetrics.PrefetchResult.AlreadyCached, 0);
        }
    }

    public void testTryPrefetchPastEOF() throws Exception {
        assumeTrue("object store prefetch feature is disabled", CacheFileReader.OBJECT_STORE_PREFETCH_FEATURE_FLAG.isEnabled());
        Settings settings = nodeSettings();
        RecordingMeterRegistry meterRegistry = new RecordingMeterRegistry();
        BlobCacheMetrics metrics = new BlobCacheMetrics(meterRegistry);

        try (
            NodeEnvironment env = new NodeEnvironment(settings, TestEnvironment.newEnvironment(settings));
            StatelessSharedBlobCacheService service = newCacheService(env, settings, threadPool)
        ) {
            String fileName = "prefetch-past-eof";
            byte[] blob = randomByteArrayOfLength(BLOB_LENGTH);
            FileCacheKey cacheKey = new FileCacheKey(new ShardId(new Index("idx", "uid"), 0), 1L, fileName);
            AtomicInteger fetchCount = new AtomicInteger();
            CacheBlobReader reader = countingObjectStoreReader(fileName, blob, service.getRangeSize(), fetchCount);
            CacheFileReader cacheFileReader = new CacheFileReader(
                service.getCacheFile(cacheKey, blob.length, SharedBlobCacheService.CacheMissHandler.NOOP),
                reader,
                createBlobFileRanges(1L, 0L, 0, blob.length),
                metrics,
                System::currentTimeMillis
            );

            long offsetAtOrPastEof = randomBoolean() ? blob.length : blob.length + randomLongBetween(1L, 1024L);
            assertFalse(cacheFileReader.tryPrefetch(offsetAtOrPastEof, randomLongBetween(1L, 1024L)));
            assertThat("no fetch should be triggered when the offset is past EOF", fetchCount.get(), equalTo(0));
            assertPrefetchMetric(meterRegistry, BlobCacheMetrics.PrefetchResult.AlreadyCached, 0);
            assertPrefetchMetric(meterRegistry, BlobCacheMetrics.PrefetchResult.Fetched, 0);
            assertPrefetchMetric(meterRegistry, BlobCacheMetrics.PrefetchResult.Failed, 0);
        }
    }

    public void testTryPrefetchNonPositiveLength() throws Exception {
        assumeTrue("object store prefetch feature is disabled", CacheFileReader.OBJECT_STORE_PREFETCH_FEATURE_FLAG.isEnabled());
        Settings settings = nodeSettings();
        RecordingMeterRegistry meterRegistry = new RecordingMeterRegistry();
        BlobCacheMetrics metrics = new BlobCacheMetrics(meterRegistry);

        try (
            NodeEnvironment env = new NodeEnvironment(settings, TestEnvironment.newEnvironment(settings));
            StatelessSharedBlobCacheService service = newCacheService(env, settings, threadPool)
        ) {
            String fileName = "prefetch-zero-length";
            byte[] blob = randomByteArrayOfLength(BLOB_LENGTH);
            FileCacheKey cacheKey = new FileCacheKey(new ShardId(new Index("idx", "uid"), 0), 1L, fileName);
            AtomicInteger fetchCount = new AtomicInteger();
            CacheBlobReader reader = countingObjectStoreReader(fileName, blob, service.getRangeSize(), fetchCount);
            CacheFileReader cacheFileReader = new CacheFileReader(
                service.getCacheFile(cacheKey, blob.length, SharedBlobCacheService.CacheMissHandler.NOOP),
                reader,
                createBlobFileRanges(1L, 0L, 0, blob.length),
                metrics,
                System::currentTimeMillis
            );

            long nonPositiveLength = randomBoolean() ? 0L : -randomLongBetween(1L, 1024L);
            assertFalse(cacheFileReader.tryPrefetch(0L, nonPositiveLength));
            assertThat("no fetch should be triggered when length is non-positive", fetchCount.get(), equalTo(0));
            assertPrefetchMetric(meterRegistry, BlobCacheMetrics.PrefetchResult.AlreadyCached, 0);
            assertPrefetchMetric(meterRegistry, BlobCacheMetrics.PrefetchResult.Fetched, 0);
            assertPrefetchMetric(meterRegistry, BlobCacheMetrics.PrefetchResult.Failed, 0);
        }
    }

    public void testTryPrefetchOversizedLengthIsLimited() throws Exception {
        assumeTrue("object store prefetch feature is disabled", CacheFileReader.OBJECT_STORE_PREFETCH_FEATURE_FLAG.isEnabled());
        Settings settings = nodeSettings();
        RecordingMeterRegistry meterRegistry = new RecordingMeterRegistry();
        BlobCacheMetrics metrics = new BlobCacheMetrics(meterRegistry);

        try (
            NodeEnvironment env = new NodeEnvironment(settings, TestEnvironment.newEnvironment(settings));
            StatelessSharedBlobCacheService service = newCacheService(env, settings, threadPool)
        ) {
            String fileName = "prefetch-midfile-oversized";
            byte[] blob = randomByteArrayOfLength(BLOB_LENGTH);
            FileCacheKey cacheKey = new FileCacheKey(new ShardId(new Index("idx", "uid"), 0), 1L, fileName);
            AtomicInteger fetchCount = new AtomicInteger();
            CacheBlobReader reader = countingObjectStoreReader(fileName, blob, service.getRangeSize(), fetchCount);
            CacheFileReader cacheFileReader = new CacheFileReader(
                service.getCacheFile(cacheKey, blob.length, SharedBlobCacheService.CacheMissHandler.NOOP),
                reader,
                createBlobFileRanges(1L, 0L, 0, blob.length),
                metrics,
                System::currentTimeMillis
            );

            long midFileOffset = randomLongBetween(1L, blob.length - 1);
            long oversizedLength = (blob.length - midFileOffset) + randomLongBetween(1L, blob.length);
            assertFalse(cacheFileReader.tryPrefetch(midFileOffset, oversizedLength));
            assertThat(fetchCount.get(), greaterThan(0));
            assertPrefetchMetric(meterRegistry, BlobCacheMetrics.PrefetchResult.Fetched, 1);
            assertPrefetchMetric(meterRegistry, BlobCacheMetrics.PrefetchResult.Failed, 0);
            assertPrefetchMetric(meterRegistry, BlobCacheMetrics.PrefetchResult.AlreadyCached, 0);
        }
    }

    public void testTryPrefetchRetriesOnAlreadyUploaded() throws Exception {
        assumeTrue("object store prefetch feature is disabled", CacheFileReader.OBJECT_STORE_PREFETCH_FEATURE_FLAG.isEnabled());
        Settings settings = nodeSettings();
        RecordingMeterRegistry meterRegistry = new RecordingMeterRegistry();
        BlobCacheMetrics metrics = new BlobCacheMetrics(meterRegistry);

        try (
            NodeEnvironment env = new NodeEnvironment(settings, TestEnvironment.newEnvironment(settings));
            StatelessSharedBlobCacheService service = newCacheService(env, settings, threadPool)
        ) {
            String fileName = "prefetch-already-uploaded-retry";
            byte[] blob = randomByteArrayOfLength(BLOB_LENGTH);
            FileCacheKey cacheKey = new FileCacheKey(new ShardId(new Index("idx", "uid"), 0), 1L, fileName);
            AtomicInteger fetchCount = new AtomicInteger();
            CacheBlobReader reader = alreadyUploadedThenServingReader(fileName, blob, service.getRangeSize(), 1, fetchCount);
            CacheFileReader cacheFileReader = new CacheFileReader(
                service.getCacheFile(cacheKey, blob.length, SharedBlobCacheService.CacheMissHandler.NOOP),
                reader,
                createBlobFileRanges(1L, 0L, 0, blob.length),
                metrics,
                System::currentTimeMillis
            );

            assertFalse(
                "first call should miss the fast path and schedule an async download",
                cacheFileReader.tryPrefetch(0L, blob.length)
            );
            assertThat("the fetch should have failed once and then succeeded on the retry", fetchCount.get(), equalTo(2));
            assertPrefetchMetric(meterRegistry, BlobCacheMetrics.PrefetchResult.Fetched, 1);
            assertPrefetchMetric(meterRegistry, BlobCacheMetrics.PrefetchResult.Failed, 0);
            assertPrefetchMetric(meterRegistry, BlobCacheMetrics.PrefetchResult.AlreadyCached, 0);

            assertTrue("the retry should have populated the cache", cacheFileReader.tryPrefetch(0L, blob.length));
            assertPrefetchMetric(meterRegistry, BlobCacheMetrics.PrefetchResult.AlreadyCached, 1);
            assertPrefetchMetric(meterRegistry, BlobCacheMetrics.PrefetchResult.Fetched, 1);
        }
    }

    public void testTryPrefetchFailsAfterMaxAlreadyUploadedRetries() throws Exception {
        assumeTrue("object store prefetch feature is disabled", CacheFileReader.OBJECT_STORE_PREFETCH_FEATURE_FLAG.isEnabled());
        Settings settings = nodeSettings();
        RecordingMeterRegistry meterRegistry = new RecordingMeterRegistry();
        BlobCacheMetrics metrics = new BlobCacheMetrics(meterRegistry);

        try (
            NodeEnvironment env = new NodeEnvironment(settings, TestEnvironment.newEnvironment(settings));
            StatelessSharedBlobCacheService service = newCacheService(env, settings, threadPool)
        ) {
            String fileName = "prefetch-already-uploaded-exhausted";
            byte[] blob = randomByteArrayOfLength(BLOB_LENGTH);
            FileCacheKey cacheKey = new FileCacheKey(new ShardId(new Index("idx", "uid"), 0), 1L, fileName);
            AtomicInteger fetchCount = new AtomicInteger();
            // Always fail with ResourceAlreadyUploadedException; if retries were unbounded this reader would be called forever.
            CacheBlobReader reader = alreadyUploadedThenServingReader(
                fileName,
                blob,
                service.getRangeSize(),
                Integer.MAX_VALUE,
                fetchCount
            );
            CacheFileReader cacheFileReader = new CacheFileReader(
                service.getCacheFile(cacheKey, blob.length, SharedBlobCacheService.CacheMissHandler.NOOP),
                reader,
                createBlobFileRanges(1L, 0L, 0, blob.length),
                metrics,
                System::currentTimeMillis
            );

            assertFalse(cacheFileReader.tryPrefetch(0L, blob.length));
            assertThat("prefetch must stop after exhausting the retry budget", fetchCount.get(), equalTo(3));
            assertPrefetchMetric(meterRegistry, BlobCacheMetrics.PrefetchResult.Failed, 1);
            assertPrefetchMetric(meterRegistry, BlobCacheMetrics.PrefetchResult.Fetched, 0);
            assertPrefetchMetric(meterRegistry, BlobCacheMetrics.PrefetchResult.AlreadyCached, 0);
        }
    }

    public void testTryPrefetchDoesNotRetryOnOtherError() throws Exception {
        assumeTrue("object store prefetch feature is disabled", CacheFileReader.OBJECT_STORE_PREFETCH_FEATURE_FLAG.isEnabled());
        Settings settings = nodeSettings();
        RecordingMeterRegistry meterRegistry = new RecordingMeterRegistry();
        BlobCacheMetrics metrics = new BlobCacheMetrics(meterRegistry);

        try (
            NodeEnvironment env = new NodeEnvironment(settings, TestEnvironment.newEnvironment(settings));
            StatelessSharedBlobCacheService service = newCacheService(env, settings, threadPool)
        ) {
            String fileName = "prefetch-other-error";
            byte[] blob = randomByteArrayOfLength(BLOB_LENGTH);
            FileCacheKey cacheKey = new FileCacheKey(new ShardId(new Index("idx", "uid"), 0), 1L, fileName);
            AtomicInteger fetchCount = new AtomicInteger();
            // A non-ResourceAlreadyUploadedException failure must not be retried.
            CacheBlobReader reader = new ObjectStoreCacheBlobReader(
                TestUtils.singleBlobContainer(fileName, blob),
                fileName,
                service.getRangeSize(),
                EsExecutors.DIRECT_EXECUTOR_SERVICE
            ) {
                @Override
                public void getRangeInputStream(long position, int length, ActionListener<InputStream> listener) {
                    fetchCount.incrementAndGet();
                    listener.onFailure(new java.io.IOException("simulated blob fetch failure"));
                }
            };
            CacheFileReader cacheFileReader = new CacheFileReader(
                service.getCacheFile(cacheKey, blob.length, SharedBlobCacheService.CacheMissHandler.NOOP),
                reader,
                createBlobFileRanges(1L, 0L, 0, blob.length),
                metrics,
                System::currentTimeMillis
            );

            assertFalse(cacheFileReader.tryPrefetch(0L, blob.length));
            assertThat(fetchCount.get(), equalTo(1));
            assertPrefetchMetric(meterRegistry, BlobCacheMetrics.PrefetchResult.Failed, 1);
            assertPrefetchMetric(meterRegistry, BlobCacheMetrics.PrefetchResult.Fetched, 0);
            assertPrefetchMetric(meterRegistry, BlobCacheMetrics.PrefetchResult.AlreadyCached, 0);
        }
    }

    /**
     * {@code SEARCH_ORIGIN_REMOTE_STORAGE_DOWNLOAD_TOOK_TIME} must carry {@link CachePopulationSource#BlobStore}
     * for SEARCH-thread reads, {@link CachePopulationSource#Peer} for VBCC-thread reads, and no measurement
     * for non-{@code EsThread} callers.
     */
    public void testReadRecordsSearchOriginMetricWithCorrectAttributes() throws Exception {
        assumeTrue(
            "pre-population via tryPrefetch requires OBJECT_STORE_PREFETCH_FEATURE_FLAG",
            CacheFileReader.OBJECT_STORE_PREFETCH_FEATURE_FLAG.isEnabled()
        );
        Settings settings = nodeSettings();
        RecordingMeterRegistry meterRegistry = new RecordingMeterRegistry();
        BlobCacheMetrics metrics = new BlobCacheMetrics(meterRegistry);

        try (
            NodeEnvironment env = new NodeEnvironment(settings, TestEnvironment.newEnvironment(settings));
            StatelessSharedBlobCacheService service = newCacheService(env, settings, threadPool)
        ) {
            String fileName = "read-attribute-test";
            byte[] blob = randomByteArrayOfLength(BLOB_LENGTH);
            FileCacheKey cacheKey = new FileCacheKey(new ShardId(new Index("idx", "uid"), 0), 1L, fileName);
            CacheBlobReader blobReader = countingObjectStoreReader(fileName, blob, service.getRangeSize(), new AtomicInteger());
            CacheFileReader cacheFileReader = new CacheFileReader(
                service.getCacheFile(cacheKey, blob.length, SharedBlobCacheService.CacheMissHandler.NOOP),
                blobReader,
                createBlobFileRanges(1L, 0L, 0, blob.length),
                metrics,
                System::currentTimeMillis
            );

            // Pre-populate the cache so read() calls go through the fast path
            // and do not block on SHARD_READ_THREAD_POOL
            assertFalse("first prefetch should schedule an async download", cacheFileReader.tryPrefetch(0L, blob.length));
            assertBusy(() -> assertTrue("cache should be fully populated", cacheFileReader.tryPrefetch(0L, blob.length)));
            meterRegistry.getRecorder().resetCalls();

            // SEARCH thread → BlobStore attribute
            PlainActionFuture<Void> searchFuture = new PlainActionFuture<>();
            threadPool.executor(ThreadPool.Names.SEARCH).execute(() -> {
                try {
                    cacheFileReader.read(this, ByteBuffer.allocate(blob.length), 0, blob.length, blob.length, "test-search");
                    searchFuture.onResponse(null);
                } catch (Exception e) {
                    searchFuture.onFailure(e);
                }
            });
            safeGet(searchFuture);
            assertSearchOriginMeasurementAttribute(meterRegistry, CachePopulationSource.BlobStore.name());
            meterRegistry.getRecorder().resetCalls();

            // VBCC (peer/index-tier) thread → Peer attribute
            PlainActionFuture<Void> vbccFuture = new PlainActionFuture<>();
            threadPool.executor(StatelessPlugin.GET_VIRTUAL_BATCHED_COMPOUND_COMMIT_CHUNK_THREAD_POOL).execute(() -> {
                try {
                    cacheFileReader.read(this, ByteBuffer.allocate(blob.length), 0, blob.length, blob.length, "test-vbcc");
                    vbccFuture.onResponse(null);
                } catch (Exception e) {
                    vbccFuture.onFailure(e);
                }
            });
            safeGet(vbccFuture);
            assertSearchOriginMeasurementAttribute(meterRegistry, CachePopulationSource.Peer.name());
            meterRegistry.getRecorder().resetCalls();

            // Non-EsThread (plain JUnit thread) → no metric recorded
            cacheFileReader.read(this, ByteBuffer.allocate(blob.length), 0, blob.length, blob.length, "test-plain");
            assertThat(
                meterRegistry.getRecorder()
                    .getMeasurements(InstrumentType.LONG_HISTOGRAM, BlobCacheMetrics.SEARCH_ORIGIN_REMOTE_STORAGE_DOWNLOAD_TOOK_TIME)
                    .size(),
                equalTo(0)
            );
        }
    }

    private static CacheBlobReader countingObjectStoreReader(String fileName, byte[] blob, long cacheRangeSize, AtomicInteger counter) {
        return new ObjectStoreCacheBlobReader(
            TestUtils.singleBlobContainer(fileName, blob),
            fileName,
            cacheRangeSize,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        ) {
            @Override
            public void getRangeInputStream(long position, int length, ActionListener<InputStream> listener) {
                counter.incrementAndGet();
                super.getRangeInputStream(position, length, listener);
            }
        };
    }

    private static CacheBlobReader alreadyUploadedThenServingReader(
        String fileName,
        byte[] blob,
        long cacheRangeSize,
        int failCount,
        AtomicInteger fetchCount
    ) {
        return new ObjectStoreCacheBlobReader(
            TestUtils.singleBlobContainer(fileName, blob),
            fileName,
            cacheRangeSize,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        ) {
            @Override
            public void getRangeInputStream(long position, int length, ActionListener<InputStream> listener) {
                if (fetchCount.getAndIncrement() < failCount) {
                    listener.onFailure(new ResourceAlreadyUploadedException("VBCC already uploaded: " + position + "+" + length));
                } else {
                    super.getRangeInputStream(position, length, listener);
                }
            }
        };
    }

    private Settings nodeSettings() {
        return Settings.builder()
            .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toAbsolutePath())
            .putList(Environment.PATH_DATA_SETTING.getKey(), createTempDir().toAbsolutePath().toString())
            .put(
                SharedBlobCacheService.SHARED_CACHE_SIZE_SETTING.getKey(),
                ByteSizeValue.ofBytes(50 * SharedBytes.PAGE_SIZE).getStringRep()
            )
            .put(SharedBlobCacheService.SHARED_CACHE_REGION_SIZE_SETTING.getKey(), REGION_SIZE.getStringRep())
            .put(SharedBlobCacheService.SHARED_CACHE_RANGE_SIZE_SETTING.getKey(), REGION_SIZE.getStringRep())
            .put(SharedBlobCacheService.SHARED_CACHE_MMAP.getKey(), true)
            .build();
    }

    private static void assertSearchOriginMeasurementAttribute(RecordingMeterRegistry meterRegistry, String expectedSource) {
        List<Measurement> measurements = meterRegistry.getRecorder()
            .getMeasurements(InstrumentType.LONG_HISTOGRAM, BlobCacheMetrics.SEARCH_ORIGIN_REMOTE_STORAGE_DOWNLOAD_TOOK_TIME);
        assertThat("expected at least one search-origin measurement", measurements.size(), greaterThan(0));
        for (Measurement measurement : measurements) {
            assertThat(measurement.attributes().get(BlobCacheMetrics.CACHE_POPULATION_SOURCE_ATTRIBUTE_KEY), equalTo(expectedSource));
        }
    }

    private static void assertPrefetchMetric(RecordingMeterRegistry meterRegistry, BlobCacheMetrics.PrefetchResult result, long expected) {
        long observed = meterRegistry.getRecorder()
            .getMeasurements(InstrumentType.LONG_COUNTER, BlobCacheMetrics.BLOB_CACHE_PREFETCH_TOTAL)
            .stream()
            .filter(m -> result.name().equals(m.attributes().get(BlobCacheMetrics.PREFETCH_RESULT_ATTRIBUTE_KEY)))
            .mapToLong(Measurement::getLong)
            .sum();
        assertEquals("expected " + expected + " [" + result + "] prefetch measurement(s)", expected, observed);
    }
}
