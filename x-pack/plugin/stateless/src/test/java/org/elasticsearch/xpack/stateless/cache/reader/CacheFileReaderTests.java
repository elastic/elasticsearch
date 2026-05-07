/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.cache.reader;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.blobcache.BlobCacheMetrics;
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

import java.io.InputStream;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.elasticsearch.xpack.stateless.TestUtils.newCacheService;
import static org.elasticsearch.xpack.stateless.commits.BlobLocationTestUtils.createBlobFileRanges;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

/**
 * Tests for {@link CacheFileReader#tryPrefetch} which delegates to
 * {@link SharedBlobCacheService.CacheFile#populate} with a {@code RangeAvailableHandler} that issues
 * an {@code madvise(WILLNEED)}-style hint via {@link SharedBytes.IO#prefetch}.
 */
public class CacheFileReaderTests extends ESTestCase {

    private static final int REGION_PAGES = 10;
    private static final int BLOB_LENGTH = REGION_PAGES * SharedBytes.PAGE_SIZE;
    private static final ByteSizeValue REGION_SIZE = ByteSizeValue.ofBytes(BLOB_LENGTH);

    private ThreadPool threadPool;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        threadPool = new TestThreadPool("CacheFileReaderTests", StatelessPlugin.statelessExecutorBuilders(Settings.EMPTY, true));
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        assertTrue(ThreadPool.terminate(threadPool, 10L, TimeUnit.SECONDS));
    }

    /**
     * On an indexing node {@link CacheFileReader#tryPrefetch} must short-circuit: the slow path is gated by
     * {@link StatelessSharedBlobCacheService#hasSearchRole()} so neither the blob reader nor the cache is touched.
     */
    public void testTryPrefetchOnIndexingNodeIsNoOp() throws Exception {
        Settings settings = nodeSettings("index");
        RecordingMeterRegistry meterRegistry = new RecordingMeterRegistry();
        BlobCacheMetrics metrics = new BlobCacheMetrics(meterRegistry);

        try (
            NodeEnvironment env = new NodeEnvironment(settings, TestEnvironment.newEnvironment(settings));
            StatelessSharedBlobCacheService service = newCacheService(env, settings, threadPool)
        ) {
            String fileName = "no-op-prefetch";
            byte[] blob = randomByteArrayOfLength(BLOB_LENGTH);
            FileCacheKey cacheKey = new FileCacheKey(new ShardId(new Index("idx", "uid"), 0), 1L, fileName);
            AtomicInteger fetchCount = new AtomicInteger();
            CacheBlobReader reader = countingObjectStoreReader(fileName, blob, service.getRangeSize(), fetchCount);
            CacheFileReader cacheFileReader = new CacheFileReader(
                service.getCacheFile(cacheKey, blob.length, SharedBlobCacheService.CacheMissHandler.NOOP),
                reader,
                createBlobFileRanges(1L, 0L, 0, blob.length),
                metrics,
                System::currentTimeMillis,
                service.hasSearchRole()
            );

            assertFalse("indexing node tryPrefetch must report false", cacheFileReader.tryPrefetch(0L, blob.length));
            assertThat("indexing node must not fetch from blob store", fetchCount.get(), equalTo(0));
            assertPrefetchMetric(meterRegistry, BlobCacheMetrics.PrefetchResult.AlreadyCached, 0);
            assertPrefetchMetric(meterRegistry, BlobCacheMetrics.PrefetchResult.Fetched, 0);
            assertPrefetchMetric(meterRegistry, BlobCacheMetrics.PrefetchResult.Failed, 0);
        }
    }

    /**
     * On a search node {@link CacheFileReader#tryPrefetch} populates the cache via {@link SharedBlobCacheService.CacheFile#populate}.
     * Because stateless wires the cache service's {@code ioExecutor} to {@link EsExecutors#DIRECT_EXECUTOR_SERVICE} and the
     * {@link ObjectStoreCacheBlobReader} below also runs on a direct executor, the populate completes synchronously, so a second
     * {@code tryPrefetch} for the same range hits the fast path. That fast path calls {@link SharedBytes.IO#prefetch}, exercising
     * the {@code madvise(WILLNEED)} hint that the populate-based prefetch was introduced for.
     */
    public void testTryPrefetchOnSearchNodeFetchesAndPrefetches() throws Exception {
        Settings settings = nodeSettings("search");
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
                System::currentTimeMillis,
                service.hasSearchRole()
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

    /**
     * When the underlying blob store fetch fails the listener receives the failure and the {@code Failed} prefetch outcome is
     * recorded (no exception is propagated to the caller).
     */
    public void testTryPrefetchRecordsFailure() throws Exception {
        Settings settings = nodeSettings("search");
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
                System::currentTimeMillis,
                service.hasSearchRole()
            );

            assertFalse(cacheFileReader.tryPrefetch(0L, blob.length));
            assertPrefetchMetric(meterRegistry, BlobCacheMetrics.PrefetchResult.Failed, 1);
            assertPrefetchMetric(meterRegistry, BlobCacheMetrics.PrefetchResult.Fetched, 0);
            assertPrefetchMetric(meterRegistry, BlobCacheMetrics.PrefetchResult.AlreadyCached, 0);
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

    private Settings nodeSettings(String role) {
        return Settings.builder()
            .putList("node.roles", role)
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
