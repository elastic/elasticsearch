/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.cache;

import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.blobcache.BlobCacheMetrics;
import org.elasticsearch.blobcache.shared.SharedBlobCacheService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.store.PluggableDirectoryMetricsHolder;
import org.elasticsearch.index.store.ThreadLocalDirectoryMetricHolder;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.stateless.lucene.BlobStoreCacheDirectoryMetrics;
import org.elasticsearch.xpack.stateless.lucene.FileCacheKey;

import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;

import static org.elasticsearch.node.Node.NODE_NAME_SETTING;
import static org.hamcrest.Matchers.is;

public class StatelessSharedBlobCacheServiceTests extends ESTestCase {

    private static final long REGION_SIZE = 4096L;

    public void testBackfillStampsHeaderRegions() throws Exception {
        final long cacheSize = REGION_SIZE * 100L;
        Settings settings = Settings.builder()
            .put(NODE_NAME_SETTING.getKey(), "node")
            .put(SharedBlobCacheService.SHARED_CACHE_SIZE_SETTING.getKey(), ByteSizeValue.ofBytes(cacheSize).getStringRep())
            .put(SharedBlobCacheService.SHARED_CACHE_REGION_SIZE_SETTING.getKey(), ByteSizeValue.ofBytes(REGION_SIZE).getStringRep())
            .put(SharedBlobCacheService.SHARED_CACHE_INITIAL_DECAYS_SETTING.getKey(), 0)
            .put(StatelessSharedBlobCacheService.STATELESS_CACHE_BOOST_PREFERENCE_ENABLED_SETTING.getKey(), true)
            .put("path.home", createTempDir())
            .build();

        final var threadPool = new TestThreadPool("test");
        // a writer that simply reports the requested bytes as populated
        final SharedBlobCacheService.RangeMissingHandler writer = (
            channel,
            channelPos,
            streamFactory,
            relativePos,
            length,
            progressUpdater,
            completionListener) -> {
            progressUpdater.accept(length);
            completionListener.onResponse(null);
        };

        try (
            NodeEnvironment environment = new NodeEnvironment(settings, TestEnvironment.newEnvironment(settings));
            var cacheService = new TestStatelessSharedBlobCacheService(
                environment,
                settings,
                threadPool,
                System::nanoTime,
                new ThreadLocalDirectoryMetricHolder<>(BlobStoreCacheDirectoryMetrics::new)
            )
        ) {
            final var shardId = new ShardId(new Index("index", "uuid"), 0);
            final long primaryTerm = 1L;
            final String blobName = "stateless_commit_1";
            final var cacheKey = new FileCacheKey(shardId, primaryTerm, blobName);
            final long blobLength = REGION_SIZE * 2;

            // populate two regions without a timestamp (UNKNOWN)
            populateRegion(cacheService, cacheKey, 0, blobLength, writer, threadPool);
            populateRegion(cacheService, cacheKey, 1, blobLength, writer, threadPool);
            assertEquals(SharedBlobCacheService.UNKNOWN_TIMESTAMP, cacheService.regionTimestamp(cacheKey, 0));
            assertEquals(SharedBlobCacheService.UNKNOWN_TIMESTAMP, cacheService.regionTimestamp(cacheKey, 1));

            // backfill a byte range spanning both regions
            final long ts = randomLongBetween(1, Long.MAX_VALUE - 1);
            cacheService.backfillTimestamp(shardId, primaryTerm, blobName, 0L, blobLength, ts);

            assertEquals(ts, cacheService.regionTimestamp(cacheKey, 0));
            assertEquals(ts, cacheService.regionTimestamp(cacheKey, 1));

            cacheService.backfillTimestamp(shardId, primaryTerm, blobName, 0L, blobLength, SharedBlobCacheService.UNKNOWN_TIMESTAMP);
            assertEquals(ts, cacheService.regionTimestamp(cacheKey, 0));

            // backfilling an absent blob is a no-op and does not allocate anything
            final var absentKey = new FileCacheKey(shardId, primaryTerm, "stateless_commit_2");
            cacheService.backfillTimestamp(shardId, primaryTerm, "stateless_commit_2", 0L, blobLength, ts);
            assertEquals(SharedBlobCacheService.UNKNOWN_TIMESTAMP, cacheService.regionTimestamp(absentKey, 0));
        } finally {
            TestThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
        }
    }

    private static void populateRegion(
        StatelessSharedBlobCacheService cacheService,
        FileCacheKey cacheKey,
        int region,
        long blobLength,
        SharedBlobCacheService.RangeMissingHandler writer,
        ThreadPool threadPool
    ) throws Exception {
        final PlainActionFuture<Boolean> future = new PlainActionFuture<>();
        cacheService.fetchRegion(cacheKey, region, blobLength, writer, threadPool.generic(), true, future);
        assertThat(future.get(10, TimeUnit.SECONDS), is(true));
    }

    /**
     * Test-only subclass that exposes the protected {@code getRegionTimestampOrUnknown} test seam and relaxes the
     * offset-within-file-length assertion so tests can fetch synthetic ranges.
     */
    private static class TestStatelessSharedBlobCacheService extends StatelessSharedBlobCacheService {
        TestStatelessSharedBlobCacheService(
            NodeEnvironment environment,
            Settings settings,
            ThreadPool threadPool,
            LongSupplier relativeTimeInNanosSupplier,
            PluggableDirectoryMetricsHolder<BlobStoreCacheDirectoryMetrics> metricsHolder
        ) {
            super(environment, settings, threadPool, BlobCacheMetrics.NOOP, relativeTimeInNanosSupplier, metricsHolder);
        }

        long regionTimestamp(FileCacheKey cacheKey, int region) {
            return getRegionTimestampOrUnknown(cacheKey, region);
        }

        @Override
        protected boolean assertOffsetsWithinFileLength(long offset, long length, long fileLength) {
            return true;
        }
    }
}
