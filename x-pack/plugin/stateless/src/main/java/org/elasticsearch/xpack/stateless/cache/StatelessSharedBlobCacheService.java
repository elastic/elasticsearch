/*
 * ELASTICSEARCH CONFIDENTIAL
 * __________________
 *
 * Copyright Elasticsearch B.V. All rights reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Elasticsearch B.V. and its suppliers, if any.
 * The intellectual and technical concepts contained herein
 * are proprietary to Elasticsearch B.V. and its suppliers and
 * may be covered by U.S. and Foreign Patents, patents in
 * process, and are protected by trade secret or copyright
 * law.  Dissemination of this information or reproduction of
 * this material is strictly forbidden unless prior written
 * permission is obtained from Elasticsearch B.V.
 */

package org.elasticsearch.xpack.stateless.cache;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.RefCountingListener;
import org.elasticsearch.blobcache.BlobCacheMetrics;
import org.elasticsearch.blobcache.common.ByteRange;
import org.elasticsearch.blobcache.shared.SharedBlobCacheService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.index.store.PluggableDirectoryMetricsHolder;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.stateless.StatelessPlugin;
import org.elasticsearch.xpack.stateless.cache.reader.CacheBlobReader;
import org.elasticsearch.xpack.stateless.cache.reader.LazyRangeMissingHandler;
import org.elasticsearch.xpack.stateless.cache.reader.SequentialRangeMissingHandler;
import org.elasticsearch.xpack.stateless.lucene.BlobStoreCacheDirectoryMetrics;
import org.elasticsearch.xpack.stateless.lucene.FileCacheKey;

import java.nio.ByteBuffer;
import java.util.concurrent.Executor;
import java.util.function.IntConsumer;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

public class StatelessSharedBlobCacheService extends SharedBlobCacheService<FileCacheKey> {

    // Stateless shared blob cache service populates-and-reads in-thread. And it relies on the cache service to fetch gap bytes
    // asynchronously using a CacheBlobReader.
    private static final Executor IO_EXECUTOR = EsExecutors.DIRECT_EXECUTOR_SERVICE;

    private final Executor shardReadThreadPoolExecutor;
    private final PluggableDirectoryMetricsHolder<BlobStoreCacheDirectoryMetrics> metricsHolder;

    public StatelessSharedBlobCacheService(
        NodeEnvironment environment,
        Settings settings,
        ThreadPool threadPool,
        BlobCacheMetrics blobCacheMetrics,
        PluggableDirectoryMetricsHolder<BlobStoreCacheDirectoryMetrics> metricsHolder
    ) {
        super(environment, settings, threadPool, IO_EXECUTOR, blobCacheMetrics);
        this.shardReadThreadPoolExecutor = threadPool.executor(StatelessPlugin.SHARD_READ_THREAD_POOL);
        this.metricsHolder = metricsHolder;
    }

    // for tests
    public StatelessSharedBlobCacheService(
        NodeEnvironment environment,
        Settings settings,
        ThreadPool threadPool,
        BlobCacheMetrics blobCacheMetrics,
        LongSupplier relativeTimeInNanosSupplier,
        PluggableDirectoryMetricsHolder<BlobStoreCacheDirectoryMetrics> metricsHolder
    ) {
        super(environment, settings, threadPool, IO_EXECUTOR, blobCacheMetrics, relativeTimeInNanosSupplier);
        this.shardReadThreadPoolExecutor = IO_EXECUTOR;
        this.metricsHolder = metricsHolder;
    }

    /**
     * Fetches and writes in cache a blob byte range, given the {@link CacheBlobReader} and the blob's associated {@link FileCacheKey}.
     */
    void fetchRange(
        FileCacheKey cacheKey,
        ByteRange byteRange,
        CacheBlobReader cacheBlobReader,
        Object initiator,
        Supplier<ByteBuffer> writeBufferSupplier,
        IntConsumer bytesCopiedConsumer,
        Executor fetchExecutor,
        boolean force,
        ActionListener<Void> listener
    ) {
        var startRegion = getRegion(byteRange.start());
        var endRegion = getEndingRegion(byteRange.end());
        try (RefCountingListener listeners = new RefCountingListener(listener)) {
            for (int region = startRegion; region <= endRegion; region++) {
                long regionRangeStart = Math.max(getRegionStart(region), byteRange.start());
                long regionRangeEnd = Math.min(getRegionEnd(region), byteRange.end());
                var adjustedByteRange = cacheBlobReader.getRange(
                    regionRangeStart,
                    Math.toIntExact(regionRangeEnd - regionRangeStart),
                    byteRange.end() - regionRangeStart
                );
                fetchRange(
                    cacheKey,
                    region,
                    adjustedByteRange,
                    // this is not really used
                    byteRange.length(),
                    new LazyRangeMissingHandler<>(
                        () -> new SequentialRangeMissingHandler(
                            initiator,
                            cacheKey.fileName(),
                            adjustedByteRange,
                            cacheBlobReader,
                            () -> writeBufferSupplier.get().clear(),
                            bytesCopiedConsumer,
                            StatelessPlugin.PREWARM_THREAD_POOL,
                            StatelessPlugin.FILL_VIRTUAL_BATCHED_COMPOUND_COMMIT_CACHE_THREAD_POOL
                        )
                    ),
                    fetchExecutor,
                    force,
                    listeners.acquire().map(populated -> null)
                );
            }
        }
    }

    public void assertInvariants() {
        assert getRangeSize() >= getRegionSize() : getRangeSize() + " < " + getRegionSize();
    }

    public Executor getShardReadThreadPoolExecutor() {
        return shardReadThreadPoolExecutor;
    }

    @Override
    protected int computeCacheFileRegionSize(long fileLength, int region) {
        return getRegionSize();
    }

    @Override
    public int getRegion(long position) {
        return super.getRegion(position);
    }

    @Override
    public int getEndingRegion(long position) {
        return super.getEndingRegion(position);
    }

    @Override
    public long getRegionStart(int region) {
        return super.getRegionStart(region);
    }

    @Override
    public long getRegionEnd(int region) {
        return super.getRegionEnd(region);
    }

    public PluggableDirectoryMetricsHolder<BlobStoreCacheDirectoryMetrics> metricsHolder() {
        return metricsHolder;
    }
}
