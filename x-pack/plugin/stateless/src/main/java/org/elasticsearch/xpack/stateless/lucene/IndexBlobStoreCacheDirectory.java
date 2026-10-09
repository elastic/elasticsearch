/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.lucene;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;
import org.elasticsearch.blobcache.BlobCacheMetrics;
import org.elasticsearch.blobcache.CachePopulationSource;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.stateless.StatelessPlugin;
import org.elasticsearch.xpack.stateless.cache.StatelessSharedBlobCacheService;
import org.elasticsearch.xpack.stateless.cache.reader.CacheBlobReader;
import org.elasticsearch.xpack.stateless.cache.reader.MeteringCacheBlobReader;
import org.elasticsearch.xpack.stateless.cache.reader.ObjectStoreCacheBlobReader;
import org.elasticsearch.xpack.stateless.commits.BlobFile;

import java.util.concurrent.Executor;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.LongFunction;

public class IndexBlobStoreCacheDirectory extends BlobStoreCacheDirectory {

    public IndexBlobStoreCacheDirectory(StatelessSharedBlobCacheService cacheService, ShardId shardId) {
        super(cacheService, shardId);
    }

    private IndexBlobStoreCacheDirectory(
        StatelessSharedBlobCacheService cacheService,
        ShardId shardId,
        LongAdder totalBytesRead,
        LongAdder totalBytesWarmed,
        @Nullable LongFunction<BlobContainer> blobContainerFunction
    ) {
        super(cacheService, shardId, totalBytesRead, totalBytesWarmed, blobContainerFunction);
    }

    @Override
    protected CacheBlobReader getCacheBlobReader(String fileName, BlobFile blobFile) {
        return createCacheBlobReader(
            fileName,
            getBlobContainer(blobFile.primaryTerm()),
            blobFile.blobName(),
            getCacheService().getShardReadThreadPoolExecutor(),
            totalBytesReadFromObjectStore,
            BlobCacheMetrics.CachePopulationReason.CacheMiss
        );
    }

    @Override
    public CacheBlobReader getCacheBlobReaderForWarming(BlobFile blobFile) {
        return createCacheBlobReader(
            blobFile.blobName(),
            getBlobContainer(blobFile.primaryTerm()),
            blobFile.blobName(),
            EsExecutors.DIRECT_EXECUTOR_SERVICE,
            totalBytesWarmedFromObjectStore,
            BlobCacheMetrics.CachePopulationReason.Warming,
            StatelessPlugin.PREWARM_THREAD_POOL,
            ThreadPool.Names.GENERIC
        );
    }

    private MeteringCacheBlobReader createCacheBlobReader(
        String fileName,
        BlobContainer blobContainer,
        String blobName,
        Executor fetchExecutor,
        LongAdder bytesReadAdder,
        BlobCacheMetrics.CachePopulationReason cachePopulationReason,
        String... expectedThreadPoolNames
    ) {
        assert expectedThreadPoolNames.length == 0 || ThreadPool.assertCurrentThreadPool(expectedThreadPoolNames);
        return new MeteringCacheBlobReader(
            new ObjectStoreCacheBlobReader(blobContainer, blobName, getCacheService().getRangeSize(), fetchExecutor),
            createReadCompleteCallback(fileName, bytesReadAdder, cachePopulationReason)
        );
    }

    private MeteringCacheBlobReader.ReadCompleteCallback createReadCompleteCallback(
        String fileName,
        LongAdder bytesReadAdder,
        BlobCacheMetrics.CachePopulationReason cachePopulationReason
    ) {
        return new MeteringCacheBlobReader.ReadCompleteCallback() {
            @Override
            public void onBytesRead(int bytesRead) {
                bytesReadAdder.add(bytesRead);
            }

            @Override
            public void onReadCompleted(int totalBytesRead, long readTimeNanos) {
                cacheService.getBlobCacheMetrics()
                    .recordCachePopulationMetrics(
                        fileName,
                        totalBytesRead,
                        readTimeNanos,
                        cachePopulationReason,
                        CachePopulationSource.BlobStore
                    );
            }
        };
    }

    @Override
    public IndexBlobStoreCacheDirectory createNewBlobStoreCacheDirectoryForWarming() {
        return new IndexBlobStoreCacheDirectory(
            cacheService,
            shardId,
            totalBytesReadFromObjectStore,
            totalBytesWarmedFromObjectStore,
            blobContainer.get()
        ) {
            @Override
            protected CacheBlobReader getCacheBlobReader(String fileName, BlobFile blobFile) {
                return createCacheBlobReader(
                    fileName,
                    getBlobContainer(blobFile.primaryTerm()),
                    blobFile.blobName(),
                    getCacheService().getShardReadThreadPoolExecutor(),
                    // account for warming instead of cache miss when doing regular reads with a "prewarming" instance
                    totalBytesWarmedFromObjectStore,
                    BlobCacheMetrics.CachePopulationReason.Warming
                );
            }
        };
    }

    /**
     * Cache misses of the returned directory claim their gaps in a task on the shard read pool instead of on the reading thread, so that
     * a concurrent region 0 prewarm, which runs on the prewarm pool, can claim and fill the same range first. The read completes from
     * whichever fill comes first.
     * <p>
     * A miss then costs a single task on the shard read pool, and only as long as these three settings line up:
     * <ul>
     * <li>the claim executor of the cache file, {@link #claimExecutor()}, is the shard read pool,
     * <li>the blob reader fetches in-thread ({@code DIRECT}) instead of dispatching to the shard read pool again, and
     * <li>the cache fills the claimed gaps inline, which holds because the stateless cache service uses a {@code DIRECT} io executor.
     * </ul>
     * Nothing enforces this. If any of them changes, a miss silently waits in the shard read queue twice.
     */
    @Override
    public IndexBlobStoreCacheDirectory createPerBccMetadataReadDirectory() {
        return new IndexBlobStoreCacheDirectory(
            cacheService,
            shardId,
            totalBytesReadFromObjectStore,
            totalBytesWarmedFromObjectStore,
            blobContainer.get()
        ) {
            @Override
            protected CacheBlobReader getCacheBlobReader(String fileName, BlobFile blobFile) {
                return createCacheBlobReader(
                    fileName,
                    getBlobContainer(blobFile.primaryTerm()),
                    blobFile.blobName(),
                    EsExecutors.DIRECT_EXECUTOR_SERVICE,
                    totalBytesWarmedFromObjectStore,
                    BlobCacheMetrics.CachePopulationReason.Warming
                );
            }

            @Override
            protected Executor claimExecutor() {
                return getCacheService().getShardReadThreadPoolExecutor();
            }
        };
    }

    public static IndexBlobStoreCacheDirectory unwrapDirectory(final Directory directory) {
        Directory dir = directory;
        while (dir != null) {
            if (dir instanceof IndexBlobStoreCacheDirectory blobStoreCacheDirectory) {
                return blobStoreCacheDirectory;
            } else if (dir instanceof IndexDirectory indexDirectory) {
                return indexDirectory.getBlobStoreCacheDirectory();
            } else if (dir instanceof FilterDirectory) {
                dir = ((FilterDirectory) dir).getDelegate();
            } else {
                dir = null;
            }
        }
        var e = new IllegalStateException(directory.getClass() + " cannot be unwrapped as " + IndexBlobStoreCacheDirectory.class);
        assert false : e;
        throw e;
    }
}
