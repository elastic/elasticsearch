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
        return (bytesRead, readTimeNanos) -> {
            bytesReadAdder.add(bytesRead);
            cacheService.getBlobCacheMetrics()
                .recordCachePopulationMetrics(fileName, bytesRead, readTimeNanos, cachePopulationReason, CachePopulationSource.BlobStore);
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
