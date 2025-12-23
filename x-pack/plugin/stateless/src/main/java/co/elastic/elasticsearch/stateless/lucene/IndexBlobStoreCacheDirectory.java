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

package co.elastic.elasticsearch.stateless.lucene;

import co.elastic.elasticsearch.stateless.cache.reader.CacheBlobReader;
import co.elastic.elasticsearch.stateless.cache.reader.MeteringCacheBlobReader;
import co.elastic.elasticsearch.stateless.cache.reader.ObjectStoreCacheBlobReader;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;
import org.elasticsearch.blobcache.BlobCacheMetrics;
import org.elasticsearch.blobcache.CachePopulationSource;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.stateless.cache.StatelessSharedBlobCacheService;
import org.elasticsearch.xpack.stateless.commits.BlobLocation;

import java.util.concurrent.Executor;
import java.util.concurrent.atomic.LongAdder;

public class IndexBlobStoreCacheDirectory extends BlobStoreCacheDirectory {

    public IndexBlobStoreCacheDirectory(StatelessSharedBlobCacheService cacheService, ShardId shardId) {
        super(cacheService, shardId);
    }

    private IndexBlobStoreCacheDirectory(
        StatelessSharedBlobCacheService cacheService,
        ShardId shardId,
        LongAdder totalBytesRead,
        LongAdder totalBytesWarmed
    ) {
        super(cacheService, shardId, totalBytesRead, totalBytesWarmed);
    }

    @Override
    protected CacheBlobReader getCacheBlobReader(String fileName, BlobLocation location) {
        return createCacheBlobReader(
            fileName,
            getBlobContainer(location.primaryTerm()),
            location.blobName(),
            getCacheService().getShardReadThreadPoolExecutor(),
            totalBytesReadFromObjectStore,
            BlobCacheMetrics.CachePopulationReason.CacheMiss
        );
    }

    @Override
    public CacheBlobReader getCacheBlobReaderForWarming(String fileName, BlobLocation location) {
        return createCacheBlobReader(
            fileName,
            getBlobContainer(location.primaryTerm()),
            location.blobName(),
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
    public IndexBlobStoreCacheDirectory createNewInstance() {
        var instance = new IndexBlobStoreCacheDirectory(
            cacheService,
            shardId,
            totalBytesReadFromObjectStore,
            totalBytesWarmedFromObjectStore
        ) {

            @Override
            protected CacheBlobReader getCacheBlobReader(String fileName, BlobLocation location) {
                return createCacheBlobReader(
                    fileName,
                    getBlobContainer(location.primaryTerm()),
                    location.blobName(),
                    getCacheService().getShardReadThreadPoolExecutor(),
                    // account for warming instead of cache miss when doing regular reads with a "prewarming" instance
                    totalBytesWarmedFromObjectStore,
                    BlobCacheMetrics.CachePopulationReason.Warming
                );
            }

            @Override
            public CacheBlobReader getCacheBlobReaderForWarming(String fileName, BlobLocation location) {
                return createCacheBlobReader(
                    fileName,
                    getBlobContainer(location.primaryTerm()),
                    location.blobName(),
                    EsExecutors.DIRECT_EXECUTOR_SERVICE,
                    totalBytesWarmedFromObjectStore,
                    BlobCacheMetrics.CachePopulationReason.Warming,
                    ThreadPool.Names.GENERIC
                );
            }
        };
        if (blobContainer.get() != null) {
            instance.setBlobContainer(blobContainer.get());
        }
        return instance;
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
