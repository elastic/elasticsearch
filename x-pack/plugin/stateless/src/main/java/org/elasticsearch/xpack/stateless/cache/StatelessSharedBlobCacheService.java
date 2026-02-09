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

import org.elasticsearch.blobcache.BlobCacheMetrics;
import org.elasticsearch.blobcache.shared.SharedBlobCacheService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.stateless.StatelessPlugin;
import org.elasticsearch.xpack.stateless.lucene.FileCacheKey;

import java.util.concurrent.Executor;
import java.util.function.LongSupplier;

public class StatelessSharedBlobCacheService extends SharedBlobCacheService<FileCacheKey> {

    // Stateless shared blob cache service populates-and-reads in-thread. And it relies on the cache service to fetch gap bytes
    // asynchronously using a CacheBlobReader.
    private static final Executor IO_EXECUTOR = EsExecutors.DIRECT_EXECUTOR_SERVICE;

    private final Executor shardReadThreadPoolExecutor;

    public StatelessSharedBlobCacheService(
        NodeEnvironment environment,
        Settings settings,
        ThreadPool threadPool,
        BlobCacheMetrics blobCacheMetrics
    ) {
        super(environment, settings, threadPool, IO_EXECUTOR, blobCacheMetrics);
        this.shardReadThreadPoolExecutor = threadPool.executor(StatelessPlugin.SHARD_READ_THREAD_POOL);
    }

    // for tests
    public StatelessSharedBlobCacheService(
        NodeEnvironment environment,
        Settings settings,
        ThreadPool threadPool,
        BlobCacheMetrics blobCacheMetrics,
        LongSupplier relativeTimeInNanosSupplier
    ) {
        super(environment, settings, threadPool, IO_EXECUTOR, blobCacheMetrics, relativeTimeInNanosSupplier);
        this.shardReadThreadPoolExecutor = IO_EXECUTOR;
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
    public long getRegionEnd(int region) {
        return super.getRegionEnd(region);
    }
}
