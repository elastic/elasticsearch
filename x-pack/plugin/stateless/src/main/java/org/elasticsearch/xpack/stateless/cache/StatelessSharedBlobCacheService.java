/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.cache;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.RefCountingListener;
import org.elasticsearch.blobcache.BlobCacheMetrics;
import org.elasticsearch.blobcache.common.ByteRange;
import org.elasticsearch.blobcache.shared.SharedBlobCacheService;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Setting;
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

    // Overall setting to disable/enable the cache boost preference feature.
    public static final Setting<Boolean> STATELESS_CACHE_BOOST_PREFERENCE_ENABLED_SETTING = Setting.boolSetting(
        "stateless.cache_boost_preference.enabled",
        false,
        Setting.Property.NodeScope
    );

    /**
     * Selects the eviction policy used by the shared blob cache when {@link #STATELESS_CACHE_BOOST_PREFERENCE_ENABLED_SETTING} is enabled.
     * When cache boost preference is disabled, {@link StatelessCacheEvictionPolicyType#ALWAYS} is used regardless of this setting.
     * Defaults to {@link StatelessCacheEvictionPolicyType#PINNED_WINDOW} on search nodes and
     * {@link StatelessCacheEvictionPolicyType#ALWAYS} on all other nodes.
     */
    public static final Setting<StatelessCacheEvictionPolicyType> STATELESS_CACHE_BOOST_PREFERENCE_EVICTION_POLICY_SETTING = Setting
        .enumSetting(
            StatelessCacheEvictionPolicyType.class,
            settings -> StatelessCacheEvictionPolicyType.resolveEvictionPolicyFromSettings(settings).name(),
            "stateless.cache_boost_preference.eviction_policy",
            s -> {},
            Setting.Property.NodeScope
        );

    // Stateless shared blob cache service populates-and-reads in-thread. And it relies on the cache service to fetch gap bytes
    // asynchronously using a CacheBlobReader.
    private static final Executor IO_EXECUTOR = EsExecutors.DIRECT_EXECUTOR_SERVICE;

    private final Executor shardReadThreadPoolExecutor;
    private final PluggableDirectoryMetricsHolder<BlobStoreCacheDirectoryMetrics> metricsHolder;
    private final boolean hasSearchRole;

    // TODO Merge the two constructors
    public StatelessSharedBlobCacheService(
        NodeEnvironment environment,
        Settings settings,
        ThreadPool threadPool,
        BlobCacheMetrics blobCacheMetrics,
        ClusterService clusterService,
        PluggableDirectoryMetricsHolder<BlobStoreCacheDirectoryMetrics> metricsHolder
    ) {
        super(
            environment,
            settings,
            threadPool,
            IO_EXECUTOR,
            blobCacheMetrics,
            StatelessCacheEvictionPolicyType.createEvictionPolicy(settings, clusterService)
        );
        this.shardReadThreadPoolExecutor = threadPool.executor(StatelessPlugin.SHARD_READ_THREAD_POOL);
        this.metricsHolder = metricsHolder;
        this.hasSearchRole = DiscoveryNode.hasRole(settings, DiscoveryNodeRole.SEARCH_ROLE);
    }

    // for tests
    public StatelessSharedBlobCacheService(
        NodeEnvironment environment,
        Settings settings,
        ThreadPool threadPool,
        BlobCacheMetrics blobCacheMetrics,
        ClusterService clusterService,
        LongSupplier relativeTimeInNanosSupplier,
        PluggableDirectoryMetricsHolder<BlobStoreCacheDirectoryMetrics> metricsHolder
    ) {
        super(
            environment,
            settings,
            threadPool,
            IO_EXECUTOR,
            blobCacheMetrics,
            relativeTimeInNanosSupplier,
            StatelessCacheEvictionPolicyType.createEvictionPolicy(settings, clusterService)
        );
        this.shardReadThreadPoolExecutor = IO_EXECUTOR;
        this.metricsHolder = metricsHolder;
        this.hasSearchRole = DiscoveryNode.hasRole(settings, DiscoveryNodeRole.SEARCH_ROLE);
    }

    /**
     * Fetches and writes in cache a blob byte range, given the {@link CacheBlobReader} and the blob's associated {@link FileCacheKey}.
     */
    private void fetchRange(
        FileCacheKey cacheKey,
        ByteRange byteRange,
        CacheBlobReader cacheBlobReader,
        Object initiator,
        Supplier<ByteBuffer> writeBufferSupplier,
        IntConsumer bytesCopiedConsumer,
        Executor fetchExecutor,
        boolean force,
        ActionListener<Void> listener,
        String... threadPools
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
                            threadPools
                        )
                    ),
                    fetchExecutor,
                    force,
                    listeners.acquire().map(populated -> null)
                );
            }
        }
    }

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
        fetchRange(
            cacheKey,
            byteRange,
            cacheBlobReader,
            initiator,
            writeBufferSupplier,
            bytesCopiedConsumer,
            fetchExecutor,
            force,
            listener,
            StatelessPlugin.PREWARM_THREAD_POOL,
            StatelessPlugin.FILL_VIRTUAL_BATCHED_COMPOUND_COMMIT_CACHE_THREAD_POOL
        );
    }

    public boolean hasSearchRole() {
        return hasSearchRole;
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
