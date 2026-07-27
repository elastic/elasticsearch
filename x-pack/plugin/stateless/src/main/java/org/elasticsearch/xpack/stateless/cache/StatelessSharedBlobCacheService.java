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
import org.elasticsearch.blobcache.shared.EvictionPolicy;
import org.elasticsearch.blobcache.shared.SharedBlobCacheService;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.Strings;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.index.store.PluggableDirectoryMetricsHolder;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.stateless.StatelessPlugin;
import org.elasticsearch.xpack.stateless.cache.reader.CacheBlobReader;
import org.elasticsearch.xpack.stateless.cache.reader.LazyRangeMissingHandler;
import org.elasticsearch.xpack.stateless.cache.reader.SequentialRangeMissingHandler;
import org.elasticsearch.xpack.stateless.lucene.BlobStoreCacheDirectoryMetrics;
import org.elasticsearch.xpack.stateless.lucene.FileCacheKey;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.function.IntConsumer;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

public class StatelessSharedBlobCacheService extends SharedBlobCacheService<FileCacheKey> {

    // Overall setting to disable/enable the cache boost preference feature.
    public static final Setting<Boolean> STATELESS_CACHE_BOOST_PREFERENCE_ENABLED_SETTING = Setting.boolSetting(
        "stateless.cache_boost_preference.enabled",
        false,
        // Boost preference relies on timestamp ranges in {@link BlobFileRanges} which are only built up when use of replicated content is
        // enabled.
        new Setting.Validator<>() {
            @Override
            public void validate(Boolean value) {}

            @Override
            public void validate(Boolean value, Map<Setting<?>, Object> settings) {
                final boolean replicatedContentEnabled = (boolean) settings.get(
                    SearchCommitPrefetcherDynamicSettings.STATELESS_SEARCH_USE_INTERNAL_FILES_REPLICATED_CONTENT
                );
                if (value && replicatedContentEnabled == false) {
                    throw new IllegalArgumentException(
                        Strings.format(
                            "Setting [%s] cannot be [true] unless setting [%s] is also [true]",
                            STATELESS_CACHE_BOOST_PREFERENCE_ENABLED_SETTING.getKey(),
                            SearchCommitPrefetcherDynamicSettings.STATELESS_SEARCH_USE_INTERNAL_FILES_REPLICATED_CONTENT.getKey()
                        )
                    );
                }
            }

            @Override
            public Iterator<Setting<?>> settings() {
                return Iterators.single(SearchCommitPrefetcherDynamicSettings.STATELESS_SEARCH_USE_INTERNAL_FILES_REPLICATED_CONTENT);
            }
        },
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

    /// Setting gating eviction of cache regions that belong to obsolete segments on search directories (see
    /// [org.elasticsearch.xpack.stateless.lucene.SearchDirectory#retainFiles]). This maintenance work was previously gated
    /// behind [#STATELESS_CACHE_BOOST_PREFERENCE_ENABLED_SETTING]; it is now controlled independently so it can be rolled out
    /// (and, if necessary, disabled) at runtime on its own. Obsolete-region eviction keys off active/inactive regions per
    /// batched-compound-commit generation and needs neither content timestamps nor the pinned-window eviction policy, so
    /// unlike the boost-preference flag it needs no validator and a dynamic flip can never leave the cache in an invalid state.
    public static final Setting<Boolean> STATELESS_CACHE_EVICT_OBSOLETE_REGIONS_ENABLED_SETTING = Setting.boolSetting(
        "stateless.cache.evict_obsolete_regions.enabled",
        false,
        Setting.Property.OperatorDynamic,
        Setting.Property.NodeScope
    );

    // Stateless shared blob cache service populates-and-reads in-thread. And it relies on the cache service to fetch gap bytes
    // asynchronously using a CacheBlobReader.
    private static final Executor IO_EXECUTOR = EsExecutors.DIRECT_EXECUTOR_SERVICE;

    private final Executor shardReadThreadPoolExecutor;
    private final PluggableDirectoryMetricsHolder<BlobStoreCacheDirectoryMetrics> metricsHolder;
    private final boolean hasSearchRole;
    private final boolean cacheBoostPreferenceEnabled;
    private volatile boolean evictObsoleteRegionsEnabled;

    // TODO Merge the two constructors
    public StatelessSharedBlobCacheService(
        NodeEnvironment environment,
        Settings settings,
        ThreadPool threadPool,
        BlobCacheMetrics blobCacheMetrics,
        ClusterService clusterService,
        IndicesService indicesService,
        PluggableDirectoryMetricsHolder<BlobStoreCacheDirectoryMetrics> metricsHolder
    ) {
        this(
            environment,
            settings,
            threadPool,
            blobCacheMetrics,
            StatelessCacheEvictionPolicyType.createEvictionPolicy(settings, clusterService, indicesService, threadPool),
            metricsHolder
        );
    }

    // for tests
    protected StatelessSharedBlobCacheService(
        NodeEnvironment environment,
        Settings settings,
        ThreadPool threadPool,
        BlobCacheMetrics blobCacheMetrics,
        EvictionPolicy<FileCacheKey> evictionPolicy,
        PluggableDirectoryMetricsHolder<BlobStoreCacheDirectoryMetrics> metricsHolder
    ) {
        super(environment, settings, threadPool, IO_EXECUTOR, blobCacheMetrics, evictionPolicy);
        this.shardReadThreadPoolExecutor = threadPool.executor(StatelessPlugin.SHARD_READ_THREAD_POOL);
        this.metricsHolder = metricsHolder;
        this.hasSearchRole = DiscoveryNode.hasRole(settings, DiscoveryNodeRole.SEARCH_ROLE);
        this.cacheBoostPreferenceEnabled = STATELESS_CACHE_BOOST_PREFERENCE_ENABLED_SETTING.get(settings);
        this.evictObsoleteRegionsEnabled = STATELESS_CACHE_EVICT_OBSOLETE_REGIONS_ENABLED_SETTING.get(settings);
    }

    // for tests
    public StatelessSharedBlobCacheService(
        NodeEnvironment environment,
        Settings settings,
        ThreadPool threadPool,
        BlobCacheMetrics blobCacheMetrics,
        ClusterService clusterService,
        IndicesService indicesService,
        LongSupplier relativeTimeInNanosSupplier,
        PluggableDirectoryMetricsHolder<BlobStoreCacheDirectoryMetrics> metricsHolder
    ) {
        this(
            environment,
            settings,
            threadPool,
            blobCacheMetrics,
            StatelessCacheEvictionPolicyType.createEvictionPolicy(settings, clusterService, indicesService, threadPool),
            relativeTimeInNanosSupplier,
            metricsHolder
        );
    }

    // for tests
    protected StatelessSharedBlobCacheService(
        NodeEnvironment environment,
        Settings settings,
        ThreadPool threadPool,
        BlobCacheMetrics blobCacheMetrics,
        EvictionPolicy<FileCacheKey> evictionPolicy,
        LongSupplier relativeTimeInNanosSupplier,
        PluggableDirectoryMetricsHolder<BlobStoreCacheDirectoryMetrics> metricsHolder
    ) {
        super(environment, settings, threadPool, IO_EXECUTOR, blobCacheMetrics, relativeTimeInNanosSupplier, evictionPolicy);
        this.shardReadThreadPoolExecutor = EsExecutors.DIRECT_EXECUTOR_SERVICE;
        this.metricsHolder = metricsHolder;
        this.hasSearchRole = DiscoveryNode.hasRole(settings, DiscoveryNodeRole.SEARCH_ROLE);
        this.cacheBoostPreferenceEnabled = STATELESS_CACHE_BOOST_PREFERENCE_ENABLED_SETTING.get(settings);
        this.evictObsoleteRegionsEnabled = STATELESS_CACHE_EVICT_OBSOLETE_REGIONS_ENABLED_SETTING.get(settings);
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
        long timestampMillis,
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
                    timestampMillis,
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
        long timestampMillis,
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
            timestampMillis,
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

    public boolean isCacheBoostPreferenceEnabled() {
        return cacheBoostPreferenceEnabled;
    }

    /// Whether to asynchronously force-evict cache regions corresponding to obsolete segments that are not referenced anymore.
    public boolean isEvictObsoleteRegionsEnabled() {
        return evictObsoleteRegionsEnabled;
    }

    /// Enable or disable asynchronous force-eviction of cache regions corresponding to obsolete segments that are not referenced anymore.
    /// - Enabling will only kick in on the next commit notification, if there are obsolete segments, force-eviction will be queued async.
    /// - Disabling will mean commit notifications won't kick off an async force-eviction, but an existing one for the commit might
    /// already be queued/executing.
    public void setEvictObsoleteRegionsEnabled(boolean evictObsoleteRegionsEnabled) {
        this.evictObsoleteRegionsEnabled = evictObsoleteRegionsEnabled;
    }
}
