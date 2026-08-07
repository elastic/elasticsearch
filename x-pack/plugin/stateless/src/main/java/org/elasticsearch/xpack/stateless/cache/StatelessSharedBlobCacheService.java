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
import org.elasticsearch.blobcache.shared.CacheRegion;
import org.elasticsearch.blobcache.shared.DefaultEvictionPolicy;
import org.elasticsearch.blobcache.shared.EvictionPolicy;
import org.elasticsearch.blobcache.shared.SharedBlobCacheService;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.time.TimeProvider;
import org.elasticsearch.common.unit.RatioValue;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.Predicates;
import org.elasticsearch.core.Strings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.index.store.PluggableDirectoryMetricsHolder;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
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
import java.util.function.Predicate;
import java.util.function.Supplier;

public class StatelessSharedBlobCacheService extends SharedBlobCacheService<FileCacheKey> {

    private static final Logger logger = LogManager.getLogger(StatelessSharedBlobCacheService.class);

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
     * On search nodes, an explicit value takes precedence even when boost preference is disabled. When unset, defaults to
     * {@link StatelessCacheEvictionPolicyType#ALWAYS} when {@link #STATELESS_CACHE_BOOST_PREFERENCE_ENABLED_SETTING} is disabled,
     * and to {@link StatelessCacheEvictionPolicyType#PINNED_WINDOW} when enabled.
     * This setting is ignored on non-search nodes, which always use {@link StatelessCacheEvictionPolicyType#ALWAYS}.
     */
    public static final Setting<StatelessCacheEvictionPolicyType> STATELESS_CACHE_BOOST_PREFERENCE_EVICTION_POLICY_SEARCH_SETTING = Setting
        .enumSetting(
            StatelessCacheEvictionPolicyType.class,
            settings -> StatelessCacheEvictionPolicyType.defaultEvictionPolicyType(settings).name(),
            "stateless.cache_boost_preference.eviction_policy.search",
            s -> {},
            Setting.Property.OperatorDynamic,
            Setting.Property.NodeScope
        );

    /**
     * Whether time-based search shards should stamp metadata-read cache regions with
     * {@link SharedBlobCacheService#BACKFILL_IN_PROGRESS_TIMESTAMP} and run completion backfill.
     */
    public static final Setting<Boolean> STATELESS_CACHE_BOOST_PREFERENCE_TIMESTAMP_BACKFILL_ENABLED_SETTING = Setting.boolSetting(
        "stateless.cache_boost_preference.timestamp_backfill.enabled",
        settings -> Boolean.toString(
            STATELESS_CACHE_BOOST_PREFERENCE_EVICTION_POLICY_SEARCH_SETTING.get(settings) == StatelessCacheEvictionPolicyType.PINNED_WINDOW
        ),
        Setting.Property.OperatorDynamic,
        Setting.Property.NodeScope
    );

    /**
     * Fraction of total regions that must be consecutively rejected by the eviction policy within a single eviction
     * scan before the cache enters a node-wide eviction degradation period. When {@code rejectedCount / numRegions} exceeds
     * this ratio the policy is bypassed for the duration of {@link #STATELESS_CACHE_EVICTION_POLICY_DEGRADATION_DURATION_SETTING}.
     * Note this setting is only relevant when the eviction policy does reject eviction. For example, the default
     * {@link DefaultEvictionPolicy} does not reject eviction and so this setting is effectively ignored.
     */
    public static final Setting<RatioValue> STATELESS_CACHE_EVICTION_POLICY_DEGRADATION_THRESHOLD_SETTING = new Setting<>(
        "stateless.cache_boost_preference.eviction_policy_degradation.threshold",
        "95%",
        RatioValue::parseRatioValue,
        Setting.Property.NodeScope
    );

    /**
     * Duration of the eviction degradation period. While active, the eviction policy is bypassed. A zero value disables degradation.
     * Set to a non-zero duration together with a threshold below {@code 100%} to fully enable degradation mode.
     * Note this setting is only relevant when the eviction policy does reject eviction. For example, the default
     * {@link DefaultEvictionPolicy} does not reject eviction and so this setting is effectively ignored.
     */
    public static final Setting<TimeValue> STATELESS_CACHE_EVICTION_POLICY_DEGRADATION_DURATION_SETTING = Setting.timeSetting(
        "stateless.cache_boost_preference.eviction_policy_degradation.duration",
        TimeValue.timeValueMinutes(5),
        TimeValue.ZERO,
        Setting.Property.NodeScope
    );

    /// Setting gating eviction of cache regions that belong to obsolete segments on search directories (see
    /// [org.elasticsearch.xpack.stateless.lucene.SearchDirectory#retainFiles]). This maintenance work was previously gated
    /// behind [#STATELESS_CACHE_BOOST_PREFERENCE_ENABLED_SETTING]; it is now controlled independently so it can be rolled out
    /// (and, if necessary, disabled) at runtime on its own. Obsolete-region eviction keys off active/inactive regions per
    /// batched-compound-commit generation and needs neither content timestamps nor the pinned-window eviction policy, so
    /// unlike the boost-preference flag it needs no validator and a dynamic flip can never leave the cache in an invalid state.
    /// Defaults to [#STATELESS_CACHE_BOOST_PREFERENCE_ENABLED_SETTING], but an explicit value wins.
    public static final Setting<Boolean> STATELESS_CACHE_EVICT_OBSOLETE_REGIONS_ENABLED_SETTING = Setting.boolSetting(
        "stateless.cache.evict_obsolete_regions.enabled",
        STATELESS_CACHE_BOOST_PREFERENCE_ENABLED_SETTING,
        Setting.Property.OperatorDynamic,
        Setting.Property.NodeScope
    );

    /// Setting gating demotion of a closed shard's cache regions (see [SharedBlobCacheService#demoteAllAsync]). Any shard leaving this
    /// node closes its store, and will have its regions move to the front of the frequency-0 queue rather than
    /// being evicted, so they are the first eviction candidates while remaining usable if the shard relocates and relocates back.
    /// Index deletion and node shutdown are handled separately.
    /// A flip takes effect on the next store close; a demotion already submitted still runs.
    /// Defaults to [#STATELESS_CACHE_BOOST_PREFERENCE_ENABLED_SETTING], but an explicit value wins.
    public static final Setting<Boolean> STATELESS_CACHE_DEMOTE_CLOSED_SHARD_REGIONS_ENABLED_SETTING = Setting.boolSetting(
        "stateless.cache.demote_closed_shard_regions.enabled",
        STATELESS_CACHE_BOOST_PREFERENCE_ENABLED_SETTING,
        Setting.Property.OperatorDynamic,
        Setting.Property.NodeScope
    );

    /// Setting gating force-eviction of a deleted index's cache regions (see [SharedBlobCacheService#forceEvictAsync]). The regions of a
    /// deleted index can never be read again, so they are dropped as soon as the index is removed rather than left for the LFU to
    /// reclaim. A flip takes effect on the next index removal; an eviction already submitted still runs.
    /// Defaults to [#STATELESS_CACHE_BOOST_PREFERENCE_ENABLED_SETTING], but an explicit value wins.
    public static final Setting<Boolean> STATELESS_CACHE_EVICT_DELETED_INDEX_REGIONS_ENABLED_SETTING = Setting.boolSetting(
        "stateless.cache.evict_deleted_index_regions.enabled",
        STATELESS_CACHE_BOOST_PREFERENCE_ENABLED_SETTING,
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
    private volatile boolean metadataTimestampBackfillEnabled;
    private volatile boolean evictObsoleteRegionsEnabled;
    private volatile boolean demoteClosedShardRegionsEnabled;
    private volatile boolean evictDeletedIndexRegionsEnabled;

    private final int evictionDegradationThreshold;
    private final long evictionDegradationDurationMillis;
    private volatile long evictionDegradationStartMillis = -1L;

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
            clusterService.getClusterSettings(),
            threadPool,
            blobCacheMetrics,
            createEvictionPolicy(settings, clusterService, indicesService, threadPool),
            System::nanoTime,
            threadPool.executor(StatelessPlugin.SHARD_READ_THREAD_POOL),
            metricsHolder
        );
    }

    /// The constructor the public one delegates to, and for tests that want to alter/inject behavior.
    protected StatelessSharedBlobCacheService(
        NodeEnvironment environment,
        Settings settings,
        ClusterSettings clusterSettings,
        ThreadPool threadPool,
        BlobCacheMetrics blobCacheMetrics,
        EvictionPolicy<FileCacheKey> evictionPolicy,
        LongSupplier relativeTimeInNanosSupplier,
        Executor shardReadThreadPoolExecutor,
        PluggableDirectoryMetricsHolder<BlobStoreCacheDirectoryMetrics> metricsHolder
    ) {
        super(environment, settings, threadPool, IO_EXECUTOR, blobCacheMetrics, relativeTimeInNanosSupplier, evictionPolicy);
        this.shardReadThreadPoolExecutor = shardReadThreadPoolExecutor;
        this.metricsHolder = metricsHolder;
        this.hasSearchRole = DiscoveryNode.hasRole(settings, DiscoveryNodeRole.SEARCH_ROLE);
        this.cacheBoostPreferenceEnabled = STATELESS_CACHE_BOOST_PREFERENCE_ENABLED_SETTING.get(settings);
        this.evictionDegradationThreshold = (int) (numRegions * STATELESS_CACHE_EVICTION_POLICY_DEGRADATION_THRESHOLD_SETTING.get(settings)
            .getAsRatio());
        this.evictionDegradationDurationMillis = STATELESS_CACHE_EVICTION_POLICY_DEGRADATION_DURATION_SETTING.get(settings).millis();
        assert evictionDegradationThreshold >= 0 && evictionDegradationThreshold <= numRegions
            : evictionDegradationThreshold + " not in [0," + numRegions + "]";
        assert evictionDegradationDurationMillis >= 0 : evictionDegradationDurationMillis + " < 0";
        clusterSettings.initializeAndWatch(
            STATELESS_CACHE_BOOST_PREFERENCE_TIMESTAMP_BACKFILL_ENABLED_SETTING,
            enabled -> this.metadataTimestampBackfillEnabled = enabled
        );
        clusterSettings.initializeAndWatch(
            STATELESS_CACHE_EVICT_OBSOLETE_REGIONS_ENABLED_SETTING,
            enabled -> this.evictObsoleteRegionsEnabled = enabled
        );
        clusterSettings.initializeAndWatch(
            STATELESS_CACHE_DEMOTE_CLOSED_SHARD_REGIONS_ENABLED_SETTING,
            enabled -> this.demoteClosedShardRegionsEnabled = enabled
        );
        clusterSettings.initializeAndWatch(
            STATELESS_CACHE_EVICT_DELETED_INDEX_REGIONS_ENABLED_SETTING,
            enabled -> this.evictDeletedIndexRegionsEnabled = enabled
        );
        assert this.rangeSize >= this.regionSize : this.rangeSize + " < " + this.regionSize;
    }

    // package private for testing
    static EvictionPolicy<FileCacheKey> createEvictionPolicy(
        Settings settings,
        ClusterService clusterService,
        IndicesService indicesService,
        TimeProvider timeProvider
    ) {
        if (DiscoveryNode.hasRole(settings, DiscoveryNodeRole.SEARCH_ROLE)) {
            return new SwitchingEvictionPolicy(settings, clusterService, indicesService, timeProvider);
        } else {
            return StatelessCacheEvictionPolicyType.createEvictionPolicy(settings, clusterService, indicesService, timeProvider);
        }
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

    @Override
    protected Predicate<CacheRegion<FileCacheKey>> createEvictionPredicate(
        EvictionPolicy<FileCacheKey> evictionPolicy,
        CacheRegion<FileCacheKey> incoming
    ) {
        if (evictionDegradationThreshold == numRegions || evictionDegradationDurationMillis == 0) {
            // Degradation is disabled, just use the eviction policy's predicate directly.
            return super.createEvictionPredicate(evictionPolicy, incoming);
        }

        final long startMillis = evictionDegradationStartMillis;
        if (startMillis >= 0 && threadPool.absoluteTimeInMillis() - startMillis < evictionDegradationDurationMillis) {
            // In the degradation period, bypass the eviction policy. This checked once before creating the predicate, which
            // means it does not detect degradation triggered by another thread. This thread can still on its own trigger
            // degradation.
            return Predicates.always();
        }

        final Predicate<CacheRegion<FileCacheKey>> policyPredicate = evictionPolicy.createPredicate(incoming);
        return new Predicate<>() {
            // NOTE that the counter assumes **single** thread usage.
            int rejectedCount = 0;

            @Override
            public boolean test(CacheRegion<FileCacheKey> region) {
                if (rejectedCount > evictionDegradationThreshold) {
                    return true;
                }
                if (policyPredicate.test(region)) {
                    return true;
                }
                if (++rejectedCount > evictionDegradationThreshold) {
                    assert rejectedCount == evictionDegradationThreshold + 1 : rejectedCount + " !=" + (evictionDegradationThreshold + 1);
                    // There could be races in setting the start time. It is ok since it does not have to be super accurate.
                    evictionDegradationStartMillis = threadPool.absoluteTimeInMillis();
                    logger.warn(
                        "Eviction policy degraded: policy rejected over [{}/{}] regions; bypassing policy for {}",
                        evictionDegradationThreshold,
                        numRegions,
                        TimeValue.timeValueMillis(evictionDegradationDurationMillis)
                    );
                    return true;
                }
                return false;
            }
        };
    }

    public PluggableDirectoryMetricsHolder<BlobStoreCacheDirectoryMetrics> metricsHolder() {
        return metricsHolder;
    }

    public boolean isCacheBoostPreferenceEnabled() {
        return cacheBoostPreferenceEnabled;
    }

    /**
     * Whether time-based shards should use metadata-read timestamp backfill (sentinel stamping followed by completion backfill).
     */
    public boolean isMetadataTimestampBackfillEnabled() {
        return metadataTimestampBackfillEnabled;
    }

    /// Whether to asynchronously force-evict cache regions corresponding to obsolete segments that are not referenced anymore.
    public boolean isEvictObsoleteRegionsEnabled() {
        return evictObsoleteRegionsEnabled;
    }

    /// Whether to asynchronously demote the cache regions of a shard whose store was closed, making them the first eviction candidates.
    public boolean isDemoteClosedShardRegionsEnabled() {
        return demoteClosedShardRegionsEnabled;
    }

    /// Whether to asynchronously force-evict the cache regions of a deleted index's shards.
    public boolean isEvictDeletedIndexRegionsEnabled() {
        return evictDeletedIndexRegionsEnabled;
    }
}
