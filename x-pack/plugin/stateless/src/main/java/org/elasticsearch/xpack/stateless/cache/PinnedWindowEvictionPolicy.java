/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.cache;

import org.elasticsearch.blobcache.shared.CacheRegion;
import org.elasticsearch.blobcache.shared.EvictionPolicy;
import org.elasticsearch.blobcache.shared.SharedBlobCacheService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.telemetry.metric.ConsumingLongGaugeMetric;
import org.elasticsearch.telemetry.metric.MeterRegistry;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.stateless.lucene.FileCacheKey;

import java.util.Objects;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Predicate;

/// Eviction policy that does not evict cache regions for shards present on this node whose content timestamp
/// falls within a configurable pinned window.
///
/// Regions are classified by their [CacheRegion#timestampMillis()] (for shards present on this node):
///   - a non-negative timestamp (`>= 0`) is pinned iff it falls within the pinned window;
///   - [SharedBlobCacheService#UNKNOWN_TIMESTAMP] is always pinned (no representative timestamp);
///   - [SharedBlobCacheService#BACKFILL_IN_PROGRESS_TIMESTAMP] is always pinned until backfill completes.
///
public class PinnedWindowEvictionPolicy implements EvictionPolicy<FileCacheKey> {

    /**
     * Configures the pinned-window duration for non-evictable data when cache boost preference is enabled.
     */
    public static final Setting<TimeValue> PINNED_WINDOW_DURATION_SETTING = Setting.timeSetting(
        "stateless.cache_boost_preference.pinned_window.duration",
        TimeValue.timeValueHours(12),
        TimeValue.timeValueSeconds(1),
        Setting.Property.OperatorDynamic,
        Setting.Property.NodeScope
    );

    public static final String PINNED_METRIC = "es.blob_cache.pinned_window.pinned";
    public static final String PINNED_FREQ_0_METRIC = "es.blob_cache.pinned_window.pinned_freq_0";
    public static final String PINNED_FREQ_POSITIVE_METRIC = "es.blob_cache.pinned_window.pinned_freq_positive";
    public static final String PINNED_BACKFILL_METRIC = "es.blob_cache.pinned_window.pinned_backfill";
    public static final String PINNED_UNKNOWN_METRIC = "es.blob_cache.pinned_window.pinned_unknown";
    public static final String MINIMAL_METRIC = "es.blob_cache.pinned_window.minimal";

    private final Predicate<ShardId> hasShardPredicate;
    private final ThreadPool threadPool;

    /**
     * Gauges are registered for the lifetime of this policy but only refreshed when
     * {@link org.elasticsearch.blobcache.shared.BlobCachePeriodicMetrics} samples (via
     * {@link #updatePeriodicMetrics}). If
     * {@link SharedBlobCacheService#SHARED_CACHE_METRICS_INTERVAL_SETTING} is
     * {@link TimeValue#MINUS_ONE}, these instruments stay registered and never receive values.
     */
    private final ConsumingLongGaugeMetric pinnedMetric;
    private final ConsumingLongGaugeMetric pinnedFreq0Metric;
    private final ConsumingLongGaugeMetric pinnedFreqPositiveMetric;
    private final ConsumingLongGaugeMetric pinnedBackfillMetric;
    private final ConsumingLongGaugeMetric pinnedUnknownMetric;
    private final ConsumingLongGaugeMetric minimalMetric;

    private volatile TimeValue pinnedWindowDuration;

    private final Releasable onCloseReleasable;

    public PinnedWindowEvictionPolicy(
        ClusterSettings clusterSettings,
        ThreadPool threadPool,
        MeterRegistry meterRegistry,
        Predicate<ShardId> hasShardPredicate
    ) {
        this.hasShardPredicate = Objects.requireNonNull(hasShardPredicate);
        this.threadPool = Objects.requireNonNull(threadPool);
        Objects.requireNonNull(clusterSettings);
        Objects.requireNonNull(meterRegistry);
        this.pinnedWindowDuration = clusterSettings.get(PINNED_WINDOW_DURATION_SETTING);
        this.pinnedMetric = ConsumingLongGaugeMetric.create(
            meterRegistry,
            PINNED_METRIC,
            "Number of occupied shared blob-cache regions protected by the pinned-window eviction policy",
            "regions"
        );
        this.pinnedFreq0Metric = ConsumingLongGaugeMetric.create(
            meterRegistry,
            PINNED_FREQ_0_METRIC,
            "Number of pinned regions at LFU frequency level 0",
            "regions"
        );
        this.pinnedFreqPositiveMetric = ConsumingLongGaugeMetric.create(
            meterRegistry,
            PINNED_FREQ_POSITIVE_METRIC,
            "Number of pinned regions at a positive LFU frequency level",
            "regions"
        );
        this.pinnedBackfillMetric = ConsumingLongGaugeMetric.create(
            meterRegistry,
            PINNED_BACKFILL_METRIC,
            "Number of pinned regions with a backfill-in-progress timestamp",
            "regions"
        );
        this.pinnedUnknownMetric = ConsumingLongGaugeMetric.create(
            meterRegistry,
            PINNED_UNKNOWN_METRIC,
            "Number of pinned regions with an unknown timestamp",
            "regions"
        );
        this.minimalMetric = ConsumingLongGaugeMetric.create(
            meterRegistry,
            MINIMAL_METRIC,
            "Number of occupied regions carrying the minimal cache timestamp",
            "regions"
        );
        this.onCloseReleasable = Releasables.releaseOnce(
            Releasables.wrap(
                clusterSettings.addRemovableSettingsUpdateConsumer(
                    PINNED_WINDOW_DURATION_SETTING,
                    value -> this.pinnedWindowDuration = value
                ),
                () -> {
                    pinnedMetric.gauge().close();
                    pinnedFreq0Metric.gauge().close();
                    pinnedFreqPositiveMetric.gauge().close();
                    pinnedBackfillMetric.gauge().close();
                    pinnedUnknownMetric.gauge().close();
                    minimalMetric.gauge().close();
                }
            )
        );
    }

    public TimeValue getPinnedWindowDuration() {
        return pinnedWindowDuration;
    }

    /**
     * Returns {@code true} if the shard is present on this node.
     */
    protected boolean hasShard(ShardId shardId) {
        return hasShardPredicate.test(shardId);
    }

    protected long currentTimeMillis() {
        return threadPool.absoluteTimeInMillis();
    }

    /**
     * Returns {@code true} if {@code region} is currently protected from eviction by this policy based on its own
     * state (independent of any incoming region).
     */
    boolean isProtected(CacheRegion<FileCacheKey> region, long pinnedWindowCutoffMillis) {
        if (hasShard(region.key().shardId()) == false) {
            return false;
        }
        final long timestampMillis = region.timestampMillis();
        if (timestampMillis < 0) {
            assert timestampMillis == SharedBlobCacheService.BACKFILL_IN_PROGRESS_TIMESTAMP
                || timestampMillis == SharedBlobCacheService.UNKNOWN_TIMESTAMP : "unexpected negative timestamp: " + timestampMillis;
            return true;
        }
        // TODO: regions of unboosted shards, and of shards with a boost multiplier of less than 1, should be
        // evicted irrespective of their timestamp.
        return timestampMillis >= pinnedWindowCutoffMillis;
    }

    @Override
    public Predicate<CacheRegion<FileCacheKey>> createPredicate(CacheRegion<FileCacheKey> incoming) {
        final long pinnedWindowCutoffMillis = currentTimeMillis() - pinnedWindowDuration.getMillis();
        return region -> isProtected(region, pinnedWindowCutoffMillis) == false;
    }

    @Override
    public void onCached(CacheRegion<FileCacheKey> region) {}

    @Override
    public void onEvicted(CacheRegion<FileCacheKey> region) {}

    @Override
    public void updatePeriodicMetrics(Consumer<BiConsumer<CacheRegion<FileCacheKey>, Integer>> regions) {
        final long[] pinned = new long[1];
        final long[] pinnedFreq0 = new long[1];
        final long[] pinnedFreqPositive = new long[1];
        final long[] pinnedBackfill = new long[1];
        final long[] pinnedUnknown = new long[1];
        final long[] minimalTimestamp = new long[1];
        final long pinnedWindowCutoffMillis = currentTimeMillis() - pinnedWindowDuration.getMillis();
        regions.accept((region, freq) -> {
            final long timestampMillis = region.timestampMillis();
            if (timestampMillis == SharedBlobCacheService.MINIMAL_CACHE_TIMESTAMP) {
                minimalTimestamp[0]++;
            }
            if (isProtected(region, pinnedWindowCutoffMillis) == false) {
                return;
            }
            pinned[0]++;
            if (freq == 0) {
                pinnedFreq0[0]++;
            } else {
                assert freq > 0 : freq;
                pinnedFreqPositive[0]++;
            }
            if (timestampMillis == SharedBlobCacheService.BACKFILL_IN_PROGRESS_TIMESTAMP) {
                pinnedBackfill[0]++;
            } else if (timestampMillis == SharedBlobCacheService.UNKNOWN_TIMESTAMP) {
                pinnedUnknown[0]++;
            }
        });
        pinnedMetric.set(pinned[0]);
        pinnedFreq0Metric.set(pinnedFreq0[0]);
        pinnedFreqPositiveMetric.set(pinnedFreqPositive[0]);
        pinnedBackfillMetric.set(pinnedBackfill[0]);
        pinnedUnknownMetric.set(pinnedUnknown[0]);
        minimalMetric.set(minimalTimestamp[0]);
    }

    @Override
    public void close() {
        Releasables.close(onCloseReleasable);
    }
}
