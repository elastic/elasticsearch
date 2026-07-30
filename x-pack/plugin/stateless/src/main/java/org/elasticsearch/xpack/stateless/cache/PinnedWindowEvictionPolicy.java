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
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.stateless.lucene.FileCacheKey;

import java.util.Map;
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

    public static final String PINNED_ATTRIBUTE_KEY = "pinned";
    public static final String PINNED_PERCENT_ATTRIBUTE_KEY = "pinned_percent";
    public static final String BACKFILL_IN_PROGRESS_ATTRIBUTE_KEY = "backfill";
    public static final String BACKFILL_PERCENT_ATTRIBUTE_KEY = "backfill_percent";
    public static final String UNKNOWN_TIMESTAMP_ATTRIBUTE_KEY = "unknown";
    public static final String UNKNOWN_PERCENT_ATTRIBUTE_KEY = "unknown_percent";
    public static final String MINIMAL_TIMESTAMP_ATTRIBUTE_KEY = "minimal";
    public static final String MINIMAL_PERCENT_ATTRIBUTE_KEY = "minimal_percent";
    /// Pinned regions at LFU frequency level 0 (least recently / about to be decayed).
    public static final String FREQ_LOW_ATTRIBUTE_KEY = "freq_low";
    public static final String FREQ_LOW_PERCENT_ATTRIBUTE_KEY = "freq_low_percent";
    /// Pinned regions in the next ~20% of frequency levels (1...midBucketMaxInclusive(maxFreq)).
    public static final String FREQ_MID_ATTRIBUTE_KEY = "freq_mid";
    public static final String FREQ_MID_PERCENT_ATTRIBUTE_KEY = "freq_mid_percent";
    /// Pinned regions in the remaining ~80% of frequency levels.
    public static final String FREQ_HIGH_ATTRIBUTE_KEY = "freq_high";
    public static final String FREQ_HIGH_PERCENT_ATTRIBUTE_KEY = "freq_high_percent";

    private final Predicate<ShardId> hasShardPredicate;
    private final ThreadPool threadPool;
    private final int maxFreq;

    private volatile TimeValue pinnedWindowDuration;

    private final Releasable releaseSettingsUpdaters;

    public PinnedWindowEvictionPolicy(ClusterSettings clusterSettings, ThreadPool threadPool, Predicate<ShardId> hasShardPredicate) {
        this.hasShardPredicate = Objects.requireNonNull(hasShardPredicate);
        this.threadPool = Objects.requireNonNull(threadPool);
        Objects.requireNonNull(clusterSettings);
        this.pinnedWindowDuration = clusterSettings.get(PINNED_WINDOW_DURATION_SETTING);
        this.maxFreq = clusterSettings.get(SharedBlobCacheService.SHARED_CACHE_MAX_FREQ_SETTING);
        this.releaseSettingsUpdaters = Releasables.releaseOnce(
            clusterSettings.addRemovableSettingsUpdateConsumer(PINNED_WINDOW_DURATION_SETTING, value -> this.pinnedWindowDuration = value)
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
    public Map<String, Object> metricAttributes(Consumer<BiConsumer<CacheRegion<FileCacheKey>, Integer>> regions, int numRegions) {
        if (numRegions <= 0) {
            return Map.of();
        }
        final int midBucketMaxInclusive = midBucketMaxInclusive(maxFreq);
        final long[] pinned = new long[1];
        final long[] backfillInProgress = new long[1];
        final long[] unknownTimestamp = new long[1];
        final long[] minimalTimestamp = new long[1];
        final long[] freqLow = new long[1];
        final long[] freqMid = new long[1];
        final long[] freqHigh = new long[1];
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
            if (timestampMillis == SharedBlobCacheService.BACKFILL_IN_PROGRESS_TIMESTAMP) {
                backfillInProgress[0]++;
            } else if (timestampMillis == SharedBlobCacheService.UNKNOWN_TIMESTAMP) {
                unknownTimestamp[0]++;
            }
            if (freq <= 0) {
                freqLow[0]++;
            } else if (freq <= midBucketMaxInclusive) {
                freqMid[0]++;
            } else {
                freqHigh[0]++;
            }
        });
        return Map.ofEntries(
            Map.entry(PINNED_ATTRIBUTE_KEY, pinned[0]),
            Map.entry(PINNED_PERCENT_ATTRIBUTE_KEY, toPercent(pinned[0], numRegions)),
            Map.entry(BACKFILL_IN_PROGRESS_ATTRIBUTE_KEY, backfillInProgress[0]),
            Map.entry(BACKFILL_PERCENT_ATTRIBUTE_KEY, toPercent(backfillInProgress[0], numRegions)),
            Map.entry(UNKNOWN_TIMESTAMP_ATTRIBUTE_KEY, unknownTimestamp[0]),
            Map.entry(UNKNOWN_PERCENT_ATTRIBUTE_KEY, toPercent(unknownTimestamp[0], numRegions)),
            Map.entry(MINIMAL_TIMESTAMP_ATTRIBUTE_KEY, minimalTimestamp[0]),
            Map.entry(MINIMAL_PERCENT_ATTRIBUTE_KEY, toPercent(minimalTimestamp[0], numRegions)),
            Map.entry(FREQ_LOW_ATTRIBUTE_KEY, freqLow[0]),
            Map.entry(FREQ_LOW_PERCENT_ATTRIBUTE_KEY, toPercent(freqLow[0], numRegions)),
            Map.entry(FREQ_MID_ATTRIBUTE_KEY, freqMid[0]),
            Map.entry(FREQ_MID_PERCENT_ATTRIBUTE_KEY, toPercent(freqMid[0], numRegions)),
            Map.entry(FREQ_HIGH_ATTRIBUTE_KEY, freqHigh[0]),
            Map.entry(FREQ_HIGH_PERCENT_ATTRIBUTE_KEY, toPercent(freqHigh[0], numRegions))
        );
    }

    /**
     * Upper inclusive frequency for the mid bucket: ~20% of levels after level 0.
     * Levels are {@code [0, maxFreq)}. Level 0 is the low bucket; {@code 1..result} is mid; the rest is high.
     */
    static int midBucketMaxInclusive(int maxFreq) {
        assert maxFreq >= 1 : maxFreq;
        if (maxFreq == 1) {
            // Only level 0 exists; mid/high buckets stay empty.
            return 0;
        }
        return Math.max(1, (maxFreq - 1) * 20 / 100);
    }

    /**
     * Converts a count to an integer percent of {@code numRegions}, rounding up so any non-zero count is at least
     * {@code 1}. Caps at {@code 100} so label values' cardinality stays bounded.
     */
    static long toPercent(long count, int numRegions) {
        assert numRegions > 0 : numRegions;
        if (count <= 0L) {
            return 0L;
        }
        return Math.min(100L, Math.max(1L, (count * 100L + numRegions - 1L) / numRegions));
    }

    @Override
    public void close() {
        Releasables.close(releaseSettingsUpdaters);
    }
}
