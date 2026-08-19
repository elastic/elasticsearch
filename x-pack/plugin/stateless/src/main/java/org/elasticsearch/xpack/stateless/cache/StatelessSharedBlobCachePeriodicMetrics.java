/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.cache;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.blobcache.shared.CacheRegion;
import org.elasticsearch.blobcache.shared.EvictionPolicy;
import org.elasticsearch.blobcache.shared.SharedBlobCacheService;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.telemetry.metric.ConsumingLongGaugeMetric;
import org.elasticsearch.telemetry.metric.MeterRegistry;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.Objects;
import java.util.function.Consumer;

/**
 * Periodically samples shared blob-cache occupancy and eviction-policy protection gauges.
 * <p>
 * Occupancy and protection breakdowns are counted in a single walk of occupied regions,
 * using {@link EvictionPolicy#isProtected} so metrics work for any active policy (including
 * when the default policy is selected, in which case protected counts stay at zero).
 */
public final class StatelessSharedBlobCachePeriodicMetrics extends AbstractLifecycleComponent {

    public static final TimeValue MIN_METRICS_INTERVAL = TimeValue.timeValueSeconds(1L);

    private static final String METRICS_INTERVAL_SETTING_KEY = "stateless.cache.metrics_interval";
    private static final Logger logger = LogManager.getLogger(StatelessSharedBlobCachePeriodicMetrics.class);

    /**
     * How often this component will sample. A value of {@link TimeValue#MINUS_ONE} disables sampling.
     * Enabled intervals must be at least {@link #MIN_METRICS_INTERVAL}.
     * <p>
     * Defaults to 3 minutes when
     * {@link StatelessSharedBlobCacheService#STATELESS_CACHE_BOOST_PREFERENCE_EVICTION_POLICY_SEARCH_SETTING}
     * resolves to {@link StatelessCacheEvictionPolicyType#PINNED_WINDOW}, otherwise {@link TimeValue#MINUS_ONE}
     * (disabled), so sampling is on by default only when the pinned-window policy these metrics primarily
     * illuminate is selected. When this setting is unset, that default is re-evaluated on dynamic updates of the
     * eviction-policy setting (and gauges are cleared when sampling becomes disabled).
     * <p>
     * Minutes frequency is cheap even at large cache sizes: a full sample walks occupied regions once without holding the
     * cache monitor. At a 2TiB cache with 16MiB regions that is at most ~131k entries of field reads and map lookups.
     */
    public static final Setting<TimeValue> METRICS_INTERVAL_SETTING = Setting.timeSetting(
        METRICS_INTERVAL_SETTING_KEY,
        settings -> StatelessSharedBlobCacheService.STATELESS_CACHE_BOOST_PREFERENCE_EVICTION_POLICY_SEARCH_SETTING.get(
            settings
        ) == StatelessCacheEvictionPolicyType.PINNED_WINDOW ? TimeValue.timeValueMinutes(3) : TimeValue.MINUS_ONE,
        value -> {
            if (TimeValue.MINUS_ONE.equals(value) == false && value.compareTo(MIN_METRICS_INTERVAL) < 0) {
                throw new IllegalArgumentException(
                    "failed to parse value ["
                        + value.getStringRep()
                        + "] for setting ["
                        + METRICS_INTERVAL_SETTING_KEY
                        + "], must be ["
                        + TimeValue.MINUS_ONE.getStringRep()
                        + "] to disable or >= ["
                        + MIN_METRICS_INTERVAL.getStringRep()
                        + "]"
                );
            }
        },
        Setting.Property.OperatorDynamic,
        Setting.Property.NodeScope
    );

    public static final String BLOB_CACHE_REGIONS_FILLED = "es.blob_cache.regions.filled.current";
    /**
     * Although a fixed value, it is primarily helpful for calculating percentages in Observability.
     */
    public static final String BLOB_CACHE_REGIONS_TOTAL = "es.blob_cache.regions.total.current";

    /**
     * Counts occupied regions for which {@link EvictionPolicy#isProtected} is true.
     */
    public static final String PROTECTED_METRIC = "es.blob_cache.regions.protected.current";
    /**
     * Counts protected regions at LFU frequency level 0. Together with
     * {@link #PROTECTED_FREQ_POSITIVE_METRIC}, partitions {@link #PROTECTED_METRIC}.
     */
    public static final String PROTECTED_FREQ_0_METRIC = "es.blob_cache.regions.protected.freq_0.current";
    /**
     * Counts protected regions at a positive LFU frequency level. Together with
     * {@link #PROTECTED_FREQ_0_METRIC}, partitions {@link #PROTECTED_METRIC}.
     */
    public static final String PROTECTED_FREQ_POSITIVE_METRIC = "es.blob_cache.regions.protected.freq_positive.current";
    /**
     * Counts occupied regions with {@link SharedBlobCacheService#BACKFILL_IN_PROGRESS_TIMESTAMP}, independent of
     * eviction-policy protection.
     */
    public static final String BACKFILL_METRIC = "es.blob_cache.regions.backfill_timestamp.current";
    /**
     * Counts occupied regions with {@link SharedBlobCacheService#UNKNOWN_TIMESTAMP}, independent of
     * eviction-policy protection.
     */
    public static final String UNKNOWN_METRIC = "es.blob_cache.regions.unknown_timestamp.current";
    /**
     * Counts occupied regions with {@link SharedBlobCacheService#MINIMAL_CACHE_TIMESTAMP}, independent of
     * eviction-policy protection.
     */
    public static final String MINIMAL_METRIC = "es.blob_cache.regions.minimal_timestamp.current";

    private final SharedBlobCacheService<?> cacheService;
    private final ThreadPool threadPool;
    private final MeterRegistry meterRegistry;
    private final Releasable removeSettingsUpdater;

    private volatile TimeValue metricsInterval;
    private Scheduler.Cancellable metricsTask;
    private final SetOnce<ConsumingLongGaugeMetric> filledRegionsMetric = new SetOnce<>();
    private final SetOnce<ConsumingLongGaugeMetric> totalRegionsMetric = new SetOnce<>();
    private final SetOnce<ConsumingLongGaugeMetric> protectedMetric = new SetOnce<>();
    private final SetOnce<ConsumingLongGaugeMetric> protectedFreq0Metric = new SetOnce<>();
    private final SetOnce<ConsumingLongGaugeMetric> protectedFreqPositiveMetric = new SetOnce<>();
    private final SetOnce<ConsumingLongGaugeMetric> backfillMetric = new SetOnce<>();
    private final SetOnce<ConsumingLongGaugeMetric> unknownMetric = new SetOnce<>();
    private final SetOnce<ConsumingLongGaugeMetric> minimalMetric = new SetOnce<>();

    public StatelessSharedBlobCachePeriodicMetrics(
        SharedBlobCacheService<?> cacheService,
        ClusterSettings clusterSettings,
        ThreadPool threadPool,
        MeterRegistry meterRegistry
    ) {
        this.cacheService = Objects.requireNonNull(cacheService);
        this.threadPool = Objects.requireNonNull(threadPool);
        this.meterRegistry = Objects.requireNonNull(meterRegistry);
        Objects.requireNonNull(clusterSettings);
        this.metricsInterval = clusterSettings.get(METRICS_INTERVAL_SETTING);
        this.removeSettingsUpdater = Releasables.releaseOnce(
            clusterSettings.addRemovableSettingsUpdateConsumer(METRICS_INTERVAL_SETTING, this::onMetricsIntervalChanged)
        );
    }

    private void onMetricsIntervalChanged(TimeValue newInterval) {
        synchronized (this) {
            this.metricsInterval = newInterval;
            if (lifecycle.started()) {
                reschedule();
            }
        }
    }

    /**
     * Cancels any in-flight fixed-delay task and, when sampling is enabled, schedules a new one.
     */
    private void reschedule() {
        assert Thread.holdsLock(this);
        if (metricsTask != null) {
            metricsTask.cancel();
            metricsTask = null;
        }
        if (TimeValue.MINUS_ONE.equals(metricsInterval)) {
            // Keep instruments registered but publish zeros so Observability does not retain a stale last sample.
            clearGauges();
            return;
        }
        ensureGaugesRegistered();
        metricsTask = threadPool.scheduleWithFixedDelay(this::sample, metricsInterval, threadPool.generic());
    }

    private void ensureGaugesRegistered() {
        assert Thread.holdsLock(this);
        if (filledRegionsMetric.get() != null) {
            return;
        }
        filledRegionsMetric.set(
            ConsumingLongGaugeMetric.create(
                meterRegistry,
                BLOB_CACHE_REGIONS_FILLED,
                "The number of occupied shared blob-cache regions",
                "regions"
            )
        );
        totalRegionsMetric.set(
            ConsumingLongGaugeMetric.create(
                meterRegistry,
                BLOB_CACHE_REGIONS_TOTAL,
                "The total number of shared blob-cache region slots (cache capacity)",
                "regions"
            )
        );
        protectedMetric.set(
            ConsumingLongGaugeMetric.create(
                meterRegistry,
                PROTECTED_METRIC,
                "Number of occupied shared blob-cache regions protected by the active eviction policy",
                "regions"
            )
        );
        protectedFreq0Metric.set(
            ConsumingLongGaugeMetric.create(
                meterRegistry,
                PROTECTED_FREQ_0_METRIC,
                "Number of protected regions at LFU frequency level 0",
                "regions"
            )
        );
        protectedFreqPositiveMetric.set(
            ConsumingLongGaugeMetric.create(
                meterRegistry,
                PROTECTED_FREQ_POSITIVE_METRIC,
                "Number of protected regions at a positive LFU frequency level",
                "regions"
            )
        );
        backfillMetric.set(
            ConsumingLongGaugeMetric.create(
                meterRegistry,
                BACKFILL_METRIC,
                "Number of occupied regions with a backfill-in-progress timestamp",
                "regions"
            )
        );
        unknownMetric.set(
            ConsumingLongGaugeMetric.create(
                meterRegistry,
                UNKNOWN_METRIC,
                "Number of occupied regions with an unknown timestamp",
                "regions"
            )
        );
        minimalMetric.set(
            ConsumingLongGaugeMetric.create(
                meterRegistry,
                MINIMAL_METRIC,
                "Number of occupied regions carrying the minimal cache timestamp",
                "regions"
            )
        );
    }

    private void clearGauges() {
        assert Thread.holdsLock(this);
        final ConsumingLongGaugeMetric filled = filledRegionsMetric.get();
        if (filled == null) {
            return;
        }
        filled.set(0);
        totalRegionsMetric.get().set(0);
        protectedMetric.get().set(0);
        protectedFreq0Metric.get().set(0);
        protectedFreqPositiveMetric.get().set(0);
        backfillMetric.get().set(0);
        unknownMetric.get().set(0);
        minimalMetric.get().set(0);
    }

    private void sample() {
        // Best-effort: skip in-flight ticks that race with stop/close.
        if (lifecycle.started() == false) {
            return;
        }
        final ConsumingLongGaugeMetric filled = filledRegionsMetric.get();
        final ConsumingLongGaugeMetric total = totalRegionsMetric.get();
        final ConsumingLongGaugeMetric protectedRegions = protectedMetric.get();
        final ConsumingLongGaugeMetric protectedFreq0 = protectedFreq0Metric.get();
        final ConsumingLongGaugeMetric protectedFreqPositive = protectedFreqPositiveMetric.get();
        final ConsumingLongGaugeMetric backfill = backfillMetric.get();
        final ConsumingLongGaugeMetric unknown = unknownMetric.get();
        final ConsumingLongGaugeMetric minimal = minimalMetric.get();
        assert filled != null;
        assert total != null;
        assert protectedRegions != null;
        assert protectedFreq0 != null;
        assert protectedFreqPositive != null;
        assert backfill != null;
        assert unknown != null;
        assert minimal != null;
        total.set(cacheService.getStats().numberOfRegions());
        sampleRegions(cacheService, filled, protectedRegions, protectedFreq0, protectedFreqPositive, backfill, unknown, minimal);
    }

    /**
     * Walks occupied regions once to publish occupancy and protection gauges.
     */
    private static <KeyType extends SharedBlobCacheService.KeyBase> void sampleRegions(
        SharedBlobCacheService<KeyType> cacheService,
        ConsumingLongGaugeMetric filledMetric,
        ConsumingLongGaugeMetric protectedMetric,
        ConsumingLongGaugeMetric protectedFreq0Metric,
        ConsumingLongGaugeMetric protectedFreqPositiveMetric,
        ConsumingLongGaugeMetric backfillMetric,
        ConsumingLongGaugeMetric unknownMetric,
        ConsumingLongGaugeMetric minimalMetric
    ) {
        final EvictionPolicy<KeyType> policy = cacheService.getEvictionPolicy();
        final long[] filled = new long[1];
        final long[] protectedCount = new long[1];
        final long[] protectedFreq0 = new long[1];
        final long[] protectedFreqPositive = new long[1];
        final long[] backfill = new long[1];
        final long[] unknown = new long[1];
        final long[] minimalTimestamp = new long[1];
        final long startTime = System.nanoTime();
        cacheService.iterateCachedRegions((CacheRegion<KeyType> region, Integer freq) -> {
            filled[0]++;
            final long timestampMillis = region.timestampMillis();
            if (timestampMillis == SharedBlobCacheService.MINIMAL_CACHE_TIMESTAMP) {
                minimalTimestamp[0]++;
            } else if (timestampMillis == SharedBlobCacheService.BACKFILL_IN_PROGRESS_TIMESTAMP) {
                backfill[0]++;
            } else if (timestampMillis == SharedBlobCacheService.UNKNOWN_TIMESTAMP) {
                unknown[0]++;
            }
            if (policy.isProtected(region) == false) {
                return;
            }
            protectedCount[0]++;
            if (freq == 0) {
                protectedFreq0[0]++;
            } else {
                assert freq > 0 : freq;
                protectedFreqPositive[0]++;
            }
        });
        logger.debug("scanned [{}] regions in [{}]", filled[0], TimeValue.timeValueNanos(System.nanoTime() - startTime));
        filledMetric.set(filled[0]);
        protectedMetric.set(protectedCount[0]);
        protectedFreq0Metric.set(protectedFreq0[0]);
        protectedFreqPositiveMetric.set(protectedFreqPositive[0]);
        backfillMetric.set(backfill[0]);
        unknownMetric.set(unknown[0]);
        minimalMetric.set(minimalTimestamp[0]);
    }

    @Override
    protected void doStart() {
        synchronized (this) {
            reschedule();
        }
    }

    @Override
    protected void doStop() {
        synchronized (this) {
            if (metricsTask != null) {
                metricsTask.cancel();
                metricsTask = null;
            }
            clearGauges();
        }
    }

    @Override
    protected void doClose() throws IOException {
        Releasables.close(removeSettingsUpdater);
        final Consumer<ConsumingLongGaugeMetric> closeGauge = (gauge) -> {
            if (gauge != null) {
                gauge.gauge().close();
            }
        };
        closeGauge.accept(filledRegionsMetric.get());
        closeGauge.accept(totalRegionsMetric.get());
        closeGauge.accept(protectedMetric.get());
        closeGauge.accept(protectedFreq0Metric.get());
        closeGauge.accept(protectedFreqPositiveMetric.get());
        closeGauge.accept(backfillMetric.get());
        closeGauge.accept(unknownMetric.get());
        closeGauge.accept(minimalMetric.get());
    }
}
