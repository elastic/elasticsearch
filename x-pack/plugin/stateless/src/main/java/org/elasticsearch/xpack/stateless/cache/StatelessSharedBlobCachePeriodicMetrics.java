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

    /**
     * How often this component will sample. A value of {@link TimeValue#MINUS_ONE} disables sampling.
     * Enabled intervals must be at least {@link #MIN_METRICS_INTERVAL}.
     * <p>
     * Minutes frequency is cheap even at large cache sizes: a full sample walks occupied regions once without holding the
     * cache monitor. At a 2TiB cache with 16MiB regions that is at most ~131k entries of field reads and map lookups.
     */
    public static final Setting<TimeValue> METRICS_INTERVAL_SETTING = Setting.timeSetting(
        METRICS_INTERVAL_SETTING_KEY,
        TimeValue.timeValueMinutes(3),
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

    public static final String PROTECTED_METRIC = "es.blob_cache.protected.current";
    public static final String PROTECTED_FREQ_0_METRIC = "es.blob_cache.protected.freq_0.current";
    public static final String PROTECTED_FREQ_POSITIVE_METRIC = "es.blob_cache.protected.freq_positive.current";
    public static final String PROTECTED_BACKFILL_METRIC = "es.blob_cache.protected.backfill.current";
    public static final String PROTECTED_UNKNOWN_METRIC = "es.blob_cache.protected.unknown.current";
    /**
     * Counts occupied regions with {@link SharedBlobCacheService#MINIMAL_CACHE_TIMESTAMP}, independent of
     * eviction-policy protection (hence under {@code regions.*}, not {@code protected.*}).
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
    private final SetOnce<ConsumingLongGaugeMetric> protectedBackfillMetric = new SetOnce<>();
    private final SetOnce<ConsumingLongGaugeMetric> protectedUnknownMetric = new SetOnce<>();
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
        protectedBackfillMetric.set(
            ConsumingLongGaugeMetric.create(
                meterRegistry,
                PROTECTED_BACKFILL_METRIC,
                "Number of protected regions with a backfill-in-progress timestamp",
                "regions"
            )
        );
        protectedUnknownMetric.set(
            ConsumingLongGaugeMetric.create(
                meterRegistry,
                PROTECTED_UNKNOWN_METRIC,
                "Number of protected regions with an unknown timestamp",
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
        final ConsumingLongGaugeMetric protectedBackfill = protectedBackfillMetric.get();
        final ConsumingLongGaugeMetric protectedUnknown = protectedUnknownMetric.get();
        final ConsumingLongGaugeMetric minimal = minimalMetric.get();
        assert filled != null;
        assert total != null;
        assert protectedRegions != null;
        assert protectedFreq0 != null;
        assert protectedFreqPositive != null;
        assert protectedBackfill != null;
        assert protectedUnknown != null;
        assert minimal != null;
        total.set(cacheService.getStats().numberOfRegions());
        sampleRegions(
            cacheService,
            filled,
            protectedRegions,
            protectedFreq0,
            protectedFreqPositive,
            protectedBackfill,
            protectedUnknown,
            minimal
        );
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
        ConsumingLongGaugeMetric protectedBackfillMetric,
        ConsumingLongGaugeMetric protectedUnknownMetric,
        ConsumingLongGaugeMetric minimalMetric
    ) {
        final EvictionPolicy<KeyType> policy = cacheService.getEvictionPolicy();
        final long[] filled = new long[1];
        final long[] protectedCount = new long[1];
        final long[] protectedFreq0 = new long[1];
        final long[] protectedFreqPositive = new long[1];
        final long[] protectedBackfill = new long[1];
        final long[] protectedUnknown = new long[1];
        final long[] minimalTimestamp = new long[1];
        cacheService.iterateCachedRegions((CacheRegion<KeyType> region, Integer freq) -> {
            filled[0]++;
            final long timestampMillis = region.timestampMillis();
            if (timestampMillis == SharedBlobCacheService.MINIMAL_CACHE_TIMESTAMP) {
                minimalTimestamp[0]++;
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
            if (timestampMillis == SharedBlobCacheService.BACKFILL_IN_PROGRESS_TIMESTAMP) {
                protectedBackfill[0]++;
            } else if (timestampMillis == SharedBlobCacheService.UNKNOWN_TIMESTAMP) {
                protectedUnknown[0]++;
            }
        });
        filledMetric.set(filled[0]);
        protectedMetric.set(protectedCount[0]);
        protectedFreq0Metric.set(protectedFreq0[0]);
        protectedFreqPositiveMetric.set(protectedFreqPositive[0]);
        protectedBackfillMetric.set(protectedBackfill[0]);
        protectedUnknownMetric.set(protectedUnknown[0]);
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
        closeGauge.accept(protectedBackfillMetric.get());
        closeGauge.accept(protectedUnknownMetric.get());
        closeGauge.accept(minimalMetric.get());
    }
}
