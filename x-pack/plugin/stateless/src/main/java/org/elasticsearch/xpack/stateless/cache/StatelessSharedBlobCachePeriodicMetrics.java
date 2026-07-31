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
 * Occupancy and pinned-window breakdowns are counted in a single walk of occupied regions,
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

    public static final String PINNED_METRIC = "es.blob_cache.pinned_window.pinned.current";
    public static final String PINNED_FREQ_0_METRIC = "es.blob_cache.pinned_window.pinned_freq_0.current";
    public static final String PINNED_FREQ_POSITIVE_METRIC = "es.blob_cache.pinned_window.pinned_freq_positive.current";
    public static final String PINNED_BACKFILL_METRIC = "es.blob_cache.pinned_window.pinned_backfill.current";
    public static final String PINNED_UNKNOWN_METRIC = "es.blob_cache.pinned_window.pinned_unknown.current";
    public static final String MINIMAL_METRIC = "es.blob_cache.pinned_window.minimal.current";

    private final SharedBlobCacheService<?> cacheService;
    private final ThreadPool threadPool;
    private final MeterRegistry meterRegistry;
    private final Releasable removeSettingsUpdater;

    private volatile TimeValue metricsInterval;
    private Scheduler.Cancellable metricsTask;
    private final SetOnce<ConsumingLongGaugeMetric> filledRegionsMetric = new SetOnce<>();
    private final SetOnce<ConsumingLongGaugeMetric> totalRegionsMetric = new SetOnce<>();
    private final SetOnce<ConsumingLongGaugeMetric> pinnedMetric = new SetOnce<>();
    private final SetOnce<ConsumingLongGaugeMetric> pinnedFreq0Metric = new SetOnce<>();
    private final SetOnce<ConsumingLongGaugeMetric> pinnedFreqPositiveMetric = new SetOnce<>();
    private final SetOnce<ConsumingLongGaugeMetric> pinnedBackfillMetric = new SetOnce<>();
    private final SetOnce<ConsumingLongGaugeMetric> pinnedUnknownMetric = new SetOnce<>();
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
        pinnedMetric.set(
            ConsumingLongGaugeMetric.create(
                meterRegistry,
                PINNED_METRIC,
                "Number of occupied shared blob-cache regions protected by the active eviction policy",
                "regions"
            )
        );
        pinnedFreq0Metric.set(
            ConsumingLongGaugeMetric.create(
                meterRegistry,
                PINNED_FREQ_0_METRIC,
                "Number of protected regions at LFU frequency level 0",
                "regions"
            )
        );
        pinnedFreqPositiveMetric.set(
            ConsumingLongGaugeMetric.create(
                meterRegistry,
                PINNED_FREQ_POSITIVE_METRIC,
                "Number of protected regions at a positive LFU frequency level",
                "regions"
            )
        );
        pinnedBackfillMetric.set(
            ConsumingLongGaugeMetric.create(
                meterRegistry,
                PINNED_BACKFILL_METRIC,
                "Number of protected regions with a backfill-in-progress timestamp",
                "regions"
            )
        );
        pinnedUnknownMetric.set(
            ConsumingLongGaugeMetric.create(
                meterRegistry,
                PINNED_UNKNOWN_METRIC,
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
        final ConsumingLongGaugeMetric filled = filledRegionsMetric.get();
        final ConsumingLongGaugeMetric total = totalRegionsMetric.get();
        final ConsumingLongGaugeMetric pinned = pinnedMetric.get();
        final ConsumingLongGaugeMetric pinnedFreq0 = pinnedFreq0Metric.get();
        final ConsumingLongGaugeMetric pinnedFreqPositive = pinnedFreqPositiveMetric.get();
        final ConsumingLongGaugeMetric pinnedBackfill = pinnedBackfillMetric.get();
        final ConsumingLongGaugeMetric pinnedUnknown = pinnedUnknownMetric.get();
        final ConsumingLongGaugeMetric minimal = minimalMetric.get();
        assert filled != null;
        assert total != null;
        assert pinned != null;
        assert pinnedFreq0 != null;
        assert pinnedFreqPositive != null;
        assert pinnedBackfill != null;
        assert pinnedUnknown != null;
        assert minimal != null;
        total.set(cacheService.getStats().numberOfRegions());
        sampleRegions(cacheService, filled, pinned, pinnedFreq0, pinnedFreqPositive, pinnedBackfill, pinnedUnknown, minimal);
    }

    /**
     * Walks occupied regions once to publish occupancy and protection gauges.
     */
    private static <KeyType extends SharedBlobCacheService.KeyBase> void sampleRegions(
        SharedBlobCacheService<KeyType> cacheService,
        ConsumingLongGaugeMetric filledMetric,
        ConsumingLongGaugeMetric pinnedMetric,
        ConsumingLongGaugeMetric pinnedFreq0Metric,
        ConsumingLongGaugeMetric pinnedFreqPositiveMetric,
        ConsumingLongGaugeMetric pinnedBackfillMetric,
        ConsumingLongGaugeMetric pinnedUnknownMetric,
        ConsumingLongGaugeMetric minimalMetric
    ) {
        final EvictionPolicy<KeyType> policy = cacheService.getEvictionPolicy();
        final long[] filled = new long[1];
        final long[] pinned = new long[1];
        final long[] pinnedFreq0 = new long[1];
        final long[] pinnedFreqPositive = new long[1];
        final long[] pinnedBackfill = new long[1];
        final long[] pinnedUnknown = new long[1];
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
        filledMetric.set(filled[0]);
        pinnedMetric.set(pinned[0]);
        pinnedFreq0Metric.set(pinnedFreq0[0]);
        pinnedFreqPositiveMetric.set(pinnedFreqPositive[0]);
        pinnedBackfillMetric.set(pinnedBackfill[0]);
        pinnedUnknownMetric.set(pinnedUnknown[0]);
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
        closeGauge.accept(pinnedMetric.get());
        closeGauge.accept(pinnedFreq0Metric.get());
        closeGauge.accept(pinnedFreqPositiveMetric.get());
        closeGauge.accept(pinnedBackfillMetric.get());
        closeGauge.accept(pinnedUnknownMetric.get());
        closeGauge.accept(minimalMetric.get());
    }
}
