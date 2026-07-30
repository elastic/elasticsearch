/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.blobcache.shared;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.telemetry.metric.ConsumingLongGaugeMetric;
import org.elasticsearch.telemetry.metric.MeterRegistry;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.blobcache.shared.SharedBlobCacheService.SHARED_CACHE_METRICS_INTERVAL_SETTING;

/**
 * Periodically samples shared blob-cache occupancy and publishes consuming gauges.
 * <p>
 * Occupancy is counted by walking occupied regions once,
 * sharing that walk with {@link EvictionPolicy#updatePeriodicMetrics} when the policy requests it.
 */
public final class BlobCachePeriodicMetrics extends AbstractLifecycleComponent {

    public static final String BLOB_CACHE_REGIONS_FILLED = "es.blob_cache.regions.filled";
    /**
     * Although a fixed value, it is primarily helpful for calculating percentages in Observability.
     */
    public static final String BLOB_CACHE_REGIONS_TOTAL = "es.blob_cache.regions.total";

    private final SharedBlobCacheService<?> cacheService;
    private final ThreadPool threadPool;
    private final MeterRegistry meterRegistry;
    private final TimeValue metricsInterval;
    private final SetOnce<Scheduler.Cancellable> metricsTask = new SetOnce<>();
    private final SetOnce<ConsumingLongGaugeMetric> filledRegionsMetric = new SetOnce<>();
    private final SetOnce<ConsumingLongGaugeMetric> totalRegionsMetric = new SetOnce<>();

    public BlobCachePeriodicMetrics(
        SharedBlobCacheService<?> cacheService,
        Settings settings,
        ThreadPool threadPool,
        MeterRegistry meterRegistry
    ) {
        this.cacheService = Objects.requireNonNull(cacheService);
        this.threadPool = Objects.requireNonNull(threadPool);
        this.meterRegistry = Objects.requireNonNull(meterRegistry);
        this.metricsInterval = SHARED_CACHE_METRICS_INTERVAL_SETTING.get(settings);
    }

    private void sample() {
        final ConsumingLongGaugeMetric filledMetric = filledRegionsMetric.get();
        final ConsumingLongGaugeMetric totalMetric = totalRegionsMetric.get();
        assert filledMetric != null;
        assert totalMetric != null;
        final int numRegions = cacheService.getStats().numberOfRegions();
        totalMetric.set(numRegions);
        filledMetric.set(countFilledAndUpdatePolicy(cacheService));
    }

    /**
     * Counts occupied regions and drives policy gauges in one map walk when the policy requests
     * iteration; otherwise walks once solely for the filled count.
     */
    private static <KeyType extends SharedBlobCacheService.KeyBase> long countFilledAndUpdatePolicy(
        SharedBlobCacheService<KeyType> cacheService
    ) {
        final long[] filled = new long[1];
        final boolean[] walked = new boolean[1];
        cacheService.getEvictionPolicy().updatePeriodicMetrics(policyConsumer -> {
            walked[0] = true;
            cacheService.iterateCachedRegions((region, freq) -> {
                filled[0]++;
                policyConsumer.accept(region, freq);
            });
        });
        if (walked[0] == false) {
            cacheService.iterateCachedRegions((region, freq) -> filled[0]++);
        }
        return filled[0];
    }

    @Override
    protected void doStart() {
        if (TimeValue.MINUS_ONE.equals(metricsInterval)) {
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
        metricsTask.set(threadPool.scheduleWithFixedDelay(this::sample, metricsInterval, threadPool.generic()));
    }

    @Override
    protected void doStop() {
        final Scheduler.Cancellable task = metricsTask.get();
        if (task != null) {
            task.cancel();
        }
    }

    @Override
    protected void doClose() throws IOException {
        final ConsumingLongGaugeMetric filled = filledRegionsMetric.get();
        if (filled != null) {
            filled.gauge().close();
        }
        final ConsumingLongGaugeMetric total = totalRegionsMetric.get();
        if (total != null) {
            total.gauge().close();
        }
    }
}
