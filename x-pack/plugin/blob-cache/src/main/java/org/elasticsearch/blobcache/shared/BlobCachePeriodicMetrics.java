/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.blobcache.shared;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.telemetry.metric.ConsumingLongGaugeMetric;
import org.elasticsearch.telemetry.metric.MeterRegistry;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

/**
 * Periodically samples shared blob-cache occupancy and publishes a consuming gauge.
 * <p>
 * The gauge value is the number of filled (occupied) regions. Additional attributes come from the
 * active {@link EvictionPolicy#metricAttributes} implementation.
 */
public final class BlobCachePeriodicMetrics extends AbstractLifecycleComponent {

    /**
     * How often to sample blob-cache occupancy metrics. A value of {@link TimeValue#MINUS_ONE} disables sampling.
     * Defaults to five minutes on stateless nodes and {@link TimeValue#MINUS_ONE} otherwise.
     */
    public static final Setting<TimeValue> BLOB_CACHE_METRICS_INTERVAL_SETTING = Setting.timeSetting(
        "es.blob_cache.metrics_interval",
        settings -> DiscoveryNode.isStateless(settings) ? TimeValue.timeValueMinutes(5) : TimeValue.MINUS_ONE,
        TimeValue.MINUS_ONE,
        Setting.Property.NodeScope
    );

    public static final String BLOB_CACHE_REGIONS_CURRENT = "es.blob_cache.regions.current";

    private final SharedBlobCacheService<?> cacheService;
    private final ThreadPool threadPool;
    private final MeterRegistry meterRegistry;
    private final TimeValue metricsInterval;
    private final SetOnce<Scheduler.Cancellable> metricsTask = new SetOnce<>();
    private final SetOnce<ConsumingLongGaugeMetric> regionsMetric = new SetOnce<>();

    public BlobCachePeriodicMetrics(
        SharedBlobCacheService<?> cacheService,
        Settings settings,
        ThreadPool threadPool,
        MeterRegistry meterRegistry
    ) {
        this(cacheService, threadPool, meterRegistry, BLOB_CACHE_METRICS_INTERVAL_SETTING.get(settings));
    }

    /**
     * For tests that configure a fixed interval.
     */
    BlobCachePeriodicMetrics(
        SharedBlobCacheService<?> cacheService,
        ThreadPool threadPool,
        MeterRegistry meterRegistry,
        TimeValue metricsInterval
    ) {
        this.cacheService = Objects.requireNonNull(cacheService);
        this.threadPool = Objects.requireNonNull(threadPool);
        this.meterRegistry = Objects.requireNonNull(meterRegistry);
        this.metricsInterval = Objects.requireNonNull(metricsInterval);
    }

    private void sample() {
        final ConsumingLongGaugeMetric metric = regionsMetric.get();
        assert metric != null;
        final int numRegions = cacheService.getStats().numberOfRegions();
        if (numRegions == 0) {
            return;
        }
        final long free = cacheService.freeRegionCount();
        final long filled = numRegions - free;
        metric.set(filled, attributesOf(cacheService, numRegions));
    }

    private static <KeyType extends SharedBlobCacheService.KeyBase> Map<String, Object> attributesOf(
        SharedBlobCacheService<KeyType> cacheService,
        int numRegions
    ) {
        return cacheService.getEvictionPolicy().metricAttributes(cacheService::iterateCachedRegions, numRegions);
    }

    @Override
    protected void doStart() {
        if (TimeValue.MINUS_ONE.equals(metricsInterval)) {
            return;
        }
        regionsMetric.set(
            ConsumingLongGaugeMetric.create(
                meterRegistry,
                BLOB_CACHE_REGIONS_CURRENT,
                "The number of occupied shared blob-cache regions, with additional attributes supplied by the active eviction policy",
                "regions"
            )
        );
        metricsTask.set(threadPool.scheduleWithFixedDelay(this::sample, metricsInterval, threadPool.generic()));
    }

    @Override
    protected void doStop() {
        if (metricsTask.get() != null) {
            metricsTask.get().cancel();
        }
    }

    @Override
    protected void doClose() throws IOException {}
}
