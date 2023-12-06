/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.indices;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.shard.IndexingOperationListener;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.telemetry.TelemetryProvider;
import org.elasticsearch.telemetry.metric.LongCounter;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.concurrent.atomic.AtomicLong;

public class TelemetryAwareIndexingMetrics implements IndexingOperationListener {

    private static final Logger logger = LogManager.getLogger(TelemetryAwareIndexingMetrics.class);

    public static final Setting<TimeValue> TELEMETRY_INDEXING_METRICS_TIME_INTERVAL_SETTING = Setting.positiveTimeSetting(
        "telemetry.indexing.metrics.time.interval",
        TimeValue.timeValueSeconds(5),
        Setting.Property.NodeScope
    );

    private final ThreadPool threadPool;
    private final TimeValue interval;
    private final LongCounter indexedDocsTotalMetric;
    private final AtomicLong indexedDocsCountAggregator = new AtomicLong(0);
    private volatile boolean isPublishingEnabled;

    public TelemetryAwareIndexingMetrics(final Settings settings, final ThreadPool threadPool, final TelemetryProvider telemetryProvider) {
        this.threadPool = threadPool;
        this.interval = TELEMETRY_INDEXING_METRICS_TIME_INTERVAL_SETTING.get(settings);
        this.indexedDocsTotalMetric = telemetryProvider.getMeterRegistry()
            .registerLongCounter("es.indices.docs.indexed.total", "Total number of successfully indexed documents", "Count");
    }

    public void start() {
        isPublishingEnabled = true;
        publish();
    }

    public void stop() {
        isPublishingEnabled = false;
    }

    private void publish() {
        if (isPublishingEnabled == false) {
            return;
        }
        doPublish();
        threadPool.scheduleUnlessShuttingDown(interval, threadPool.executor(ThreadPool.Names.MANAGEMENT), this::publish);
    }

    private void doPublish() {
        final long value = indexedDocsCountAggregator.get();
        if (logger.isDebugEnabled()) {
            logger.debug("Publishing 'indexedDocsTotalMetric' : {} ", value);
        }
        this.indexedDocsTotalMetric.incrementBy(value);
    }

    @Override
    public void postIndex(ShardId shardId, Engine.Index index, Engine.IndexResult result) {
        switch (result.getResultType()) {
            case SUCCESS -> {
                if (index.origin().isRecovery() == false) {
                    indexedDocsCountAggregator.incrementAndGet();
                }
            }
            case FAILURE -> postIndex(shardId, index, result.getFailure());
            default -> throw new IllegalArgumentException("unknown result type: " + result.getResultType());
        }
    }
}
