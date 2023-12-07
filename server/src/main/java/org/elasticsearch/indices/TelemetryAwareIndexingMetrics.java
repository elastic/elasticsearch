/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.indices;

import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.shard.IndexingOperationListener;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.telemetry.TelemetryProvider;
import org.elasticsearch.telemetry.metric.LongCounter;
import org.elasticsearch.telemetry.metric.LongGaugeMetric;

public class TelemetryAwareIndexingMetrics implements IndexingOperationListener {

    private final LongCounter indexingTotalCountMetric;
    private final LongGaugeMetric indexingDurationTimeMetric;
    private final LongCounter indexingFailuresCountMetric;
    private final LongCounter deletionTotalCountMetric;
    private final LongGaugeMetric deletionDurationTimeMetric;
    private final LongCounter deletionFailuresCountMetric;

    public TelemetryAwareIndexingMetrics(final TelemetryProvider telemetryProvider) {

        this.indexingTotalCountMetric = telemetryProvider.getMeterRegistry()
            .registerLongCounter("es.indices.docs.indexing.total", "Total count of successfully indexed documents", "Count");

        this.indexingDurationTimeMetric = LongGaugeMetric.create(
            telemetryProvider.getMeterRegistry(),
            "es.indices.docs.indexing.time",
            "Document indexing operation duration time",
            "Millis"
        );

        this.indexingFailuresCountMetric = telemetryProvider.getMeterRegistry()
            .registerLongCounter("es.indices.docs.indexing.failures.count", "Count of document indexing failures", "Count");

        this.deletionTotalCountMetric = telemetryProvider.getMeterRegistry()
            .registerLongCounter("es.indices.docs.deletion.total", "Total count of successfully deleted documents", "Count");

        this.deletionDurationTimeMetric = LongGaugeMetric.create(
            telemetryProvider.getMeterRegistry(),
            "es.indices.docs.deletion.time",
            "Document deletion operation duration time",
            "Millis"
        );

        this.deletionFailuresCountMetric = telemetryProvider.getMeterRegistry()
            .registerLongCounter("es.indices.docs.deletion.failures.count", "Count of document deletion failures", "Count");
    }

    @Override
    public void postIndex(ShardId shardId, Engine.Index index, Engine.IndexResult result) {
        switch (result.getResultType()) {
            case SUCCESS -> {
                if (index.origin().isRecovery() == false) {
                    indexingTotalCountMetric.increment();

                    long durationTimeMillis = result.getTook();
                    indexingDurationTimeMetric.set(durationTimeMillis);
                }
            }
            case FAILURE -> postIndex(shardId, index, result.getFailure());
            default -> throw new IllegalArgumentException("Unknown result type: " + result.getResultType());
        }
    }

    @Override
    public void postIndex(ShardId shardId, Engine.Index index, Exception ex) {
        if (index.origin().isRecovery() == false) {
            indexingFailuresCountMetric.increment();
        }
    }

    @Override
    public void postDelete(ShardId shardId, Engine.Delete delete, Engine.DeleteResult result) {
        switch (result.getResultType()) {
            case SUCCESS -> {
                if (delete.origin().isRecovery() == false) {
                    deletionTotalCountMetric.increment();

                    long durationTimeMillis = result.getTook();
                    deletionDurationTimeMetric.set(durationTimeMillis);
                }
            }
            case FAILURE -> postDelete(shardId, delete, result.getFailure());
            default -> throw new IllegalArgumentException("Unknown result type: " + result.getResultType());
        }
    }

    @Override
    public void postDelete(ShardId shardId, Engine.Delete delete, Exception ex) {
        if (delete.origin().isRecovery() == false) {
            deletionFailuresCountMetric.increment();
        }
    }
}
