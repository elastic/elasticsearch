/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.search.stats;

import org.elasticsearch.telemetry.metric.LongHistogram;
import org.elasticsearch.telemetry.metric.MeterRegistry;

import java.util.concurrent.TimeUnit;

/**
 * Coordinator level APM metrics for search phases. Records phase execution times as histograms.
 */
public class CoordinatorSearchPhaseAPMMetrics {

    public static final CoordinatorSearchPhaseAPMMetrics NOOP = new CoordinatorSearchPhaseAPMMetrics(MeterRegistry.NOOP);

    public static final String QUERY_SEARCH_PHASE_METRIC = "es.search.coordinator.phases.query.duration.histogram";
    private final LongHistogram queryPhaseMetric;

    public CoordinatorSearchPhaseAPMMetrics(MeterRegistry meterRegistry) {
        this.queryPhaseMetric = meterRegistry.registerLongHistogram(
            QUERY_SEARCH_PHASE_METRIC,
            "Query search phase execution times at the coordinator level, expressed as a histogram",
            "ms"
        );
    }

    public void onQueryPhaseDone(long tookInNanos) {
        recordPhaseLatency(queryPhaseMetric, tookInNanos);
    }

    protected void recordPhaseLatency(LongHistogram histogramMetric, long tookInNanos) {
        histogramMetric.record(TimeUnit.NANOSECONDS.toMillis(tookInNanos));
    }
}
