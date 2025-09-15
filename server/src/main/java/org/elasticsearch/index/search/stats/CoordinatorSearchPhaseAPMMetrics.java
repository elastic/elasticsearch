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

public class CoordinatorSearchPhaseAPMMetrics extends SearchPhaseAPMMetrics {

    private final LongHistogram coordinatorPhaseMetric;
    private final String metricName;

    public CoordinatorSearchPhaseAPMMetrics(MeterRegistry meterRegistry, String phaseName) {
        this.metricName = String.format("es.search.coordinator.phases.%s.duration.histogram", phaseName);
        this.coordinatorPhaseMetric = meterRegistry.registerLongHistogram(
            metricName,
            String.format("Coordinator %s phase execution times at the shard level, expressed as a histogram", phaseName),
            "ms"
        );
    }

    public void onCoordinatorPhase(long tookInNanos) {
        recordPhaseLatency(coordinatorPhaseMetric, tookInNanos);
    }

    public String getMetricName() {
        return metricName;
    }
}
