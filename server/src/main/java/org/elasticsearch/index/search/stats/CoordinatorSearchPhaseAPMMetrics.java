/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.search.stats;

import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.telemetry.metric.LongHistogram;
import org.elasticsearch.telemetry.metric.MeterRegistry;

import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Coordinator level APM metrics for search phases. Records phase execution times as histograms.
 * Assumes that this is a singleton shared by all searches running on the coordinating node.
 */
public class CoordinatorSearchPhaseAPMMetrics {

    private static final String metricNameFormat = "es.search.coordinator.phases.%s.duration.histogram";
    public static final CoordinatorSearchPhaseAPMMetrics NOOP = new CoordinatorSearchPhaseAPMMetrics(MeterRegistry.NOOP);

    private final Map<String, LongHistogram> histogramsCache = ConcurrentCollections.newConcurrentMap();
    private final MeterRegistry meterRegistry;

    public CoordinatorSearchPhaseAPMMetrics(MeterRegistry meterRegistry) {
        this.meterRegistry = meterRegistry;
    }

    public void onCoordinatorPhaseDone(String phaseName, long tookInNanos) {
        LongHistogram histogram = histogramsCache.computeIfAbsent(phaseName, this::createHistogram);
        if (histogram != null) {
            recordPhaseLatency(histogram, tookInNanos);
        } else {
            throw new IllegalStateException("phase [" + phaseName + "] not found");
        }
    }

    private LongHistogram createHistogram(String phaseName) {
        return meterRegistry.registerLongHistogram(
            String.format(Locale.ROOT, metricNameFormat, phaseName),
            String.format(
                Locale.ROOT,
                "%s phase execution times at the coordinator level, expressed as a histogram",
                phaseName.toUpperCase(Locale.ROOT)
            ),
            "ms"
        );
    }

    protected void recordPhaseLatency(LongHistogram histogramMetric, long tookInNanos) {
        histogramMetric.record(TimeUnit.NANOSECONDS.toMillis(tookInNanos));
    }
}
