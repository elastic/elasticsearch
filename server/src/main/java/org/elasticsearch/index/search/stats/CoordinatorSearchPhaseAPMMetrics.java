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

import java.util.Map;

import static org.elasticsearch.action.search.TransportSearchAction.SearchTimeProvider;

/**
 * Coordinator level APM metrics for search phases.
 * Records phase execution times as histograms.
 */
public class CoordinatorSearchPhaseAPMMetrics extends SearchPhaseAPMMetrics {

    private static final String metricNameFormat = "es.search.coordinator.phases.%s.duration.histogram";
    private final Map<String, LongHistogram> histogramsCache = ConcurrentCollections.newConcurrentMap();
    private final MeterRegistry meterRegistry;
    private final SearchTimeProvider timeProvider;

    private long phaseStartTimeNanos;

    public CoordinatorSearchPhaseAPMMetrics(MeterRegistry meterRegistry, SearchTimeProvider timeProvider) {
        this.meterRegistry = meterRegistry;
        this.timeProvider = timeProvider;
    }

    public void onCoordinatorPhaseStart(String phaseName) {
        this.phaseStartTimeNanos = timeProvider.relativeCurrentNanosProvider().getAsLong();
        histogramsCache.computeIfAbsent(phaseName, this::createHistogram);
    }

    private LongHistogram createHistogram(String phaseName) {
        return meterRegistry.registerLongHistogram(
            String.format(metricNameFormat, phaseName),
            String.format("%s phase execution times at the coordinator level, expressed as a histogram", phaseName.toUpperCase()),
            "ms"
        );
    }

    public void onCoordinatorPhaseDone(String phaseName) {
        long tookInNanos = timeProvider.relativeCurrentNanosProvider().getAsLong() - phaseStartTimeNanos;
        LongHistogram histogram = histogramsCache.get(phaseName);
        if (histogram != null) {
            recordPhaseLatency(histogram, tookInNanos);
        } else {
            throw new IllegalStateException("phase [" + phaseName + "] not found");
        }
    }
}
