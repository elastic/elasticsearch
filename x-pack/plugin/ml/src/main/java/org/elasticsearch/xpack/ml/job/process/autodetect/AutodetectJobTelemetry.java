/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.job.process.autodetect;

import org.elasticsearch.telemetry.metric.DoubleHistogram;
import org.elasticsearch.telemetry.metric.LongHistogram;
import org.elasticsearch.telemetry.metric.MeterRegistry;

import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Per-project telemetry for anomaly detection jobs. Attributes never include job ids, so the series count does not grow
 * with the number of jobs. A bucket counts as persisted once it is queued in the bulk request, not once the write is acknowledged.
 */
public final class AutodetectJobTelemetry {

    public static final String TIME_TO_OPENED_METRIC = "es.ml.anomaly_detectors.time_to_opened.duration";
    public static final String RESULTS_LAG_RATIO_METRIC = "es.ml.anomaly_detectors.results_lag.ratio";

    public static final String OPEN_PHASE_ATTRIBUTE = "es_open_phase";

    /** PROCESS_START covers all node-local open work before the OPENED state write; STATE_WRITE is that write until its ack. */
    public enum OpenPhase {
        PROCESS_START("process_start"),
        STATE_WRITE("state_write");

        private final String attributeValue;

        OpenPhase(String attributeValue) {
            this.attributeValue = attributeValue;
        }

        public String attributeValue() {
            return attributeValue;
        }
    }

    /** Milliseconds from one second to thirty minutes, so a restore of a large model stays distinguishable from a stuck open. */
    public static final List<Long> TIME_TO_OPENED_BUCKET_BOUNDARIES = List.of(
        1_000L,
        2_000L,
        5_000L,
        10_000L,
        30_000L,
        60_000L,
        120_000L,
        300_000L,
        600_000L,
        1_800_000L
    );

    /** Real-time buckets are expected below 1 bucket span; the tail runs to 128 spans to tell a stalled job from a briefly slow one. */
    public static final List<Double> RESULTS_LAG_RATIO_BUCKET_BOUNDARIES = List.of(0.25, 0.5, 1.0, 2.0, 4.0, 8.0, 16.0, 32.0, 64.0, 128.0);

    private final LongHistogram timeToOpenedHistogram;
    private final DoubleHistogram resultsLagRatioHistogram;

    public AutodetectJobTelemetry(MeterRegistry meterRegistry) {
        Objects.requireNonNull(meterRegistry);
        this.timeToOpenedHistogram = meterRegistry.registerLongHistogram(
            TIME_TO_OPENED_METRIC,
            "Duration of each phase of opening an anomaly detection job, recorded when the job state is acknowledged as OPENED.",
            "ms",
            TIME_TO_OPENED_BUCKET_BOUNDARIES
        );
        this.resultsLagRatioHistogram = meterRegistry.registerDoubleHistogram(
            RESULTS_LAG_RATIO_METRIC,
            "Distance between now and the end of each real-time bucket queued for persistence, in bucket spans.",
            "ratio",
            RESULTS_LAG_RATIO_BUCKET_BOUNDARIES
        );
    }

    public void recordOpenPhaseDuration(OpenPhase phase, long durationMillis) {
        timeToOpenedHistogram.record(durationMillis, Map.of(OPEN_PHASE_ATTRIBUTE, phase.attributeValue()));
    }

    public void recordResultsLagRatio(double ratio) {
        resultsLagRatioHistogram.record(ratio);
    }
}
