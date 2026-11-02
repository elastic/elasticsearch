/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.datafeed;

import org.elasticsearch.telemetry.metric.DoubleHistogram;
import org.elasticsearch.telemetry.metric.MeterRegistry;
import org.elasticsearch.xpack.ml.datafeed.DatafeedSearchTelemetry.ExtractorType;

import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Per-project telemetry for how far real-time datafeeds trail the horizon (now minus query delay), in bucket spans.
 * Attributes are limited to the extractor type, so series count does not grow with the number of jobs or datafeeds.
 * Observations start once a datafeed's lookback has finished, so the expected real-time count minus the histogram count
 * includes datafeeds still in lookback.
 */
public final class DatafeedHorizonTelemetry {

    public static final String HORIZON_LAG_RATIO_METRIC = "es.ml.datafeeds.realtime.horizon_lag.ratio";

    /**
     * Healthy datafeeds sit below 1 bucket span; the tail is resolved up to 128 spans so a stalled datafeed stays distinguishable
     * from a briefly slow one.
     */
    public static final List<Double> HORIZON_LAG_RATIO_BUCKET_BOUNDARIES = List.of(0.25, 0.5, 1.0, 2.0, 4.0, 8.0, 16.0, 32.0, 64.0, 128.0);

    private final DoubleHistogram horizonLagRatioHistogram;

    public DatafeedHorizonTelemetry(MeterRegistry meterRegistry) {
        Objects.requireNonNull(meterRegistry);
        this.horizonLagRatioHistogram = meterRegistry.registerDoubleHistogram(
            HORIZON_LAG_RATIO_METRIC,
            "Distance between the end of a real-time datafeed's last search and now minus query_delay, in bucket spans.",
            "ratio",
            HORIZON_LAG_RATIO_BUCKET_BOUNDARIES
        );
    }

    public void recordHorizonLagRatio(ExtractorType extractorType, double ratio) {
        horizonLagRatioHistogram.record(ratio, Map.of(DatafeedSearchTelemetry.EXTRACTOR_TYPE_ATTRIBUTE, extractorType.attributeValue()));
    }
}
