/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.downsample;

import org.elasticsearch.exponentialhistogram.ExponentialHistogram;
import org.elasticsearch.exponentialhistogram.ExponentialHistogramXContent;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Stores field values at reset boundaries during downsampling. These data points are needed to
 * reconstruct counter resets when querying the downsampled index.
 * <p>
 * Supports both numeric counters and exponential histograms via the sealed {@link ResetValue} hierarchy.
 * <p>
 * Invariant: at most one reset value per (fieldName, timestamp) pair. {@link #addDataPoint}
 * enforces this defensively: duplicate adds are dropped and logged.
 */
class ResetDataPoints {

    private static final Logger logger = LogManager.getLogger(ResetDataPoints.class);

    private final Map<Long, Map<String, ResetValue>> dataPoints = new HashMap<>();

    void addDataPoint(String fieldName, ResetPoint resetPoint) {
        var values = dataPoints.computeIfAbsent(resetPoint.timestamp(), k -> new HashMap<>());
        if (values.putIfAbsent(fieldName, resetPoint.value()) != null) {
            assert false : "duplicate reset data point for field [" + fieldName + "] at timestamp [" + resetPoint.timestamp() + "]";
            logger.warn("Skipping duplicate reset data point for field [{}] at timestamp [{}]", fieldName, resetPoint.timestamp());
        }
    }

    public boolean isEmpty() {
        return dataPoints.isEmpty();
    }

    public int countResetDocuments() {
        return dataPoints.size();
    }

    /**
     * Apply the processor on each tracked measurement.
     */
    public void processDataPoints(ResetPointProcessor processor) throws IOException {
        for (var entry : dataPoints.entrySet()) {
            processor.process(entry.getKey(), entry.getValue());
        }
    }

    @FunctionalInterface
    interface ResetPointProcessor {
        void process(long timestamp, Map<String, ResetValue> resetValues);
    }

    record ResetPoint(long timestamp, ResetValue value) {
        ResetPoint(long timestamp, double value) {
            this(timestamp, new CounterResetValue(value));
        }

        ResetPoint(long timestamp, ExponentialHistogram value) {
            this(timestamp, new HistogramResetValue(value));
        }
    }

    sealed interface ResetValue {
        void write(String fieldName, XContentBuilder builder) throws IOException;
    }

    record CounterResetValue(double value) implements ResetValue {
        @Override
        public void write(String fieldName, XContentBuilder builder) throws IOException {
            builder.field(fieldName, value);
        }
    }

    record HistogramResetValue(ExponentialHistogram value) implements ResetValue {
        @Override
        public void write(String fieldName, XContentBuilder builder) throws IOException {
            builder.field(fieldName);
            ExponentialHistogramXContent.serialize(builder, value);
        }
    }
}
