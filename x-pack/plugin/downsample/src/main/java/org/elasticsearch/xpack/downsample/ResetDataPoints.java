/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.downsample;

import org.elasticsearch.core.Tuple;
import org.elasticsearch.exponentialhistogram.ExponentialHistogram;
import org.elasticsearch.exponentialhistogram.ExponentialHistogramXContent;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Stores field values at reset boundaries during downsampling. These data points are needed to
 * reconstruct counter resets when querying the downsampled index.
 * <p>
 * Supports both numeric counters and exponential histograms via the sealed {@link ResetValue} hierarchy.
 */
class ResetDataPoints {

    private final Map<Long, List<Tuple<String, ResetValue>>> dataPoints = new HashMap<>();

    void addDataPoint(String fieldName, ResetPoint resetPoint) {
        dataPoints.computeIfAbsent(resetPoint.timestamp(), k -> new ArrayList<>()).add(Tuple.tuple(fieldName, resetPoint.value()));
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
        void process(long timestamp, List<Tuple<String, ResetValue>> resetValues);
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
