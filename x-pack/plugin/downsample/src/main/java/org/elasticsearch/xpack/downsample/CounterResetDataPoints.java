/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.downsample;

import java.util.HashMap;
import java.util.Map;
import java.util.function.BiConsumer;

/**
 * This class stores counter measurements necessary to detect a reset during downsampling. These measurements are
 * the last measurement before a reset and the next measurement.
 *
 * Note: this code is used sequentially, and therefore there is no thread-safety built-in.
 */
class CounterResetDataPoints {

    /**
     * Tracks timestamp measurements and the counter values at that moment.
     */
    private final Map<Long, Map<String, Double>> dataPoints = new HashMap<>();

    /**
     * When the downsampled bucket is completed, the tracked measurements need to be reset.
     */
    void reset() {
        dataPoints.clear();
    }

    void addDataPoint(String counterName, ResetPoint resetPoint) {
        dataPoints.computeIfAbsent(resetPoint.timestamp, k -> new HashMap<>()).put(counterName, resetPoint.value);
    }

    public boolean isEmpty() {
        return dataPoints.isEmpty();
    }

    /**
     * Apply the processor consumer on each tracked measurement.
     */
    public void processDataPoints(BiConsumer<Long, Map<String, Double>> processor) {
        if (isEmpty() == false) {
            dataPoints.forEach(processor);
        }
    }

    public int count() {
        return dataPoints.size();
    }

    record ResetPoint(long timestamp, double value) {}
}
