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

class CounterResetDataPoints {
    private final Map<Long, Map<String, Double>> dataPoints = new HashMap<>();

    void reset() {
        dataPoints.clear();
    }

    void addDataPoint(String fieldName, ResetPoint resetPoint) {
        dataPoints.computeIfAbsent(resetPoint.timestamp, k -> new HashMap<>()).put(fieldName, resetPoint.value);
    }

    public boolean isEmpty() {
        return dataPoints.isEmpty();
    }

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
