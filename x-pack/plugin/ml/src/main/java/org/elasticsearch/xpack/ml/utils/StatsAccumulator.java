/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.utils;

import java.util.HashMap;
import java.util.Map;

/**
 * Helper class to collect min, max, avg and total statistics for a quantity
 */
public class StatsAccumulator {

    private static final String MIN = "min";
    private static final String MAX = "max";
    private static final String AVG = "avg";
    private static final String TOTAL = "total";

    private long count;
    private double total;
    private Double min;
    private Double max;

    public void add(double value) {
        count++;
        total += value;
        min = min == null ? value : (value < min ? value : min);
        max = max == null ? value : (value > max ? value : max);
    }

    public double getMin() {
        return min == null ? 0.0 : min;
    }

    public double getMax() {
        return max == null ? 0.0 : max;
    }

    public double getAvg() {
        return count == 0.0 ? 0.0 : total/count;
    }

    public double getTotal() {
        return total;
    }

    public Map<String, Double> asMap() {
        Map<String, Double> map = new HashMap<>();
        map.put(MIN, getMin());
        map.put(MAX, getMax());
        map.put(AVG, getAvg());
        map.put(TOTAL, getTotal());
        return map;
    }
}
