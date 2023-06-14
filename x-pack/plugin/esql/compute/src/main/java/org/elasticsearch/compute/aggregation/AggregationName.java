/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

/** Name of the aggregation function. */
public enum AggregationName {

    avg,

    count,

    max,

    median,

    median_absolute_deviation,

    min,

    percentile,

    sum;

    public static AggregationName of(String planName) {
        return switch (planName) {
            case "avg" -> avg;
            case "count" -> count;
            case "max" -> max;
            case "median" -> median;
            case "medianabsolutedeviation" -> median_absolute_deviation;
            case "min" -> min;
            case "percentile" -> percentile;
            case "sum" -> sum;
            default -> throw new UnsupportedOperationException("unknown agg function:" + planName);
        };
    }
}
