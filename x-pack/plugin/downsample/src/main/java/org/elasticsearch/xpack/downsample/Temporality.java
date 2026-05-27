/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.downsample;

/**
 * Represents the metric temporality for a time series, controlling how counter values are interpreted during downsampling.
 */
enum Temporality {
    DELTA,
    CUMULATIVE,
    /**
     * The default temporality, which depends on the metric type: cumulative for counters, delta for histograms.
     */
    DEFAULT;

    static Temporality fromDimensionValue(Object value) {
        if (value == null) {
            return DEFAULT;
        }
        if (value instanceof String s) {
            if ("delta".equals(s)) {
                return DELTA;
            } else if ("cumulative".equals(s)) {
                return CUMULATIVE;
            }
            // Use default for unknown values
            return DEFAULT;
        } else {
            throw new IllegalArgumentException("Unexpected type for temporality value: " + value.getClass());
        }
    }
}
