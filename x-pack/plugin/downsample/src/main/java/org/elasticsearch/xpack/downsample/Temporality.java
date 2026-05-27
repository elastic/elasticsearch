/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.downsample;

/**
 * Represents the metric temporality for a time series, controlling how counter values are interpreted during downsampling.
 * <ul>
 *     <li>{@link #CUMULATIVE}: counter values are monotonically increasing (reset detection applies).</li>
 *     <li>{@link #DELTA}: counter values represent increments per interval (summed during downsampling).</li>
 *     <li>{@link #DEFAULT}: no explicit temporality was provided; falls back to cumulative behavior for counters.</li>
 * </ul>
 */
enum Temporality {
    DELTA,
    CUMULATIVE,
    DEFAULT;

    static Temporality fromDimensionValue(Object value) {
        if (value == null) {
            return DEFAULT;
        }
        String s = value.toString();
        if ("delta".equals(s)) {
            return DELTA;
        } else if ("cumulative".equals(s)) {
            return CUMULATIVE;
        }
        return DEFAULT;
    }
}
