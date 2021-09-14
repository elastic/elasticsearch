/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.utils;

/**
 * A collection of utilities related to intervals
 */
public class Intervals {

    private Intervals() {}

    /**
     * Aligns a {@code value} to a multiple of an {@code interval} by rounding down.
     * @param value the value to align to a multiple of the {@code interval}
     * @param interval the interval
     * @return the multiple of the {@code interval} that is less or equal to the {@code value}
     */
    public static long alignToFloor(long value, long interval) {
        long result = (value / interval) * interval;
        if (result == value || value >= 0) {
            return result;
        }
        return result - interval;
    }

    /**
     * Aligns a {@code value} to a multiple of an {@code interval} by rounding up.
     * @param value the value to align to a multiple of the {@code interval}
     * @param interval the interval
     * @return the multiple of the {@code interval} that is greater or equal to the {@code value}
     */
    public static long alignToCeil(long value, long interval) {
        long result = alignToFloor(value, interval);
        return result == value ? result : result + interval;
    }
}
