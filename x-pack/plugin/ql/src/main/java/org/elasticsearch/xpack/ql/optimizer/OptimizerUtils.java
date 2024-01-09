/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ql.optimizer;

public class OptimizerUtils {
    private static final int HALF_FLOAT_MAX = 65504;

    /**
     * Check if an object  is a numerical value that fits into an integer.
     */
    public static boolean isInIntegerRange(Object num) {
        if (num instanceof Integer || num instanceof Short || num instanceof Byte) {
            return true;
        }
        if (num instanceof Long l) {
            long value = l;
            return value <= Integer.MAX_VALUE && value >= Integer.MIN_VALUE;
        }
        if (num instanceof Float f) {
            float value = f;
            return value <= Integer.MAX_VALUE && value >= Integer.MIN_VALUE;
        }
        if (num instanceof Double d) {
            double value = d;
            return value <= Integer.MAX_VALUE && value >= Integer.MIN_VALUE;
        }

        return false;
    }

    /**
     * Check if an object  is a numerical value that fits into a half_float.
     */
    public static boolean isInHalfFloatRange(Object num) {
        if (num instanceof Short || num instanceof Byte) {
            return true;
        }
        if (num instanceof Integer i) {
            int value = i;
            return value <= HALF_FLOAT_MAX && value >= -HALF_FLOAT_MAX;
        }
        if (num instanceof Long l) {
            long value = l;
            return value <= HALF_FLOAT_MAX && value >= -HALF_FLOAT_MAX;
        }
        if (num instanceof Float f) {
            float value = f;
            return value <= HALF_FLOAT_MAX && value >= -HALF_FLOAT_MAX;
        }
        if (num instanceof Double d) {
            double value = d;
            return value <= HALF_FLOAT_MAX && value >= -HALF_FLOAT_MAX;
        }

        return false;
    }

    /**
     * Check if an object is a numerical value that fits into a float.
     */
    public static boolean isInFloatRange(Object num) {
        if (num instanceof Float || num instanceof Long || num instanceof Integer || num instanceof Short || num instanceof Byte) {
            return true;
        }
        if (num instanceof Double d) {
            double value = d;
            return value <= Float.MAX_VALUE && value >= -Float.MAX_VALUE;
        }

        return false;
    }

    /**
     * Check if a folded expression is a positive numerical value.
     */
    public static boolean isPositive(Object num) {
        if (num == null) return false;

        if (num instanceof Byte val) return val > 0;
        if (num instanceof Short val) return val > 0;
        if (num instanceof Integer val) return val > 0;
        if (num instanceof Long val) return val > 0;
        if (num instanceof Float val) return val > 0;
        if (num instanceof Double val) return val > 0;

        return false;
    }
}
