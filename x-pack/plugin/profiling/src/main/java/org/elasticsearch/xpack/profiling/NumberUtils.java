/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.profiling;

final class NumberUtils {
    private NumberUtils() {
        // no instances intended
    }

    /**
     * Converts a positive double number to a string.
     *
     * @param value The double value.
     * @return The corresponding string representation rounded to four fractional digits.
     */
    public static String doubleToString(double value) {
        if (value < 0.0001d) {
            return "0";
        }
        StringBuilder sb = new StringBuilder();
        int i = (int) value;
        int f = (int) ((value - i) * 10000.0d + 0.5d);
        sb.append(i);
        sb.append(".");
        if (f < 10) {
            sb.append("000");
        } else if (f < 100) {
            sb.append("00");
        } else if (f < 1000) {
            sb.append("0");
        }
        sb.append(f);
        return sb.toString();
    }
}
