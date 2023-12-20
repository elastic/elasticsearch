/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.unit;

import org.elasticsearch.ElasticsearchParseException;

/**
 * Utility class to represent ratio and percentage values between 0 and 100
 */
public class RatioValue {
    private final double percent;

    public RatioValue(double percent) {
        this.percent = percent;
    }

    public double getAsRatio() {
        return this.percent / 100.0;
    }

    public double getAsPercent() {
        return this.percent;
    }

    @Override
    public String toString() {
        return this.percent + "%";
    }

    /**
     * Parses the provided string as a {@link RatioValue}, the string can
     * either be in percentage format (eg. 73.5%), or a floating-point ratio
     * format (eg. 0.735)
     */
    public static RatioValue parseRatioValue(String sValue) {
        if (sValue.endsWith("%")) {
            final String percentAsString = sValue.substring(0, sValue.length() - 1);
            try {
                final double percent = Double.parseDouble(percentAsString);
                if (percent < 0 || percent > 100) {
                    throw new ElasticsearchParseException("Percentage should be in [0-100], got [{}]", percentAsString);
                }
                return new RatioValue(Math.abs(percent));
            } catch (NumberFormatException e) {
                throw new ElasticsearchParseException("Failed to parse [{}] as a double", e, percentAsString);
            }
        } else {
            try {
                double ratio = Double.parseDouble(sValue);
                if (ratio < 0 || ratio > 1.0) {
                    throw new ElasticsearchParseException("Ratio should be in [0-1.0], got [{}]", ratio);
                }
                return new RatioValue(100.0 * Math.abs(ratio));
            } catch (NumberFormatException e) {
                throw new ElasticsearchParseException("Invalid ratio or percentage [{}]", sValue);
            }

        }
    }

    /**
     * Returns the percent as a string with no trailing zeros and the '%' suffix.
     */
    public String formatNoTrailingZerosPercent() {
        String value = String.valueOf(getAsPercent());
        int i = value.length() - 1;
        while (i >= 0 && value.charAt(i) == '0') {
            i--;
        }
        if (i < 0) {
            return "0%";
        } else if (value.charAt(i) == '.') {
            return value.substring(0, i) + "%";
        } else {
            return value.substring(0, Math.min(i + 1, value.length())) + "%";
        }
    }
}
