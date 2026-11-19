/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.unit;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;

import java.io.IOException;

/**
 * Utility class to represent ratio and percentage values between 0 and 100
 */
public class RatioValue implements Writeable {
    public static final RatioValue ZERO_PERCENT = new RatioValue(0);
    public static final RatioValue ONE_HUNDRED_PERCENT = new RatioValue(100);

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
     * Creates a {@link RatioValue} from a percentage value (i.e., in [0,100]).
     *
     * @param percent the percentage value
     * @return a {@link RatioValue} representing the given percentage
     */
    public static RatioValue ofPercent(double percent) {
        return new RatioValue(percent);
    }

    /**
     * Parses the provided string as a {@link RatioValue}, the string can
     * either be in percentage format (eg. 73.5%), or a floating-point ratio
     * format (eg. 0.735)
     *
     * @throws ElasticsearchParseException if the provided string represents a percentage outside [0,100] or ratio outside [0,1],
     *                                     or if the provided string cannot be parsed as a double
     */
    public static RatioValue parseRatioValue(String sValue) {
        return parseRatioValue(sValue, RatioValue.ZERO_PERCENT, RatioValue.ONE_HUNDRED_PERCENT);
    }

    /**
     * Parses the provided string as a {@link RatioValue}, the string can
     * either be in percentage format (eg. 73.5%), or a floating-point ratio
     * format (eg. 0.735)
     *
     * @throws ElasticsearchParseException if the provided string represents a value outside
     *                                     [{@code lowerBoundInclusive},{@code upperBoundInclusive}],
     *                                     or if the provided string cannot be parsed as a double
     */
    public static RatioValue parseRatioValue(String sValue, RatioValue lowerBoundInclusive, RatioValue upperBoundInclusive) {
        assert lowerBoundInclusive.getAsPercent() <= upperBoundInclusive.getAsPercent();
        if (sValue.endsWith("%")) {
            final String percentAsString = sValue.substring(0, sValue.length() - 1);
            try {
                final double percent = Double.parseDouble(percentAsString);
                if (percent < lowerBoundInclusive.getAsPercent() || percent > upperBoundInclusive.getAsPercent()) {
                    throw new ElasticsearchParseException(
                        "Percentage should be in [{}-{}], got [{}]",
                        formatNoTrailingZeros(lowerBoundInclusive.getAsPercent()),
                        formatNoTrailingZeros(upperBoundInclusive.getAsPercent()),
                        percentAsString
                    );
                }
                return new RatioValue(Math.abs(percent));
            } catch (NumberFormatException e) {
                throw new ElasticsearchParseException("Failed to parse [{}] as a double", e, percentAsString);
            }
        } else {
            try {
                double ratio = Double.parseDouble(sValue);
                if (ratio < lowerBoundInclusive.getAsRatio() || ratio > upperBoundInclusive.getAsRatio()) {
                    throw new ElasticsearchParseException(
                        "Ratio should be in [{}-{}], got [{}]",
                        formatNoTrailingZeros(lowerBoundInclusive.getAsRatio()),
                        formatNoTrailingZeros(upperBoundInclusive.getAsRatio()),
                        formatNoTrailingZeros(ratio)
                    );
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
        return formatNoTrailingZeros(getAsPercent()) + "%";
    }

    private static String formatNoTrailingZeros(double doubleValue) {
        String value = String.valueOf(doubleValue);
        int i = value.length() - 1;
        while (i >= 0 && value.charAt(i) == '0') {
            i--;
        }
        if (i < 0) {
            return "0";
        } else if (value.charAt(i) == '.') {
            return value.substring(0, i);
        } else {
            return value.substring(0, Math.min(i + 1, value.length()));
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeDouble(percent);
    }

    public static RatioValue readFrom(StreamInput in) throws IOException {
        return new RatioValue(in.readDouble());
    }
}
