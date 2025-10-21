/*
 * Copyright Elasticsearch B.V., and/or licensed to Elasticsearch B.V.
 * under one or more license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch B.V. licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 * This file is based on a modification of https://github.com/open-telemetry/opentelemetry-java which is licensed under the Apache 2.0 License.
 */

package org.elasticsearch.exponentialhistogram;

/**
 * The code in this class was copied and slightly adapted from the
 * <a href="https://github.com/open-telemetry/opentelemetry-java/blob/78a917da2e8f4bc3645f4fb10361e3e844aab9fb/sdk/metrics/src/main/java/io/opentelemetry/sdk/metrics/internal/aggregator/Base2ExponentialHistogramIndexer.java">OpenTelemetry Base2ExponentialHistogramIndexer implementation</a>,
 * licensed under the Apache License 2.0.
 */
class Base2ExponentialHistogramIndexer {

    /**
     * Bit mask used to isolate exponent of IEEE 754 double precision number.
     */
    private static final long EXPONENT_BIT_MASK = 0x7FF0000000000000L;

    /**
     * Bit mask used to isolate the significand of IEEE 754 double precision number.
     */
    private static final long SIGNIFICAND_BIT_MASK = 0xFFFFFFFFFFFFFL;

    /**
     * Bias used in representing the exponent of IEEE 754 double precision number.
     */
    private static final int EXPONENT_BIAS = 1023;

    /**
     * The number of bits used to represent the significand of IEEE 754 double precision number,
     * excluding the implicit bit.
     */
    private static final int SIGNIFICAND_WIDTH = 52;

    /**
     * The number of bits used to represent the exponent of IEEE 754 double precision number.
     */
    private static final int EXPONENT_WIDTH = 11;

    private static final double LOG_BASE2_E = 1D / Math.log(2);

    static long computeIndex(double value, int scale) {
        double absValue = Math.abs(value);
        // For positive scales, compute the index by logarithm, which is simpler but may be
        // inaccurate near bucket boundaries
        if (scale > 0) {
            return getIndexByLogarithm(absValue, scale);
        }
        // For scale zero, compute the exact index by extracting the exponent
        if (scale == 0) {
            return mapToIndexScaleZero(absValue);
        }
        // For negative scales, compute the exact index by extracting the exponent and shifting it to
        // the right by -scale
        return mapToIndexScaleZero(absValue) >> -scale;
    }

    /**
     * Compute the bucket index using a logarithm based approach.
     *
     * @see <a
     * href="https://github.com/open-telemetry/opentelemetry-specification/blob/main/specification/metrics/data-model.md#all-scales-use-the-logarithm-function">All
     * Scales: Use the Logarithm Function</a>
     */
    private static long getIndexByLogarithm(double value, int scale) {
        return (long) Math.ceil(Math.scalb(Math.log(value) * LOG_BASE2_E, scale)) - 1;
    }

    /**
     * Compute the exact bucket index for scale zero by extracting the exponent.
     *
     * @see <a
     * href="https://github.com/open-telemetry/opentelemetry-specification/blob/main/specification/metrics/data-model.md#scale-zero-extract-the-exponent">Scale
     * Zero: Extract the Exponent</a>
     */
    private static long mapToIndexScaleZero(double value) {
        long rawBits = Double.doubleToLongBits(value);
        long rawExponent = (rawBits & EXPONENT_BIT_MASK) >> SIGNIFICAND_WIDTH;
        long rawSignificand = rawBits & SIGNIFICAND_BIT_MASK;
        if (rawExponent == 0) {
            rawExponent -= Long.numberOfLeadingZeros(rawSignificand - 1) - EXPONENT_WIDTH - 1;
        }
        int ieeeExponent = (int) (rawExponent - EXPONENT_BIAS);
        if (rawSignificand == 0) {
            return ieeeExponent - 1;
        }
        return ieeeExponent;
    }
}
