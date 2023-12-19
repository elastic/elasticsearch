/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.elasticsearch.xpack.exponentialhistogram.otel;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public final class Base2ExponentialHistogramIndexer {

  private static final Map<Integer, Base2ExponentialHistogramIndexer> cache =
      new ConcurrentHashMap<>();

  /** Bit mask used to isolate exponent of IEEE 754 double precision number. */
  private static final long EXPONENT_BIT_MASK = 0x7FF0000000000000L;

  /** Bit mask used to isolate the significand of IEEE 754 double precision number. */
  private static final long SIGNIFICAND_BIT_MASK = 0xFFFFFFFFFFFFFL;

  /** Bias used in representing the exponent of IEEE 754 double precision number. */
  private static final int EXPONENT_BIAS = 1023;

  /**
   * The number of bits used to represent the significand of IEEE 754 double precision number,
   * excluding the implicit bit.
   */
  private static final int SIGNIFICAND_WIDTH = 52;

  /** The number of bits used to represent the exponent of IEEE 754 double precision number. */
  private static final int EXPONENT_WIDTH = 11;

  private static final double LOG_BASE2_E = 1D / Math.log(2);

  private final int scale;
  private final double scaleFactor;

  private Base2ExponentialHistogramIndexer(int scale) {
    this.scale = scale;
    this.scaleFactor = computeScaleFactor(scale);
  }

  /** Get an indexer for the given scale. Indexers are cached and reused for performance. */
  public static Base2ExponentialHistogramIndexer get(int scale) {
    return cache.computeIfAbsent(scale, unused -> new Base2ExponentialHistogramIndexer(scale));
  }

  /**
   * Compute the index for the given value.
   *
   * <p>The algorithm to retrieve the index is specified in the <a
   * href="https://github.com/open-telemetry/opentelemetry-specification/blob/main/specification/metrics/data-model.md#exponential-buckets">OpenTelemetry
   * specification</a>.
   *
   * @param value Measured value (must be non-zero).
   * @return the index of the bucket which the value maps to.
   */
  public int computeIndex(double value) {
    double absValue = Math.abs(value);
    // For positive scales, compute the index by logarithm, which is simpler but may be
    // inaccurate near bucket boundaries
    if (scale > 0) {
      return getIndexByLogarithm(absValue);
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
   *     href="https://github.com/open-telemetry/opentelemetry-specification/blob/main/specification/metrics/data-model.md#all-scales-use-the-logarithm-function">All
   *     Scales: Use the Logarithm Function</a>
   */
  private int getIndexByLogarithm(double value) {
    return (int) Math.ceil(Math.log(value) * scaleFactor) - 1;
  }

  /**
   * Compute the exact bucket index for scale zero by extracting the exponent.
   *
   * @see <a
   *     href="https://github.com/open-telemetry/opentelemetry-specification/blob/main/specification/metrics/data-model.md#scale-zero-extract-the-exponent">Scale
   *     Zero: Extract the Exponent</a>
   */
  private static int mapToIndexScaleZero(double value) {
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

  private static double computeScaleFactor(int scale) {
    return Math.scalb(LOG_BASE2_E, scale);
  }
}
