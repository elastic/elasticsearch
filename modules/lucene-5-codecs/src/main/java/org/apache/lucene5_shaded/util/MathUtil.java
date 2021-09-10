/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.lucene5_shaded.util;


import java.math.BigInteger;

/**
 * Math static utility methods.
 */
public final class MathUtil {

  // No instance:
  private MathUtil() {
  }

  /**
   * Returns {@code x <= 0 ? 0 : Math.floor(Math.log(x) / Math.log(base))}
   * @param base must be {@code > 1}
   */
  public static int log(long x, int base) {
    if (base <= 1) {
      throw new IllegalArgumentException("base must be > 1");
    }
    int ret = 0;
    while (x >= base) {
      x /= base;
      ret++;
    }
    return ret;
  }

  /**
   * Calculates logarithm in a given base with doubles.
   */
  public static double log(double base, double x) {
    return Math.log(x) / Math.log(base);
  }

  /** Return the greatest common divisor of <code>a</code> and <code>b</code>,
   *  consistently with {@link BigInteger#gcd(BigInteger)}.
   *  <p><b>NOTE</b>: A greatest common divisor must be positive, but
   *  <code>2^64</code> cannot be expressed as a long although it
   *  is the GCD of {@link Long#MIN_VALUE} and <code>0</code> and the GCD of
   *  {@link Long#MIN_VALUE} and {@link Long#MIN_VALUE}. So in these 2 cases,
   *  and only them, this method will return {@link Long#MIN_VALUE}. */
  // see http://en.wikipedia.org/wiki/Binary_GCD_algorithm#Iterative_version_in_C.2B.2B_using_ctz_.28count_trailing_zeros.29
  public static long gcd(long a, long b) {
    a = Math.abs(a);
    b = Math.abs(b);
    if (a == 0) {
      return b;
    } else if (b == 0) {
      return a;
    }
    final int commonTrailingZeros = Long.numberOfTrailingZeros(a | b);
    a >>>= Long.numberOfTrailingZeros(a);
    while (true) {
      b >>>= Long.numberOfTrailingZeros(b);
      if (a == b) {
        break;
      } else if (a > b || a == Long.MIN_VALUE) { // MIN_VALUE is treated as 2^64
        final long tmp = a;
        a = b;
        b = tmp;
      }
      if (a == 1) {
        break;
      }
      b -= a;
    }
    return a << commonTrailingZeros;
  }


  /**
   * Calculates inverse hyperbolic sine of a {@code double} value.
   * <p>
   * Special cases:
   * <ul>
   *    <li>If the argument is NaN, then the result is NaN.
   *    <li>If the argument is zero, then the result is a zero with the same sign as the argument.
   *    <li>If the argument is infinite, then the result is infinity with the same sign as the argument.
   * </ul>
   */
  public static double asinh(double a) {
    final double sign;
    // check the sign bit of the raw representation to handle -0
    if (Double.doubleToRawLongBits(a) < 0) {
      a = Math.abs(a);
      sign = -1.0d;
    } else {
      sign = 1.0d;
    }

    return sign * Math.log(Math.sqrt(a * a + 1.0d) + a);
  }

  /**
   * Calculates inverse hyperbolic cosine of a {@code double} value.
   * <p>
   * Special cases:
   * <ul>
   *    <li>If the argument is NaN, then the result is NaN.
   *    <li>If the argument is +1, then the result is a zero.
   *    <li>If the argument is positive infinity, then the result is positive infinity.
   *    <li>If the argument is less than 1, then the result is NaN.
   * </ul>
   */
  public static double acosh(double a) {
    return Math.log(Math.sqrt(a * a - 1.0d) + a);
  }

  /**
   * Calculates inverse hyperbolic tangent of a {@code double} value.
   * <p>
   * Special cases:
   * <ul>
   *    <li>If the argument is NaN, then the result is NaN.
   *    <li>If the argument is zero, then the result is a zero with the same sign as the argument.
   *    <li>If the argument is +1, then the result is positive infinity.
   *    <li>If the argument is -1, then the result is negative infinity.
   *    <li>If the argument's absolute value is greater than 1, then the result is NaN.
   * </ul>
   */
  public static double atanh(double a) {
    final double mult;
    // check the sign bit of the raw representation to handle -0
    if (Double.doubleToRawLongBits(a) < 0) {
      a = Math.abs(a);
      mult = -0.5d;
    } else {
      mult = 0.5d;
    }
    return mult * Math.log((1.0d + a) / (1.0d - a));
  }


}
