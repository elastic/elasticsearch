/*
 * @notice
 * Copyright 2012 Jeff Hain
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * =============================================================================
 * Notice of fdlibm package this program is partially derived from:
 *
 * Copyright (C) 1993 by Sun Microsystems, Inc. All rights reserved.
 *
 * Developed at SunSoft, a Sun Microsystems, Inc. business.
 * Permission to use, copy, modify, and distribute this
 * software is freely granted, provided that this notice
 * is preserved.
 * =============================================================================
 *
 * This code sourced from:
 * https://github.com/yannrichet/jmathplot/blob/f25426e0ab0e68647ad2b75f577c7be050ecac86/src/main/java/org/math/plot/utils/FastMath.java
 */

package org.elasticsearch.core;

/**
 * Additions or modifications to this class should only come from the original org.math.plot.utils.FastMath source
 */
final class FastMath {

    private FastMath() {}

    // --------------------------------------------------------------------------
    // RE-USABLE CONSTANTS
    // --------------------------------------------------------------------------

    private static final double ONE_DIV_F2 = 1 / 2.0;
    private static final double ONE_DIV_F3 = 1 / 6.0;
    private static final double ONE_DIV_F4 = 1 / 24.0;
    private static final double TWO_POW_N28 = Double.longBitsToDouble(0x3E30000000000000L);
    private static final double TWO_POW_66 = Double.longBitsToDouble(0x4410000000000000L);
    private static final double LOG_DOUBLE_MAX_VALUE = StrictMath.log(Double.MAX_VALUE);
    // First double value (from zero) such as (value+-1/value == value).
    private static final double TWO_POW_27 = Double.longBitsToDouble(0x41A0000000000000L);
    private static final double TWO_POW_52 = Double.longBitsToDouble(0x4330000000000000L);
    // Smallest double normal value.
    private static final double MIN_DOUBLE_NORMAL = Double.longBitsToDouble(0x0010000000000000L); // 2.2250738585072014E-308
    private static final int MIN_DOUBLE_EXPONENT = -1074;
    private static final int MAX_DOUBLE_EXPONENT = 1023;
    private static final double LOG_2 = StrictMath.log(2.0);

    // --------------------------------------------------------------------------
    // CONSTANTS AND TABLES FOR ATAN
    // --------------------------------------------------------------------------

    // We use the formula atan(-x) = -atan(x)
    // ---> we only have to compute atan(x) on [0,+infinity[.
    // For values corresponding to angles not close to +-PI/2, we use look-up tables;
    // for values corresponding to angles near +-PI/2, we use code derived from fdlibm.

    // Supposed to be >= tan(67.7deg), as fdlibm code is supposed to work with values > 2.4375.
    private static final double ATAN_MAX_VALUE_FOR_TABS = StrictMath.tan(Math.toRadians(74.0));

    private static final int ATAN_TABS_SIZE = 1 << 12 + 1;
    private static final double ATAN_DELTA = ATAN_MAX_VALUE_FOR_TABS / (ATAN_TABS_SIZE - 1);
    private static final double ATAN_INDEXER = 1 / ATAN_DELTA;
    private static final double[] atanTab = new double[ATAN_TABS_SIZE];
    private static final double[] atanDer1DivF1Tab = new double[ATAN_TABS_SIZE];
    private static final double[] atanDer2DivF2Tab = new double[ATAN_TABS_SIZE];
    private static final double[] atanDer3DivF3Tab = new double[ATAN_TABS_SIZE];
    private static final double[] atanDer4DivF4Tab = new double[ATAN_TABS_SIZE];

    private static final double ATAN_HI3 = Double.longBitsToDouble(0x3ff921fb54442d18L); // 1.57079632679489655800e+00 atan(inf)hi
    private static final double ATAN_LO3 = Double.longBitsToDouble(0x3c91a62633145c07L); // 6.12323399573676603587e-17 atan(inf)lo
    private static final double ATAN_AT0 = Double.longBitsToDouble(0x3fd555555555550dL); // 3.33333333333329318027e-01
    private static final double ATAN_AT1 = Double.longBitsToDouble(0xbfc999999998ebc4L); // -1.99999999998764832476e-01
    private static final double ATAN_AT2 = Double.longBitsToDouble(0x3fc24924920083ffL); // 1.42857142725034663711e-01
    private static final double ATAN_AT3 = Double.longBitsToDouble(0xbfbc71c6fe231671L); // -1.11111104054623557880e-01
    private static final double ATAN_AT4 = Double.longBitsToDouble(0x3fb745cdc54c206eL); // 9.09088713343650656196e-02
    private static final double ATAN_AT5 = Double.longBitsToDouble(0xbfb3b0f2af749a6dL); // -7.69187620504482999495e-02
    private static final double ATAN_AT6 = Double.longBitsToDouble(0x3fb10d66a0d03d51L); // 6.66107313738753120669e-02
    private static final double ATAN_AT7 = Double.longBitsToDouble(0xbfadde2d52defd9aL); // -5.83357013379057348645e-02
    private static final double ATAN_AT8 = Double.longBitsToDouble(0x3fa97b4b24760debL); // 4.97687799461593236017e-02
    private static final double ATAN_AT9 = Double.longBitsToDouble(0xbfa2b4442c6a6c2fL); // -3.65315727442169155270e-02
    private static final double ATAN_AT10 = Double.longBitsToDouble(0x3f90ad3ae322da11L); // 1.62858201153657823623e-02

    // --------------------------------------------------------------------------
    // CONSTANTS AND TABLES FOR LOG AND LOG1P
    // --------------------------------------------------------------------------

    private static final int LOG_BITS = 12;
    private static final int LOG_TAB_SIZE = (1 << LOG_BITS);
    private static final double[] logXLogTab = new double[LOG_TAB_SIZE];
    private static final double[] logXTab = new double[LOG_TAB_SIZE];
    private static final double[] logXInvTab = new double[LOG_TAB_SIZE];

    // --------------------------------------------------------------------------
    // TABLE FOR POWERS OF TWO
    // --------------------------------------------------------------------------

    private static final double[] twoPowTab = new double[(MAX_DOUBLE_EXPONENT - MIN_DOUBLE_EXPONENT) + 1];

    static {
        // atan
        for (int i = 0; i < ATAN_TABS_SIZE; i++) {
            // x: in [0,ATAN_MAX_VALUE_FOR_TABS].
            double x = i * ATAN_DELTA;
            double onePlusXSqInv = 1.0 / (1 + x * x);
            double onePlusXSqInv2 = onePlusXSqInv * onePlusXSqInv;
            double onePlusXSqInv3 = onePlusXSqInv2 * onePlusXSqInv;
            double onePlusXSqInv4 = onePlusXSqInv2 * onePlusXSqInv2;
            atanTab[i] = StrictMath.atan(x);
            atanDer1DivF1Tab[i] = onePlusXSqInv;
            atanDer2DivF2Tab[i] = (-2 * x * onePlusXSqInv2) * ONE_DIV_F2;
            atanDer3DivF3Tab[i] = ((-2 + 6 * x * x) * onePlusXSqInv3) * ONE_DIV_F3;
            atanDer4DivF4Tab[i] = ((24 * x * (1 - x * x)) * onePlusXSqInv4) * ONE_DIV_F4;
        }

        // log

        for (int i = 0; i < LOG_TAB_SIZE; i++) {
            // Exact to use inverse of tab size, since it is a power of two.
            double x = 1 + i * (1.0 / LOG_TAB_SIZE);
            logXLogTab[i] = StrictMath.log(x);
            logXTab[i] = x;
            logXInvTab[i] = 1 / x;
        }

        // twoPow

        for (int i = MIN_DOUBLE_EXPONENT; i <= MAX_DOUBLE_EXPONENT; i++) {
            twoPowTab[i - MIN_DOUBLE_EXPONENT] = StrictMath.pow(2.0, i);
        }

    }

    /**
     * A faster and less accurate {@link Math#sinh}
     *
     * @param value A double value.
     * @return Value hyperbolic sine.
     */
    public static double sinh(double value) {
        // sinh(x) = (exp(x)-exp(-x))/2
        double h;
        if (value < 0.0) {
            value = -value;
            h = -0.5;
        } else {
            h = 0.5;
        }
        if (value < 22.0) {
            if (value < TWO_POW_N28) {
                return (h < 0.0) ? -value : value;
            } else {
                double t = Math.expm1(value);
                // Might be more accurate, if value < 1: return h*((t+t)-t*t/(t+1.0)).
                return h * (t + t / (t + 1.0));
            }
        } else if (value < LOG_DOUBLE_MAX_VALUE) {
            return h * Math.exp(value);
        } else {
            double t = Math.exp(value * 0.5);
            return (h * t) * t;
        }
    }

    /**
     * A faster and less accurate {@link Math#atan}
     *
     * @param value A double value.
     * @return Value arctangent, in radians, in [-PI/2,PI/2].
     */
    public static double atan(double value) {
        boolean negateResult;
        if (value < 0.0) {
            value = -value;
            negateResult = true;
        } else {
            negateResult = false;
        }
        if (value == 1.0) {
            // We want "exact" result for 1.0.
            return negateResult ? -Math.PI / 4 : Math.PI / 4;
        } else if (value <= ATAN_MAX_VALUE_FOR_TABS) {
            int index = (int) (value * ATAN_INDEXER + 0.5);
            double delta = value - index * ATAN_DELTA;
            double result = atanTab[index] + delta * (atanDer1DivF1Tab[index] + delta * (atanDer2DivF2Tab[index] + delta
                * (atanDer3DivF3Tab[index] + delta * atanDer4DivF4Tab[index])));
            return negateResult ? -result : result;
        } else { // value > ATAN_MAX_VALUE_FOR_TABS, or value is NaN
            // This part is derived from fdlibm.
            if (value < TWO_POW_66) {
                double x = -1 / value;
                double x2 = x * x;
                double x4 = x2 * x2;
                double s1 = x2 * (ATAN_AT0 + x4 * (ATAN_AT2 + x4 * (ATAN_AT4 + x4 * (ATAN_AT6 + x4 * (ATAN_AT8 + x4 * ATAN_AT10)))));
                double s2 = x4 * (ATAN_AT1 + x4 * (ATAN_AT3 + x4 * (ATAN_AT5 + x4 * (ATAN_AT7 + x4 * ATAN_AT9))));
                double result = ATAN_HI3 - ((x * (s1 + s2) - ATAN_LO3) - x);
                return negateResult ? -result : result;
            } else { // value >= 2^66, or value is NaN
                if (Double.isNaN(value)) {
                    return Double.NaN;
                } else {
                    return negateResult ? -Math.PI / 2 : Math.PI / 2;
                }
            }
        }
    }

    /**
     * @param value A double value.
     * @return Value logarithm (base e).
     */
    public static double log(double value) {
        if (value > 0.0) {
            if (value == Double.POSITIVE_INFINITY) {
                return Double.POSITIVE_INFINITY;
            }

            // For normal values not close to 1.0, we use the following formula:
            // log(value)
            // = log(2^exponent*1.mantissa)
            // = log(2^exponent) + log(1.mantissa)
            // = exponent * log(2) + log(1.mantissa)
            // = exponent * log(2) + log(1.mantissaApprox) + log(1.mantissa/1.mantissaApprox)
            // = exponent * log(2) + log(1.mantissaApprox) + log(1+epsilon)
            // = exponent * log(2) + log(1.mantissaApprox) + epsilon-epsilon^2/2+epsilon^3/3-epsilon^4/4+...
            // with:
            // 1.mantissaApprox <= 1.mantissa,
            // log(1.mantissaApprox) in table,
            // epsilon = (1.mantissa/1.mantissaApprox)-1
            //
            // To avoid bad relative error for small results,
            // values close to 1.0 are treated aside, with the formula:
            // log(x) = z*(2+z^2*((2.0/3)+z^2*((2.0/5))+z^2*((2.0/7))+...)))
            // with z=(x-1)/(x+1)

            double h;
            if (value > 0.95) {
                if (value < 1.14) {
                    double z = (value - 1.0) / (value + 1.0);
                    double z2 = z * z;
                    return z * (2 + z2 * ((2.0 / 3) + z2 * ((2.0 / 5) + z2 * ((2.0 / 7) + z2 * ((2.0 / 9) + z2 * ((2.0 / 11)))))));
                }
                h = 0.0;
            } else if (value < MIN_DOUBLE_NORMAL) {
                // Ensuring value is normal.
                value *= TWO_POW_52;
                // log(x*2^52)
                // = log(x)-ln(2^52)
                // = log(x)-52*ln(2)
                h = -52 * LOG_2;
            } else {
                h = 0.0;
            }

            int valueBitsHi = (int) (Double.doubleToRawLongBits(value) >> 32);
            int valueExp = (valueBitsHi >> 20) - MAX_DOUBLE_EXPONENT;
            // Getting the first LOG_BITS bits of the mantissa.
            int xIndex = ((valueBitsHi << 12) >>> (32 - LOG_BITS));

            // 1.mantissa/1.mantissaApprox - 1
            double z = (value * twoPowTab[-valueExp - MIN_DOUBLE_EXPONENT]) * logXInvTab[xIndex] - 1;

            z *= (1 - z * ((1.0 / 2) - z * ((1.0 / 3))));

            return h + valueExp * LOG_2 + (logXLogTab[xIndex] + z);

        } else if (value == 0.0) {
            return Double.NEGATIVE_INFINITY;
        } else { // value < 0.0, or value is NaN
            return Double.NaN;
        }
    }
}
