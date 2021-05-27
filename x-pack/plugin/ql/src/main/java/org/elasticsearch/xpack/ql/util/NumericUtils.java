/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ql.util;

import java.math.BigInteger;

public abstract class NumericUtils {
    // 18446744073709551615
    public static final BigInteger UNSIGNED_LONG_MAX = BigInteger.ONE.shiftLeft(Long.SIZE).subtract(BigInteger.ONE);

    // 18446744073709551615.0
    public static final double UNSIGNED_LONG_MAX_AS_DOUBLE = UNSIGNED_LONG_MAX.doubleValue();

    public static boolean isUnsignedLong(BigInteger bi) {
        return bi.signum() >= 0 && bi.compareTo(UNSIGNED_LONG_MAX) <= 0;
    }

    public static boolean inUnsignedLongRange(double d) {
        // UNSIGNED_LONG_MAX can't be represented precisely enough on a double, being converted as a rounded up value.
        // Converting it to a double and back will yield a larger unsigned long, so the double comparison is still preferred, but
        // it'll require the equality check. (BigDecimal comparisons only make sense for string-recovered floating point numbers.)
        // This also means that 18446744073709551615.0 is actually a double too high to be converted as an unsigned long.
        return d >= 0 && d < UNSIGNED_LONG_MAX_AS_DOUBLE;
    }

    public static BigInteger asUnsignedLong(BigInteger bi) {
        if (isUnsignedLong(bi) == false) {
            throw new ArithmeticException("unsigned_long overflow");
        }
        return bi;
    }
}
