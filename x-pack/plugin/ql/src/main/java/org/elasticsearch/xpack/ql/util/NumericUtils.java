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

    // 0x8000000000000000
    public static final long TWOS_COMPLEMENT_BITMASK = Long.MIN_VALUE;
    // 9223372036854775808 == 0x8000000000000000
    public static final BigInteger LONG_MAX_PLUS_ONE_AS_BIGINTEGER = BigInteger.ONE.shiftLeft(Long.SIZE - 1);
    // 9223372036854775808.0
    public static final double LONG_MAX_PLUS_ONE_AS_DOUBLE = LONG_MAX_PLUS_ONE_AS_BIGINTEGER.doubleValue();
    public static final long ONE_AS_UNSIGNED_LONG = asLongUnsigned(BigInteger.ONE);
    public static final long ZERO_AS_UNSIGNED_LONG = asLongUnsigned(BigInteger.ZERO);

    private static final String UNSIGNED_LONG_OVERFLOW = "unsigned_long overflow";

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
            throw new ArithmeticException(UNSIGNED_LONG_OVERFLOW);
        }
        return bi;
    }

    /**
     * Converts a BigInteger holding an unsigned_long to its (signed) long representation.
     * There's no checking on the input value, if this is negative or exceeds unsigned_long range -- call
     * {@link #isUnsignedLong(BigInteger)} if needed.
     * @param ul The unsigned_long value to convert.
     * @return The long representation of the unsigned_long.
     */
    public static long asLongUnsigned(BigInteger ul) {
        if (ul.bitLength() < Long.SIZE) {
            return twosComplement(ul.longValue());
        } else {
            return ul.subtract(LONG_MAX_PLUS_ONE_AS_BIGINTEGER).longValue();
        }
    }

    /**
     * Converts a long value to an unsigned long stored as a (signed) long.
     * @param ul Long value to convert to unsigned long
     * @return The long representation of the converted unsigned long.
     */
    public static long asLongUnsigned(long ul) {
        return twosComplement(ul);
    }

    /**
     * Converts an unsigned long value "encoded" into a (signed) long to a Number, holding the "expanded" value. This can be either a
     * Long (if original value fits), or a BigInteger, otherwise.
     * <p>
     *     An unsigned long is converted to a (signed) long by adding Long.MIN_VALUE (or subtracting "abs"(Long.MIN_VALUE), so that
     *     [0, "abs"(MIN_VALUE) + MAX_VALUE] becomes [MIN_VALUE, MAX_VALUE]) before storing the result. When recovering the original value:
     *     - if the result is negative, the unsigned long value has been less than Long.MAX_VALUE, so recovering it requires adding the
     *     Long.MIN_VALUE back; this is equivalent to 2-complementing it; the function returns a Long;
     *     - if the result remained positive, the value was greater than Long.MAX_VALUE, so we need to add that back; the function returns
     *     a BigInteger.
     * </p>
     * @param l "Encoded" unsigned long.
     * @return Number, holding the "decoded" value.
     */
    public static Number unsignedLongAsNumber(long l) {
        return l < 0 ? twosComplement(l) : LONG_MAX_PLUS_ONE_AS_BIGINTEGER.add(BigInteger.valueOf(l));
    }

    public static BigInteger unsignedLongAsBigInteger(long l) {
        return l < 0 ? BigInteger.valueOf(twosComplement(l)) : LONG_MAX_PLUS_ONE_AS_BIGINTEGER.add(BigInteger.valueOf(l));
    }

    public static double unsignedLongToDouble(long l) {
        return l < 0 ? twosComplement(l) : LONG_MAX_PLUS_ONE_AS_DOUBLE + l;
    }

    public static long unsignedLongAddExact(long x, long y) {
        long s;
        if (
        // both operands are positive, so the UL equivalents are >= Long.MAX_VALUE + 1, so sum will be above UNSIGNED_LONG_MAX
        (x | y) >= 0
            // if operands have opposing signs, the UL corresponding to the positive one is >= Long.MAX_VALUE + 1 and
            // the UL corresponding to the negative one between [0, Long.MAX_VALUE] ==> non-negative sum means value wrap, i.e. overflow
            || ((s = (x + y)) >= 0 && (x ^ y) < 0)) {
            throw new ArithmeticException(UNSIGNED_LONG_OVERFLOW);
        }
        return asLongUnsigned(s);
    }

    public static long unsignedLongSubtractExact(long x, long y) {
        if (x < y) { // UL keeps the ordering after shifting to fit into long range
            throw new ArithmeticException(UNSIGNED_LONG_OVERFLOW);
        }
        return asLongUnsigned(x - y);
    }

    public static long unsignedLongMultiplyExact(long x, long y) {
        long ux = asLongUnsigned(x);
        long uy = asLongUnsigned(y);
        if (unsignedLongMultiplyHigh(ux, uy) != 0) { // TODO: replace with Math#unsignedMultiplyHigh() in JDK 18 when available
            throw new ArithmeticException(UNSIGNED_LONG_OVERFLOW);
        }
        return asLongUnsigned(ux * uy);
    }

    public static long unsignedLongMultiplyHigh(long x, long y) {
        return Math.multiplyHigh(x, y) + (y & (x >> 63)) + (x & (y >> 63));
    }

    private static long twosComplement(long l) {
        return l ^ TWOS_COMPLEMENT_BITMASK;
    }

    /**
     * Check if the provided double is both finite and a number (i.e. not Double.NaN).
     * @param dbl The double to verify.
     * @return The input value.
     * @throws ArithmeticException if the provided double is either infinite or not a number.
     */
    public static double asFiniteNumber(double dbl) {
        if (Double.isNaN(dbl) || Double.isInfinite(dbl)) {
            throw new ArithmeticException("not a finite double number: " + dbl);
        }
        return dbl;
    }

    /**
     * Converts a number to an integer, saturating that integer if the number doesn't fit naturally.  That is to say, values
     * greater than Integer.MAX_VALUE yield Integer.MAX_VALUE and values less than Integer.MIN_VALUE yield Integer.MIN_VALUE
     *
     * This function exists because Long::intValue() yields -1 and 0 for Long.MAX_VALUE and Long.MIN_VALUE, respectively.
     *
     * @param n the nubmer to convert
     * @return a valid integer
     */
    public static int saturatingIntValue(Number n) {
        if (n instanceof Long ln) {
            if (ln > Integer.MAX_VALUE) {
                return Integer.MAX_VALUE;
            }
            if (ln < Integer.MIN_VALUE) {
                return Integer.MIN_VALUE;
            }
        }
        return n.intValue();
    }
}
