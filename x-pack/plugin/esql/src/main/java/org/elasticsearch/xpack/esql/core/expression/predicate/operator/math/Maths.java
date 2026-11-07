/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.core.expression.predicate.operator.math;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.MathContext;

import static org.elasticsearch.xpack.esql.core.type.DataTypeConverter.safeToInt;

public final class Maths {

    /**
     * Half of 10^19, the smallest power of ten that does not fit into a long. A long rounded at that
     * scale is either zero or +/-10^19, so this is the only threshold needed to tell the two apart.
     */
    private static final long HALF_OF_TEN_POW_19 = 5_000_000_000_000_000_000L;

    public static Number round(Number n, long precision) throws ArithmeticException {
        if (n instanceof Long || n instanceof Integer || n instanceof Short || n instanceof Byte) {
            return convertToIntegerType(round(n.longValue(), precision), n.getClass());
        }
        double nDouble = n.doubleValue();
        if (Double.isNaN(nDouble)) {
            return n instanceof Float ? 0.0f : 0.0d;
        }

        double tenAtScale = tenPower(precision);
        if (tenAtScale == 0.0 || nDouble == 0.0) {
            return n instanceof Float ? 0.0f : 0.0d;
        }

        double middleResult = nDouble * tenAtScale;
        int sign = middleResult >= 0 ? 1 : -1;

        if (Double.POSITIVE_INFINITY == middleResult || Double.NEGATIVE_INFINITY == middleResult) {
            return n;
        }
        if (Long.MIN_VALUE + 1 < middleResult && middleResult < Long.MAX_VALUE) {
            // the result can still be rounded using Math.round(), that is limited to long values
            Double result = Math.round(Math.abs(middleResult)) / tenAtScale * sign;
            return n instanceof Float ? result.floatValue() : result;
        }

        // otherwise fall back to BigDecimal, that is ~40x slower, but works fine
        MathContext prec = MathContext.DECIMAL128;
        Double result = new BigDecimal(Math.abs(middleResult), prec).round(new MathContext(0))
            .divide(new BigDecimal(tenAtScale), prec)
            .doubleValue() * sign;
        return n instanceof Float ? result.floatValue() : result;
    }

    public static BigInteger round(BigInteger n, long precision) throws ArithmeticException {
        if (n.signum() == 0 || precision > 0) {
            return n;
        }

        int digitsToRound = safeToInt(-precision); // TODO: why is precision a long?
        BigInteger tenAtScaleMinusOne = BigInteger.TEN.pow(digitsToRound - 1);
        BigInteger tenAtScale = tenAtScaleMinusOne.multiply(BigInteger.TEN);
        BigInteger middleResult = n.divide(tenAtScale); // TODO: "intermediateResult"?
        BigInteger remainder = n.mod(tenAtScale);
        BigInteger having = tenAtScaleMinusOne.multiply(BigInteger.valueOf(5));
        if (remainder.compareTo(having) >= 0) {
            middleResult = middleResult.add(BigInteger.ONE);
        } else if (remainder.compareTo(having.negate()) <= 0) {
            middleResult = middleResult.subtract(BigInteger.ONE);
        }

        return middleResult.multiply(tenAtScale);
    }

    /**
     * Rounds {@code n} half away from zero at a scale of {@code 10^-precision}.
     * <p>
     * Note there is deliberately no shortcut for numbers with fewer digits than the scale: a leading
     * digit of 5 or more rounds up to the next power of ten rather than to zero, so {@code ROUND(5, -1)}
     * is 10 and not 0. Only truncation may take that shortcut, and {@link #truncate} still does.
     *
     * @throws ArithmeticException if the rounded value does not fit into a long
     */
    public static Long round(long n, long precision) throws ArithmeticException {
        if (n == 0L || precision >= 0) {
            return n;
        }
        // Every long is below 10^19, so at a scale of 10^20 or more it is smaller than half of the scale
        // and rounds to zero. Testing before negating also keeps -Long.MIN_VALUE from overflowing back
        // to a negative value, which would leave the scale nonsensical.
        if (precision <= -20) {
            return 0L;
        }

        int digitsToRound = (int) -precision;
        if (digitsToRound == 19) {
            // the result is either zero or +/-10^19, and 10^19 does not fit into a long
            if (n >= HALF_OF_TEN_POW_19 || n <= -HALF_OF_TEN_POW_19) {
                throw new ArithmeticException("long overflow");
            }
            return 0L;
        }

        long tenAtScale = (long) tenPower(digitsToRound);
        long middleResult = n / tenAtScale;
        long remainder = n % tenAtScale; // TODO: vs.: n - middleResult * tenAtScale
        long halving = tenAtScale / 2;
        if (remainder >= halving) {
            middleResult++;
        } else if (remainder <= -halving) {
            middleResult--;
        }

        // multiplyExact reports the overflow that a signum comparison cannot, since a legitimate
        // result of zero is indistinguishable from a wrapped one by sign alone
        return Math.multiplyExact(middleResult, tenAtScale);
    }

    public static Number truncate(Number n, Number precision) {
        long longPrecision = precision.longValue();
        if (n instanceof Long || n instanceof Integer || n instanceof Short || n instanceof Byte) {
            long nLong = n.longValue();
            if (nLong == 0L || longPrecision >= 0) {
                return n;
            }
            // Every long is below 10^19, so truncating at that scale or beyond always yields zero.
            // Testing before negating also keeps -Long.MIN_VALUE from overflowing back to a negative
            // value, which would drive tenPower() to zero and divide by zero below.
            if (longPrecision <= -19) {
                return convertToIntegerType(0L, n.getClass());
            }

            long digitsToTruncate = -longPrecision;
            int digits = (int) (Math.log10(Math.abs(n.doubleValue())) + 1);
            if (digits <= digitsToTruncate) {
                return convertToIntegerType(0L, n.getClass());
            }

            long tenAtScale = (long) tenPower(digitsToTruncate);
            return convertToIntegerType((nLong / tenAtScale) * tenAtScale, n.getClass());
        }
        double tenAtScale = Math.pow(10d, longPrecision);
        double g = n.doubleValue() * tenAtScale;
        Double result = (((n.doubleValue() < 0) ? Math.ceil(g) : Math.floor(g)) / tenAtScale);
        return n instanceof Float ? result.floatValue() : result;
    }

    // optimise very common cases for round and truncate
    private static double tenPower(long n) {
        if (n == 0L) {
            return 1d;
        } else if (n == 1L) {
            return 10d;
        } else if (n == 2L) {
            return 100d;
        } else if (n == 3L) {
            return 1000d;
        } else if (n == 4L) {
            return 10000d;
        } else if (n == 5L) {
            return 100000d;
        }
        return Math.pow(10, n);
    }

    /**
     * does not take number precision and overflow into consideration!
     * Use only in cases when these aspects are guaranteed by previous logic (eg. ROUND, TRUNCATE)
     * @param number the number to convert
     * @param type the destination type
     * @return the same number converted to the right type
     * @throws ArithmeticException in case of integer overflow.
     * See {@link org.elasticsearch.xpack.esql.core.expression.predicate.operator.arithmetic.Arithmetics}
     */
    private static Number convertToIntegerType(Long number, Class<? extends Number> type) throws ArithmeticException {
        if (type == Integer.class) {
            if (number > Integer.MAX_VALUE || number < Integer.MIN_VALUE) {
                throw new ArithmeticException("integer overflow");
            }
            return number.intValue();
        } else if (type == Short.class) {
            return number.shortValue();
        } else if (type == Byte.class) {
            return number.byteValue();
        }
        return number;
    }
}
