/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ql.expression.function.scalar;

import java.time.Instant;
import java.time.ZonedDateTime;

public class NullSafeSort {

    public static String nullableToOrderPreservingString(Object o, boolean nullsLeast) {
        if (o == null) {
            return nullsLeast ? "0" : "1";
        } else {
            return (nullsLeast ? "1" : "0") + objectToOrderPreservingString(o);
        }
    }

    public static String objectToOrderPreservingString(Object o) {
        if (o instanceof Byte || o instanceof Integer) {
            return intToOrderPreservingString(((Number) o).intValue());
        } else if (o instanceof Long) {
            return longToOrderPreservingString(((Number) o).longValue());
        } else if (o instanceof Number) {
            // fall back to double representation for any other number, might cause precision loss
            return doubleToOrderPreservingString(((Number) o).doubleValue());
        } else if (o instanceof Instant) {
            return instantToOrderPreservingString((Instant) o);
        } else if (o instanceof ZonedDateTime) {
            return zonedDateTimeToOrderPreservingString((ZonedDateTime) o);
        } else if (o instanceof Boolean) {
            return boolToOrderPreservingString((Boolean) o);
        } else {
            // Fall back to string representation. This might produce the right order for all types but is consistent with the legacy
            // behavior.
            return o.toString();
        }
    }

    public static String instantToOrderPreservingString(Instant i) {
        return longToOrderPreservingString(i.getEpochSecond()) + intToOrderPreservingString(i.getNano());
    }

    public static String zonedDateTimeToOrderPreservingString(ZonedDateTime z) {
        // see java.time.chrono.ChronoZonedDateTime.compareTo
        return longToOrderPreservingString(z.toEpochSecond()) + intToOrderPreservingString(z.getNano())
            + intToOrderPreservingString(z.getOffset().getTotalSeconds())
            + z.getZone().getId();
    }

    public static String boolToOrderPreservingString(boolean b) {
        return b ? "1" : "0";
    }

    private static final int INT_HEX_DIGITS = 8;

    public static String intToOrderPreservingString(int l) {
        if (l < 0) {
            return "0" + pad(Integer.toHexString(l + Integer.MIN_VALUE), INT_HEX_DIGITS);
        } else {
            return "1" + pad(Integer.toHexString(l), INT_HEX_DIGITS);
        }
    }

    private static final int LONG_HEX_DIGITS = 16;

    public static String longToOrderPreservingString(long l) {
        if (l < 0) {
            return "0" + pad(Long.toHexString(l + Long.MIN_VALUE), LONG_HEX_DIGITS);
        } else {
            return "1" + pad(Long.toHexString(l), LONG_HEX_DIGITS);
        }
    }

    // values according to Double.doubleToRawLongBits
    private static final long EXPONENT_MASK = 0x7FF0000000000000L;
    private static final int EXPONENT_BIAS = 1023;
    private static final int SIGNIFICAND_BITS = 53;
    private static final long SIGNIFICAND_MASK = 0x000FFFFFFFFFFFFFL;

    /**
     * Converts a double to a compact string preserving the ordering of the values provided.
     *
     * Hence, for all a and b of type double, <code>doubleToOrderPreservingString(a) &lt; doubleToOrderPreservingString(b)</code> iff
     * <code>a &lt; b</code>.
     *
     * The implementation works for all doubles including POSITIVE_INFINITY and NEGATIVE_INFINITY but has undefined behavior for NaN.
     */
    public static String doubleToOrderPreservingString(double d) {
        String sign = boolToOrderPreservingString(d >= 0);

        long bits = Double.doubleToRawLongBits(d);
        int exponent = ((int) ((bits & EXPONENT_MASK) >> (SIGNIFICAND_BITS - 1)) - EXPONENT_BIAS);
        long significant = bits & SIGNIFICAND_MASK;

        return sign
            + intToOrderPreservingString(d < 0 ? -exponent : exponent)
            + longToOrderPreservingString(d < 0 ? -significant : significant);
    }

    private static String pad(String s, int digits) {
        StringBuilder sb = new StringBuilder(digits);
        while (sb.length() + s.length() < digits) {
            sb.append(' ');
        }
        sb.append(s);
        return sb.toString();
    }

}
