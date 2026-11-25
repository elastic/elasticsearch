/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.escf;

import org.elasticsearch.sourcebatch.SourceValueType;

import java.util.function.Function;

/**
 * Reads single numbers from an {@link EscfColumn} the way the x-content path does for a parser that requires
 * {@code VALUE_NUMBER} and reads it with {@code longValue()} or {@code doubleValue()}: the scalar counterpart of
 * {@link EscfNumericArrayIterator}. Mappers that parse numbers that way get parity with their row path by construction.
 *
 * <p>Wherever the x-content path would fail, these throw the exception {@code onUnsupported} builds from a message.
 */
public final class EscfNumbers {

    /** The bounds Jackson range-checks a fractional number against before narrowing it; outside them it fails. */
    private static final double MIN_LONG_AS_DOUBLE = Long.MIN_VALUE;
    private static final double MAX_LONG_AS_DOUBLE = Long.MAX_VALUE;

    private EscfNumbers() {}

    /**
     * Reads document {@code row} as {@code XContentParser#longValue()} would: integers as they are, and fractional numbers
     * truncated toward zero under the default coerce policy.
     */
    public static long longValue(EscfColumn column, int row, Function<String, ? extends RuntimeException> onUnsupported) {
        final byte type = column.getTypeByte(row);
        return switch (type) {
            case SourceValueType.INT -> column.getIntValue(row);
            case SourceValueType.LONG -> column.getLongValue(row);
            case SourceValueType.FLOAT, SourceValueType.DOUBLE -> {
                final double d = type == SourceValueType.FLOAT ? column.getFloatValue(row) : column.getDoubleValue(row);
                if (isInLongRange(d) == false) {
                    throw onUnsupported.apply("value [" + d + "] in document [" + row + "] is out of range for a long");
                }
                yield (long) d;
            }
            default -> throw notANumber(row, type, onUnsupported);
        };
    }

    /** Reads document {@code row} as {@code XContentParser#doubleValue()} would. */
    public static double doubleValue(EscfColumn column, int row, Function<String, ? extends RuntimeException> onUnsupported) {
        final byte type = column.getTypeByte(row);
        return switch (type) {
            case SourceValueType.INT -> column.getIntValue(row);
            case SourceValueType.LONG -> column.getLongValue(row);
            case SourceValueType.FLOAT -> column.getFloatValue(row);
            case SourceValueType.DOUBLE -> column.getDoubleValue(row);
            default -> throw notANumber(row, type, onUnsupported);
        };
    }

    /**
     * Whether the x-content path narrows {@code d} to a {@code long} rather than failing; a bare cast would saturate instead.
     * Phrased as the check of Jackson's JSON, CBOR and SMILE parsers is, so that {@code NaN}, which the binary formats can
     * hold, passes and reads as {@code 0}.
     */
    static boolean isInLongRange(double d) {
        return (d < MIN_LONG_AS_DOUBLE || d > MAX_LONG_AS_DOUBLE) == false;
    }

    private static RuntimeException notANumber(int row, byte type, Function<String, ? extends RuntimeException> onUnsupported) {
        return onUnsupported.apply("value in document [" + row + "] is [" + SourceValueType.name(type) + "], not a number");
    }
}
