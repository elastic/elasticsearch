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
 * An {@link EscfNumericArrayIterator} reading each element as {@code XContentParser#longValue()} would: integers as
 * they are, and fractional numbers truncated toward zero under the default coerce policy, so {@code 2.5} reads as
 * {@code 2} and {@code -0.5} as {@code 0}.
 */
public final class EscfLongArrayIterator extends EscfNumericArrayIterator {

    /** The bounds Jackson range-checks a fractional number against before narrowing it; outside them it fails. */
    private static final double MIN_LONG_AS_DOUBLE = Long.MIN_VALUE;
    private static final double MAX_LONG_AS_DOUBLE = Long.MAX_VALUE;

    private long value;

    private EscfLongArrayIterator(EscfColumn column, Function<String, ? extends RuntimeException> onUnsupported) {
        super(column, onUnsupported);
    }

    /** Builds an iterator over {@code column}, failing right away if no value in it can be an array. */
    public static EscfLongArrayIterator of(EscfColumn column, Function<String, ? extends RuntimeException> onUnsupported) {
        // TODO (performance): read ARRAY columns with a LONG or DOUBLE child straight from the child, without the per-element type switch.
        return new EscfLongArrayIterator(column, onUnsupported);
    }

    @Override
    public boolean nextArrayEntry() {
        if (elements.next() == false) {
            return false;
        }
        final byte type = elements.type();
        value = switch (type) {
            case SourceValueType.INT -> elements.intValue();
            case SourceValueType.LONG -> elements.longValue();
            case SourceValueType.FLOAT -> truncate(elements.floatValue());
            case SourceValueType.DOUBLE -> truncate(elements.doubleValue());
            default -> throw notANumber(type);
        };
        return true;
    }

    /** The current element, valid after {@link #nextArrayEntry()} returned {@code true}. */
    public long value() {
        return value;
    }

    private long truncate(double d) {
        // A bare cast would saturate instead of failing.
        if (d < MIN_LONG_AS_DOUBLE || d > MAX_LONG_AS_DOUBLE) {
            throw unsupported("element [" + d + "] is out of range for a long");
        }
        return (long) d;
    }
}
