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
 * An {@link EscfNumericArrayIterator} reading each element as {@code XContentParser#doubleValue()} would.
 */
public final class EscfDoubleArrayIterator extends EscfNumericArrayIterator {

    private double value;

    private EscfDoubleArrayIterator(EscfColumn column, Function<String, ? extends RuntimeException> onUnsupported) {
        super(column, onUnsupported);
    }

    /** Builds an iterator over {@code column}, failing right away if no value in it can be an array. */
    public static EscfDoubleArrayIterator of(EscfColumn column, Function<String, ? extends RuntimeException> onUnsupported) {
        // TODO (performance): read ARRAY columns with a LONG or DOUBLE child straight from the child, without the per-element type switch.
        return new EscfDoubleArrayIterator(column, onUnsupported);
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
            case SourceValueType.FLOAT -> elements.floatValue();
            case SourceValueType.DOUBLE -> elements.doubleValue();
            default -> throw notANumber(type);
        };
        return true;
    }

    /** The current element, valid after {@link #nextArrayEntry()} returned {@code true}. */
    public double value() {
        return value;
    }
}
