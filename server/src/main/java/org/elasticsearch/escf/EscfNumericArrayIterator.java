/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.escf;

import org.apache.lucene.search.DocIdSetIterator;
import org.elasticsearch.sourcebatch.ArrayReader;
import org.elasticsearch.sourcebatch.SourceValueType;

import java.util.function.Function;

/**
 * Iterates the numeric arrays of an {@link EscfColumn}, row by row with {@link #nextRow()} and element by element
 * with {@link #nextArrayEntry()}, converting each element the way the x-content path does for a parser that requires
 * {@code START_ARRAY}, then {@code VALUE_NUMBER} per element, and reads it with {@code longValue()} or
 * {@code doubleValue()}. Mappers that parse arrays of numbers that way get parity with their row path by construction.
 *
 * <p>Wherever the x-content path would fail, this throws the exception {@code onUnsupported} builds from a message.
 */
public abstract sealed class EscfNumericArrayIterator permits EscfLongArrayIterator, EscfDoubleArrayIterator {

    private final EscfColumn column;
    private final PresentDocIterator presentRows;
    private final Function<String, ? extends RuntimeException> onUnsupported;
    private int row = -1;
    ArrayReader elements;

    EscfNumericArrayIterator(EscfColumn column, Function<String, ? extends RuntimeException> onUnsupported) {
        this.column = column;
        this.presentRows = column.presentDocs();
        this.onUnsupported = onUnsupported;
        switch (column.kind()) {
            case EscfColumnKind.ARRAY, EscfColumnKind.UNION -> {
            }
            case EscfColumnKind.LONG, EscfColumnKind.DOUBLE, EscfColumnKind.BOOL, EscfColumnKind.STRING, EscfColumnKind.BINARY -> {
                final int first = column.presentDocs().nextDoc();
                if (first != DocIdSetIterator.NO_MORE_DOCS) {
                    throw onUnsupported.apply(notAnArray(first, column.getTypeByte(first)));
                }
            }
            default -> throw new IllegalStateException("unknown ESCF column kind [" + EscfColumnKind.name(column.kind()) + "]");
        }
    }

    /**
     * Advances to the next document that has a value in this column and positions before its first element, returning
     * the document or {@link DocIdSetIterator#NO_MORE_DOCS}. A document without a value is skipped, since whether that is
     * an error depends on the rest of the document; an explicit {@code null} fails, being no {@code START_ARRAY}.
     */
    public final int nextRow() {
        if (row != DocIdSetIterator.NO_MORE_DOCS) {
            row = presentRows.nextDoc();
            if (row != DocIdSetIterator.NO_MORE_DOCS) {
                startRow(row);
            }
        }
        return row;
    }

    /** The document {@link #nextRow()} last returned, including one it then failed on. */
    // visible for testing
    final int row() {
        return row;
    }

    /** Advances to and converts the next element of the current row. Returns {@code false} once the row is exhausted. */
    public abstract boolean nextArrayEntry();

    private void startRow(int row) {
        if (column.kind() == EscfColumnKind.UNION) {
            final byte type = column.getTypeByte(row);
            if (type != SourceValueType.FIXED_ARRAY && type != SourceValueType.UNION_ARRAY) {
                throw unsupported(notAnArray(row, type));
            }
        }
        elements = column.getArrayValue(row);
    }

    final RuntimeException unsupported(String message) {
        return onUnsupported.apply(message);
    }

    final RuntimeException notANumber(byte type) {
        return unsupported("element of type [" + SourceValueType.name(type) + "] is not a number");
    }

    private static String notAnArray(int row, byte type) {
        return "value in document [" + row + "] is [" + SourceValueType.name(type) + "], not an array";
    }
}
