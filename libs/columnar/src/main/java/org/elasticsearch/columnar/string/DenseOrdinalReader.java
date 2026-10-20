/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar.string;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.columnar.substrate.ColumnIterator;

import java.io.IOException;

/**
 * Reads a dictionary column as dense ordinals, so a caller that counts or groups by value can work with small ints and
 * read each term once rather than once for each document.
 *
 * <p>A dense ordinal runs from zero up to {@link #size()}, so a caller can index an array by it. It is not the ordinal the
 * column stores, which reserves values for a null slot and for the marker of an escaped value, and so starts above zero.
 * That offset stays in here. A value the dictionary does not name has no dense ordinal: it is {@link #ESCAPED}, and the
 * column holds its bytes.
 *
 * <p>Positioned by the doc values it came from, so it reads whichever document they are on. A lookup of a term by ordinal
 * holds no per-document state, so it can be asked for any ordinal at any time, such as to read the keys of a result
 * after the documents are collected.
 */
public final class DenseOrdinalReader {

    /** {@link #ordinal} answer: the document's one value is not in the dictionary. Read it with {@link #escapedValue}. */
    public static final int ESCAPED = -1;

    private final DictionaryStringColumnReader column;
    private final ColumnIterator iterator;

    /** The slot {@link #ordinal} last answered for, which {@link #escapedValue} reads. */
    private long valueSlot = -1;

    DenseOrdinalReader(DictionaryStringColumnReader column, ColumnIterator iterator) {
        this.column = column;
        this.iterator = iterator;
    }

    /** How many terms the dictionary holds. */
    public int size() {
        return column.dictionarySize();
    }

    /**
     * The term at a dense {@code ordinal}. The returned {@link BytesRef} is {@code dst}, and is only valid until the next
     * call.
     */
    public BytesRef term(int ordinal, BytesRef dst) throws IOException {
        assert ordinal >= 0 && ordinal < size() : "ordinal [" + ordinal + "] names no term in a dictionary of " + size();
        return column.termAt(ordinal + StringColumnMetadata.Dictionary.FIRST_TERM_ORDINAL, dst);
    }

    /**
     * How many non-null values the document these values are positioned on holds. A null slot is no value, so a document
     * of several slots with one non-null slot holds one value.
     *
     * <p>Answered from the ordinals the column records, so nothing is decoded.
     */
    public int nonNullValueCount() throws IOException {
        final int rank = iterator.rank();
        final long first = column.firstValueAddress(rank);
        final long slots = column.valueCount(rank);
        int count = 0;
        for (long slot = first; slot < first + slots; slot++) {
            if (column.isNullSlot(slot) == false) {
                count++;
            }
        }
        return count;
    }

    /**
     * The dense ordinal of the one value the document holds, or {@link #ESCAPED}. Only for a document that
     * {@link #nonNullValueCount()} says holds exactly one value.
     */
    public int ordinal() throws IOException {
        final int rank = iterator.rank();
        final long first = column.firstValueAddress(rank);
        final long slots = column.valueCount(rank);
        for (long slot = first; slot < first + slots; slot++) {
            final int ordinal = column.ordinalAt(slot);
            if (ordinal != StringColumnMetadata.Dictionary.NULL_ORDINAL) {
                valueSlot = slot;
                return ordinal == column.escapeOrdinal() ? ESCAPED : ordinal - StringColumnMetadata.Dictionary.FIRST_TERM_ORDINAL;
            }
        }
        throw new IllegalStateException("the document holds no value");
    }

    /**
     * The value that {@link #ordinal} answered {@link #ESCAPED} for, which is only known until the next call to it. The
     * returned {@link BytesRef} is {@code dst}, and is only valid until the next call.
     */
    public BytesRef escapedValue(BytesRef dst) throws IOException {
        assert valueSlot >= 0 : "no document has answered with a value";
        return column.resolveEscape(valueSlot, dst);
    }
}
