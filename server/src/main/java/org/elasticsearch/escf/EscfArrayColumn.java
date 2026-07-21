/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.escf;

import org.apache.lucene.document.column.LongTupleCursor;
import org.apache.lucene.document.column.ObjectTupleCursor;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.IntsRef;
import org.elasticsearch.sourcebatch.ArrayReader;
import org.elasticsearch.sourcebatch.SourceValueType;

/**
 * An ESCF column whose values are all arrays of a single fixed primitive element kind, stored in a
 * columnar list layout: a per-row element-range offset vector ({@code offsets}) over a single dense
 * primitive {@code child} sub-column. Row {@code d}'s elements are the child elements in
 * {@code [offsets[d], offsets[d + 1])}. There are no inline arrays.
 */
final class EscfArrayColumn extends EscfColumn {

    private final EscfColumn child;
    private final IntsRef rowOffsets;

    EscfArrayColumn(int docCount, FixedBitSet validity, EscfColumn child, IntsRef rowOffsets) {
        super(docCount, validity);
        this.child = child;
        this.rowOffsets = rowOffsets;
    }

    @Override
    byte kind() {
        return EscfColumnKind.ARRAY;
    }

    @Override
    byte typeByteForPresent(int row) {
        return SourceValueType.FIXED_ARRAY;
    }

    @Override
    ArrayReader getArrayValue(int row) {
        int elemFrom = intAt(rowOffsets, row);
        int elemTo = intAt(rowOffsets, row + 1);
        return new ColumnarArrayReader(child, elemFrom, elemTo);
    }

    /**
     * Returns an element-granular {@link LongTupleCursor} over this array column's long element
     * values. The child column must be an {@link EscfLongColumn}; throws
     * {@link UnsupportedOperationException} otherwise.
     *
     * <p>For multi-valued rows the same row-id is returned once per element. Empty rows (zero-width
     * offset range) and absent rows (no elements) are skipped automatically.
     */
    // TODO: this cursor is what we need for Lucene integration. At the mapper level we will eventually need a cursor which maintains empty
    // arrays. Add that when needed.
    @Override
    LongTupleCursor longCursor() {
        if (!(child instanceof EscfLongColumn longChild)) {
            throw new UnsupportedOperationException("longCursor() requires a long child column, got: " + EscfColumnKind.name(child.kind()));
        }
        final int numRows = docCount;
        final int startElem = intAt(rowOffsets, 0);
        return new LongTupleCursor() {
            private int elemPos = startElem - 1;
            private int currentDoc = 0;

            @Override
            public int nextDoc() {
                elemPos++;
                // Advance past all rows whose element range ends at or before the current element
                while (currentDoc < numRows && intAt(rowOffsets, currentDoc + 1) <= elemPos) {
                    currentDoc++;
                }
                return currentDoc < numRows ? currentDoc : DocIdSetIterator.NO_MORE_DOCS;
            }

            @Override
            public long longValue() {
                return longChild.getLongValue(elemPos);
            }
        };
    }

    /**
     * Returns an element-granular {@link ObjectTupleCursor}{@code <BytesRef>} over this array
     * column's byte-string element values. The child column must be a var-width (STRING or BINARY)
     * column; throws {@link UnsupportedOperationException} otherwise.
     *
     * <p>For multi-valued rows the same row-id is returned once per element. Empty rows (zero-width
     * offset range) and absent rows (no elements) are skipped automatically.
     */
    // TODO: this cursor is what we need for Lucene integration. At the mapper level we will eventually need a cursor which maintains empty
    // arrays. Add that when needed.
    @Override
    ObjectTupleCursor<BytesRef> bytesRefCursor() {
        if (!(child instanceof AbstractVarColumn varChild)) {
            throw new UnsupportedOperationException(
                "bytesRefCursor() requires a var-width child column, got: " + EscfColumnKind.name(child.kind())
            );
        }
        final int numRows = docCount;
        final int startElem = intAt(rowOffsets, 0);
        return new ObjectTupleCursor<>() {
            private int elemPos = startElem - 1;
            private int currentDoc = 0;

            @Override
            public int nextDoc() {
                elemPos++;
                // Advance past all rows whose element range ends at or before the current element
                while (currentDoc < numRows && intAt(rowOffsets, currentDoc + 1) <= elemPos) {
                    currentDoc++;
                }
                return currentDoc < numRows ? currentDoc : DocIdSetIterator.NO_MORE_DOCS;
            }

            @Override
            public BytesRef value() {
                return varChild.getBinaryValue(elemPos);
            }
        };
    }

    @Override
    EscfColumn sliceInternal(int from, int count) {
        // Child stays full/unsliced — ColumnarArrayReader uses absolute element indices.
        return new EscfArrayColumn(count, windowValidity(validity, from, count), child, sliceOffsets(rowOffsets, from, count));
    }

    @Override
    EscfColumnData toColumnData() {
        int[] newRowOffsets = rebasedOffsets(rowOffsets, docCount);
        int elemFrom = intAt(rowOffsets, 0);
        int elemTo = intAt(rowOffsets, docCount);
        // Slice the child to the element range referenced by this window, then materialize it.
        EscfColumnData childData = child.sliceInternal(elemFrom, elemTo - elemFrom).toColumnData();
        return EscfColumnData.ofArray(docCount, validity, newRowOffsets, childData);
    }
}
