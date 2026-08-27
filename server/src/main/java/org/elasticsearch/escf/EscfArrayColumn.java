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
 * columnar list layout: a per-row element-range offset vector ({@code offsets}) over a single
 * primitive {@code child} sub-column. Row {@code d}'s elements are the child elements in
 * {@code [offsets[d], offsets[d + 1])}. There are no inline arrays.
 *
 * <p>The child sub-column may carry its own validity bitset (distinct from this column's outer
 * {@link #validity} bitset). Clear bits in the child validity represent explicit JSON {@code null}
 * elements within an array, not absent elements — an element can never be absent inside an array.
 * Null elements occupy a placeholder slot in the child data (8 zero bytes for fixed-64, a
 * zero-length range for var-width) so that the child is always positionally indexed from 0 to
 * {@code totalElems - 1}.
 */
final class EscfArrayColumn extends EscfColumn {

    private final EscfColumn child;
    private final IntsRef rowOffsets;

    EscfArrayColumn(int docCount, FixedBitSet validity, EscfColumn child, IntsRef rowOffsets) {
        super(docCount, validity);
        this.child = child;
        this.rowOffsets = rowOffsets;
    }

    EscfColumn child() {
        return child;
    }

    IntsRef rowOffsets() {
        return rowOffsets;
    }

    @Override
    public byte kind() {
        return EscfColumnKind.ARRAY;
    }

    /**
     * Returns the element (child) column kind, so callers can decide whether the array values are
     * directly usable as byte-strings (kind == {@link EscfColumnKind#STRING}) without iterating.
     */
    @Override
    public byte leafValueKind() {
        return child.kind();
    }

    @Override
    public boolean hasNullLeafValues() {
        return child.isDense() == false;
    }

    @Override
    byte typeByteForPresent(int row) {
        return SourceValueType.FIXED_ARRAY;
    }

    @Override
    public ArrayReader getArrayValue(int row) {
        int elemFrom = intAt(rowOffsets, row);
        int elemTo = intAt(rowOffsets, row + 1);
        return new ColumnarArrayReader(child, elemFrom, elemTo);
    }

    /**
     * Returns an element-granular {@link LongTupleCursor} over this array column's fixed-64 element
     * values. The child column must be an {@link AbstractFixed64Column} (LONG or DOUBLE); throws
     * {@link UnsupportedOperationException} otherwise.
     *
     * <p>As with {@link AbstractFixed64Column#longCursor()}, the yielded {@code longValue()} is the
     * <em>raw 64-bit stored word</em>: the long value for a {@link EscfLongColumn} child, and
     * {@code Double.doubleToRawLongBits(d)} for a {@link EscfDoubleColumn} child.
     *
     * <p>For multi-valued rows the same row-id is returned once per element. Empty rows (zero-width
     * offset range) and absent rows (no elements) are skipped automatically.
     */
    // TODO: this cursor is what we need for Lucene integration. At the mapper level we will eventually need a cursor which maintains empty
    // arrays. Add that when needed.
    @Override
    public LongTupleCursor longCursor() {
        if (!(child instanceof AbstractFixed64Column fixedChild)) {
            throw new UnsupportedOperationException(
                "longCursor() requires a fixed-64 child column, got: " + EscfColumnKind.name(child.kind())
            );
        }
        final int numRows = docCount;
        final int[] offs = rowOffsets.ints;
        final int base = rowOffsets.offset;
        final AbstractFixed64Column.DenseLongValuesCursor values = fixedChild.longValuesCursor();
        final int startElem = offs[base];
        if (numRows > 0 && startElem > 0) {
            values.skip(startElem); // this window starts mid-child because sliceInternal keeps the child unsliced
        }
        return new LongTupleCursor() {
            private int currentDoc = -1;
            private int rowEnd = startElem;  // element index one past the last element of the current row
            private int remainingInRow;
            private long currentValue;

            @Override
            public int nextDoc() {
                // Advance past rows with no elements (empty arrays and absent rows are both zero-width).
                while (remainingInRow == 0) {
                    if (currentDoc + 1 >= numRows) {
                        return DocIdSetIterator.NO_MORE_DOCS;
                    }
                    currentDoc++;
                    // Rows are contiguous: one offset read per row, zero per mid-row element.
                    int nextEnd = offs[base + currentDoc + 1];
                    remainingInRow = nextEnd - rowEnd;
                    rowEnd = nextEnd;
                }
                remainingInRow--;
                currentValue = values.nextLong();
                return currentDoc;
            }

            @Override
            public long longValue() {
                return currentValue;
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
     *
     * @param retainValues {@code false} to reuse a single {@link BytesRef} across the whole scan (valid
     *                     only until the next {@link ObjectTupleCursor#nextDoc()}, and allocation-free);
     *                     {@code true} to hand back a fresh {@link BytesRef} per element, which matters for
     *                     multi-valued rows whose elements are all held live at once
     */
    // TODO: this cursor is what we need for Lucene integration. At the mapper level we will eventually need a cursor which maintains empty
    // arrays. Add that when needed.
    @Override
    public ObjectTupleCursor<BytesRef> bytesRefCursor(boolean retainValues) {
        if (!(child instanceof AbstractVarColumn varChild)) {
            throw new UnsupportedOperationException(
                "bytesRefCursor() requires a var-width child column, got: " + EscfColumnKind.name(child.kind())
            );
        }
        final int numRows = docCount;
        final int[] offs = rowOffsets.ints;
        final int base = rowOffsets.offset;
        final AbstractVarColumn.DenseBytesRefValuesCursor values = varChild.bytesRefValuesCursor(retainValues);
        final int startElem = offs[base];
        if (numRows > 0 && startElem > 0) {
            values.skip(startElem); // this window starts mid-child because sliceInternal keeps the child unsliced
        }
        return new ObjectTupleCursor<>() {
            private int currentDoc = -1;
            private int rowEnd = startElem;  // element index one past the last element of the current row
            private int remainingInRow;
            private BytesRef currentValue;

            @Override
            public int nextDoc() {
                // Advance past rows with no elements (empty arrays and absent rows are both zero-width).
                while (remainingInRow == 0) {
                    if (currentDoc + 1 >= numRows) {
                        return DocIdSetIterator.NO_MORE_DOCS;
                    }
                    currentDoc++;
                    // Rows are contiguous: one offset read per row, zero per mid-row element.
                    int nextEnd = offs[base + currentDoc + 1];
                    remainingInRow = nextEnd - rowEnd;
                    rowEnd = nextEnd;
                }
                remainingInRow--;
                currentValue = values.nextValue();
                return currentDoc;
            }

            @Override
            public BytesRef value() {
                return currentValue;
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
