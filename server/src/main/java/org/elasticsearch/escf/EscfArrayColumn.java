/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.escf;

import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.IntsRef;
import org.elasticsearch.sourcebatch.ArrayReader;
import org.elasticsearch.sourcebatch.SourceValueType;

/**
 * An ESCF column whose values are all arrays of a single fixed primitive element kind, stored in a
 * columnar list layout: a per-row element-range offset vector ({@code rowOffsets}) over a single dense
 * primitive {@code child} sub-column. Row {@code d}'s elements are the child elements in
 * {@code [rowOffsets.ints[rowOffsets.offset + d], rowOffsets.ints[rowOffsets.offset + d + 1])}.
 * There are no inline arrays.
 *
 * <p>The {@code child} column always spans the full element range so that {@link ColumnarArrayReader}
 * can address elements by their absolute index. When slicing an {@code EscfArrayColumn}, only
 * {@code rowOffsets.offset} is adjusted; the child remains full/unsliced. At serialization time
 * ({@link #toColumnData}), the child is explicitly sliced to cover only the element range referenced
 * by this window before materializing.
 */
final class EscfArrayColumn extends EscfColumn {

    private final EscfColumn child;

    /**
     * Windowed row-offset vector. {@code rowOffsets.ints[rowOffsets.offset + d]} is the absolute
     * element index (into {@code child}) of the first element of row {@code d}. The window covers
     * {@code docCount + 1} entries.
     */
    private final IntsRef rowOffsets;

    EscfArrayColumn(int docCount, FixedBitSet absent, EscfColumn child, IntsRef rowOffsets) {
        super(docCount, absent);
        this.child = child;
        this.rowOffsets = rowOffsets;
    }

    @Override
    byte kind() {
        return EscfColumnKind.ARRAY;
    }

    @Override
    byte typeByteForPresent(int d) {
        return SourceValueType.FIXED_ARRAY;
    }

    @Override
    ArrayReader getArrayValue(int d) {
        // rowOffsets hold absolute element indices into the unsliced child column.
        return new ColumnarArrayReader(child, rowOffsets.ints[rowOffsets.offset + d], rowOffsets.ints[rowOffsets.offset + d + 1]);
    }

    @Override
    EscfColumn sliceInternal(int from, int count) {
        // Child stays full/unsliced — ColumnarArrayReader uses absolute element indices.
        return new EscfArrayColumn(
            count,
            windowBitSet(absent, from, count),
            child,
            new IntsRef(rowOffsets.ints, rowOffsets.offset + from, count + 1)
        );
    }

    @Override
    EscfColumnData toColumnData() {
        int elemFrom = rowOffsets.ints[rowOffsets.offset];
        int elemTo = rowOffsets.ints[rowOffsets.offset + docCount];
        int[] newRowOffsets = rebasedOffsets(rowOffsets, docCount);
        // Slice the child to the element range referenced by this window, then materialize it.
        EscfColumnData childData = child.sliceInternal(elemFrom, elemTo - elemFrom).toColumnData();
        return EscfColumnData.ofArray(docCount, absent, newRowOffsets, childData);
    }
}
