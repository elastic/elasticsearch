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
 * columnar list layout: a per-row element-range offset vector ({@code offsets}) over a single dense
 * primitive {@code child} sub-column. Row {@code d}'s elements are the child elements in
 * {@code [offsets[d], offsets[d + 1])}. There are no inline arrays.
 */
final class EscfArrayColumn extends EscfColumn {

    private final EscfColumn child;
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
    ArrayReader getArrayValue(int row) {
        int elemFrom = intAt(rowOffsets, row);
        int elemTo = intAt(rowOffsets, row + 1);
        return new ColumnarArrayReader(child, elemFrom, elemTo);
    }

    @Override
    EscfColumn sliceInternal(int from, int count) {
        // Child stays full/unsliced — ColumnarArrayReader uses absolute element indices.
        return new EscfArrayColumn(count, windowBitSet(absent, from, count), child, sliceOffsets(rowOffsets, from, count));
    }

    @Override
    EscfColumnData toColumnData() {
        int[] newRowOffsets = rebasedOffsets(rowOffsets, docCount);
        int elemFrom = intAt(rowOffsets, 0);
        int elemTo = intAt(rowOffsets, docCount);
        // Slice the child to the element range referenced by this window, then materialize it.
        EscfColumnData childData = child.sliceInternal(elemFrom, elemTo - elemFrom).toColumnData();
        return EscfColumnData.ofArray(docCount, absent, newRowOffsets, childData);
    }
}
