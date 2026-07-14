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
import org.elasticsearch.sourcebatch.ArrayReader;
import org.elasticsearch.sourcebatch.SourceValueType;

/**
 * An ESCF column whose values are all arrays of a single fixed primitive element kind, stored in a
 * columnar list layout: a per-row element-range offset vector ({@code rowOffsets}) over a single dense
 * primitive {@code child} sub-column. Row {@code d}'s elements are the child elements in
 * {@code [rowOffsets[base + d], rowOffsets[base + d + 1])}. There are no inline arrays.
 *
 * <p>The {@code child} column always spans the full element range (base=0) so that
 * {@link ColumnarArrayReader} can address elements by their absolute index. When slicing an
 * {@code EscfArrayColumn}, only {@code base} is adjusted; the child remains full/unsliced. At
 * serialization time ({@link #toColumnData}), the child is explicitly sliced to cover only the
 * element range referenced by this window before materializing.
 */
final class EscfArrayColumn extends EscfColumn {

    private final EscfColumn child;
    private final int[] rowOffsets;

    EscfArrayColumn(int docCount, FixedBitSet absent, EscfColumn child, int[] rowOffsets) {
        super(docCount, absent);
        this.child = child;
        this.rowOffsets = rowOffsets;
    }

    private EscfArrayColumn(int docCount, FixedBitSet absent, EscfColumn child, int[] rowOffsets, int base) {
        super(docCount, absent, base);
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
        // rowOffsets are absolute element indices into the unsliced child column.
        return new ColumnarArrayReader(child, rowOffsets[base + d], rowOffsets[base + d + 1]);
    }

    @Override
    EscfColumn sliceInternal(int from, int count) {
        // Child stays full/unsliced — ColumnarArrayReader uses absolute element indices.
        return new EscfArrayColumn(count, absent, child, rowOffsets, base + from);
    }

    @Override
    EscfColumnData toColumnData() {
        FixedBitSet newAbsent = windowBitSet(absent, base, docCount);
        int elemFrom = rowOffsets[base];
        int elemTo = rowOffsets[base + docCount];
        int[] newRowOffsets = rebasedOffsets(rowOffsets, base, docCount);
        // Slice the child to the element range referenced by this window, then materialize it.
        EscfColumnData childData = child.sliceInternal(elemFrom, elemTo - elemFrom).toColumnData();
        return EscfColumnData.ofArray(docCount, newAbsent, newRowOffsets, childData);
    }
}
