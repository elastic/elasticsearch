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
import org.apache.lucene.util.FixedBitSet;
import org.elasticsearch.plugins.internal.MeteringModel;
import org.elasticsearch.sourcebatch.SourceValueType;

/**
 * An column whose values are all booleans, held as the value bitset directly (bit set = {@code true}).
 */
final class EscfBoolColumn extends EscfColumn {

    /**
     * Value bitset (bit set = {@code true}), or {@code null} when every value is {@code false}.
     * Always zero-based and covers {@code [0, docCount)} when non-null.
     */
    private final FixedBitSet values;

    EscfBoolColumn(int docCount, FixedBitSet validity, FixedBitSet values) {
        super(docCount, validity);
        this.values = values;
    }

    @Override
    public byte kind() {
        return EscfColumnKind.BOOL;
    }

    @Override
    byte typeByteForPresent(int row) {
        return bitSet(row) ? SourceValueType.TRUE : SourceValueType.FALSE;
    }

    @Override
    public boolean getBooleanValue(int row) {
        return bitSet(row);
    }

    private boolean bitSet(int row) {
        // values is null or covers [0, docCount), so no length guard is needed.
        return values != null && values.get(row);
    }

    // A boolean is one bit, which rounds to 0 bytes (SourceValueType.elemDataSize(TRUE/FALSE) == 0).
    @Override
    public int sizeInBytes(int row) {
        return 0;
    }

    @Override
    public void addSizesInBytes(int[] sizes) {}

    @Override
    public void addMeteredSizes(long[] sizes, MeteringModel model) {
        final long booleanBytes = model.booleanBytes();
        if (validity == null) {
            for (int d = 0; d < docCount; d++) {
                sizes[d] += booleanBytes;
            }
            return;
        }
        PresentDocIterator present = presentDocs();
        for (int d = present.nextDoc(); d != DocIdSetIterator.NO_MORE_DOCS; d = present.nextDoc()) {
            sizes[d] += booleanBytes;
        }
    }

    @Override
    EscfColumn sliceInternal(int from, int count) {
        return new EscfBoolColumn(count, windowValidity(validity, from, count), windowBitSet(values, from, count));
    }

    @Override
    EscfColumnData toColumnData() {
        // validity and values are already windowed and zero-based; return them directly.
        return EscfColumnData.ofBool(docCount, validity, values);
    }
}
