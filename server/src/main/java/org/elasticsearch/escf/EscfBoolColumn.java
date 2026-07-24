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
    byte kind() {
        return EscfColumnKind.BOOL;
    }

    @Override
    byte typeByteForPresent(int row) {
        return bitSet(row) ? SourceValueType.TRUE : SourceValueType.FALSE;
    }

    @Override
    boolean getBooleanValue(int row) {
        return bitSet(row);
    }

    private boolean bitSet(int row) {
        // values is null or covers [0, docCount), so no length guard is needed.
        return values != null && values.get(row);
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
