/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.fielddata;

import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.index.codec.flattened.ColumnarKeyedBinaryDocValues;

import java.io.IOException;

/**
 * Columnar fast-path variant of {@link KeyFilteredSortingArrayOrderBinaryDocValues}.
 *
 * <p>When the {@code ._keyed} binary doc values are stored in a columnar layout
 * (i.e. the underlying {@link org.apache.lucene.index.BinaryDocValues} is a
 * {@link ColumnarKeyedBinaryDocValues}), this reader avoids decompressing the whole
 * per-doc blob: it resolves the target key to its segment ordinal once at construction
 * time and then on every document decompresses only that sub-field's compressed run.
 *
 * <p>When the key is absent from the entire segment ({@link #keyOrdinal} == -1),
 * {@link #advanceExact(int)} returns {@code false} immediately for every document,
 * allowing the caller to skip the whole segment without any per-doc work.
 */
public final class KeyLookupArrayOrderBinaryDocValues extends SortingBinaryDocValues {

    private final ColumnarKeyedBinaryDocValues binary;
    /** Segment-wide ordinal for the target key; -1 if the key is absent from the segment. */
    private final int keyOrdinal;

    public KeyLookupArrayOrderBinaryDocValues(ColumnarKeyedBinaryDocValues binary, BytesRef key) {
        this.binary = binary;
        this.keyOrdinal = binary.lookupKeyOrdinal(key);
    }

    @Override
    public boolean advanceExact(int doc) throws IOException {
        count = 0;

        if (keyOrdinal < 0) {
            return false;
        }

        if (binary.advanceExact(doc) == false) {
            return false;
        }

        int slotCount = binary.advanceExactKey(keyOrdinal);
        if (slotCount == 0) {
            return false;
        }

        // Pre-allocate; slotCount is an upper bound (null slots are dropped below).
        count = slotCount;
        grow();

        int nonNull = 0;
        for (int s = 0; s < slotCount; s++) {
            BytesRef val = binary.nextKeyValue();
            if (val != null) {
                values[nonNull].copyBytes(val);
                nonNull++;
            }
        }
        count = nonNull;
        if (count == 0) {
            return false;
        }
        sort();
        dedup();
        return true;
    }

    /**
     * Removes consecutive equal values from {@code values[0..count-1]}. Uses swaps so
     * every {@link org.apache.lucene.util.BytesRefBuilder} stays reachable for reuse.
     */
    private void dedup() {
        if (count <= 1) {
            return;
        }
        int deduped = 1;
        for (int i = 1; i < count; i++) {
            if (values[i].get().bytesEquals(values[deduped - 1].get()) == false) {
                ArrayUtil.swap(values, i, deduped);
                deduped++;
            }
        }
        count = deduped;
    }
}
