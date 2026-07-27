/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.recycler.Recycler;
import org.elasticsearch.escf.EscfColumnData;
import org.elasticsearch.escf.EscfRowColumnBuilder;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * Accumulates {@code (doc, value)} string pairs for metadata fields (e.g. {@code _field_names})
 * and drains them into an {@code ARRAY[STRING]} {@link EscfColumnData} via {@link #finish}.
 * Interns values by ordinal to share {@link BytesRef} instances across documents. Single-use:
 * {@link #finish} releases all state. Duplicate {@code (doc, value)} pairs are a bug and are
 * caught by assertions.
 */
final class DeduplicatingStringColumnAccumulator {

    // Consider using BytesRefHash as a performance optimization if this appears as a hotspot.
    private Map<BytesRef, Integer> valueToOrd = new HashMap<>();
    private BytesRef[] ordToValue = new BytesRef[4];
    private int ordCount;

    // docOrds[doc]: ordinals recorded for that document; null until first record; exact-size.
    private int[][] docOrds;

    private boolean hasEntries;

    DeduplicatingStringColumnAccumulator(int docCount) {
        this.docOrds = new int[docCount][];
    }

    /** Returns {@code true} if no entry has been recorded; callers may skip {@link #finish}. */
    boolean isEmpty() {
        return hasEntries == false;
    }

    /** Records {@code value} for {@code doc}. Each {@code (doc, value)} pair must be unique. */
    void record(int doc, BytesRef value) {
        final int ord = intern(value);
        assert noDuplicateOrd(doc, ord) : "duplicate value [" + value.utf8ToString() + "] for doc " + doc;
        final int[] ords = docOrds[doc];
        if (ords != null) {
            final int[] extended = Arrays.copyOf(ords, ords.length + 1);
            extended[ords.length] = ord;
            docOrds[doc] = extended;
        } else {
            docOrds[doc] = new int[] { ord };
        }
        hasEntries = true;
    }

    private boolean noDuplicateOrd(int doc, int ord) {
        final int[] ords = docOrds[doc];
        if (ords != null) {
            for (int o : ords) {
                if (o == ord) {
                    return false;
                }
            }
        }
        return true;
    }

    /** Drains into a STRING {@link EscfColumnData} (scalar if every doc has at most one value, ARRAY otherwise) and releases all state. */
    EscfColumnData finish(Recycler<BytesRef> recycler) {
        final int docCount = docOrds.length;
        final EscfRowColumnBuilder builder = EscfRowColumnBuilder.strings(recycler);
        for (int doc = 0; doc < docCount; doc++) {
            final int[] ords = docOrds[doc];
            if (ords == null) {
                continue;
            }
            for (int ord : ords) {
                builder.setString(doc, ordToValue[ord]);
            }
        }
        docOrds = null;
        valueToOrd = null;
        ordToValue = null;
        return builder.finish(docCount);
    }

    private int intern(BytesRef value) {
        final Integer existing = valueToOrd.get(value);
        if (existing != null) {
            return existing;
        }
        final int ord = ordCount++;
        valueToOrd.put(value, ord);
        if (ord >= ordToValue.length) {
            // Rely on Lucene's ArrayUtil.oversize to determine the best target array size
            ordToValue = Arrays.copyOf(ordToValue, ArrayUtil.oversize(ord + 1, Integer.BYTES));
        }
        ordToValue[ord] = value;
        return ord;
    }
}
