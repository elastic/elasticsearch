/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar.string;

import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * A per-segment terms dictionary for a {@link StringColumnLayout#DICTIONARY} column: the distinct values of
 * the segment, addressed by ordinal. Terms are held in first-seen order, so an ordinal carries no ordering
 * relative to the term bytes.
 *
 * <p>The dictionary is bounded by {@link #MAX_SIZE}, which keeps its on-heap footprint bounded (the format's
 * rule is that nothing <em>column</em>-proportional is held on the heap; a capped dictionary is bounded
 * metadata) and keeps every ordinal inside 8 bits. A segment whose distinct-value count exceeds the cap
 * carries no dictionary at all and falls back to {@link StringColumnLayout#PLAIN}.
 */
public final class StringDictionary {

    /** Per-segment dictionary cap. Picked so ordinals fit in 8 bits with bit-packing. */
    public static final int MAX_SIZE = 256;

    private final BytesRef[] terms;
    /** Term to ordinal, present only on the write path; {@code null} on a dictionary read back from disk. */
    private final Map<BytesRef, Integer> ordinals;

    private StringDictionary(BytesRef[] terms, Map<BytesRef, Integer> ordinals) {
        this.terms = terms;
        this.ordinals = ordinals;
    }

    /** The number of distinct terms. */
    public int size() {
        return terms.length;
    }

    /** The term at {@code ordinal}; the returned reference is owned by this dictionary, so do not mutate it. */
    public BytesRef term(int ordinal) {
        return terms[ordinal];
    }

    /**
     * The ordinal of {@code term}, which must be present. Only available on a dictionary built by
     * {@link Builder} during a write.
     */
    public int ordinal(BytesRef term) {
        Integer ordinal = ordinals.get(term);
        assert ordinal != null : "term not in dictionary: " + term;
        return ordinal;
    }

    /** Writes the dictionary: {@code [VInt size]} then {@code [VInt length][bytes]} per term, in ordinal order. */
    public void writeTo(DataOutput out) throws IOException {
        out.writeVInt(terms.length);
        for (BytesRef term : terms) {
            out.writeVInt(term.length);
            out.writeBytes(term.bytes, term.offset, term.length);
        }
    }

    /** Reads a dictionary written by {@link #writeTo}. The result serves reads only; {@link #ordinal} is unavailable. */
    public static StringDictionary readFrom(DataInput in) throws IOException {
        int size = in.readVInt();
        BytesRef[] terms = new BytesRef[size];
        for (int i = 0; i < size; i++) {
            byte[] bytes = new byte[in.readVInt()];
            in.readBytes(bytes, 0, bytes.length);
            terms[i] = new BytesRef(bytes);
        }
        return new StringDictionary(terms, null);
    }

    /**
     * Accumulates distinct terms while the writer's counting pass walks the column, giving up as soon as the
     * segment proves too high-cardinality to be worth a dictionary.
     */
    public static final class Builder {

        private final LinkedHashMap<BytesRef, Integer> ordinals = new LinkedHashMap<>();
        private boolean overflowed = false;

        /**
         * Records {@code value} as a candidate term. Once {@link #MAX_SIZE} distinct terms have been seen and
         * another arrives, the builder overflows: it drops what it collected and ignores everything after, so a
         * high-cardinality column costs no more than the cap in heap.
         */
        public void add(BytesRef value) {
            if (overflowed) {
                return;
            }
            if (ordinals.containsKey(value)) {
                return;
            }
            if (ordinals.size() == MAX_SIZE) {
                overflowed = true;
                ordinals.clear();
                return;
            }
            // The cursor reuses its BytesRef across values, so the key must be a copy.
            ordinals.put(BytesRef.deepCopyOf(value), ordinals.size());
        }

        /**
         * The dictionary for this segment, or {@code null} when the column should use
         * {@link StringColumnLayout#PLAIN} — either because the distinct-value count overflowed the cap or
         * because no value was seen at all.
         */
        public StringDictionary build() {
            if (overflowed || ordinals.isEmpty()) {
                return null;
            }
            return new StringDictionary(ordinals.keySet().toArray(new BytesRef[0]), ordinals);
        }
    }
}
