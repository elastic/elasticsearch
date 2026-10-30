/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb;

import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.ArrayUtil;

import java.io.IOException;

/**
 * The values of one single-valued numeric field, read once from a {@link NumericDocValues} into arrays.
 *
 * <p>At flush the consumer sees a field through an iterator that it has to walk several times: once to count documents and values,
 * once for the skip index and once to cut the values into blocks. Those walks share their call sites with every other doc values
 * iterator in the JVM (merges, ordinals, other formats), so each {@code nextDoc} and {@code nextValue} is a megamorphic call. Reading the
 * field once, through a call site that only flush reaches, and running the remaining passes over plain arrays removes most of that cost.
 *
 * <p>Documents are stored in increasing doc id order. When every document in the segment has a value the doc ids are implicit
 * ({@code doc == index}) and no {@code int[]} is kept.
 */
final class MaterializedNumericValues {

    private long[] values;
    /** {@code null} while the field is dense, that is while {@code doc == index} for every entry. */
    private int[] docs;
    private int size;

    private MaterializedNumericValues(long[] values, int[] docs, int size) {
        this.values = values;
        this.docs = docs;
        this.size = size;
    }

    /**
     * Reads every remaining document of {@code in}.
     *
     * @param in     iterator positioned before its first document
     * @param maxDoc number of documents in the segment, used only to size the initial buffers
     */
    static MaterializedNumericValues load(final NumericDocValues in, final int maxDoc) throws IOException {
        final int initialCapacity = (int) Math.max(Math.min(in.cost(), maxDoc), 16);
        long[] values = new long[initialCapacity];
        int[] docs = null;
        int size = 0;
        for (int doc = in.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = in.nextDoc()) {
            if (size == values.length) {
                values = ArrayUtil.grow(values, size + 1);
                if (docs != null) {
                    docs = ArrayUtil.growExact(docs, values.length);
                }
            }
            if (docs == null) {
                if (doc != size) {
                    docs = new int[values.length];
                    for (int i = 0; i < size; i++) {
                        docs[i] = i;
                    }
                    docs[size] = doc;
                }
            } else {
                docs[size] = doc;
            }
            values[size++] = in.longValue();
        }
        return new MaterializedNumericValues(values, docs, size);
    }

    /** Number of documents with a value. Equal to the number of values, as the field is single-valued. */
    int size() {
        return size;
    }

    /** The values in doc id order. Only the first {@link #size()} entries are meaningful. */
    long[] values() {
        return values;
    }

    /** The doc id of the {@code index}-th value. */
    int doc(int index) {
        assert index >= 0 && index < size;
        return docs == null ? index : docs[index];
    }

    /** An iterator over the doc ids that have a value, for writing the presence bit set of a sparse field. */
    DocIdSetIterator docIds() {
        return new DocIdSetIterator() {
            private int index = -1;
            private int doc = -1;

            @Override
            public int docID() {
                return doc;
            }

            @Override
            public int nextDoc() {
                return doc = ++index < size ? doc(index) : NO_MORE_DOCS;
            }

            @Override
            public int advance(int target) {
                while (doc < target) {
                    nextDoc();
                }
                return doc;
            }

            @Override
            public long cost() {
                return size;
            }
        };
    }
}
