/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors.diskbbq.calibrate;

import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.KnnVectorValues.DocIndexIterator;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.DocIdSetIterator;

import java.io.IOException;
import java.util.Arrays;

/**
 * Concatenation of multiple {@link FloatVectorValues} into a single logical view for calibration
 * sampling during merge. Vectors are addressed by a global ordinal; lookups dispatch to the
 * underlying segment reader via a prefix-sum offset table without copying vector data to the heap.
 *
 * <p>{@link #vectorValue(int)} forwards to the owning part, which may return a reused scratch
 * buffer; callers that need to retain a vector across subsequent calls must copy it.
 */
public final class ConcatenatedFloatVectorValues extends FloatVectorValues {

    private final FloatVectorValues[] parts;
    // offsets[p] = global ord at which part p starts; offsets[parts.length] = total size.
    private final int[] offsets;
    private final int totalSize;
    private final int dims;

    public ConcatenatedFloatVectorValues(FloatVectorValues[] parts) {
        if (parts.length == 0) {
            throw new IllegalArgumentException("parts must not be empty");
        }
        this.parts = parts;
        this.offsets = new int[parts.length + 1];
        this.dims = parts[0].dimension();
        int running = 0;
        for (int i = 0; i < parts.length; i++) {
            if (parts[i].dimension() != dims) {
                throw new IllegalArgumentException("all parts must share dimension");
            }
            offsets[i] = running;
            running += parts[i].size();
        }
        offsets[parts.length] = running;
        this.totalSize = running;
    }

    private int partFor(int ord) {
        int lo = 0;
        int hi = parts.length - 1;
        while (lo < hi) {
            int mid = (lo + hi + 1) >>> 1;
            if (offsets[mid] <= ord) {
                lo = mid;
            } else {
                hi = mid - 1;
            }
        }
        return lo;
    }

    @Override
    public float[] vectorValue(int ord) throws IOException {
        int p = partFor(ord);
        return parts[p].vectorValue(ord - offsets[p]);
    }

    @Override
    public int dimension() {
        return dims;
    }

    @Override
    public int size() {
        return totalSize;
    }

    @Override
    public DocIndexIterator iterator() {
        return new ConcatenatedDocIndexIterator(Arrays.stream(parts).map(FloatVectorValues::iterator).toArray(DocIndexIterator[]::new));
    }

    @Override
    public ConcatenatedFloatVectorValues copy() throws IOException {
        FloatVectorValues[] copies = new FloatVectorValues[parts.length];
        for (int i = 0; i < parts.length; i++) {
            copies[i] = parts[i].copy();
        }
        return new ConcatenatedFloatVectorValues(copies);
    }

    /**
     * Iterates parts in order, exposing a single global ordinal space.
     */
    private static final class ConcatenatedDocIndexIterator extends DocIndexIterator {
        private final DocIndexIterator[] partIterators;
        private int partIndex;
        private int globalOrd = -1;

        private ConcatenatedDocIndexIterator(DocIndexIterator[] partIterators) {
            this.partIterators = partIterators;
            this.partIndex = 0;
        }

        @Override
        public int docID() {
            return partIterators[partIndex].docID();
        }

        @Override
        public int nextDoc() throws IOException {
            while (partIndex < partIterators.length) {
                int doc = partIterators[partIndex].nextDoc();
                if (doc != DocIdSetIterator.NO_MORE_DOCS) {
                    globalOrd++;
                    return doc;
                }
                partIndex++;
            }
            return DocIdSetIterator.NO_MORE_DOCS;
        }

        @Override
        public int advance(int target) {
            throw new UnsupportedOperationException();
        }

        @Override
        public long cost() {
            long cost = 0;
            for (DocIndexIterator it : partIterators) {
                cost += it.cost();
            }
            return cost;
        }

        @Override
        public int index() {
            return globalOrd;
        }
    }
}
