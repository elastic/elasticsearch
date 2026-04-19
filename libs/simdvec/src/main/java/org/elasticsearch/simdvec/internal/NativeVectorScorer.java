/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.simdvec.internal;

import org.apache.lucene.codecs.lucene95.HasIndexSlice;
import org.apache.lucene.index.KnnVectorValues;
import org.apache.lucene.store.FilterIndexInput;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.MemorySegmentAccessInput;
import org.apache.lucene.util.hnsw.RandomVectorScorer;
import org.elasticsearch.core.DirectAccessInput;
import org.elasticsearch.simdvec.MemorySegmentAccessInputAccess;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.util.Optional;

import static org.elasticsearch.simdvec.internal.vectorization.JdkFeatures.SUPPORTS_HEAP_SEGMENTS;

abstract class NativeVectorScorer extends RandomVectorScorer.AbstractRandomVectorScorer {

    static Optional<IndexInput> getNativeAccessibleSegment(KnnVectorValues values) {
        if (SUPPORTS_HEAP_SEGMENTS == false) {
            return Optional.empty();
        }
        IndexInput input = values instanceof HasIndexSlice slice ? slice.getSlice() : null;
        if (input == null) {
            return Optional.empty();
        }
        input = FilterIndexInput.unwrapOnlyTest(input);
        input = MemorySegmentAccessInputAccess.unwrap(input);
        if (input instanceof MemorySegmentAccessInput || input instanceof DirectAccessInput) {
            IndexInputUtils.checkInputType(input);
            checkInvariants(values.size(), values.getVectorByteLength(), input);
            return Optional.of(input);
        }
        return Optional.empty();
    }

    static void checkInvariants(int maxOrd, int vectorByteLength, IndexInput input) {
        if (input.length() < (long) vectorByteLength * maxOrd) {
            throw new IllegalArgumentException("input length is less than expected vector data");
        }
    }

    static void checkDimensions(int queryLen, int fieldLen) {
        if (queryLen != fieldLen) {
            throw new IllegalArgumentException("vector query dimension: " + queryLen + " differs from field dimension: " + fieldLen);
        }
    }

    final int dimensions;
    final int vectorByteSize;
    final IndexInput input;
    final MemorySegment query;

    byte[] scratch;

    NativeVectorScorer(IndexInput input, KnnVectorValues values, MemorySegment query) {
        super(values);
        this.input = input;
        this.dimensions = values.dimension();
        this.vectorByteSize = values.getVectorByteLength();
        this.query = query;
    }

    byte[] getScratch(int length) {
        if (scratch == null || scratch.length < length) {
            scratch = new byte[length];
        }
        return scratch;
    }

    final boolean bulkScore(int[] nodes, float[] scores, int numNodes, BulkScorer bulkScorer) throws IOException {
        if (numNodes == 0) {
            return false;
        }
        return IndexInputUtils.trySingleSlice(input, input.length(), a -> {
            bulkScorer.score(a, query, dimensions, vectorByteSize, MemorySegment.ofArray(nodes), numNodes, MemorySegment.ofArray(scores));
        });
    }

    /**
     * Resolves native memory addresses for the given node ordinals and calls
     * the sparse scoring function. Returns true if addresses were resolved
     * (via mmap or DirectAccessInput), false if fallback scoring is needed.
     */
    final boolean bulkScoreWithSparse(int[] nodes, float[] scores, int numNodes, SparseScorer sparseScorer) throws IOException {
        if (numNodes == 0) {
            return false;
        }
        long[] offsets = new long[numNodes];
        for (int i = 0; i < numNodes; i++) {
            offsets[i] = (long) nodes[i] * vectorByteSize;
        }
        return IndexInputUtils.withSliceAddresses(input, offsets, vectorByteSize, numNodes, a -> {
            sparseScorer.score(a, query, dimensions, numNodes, MemorySegment.ofArray(scores));
        });
    }

    final void checkOrdinal(int ord) {
        if (ord < 0 || ord >= maxOrd()) {
            throw new IllegalArgumentException("illegal ordinal: " + ord);
        }
    }
}
