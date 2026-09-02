/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.simdvec.internal.vectorization;

import org.apache.lucene.store.IndexInput;
import org.elasticsearch.simdvec.ESVectorUtil;
import org.elasticsearch.simdvec.internal.BufferScratch;

import java.io.IOException;

/**
 * Calculates the dot-product of BBQ-encoded vectors read from an {@link IndexInput}.
 *
 * <p>A vector quantized to {@code b} bits per dimension is stored as {@code b} bit-planes. Plane
 * {@code p} holds bit {@code p} of every dimension, one bit per dimension, most significant bit
 * first within each byte; plane 0 holds the least significant bit. A plane occupies
 * {@code planeBytes = ceil(dimensions / 8)} bytes, and the planes of one vector are contiguous,
 * lowest first. A document vector is therefore {@code docBits * planeBytes} bytes and a packed query
 * is {@code queryBits * planeBytes} bytes.
 * <p>
 * These methods return the raw ANDed popcounts between the query and data bit-planes.
 * Corrections or other modifications should be applied afterwards.
 * <p>
 * This base class contains scalar implementations suitable for any valid combination of query and data bits.
 */
public class BBQDotProduct {

    /** Widest quantization this kernel supports, matching the widest the formats above it can be configured with. */
    public static final int MAX_BITS = Byte.SIZE;

    protected final IndexInput in;
    /** Bits per dimension of the data vector */
    protected final int docBits;
    /** Bits per dimension of the query vector */
    protected final int queryBits;
    /** Byte length of a single plane, {@code ceil(dimensions / 8)} */
    protected final int planeBytes;
    /** Byte length of one data vector, {@code docBits * planeBytes} */
    protected final int docBytes;
    /** Byte length of the query vector, {@code queryBits * planeBytes} */
    protected final int queryBytes;

    protected final BufferScratch scratch = new BufferScratch();

    /**
     * Factory method for a scalar dot-product implementation.
     *
     * @param in         input positioned at the first document code to score
     * @param docBits    bits per dimension of the document codes, in {@code [1, MAX_BITS]}
     * @param queryBits  bits per dimension of the packed query, in {@code [1, MAX_BITS]}
     * @param planeBytes bytes in a single bit-plane, {@code ceil(dimensions / 8)}
     */
    public static BBQDotProduct create(IndexInput in, int docBits, int queryBits, int planeBytes) {
        return new BBQDotProduct(in, docBits, queryBits, planeBytes);
    }

    protected BBQDotProduct(IndexInput in, int docBits, int queryBits, int planeBytes) {
        checkConfiguration(docBits, queryBits, planeBytes);
        this.in = in;
        this.docBits = docBits;
        this.queryBits = queryBits;
        this.planeBytes = planeBytes;
        this.docBytes = docBits * planeBytes;
        this.queryBytes = queryBits * planeBytes;
    }

    private static void checkConfiguration(int docBits, int queryBits, int planeBytes) {
        if (docBits < 1 || docBits > MAX_BITS) {
            throw new IllegalArgumentException("docBits must be in [1, " + MAX_BITS + "], got: " + docBits);
        }
        if (queryBits < 1 || queryBits > MAX_BITS) {
            throw new IllegalArgumentException("queryBits must be in [1, " + MAX_BITS + "], got: " + queryBits);
        }
        if (planeBytes < 1) {
            throw new IllegalArgumentException("planeBytes must be positive, got: " + planeBytes);
        }
    }

    /**
     * Scores the next data vector and advances the input by {@code docBits * planeBytes} bytes.
     *
     * @param query the query vector, of {@code queryBits * planeBytes} bytes
     */
    public long dotProduct(byte[] query) throws IOException {
        assert query.length == queryBytes : "query length " + query.length + " != " + queryBytes;
        byte[] docPlanes = scratch.apply(docBytes);
        in.readBytes(docPlanes, 0, docBytes);
        return scalarDotProduct(query, docPlanes);
    }

    /**
     * Scores the next {@code count} document codes into {@code scores[0..count)} and advances the
     * input past all of them.
     */
    public void dotProductBulk(byte[] query, int count, float[] scores) throws IOException {
        for (int i = 0; i < count; i++) {
            scores[i] = dotProduct(query);
        }
    }

    /**
     * Scores a subset of the next {@code count} document codes and advances the input past all of
     * them, so that a block can be consumed whole even when most of it is filtered out.
     *
     * @param offsets      positions within the block to score, strictly ascending and all less than {@code count}
     * @param offsetsCount number of valid entries in {@code offsets}
     * @param scores       receives the score at each listed position
     */
    public void dotProductBulkOffsets(byte[] query, int[] offsets, int offsetsCount, float[] scores, int count) throws IOException {
        int next = 0;
        for (int i = 0; i < count; i++) {
            if (next < offsetsCount && offsets[next] == i) {
                next++;
                scores[i] = dotProduct(query);
            } else {
                in.skipBytes(docBytes);
            }
        }
    }

    /**
     * Basic generalized dot-product implementation using AND popcount
     */
    private long scalarDotProduct(byte[] query, byte[] data) {
        long dot = 0;
        for (int qp = 0; qp < queryBits; qp++) {
            for (int dp = 0; dp < docBits; dp++) {
                int popCount = ESVectorUtil.andBitCount(query, qp * planeBytes, data, dp * planeBytes, planeBytes);
                dot += (long) popCount << (qp + dp);
            }
        }
        return dot;
    }
}
