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
import org.apache.lucene.util.BitUtil;
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
 * lowest first. A data vector is therefore {@code docBits * planeBytes} bytes and a query
 * vector is {@code queryBits * planeBytes} bytes.
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
     * @param in         input positioned at the first data vector to score
     * @param nDims      number of dimensions
     * @param docBits    bits per dimension of the data vector, in {@code [1, MAX_BITS]}
     * @param queryBits  bits per dimension of the query vector, in {@code [1, MAX_BITS]}
     */
    public static BBQDotProduct create(IndexInput in, int nDims, int docBits, int queryBits) {
        int planeBytes = planeBytes(nDims);
        return switch (queryBits) {
            case 1 -> new DxQ1Impl(in, docBits, planeBytes);
            case 4 -> new DxQ4Impl(in, docBits, planeBytes);
            default -> new BBQDotProduct(in, docBits, queryBits, planeBytes);
        };
    }

    public static int planeBytes(int nDims) {
        return (nDims + 7) >>> 3;
    }

    /** Plane width of a bit-plane encoded vector, which stores exactly one plane per data bit */
    public static int planeBytes(int docBits, int dataLength) {
        assert dataLength % docBits == 0 : "data length " + dataLength + " is not " + docBits + " whole planes";
        return dataLength / docBits;
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

        /*
         * Basic generalized dot-product implementation using AND popcount on byte arrays
         */
        byte[] data = scratch.apply(docBytes);
        in.readBytes(data, 0, docBytes);

        long dot = 0;
        for (int qp = 0; qp < queryBits; qp++) {
            for (int dp = 0; dp < docBits; dp++) {
                int popCount = ESVectorUtil.andBitCount(query, qp * planeBytes, data, dp * planeBytes, planeBytes);
                dot += (long) popCount << (qp + dp);
            }
        }
        return dot;
    }

    /**
     * Scores the next {@code count} data vectors into {@code scores[0..count)} and advances the
     * input past all of them.
     */
    public void dotProductBulk(byte[] query, int count, float[] scores) throws IOException {
        for (int i = 0; i < count; i++) {
            scores[i] = dotProduct(query);
        }
    }

    /**
     * Scores a subset of the next {@code count} data vectors, advancing the input past all of them
     * so a block can be consumed whole even when most of it is filtered out.
     * Positions not listed in {@code offsets} are left untouched.
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

    private static final class DxQ1Impl extends BBQDotProduct {

        DxQ1Impl(IndexInput in, int docBits, int planeBytes) {
            super(in, docBits, 1, planeBytes);
        }

        @Override
        public long dotProduct(byte[] query) throws IOException {
            long dot = 0;
            for (int dp = 0; dp < docBits; dp++) {
                dot += dotProductImpl(query) << dp;
            }
            return dot;
        }

        private long dotProductImpl(byte[] q) throws IOException {
            assert q.length == planeBytes : "length mismatch q " + q.length + " vs " + planeBytes;
            long ret = 0;
            int r = 0;
            for (final int upperBound = planeBytes & -Long.BYTES; r < upperBound; r += Long.BYTES) {
                final long value = in.readLong();
                ret += Long.bitCount((long) BitUtil.VH_LE_LONG.get(q, r) & value);
            }
            for (final int upperBound = planeBytes & -Integer.BYTES; r < upperBound; r += Integer.BYTES) {
                final int value = in.readInt();
                ret += Integer.bitCount((int) BitUtil.VH_LE_INT.get(q, r) & value);
            }
            for (; r < planeBytes; r++) {
                final byte value = in.readByte();
                ret += Integer.bitCount((q[r] & value) & 0xFF);
            }
            return ret;
        }
    }

    private static final class DxQ4Impl extends BBQDotProduct {

        DxQ4Impl(IndexInput in, int docBits, int planeBytes) {
            super(in, docBits, 4, planeBytes);
        }

        @Override
        public long dotProduct(byte[] query) throws IOException {
            long dot = 0;
            for (int dp = 0; dp < docBits; dp++) {
                dot += dotProductImpl(query) << dp;
            }
            return dot;
        }

        private long dotProductImpl(byte[] q) throws IOException {
            assert q.length == planeBytes * 4;
            long subRet0 = 0;
            long subRet1 = 0;
            long subRet2 = 0;
            long subRet3 = 0;
            int r = 0;
            for (final int upperBound = planeBytes & -Long.BYTES; r < upperBound; r += Long.BYTES) {
                final long value = in.readLong();
                subRet0 += Long.bitCount((long) BitUtil.VH_LE_LONG.get(q, r) & value);
                subRet1 += Long.bitCount((long) BitUtil.VH_LE_LONG.get(q, r + planeBytes) & value);
                subRet2 += Long.bitCount((long) BitUtil.VH_LE_LONG.get(q, r + 2 * planeBytes) & value);
                subRet3 += Long.bitCount((long) BitUtil.VH_LE_LONG.get(q, r + 3 * planeBytes) & value);
            }
            for (final int upperBound = planeBytes & -Integer.BYTES; r < upperBound; r += Integer.BYTES) {
                final int value = in.readInt();
                subRet0 += Integer.bitCount((int) BitUtil.VH_LE_INT.get(q, r) & value);
                subRet1 += Integer.bitCount((int) BitUtil.VH_LE_INT.get(q, r + planeBytes) & value);
                subRet2 += Integer.bitCount((int) BitUtil.VH_LE_INT.get(q, r + 2 * planeBytes) & value);
                subRet3 += Integer.bitCount((int) BitUtil.VH_LE_INT.get(q, r + 3 * planeBytes) & value);
            }
            for (; r < planeBytes; r++) {
                final byte value = in.readByte();
                subRet0 += Integer.bitCount((q[r] & value) & 0xFF);
                subRet1 += Integer.bitCount((q[r + planeBytes] & value) & 0xFF);
                subRet2 += Integer.bitCount((q[r + 2 * planeBytes] & value) & 0xFF);
                subRet3 += Integer.bitCount((q[r + 3 * planeBytes] & value) & 0xFF);
            }
            return subRet0 + (subRet1 << 1) + (subRet2 << 2) + (subRet3 << 3);
        }
    }
}
