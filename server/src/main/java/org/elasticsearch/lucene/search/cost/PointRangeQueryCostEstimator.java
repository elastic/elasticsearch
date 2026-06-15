/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.lucene.search.cost;

/**
 * {@link QueryCostEstimator} for a Lucene {@link org.apache.lucene.search.PointRangeQuery}: an upper
 * bound on the bytes a point-range clause should reserve from the request circuit breaker.
 * <p>
 * The estimate sums two terms:
 * <ul>
 *   <li><b>Structural</b> — the query object plus its two {@code byte[]} points, each of length
 *       {@code numDims · bytesPerDim}: {@code max(BASE_BYTES, BASE_BYTES + 2 · (ARRAY_HEADER_BYTES + roundUpTo8(pointLen)))}.</li>
 *   <li><b>Execution</b> — RAM allocated while the query runs, per searched segment: a result
 *       {@code FixedBitSet} of {@code maxDoc} bits ({@code ceilDiv(maxDoc, 64) · 8} bytes plus
 *       {@link #FIXED_BITSET_BASE_BYTES} object overhead) and BKD scratch bounded by
 *       {@link #MAX_POINTS_IN_LEAF_NODE} encoded points ({@code MAX_POINTS_IN_LEAF_NODE · pointLen}).
 *       Summed across all {@code numSegments} as if every structure is live at once.</li>
 * </ul>
 * Pass {@code maxDoc == 0} or {@code numSegments == 0} for a structural-only estimate (no reader available).
 */
public final class PointRangeQueryCostEstimator implements QueryCostEstimator {

    /**
     * Flat floor covering the shallow size of the {@code PointRangeQuery} object (object header, the
     * {@code numDims}/{@code bytesPerDim} {@code int} fields, and the {@code field}/{@code lowerPoint}/
     * {@code upperPoint} references) with margin for JVM object-layout differences. Also the minimum
     * reservation for degenerate zero-dimension queries.
     */
    public static final long BASE_BYTES = 64L;

    /**
     * Per-{@code byte[]} object header charged for each of the two point arrays. {@code 16} matches the
     * array header on a HotSpot JVM with compressed oops; it is conservative on layouts with a smaller
     * header, which keeps the estimate a ceiling.
     */
    public static final long ARRAY_HEADER_BYTES = 16L;

    /**
     * Fixed per-segment overhead charged for the result {@code DocIdSetBuilder}/{@code FixedBitSet}
     * objects (the {@code FixedBitSet} shallow object plus its {@code long[]} array header and the
     * wrapping {@code BitDocIdSet}). Conservative on a compressed-oops JVM.
     */
    public static final long FIXED_BITSET_BASE_BYTES = 48L;

    /**
     * Maximum number of points held in a BKD leaf block ({@code BKDConfig}'s default). The per-segment
     * BKD traversal scratch is bounded by this many encoded points, i.e.
     * {@code MAX_POINTS_IN_LEAF_NODE · numDims · bytesPerDim} bytes.
     */
    public static final long MAX_POINTS_IN_LEAF_NODE = 512L;

    private static final long BITS_PER_WORD = 64L;
    private static final long BYTES_PER_WORD = 8L;
    private static final long OBJECT_ALIGNMENT_BYTES = 8L;

    private final int numDims;
    private final int bytesPerDim;
    private final int maxDoc;
    private final int numSegments;

    /**
     * Estimator including the execution-time term modelled from the searched segments. Pass
     * {@code maxDoc == 0} or {@code numSegments == 0} for a structural-only estimate when no reader is
     * available.
     *
     * @param numDims     number of indexed dimensions of the point range query. Must be {@code >= 0}.
     * @param bytesPerDim number of bytes encoding each dimension. Must be {@code >= 0}.
     * @param maxDoc      total number of documents across the searched segments (e.g.
     *                    {@code reader.maxDoc()}). Must be {@code >= 0}; {@code 0} disables the
     *                    execution-time term.
     * @param numSegments number of searched segments (e.g. {@code reader.leaves().size()}). Must be
     *                    {@code >= 0}; {@code 0} disables the execution-time term.
     */
    public PointRangeQueryCostEstimator(int numDims, int bytesPerDim, int maxDoc, int numSegments) {
        if (numDims < 0) {
            throw new IllegalArgumentException("numDims must be >= 0, got: " + numDims);
        }
        if (bytesPerDim < 0) {
            throw new IllegalArgumentException("bytesPerDim must be >= 0, got: " + bytesPerDim);
        }
        if (maxDoc < 0) {
            throw new IllegalArgumentException("maxDoc must be >= 0, got: " + maxDoc);
        }
        if (numSegments < 0) {
            throw new IllegalArgumentException("numSegments must be >= 0, got: " + numSegments);
        }
        this.numDims = numDims;
        this.bytesPerDim = bytesPerDim;
        this.maxDoc = maxDoc;
        this.numSegments = numSegments;
    }

    /**
     * Returns the structural-plus-execution RAM ceiling in bytes. The result is at least
     * {@link #BASE_BYTES} and saturates to {@link Long#MAX_VALUE} on overflow, so pathologically large
     * inputs cannot wrap to a small reservation.
     */
    @Override
    public long estimate() {
        try {
            long pointLen = Math.multiplyExact(numDims, (long) bytesPerDim);
            long perArray = Math.addExact(ARRAY_HEADER_BYTES, roundUpTo8(pointLen));
            long dynamic = Math.multiplyExact(2L, perArray);
            long structural = Math.max(BASE_BYTES, Math.addExact(BASE_BYTES, dynamic));
            return Math.addExact(structural, executionBytes(pointLen));
        } catch (ArithmeticException e) {
            return Long.MAX_VALUE;
        }
    }

    /**
     * Conservative ceiling on the per-segment result {@code FixedBitSet}s and BKD traversal scratch,
     * summed across all searched segments. Returns {@code 0} when no segment information is available.
     */
    private long executionBytes(long pointLen) {
        if (maxDoc == 0 || numSegments == 0) {
            return 0L;
        }
        long words = Math.addExact(Math.ceilDiv(maxDoc, BITS_PER_WORD), numSegments);
        long bitSetData = Math.multiplyExact(words, BYTES_PER_WORD);
        long bitSetObjects = Math.multiplyExact(numSegments, FIXED_BITSET_BASE_BYTES);
        long bitSet = Math.addExact(bitSetData, bitSetObjects);
        long bkdScratchPerSegment = Math.multiplyExact(MAX_POINTS_IN_LEAF_NODE, pointLen);
        long bkdScratch = Math.multiplyExact(numSegments, bkdScratchPerSegment);
        return Math.addExact(bitSet, bkdScratch);
    }

    private static long roundUpTo8(long bytes) {
        long aligned = Math.addExact(bytes, OBJECT_ALIGNMENT_BYTES - 1L);
        return aligned - (aligned % OBJECT_ALIGNMENT_BYTES);
    }
}
