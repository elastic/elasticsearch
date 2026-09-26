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
 * The instance {@link #estimate()} only covers the <b>structural</b> cost: the query object plus its
 * two {@code byte[]} points, each of length {@code numDims · bytesPerDim}. This is cheap and known at
 * query-build time, so it is charged once while the query tree is walked.
 * <p>
 * The <b>execution</b> cost — the per-segment result {@code DocIdSet} a point-range clause
 * materialises while it runs — is intentionally not part of the structural estimate. It
 * depends on how many documents actually match in each leaf and which Lucene code path runs
 * (dense inverse {@code FixedBitSet}, sparse {@code DocIdSetBuilder}, or a no-allocation match-all),
 * none of which is known until a {@code ScorerSupplier} exists for that leaf. Callers that execute a
 * query per leaf should size that cost with {@link #executionBytesForLeaf} and release it once the
 * leaf has been scored, rather than reserving a worst-case bitset for every segment up front.
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
     * Fixed per-segment overhead charged for the dense result {@code FixedBitSet} (the {@code FixedBitSet}
     * shallow object plus its {@code long[]} array header and the wrapping {@code BitDocIdSet}).
     * Conservative on a compressed-oops JVM.
     */
    public static final long FIXED_BITSET_BASE_BYTES = 48L;

    /**
     * Fixed per-segment overhead charged for the sparse {@code DocIdSetBuilder} path (the builder object
     * plus its growable {@code int[]} buffer header). Conservative on a compressed-oops JVM.
     */
    public static final long DOC_ID_SET_BUILDER_BASE_BYTES = 48L;

    /**
     * Bytes the sparse {@code DocIdSetBuilder} buffer holds per candidate document id (a single
     * {@code int}). The buffer can grow up to one entry per matching document before Lucene upgrades it
     * to a {@code FixedBitSet}, so the sparse cost is bounded by the dense cost (see
     * {@link #executionBytesForLeaf}).
     */
    public static final long BYTES_PER_DOC_ID = 4L;

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

    /**
     * Structural-only estimator.
     *
     * @param numDims     number of indexed dimensions of the point range query. Must be {@code >= 0}.
     * @param bytesPerDim number of bytes encoding each dimension. Must be {@code >= 0}.
     */
    public PointRangeQueryCostEstimator(int numDims, int bytesPerDim) {
        if (numDims < 0) {
            throw new IllegalArgumentException("numDims must be >= 0, got: " + numDims);
        }
        if (bytesPerDim < 0) {
            throw new IllegalArgumentException("bytesPerDim must be >= 0, got: " + bytesPerDim);
        }
        this.numDims = numDims;
        this.bytesPerDim = bytesPerDim;
    }

    /**
     * Returns the structural RAM ceiling in bytes (the query object plus its two point arrays). The
     * result is at least {@link #BASE_BYTES} and saturates to {@link Long#MAX_VALUE} on overflow, so
     * pathologically large inputs cannot wrap to a small reservation.
     */
    @Override
    public long estimate() {
        try {
            long pointLen = Math.multiplyExact(numDims, (long) bytesPerDim);
            long perArray = Math.addExact(ARRAY_HEADER_BYTES, roundUpTo8(pointLen));
            long dynamic = Math.multiplyExact(2L, perArray);
            return Math.max(BASE_BYTES, Math.addExact(BASE_BYTES, dynamic));
        } catch (ArithmeticException e) {
            return Long.MAX_VALUE;
        }
    }

    /**
     * Conservative ceiling on the RAM a single leaf's point-range scorer allocates, modelled on Lucene
     * 10.x {@code PointRangeQuery}:
     * <ul>
     *   <li><b>match-all</b> — the query covers the leaf's whole value range, so Lucene returns a
     *       {@code DocIdSetIterator.all(maxDoc)} without touching the BKD tree: {@code 0} bytes.</li>
     *   <li><b>dense single-valued</b> ({@code cost > maxDoc/2}) — the inverse path allocates a full
     *       {@code FixedBitSet(maxDoc)}.</li>
     *   <li><b>otherwise</b> — a {@code DocIdSetBuilder} buffer sized to the matching-doc estimate
     *       ({@code cost}), capped by the dense {@code FixedBitSet} size since Lucene upgrades a dense
     *       builder to a bitset.</li>
     * </ul>
     * plus, for the two non-match-all paths, the BKD leaf-block traversal scratch.
     *
     * @param cost        the {@code ScorerSupplier.cost()} BKD estimate of matching documents for this leaf
     * @param leafMaxDoc  {@code ctx.reader().maxDoc()} of the leaf. {@code 0} yields {@code 0} bytes.
     * @param singleValued whether the field has at most one value per document in this leaf
     * @param matchAll    whether the query range covers the leaf's entire indexed value range
     * @param numDims     number of indexed dimensions
     * @param bytesPerDim number of bytes encoding each dimension
     * @return the per-leaf execution RAM ceiling in bytes, saturating to {@link Long#MAX_VALUE} on overflow
     */
    public static long executionBytesForLeaf(
        long cost,
        int leafMaxDoc,
        boolean singleValued,
        boolean matchAll,
        int numDims,
        int bytesPerDim
    ) {
        if (leafMaxDoc <= 0) {
            return 0L;
        }
        if (matchAll) {
            return 0L;
        }
        try {
            long words = Math.ceilDiv(leafMaxDoc, BITS_PER_WORD);
            long denseBytes = Math.addExact(Math.multiplyExact(words, BYTES_PER_WORD), FIXED_BITSET_BASE_BYTES);

            long docSetBytes;
            if (singleValued && cost > leafMaxDoc / 2L) {
                docSetBytes = denseBytes;
            } else {
                long boundedCost = Math.max(0L, cost);
                long builderBytes = Math.addExact(DOC_ID_SET_BUILDER_BASE_BYTES, Math.multiplyExact(boundedCost, BYTES_PER_DOC_ID));
                docSetBytes = Math.min(denseBytes, builderBytes);
            }

            long pointLen = Math.multiplyExact(numDims, (long) bytesPerDim);
            long bkdScratch = Math.multiplyExact(MAX_POINTS_IN_LEAF_NODE, pointLen);
            return Math.addExact(docSetBytes, bkdScratch);
        } catch (ArithmeticException e) {
            return Long.MAX_VALUE;
        }
    }

    private static long roundUpTo8(long bytes) {
        long aligned = Math.addExact(bytes, OBJECT_ALIGNMENT_BYTES - 1L);
        return aligned - (aligned % OBJECT_ALIGNMENT_BYTES);
    }
}
