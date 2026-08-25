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
 * {@link QueryCostEstimator} for a Lucene {@link org.apache.lucene.search.TermInSetQuery}.
 * <p>
 * {@link #estimate()} covers only the <b>structural</b> cost (the built query's {@code ramBytesUsed()}),
 * charged once at query-build time. The <b>execution</b> cost - the per-leaf result {@code DocIdSet} the
 * multi-term constant-score wrapper materialises while scoring - depends on the leaf and is sized
 * separately by {@link #executionBytesForLeaf}.
 */
public final class TermsQueryCostEstimator implements QueryCostEstimator {

    /** Flat floor for the shallow size of a {@code TermInSetQuery}; also the minimum for an empty terms list. */
    public static final long BASE_BYTES = 64L;

    /** Fixed per-leaf overhead for a dense result {@code FixedBitSet}. */
    public static final long FIXED_BITSET_BASE_BYTES = 96L;

    /** Fixed per-leaf overhead for a sparse {@code DocIdSetBuilder}. */
    public static final long DOC_ID_SET_BUILDER_BASE_BYTES = 192L;

    /** Bytes the sparse {@code DocIdSetBuilder} buffer holds per candidate document id. */
    public static final long BYTES_PER_DOC_ID = 4L;

    private static final long BITS_PER_WORD = 64L;
    private static final long BYTES_PER_WORD = 8L;

    private final long termsRamBytes;

    /**
     * @param termsRamBytes {@code ramBytesUsed()} of the already-built {@code TermInSetQuery}; must be {@code >= 0}.
     */
    public TermsQueryCostEstimator(long termsRamBytes) {
        if (termsRamBytes < 0) {
            throw new IllegalArgumentException("termsRamBytes must be >= 0, got: " + termsRamBytes);
        }
        this.termsRamBytes = termsRamBytes;
    }

    /**
     * @return the structural RAM ceiling in bytes; at least {@link #BASE_BYTES}, saturating to {@link Long#MAX_VALUE} on overflow.
     */
    @Override
    public long estimate() {
        try {
            return Math.max(BASE_BYTES, Math.addExact(termsRamBytes, BASE_BYTES));
        } catch (ArithmeticException e) {
            return Long.MAX_VALUE;
        }
    }

    /**
     * Ceiling on the RAM a single leaf's multi-term constant-score scorer allocates: the smaller of a
     * dense {@code FixedBitSet(leafMaxDoc)} and a sparse {@code DocIdSetBuilder} sized to {@code cost}.
     *
     * @param cost       the {@code ScorerSupplier.cost()} estimate of matching documents for this leaf
     * @param leafMaxDoc {@code ctx.reader().maxDoc()} of the leaf; {@code <= 0} yields {@code 0} bytes
     * @return the per-leaf execution RAM ceiling in bytes, saturating to {@link Long#MAX_VALUE} on overflow
     */
    public static long executionBytesForLeaf(long cost, int leafMaxDoc) {
        if (leafMaxDoc <= 0) {
            return 0L;
        }
        try {
            long words = Math.ceilDiv((long) leafMaxDoc, BITS_PER_WORD);
            long denseBytes = Math.addExact(Math.multiplyExact(words, BYTES_PER_WORD), FIXED_BITSET_BASE_BYTES);

            long boundedCost = Math.max(0L, cost);
            long sparseBytes = Math.addExact(DOC_ID_SET_BUILDER_BASE_BYTES, Math.multiplyExact(boundedCost, BYTES_PER_DOC_ID));

            return Math.min(denseBytes, sparseBytes);
        } catch (ArithmeticException e) {
            return Long.MAX_VALUE;
        }
    }
}
