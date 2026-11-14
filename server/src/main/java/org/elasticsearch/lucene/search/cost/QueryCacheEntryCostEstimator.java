/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.lucene.search.cost;

import org.apache.lucene.util.FixedBitSet;

/**
 * Upper bound on the heap that populating one query cache entry allocates, i.e. what
 * {@code LRUQueryCache.cacheImpl} builds for one query on one leaf. Lucene decides the entry's shape from the
 * clause's {@code ScorerSupplier.cost()} alone, before scoring a single document, and so does this estimator,
 * so that what is charged to the request circuit breaker matches what Lucene is about to build:
 * <ul>
 *   <li><b>dense</b> ({@code cost * 100 >= maxDoc}): a {@code FixedBitSet(maxDoc)}, {@code maxDoc / 8} bytes
 *       whatever the clause actually matches. This is the case that exhausts the heap when many searches
 *       populate at once, since doc-values queries report {@code cost == maxDoc} regardless of selectivity.</li>
 *   <li><b>sparse</b> (otherwise): a {@code RoaringDocIdSet}, one sub-set per block of 64K documents. Every
 *       block representation Lucene picks (a {@code short[]} of matching docs, a 64K-bit {@code FixedBitSet}
 *       once more than 4096 match, or a {@code short[]} of the excluded docs once fewer than 4096 do not) holds at
 *       most two bytes per matching document, and at most 8 KiB per block, so the data is bounded by
 *       {@code min(2 * cost, 8 KiB * blocks)}.</li>
 * </ul>
 */
public final class QueryCacheEntryCostEstimator implements QueryCostEstimator {

    /**
     * Fixed overhead of a dense entry: the {@code CacheAndCount} holder, the {@code BitDocIdSet}, the
     * {@code FixedBitSet} and its {@code long[]} header. Conservative on a compressed-oops JVM.
     */
    public static final long DENSE_BASE_BYTES = 96L;

    /**
     * Fixed overhead of a sparse entry independent of its blocks: the {@code CacheAndCount} holder, the
     * {@code RoaringDocIdSet} and the header of its {@code DocIdSet[]}. Conservative on a compressed-oops JVM.
     */
    public static final long ROARING_BASE_BYTES = 96L;

    /**
     * Overhead charged per 64K-document block of a sparse entry, assuming every block is non-empty: the reference
     * in the {@code DocIdSet[]} plus the largest shallow footprint of any block representation (a
     * {@code BitDocIdSet} over a {@code FixedBitSet} with its array header, or a {@code NotDocIdSet} over a
     * {@code short[]}-backed set), with alignment slack. Conservative on a compressed-oops JVM.
     */
    public static final long ROARING_BLOCK_BASE_BYTES = 72L;

    /** Bytes a sparse entry holds per matching document in the worst case: one {@code short} per doc. */
    public static final long ROARING_BYTES_PER_DOC = 2L;

    /**
     * Scratch the {@code RoaringDocIdSet.Builder} holds while a sparse entry is built and drops once it is
     * done: a {@code short[4096]} buffer plus, for a block that overflows it, a 64K-bit {@code FixedBitSet}.
     */
    public static final long ROARING_BUILDER_SCRATCH_BYTES = 2L * (8192L + 64L);

    /** Documents per {@code RoaringDocIdSet} block, mirroring its private {@code BLOCK_SIZE}. */
    static final long ROARING_BLOCK_DOCS = 1L << 16;

    /** Largest data footprint of one block: a 64K-bit {@code FixedBitSet}. */
    static final long ROARING_BLOCK_MAX_DATA_BYTES = ROARING_BLOCK_DOCS / Byte.SIZE;

    private static final long BYTES_PER_REFERENCE = 4L;

    private final long cost;
    private final int maxDoc;

    /**
     * @param cost   the clause's {@code ScorerSupplier.cost()} on the leaf, Lucene's estimate of its matches
     * @param maxDoc {@code ctx.reader().maxDoc()} of the leaf. Must be {@code >= 0}; an empty leaf costs {@code 0} bytes
     */
    public QueryCacheEntryCostEstimator(long cost, int maxDoc) {
        if (maxDoc < 0) {
            throw new IllegalArgumentException("maxDoc must be >= 0, got: " + maxDoc);
        }
        this.cost = cost;
        this.maxDoc = maxDoc;
    }

    /**
     * Whether {@code LRUQueryCache.cacheImpl} builds a dense {@code FixedBitSet} for this clause. Same expression
     * as in Lucene code.
     */
    private boolean isDense() {
        return cost * 100 >= maxDoc;
    }

    /** Ceiling on the bytes allocated to populate the cache entry for this leaf. */
    @Override
    public long estimate() {
        if (maxDoc == 0) {
            return 0L;
        }
        return isDense() ? denseBytes() : roaringBytes();
    }

    /** Ceiling on a dense entry: a {@code FixedBitSet(maxDoc)} plus {@link #DENSE_BASE_BYTES}. */
    private long denseBytes() {
        return (long) FixedBitSet.bits2words(maxDoc) * Long.BYTES + DENSE_BASE_BYTES;
    }

    /**
     * Ceiling on a sparse entry of {@code cost} matches, including the builder's scratch. Grows with the cost
     * until every block is a full bitset, so it never exceeds the data of a dense entry by more than the
     * per-block overhead.
     */
    private long roaringBytes() {
        // Every term is bounded by maxDoc, an int, so none of this can overflow.
        long blocks = Math.ceilDiv(maxDoc, ROARING_BLOCK_DOCS);
        long docs = Math.clamp(cost, 0L, maxDoc);
        long data = Math.min(docs * ROARING_BYTES_PER_DOC, blocks * ROARING_BLOCK_MAX_DATA_BYTES);
        return ROARING_BASE_BYTES + blocks * (ROARING_BLOCK_BASE_BYTES + BYTES_PER_REFERENCE) + data + ROARING_BUILDER_SCRATCH_BYTES;
    }
}
