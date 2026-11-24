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
 * Upper bound on the heap a Lucene {@code DocIdSetBuilder(maxDoc)} holds at its peak, from the first {@code grow} through
 * {@code build()} and the {@code DocIdSet} it returns, given an upper bound on the total count passed to {@code grow}. Points
 * queries collect their matches this way; what each of them passes to {@code grow} is for the caller to bound. Modeled on
 * Lucene 10.x:
 * <ul>
 *   <li><b>sparse</b>: {@code int[]} buffers of at most {@code maxDoc >>> 7} entries in total. They grow by doubling, a
 *       resize copies the buffer and {@code build()} may copy all of them into one array, so up to twice the allocated
 *       entries can be live at once. The built {@code IntArrayDocIdSet} keeps one of those arrays.</li>
 *   <li><b>dense</b>: once a {@code grow} would take the buffers past {@code maxDoc >>> 7} entries, a
 *       {@code FixedBitSet(maxDoc)}, filled from the buffers while they are still live.</li>
 * </ul>
 */
public final class DocIdSetBuilderCostEstimator implements QueryCostEstimator {

    /**
     * Fixed overhead of the builder: the builder object, its buffer list and adder, and the {@code IntArrayDocIdSet} or
     * {@code BitDocIdSet} it builds. Conservative on a compressed-oops JVM.
     */
    public static final long BUILDER_BASE_BYTES = 128L;

    /** Overhead of each buffer: its holder object, its {@code int[]} header and its slot in the buffer list. */
    public static final long BUFFER_BASE_BYTES = 48L;

    /**
     * Bound on the buffer entries allocated per entry requested by {@code grow}. A buffer at most doubles what is already
     * allocated and is only abandoned once it is 7/8 full, so allocation stays below 16/7 of what was requested, plus
     * {@link #MIN_BUFFER_SLACK}.
     */
    static final long ALLOCATION_FACTOR = 3L;

    /** Covers the 32-entry minimum buffer and the spare slot each allocation reserves. */
    static final long MIN_BUFFER_SLACK = 64L;

    /** Smallest buffer the builder allocates, mirroring {@code DocIdSetBuilder#additionalCapacity}. */
    private static final long MIN_BUFFER_LENGTH = 32L;

    private final long grownValues;
    private final int maxDoc;
    /** Most buffer entries the builder allocates before it upgrades, mirroring {@code DocIdSetBuilder#threshold}. */
    private final long threshold;
    /** Bound on the buffer entries allocated while the builder stays sparse. */
    private final long allocatedEntries;
    /**
     * Whether the builder may upgrade to a {@code FixedBitSet}. It does so when the entries already allocated plus a
     * {@code grow} request exceed {@link #threshold}; both are bounded by what is requested in total.
     */
    private final boolean dense;

    /**
     * @param grownValues upper bound on the total count passed to {@code grow} while the set is built
     * @param maxDoc      {@code ctx.reader().maxDoc()} of the leaf. Must be {@code >= 0}; an empty leaf costs {@code 0} bytes
     */
    public DocIdSetBuilderCostEstimator(long grownValues, int maxDoc) {
        if (maxDoc < 0) {
            throw new IllegalArgumentException("maxDoc must be >= 0, got: " + maxDoc);
        }
        this.grownValues = grownValues;
        this.maxDoc = maxDoc;
        this.threshold = maxDoc >>> 7;
        this.allocatedEntries = Math.min(threshold, ALLOCATION_FACTOR * Math.min(grownValues, maxDoc) + MIN_BUFFER_SLACK);
        this.dense = grownValues > threshold || allocatedEntries + grownValues > threshold;
    }

    /** Ceiling on the bytes the builder and the set it builds hold at once. */
    @Override
    public long estimate() {
        if (maxDoc == 0 || grownValues <= 0) {
            return 0L;
        }
        return dense ? denseBytes() : sparseBytes();
    }

    /** Test-only */
    boolean isDense() {
        return dense;
    }

    /** The bit set, plus the buffers it is filled from. */
    private long denseBytes() {
        return BUILDER_BASE_BYTES + FixedBitSetCost.bytes(maxDoc) + bufferBytes(threshold, threshold);
    }

    /** The buffers, with room for a copy of all of them, which a resize or {@code build()} may make. */
    private long sparseBytes() {
        return BUILDER_BASE_BYTES + bufferBytes(allocatedEntries, 2 * allocatedEntries + 1);
    }

    private static long bufferBytes(long allocated, long liveEntries) {
        // Every buffer after the first at least doubles the allocation, up to the last one.
        long buffers = 2 + Long.SIZE - Long.numberOfLeadingZeros(Math.max(1L, allocated / MIN_BUFFER_LENGTH));
        return buffers * BUFFER_BASE_BYTES + liveEntries * Integer.BYTES;
    }
}
