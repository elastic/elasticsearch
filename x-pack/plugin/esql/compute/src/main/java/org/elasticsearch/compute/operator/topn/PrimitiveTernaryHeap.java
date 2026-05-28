/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.topn;

import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.core.Releasable;

import java.util.BitSet;

/**
 * A fixed-capacity ternary min-heap over three parallel structures that always move together:
 * a primary {@code long} sort value, an associated {@code int} row position, and a per-slot
 * {@link BitSet} null flag.
 *
 * <p>This class is the storage core of {@link NumericTopNOperator}. It is intentionally
 * {@code final} and uses raw arrays so the hot path (compare, swap, sift) compiles to direct
 * memory accesses and monomorphic call sites — no interface dispatch, no paged-array shims.
 * The triple ({@link #values}, {@link #rowPositions}, {@link #nulls}) is treated as a single
 * logical record indexed by heap slot; the {@code private} {@link #swap(int, int)} method is
 * the only place that touches all three structures so the three remain aligned by construction.
 *
 * <p>The heap is min-ordered on the encoded sort value. To run a top-K, encode each input value
 * so the "winners" compare as greater (e.g. negate for ASC numeric sorts), then push until the
 * heap fills and {@link #updateTop} thereafter when the incoming encoded value is greater than
 * {@link #peekTop()}. The threshold for skipping non-competitive rows is always {@code peekTop()}.
 *
 * <p>Nulls participate in the ordering via the convention encoded by the operator: the operator
 * picks the sentinel encoded value that places nulls correctly for the configured nulls-first /
 * nulls-last semantics and sets the slot's bit in {@link #nulls}. Heap operations themselves
 * only compare the encoded {@code long}; the {@link #nulls} bit is just carried alongside so
 * the operator can reconstruct a null block when draining results.
 *
 * <p>Ternary arity (each parent has up to three children) is chosen over binary so the heap is
 * shallower at the same capacity ({@code log3 K} vs {@code log2 K}), which translates to fewer
 * comparisons in {@code downHeap} on competitive inserts — the operator's dominant cost when
 * the threshold is tight. Indexing is 1-based to avoid an extra subtraction in
 * parent / first-child arithmetic; slot 0 is unused so we allocate {@code K + 1} entries.
 *
 * <p>Memory accounting follows the {@code TopNRow} pattern: one
 * {@link CircuitBreaker#addEstimateBytesAndMaybeBreak} call up front for the arrays and the
 * {@link BitSet} backing storage, paired with a single {@link CircuitBreaker#addWithoutBreaking}
 * release in {@link #close()} and a defensive release if the constructor fails partway through.
 */
final class PrimitiveTernaryHeap implements Releasable {

    private static final long SHALLOW_SIZE = RamUsageEstimator.shallowSizeOfInstance(PrimitiveTernaryHeap.class);
    /**
     * Shallow size of the {@link BitSet} object itself. {@link BitSet#size} is reported by the
     * BitSet (it rounds capacity up to a 64-bit boundary), so the backing-array bytes are
     * computed from {@code BitSet#size() / 8}.
     */
    private static final long BITSET_SHALLOW_SIZE = RamUsageEstimator.shallowSizeOfInstance(BitSet.class);

    private final CircuitBreaker breaker;

    /**
     * Whether nulls are "more competitive" than any non-null. Captures the operator's
     * nulls-first policy in a form the heap can evaluate inside {@link #lessThan(int, int)}
     * without an extra parameter on every call. The hot-path comparison reads this once per
     * field access (constant after construction, so the JIT can hoist it out of the loop).
     */
    private final boolean nullsAreMoreCompetitive;

    /**
     * Encoded sort values. Slot 0 is unused; live slots are {@code 1..size}.
     */
    private final long[] values;
    /**
     * Row positions parallel to {@link #values}. Slot 0 is unused; live slots are
     * {@code 1..size}.
     *
     * <p>Stored as {@code long} (not {@code int}) because the external source's synthetic
     * {@code _rowPosition} column carries a packed value (extractor id high bits + file-local
     * position low bits) that does not fit in 32 bits. The downstream {@code SourceExtractors}
     * registry expects the long round-tripped through TopN unchanged.
     */
    private final long[] rowPositions;
    /**
     * Null flags parallel to {@link #values}: bit {@code i} set means the slot's row was null
     * in the input. Slot 0 is unused. The hot path checks {@link BitSet#isEmpty()} once at the
     * top of {@link #downHeap(int)} so the common (no-nulls-in-heap) case skips per-step
     * {@link BitSet} accesses entirely.
     */
    private final BitSet nulls;

    private final int capacity;
    private int size;
    private int nullsInHeap;
    /**
     * Bytes registered with {@link #breaker}; recorded so {@link #close()} can release exactly
     * what was reserved even if the constructor's last bookkeeping step is updated later.
     */
    private final long registeredBytes;
    private boolean closed;

    PrimitiveTernaryHeap(CircuitBreaker breaker, int capacity, boolean nullsAreMoreCompetitive) {
        if (capacity <= 0) {
            throw new IllegalArgumentException("capacity must be > 0, got [" + capacity + "]");
        }
        this.breaker = breaker;
        this.capacity = capacity;
        this.nullsAreMoreCompetitive = nullsAreMoreCompetitive;
        // BitSet sizes its backing long[] to ceil((K+1)/64) longs; precompute the exact byte
        // footprint so the breaker reservation matches the eventual allocation.
        int bitSetLongs = ((capacity + 1) + 63) >>> 6;
        long bytes = SHALLOW_SIZE + RamUsageEstimator.alignObjectSize(
            RamUsageEstimator.NUM_BYTES_ARRAY_HEADER + (long) (capacity + 1) * Long.BYTES
        ) + RamUsageEstimator.alignObjectSize(RamUsageEstimator.NUM_BYTES_ARRAY_HEADER + (long) (capacity + 1) * Long.BYTES)
            + BITSET_SHALLOW_SIZE + RamUsageEstimator.alignObjectSize(
                RamUsageEstimator.NUM_BYTES_ARRAY_HEADER + (long) bitSetLongs * Long.BYTES
            );
        breaker.addEstimateBytesAndMaybeBreak(bytes, "numeric_topn_heap");
        boolean success = false;
        try {
            this.values = new long[capacity + 1];
            this.rowPositions = new long[capacity + 1];
            this.nulls = new BitSet(capacity + 1);
            this.registeredBytes = bytes;
            this.size = 0;
            success = true;
        } finally {
            if (success == false) {
                breaker.addWithoutBreaking(-bytes);
            }
        }
    }

    int size() {
        return size;
    }

    int capacity() {
        return capacity;
    }

    boolean isFull() {
        return size == capacity;
    }

    /**
     * Encoded value of the current root. Callers use this only for diagnostic reads and for
     * the rejection threshold in the dominant "no nulls in heap" fast path; the precise
     * competitiveness check is {@link #wouldEvictTop(long, long, boolean)} which also accounts
     * for the null bit and the row-position tiebreaker.
     */
    long peekTop() {
        assert size > 0 : "peekTop on empty heap";
        return values[1];
    }

    /**
     * Whether inserting {@code (newValue, newRowPosition, newIsNull)} would replace the
     * current root under the heap's composite ordering. Equivalent to "the proposed entry is
     * strictly greater than the current root", which is the operator's hot-path predicate for
     * choosing between {@link #updateTop} and an O(1) reject.
     *
     * <p>The full triple check is the right thing to do — value-only comparison would either
     * (a) miss inserts that beat the root via the null bit (when null status differs) or (b)
     * silently break stability on equal values. The per-call cost is one BitSet read in the
     * worst case ({@link BitSet#isEmpty()} short-circuit elides it for the dominant
     * no-null-in-heap path).
     */
    boolean wouldEvictTop(long newValue, long newRowPosition, boolean newIsNull) {
        // Null bit check (skipped when nothing in the heap is null AND the proposed entry is
        // not null — i.e. the dominant fast path stays branch-free on values).
        boolean rootIsNull = nulls.isEmpty() == false && nulls.get(1);
        if (rootIsNull != newIsNull) {
            // Root and proposal differ on null status. Proposal evicts iff it is the MORE
            // competitive of the two:
            // nulls-first: nulls are more competitive -> proposal evicts iff it is null.
            // nulls-last: nulls are less competitive -> proposal evicts iff it is non-null.
            return nullsAreMoreCompetitive == newIsNull;
        }
        // Same null status: compare value, then row position.
        long rootValue = values[1];
        if (newValue != rootValue) {
            return newValue > rootValue;
        }
        return Long.compareUnsigned(newRowPosition, rowPositions[1]) < 0;
    }

    /**
     * Whether the current heap root represents a null input row. Used at drain time to
     * reconstruct null blocks; the operator does not need to gate hot-loop comparisons on this.
     */
    boolean topIsNull() {
        assert size > 0 : "topIsNull on empty heap";
        return nulls.get(1);
    }

    long topRowPosition() {
        assert size > 0 : "topRowPosition on empty heap";
        return rowPositions[1];
    }

    /**
     * Insert a new entry. Caller guarantees {@code size < capacity}: the operator's hot loop
     * routes overflowing inserts through {@link #updateTop} instead so the heap never has to
     * resize.
     */
    void push(long value, long rowPosition, boolean isNull) {
        assert size < capacity : "push on full heap";
        size++;
        values[size] = value;
        rowPositions[size] = rowPosition;
        // Only touch the BitSet when the new row is null — for the common all-non-null run we
        // leave the backing bit cleared by construction and nulls.isEmpty() stays true, which
        // the downHeap fast path keys off.
        if (isNull) {
            nulls.set(size);
            nullsInHeap++;
        }
        upHeap(size);
        assert assertNullCount();
    }

    /**
     * Replace the current root with a new entry and restore the heap invariant. Used by the
     * operator when an incoming row is greater than the current threshold (= peekTop) and the
     * heap is already full.
     */
    void updateTop(long value, long rowPosition, boolean isNull) {
        assert size > 0 : "updateTop on empty heap";
        boolean wasNull = nulls.get(1);
        values[1] = value;
        rowPositions[1] = rowPosition;
        if (isNull) {
            nulls.set(1);
        } else {
            // Explicit clear: a slot can transition null -> non-null when overwriting a null
            // root; without this the stale bit would survive into draining.
            nulls.clear(1);
        }
        nullsInHeap += (isNull ? 1 : 0) - (wasNull ? 1 : 0);
        downHeap(1);
        assert assertNullCount();
    }

    /**
     * Pop the current root and return its slot index after the swap so the caller can read
     * {@code values[i]}, {@code rowPositions[i]}, and {@code nulls.get(i)} before the next pop.
     * Used at drain time; not part of the hot insert path.
     *
     * <p>After this returns, {@link #size} has been decremented and the popped slot lives at
     * {@code size + 1}, where the caller can read it. The next call to {@code popTop} (or any
     * mutator) invalidates that index, so callers must copy out before continuing.
     */
    int popTop() {
        assert size > 0 : "popTop on empty heap";
        boolean poppedNull = nulls.get(1);
        int popped = size;
        // Swap root with last live slot, then sift down. The popped slot at index `popped`
        // (formerly index 1) holds the popped values until the next mutation.
        swap(1, popped);
        size--;
        if (poppedNull) {
            nullsInHeap--;
        }
        if (size > 0) {
            downHeap(1);
        }
        assert assertNullCount();
        return popped;
    }

    long valueAt(int slot) {
        return values[slot];
    }

    long rowPositionAt(int slot) {
        return rowPositions[slot];
    }

    boolean isNullAt(int slot) {
        return nulls.get(slot);
    }

    int nullsInHeap() {
        return nullsInHeap;
    }

    /**
     * Single private mutator that moves all three parallel structures atomically. Centralising
     * the swap is what keeps the {@code (value, rowPosition, isNull)} triple aligned through
     * every heap operation; reviewers (and the runtime invariant assertion) only need to verify
     * this one method.
     */
    private void swap(int i, int j) {
        if (i == j) {
            return;
        }
        long v = values[i];
        values[i] = values[j];
        values[j] = v;
        long rp = rowPositions[i];
        rowPositions[i] = rowPositions[j];
        rowPositions[j] = rp;
        // Hot-path skip: an empty nulls BitSet stays empty under a swap (no bits to move).
        // This branch elides every BitSet access for the all-non-null common case.
        if (nulls.isEmpty() == false) {
            boolean ni = nulls.get(i);
            boolean nj = nulls.get(j);
            if (ni != nj) {
                // Swapping preserves the number of live nulls, so nullsInHeap is unchanged here.
                nulls.set(i, nj);
                nulls.set(j, ni);
            }
        }
    }

    /**
     * Heap ordering, primary then secondary then tertiary:
     * <ol>
     *     <li>Null bit: under {@link #nullsAreMoreCompetitive} = {@code true} (nulls-first) a
     *         null slot is <em>less</em> than a non-null slot, so nulls bubble toward the root
     *         and are the candidates for eviction once K nulls are in the heap. Under
     *         {@code false} (nulls-last) a non-null slot is <em>less</em> than a null slot.
     *         This avoids the sentinel-collision trap of "encode null as {@code Long.MAX_VALUE}":
     *         {@code ~Long.MIN_VALUE} also equals {@code Long.MAX_VALUE}, so a sentinel cannot
     *         be distinguished from a real value at the long range's extreme.</li>
     *     <li>Encoded {@link #values} ascending: smaller encoded sorts to the root.</li>
     *     <li>{@link #rowPositions} <em>descending</em> (unsigned): when both null status and
     *         encoded value tie, the slot with the larger row position is "less" and sinks to
     *         the root, so it is the next eviction candidate. This matches the operator's
     *         "first-seen wins" stability.</li>
     * </ol>
     * The {@code nulls.isEmpty()} fast path keeps the per-comparison BitSet access cost at zero
     * for the dominant case (no nulls ever entered the heap).
     */
    private boolean lessThan(int i, int j) {
        // BitSet skip: when nothing in the heap is null, all slots agree on the null bit, so
        // we can collapse to value-then-rowPosition.
        if (nulls.isEmpty() == false) {
            boolean ni = nulls.get(i);
            boolean nj = nulls.get(j);
            if (ni != nj) {
                // i is "less" (sinks toward the root = next eviction candidate) when it is the
                // LESS competitive of the two. Nulls-first means nulls are the most
                // competitive; under that policy a null sinks to the root only when paired
                // with another null — never against a non-null. Nulls-last is the inverse.
                // The pair (ni, nj) is asymmetric here (we already know they differ), so:
                // nulls-first: i is less iff i is non-null -> return ni == false
                // nulls-last: i is less iff i is null -> return ni == true
                return nullsAreMoreCompetitive != ni;
            }
        }
        long vi = values[i];
        long vj = values[j];
        if (vi != vj) {
            return vi < vj;
        }
        // Equal encoded values: the slot with a strictly greater rowPosition is "less" — it
        // sinks toward the root, so it is the one evicted on the next competitive insert.
        // {@code rowPositions} carries the producer's packed (extractor id, file-local
        // position) long; the comparison uses unsigned ordering so the packing is reproducible
        // regardless of the high-bit sign of the packed long.
        return Long.compareUnsigned(rowPositions[i], rowPositions[j]) > 0;
    }

    private void upHeap(int i) {
        while (i > 1) {
            int parent = ((i - 2) / 3) + 1;
            if (lessThan(i, parent)) {
                swap(i, parent);
                i = parent;
            } else {
                break;
            }
        }
    }

    private void downHeap(int i) {
        while (true) {
            int firstChild = 3 * (i - 1) + 2;
            if (firstChild > size) {
                return;
            }
            int smallest = i;
            int c = firstChild;
            if (lessThan(c, smallest)) {
                smallest = c;
            }
            c++;
            if (c <= size && lessThan(c, smallest)) {
                smallest = c;
            }
            c++;
            if (c <= size && lessThan(c, smallest)) {
                smallest = c;
            }
            if (smallest == i) {
                return;
            }
            swap(i, smallest);
            i = smallest;
        }
    }

    /**
     * Test-only walk over every occupied slot {@code [1..size]}, invoking the visitor with the
     * slot index and its {@code (value, rowPosition, isNull)} triple. The visitor must not
     * mutate the heap. Used by the Stage 3 triple-alignment stress test to verify
     * {@link #swap(int, int)} keeps all three structures in lockstep through every up- and
     * down-heap.
     */
    void forEachSlot(SlotVisitor visitor) {
        for (int slot = 1; slot <= size; slot++) {
            visitor.visit(slot, values[slot], rowPositions[slot], nulls.get(slot));
        }
    }

    /**
     * Visitor over an occupied heap slot. See {@link #forEachSlot}.
     */
    @FunctionalInterface
    interface SlotVisitor {
        void visit(int slot, long value, long rowPosition, boolean isNull);
    }

    /**
     * Walks the heap and asserts the heap invariant under the composite ordering used by
     * {@link #lessThan(int, int)}: every parent compares as less-than-or-equal to each of its
     * (up to three) children. Test-only — called from operator unit tests after every mutation
     * in randomised sequences (Stage 3 alignment test).
     */
    boolean assertInvariant() {
        assert assertNullCount();
        for (int parent = 1; parent <= size; parent++) {
            int firstChild = 3 * (parent - 1) + 2;
            for (int c = firstChild; c < firstChild + 3 && c <= size; c++) {
                assert lessThan(c, parent) == false
                    : "heap invariant broken at parent="
                        + parent
                        + " child="
                        + c
                        + " values=["
                        + values[parent]
                        + ","
                        + values[c]
                        + "] rowPositions=["
                        + rowPositions[parent]
                        + ","
                        + rowPositions[c]
                        + "]";
            }
        }
        return true;
    }

    private boolean assertNullCount() {
        int liveNulls = 0;
        for (int slot = 1; slot <= size; slot++) {
            if (nulls.get(slot)) {
                liveNulls++;
            }
        }
        assert nullsInHeap == liveNulls : "nullsInHeap [" + nullsInHeap + "] != live null bits [" + liveNulls + "]";
        return true;
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        breaker.addWithoutBreaking(-registeredBytes);
    }
}
