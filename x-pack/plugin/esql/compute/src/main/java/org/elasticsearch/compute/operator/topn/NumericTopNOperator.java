/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.topn;

import org.apache.lucene.util.NumericUtils;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BooleanVector;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.DoubleVector;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.LongVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.core.Releasables;

import java.util.Locale;

/**
 * Specialised, allocation-free Top-K operator for a single fixed-width numeric sort key sitting
 * directly above an {@code ExternalSourceExec} that has been narrowed to
 * {@code [sortKey, _rowPosition]} by the {@code InsertExternalFieldExtraction} rule.
 *
 * <p>The operator replaces {@link TopNOperator} for this specific plan shape: instead of
 * encoding each sort key into a {@code BytesRef} and ranking {@code TopNRow} objects in a
 * Lucene {@link org.apache.lucene.util.PriorityQueue}, it ranks primitive {@code long} encodings
 * directly using {@link PrimitiveTernaryHeap}. The encoding maps the raw sort value to a long
 * such that "more competitive" corresponds to "larger encoded" — a min-heap of size K then
 * exactly holds the top-K most competitive rows, with the root being the rejection threshold
 * for the next incoming row.
 *
 * <p>Scope (PR 1, Tier 1):
 * <ul>
 *     <li>Exactly one sort key.</li>
 *     <li>Sort key {@link ElementType} is {@link ElementType#LONG}, {@link ElementType#INT},
 *         {@link ElementType#DOUBLE}, or {@link ElementType#BOOLEAN}. ESQL DATETIME and
 *         DATE_NANOS map to LONG at planning time, so they go through the LONG path.</li>
 *     <li>Input page has exactly 2 channels in this fixed order:
 *         {@code [sortKey, _rowPosition]}. Enforced by the
 *         {@code ReplaceTopNWithNumericTopN} optimizer rule, asserted at runtime.</li>
 *     <li>Sort key may contain nulls but must be single-valued: multi-valued blocks trigger
 *         {@link IllegalStateException} at runtime with a rewrite hint.</li>
 * </ul>
 *
 * <p>Out of scope (deferred PRs): cross-driver shared threshold, row-group skipping in the
 * external source, multi-key sorts, byte-keyed sorts, FLOAT / UNSIGNED_LONG / HALF_FLOAT /
 * SCALED_FLOAT sort keys.
 */
public final class NumericTopNOperator implements Operator {

    /**
     * Channel of the primary sort key in every input page.
     */
    public static final int SORT_KEY_CHANNEL = 0;
    /**
     * Channel of the synthetic {@code _rowPosition} column in every input page.
     */
    public static final int ROW_POSITION_CHANNEL = 1;

    /**
     * Factory wired in by {@code LocalExecutionPlanner.planNumericTopN()}. Carries the topN
     * limit, the sort element type, and the sort order (asc/desc, nulls position) — everything
     * else (channel layout) is fixed by the rule's precondition.
     */
    public record NumericTopNOperatorFactory(int topCount, ElementType elementType, boolean asc, boolean nullsFirst)
        implements
            OperatorFactory {

        public NumericTopNOperatorFactory {
            if (topCount <= 0) {
                throw new IllegalArgumentException("topCount must be > 0, got [" + topCount + "]");
            }
            if (elementType == null) {
                throw new IllegalArgumentException("elementType must not be null");
            }
            assertSupportedType(elementType);
        }

        @Override
        public Operator get(DriverContext driverContext) {
            return new NumericTopNOperator(driverContext.blockFactory(), driverContext.breaker(), topCount, elementType, asc, nullsFirst);
        }

        @Override
        public String describe() {
            return String.format(
                Locale.ROOT,
                "NumericTopNOperator[count=%d, elementType=%s, asc=%s, nullsFirst=%s]",
                topCount,
                elementType,
                asc,
                nullsFirst
            );
        }
    }

    private final BlockFactory blockFactory;
    private final CircuitBreaker breaker;
    private final int topCount;
    private final ElementType elementType;
    private final boolean asc;
    private final boolean nullsFirst;

    private PrimitiveTernaryHeap heap;
    private boolean heapFull;

    private Page output;

    private long receiveNanos;
    private long emitNanos;
    private int pagesReceived;
    private int pagesEmitted;
    private long rowsReceived;
    private long rowsEmitted;

    NumericTopNOperator(
        BlockFactory blockFactory,
        CircuitBreaker breaker,
        int topCount,
        ElementType elementType,
        boolean asc,
        boolean nullsFirst
    ) {
        this.blockFactory = blockFactory;
        this.breaker = breaker;
        this.topCount = topCount;
        this.elementType = elementType;
        this.asc = asc;
        this.nullsFirst = nullsFirst;
        assertSupportedType(elementType);
        // Pass nullsFirst through to the heap so its composite {@code lessThan} ordering
        // handles the null bit directly. This avoids the sentinel-collision trap (encoding a
        // null as {@link Long#MAX_VALUE} collides with the encoded form of {@link Long#MIN_VALUE}
        // raw under ASC, where {@code ~Long.MIN_VALUE == Long.MAX_VALUE}); see the heap's
        // {@link PrimitiveTernaryHeap#lessThan(int, int)} javadoc for the full argument.
        this.heap = new PrimitiveTernaryHeap(breaker, topCount, nullsFirst);
    }

    private static void assertSupportedType(ElementType elementType) {
        // The optimizer rule is the real gate; this is a defence-in-depth check so an
        // accidental cross-type wiring fails loudly with a clear message.
        switch (elementType) {
            case LONG, INT, DOUBLE, BOOLEAN -> {
                // supported in PR 1
            }
            default -> throw new IllegalArgumentException(
                "NumericTopNOperator does not support ElementType [" + elementType + "]; supported types are LONG, INT, DOUBLE, BOOLEAN"
            );
        }
    }

    @Override
    public boolean needsInput() {
        return output == null;
    }

    @Override
    public void addInput(Page page) {
        long start = System.nanoTime();
        try {
            assert page.getBlockCount() == 2
                : "NumericTopNOperator expects a 2-channel page [sortKey, _rowPosition]; got blockCount=" + page.getBlockCount();
            Block sortBlock = page.getBlock(SORT_KEY_CHANNEL);
            LongBlock rowPositionBlock = page.getBlock(ROW_POSITION_CHANNEL);

            if (sortBlock.mayHaveMultivaluedFields()) {
                throw new IllegalStateException(
                    "NumericTopNOperator requires single-valued sort keys; "
                        + "got a multi-valued page in the sort key channel. "
                        + "Wrap the sort key with MV_MIN(...) or MV_MAX(...)."
                );
            }

            // The _rowPosition column is emitted by AsyncExternalSourceOperatorFactory in
            // deferredExtraction mode and is, by contract, always a dense non-null LongVector.
            // We read through the Block API to stay decoupled from the producer's exact layout;
            // any null in this channel signals a serious wiring bug, so we fail loudly.
            LongVector rowPositionVector = rowPositionBlock.asVector();
            assert rowPositionVector != null : "_rowPosition channel must be a non-nullable LongVector";

            int positions = page.getPositionCount();
            switch (elementType) {
                case LONG -> ingestLong((LongBlock) sortBlock, rowPositionVector, positions);
                case INT -> ingestInt((IntBlock) sortBlock, rowPositionVector, positions);
                case DOUBLE -> ingestDouble((DoubleBlock) sortBlock, rowPositionVector, positions);
                case BOOLEAN -> ingestBoolean((BooleanBlock) sortBlock, rowPositionVector, positions);
                default -> throw new IllegalStateException(
                    "Unreachable: assertSupportedType already validated elementType [" + elementType + "]"
                );
            }
        } finally {
            page.releaseBlocks();
            pagesReceived++;
            rowsReceived += page.getPositionCount();
            receiveNanos += System.nanoTime() - start;
        }
    }

    private void ingestLong(LongBlock sortBlock, LongVector rowPositionVector, int positions) {
        LongVector v = sortBlock.asVector();
        if (v != null) {
            for (int p = 0; p < positions; p++) {
                ingest(encodeLong(v.getLong(p)), rowPositionVector.getLong(p), false);
            }
        } else {
            for (int p = 0; p < positions; p++) {
                long rp = rowPositionVector.getLong(p);
                if (sortBlock.isNull(p)) {
                    ingest(0L, rp, true);
                } else {
                    long raw = sortBlock.getLong(sortBlock.getFirstValueIndex(p));
                    ingest(encodeLong(raw), rp, false);
                }
            }
        }
    }

    private void ingestInt(IntBlock sortBlock, LongVector rowPositionVector, int positions) {
        IntVector v = sortBlock.asVector();
        if (v != null) {
            for (int p = 0; p < positions; p++) {
                // Widen to long: signed extension preserves ordering, so the encoded value is
                // identical to LONG's identity encoding under both ASC and DESC.
                ingest(encodeLong(v.getInt(p)), rowPositionVector.getLong(p), false);
            }
        } else {
            for (int p = 0; p < positions; p++) {
                long rp = rowPositionVector.getLong(p);
                if (sortBlock.isNull(p)) {
                    ingest(0L, rp, true);
                } else {
                    int raw = sortBlock.getInt(sortBlock.getFirstValueIndex(p));
                    ingest(encodeLong(raw), rp, false);
                }
            }
        }
    }

    private void ingestDouble(DoubleBlock sortBlock, LongVector rowPositionVector, int positions) {
        DoubleVector v = sortBlock.asVector();
        if (v != null) {
            for (int p = 0; p < positions; p++) {
                // {@link NumericUtils#doubleToSortableLong} maps doubles to longs preserving
                // numeric order (handles signed zeros, NaNs, and the sign bit correctly). The
                // resulting long can then be encoded the same way as a LONG sort key.
                ingest(encodeLong(NumericUtils.doubleToSortableLong(v.getDouble(p))), rowPositionVector.getLong(p), false);
            }
        } else {
            for (int p = 0; p < positions; p++) {
                long rp = rowPositionVector.getLong(p);
                if (sortBlock.isNull(p)) {
                    ingest(0L, rp, true);
                } else {
                    double raw = sortBlock.getDouble(sortBlock.getFirstValueIndex(p));
                    ingest(encodeLong(NumericUtils.doubleToSortableLong(raw)), rp, false);
                }
            }
        }
    }

    private void ingestBoolean(BooleanBlock sortBlock, LongVector rowPositionVector, int positions) {
        BooleanVector v = sortBlock.asVector();
        if (v != null) {
            for (int p = 0; p < positions; p++) {
                ingest(encodeLong(v.getBoolean(p) ? 1L : 0L), rowPositionVector.getLong(p), false);
            }
        } else {
            for (int p = 0; p < positions; p++) {
                long rp = rowPositionVector.getLong(p);
                if (sortBlock.isNull(p)) {
                    ingest(0L, rp, true);
                } else {
                    boolean raw = sortBlock.getBoolean(sortBlock.getFirstValueIndex(p));
                    ingest(encodeLong(raw ? 1L : 0L), rp, false);
                }
            }
        }
    }

    /**
     * Decision point for every incoming row. Three outcomes:
     * <ul>
     *     <li>Heap not yet full: push unconditionally. Flip {@link #heapFull} on the push that
     *         fills it.</li>
     *     <li>Heap full and the row would evict the root under the composite ordering:
     *         replace the root.</li>
     *     <li>Otherwise: O(1) reject.</li>
     * </ul>
     */
    private void ingest(long encoded, long rowPosition, boolean isNull) {
        if (heapFull == false) {
            heap.push(encoded, rowPosition, isNull);
            if (heap.isFull()) {
                heapFull = true;
            }
            return;
        }
        if (heap.wouldEvictTop(encoded, rowPosition, isNull)) {
            heap.updateTop(encoded, rowPosition, isNull);
        }
    }

    /**
     * Encode the raw sort value so that, under the configured order, a "more competitive" raw
     * value maps to a larger encoded long. {@link PrimitiveTernaryHeap} is a min-heap, so the
     * root after K inserts is the smallest encoded = the least competitive survivor = the
     * rejection threshold for the next row.
     *
     * <ul>
     *     <li>DESC (largest raw wins): {@code encoded = raw}. Root = smallest raw survivor.</li>
     *     <li>ASC (smallest raw wins): {@code encoded = ~raw}. Bitwise complement is the
     *         monotonically-decreasing involution on signed longs that flips the order without
     *         the overflow trap of unary negation (which is undefined for {@link Long#MIN_VALUE}).
     *         Root = largest raw survivor (its complement is smallest in encoded space).</li>
     * </ul>
     */
    private long encodeLong(long raw) {
        return asc ? ~raw : raw;
    }

    /**
     * Inverse of {@link #encodeLong(long)}: bitwise NOT is its own inverse, so this is a
     * one-liner. Kept named so the call sites in {@link #buildOutput()} document intent.
     */
    private long decodeLong(long encoded) {
        return asc ? ~encoded : encoded;
    }

    @Override
    public void finish() {
        if (output == null) {
            long start = System.nanoTime();
            output = buildOutput();
            emitNanos += System.nanoTime() - start;
        }
    }

    @Override
    public boolean isFinished() {
        return output != null && output == EMPTY_OUTPUT;
    }

    @Override
    public boolean canProduceMoreDataWithoutExtraInput() {
        return output != null && output != EMPTY_OUTPUT;
    }

    /**
     * Sentinel returned from {@link #getOutput()} to signal "already produced my single page".
     * Using an object identity rather than {@code null} lets {@link #needsInput()} and
     * {@link #isFinished()} distinguish "haven't finished yet" from "produced and drained".
     */
    private static final Page EMPTY_OUTPUT = new Page(0);

    @Override
    public Page getOutput() {
        if (output == null || output == EMPTY_OUTPUT) {
            return null;
        }
        Page ret = output;
        output = EMPTY_OUTPUT;
        pagesEmitted++;
        rowsEmitted += ret.getPositionCount();
        return ret;
    }

    /**
     * Drain the heap into a single output page of shape {@code [sortKey, _rowPosition]} in the
     * configured sort order. Implementation: {@link PrimitiveTernaryHeap#popTop()} repeatedly
     * yields the slot index of each popped entry in increasing-encoded order; we record into
     * a reversed buffer so the emitted page is in descending-encoded order = ascending-raw
     * order for ASC sorts, descending-raw for DESC sorts.
     *
     * <p>Allocation: the operator only owns the long buffer for raw values, the int buffer for
     * row positions, and a boolean array for null flags. The block builders for the output page
     * are sized exactly to the surviving heap size (≤ K) and built directly from those buffers.
     */
    private Page buildOutput() {
        int size = heap.size();
        if (size == 0) {
            // Empty result: emit no page. Driver treats this as "operator finished with no
            // output", which is exactly the desired semantics for a TopN over zero input.
            return EMPTY_OUTPUT;
        }
        long[] encodedValues = new long[size];
        long[] outRowPositions = new long[size];
        boolean[] isNull = new boolean[size];
        // Pop yields entries in min-first (least-competitive-first) order. We want the output
        // page in most-competitive-first order, so we fill the buffers from the end.
        for (int i = size - 1; i >= 0; i--) {
            int slot = heap.popTop();
            isNull[i] = heap.isNullAt(slot);
            outRowPositions[i] = heap.rowPositionAt(slot);
            encodedValues[i] = isNull[i] ? 0L : heap.valueAt(slot);
        }
        // Build the two output blocks. The sort key column may contain nulls (carried over
        // from the input); the _rowPosition column is dense non-nullable by construction.
        Block sortBlock = null;
        LongVector rowPositionVector = null;
        boolean success = false;
        try {
            sortBlock = buildSortBlock(size, encodedValues, isNull);
            try (LongVector.FixedBuilder rowPositionBuilder = blockFactory.newLongVectorFixedBuilder(size)) {
                for (int i = 0; i < size; i++) {
                    rowPositionBuilder.appendLong(outRowPositions[i]);
                }
                rowPositionVector = rowPositionBuilder.build();
            }
            Page page = new Page(size, sortBlock, rowPositionVector.asBlock());
            success = true;
            return page;
        } finally {
            if (success == false) {
                if (sortBlock != null) {
                    sortBlock.close();
                }
                Releasables.close(rowPositionVector);
            }
        }
    }

    /**
     * Build the sort-key output block as the type the operator was configured for. Each
     * encoded value is undone by {@link #decodeLong} (bitwise NOT for ASC, identity for DESC)
     * and then narrowed / round-tripped back into the type's raw form before being appended.
     */
    private Block buildSortBlock(int size, long[] encodedValues, boolean[] isNull) {
        return switch (elementType) {
            case LONG -> {
                try (LongBlock.Builder b = blockFactory.newLongBlockBuilder(size)) {
                    for (int i = 0; i < size; i++) {
                        if (isNull[i]) {
                            b.appendNull();
                        } else {
                            b.appendLong(decodeLong(encodedValues[i]));
                        }
                    }
                    yield b.build();
                }
            }
            case INT -> {
                try (IntBlock.Builder b = blockFactory.newIntBlockBuilder(size)) {
                    for (int i = 0; i < size; i++) {
                        if (isNull[i]) {
                            b.appendNull();
                        } else {
                            // The encoded long was widened from int; narrowing back is safe
                            // because we never modified the low 32 bits (signed extension only
                            // touched the upper bits).
                            b.appendInt((int) decodeLong(encodedValues[i]));
                        }
                    }
                    yield b.build();
                }
            }
            case DOUBLE -> {
                try (DoubleBlock.Builder b = blockFactory.newDoubleBlockBuilder(size)) {
                    for (int i = 0; i < size; i++) {
                        if (isNull[i]) {
                            b.appendNull();
                        } else {
                            b.appendDouble(NumericUtils.sortableLongToDouble(decodeLong(encodedValues[i])));
                        }
                    }
                    yield b.build();
                }
            }
            case BOOLEAN -> {
                try (BooleanBlock.Builder b = blockFactory.newBooleanBlockBuilder(size)) {
                    for (int i = 0; i < size; i++) {
                        if (isNull[i]) {
                            b.appendNull();
                        } else {
                            b.appendBoolean(decodeLong(encodedValues[i]) != 0L);
                        }
                    }
                    yield b.build();
                }
            }
            default -> throw new IllegalStateException("Unreachable: " + elementType);
        };
    }

    @Override
    public void close() {
        // The heap owns its breaker accounting and is always safe to close. The output page is
        // released only if finish() built one and getOutput() never drained it (e.g. driver
        // aborted between finish and the first getOutput). EMPTY_OUTPUT is a shared sentinel
        // and must not be closed.
        Page pageToRelease = (output != null && output != EMPTY_OUTPUT) ? output : null;
        try {
            Releasables.closeExpectNoException(heap);
        } finally {
            if (pageToRelease != null) {
                pageToRelease.releaseBlocks();
            }
            heap = null;
            output = EMPTY_OUTPUT;
        }
    }

    @Override
    public Status status() {
        // {@code minCompetitiveUpdates} is null here — the numeric operator doesn't yet publish a
        // cross-driver shared threshold (PR 2 / PR 3 work), so there is no "tracked" counter to
        // report. Passing 0 would lie by suggesting the threshold is being tracked but never
        // updated; null is the explicit "not tracked" sentinel that the status field's nullable
        // {@link Integer} type was designed for.
        return new TopNOperatorStatus(
            receiveNanos,
            emitNanos,
            heap != null ? heap.size() : 0,
            0L,
            pagesReceived,
            pagesEmitted,
            rowsReceived,
            rowsEmitted,
            null
        );
    }

    @Override
    public String toString() {
        return String.format(
            Locale.ROOT,
            "NumericTopNOperator[count=%d/%d, elementType=%s, asc=%s, nullsFirst=%s]",
            heap == null ? 0 : heap.size(),
            topCount,
            elementType,
            asc,
            nullsFirst
        );
    }
}
