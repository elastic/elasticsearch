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
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.LongVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.core.Nullable;
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
 *         DATE_NANOS map to LONG at planning time, so they go through the LONG path. ESQL FLOAT,
 *         HALF_FLOAT and SCALED_FLOAT widen to DOUBLE on load so they reach this operator as
 *         {@link org.elasticsearch.compute.data.DoubleBlock}.</li>
 *     <li>Input page has exactly 2 channels in this fixed order:
 *         {@code [sortKey, _rowPosition]}. Enforced by
 *         {@code LocalExecutionPlanner.tryBuildNumericTopN()}, asserted at runtime.</li>
 *     <li>Sort key may contain nulls and may be multi-valued. Multi-values are reduced to the
 *         most-favourable single value for the configured direction (MV-min for ASC, MV-max for
 *         DESC) — exactly the semantics the generic {@code TopNOperator} applies via its
 *         {@code KeyExtractorForX} family. An empty MV slot is treated as a null. See
 *         {@link NumericSortKeyExtractor} for the per-type breakdown.</li>
 * </ul>
 *
 * <p>Out of scope (deferred PRs): multi-key sorts, byte-keyed sorts, UNSIGNED_LONG sort keys.
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
     * Factory wired in by {@code LocalExecutionPlanner.tryBuildNumericTopN()}. Carries the topN
     * limit, the sort element type, and the sort order (asc/desc, nulls position) — everything
     * else (channel layout) is fixed by the planner's precondition check.
     */
    public record NumericTopNOperatorFactory(
        int topCount,
        ElementType elementType,
        boolean asc,
        boolean nullsFirst,
        @Nullable SharedNumericThreshold.Supplier thresholdSupplier
    ) implements OperatorFactory {

        public NumericTopNOperatorFactory(int topCount, ElementType elementType, boolean asc, boolean nullsFirst) {
            this(topCount, elementType, asc, nullsFirst, null);
        }

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
            SharedNumericThreshold threshold = thresholdSupplier == null ? null : thresholdSupplier.get();
            try {
                return new NumericTopNOperator(
                    driverContext.blockFactory(),
                    driverContext.breaker(),
                    topCount,
                    elementType,
                    asc,
                    nullsFirst,
                    threshold
                );
            } catch (Exception e) {
                Releasables.closeExpectNoException(threshold);
                throw e;
            }
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
    @Nullable
    private final SharedNumericThreshold threshold;

    private PrimitiveTernaryHeap heap;
    private boolean heapFull;

    private Page output;

    private long receiveNanos;
    private long emitNanos;
    private int pagesReceived;
    private int pagesEmitted;
    private long rowsReceived;
    private long rowsEmitted;
    private int offeredCount;

    NumericTopNOperator(
        BlockFactory blockFactory,
        CircuitBreaker breaker,
        int topCount,
        ElementType elementType,
        boolean asc,
        boolean nullsFirst
    ) {
        this(blockFactory, breaker, topCount, elementType, asc, nullsFirst, null);
    }

    NumericTopNOperator(
        BlockFactory blockFactory,
        CircuitBreaker breaker,
        int topCount,
        ElementType elementType,
        boolean asc,
        boolean nullsFirst,
        @Nullable SharedNumericThreshold threshold
    ) {
        this.blockFactory = blockFactory;
        this.breaker = breaker;
        this.topCount = topCount;
        this.elementType = elementType;
        this.asc = asc;
        this.nullsFirst = nullsFirst;
        this.threshold = threshold;
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
        // Capture the position count up front: the finally block updates the {@code rowsReceived}
        // counter after {@code page.releaseBlocks()} has been called, and we don't want to depend
        // on {@code Page#getPositionCount()} continuing to return the right value after release.
        int positions = page.getPositionCount();
        try {
            assert page.getBlockCount() == 2
                : "NumericTopNOperator expects a 2-channel page [sortKey, _rowPosition]; got blockCount=" + page.getBlockCount();
            Block sortBlock = page.getBlock(SORT_KEY_CHANNEL);
            LongBlock rowPositionBlock = page.getBlock(ROW_POSITION_CHANNEL);

            // The _rowPosition column is emitted by AsyncExternalSourceOperatorFactory in
            // deferredExtraction mode and is, by contract, always a dense non-null LongVector.
            // We read through the Block API to stay decoupled from the producer's exact layout;
            // any null in this channel signals a serious wiring bug, so we fail loudly.
            LongVector rowPositionVector = rowPositionBlock.asVector();
            assert rowPositionVector != null : "_rowPosition channel must be a non-nullable LongVector";

            // Build the per-type, per-direction, per-page extractor once and let the inner loop
            // see a single monomorphic instance. The dispatch matches KeyExtractorForX.extractorFor
            // so MV semantics (MV-min for ASC, MV-max for DESC, empty MV = null) are exactly the
            // ones the generic TopNOperator would have applied to the same page.
            NumericSortKeyExtractor extractor = extractorFor(sortBlock);
            for (int p = 0; p < positions; p++) {
                boolean isNull = extractor.isNullAt(p);
                long encoded = isNull ? 0L : extractor.encodedAt(p);
                ingest(encoded, rowPositionVector.getLong(p), isNull);
            }
        } finally {
            page.releaseBlocks();
            pagesReceived++;
            rowsReceived += positions;
            receiveNanos += System.nanoTime() - start;
        }
    }

    /**
     * Build the per-type extractor for {@code sortBlock} under the configured direction. Picking
     * the extractor concrete class is the operator's only per-page type-dispatch — the rest of
     * {@link #addInput(Page)} stays type-free.
     */
    private NumericSortKeyExtractor extractorFor(Block sortBlock) {
        return switch (elementType) {
            case LONG -> LongSortKeyExtractor.extractorFor((LongBlock) sortBlock, asc);
            case INT -> IntSortKeyExtractor.extractorFor((IntBlock) sortBlock, asc);
            case DOUBLE -> DoubleSortKeyExtractor.extractorFor((DoubleBlock) sortBlock, asc);
            case BOOLEAN -> BooleanSortKeyExtractor.extractorFor((BooleanBlock) sortBlock, asc);
            default -> throw new IllegalStateException(
                "Unreachable: assertSupportedType already validated elementType [" + elementType + "]"
            );
        };
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
                publishThreshold();
            }
            return;
        }
        if (heap.wouldEvictTop(encoded, rowPosition, isNull)) {
            heap.updateTop(encoded, rowPosition, isNull);
            publishThreshold();
        }
    }

    private void publishThreshold() {
        if (threshold == null) {
            return;
        }
        if (nullsFirst && heap.nullsInHeap() == topCount) {
            threshold.markNoFurtherCandidates();
            return;
        }
        // If the K-th heap entry is null under NULLS FIRST, there is no numeric threshold to
        // publish yet and the heap does not track a second-best non-null bound. Readers fall
        // back to "no numeric bound" until the heap saturates with nulls or a numeric root wins.
        if (heap.topIsNull() == false) {
            threshold.offer(decodeLong(heap.peekTop()));
            offeredCount++;
        }
    }

    /**
     * Undo the per-direction bit flip applied by
     * {@link NumericSortKeyExtractor#encode(long, boolean)}. Bitwise NOT is its own inverse, so
     * this is a one-liner. Kept named so the call sites in {@link #buildOutput()} document
     * intent. Decoding stays here because the operator owns the round-trip back to the per-type
     * raw form in {@link #buildSortBlock}.
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
            Releasables.closeExpectNoException(heap, threshold);
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
        return new TopNOperatorStatus(
            receiveNanos,
            emitNanos,
            heap != null ? heap.size() : 0,
            0L,
            pagesReceived,
            pagesEmitted,
            rowsReceived,
            rowsEmitted,
            offeredCount
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
