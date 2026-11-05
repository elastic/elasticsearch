/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.sketch;

import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.LongVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.IsBlockedResult;
import org.elasticsearch.compute.operator.Operator;

import java.util.ArrayDeque;
import java.util.Deque;

/**
 * Removes the rows that cannot belong to the top N groups of a {@code COUNT(*) BY key | SORT count DESC | LIMIT N} query, before
 * they reach the aggregation.
 * <p>
 * Every data driver runs one of these operators in front of its partial aggregation. While the input flows in, the operator
 * counts the key of each row in a small {@link CountSketch} and keeps the page in memory. When the input ends, it hands the
 * sketch to the shared {@link SketchFilterBarrier} and waits (without holding a thread) until all drivers have done the same.
 * If the proof then shows that only a few keys can be in the top N, each driver releases its own pages, and only the rows of
 * those keys. The aggregation then builds a tiny table instead of one entry per distinct key. Otherwise, the pages are
 * released unchanged.
 * <p>
 * The operator never changes the result. It only drops rows that cannot be in the answer, and only if the proof passed.
 * It falls back to passing every page through when the key block is not a plain {@link LongVector} (it has nulls or multivalued
 * positions), or when the shared buffer limit is reached.
 */
public final class SketchFilterOperator implements Operator {
    public static final class Factory implements OperatorFactory {
        private final int keyChannel;
        private final int sketchSlots;
        private final int topN;
        private final SketchFilterBarrier barrier;

        /**
         * @param keyChannel       the channel of the {@code long} grouping key
         * @param sketchSlots      the number of sketch slots per driver
         * @param topN             the {@code N} of {@code LIMIT N}
         * @param maxCandidates    the most candidate keys for which filtering is still worth it
         * @param maxBufferedBytes the most bytes of pages that all drivers together can buffer
         */
        public Factory(int keyChannel, int sketchSlots, int topN, int maxCandidates, long maxBufferedBytes) {
            this.keyChannel = keyChannel;
            this.sketchSlots = sketchSlots;
            this.topN = topN;
            this.barrier = new SketchFilterBarrier(topN, maxCandidates, maxBufferedBytes);
        }

        @Override
        public Operator get(DriverContext driverContext) {
            // Register only after the operator exists: a registered operator that never arrives would block all the others.
            SketchFilterOperator operator = new SketchFilterOperator(driverContext, keyChannel, new CountSketch(sketchSlots), barrier);
            barrier.register();
            return operator;
        }

        @Override
        public String describe() {
            return "SketchFilterOperator[keyChannel=" + keyChannel + ", sketchSlots=" + sketchSlots + ", topN=" + topN + "]";
        }
    }

    private record Buffered(Page page, long countedBytes) {}

    private final DriverContext driverContext;
    private final int keyChannel;
    private final SketchFilterBarrier barrier;
    private final long sketchBytes;

    /** The sketch, or {@code null} once this operator has given up, which means that it passes pages through. */
    private CountSketch sketch;
    private final Deque<Buffered> pages = new ArrayDeque<>();
    private boolean passthrough;
    private boolean finished;
    private boolean arrived;
    private boolean closed;
    private int[] positions = new int[0];

    private long rowsReceived;
    private long rowsEmitted;

    SketchFilterOperator(DriverContext driverContext, int keyChannel, CountSketch sketch, SketchFilterBarrier barrier) {
        this.driverContext = driverContext;
        this.keyChannel = keyChannel;
        this.sketch = sketch;
        this.barrier = barrier;
        this.sketchBytes = sketch.ramBytes();
        driverContext.breaker().addEstimateBytesAndMaybeBreak(sketchBytes, "sketch_filter");
    }

    @Override
    public boolean needsInput() {
        return finished == false && (passthrough == false || pages.isEmpty());
    }

    @Override
    public void addInput(Page page) {
        rowsReceived += page.getPositionCount();
        long counted = 0;
        if (passthrough == false) {
            if (barrier.aborted() || sketchPage(page) == false) {
                giveUp();
            } else {
                counted = page.ramBytesUsedByBlocks();
                if (barrier.tryBuffer(counted) == false) {
                    counted = 0;
                    giveUp();
                }
            }
        }
        pages.add(new Buffered(page, counted));
    }

    /** Count the keys of a page. Returns false if the page is not supported, so this operator must give up. */
    private boolean sketchPage(Page page) {
        LongBlock block = page.getBlock(keyChannel);
        LongVector vector = block.asVector();
        if (vector == null) {
            return false; // nulls or multivalued positions
        }
        final CountSketch s = sketch;
        final int count = vector.getPositionCount();
        for (int i = 0; i < count; i++) {
            s.add(vector.getLong(i));
        }
        return true;
    }

    /** Stop sketching and let all pages through. The barrier then decides not to filter. */
    private void giveUp() {
        passthrough = true;
        sketch = null;
    }

    @Override
    public void finish() {
        if (finished) {
            return;
        }
        finished = true;
        arrive();
    }

    private void arrive() {
        if (arrived == false) {
            arrived = true;
            barrier.arrive(sketch);
        }
    }

    @Override
    public IsBlockedResult isBlocked() {
        if (finished && passthrough == false && barrier.isDecided() == false) {
            return new IsBlockedResult(barrier.decided(), "sketch filter barrier");
        }
        return NOT_BLOCKED;
    }

    private boolean canRelease() {
        return passthrough || (finished && barrier.isDecided());
    }

    @Override
    public boolean canProduceMoreDataWithoutExtraInput() {
        return pages.isEmpty() == false && canRelease();
    }

    @Override
    public Page getOutput() {
        if (canRelease() == false) {
            return null;
        }
        final boolean filter = passthrough == false && barrier.filtering();
        while (pages.isEmpty() == false) {
            Buffered buffered = pages.poll();
            barrier.unbuffer(buffered.countedBytes);
            Page page = buffered.page;
            if (filter) {
                page = filter(page);
                if (page == null) {
                    continue;
                }
            }
            rowsEmitted += page.getPositionCount();
            return page;
        }
        return null;
    }

    /** Keep only the rows of candidate keys. Returns null if no row is left. Takes ownership of {@code page}. */
    private Page filter(Page page) {
        final LongSet candidates = barrier.candidates();
        final LongVector keys = ((LongBlock) page.getBlock(keyChannel)).asVector();
        assert keys != null : "pages were checked when they were buffered";
        final int count = keys.getPositionCount();
        if (positions.length < count) {
            positions = new int[count];
        }
        int kept = 0;
        for (int i = 0; i < count; i++) {
            if (candidates.contains(keys.getLong(i))) {
                positions[kept++] = i;
            }
        }
        if (kept == count) {
            return page;
        }
        try {
            if (kept == 0) {
                return null;
            }
            return page.filter(false, positions, 0, kept);
        } finally {
            page.releaseBlocks();
        }
    }

    @Override
    public boolean isFinished() {
        return finished && pages.isEmpty() && canRelease();
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        // An operator that is closed before it finished must still arrive, or the other drivers wait for ever. Its sketch is
        // incomplete, so it arrives without one, which makes the barrier decide not to filter.
        if (finished == false) {
            sketch = null;
        }
        arrive();
        for (Buffered buffered : pages) {
            barrier.unbuffer(buffered.countedBytes);
            buffered.page.releaseBlocks();
        }
        pages.clear();
        sketch = null;
        driverContext.breaker().addWithoutBreaking(-sketchBytes);
    }

    @Override
    public String toString() {
        return "SketchFilterOperator[rowsReceived=" + rowsReceived + ", rowsEmitted=" + rowsEmitted + ", passthrough=" + passthrough + "]";
    }
}
