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

    /**
     * Pages with more candidate keys than this are always read: each candidate adds bits to test, and the filter then skips fewer pages.
     */
    private static final int MAX_BLOOM_CANDIDATES = 64;

    /** A buffered page, the bytes it counts against the shared buffer limit, and the Bloom filter of its keys (null if none). */
    private record Buffered(Page page, long countedBytes, long[] bloom, int bloomShift) {
        long bloomBytes() {
            return bloom == null ? 0 : (long) bloom.length * Long.BYTES;
        }
    }

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
    private long[] keys = new long[0];

    /** Set by {@link #sketchPage}: the Bloom filter of the page that was just counted. */
    private long[] pageBloom;
    private int pageBloomShift;

    private long rowsReceived;
    private long rowsEmitted;
    private long pagesBuffered;
    private long pagesSkipped;

    /** Timing for the profile, see {@link #toString}. All are {@link System#nanoTime} values or durations, {@code 0} until set. */
    private long firstInputNanos;
    private long arrivedNanos;
    private long lastOutputNanos;
    private long sketchNanos;
    private long filterNanos;

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
        if (firstInputNanos == 0) {
            firstInputNanos = System.nanoTime();
        }
        long counted = 0;
        long[] bloom = null;
        int bloomShift = 0;
        if (passthrough == false) {
            final long sketchStart = System.nanoTime();
            final boolean sketched = barrier.aborted() == false && sketchPage(page);
            sketchNanos += System.nanoTime() - sketchStart;
            if (sketched == false) {
                giveUp();
            } else {
                bloom = pageBloom;
                bloomShift = pageBloomShift;
                // An estimate that does not walk the blocks: this runs once per page and the exact size is not needed for the limit.
                counted = (long) page.getPositionCount() * Long.BYTES * page.getBlockCount() + (long) bloom.length * Long.BYTES;
                if (barrier.tryBuffer(counted) == false) {
                    driverContext.breaker().addWithoutBreaking(-(long) bloom.length * Long.BYTES);
                    counted = 0;
                    bloom = null;
                    giveUp();
                }
            }
        }
        if (bloom != null) {
            pagesBuffered++;
        }
        pages.add(new Buffered(page, counted, bloom, bloomShift));
    }

    /** Count the keys of a page. Returns false if the page is not supported, so this operator must give up. */
    private boolean sketchPage(Page page) {
        LongBlock block = page.getBlock(keyChannel);
        LongVector vector = block.asVector();
        if (vector == null) {
            return false; // nulls or multivalued positions
        }
        final int count = vector.getPositionCount();
        if (keys.length < count) {
            keys = new long[count];
        }
        // Copy to an array first: the loop below then reads a plain array instead of calling a vector of some unknown class per row.
        vector.copyTo(0, keys, 0, count);
        final int shift = PageBloom.shiftFor(count);
        final long[] bloom = PageBloom.create(shift);
        driverContext.breaker().addEstimateBytesAndMaybeBreak((long) bloom.length * Long.BYTES, "sketch_filter");
        sketch.addAll(keys, count, bloom, shift);
        pageBloom = bloom;
        pageBloomShift = shift;
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
            arrivedNanos = System.nanoTime();
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
            driverContext.breaker().addWithoutBreaking(-buffered.bloomBytes());
            Page page = buffered.page;
            if (filter) {
                final long filterStart = System.nanoTime();
                page = filter(buffered);
                filterNanos += System.nanoTime() - filterStart;
                if (page == null) {
                    continue;
                }
            }
            rowsEmitted += page.getPositionCount();
            lastOutputNanos = System.nanoTime();
            return page;
        }
        return null;
    }

    /**
     * Keep only the rows of candidate keys. Returns null if no row is left. Takes ownership of the page. A page whose Bloom filter
     * has none of the candidates is released without being read.
     */
    private Page filter(Buffered buffered) {
        final Page page = buffered.page;
        final LongSet candidates = barrier.candidates();
        final long[] candidateKeys = candidates.values();
        if (buffered.bloom != null && candidateKeys.length <= MAX_BLOOM_CANDIDATES) {
            boolean mayHaveCandidate = false;
            for (long candidate : candidateKeys) {
                if (PageBloom.mayContain(buffered.bloom, buffered.bloomShift, candidate)) {
                    mayHaveCandidate = true;
                    break;
                }
            }
            if (mayHaveCandidate == false) {
                pagesSkipped++;
                page.releaseBlocks();
                return null;
            }
        }
        final LongVector keyVector = ((LongBlock) page.getBlock(keyChannel)).asVector();
        assert keyVector != null : "pages were checked when they were buffered";
        final int count = keyVector.getPositionCount();
        if (positions.length < count) {
            positions = new int[count];
        }
        if (this.keys.length < count) {
            this.keys = new long[count];
        }
        final long[] values = this.keys;
        keyVector.copyTo(0, values, 0, count);
        int kept = 0;
        for (int i = 0; i < count; i++) {
            if (candidates.contains(values[i])) {
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
            driverContext.breaker().addWithoutBreaking(-buffered.bloomBytes());
            buffered.page.releaseBlocks();
        }
        pages.clear();
        sketch = null;
        driverContext.breaker().addWithoutBreaking(-sketchBytes);
    }

    /**
     * The first three fields are read by tests and by the profile scripts, so they keep their place. The rest is timing: times are
     * milliseconds since the first operator of the query was built ({@code startEpochMs} is that moment as wall-clock time),
     * {@code sketchMs} and {@code filterMs} are the time spent counting keys and filtering pages by this operator.
     */
    @Override
    public String toString() {
        return String.format(
            java.util.Locale.ROOT,
            "SketchFilterOperator[rowsReceived=%d, rowsEmitted=%d, passthrough=%s, startEpochMs=%d, firstInputMs=%.1f, arrivedMs=%.1f, "
                + "decideStartMs=%.1f, decideEndMs=%.1f, lastOutputMs=%.1f, sketchMs=%.1f, filterMs=%.1f, pages=%d, pagesSkipped=%d]",
            rowsReceived,
            rowsEmitted,
            passthrough,
            barrier.startEpochMillis(),
            barrier.millisSinceStart(firstInputNanos),
            barrier.millisSinceStart(arrivedNanos),
            barrier.millisSinceStart(barrier.decideStartNanos()),
            barrier.millisSinceStart(barrier.decideEndNanos()),
            barrier.millisSinceStart(lastOutputNanos),
            sketchNanos / 1_000_000.0,
            filterNanos / 1_000_000.0,
            pagesBuffered,
            pagesSkipped
        );
    }
}
