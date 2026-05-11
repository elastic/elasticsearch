/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.topn;

import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.BreakingBytesRefBuilder;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.IsBlockedResult;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.compute.operator.WorkerFanOut;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.ReleasableIterator;
import org.elasticsearch.core.Releasables;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Executor;

/**
 * An operator that sorts "rows" of values by encoding the values to sort on, as bytes (using BytesRef). Each data type is encoded
 * in a specific way, defined by methods of a TopNEncoder. All the values used to sort a specific row (think of column/block 3
 * and column/block 6) are converted/encoded in a byte array and the concatenated bytes are all compared in bulk.
 * For now, the only values that have a special "treatment" when it comes to encoding are the text-based ones (text, keyword, ip, version).
 * For each "special" encoding there is should be new TopNEncoder implementation. See {@link TopNEncoder#UTF8} for
 * encoding regular "text" and "keyword" data types. See LocalExecutionPlanner for which data type uses which encoder.
 *
 * This Operator will not be able to sort binary values (encoded as BytesRef) because the bytes used as separator and "null"s can appear
 * as valid bytes inside a binary value.
 */
public class TopNOperator implements Operator, Accountable {
    static final byte SMALL_NULL = 0x01; // "null" representation for "nulls first"
    static final byte BIG_NULL = 0x02; // "null" representation for "nulls last"

    public enum InputOrdering {
        SORTED,
        NOT_SORTED
    }

    /**
     * Fills {@link TopNRow}s from page data. Handles both sort-key encoding and value
     * extraction, and tracks pre-allocation sizes for key and value buffers.
     */
    static final class RowFiller {
        private final ValueExtractor[] valueExtractors;
        private final KeyExtractor[] keyExtractors;

        private int keyPreAllocSize = 0;
        private int valuePreAllocSize = 0;

        RowFiller(
            List<ElementType> elementTypes,
            List<TopNEncoder> encoders,
            List<SortOrder> sortOrders,
            boolean[] channelInKey,
            Page page
        ) {
            valueExtractors = new ValueExtractor[page.getBlockCount()];
            for (int b = 0; b < valueExtractors.length; b++) {
                valueExtractors[b] = ValueExtractor.extractorFor(
                    elementTypes.get(b),
                    encoders.get(b).toUnsortable(),
                    channelInKey[b],
                    page.getBlock(b)
                );
            }
            keyExtractors = new KeyExtractor[sortOrders.size()];
            for (int k = 0; k < keyExtractors.length; k++) {
                SortOrder so = sortOrders.get(k);
                keyExtractors[k] = KeyExtractor.extractorFor(
                    elementTypes.get(so.channel),
                    encoders.get(so.channel),
                    so.asc,
                    so.nul(),
                    so.nonNul(),
                    page.getBlock(so.channel)
                );
            }
        }

        int preAllocatedKeysSize() {
            return keyPreAllocSize;
        }

        int preAllocatedValueSize() {
            return valuePreAllocSize;
        }

        void writeKey(int position, TopNRow row) {
            for (KeyExtractor keyExtractor : keyExtractors) {
                keyExtractor.writeKey(row.keys, position);
            }
            keyPreAllocSize = newPreAllocSize(row.keys, keyPreAllocSize);
        }

        void writeValues(int position, TopNRow destination) {
            for (ValueExtractor e : valueExtractors) {
                var refCounted = e.getRefCountedForShard(position);
                if (refCounted != null) {
                    destination.setShardRefCounted(refCounted);
                }
                e.writeValue(destination.values, position);
            }
            valuePreAllocSize = newPreAllocSize(destination.values, valuePreAllocSize);
        }

        /**
         * Pre-allocation size heuristic: use the larger of the current builder length and half
         * the previous pre-alloc size, so the size decays after a single unusually large row.
         */
        private static int newPreAllocSize(BreakingBytesRefBuilder builder, int sparePreAllocSize) {
            return Math.max(builder.length(), sparePreAllocSize / 2);
        }
    }

    public record SortOrder(int channel, boolean asc, boolean nullsFirst) {

        private static final long SHALLOW_SIZE = RamUsageEstimator.shallowSizeOfInstance(SortOrder.class);

        @Override
        public String toString() {
            return "SortOrder[channel=" + this.channel + ", asc=" + this.asc + ", nullsFirst=" + this.nullsFirst + "]";
        }

        byte nul() {
            return nullsFirst ? SMALL_NULL : BIG_NULL;
        }

        byte nonNul() {
            return nullsFirst ? BIG_NULL : SMALL_NULL;
        }
    }

    /**
     * Number of rows received before sequential mode promotes itself to the
     * parallel final-merge path. Operator-level policy; not threaded through
     * the planner. Tests can override via the {@link #enableParallelFinalMerge}
     * argument.
     */
    public static final long PROMOTION_THRESHOLD_ROWS = 1_000_000L;

    /**
     * Optional config that, when present, opts the operator into the parallel
     * final-merge path. The factory invokes {@link #enableParallelFinalMerge}
     * after constructing the operator; the operator decides whether to actually
     * promote based on {@link #PROMOTION_THRESHOLD_ROWS}.
     */
    public record ParallelFinalMergeConfig(Executor executor, int workerCount, int maxInFlightPages) {}

    public record TopNOperatorFactory(
        int topCount,
        List<ElementType> elementTypes,
        List<TopNEncoder> encoders,
        List<SortOrder> sortOrders,
        int maxPageRows,
        long jumboPageBytes,
        InputOrdering inputOrdering,
        @Nullable SharedMinCompetitive.Supplier minCompetitive,
        @Nullable ParallelFinalMergeConfig parallelFinalMerge
    ) implements OperatorFactory {
        public TopNOperatorFactory

        {
            for (ElementType e : elementTypes) {
                if (e == null) {
                    throw new IllegalArgumentException("ElementType not known");
                }
            }
        }

        @Override
        public TopNOperator get(DriverContext driverContext) {
            TopNOperator op = new TopNOperator(
                driverContext.blockFactory(),
                driverContext.breaker(),
                topCount,
                elementTypes,
                encoders,
                sortOrders,
                maxPageRows,
                jumboPageBytes,
                inputOrdering,
                minCompetitive
            );
            if (parallelFinalMerge != null && parallelFinalMerge.workerCount() >= 2) {
                op.enableParallelFinalMerge(
                    driverContext,
                    parallelFinalMerge.executor(),
                    parallelFinalMerge.workerCount(),
                    parallelFinalMerge.maxInFlightPages(),
                    PROMOTION_THRESHOLD_ROWS
                );
            }
            return op;
        }

        @Override
        public String describe() {
            return "TopNOperator[count="
                + topCount
                + ", elementTypes="
                + elementTypes
                + ", encoders="
                + encoders
                + ", sortOrders="
                + sortOrders
                + ", inputOrdering="
                + inputOrdering
                + "]";
        }
    }

    private final BlockFactory blockFactory;
    private final CircuitBreaker breaker;

    /**
     * Maximum number of rows per output page.
     */
    private final int maxPageRows;
    /**
     * If a page has more than this many bytes we stop after the current row and
     * emit that page. Then start a new page for the next row.
     */
    private final long jumboPageBytes;

    private final List<ElementType> elementTypes;
    private final List<TopNEncoder> encoders;
    private final List<SortOrder> sortOrders;
    private final boolean[] channelInKey;

    /**
     * Tracker for the minimum competitive value. If this is null no one is listening
     * for the min competitive so we don't track it.
     */
    @Nullable
    private final SharedMinCompetitive minCompetitive;
    /**
     * How many times {@link #minCompetitive} was updated.
     */
    private int minCompetitiveUpdates;

    private TopNQueue inputQueue;
    private TopNRow spare;

    private ReleasableIterator<Page> output;

    private long receiveNanos;
    private long emitNanos;

    /**
     * Count of pages that have been received by this operator.
     */
    private int pagesReceived;

    /**
     * Count of pages that have been emitted by this operator.
     */
    private int pagesEmitted;

    /**
     * Count of rows this operator has received.
     */
    private long rowsReceived;

    /**
     * Count of rows this operator has emitted.
     */
    private long rowsEmitted;

    private final InputOrdering inputOrdering;

    /**
     * Captured at construction so the parallel helper can build per-worker
     * {@link TopNQueue}s after promotion (when {@link #inputQueue} is null).
     */
    private final int topCount;

    // ---- Parallel-final-merge configuration (set via enableParallelFinalMerge, optional) ----
    //
    // When configured and the row threshold is exceeded mid-ingest, the operator
    // promotes itself: hands the current inputQueue to worker 0 of `helper` and
    // delegates all further lifecycle calls to the helper. Until promotion, the
    // operator behaves byte-for-byte as today. If `helper` stays null for the
    // entire query, this is the existing single-threaded TopN.
    @Nullable
    private DriverContext driverContext;
    @Nullable
    private Executor executor;
    private int workerCount;
    private int maxInFlightPages;
    private long promotionThresholdRows = Long.MAX_VALUE;
    @Nullable
    private WorkerFanOut<TopNWorkerState> helper;

    public TopNOperator(
        BlockFactory blockFactory,
        CircuitBreaker breaker,
        int topCount,
        List<ElementType> elementTypes,
        List<TopNEncoder> encoders,
        List<SortOrder> sortOrders,
        int maxPageRows,
        long jumboPageBytes,
        InputOrdering inputOrdering,
        @Nullable SharedMinCompetitive.Supplier minCompetitiveSupplier
    ) {
        TopNQueue inputQueue = null;
        SharedMinCompetitive minCompetitive = null;
        boolean success = false;
        try {
            inputQueue = TopNQueue.build(breaker, topCount);
            minCompetitive = minCompetitiveSupplier == null ? null : minCompetitiveSupplier.get();
            success = true;
        } finally {
            if (success == false) {
                Releasables.close(inputQueue, minCompetitive);
            }
        }
        this.inputQueue = inputQueue;
        this.minCompetitive = minCompetitive;
        this.blockFactory = blockFactory;
        this.breaker = breaker;
        this.maxPageRows = maxPageRows;
        this.jumboPageBytes = jumboPageBytes;
        this.elementTypes = elementTypes;
        this.encoders = encoders;
        this.sortOrders = sortOrders;
        this.inputOrdering = inputOrdering;
        this.topCount = topCount;
        this.channelInKey = new boolean[elementTypes.size()];
        for (SortOrder so : sortOrders) {
            channelInKey[so.channel] = true;
        }
    }

    /**
     * Opt the operator into parallel final-merge. Must be called before any
     * {@link #addInput}. While {@link #rowsReceived} is below
     * {@code promotionThresholdRows} the operator behaves identically to the
     * single-threaded version. Once the threshold is exceeded, subsequent pages
     * are dispatched round-robin across {@code workerCount} background tasks on
     * {@code executor}; the pre-promotion {@link TopNQueue} is handed to worker
     * 0 so no rows are lost.
     */
    public TopNOperator enableParallelFinalMerge(
        DriverContext driverContext,
        Executor executor,
        int workerCount,
        int maxInFlightPages,
        long promotionThresholdRows
    ) {
        if (pagesReceived != 0) {
            throw new IllegalStateException("enableParallelFinalMerge must be called before addInput");
        }
        if (workerCount < 2) {
            throw new IllegalArgumentException("parallel final merge requires workerCount >= 2, got " + workerCount);
        }
        this.driverContext = driverContext;
        this.executor = executor;
        this.workerCount = workerCount;
        this.maxInFlightPages = maxInFlightPages;
        this.promotionThresholdRows = promotionThresholdRows;
        return this;
    }

    @Override
    public boolean needsInput() {
        return helper != null ? helper.needsInput() : output == null;
    }

    @Override
    public void addInput(Page page) {
        if (helper != null) {
            pagesReceived++;
            rowsReceived += page.getPositionCount();
            helper.addInput(page);
            return;
        }
        long start = System.nanoTime();
        /*
         * mergePageIntoQueue may allocate a new spare row and then throw mid-page
         * (e.g., circuit breaker during writeKey/writeValues). The SpareRef holder
         * keeps the allocated spare rooted in `this.spare` even on exceptional exit,
         * so close() can release it — analogous to the original code that wrote
         * directly to `this.spare`.
         */
        SpareRef spareRef = new SpareRef(spare);
        spare = null;
        try {
            mergePageIntoQueue(page, inputQueue, spareRef, breaker, inputOrdering, elementTypes, encoders, sortOrders, channelInKey);
            updateMinCompetitive();
        } finally {
            spare = spareRef.value;
            page.releaseBlocks();
            pagesReceived++;
            rowsReceived += page.getPositionCount();
            receiveNanos += System.nanoTime() - start;
        }
        if (executor != null && helper == null && output == null && rowsReceived > promotionThresholdRows) {
            promote();
        }
    }

    /**
     * Holder for the reused scratch {@link TopNRow}. Used so the caller's view of
     * {@code spare} stays in sync with the helper's local state even if the helper
     * throws partway through a page.
     */
    static final class SpareRef {
        TopNRow value;

        SpareRef(TopNRow value) {
            this.value = value;
        }
    }

    /**
     * Encode rows from {@code page} into top-K candidates and offer them to {@code queue}.
     * The current scratch row is held in {@code spareRef}; the helper writes it back on
     * every allocation/consumption so that on exceptional exit the caller still owns
     * any just-allocated row.
     *
     * <p>Called from both the sequential code path (with {@code this.inputQueue} and a
     * holder over {@code this.spare}) and from worker tasks in parallel mode (with
     * per-worker state).
     */
    static void mergePageIntoQueue(
        Page page,
        TopNQueue queue,
        SpareRef spareRef,
        CircuitBreaker breaker,
        InputOrdering inputOrdering,
        List<ElementType> elementTypes,
        List<TopNEncoder> encoders,
        List<SortOrder> sortOrders,
        boolean[] channelInKey
    ) {
        if (queue.topCount <= 0) {
            return;
        }
        RowFiller rowFiller = new RowFiller(elementTypes, encoders, sortOrders, channelInKey, page);
        for (int i = 0; i < page.getPositionCount(); i++) {
            if (spareRef.value == null) {
                spareRef.value = new TopNRow(breaker, rowFiller.preAllocatedKeysSize(), rowFiller.preAllocatedValueSize());
            } else {
                spareRef.value.clear();
            }
            rowFiller.writeKey(i, spareRef.value);
            // Write values BEFORE modifying the queue so that if writeValues throws (e.g. circuit breaker),
            // spare is not left in both the queue and the caller's reference (which would double-close).
            if (queue.size() < queue.topCount) {
                rowFiller.writeValues(i, spareRef.value);
                queue.add(spareRef.value);
                spareRef.value = null;
            } else if (queue.lessThan(queue.top(), spareRef.value)) {
                TopNRow nextSpare = queue.top();
                rowFiller.writeValues(i, spareRef.value);
                queue.updateTop(spareRef.value);
                spareRef.value = nextSpare;
            } else if (inputOrdering == InputOrdering.SORTED) {
                // Heap full and input is sorted globally — remaining rows can't beat the current top.
                // Note: this short-circuit is only meaningful in the sequential path, since parallel
                // dispatch shuffles page order across workers.
                break;
            }
        }
    }

    /**
     * Offer an update to {@link #minCompetitive} if it is non-null.
     */
    private void updateMinCompetitive() {
        if (minCompetitive == null || inputQueue == null || inputQueue.size() < inputQueue.topCount) {
            return;
        }
        if (minCompetitive.offer(inputQueue.top().keys.bytesRefView())) {
            minCompetitiveUpdates++;
        }
    }

    @Override
    public void finish() {
        if (helper != null) {
            helper.finish();
            return;
        }
        if (output == null) {
            long start = System.nanoTime();
            output = buildResult();
            emitNanos += System.nanoTime() - start;
        }
    }

    @Override
    public boolean isFinished() {
        if (helper != null) {
            return helper.isFinished();
        }
        return output != null && output.hasNext() == false;
    }

    @Override
    public boolean canProduceMoreDataWithoutExtraInput() {
        if (helper != null) {
            return helper.canProduceMoreDataWithoutExtraInput();
        }
        return output != null && output.hasNext();
    }

    @Override
    public Page getOutput() {
        Page ret;
        if (helper != null) {
            ret = helper.getOutput();
        } else {
            if (output == null || output.hasNext() == false) {
                return null;
            }
            ret = output.next();
        }
        if (ret != null) {
            pagesEmitted++;
            rowsEmitted += ret.getPositionCount();
        }
        return ret;
    }

    @Override
    public IsBlockedResult isBlocked() {
        return helper != null ? helper.isBlocked() : Operator.NOT_BLOCKED;
    }

    @Override
    public void close() {
        Releasables.closeExpectNoException(
            /*
             * The helper owns the per-worker queues after promotion. Closing it
             * releases all worker states (including the one seeded with the
             * pre-promotion inputQueue) and any built result iterator.
             */
            helper,
            /*
             * The spare is used during most collections. It's cleared when this Operator
             * is finish()ed. So it could be null here. After promotion, spare and
             * inputQueue have been handed to worker 0 (and nulled here).
             */
            spare,
            /*
             * The inputQueue is a min heap of all live rows. Closing it will close all
             * the rows it contains and all decrement the breaker for the size of
             * the heap itself.
             */
            inputQueue,
            /*
             * If we're in the process of outputting pages then output will contain all
             * allocated but un-emitted rows.
             */
            output,
            minCompetitive
        );
        // Aggressively null these so they can be GCed more quickly.
        inputQueue = null;
        output = null;
        helper = null;
    }

    private static final long SHALLOW_SIZE = RamUsageEstimator.shallowSizeOfInstance(TopNOperator.class) + RamUsageEstimator
        .shallowSizeOfInstance(List.class) * 3;

    @Override
    public long ramBytesUsed() {
        // NOTE: this is ignoring the output iterator for now. Pages are not Accountable. Yet.
        long arrHeader = RamUsageEstimator.NUM_BYTES_ARRAY_HEADER;
        long ref = RamUsageEstimator.NUM_BYTES_OBJECT_REF;
        long size = SHALLOW_SIZE;
        // These lists may slightly under-count, but it's not likely to be by much.
        size += RamUsageEstimator.alignObjectSize(arrHeader + ref * elementTypes.size());
        size += RamUsageEstimator.alignObjectSize(arrHeader + ref * encoders.size());
        size += RamUsageEstimator.alignObjectSize(arrHeader + ref * sortOrders.size());
        size += sortOrders.size() * SortOrder.SHALLOW_SIZE;
        if (inputQueue != null) {
            size += inputQueue.ramBytesUsed();
        }
        return size;
    }

    @Override
    public Status status() {
        return new TopNOperatorStatus(
            receiveNanos,
            emitNanos,
            inputQueue != null ? inputQueue.size() : 0,
            ramBytesUsed(),
            pagesReceived,
            pagesEmitted,
            rowsReceived,
            rowsEmitted,
            minCompetitiveUpdates
        );
    }

    @Override
    public String toString() {
        return "TopNOperator[count="
            + inputQueue
            + ", elementTypes="
            + elementTypes
            + ", encoders="
            + encoders
            + ", sortOrders="
            + sortOrders
            + ", inputOrdering="
            + inputOrdering
            + "]";
    }

    /**
     * Promote the operator from sequential to parallel mode. Allocates W per-worker
     * states (with empty {@link TopNQueue}s), then replaces worker 0's state with
     * the pre-promotion {@link #inputQueue} so the rows already accumulated are
     * preserved. After this returns, {@link #helper} is non-null and all subsequent
     * lifecycle calls delegate to it.
     */
    private void promote() {
        final WorkerFanOut<TopNWorkerState> built = new WorkerFanOut<>(driverContext, executor, workerCount, maxInFlightPages) {
            int dispatchCursor = 0;

            @Override
            protected TopNWorkerState createWorkerState(int workerIndex) {
                return new TopNWorkerState(TopNQueue.build(breaker, topCount));
            }

            @Override
            protected void dispatch(Page page, Dispatcher dispatcher) {
                dispatcher.submitTo(dispatchCursor++ % workerCount, page);
            }

            @Override
            protected void processPage(TopNWorkerState state, Page page) {
                mergePageIntoQueue(page, state.queue, state.spareRef, breaker, inputOrdering, elementTypes, encoders, sortOrders, channelInKey);
            }

            @Override
            protected ReleasableIterator<Page> mergeAndBuildResult(List<TopNWorkerState> states) {
                TopNQueue merged = TopNQueue.build(breaker, topCount);
                boolean success = false;
                try {
                    for (TopNWorkerState s : states) {
                        if (s == null) {
                            continue;
                        }
                        List<TopNRow> drained = new ArrayList<>(s.queue.size());
                        s.queue.popAllInto(drained);
                        for (TopNRow row : drained) {
                            TopNRow leftover = merged.addRow(row);
                            if (leftover != null) {
                                leftover.close();
                            }
                        }
                        Releasables.closeExpectNoException(s.spareRef.value, s.queue);
                        s.spareRef.value = null;
                        s.queue = null;
                    }
                    if (merged.size() == 0) {
                        merged.close();
                        success = true;
                        return ReleasableIterator.empty();
                    }
                    List<TopNRow> resultRows = new ArrayList<>(merged.size());
                    merged.popAllInto(resultRows);
                    Collections.reverse(resultRows);
                    merged.close();
                    success = true;
                    return TopNOperator.this.new Result(resultRows);
                } finally {
                    if (success == false) {
                        Releasables.closeExpectNoException(merged);
                    }
                }
            }
        };

        // Hand pre-promotion state to worker 0. The spare is not needed post-promotion
        // (each worker maintains its own); close it now.
        if (spare != null) {
            spare.close();
            spare = null;
        }
        TopNWorkerState worker0 = new TopNWorkerState(inputQueue);
        inputQueue = null;
        built.seedWorkerState(0, worker0);
        helper = built;
    }

    /**
     * Per-worker state in parallel mode: a private {@link TopNQueue} that only the
     * owning worker thread touches (under the slot's lock), plus a reusable spare
     * {@link TopNRow} (held inside a {@link SpareRef}) to avoid re-allocating
     * between pages.
     */
    static final class TopNWorkerState implements Releasable {
        TopNQueue queue;
        final SpareRef spareRef = new SpareRef(null);

        TopNWorkerState(TopNQueue queue) {
            this.queue = queue;
        }

        @Override
        public void close() {
            Releasables.closeExpectNoException(spareRef.value, queue);
            spareRef.value = null;
            queue = null;
        }
    }

    /**
     * Build the result iterator. Moves all rows from the {@link #inputQueue} and
     * {@link #close}s it.
     */
    private ReleasableIterator<Page> buildResult() {
        if (spare != null) {
            // Remove the spare, we're never going to use it again.
            spare.close();
            spare = null;
        }

        if (inputQueue.size() == 0) {
            return ReleasableIterator.empty();
        }

        List<TopNRow> rows = new ArrayList<>(inputQueue.size());
        inputQueue.popAllInto(rows);
        Collections.reverse(rows);
        inputQueue.close();
        inputQueue = null;
        return new Result(rows);
    }

    private class Result implements ReleasableIterator<Page> {
        private final List<TopNRow> rows;
        private int r;

        private Result(List<TopNRow> rows) {
            this.rows = rows;
        }

        @Override
        public boolean hasNext() {
            return r < rows.size();
        }

        @Override
        public Page next() {
            long start = System.nanoTime();
            int size = Math.min(maxPageRows, rows.size() - r);
            if (size <= 0) {
                throw new IllegalStateException("can't make empty pages. " + size + " must be > 0");
            }
            ResultBuilder[] builders = new ResultBuilder[elementTypes.size()];
            try {
                for (int b = 0; b < builders.length; b++) {
                    builders[b] = ResultBuilder.resultBuilderFor(blockFactory, elementTypes.get(b), encoders.get(b), channelInKey[b], size);
                }
                int rEnd = r + size;
                while (r < rEnd) {
                    try (TopNRow row = rows.set(r++, null)) {
                        readKeys(builders, row.keys.bytesRefView());
                        readValues(builders, row.values.bytesRefView());
                    }
                    if (totalSize(builders) > jumboPageBytes) {
                        break;
                    }
                }
                return new Page(ResultBuilder.buildAll(builders));
            } finally {
                Releasables.close(builders);
                emitNanos += System.nanoTime() - start;
            }
        }

        private long totalSize(ResultBuilder[] builders) {
            long total = 0;
            for (ResultBuilder b : builders) {
                total += b.estimatedBytes();
            }
            return total;
        }

        @Override
        public void close() {
            Releasables.close(rows);
        }

        /**
         * Read keys into the results. See {@link KeyExtractor} for the key layout.
         */
        private void readKeys(ResultBuilder[] builders, BytesRef keys) {
            for (SortOrder so : sortOrders) {
                if (keys.bytes[keys.offset] == so.nul()) {
                    // Discard the null byte.
                    keys.offset++;
                    keys.length--;
                    continue;
                }
                // Discard the non_null byte.
                keys.offset++;
                keys.length--;
                // Read the key. This will modify offset and length for the next iteration.
                builders[so.channel].decodeKey(keys, so.asc);
            }
            if (keys.length != 0) {
                throw new IllegalArgumentException("didn't read all keys");
            }
        }

        private void readValues(ResultBuilder[] builders, BytesRef values) {
            for (ResultBuilder builder : builders) {
                builder.decodeValue(values);
            }
            if (values.length != 0) {
                throw new IllegalArgumentException("didn't read all values");
            }
        }
    }
}
