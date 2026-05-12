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
import org.elasticsearch.common.util.FeatureFlag;
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
     * Gates the parallel final-merge path. Auto-enabled in snapshot builds; release
     * builds require {@code -Des.parallel_topn_feature_flag_enabled=true}. When disabled,
     * the planner refuses to attach a {@link ParallelWorkerConfig}, and the operator
     * stays sequential regardless of input size.
     */
    public static final FeatureFlag PARALLEL_TOPN_FEATURE_FLAG = new FeatureFlag("parallel_topn");

    /** Default row count at which sequential mode promotes itself to parallel final-merge. */
    public static final long DEFAULT_PROMOTION_THRESHOLD_ROWS = 1_000_000L;

    /**
     * When passed to the constructor, opts the operator into parallel final-merge.
     * Promotion happens one-way once row count exceeds {@code promotionThresholdRows}.
     */
    public record ParallelWorkerConfig(Executor executor, int workerCount, int maxInFlightPages, long promotionThresholdRows) {}

    public record TopNOperatorFactory(
        int topCount,
        List<ElementType> elementTypes,
        List<TopNEncoder> encoders,
        List<SortOrder> sortOrders,
        int maxPageRows,
        long jumboPageBytes,
        InputOrdering inputOrdering,
        @Nullable SharedMinCompetitive.Supplier minCompetitive,
        @Nullable ParallelWorkerConfig workerConfig
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
            return new TopNOperator(
                driverContext.blockFactory(),
                driverContext.breaker(),
                topCount,
                elementTypes,
                encoders,
                sortOrders,
                maxPageRows,
                jumboPageBytes,
                inputOrdering,
                minCompetitive,
                driverContext,
                workerConfig
            );
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

    /**
     * Sequential-mode state: queue + reusable scratch row. Null after promotion
     * (its content was handed to worker 0 of {@link #workers}).
     */
    private TopNWorkerState sequentialState;

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

    /** Saved here so per-worker {@link TopNQueue}s can be built after promotion. */
    private final int topCount;

    // Parallel-final-merge: when workerConfig != null and rowsReceived crosses
    // PROMOTION_THRESHOLD_ROWS, sequentialState is handed to worker 0 of `workers`
    // and lifecycle calls delegate from then on. Null = sequential-only.
    @Nullable
    private final DriverContext driverContext;
    @Nullable
    private final ParallelWorkerConfig workerConfig;
    @Nullable
    private WorkerFanOut<TopNWorkerState> workers;

    /** Sequential-only convenience constructor. */
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
        this(
            blockFactory,
            breaker,
            topCount,
            elementTypes,
            encoders,
            sortOrders,
            maxPageRows,
            jumboPageBytes,
            inputOrdering,
            minCompetitiveSupplier,
            null,
            null
        );
    }

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
        @Nullable SharedMinCompetitive.Supplier minCompetitiveSupplier,
        @Nullable DriverContext driverContext,
        @Nullable ParallelWorkerConfig workerConfig
    ) {
        if (workerConfig != null) {
            if (driverContext == null) {
                throw new IllegalArgumentException("workerConfig requires a non-null DriverContext");
            }
            if (workerConfig.workerCount() < 2) {
                throw new IllegalArgumentException("workerConfig requires workerCount >= 2, got " + workerConfig.workerCount());
            }
        }
        TopNQueue initialQueue = null;
        SharedMinCompetitive minCompetitive = null;
        boolean success = false;
        try {
            initialQueue = TopNQueue.build(breaker, topCount);
            minCompetitive = minCompetitiveSupplier == null ? null : minCompetitiveSupplier.get();
            success = true;
        } finally {
            if (success == false) {
                Releasables.close(initialQueue, minCompetitive);
            }
        }
        this.sequentialState = new TopNWorkerState(initialQueue);
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
        // Drop the parallel-mode wiring if we can never promote: SORTED input is already
        // O(K) sequential, and a non-null minCompetitive means we publish a skip bound
        // upstream that promotion would freeze (see shouldPromoteToParallel()).
        boolean canPromote = workerConfig != null && inputOrdering != InputOrdering.SORTED && minCompetitive == null;
        this.driverContext = canPromote ? driverContext : null;
        this.workerConfig = canPromote ? workerConfig : null;
    }

    @Override
    public boolean needsInput() {
        return workers != null ? workers.needsInput() : output == null;
    }

    @Override
    public void addInput(Page page) {
        if (workers != null) {
            pagesReceived++;
            rowsReceived += page.getPositionCount();
            workers.addInput(page);
            return;
        }
        long start = System.nanoTime();
        try {
            mergePageIntoQueue(page, sequentialState, breaker, inputOrdering, elementTypes, encoders, sortOrders, channelInKey);
            updateMinCompetitive();
        } finally {
            page.releaseBlocks();
            pagesReceived++;
            rowsReceived += page.getPositionCount();
            receiveNanos += System.nanoTime() - start;
        }
        if (shouldPromoteToParallel()) {
            promoteToParallel();
        }
    }

    /**
     * Whether to promote sequential → parallel on the next page. {@code workerConfig}
     * is already null when SORTED input or a non-null minCompetitive ruled out
     * promotion at construction (see the constructor).
     *
     * <p>TODO: workers could offer their local heap-tops to {@code minCompetitive},
     * producing {@code max(worker.tops)} as a (looser but correct) parallel skip
     * bound. That'd let promotion fire under non-null minCompetitive as well.
     */
    private boolean shouldPromoteToParallel() {
        return workerConfig != null && workers == null && output == null && rowsReceived > workerConfig.promotionThresholdRows();
    }

    /**
     * Encode rows from {@code page} into top-K candidates and offer them to {@code state.queue}.
     * Called from both the sequential and parallel paths; the latter passes the per-worker state directly.
     */
    static void mergePageIntoQueue(
        Page page,
        TopNWorkerState state,
        CircuitBreaker breaker,
        InputOrdering inputOrdering,
        List<ElementType> elementTypes,
        List<TopNEncoder> encoders,
        List<SortOrder> sortOrders,
        boolean[] channelInKey
    ) {
        if (state.queue.topCount <= 0) {
            return;
        }
        RowFiller rowFiller = new RowFiller(elementTypes, encoders, sortOrders, channelInKey, page);
        for (int i = 0; i < page.getPositionCount(); i++) {
            if (state.spare == null) {
                state.spare = new TopNRow(breaker, rowFiller.preAllocatedKeysSize(), rowFiller.preAllocatedValueSize());
            } else {
                state.spare.clear();
            }
            rowFiller.writeKey(i, state.spare);
            // Write values before mutating the queue: if writeValues throws (e.g. circuit breaker),
            // we'd otherwise risk having spare in both the queue and state.spare (double-close).
            if (state.queue.size() < state.queue.topCount) {
                rowFiller.writeValues(i, state.spare);
                state.queue.add(state.spare);
                state.spare = null;
            } else if (state.queue.lessThan(state.queue.top(), state.spare)) {
                TopNRow nextSpare = state.queue.top();
                rowFiller.writeValues(i, state.spare);
                state.queue.updateTop(state.spare);
                state.spare = nextSpare;
            } else if (inputOrdering == InputOrdering.SORTED) {
                // Heap full, input globally sorted — remaining rows can't beat top. Only meaningful
                // in sequential mode; parallel dispatch shuffles page order across workers.
                break;
            }
        }
    }

    /**
     * Offer an update to {@link #minCompetitive} if it is non-null.
     */
    private void updateMinCompetitive() {
        if (minCompetitive == null || sequentialState == null) {
            return;
        }
        TopNQueue queue = sequentialState.queue;
        if (queue.size() < queue.topCount) {
            return;
        }
        if (minCompetitive.offer(queue.top().keys.bytesRefView())) {
            minCompetitiveUpdates++;
        }
    }

    @Override
    public void finish() {
        if (workers != null) {
            workers.finish();
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
        if (workers != null) {
            return workers.isFinished();
        }
        return output != null && output.hasNext() == false;
    }

    @Override
    public boolean canProduceMoreDataWithoutExtraInput() {
        if (workers != null) {
            return workers.canProduceMoreDataWithoutExtraInput();
        }
        return output != null && output.hasNext();
    }

    @Override
    public Page getOutput() {
        Page ret;
        if (workers != null) {
            ret = workers.getOutput();
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
        return workers != null ? workers.isBlocked() : Operator.NOT_BLOCKED;
    }

    @Override
    public void close() {
        Releasables.closeExpectNoException(
            // After promotion the fan-out owns the per-worker queues plus any built result iterator.
            workers,
            // Sequential queue + scratch row. Null after promotion (handed to worker 0).
            sequentialState,
            // Any allocated but un-emitted output rows.
            output,
            minCompetitive
        );
        sequentialState = null;
        output = null;
        workers = null;
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
        if (sequentialState != null) {
            size += sequentialState.ramBytesUsed();
        }
        if (workers != null) {
            size += workers.ramBytesUsed();
        }
        return size;
    }

    @Override
    public Status status() {
        return new TopNOperatorStatus(
            receiveNanos,
            emitNanos,
            sequentialState != null ? sequentialState.queue.size() : 0,
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
            + (sequentialState != null ? sequentialState.queue : "(promoted)")
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
     * Promote sequential → parallel. Worker 0 inherits the pre-promotion
     * {@link #sequentialState} so no rows are lost. From here on, lifecycle
     * calls delegate to {@link #workers}.
     */
    private void promoteToParallel() {
        assert workerConfig != null && driverContext != null : "promoteToParallel() called without parallel config";
        final int workerCount = workerConfig.workerCount();
        workers = new WorkerFanOut<>(driverContext, workerConfig.executor(), workerCount, workerConfig.maxInFlightPages()) {
            int dispatchCursor = 0;

            @Override
            protected TopNWorkerState createWorkerState(int workerIndex) {
                if (workerIndex == 0) {
                    // Worker 0 inherits the sequential state directly.
                    TopNWorkerState seeded = sequentialState;
                    sequentialState = null;
                    return seeded;
                }
                return new TopNWorkerState(TopNQueue.build(breaker, topCount));
            }

            @Override
            protected int chooseWorker(Page page) {
                return dispatchCursor++ % workerCount;
            }

            @Override
            protected void processPage(TopNWorkerState state, Page page) {
                mergePageIntoQueue(page, state, breaker, inputOrdering, elementTypes, encoders, sortOrders, channelInKey);
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
                        Releasables.closeExpectNoException(s.spare, s.queue);
                        s.spare = null;
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
    }

    /** Per-worker state: a private {@link TopNQueue} plus a reusable scratch row. */
    static final class TopNWorkerState implements Releasable, Accountable {
        TopNQueue queue;
        TopNRow spare;

        TopNWorkerState(TopNQueue queue) {
            this.queue = queue;
        }

        @Override
        public long ramBytesUsed() {
            return (queue == null ? 0 : queue.ramBytesUsed()) + (spare == null ? 0 : spare.ramBytesUsed());
        }

        @Override
        public void close() {
            Releasables.closeExpectNoException(spare, queue);
            spare = null;
            queue = null;
        }
    }

    /**
     * Build the result iterator. Drains the sequential state's queue and closes it.
     */
    private ReleasableIterator<Page> buildResult() {
        TopNWorkerState s = sequentialState;
        sequentialState = null;
        if (s.spare != null) {
            s.spare.close();
            s.spare = null;
        }
        if (s.queue.size() == 0) {
            s.queue.close();
            return ReleasableIterator.empty();
        }
        List<TopNRow> rows = new ArrayList<>(s.queue.size());
        s.queue.popAllInto(rows);
        Collections.reverse(rows);
        s.queue.close();
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
