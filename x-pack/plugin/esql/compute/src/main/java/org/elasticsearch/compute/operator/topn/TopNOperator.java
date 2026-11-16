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
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.DocOriginTable;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.BreakingBytesRefBuilder;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.IsBlockedResult;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.compute.spill.DriverSpillOperation;
import org.elasticsearch.compute.spill.QueryScope;
import org.elasticsearch.compute.spill.SpillStatus;
import org.elasticsearch.core.Nullable;
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

    /** Opts the operator into parallel workers; promotion is one-way once {@code promotionThresholdRows} is crossed. */
    public record ParallelWorkerConfig(Executor executor, int workerCount, int maxInFlightPages, long promotionThresholdRows) {}

    /**
     * Configures the cross-driver global top-K merge: a {@link SharedGlobalTopK} shared across all
     * drivers, merged every {@code batchPages} pages or whenever a driver's pending-keys list reaches
     * {@code maxPendingKeys} entries, whichever comes first.
     */
    public record GlobalTopKMergeConfig(SharedGlobalTopK.Supplier globalTopK, int batchPages, int maxPendingKeys) {}

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

    public record TopNOperatorFactory(
        int topCount,
        List<ElementType> elementTypes,
        List<TopNEncoder> encoders,
        List<SortOrder> sortOrders,
        int maxPageRows,
        long jumboPageBytes,
        InputOrdering inputOrdering,
        @Nullable SharedMinCompetitive.Supplier minCompetitive,
        @Nullable GlobalTopKMergeConfig globalTopKMerge,
        @Nullable ParallelWorkerConfig parallelWorkerConfig
    ) implements OperatorFactory {
        /** Backward-compat: no global merge, no parallel workers. */
        public TopNOperatorFactory(
            int topCount,
            List<ElementType> elementTypes,
            List<TopNEncoder> encoders,
            List<SortOrder> sortOrders,
            int maxPageRows,
            long jumboPageBytes,
            InputOrdering inputOrdering,
            @Nullable SharedMinCompetitive.Supplier minCompetitive
        ) {
            this(topCount, elementTypes, encoders, sortOrders, maxPageRows, jumboPageBytes, inputOrdering, minCompetitive, null, null);
        }

        /** Backward-compat: no global merge. */
        public TopNOperatorFactory(
            int topCount,
            List<ElementType> elementTypes,
            List<TopNEncoder> encoders,
            List<SortOrder> sortOrders,
            int maxPageRows,
            long jumboPageBytes,
            InputOrdering inputOrdering,
            @Nullable SharedMinCompetitive.Supplier minCompetitive,
            @Nullable ParallelWorkerConfig parallelWorkerConfig
        ) {
            this(
                topCount,
                elementTypes,
                encoders,
                sortOrders,
                maxPageRows,
                jumboPageBytes,
                inputOrdering,
                minCompetitive,
                null,
                parallelWorkerConfig
            );
        }

        public TopNOperatorFactory {
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
                driverContext.blockFactory().breaker(),
                topCount,
                elementTypes,
                encoders,
                sortOrders,
                maxPageRows,
                jumboPageBytes,
                inputOrdering,
                minCompetitive,
                globalTopKMerge,
                parallelWorkerConfig,
                driverContext,
                null
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

    @Nullable
    private final ParallelWorkerConfig parallelWorkerConfig;

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
    /**
     * The encoders of this operator, with detached {@code _doc} columns bound to {@link #originTable}.
     */
    private final List<TopNEncoder> encoders;
    /**
     * Numbers the origins of detached {@code _doc} rows, {@code null} if no column holds them.
     */
    @Nullable
    private final DocOriginTable originTable;
    private final List<SortOrder> sortOrders;
    private final boolean[] channelInKey;

    /**
     * Tracker for the minimum competitive value. If this is null no one is listening
     * for the min competitive so we don't track it.
     */
    @Nullable
    private final SharedMinCompetitive minCompetitive;

    @Nullable
    private final SharedMinCompetitive.Supplier minCompetitiveSupplier;

    @Nullable
    private final GlobalTopKMergeConfig globalTopKMergeConfig;

    @Nullable
    private SharedGlobalTopK globalTopK;

    /**
     * Sort keys of rows that entered {@link #inputQueue} since the last global merge. Passed as
     * the delta to {@link SharedGlobalTopK#mergeKeys} so that already-contributed rows are never
     * re-added (which would create duplicates in the global heap and publish a bound that is too
     * tight).
     * <p>
     * {@code null} when no {@link GlobalTopKMergeConfig} is active.
     */
    @Nullable
    private final List<BytesRef> pendingGlobalKeys;

    private int globalTopKBatchPages;
    private int globalTopKMaxPendingKeys;

    /**
     * How many times {@link #minCompetitive} was updated.
     */
    private int minCompetitiveUpdates;

    TopNQueue inputQueue;
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

    // ---- spilling: the value run, see TopNValueStore; every field below is the operator's thread's

    /** The driver's context when the operator may spill its values, null otherwise: every path then runs as today. */
    @Nullable
    private final DriverContext driverContext;
    /** The value store, this operator's own or the parallel operator's shared one; null when the query cannot spill. */
    @Nullable
    private final TopNValueStore valueStore;
    private final boolean ownsStore;
    /** This operator's lane of the store; present exactly when {@link #valueStore} is. */
    @Nullable
    private final TopNValueStore.Lane lane;
    /** Whether the values of every row in the heap are in the store: the second regime, entered once and for good. */
    private boolean spilling;
    /** What the heap's rows hold in memory, the policy's own-state size. */
    private long heapBytes;
    /** Bytes of values in the segments that rows still reference, and that evicted rows left behind. */
    private long liveValueBytes;
    private long deadValueBytes;
    private long rowsSpilled;
    /** Whether a compaction failed: the operator then keeps its dead bytes rather than trying again. */
    private boolean compactionRefused;
    /**
     * Rows admitted resident after the switch, while the lane had no writer or too much queued: a worker's lane takes
     * pages without the operator's say, and a page may reach the parallel operator's merge target at any time. They go
     * to the lane at the next page that finds it ready.
     */
    private int residentRows;
    private boolean finished;

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
            null,
            null,
            null
        );
    }

    /**
     * @param driverContext the driver's, for the value store's operations; null keeps the values in memory whatever the
     *                      query's scope
     * @param sharedStore the parallel operator's store, which a worker shares so that its rows keep their pointers
     *                    through the merge; null for a store of this operator's own
     */
    private TopNOperator(
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
        @Nullable GlobalTopKMergeConfig globalTopKMergeConfig,
        @Nullable ParallelWorkerConfig parallelWorkerConfig,
        @Nullable DriverContext driverContext,
        @Nullable TopNValueStore sharedStore
    ) {
        TopNQueue inputQueue = null;
        SharedMinCompetitive minCompetitive = null;
        SharedGlobalTopK globalTopK = null;
        DocOriginTable originTable = null;
        boolean success = false;
        try {
            originTable = DocVectorEncoder.originTableFor(encoders, breaker);
            inputQueue = TopNQueue.build(breaker, topCount);
            minCompetitive = minCompetitiveSupplier == null ? null : minCompetitiveSupplier.get();
            if (globalTopKMergeConfig != null) {
                globalTopK = globalTopKMergeConfig.globalTopK().get();
                globalTopKBatchPages = globalTopKMergeConfig.batchPages();
                globalTopKMaxPendingKeys = globalTopKMergeConfig.maxPendingKeys();
            }
            success = true;
        } finally {
            if (success == false) {
                Releasables.close(inputQueue, minCompetitive, globalTopK, originTable);
            }
        }
        this.originTable = originTable;
        this.encoders = originTable == null ? encoders : DocVectorEncoder.withOrigins(encoders, originTable);
        this.inputQueue = inputQueue;
        this.minCompetitive = minCompetitive;
        this.minCompetitiveSupplier = minCompetitiveSupplier;
        this.globalTopKMergeConfig = globalTopKMergeConfig;
        this.globalTopK = globalTopK;
        this.pendingGlobalKeys = globalTopK != null ? new ArrayList<>() : null;
        this.blockFactory = blockFactory;
        this.breaker = breaker;
        this.maxPageRows = maxPageRows;
        this.jumboPageBytes = jumboPageBytes;
        this.elementTypes = elementTypes;
        this.sortOrders = sortOrders;
        this.inputOrdering = inputOrdering;
        this.channelInKey = new boolean[elementTypes.size()];
        for (SortOrder so : sortOrders) {
            channelInKey[so.channel] = true;
        }
        this.parallelWorkerConfig = parallelWorkerConfig;
        this.driverContext = driverContext;
        QueryScope scope = driverContext == null ? null : driverContext.queryScope();
        if (scope != null && scope.spillEnabled()) {
            this.valueStore = sharedStore != null ? sharedStore : new TopNValueStore(driverContext, scope, "top-n-values");
            this.ownsStore = sharedStore == null;
            this.lane = valueStore.newLane();
        } else {
            this.valueStore = null;
            this.ownsStore = false;
            this.lane = null;
        }
    }

    @Override
    public boolean needsInput() {
        if (output != null || finished) {
            return false;
        }
        if (lane == null) {
            return true;
        }
        beforePage();
        if (lane.compacting()) {
            return false;
        }
        if (spilling == false) {
            // the headroom was asked for and the budget is crossed already: the next page waits for it rather than
            // growing the heap under pressure
            return lane.capable() == false || lane.ready() || driverContext.queryScope().shouldSpill(heapBytes, 0) == false;
        }
        return lane.ready() && lane.appendsBlocked().listener().isDone();
    }

    /**
     * The value store, for tests of what went to it.
     */
    @Nullable
    TopNValueStore valueStore() {
        return valueStore;
    }

    @Override
    public void addInput(Page page) {
        long start = System.nanoTime();
        boolean modified = false;
        /*
         * Since row tracks memory we have to be careful to close any unused rows,
         * including any rows that fail while constructing because they allocate
         * too much memory. The simplest way to manage that is this try/finally
         * block and the mutable row variable here.
         *
         * If you exit the try/finally block and row is non-null it's always unused
         * and must be closed. That happens either because it's overflow from the
         * inputQueue or because we hit an allocation failure while building it.
         */
        try {
            if (inputQueue.topCount <= 0) {
                return;
            }
            if (lane != null) {
                // a worker's lane, or the merge target's through the parallel operator, sees no needsInput before a page
                beforePage();
            }
            RowFiller rowFiller = new RowFiller(elementTypes, encoders, sortOrders, channelInKey, page);

            for (int i = 0; i < page.getPositionCount(); i++) {
                if (spare == null) {
                    spare = new TopNRow(breaker, rowFiller.preAllocatedKeysSize(), rowFiller.preAllocatedValueSize());
                } else {
                    spare.clear();
                    spare.ensureValues(rowFiller.preAllocatedValueSize());
                }
                rowFiller.writeKey(i, spare);

                // This is `inputQueue.insertWithOverflow` followed by filling in the value only if we inserted.
                // We must write values BEFORE modifying the queue so that if writeValues throws (e.g. circuit
                // breaker), spare is not left in both the queue and the spare field (which would double-close).
                if (inputQueue.size() < inputQueue.topCount) {
                    // Heap not yet full, just add elements
                    admit(rowFiller, i, spare);
                    if (pendingGlobalKeys != null) {
                        pendingGlobalKeys.add(BytesRef.deepCopyOf(spare.keys.bytesRefView()));
                        if (pendingGlobalKeys.size() >= globalTopKMaxPendingKeys) {
                            flushPendingGlobalKeys();
                        }
                    }
                    inputQueue.add(spare);
                    inserted(spare);
                    spare = null;
                    modified = true;
                } else if (inputQueue.lessThan(inputQueue.top(), spare)) {
                    // Heap full AND this node fits in it.
                    TopNRow nextSpare = inputQueue.top();
                    admit(rowFiller, i, spare);
                    if (pendingGlobalKeys != null) {
                        pendingGlobalKeys.add(BytesRef.deepCopyOf(spare.keys.bytesRefView()));
                        if (pendingGlobalKeys.size() >= globalTopKMaxPendingKeys) {
                            flushPendingGlobalKeys();
                        }
                    }
                    inputQueue.updateTop(spare);
                    inserted(spare);
                    evicted(nextSpare);
                    spare = nextSpare;
                    modified = true;
                } else if (inputOrdering == InputOrdering.SORTED) {
                    /*
                     The queue (min-heap) is full and we have sorted input for the input page. Any other element that comes after the one
                     we just compared will be greater or equal than any other one in the queue, so we can short circuit the execution here.

                     Note we always need to check whether the min-heap top's is greater or equal than the current element. For example: we
                     could have processed all the data from a first data node, have a full queue (a partial result), but a page from a
                     second data node could interleave with our partial result in arbitrary ways.
                     */
                    break;
                }
            }

            if (modified) {
                updateMinCompetitive();
            }

        } finally {
            page.releaseBlocks();
            pagesReceived++;
            rowsReceived += page.getPositionCount();
            receiveNanos += System.nanoTime() - start;
            maybeMergeGlobalTopK();
        }
    }

    private void maybeMergeGlobalTopK() {
        if (globalTopK == null || globalTopKBatchPages <= 0 || pagesReceived % globalTopKBatchPages != 0) {
            return;
        }
        flushPendingGlobalKeys();
    }

    private void flushPendingGlobalKeys() {
        if (pendingGlobalKeys != null && pendingGlobalKeys.isEmpty() == false) {
            if (globalTopK.mergeKeys(pendingGlobalKeys)) {
                minCompetitiveUpdates++;
            }
            pendingGlobalKeys.clear();
        }
    }

    /**
     * Offer an update to {@link #minCompetitive} if it is non-null.
     * <p>
     * Also detects single-key {@code NULLS FIRST} exhaustion: {@code inputQueue.top()} is the
     * least-competitive row currently kept, so if even it is null then the whole top-K is null.
     * Under {@code NULLS FIRST} nulls are the most competitive rows, so no later non-null row can
     * compete and the source can stop entirely. This is the {@code BYTES_REF} counterpart to
     * {@link NumericTopNOperator}'s {@code markNoFurtherCandidates} on a null-saturated heap.
     */
    private void updateMinCompetitive() {
        if (minCompetitive == null || inputQueue == null || inputQueue.size() < inputQueue.topCount) {
            return;
        }
        BytesRef worstKept = inputQueue.top().keys.bytesRefView();
        // When global merge is active, the competitive bound flows through SharedGlobalTopK rather
        // than directly from each driver. Per-driver publishing is suppressed to avoid conflicting
        // with the global merge path.
        if (globalTopK == null) {
            if (minCompetitive.offer(worstKept)) {
                minCompetitiveUpdates++;
            }
        }
        if (sortOrders.size() == 1) {
            SortOrder order = sortOrders.get(0);
            if (order.nullsFirst() && worstKept.length > 0 && worstKept.bytes[worstKept.offset] == order.nul()) {
                minCompetitive.markNoFurtherCandidates();
            }
        }
    }

    @Override
    public void finish() {
        if (globalTopK != null && pendingGlobalKeys != null && pendingGlobalKeys.isEmpty() == false) {
            if (globalTopK.mergeKeys(pendingGlobalKeys)) {
                minCompetitiveUpdates++;
            }
            pendingGlobalKeys.clear();
        }
        if (finished) {
            return;
        }
        finished = true;
        if (lane != null) {
            // the rows still resident go to the lane when it can take them; then the lane's segment is sealed, and the
            // result built once that and what else is in flight have landed
            beforePage();
            if (spilling && lane.compacting() == false && lane.ready()) {
                spillResident();
            }
            lane.sealForGood();
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
        return output != null && output.hasNext() == false;
    }

    @Override
    public boolean canProduceMoreDataWithoutExtraInput() {
        return (output != null && output.hasNext()) || (finished && output == null && lane != null);
    }

    @Override
    public IsBlockedResult isBlocked() {
        if (lane == null) {
            return NOT_BLOCKED;
        }
        if (output instanceof SpilledResult spilled) {
            return spilled.blocked();
        }
        if (finished) {
            return output == null ? lane.blocked() : NOT_BLOCKED;
        }
        if (lane.compacting() || lane.pending()) {
            return lane.blocked();
        }
        return spilling ? lane.appendsBlocked() : NOT_BLOCKED;
    }

    @Override
    public Page getOutput() {
        if (output == null) {
            if (lane == null || finished == false) {
                return null;
            }
            landCompaction();
            if (lane.compacting() || lane.settled() == false) {
                return null;
            }
            checkLaneFailure();
            long start = System.nanoTime();
            output = buildResult();
            emitNanos += System.nanoTime() - start;
        }
        if (output.hasNext() == false) {
            return null;
        }
        if (output instanceof SpilledResult spilled && spilled.ready() == false) {
            return null;
        }
        Page ret = output.next();
        pagesEmitted++;
        rowsEmitted += ret.getPositionCount();
        return ret;
    }

    // ---- the value run

    /**
     * Fills the row's values: into its builder, or, once values no longer fit, through its builder into the lane's
     * segment, the builder going to the codec and the row keeping a pointer. The switch is decided here, at the row's
     * reservation point: by the policy, when the query would cross its budget with this row, or when the builder's
     * allocation trips with headroom in hand. After the switch a row still stays resident while the lane has no
     * writer, between a seal and the next writer, or has too much queued, and goes to the lane at the next page.
     */
    private void admit(RowFiller rowFiller, int position, TopNRow row) {
        if (lane == null) {
            rowFiller.writeValues(position, row);
            return;
        }
        if (spilling == false) {
            maybeSwitch(rowFiller.preAllocatedValueSize());
        }
        if (spilling == false) {
            try {
                rowFiller.writeValues(position, row);
                return;
            } catch (CircuitBreakingException e) {
                if (lane.failure() != null || lane.capable() == false || lane.ready() == false) {
                    throw e;
                }
                row.values.clear();
                switchToValueRun();
            }
        }
        rowFiller.writeValues(position, row);
        if (lane.ready() && lane.appendsBlocked().listener().isDone() && spillValues(row)) {
            return;
        }
        residentRows++;
    }

    /**
     * The policy at the row's reservation point: headroom once the heap holds the own-state minimum or the query is
     * half way to its budget, the switch once the budget would be crossed, with the headroom in hand.
     */
    private void maybeSwitch(long estimate) {
        if (lane.failure() != null) {
            // the headroom was refused: the values stay in memory and a later trip fails the query, as it does today
            return;
        }
        QueryScope scope = driverContext.queryScope();
        if (lane.capable() == false) {
            if (heapBytes < scope.minSpillBytes() && scope.breaker().used() * 2 < scope.budgetBytes()) {
                return;
            }
            lane.acquire();
        }
        if (lane.ready() && scope.shouldSpill(heapBytes, estimate)) {
            switchToValueRun();
        }
    }

    /**
     * Enters the second regime: the heap's values go to the lane at once, their builders released as the codec is done
     * with them.
     */
    private void switchToValueRun() {
        spilling = true;
        residentRows = inputQueue.size();
        spillResident();
    }

    /**
     * Sends the values of the rows admitted resident to the lane; the heap is walked only while it holds some, and the
     * walk stops at a row the root view refuses, which stays resident for the next page.
     */
    private void spillResident() {
        if (residentRows == 0) {
            return;
        }
        for (TopNRow row : inputQueue) {
            if (row.spilled() == false) {
                long held = row.values.ramBytesUsed();
                if (spillValues(row) == false) {
                    return;
                }
                heapBytes -= held;
                residentRows--;
            }
        }
        assert residentRows == 0 : residentRows;
        residentRows = 0;
    }

    /**
     * The lane's housekeeping before a page, on the appender's thread: what the codec is done with goes, a landed
     * compaction is published, a full segment rolls, a sealed lane gets its next writer, the rows admitted resident go
     * to the lane once it is ready, and a compaction starts once the dead bytes outgrow the live ones.
     */
    private void beforePage() {
        landCompaction();
        if (spilling == false) {
            return;
        }
        checkLaneFailure();
        if (lane.compacting()) {
            return;
        }
        if (lane.needsRoll()) {
            lane.roll();
        } else if (lane.capable() == false) {
            // a compaction sealed the active segment: the next one starts here
            lane.acquire();
        }
        if (lane.ready() && lane.appendsBlocked().listener().isDone()) {
            spillResident();
            if (shouldCompact()) {
                compact();
            }
        }
    }

    /**
     * Hands the row's values to the lane; false, with the row untouched, when the root view refuses to hold them in
     * flight.
     */
    private boolean spillValues(TopNRow row) {
        try {
            lane.append(row, row.values);
        } catch (CircuitBreakingException e) {
            return false;
        }
        liveValueBytes += row.valueLength;
        rowsSpilled++;
        return true;
    }

    private void inserted(TopNRow row) {
        heapBytes += row.ramBytesUsed();
    }

    private void evicted(TopNRow row) {
        heapBytes -= row.ramBytesUsed();
        if (row.spilled()) {
            deadValueBytes += row.valueLength;
            liveValueBytes -= row.valueLength;
        } else if (spilling) {
            residentRows--;
        }
    }

    private boolean shouldCompact() {
        return compactionRefused == false
            && deadValueBytes > liveValueBytes
            && deadValueBytes >= TopNValueStore.CHUNK_BYTES
            && lane.quiescent();
    }

    private void compact() {
        List<TopNRow> live = new ArrayList<>(inputQueue.size());
        for (TopNRow row : inputQueue) {
            if (row.spilled()) {
                live.add(row);
            }
        }
        lane.compact(live);
    }

    private void landCompaction() {
        Boolean landed = lane.finishCompaction();
        if (landed == null) {
            return;
        }
        if (landed) {
            deadValueBytes = 0;
        } else {
            compactionRefused = true;
        }
    }

    private void checkLaneFailure() {
        Exception failure = lane.failure();
        if (failure != null) {
            throw ExceptionsHelper.convertToRuntime(failure);
        }
    }

    /**
     * Seals this worker's lane for the parallel operator's merge; the rows keep their pointers into the store.
     */
    void sealForMerge() {
        if (lane != null) {
            lane.sealForGood();
        }
    }

    /**
     * Whether this worker's lane has nothing in flight, so that the merge may read its rows' values; lands what has.
     * The rows still resident stay so: the merge reads them from their builders.
     */
    boolean laneSettled() {
        if (lane == null) {
            return true;
        }
        landCompaction();
        if (lane.compacting() || lane.settled() == false) {
            return false;
        }
        checkLaneFailure();
        return true;
    }

    IsBlockedResult laneBlocked() {
        return lane == null ? NOT_BLOCKED : lane.blocked();
    }

    @Override
    public Operator tryPromote(DriverContext driverContext) {
        if (parallelWorkerConfig == null) {
            return this;
        }
        if (originTable != null) {
            // Each worker numbers origins in a table of its own, so the merged queue would mix numbers from different tables
            return this;
        }
        if (rowsReceived > parallelWorkerConfig.promotionThresholdRows()) {
            return new ParallelTopNOperator(parallelWorkerConfig, driverContext, this);
        }
        return this;
    }

    TopNOperator spawnWorker(BlockFactory childBlockFactory) {
        return new TopNOperator(
            childBlockFactory,
            childBlockFactory.breaker(),
            inputQueue.topCount,
            elementTypes,
            encoders,
            sortOrders,
            maxPageRows,
            jumboPageBytes,
            inputOrdering,
            minCompetitiveSupplier,
            globalTopKMergeConfig,
            null,
            driverContext,
            valueStore
        );
    }

    @Override
    public void close() {
        Releasables.closeExpectNoException(
            /*
             * The spare is used during most collections. It's cleared when this Operator
             * is finish()ed. So it could be null here.
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
            minCompetitive,
            globalTopK,
            originTable,
            lane,
            ownsStore ? valueStore : null
        );
        // Aggressively null these so they can be GCed more quickly, and so that close() is idempotent.
        spare = null;
        inputQueue = null;
        output = null;
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
        if (originTable != null) {
            size += originTable.ramBytesUsed();
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
            minCompetitiveUpdates,
            rowsSpilled,
            ownsStore ? valueStore.compactions() : 0,
            ownsStore ? valueStore.status() : SpillStatus.NONE
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
        for (TopNRow row : rows) {
            if (row.spilled()) {
                return new SpilledResult(rows);
            }
        }
        return new Result(rows);
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

    private static void readValues(ResultBuilder[] builders, BytesRef values) {
        for (ResultBuilder builder : builders) {
            builder.decodeValue(values);
        }
        if (values.length != 0) {
            throw new IllegalArgumentException("didn't read all values");
        }
    }

    private static long totalSize(ResultBuilder[] builders) {
        long total = 0;
        for (ResultBuilder b : builders) {
            total += b.estimatedBytes();
        }
        return total;
    }

    /**
     * The result of rows whose values are in the store, some or all: each page's values are gathered in physical order
     * before the page is decoded in the rows' order. A gather takes at most a page's rows and, since every value it reads
     * is held until its page is decoded, at most the jumbo bound in value bytes, or its first row alone when that is
     * wider, so the memory held is the keys plus about one page's values. A page the jumbo bound cuts short, the
     * decoded blocks outgrowing the values, leaves its gathered values for the next.
     */
    private class SpilledResult implements ReleasableIterator<Page> {
        private final List<TopNRow> rows;
        private int r;
        @Nullable
        private DriverSpillOperation<ReleasableBytesReference[]> gather;
        /** The gathered values of {@code rows[valuesFrom, valuesFrom + values.length)}, consumed as pages are decoded. */
        @Nullable
        private ReleasableBytesReference[] values;
        private int valuesFrom;

        private SpilledResult(List<TopNRow> rows) {
            this.rows = rows;
        }

        @Override
        public boolean hasNext() {
            return r < rows.size();
        }

        /**
         * Whether the next page's values are at hand; issues their gather otherwise.
         */
        boolean ready() {
            if (values != null) {
                return true;
            }
            if (gather == null) {
                gather = valueStore.gather(rows, r, gatherEnd());
            }
            if (gather.isDone() == false) {
                return false;
            }
            Exception failure = gather.failure();
            if (failure != null) {
                gather = null;
                throw ExceptionsHelper.convertToRuntime(failure);
            }
            values = gather.result();
            valuesFrom = r;
            gather = null;
            return true;
        }

        /**
         * Where the next gather ends: at {@code maxPageRows} rows, or before the row whose values would take the
         * gathered bytes past {@code jumboPageBytes}, since every value gathered is held until its page is decoded; at
         * least one row, however wide.
         */
        private int gatherEnd() {
            int limit = Math.min(rows.size(), r + maxPageRows);
            long bytes = valueBytes(rows.get(r));
            int end = r + 1;
            while (end < limit) {
                long next = valueBytes(rows.get(end));
                if (bytes + next > jumboPageBytes) {
                    break;
                }
                bytes += next;
                end++;
            }
            return end;
        }

        private long valueBytes(TopNRow row) {
            return row.spilled() ? row.valueLength : row.values.length();
        }

        IsBlockedResult blocked() {
            return gather == null || gather.isDone() ? NOT_BLOCKED : gather.blocked();
        }

        @Override
        public Page next() {
            long start = System.nanoTime();
            if (values == null) {
                throw new IllegalStateException("the page's values are not gathered");
            }
            int size = Math.min(values.length - (r - valuesFrom), rows.size() - r);
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
                    int v = r - valuesFrom;
                    try (TopNRow row = rows.set(r++, null); ReleasableBytesReference gathered = values[v]) {
                        values[v] = null;
                        readKeys(builders, row.keys.bytesRefView());
                        readValues(builders, gathered == null ? row.values.bytesRefView() : gathered.toBytesRef());
                    }
                    if (totalSize(builders) > jumboPageBytes) {
                        break;
                    }
                }
                if (r == valuesFrom + values.length) {
                    values = null;
                }
                return new Page(ResultBuilder.buildAll(builders));
            } finally {
                Releasables.close(builders);
                emitNanos += System.nanoTime() - start;
            }
        }

        @Override
        public void close() {
            Releasables.close(rows);
            if (values != null) {
                Releasables.closeExpectNoException(values);
                values = null;
            }
            if (gather != null) {
                DriverSpillOperation<ReleasableBytesReference[]> pending = gather;
                gather = null;
                pending.completion().addListener(ActionListener.wrap(Releasables::closeExpectNoException, e -> {}));
            }
        }
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

        @Override
        public void close() {
            Releasables.close(rows);
        }
    }
}
