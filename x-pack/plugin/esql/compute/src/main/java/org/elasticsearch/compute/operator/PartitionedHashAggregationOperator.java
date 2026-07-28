/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.compute.aggregation.AggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.AggregatorMode;
import org.elasticsearch.compute.aggregation.GroupingAggregator;
import org.elasticsearch.compute.aggregation.GroupingAggregatorFunction;
import org.elasticsearch.compute.aggregation.blockhash.BlockHash;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.IntArrayBlock;
import org.elasticsearch.compute.data.IntBigArrayBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.ReleasableIterator;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.NoSuchElementException;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;

/**
 * Aggregates raw input {@link Page}s into many output rows, grouping by one or more columns,
 * by routing rows across {@code N} independent, otherwise-completely-unmodified
 * {@code (BlockHash, List<GroupingAggregator>)} pairs ("partitions") rather than one big shared
 * table.
 * <p>
 *     Starts as a single ordinary table (the "legacy" table) and converts, non-destructively,
 *     into {@code N} partitions once that table's key count crosses
 *     {@code partitionConversionThreshold}. Steady-state routing uses a two-phase bucket-sort:
 *     a routing-only {@link BlockHash} ({@code probeHash}) computes a partition ID per row via
 *     {@link BlockHash.Router#partitionHashOfRow}; a single scatter pass then places positions
 *     into contiguous per-partition ranges; and for each non-empty partition a filtered sub-page
 *     (a physical copy of just that partition's rows) is fed to the partition's own table via
 *     {@code blockHash.add()} — typically the faster {@code LongIntAdaptiveBlockHash}, which does
 *     not need to expose a router itself.
 * </p>
 * <p>
 *     Only supports {@link AggregatorMode#INITIAL} (raw input, partial output): partitioning is a
 *     data-node-local optimization over raw rows, not something the coordinator-side merge (a
 *     later phase) needs to know about.
 * </p>
 * <p>
 *     Multi-valued grouping keys break the one-row-one-partition assumption bucket-sort routing
 *     depends on. Before conversion, MV keys are handled natively by the single legacy table.
 *     After conversion, any page that contains at least one MV key is routed in its entirety to a
 *     lazily-created overflow table ({@code legacyOp}), tagged as {@link #NONE_PARTITION} on emit
 *     so the coordinator's {@code noneOp} can reconcile it — non-MV pages continue to use the
 *     partition tables as normal.
 * </p>
 */
public class PartitionedHashAggregationOperator extends AbstractPartitionedHashAggregationOperator {

    public static final int DEFAULT_PARTITION_COUNT = 8;
    public static final int DEFAULT_PARTITION_CONVERSION_THRESHOLD = 100_000;
    public static final int DEFAULT_PER_PARTITION_EMIT_THRESHOLD = 50_000;

    /**
     * Returns true if the given group specs can be routed across partitions at runtime.
     * <p>
     * Single-column schemas are always routable: every single-column {@link BlockHash}
     * implementation (Long, Int, Double, BytesRef, …) exposes a {@link BlockHash.Router}.
     * Multi-column schemas use {@code PackedValuesBlockHash}, which supports routing for any
     * combination of {@code BOOLEAN}, {@code INT}, {@code LONG}, {@code DOUBLE}, and
     * {@code BYTES_REF} keys. Other element types (e.g. {@code NULL}) cause
     * {@code PackedValuesBlockHash.router()} to return {@code null} at runtime, and the operator
     * falls back to single-table mode via {@code permanentlyUnpartitioned}.
     * </p>
     */
    public static boolean canPartition(List<BlockHash.GroupSpec> groupSpecs) {
        if (groupSpecs.size() <= 1) {
            return true;
        }
        return groupSpecs.stream().allMatch(gs -> switch (gs.elementType()) {
            case BOOLEAN, INT, LONG, DOUBLE, BYTES_REF -> true;
            default -> false;
        });
    }

    /** Partition value for pages emitted from the legacy (pre-conversion) table; no partition tag is applied. */
    public static final int NONE_PARTITION = -1;

    /**
     * An aggregator plus the raw-input channel(s) it reads its value(s) from. Unlike
     * {@link HashAggregationOperator}, this operator can't accept pre-bound
     * {@link GroupingAggregator.Factory} instances: each aggregator's channels must be wide
     * enough to also read back its own intermediate state during the internal
     * evaluate-intermediate/{@code addIntermediateInput} merges conversion and the
     * multi-valued-key fallback rely on (see {@link #toInternalLayout}), so this operator builds
     * the actual channel list itself.
     */
    public record AggregatorSpec(AggregatorFunctionSupplier supplier, List<Integer> rawChannels) {}

    public static class Builder {
        private List<BlockHash.GroupSpec> groupSpecs;
        private List<AggregatorSpec> aggregators;
        private int partitionCount = DEFAULT_PARTITION_COUNT;
        private int partitionConversionThreshold = DEFAULT_PARTITION_CONVERSION_THRESHOLD;
        private int perPartitionEmitThreshold = DEFAULT_PER_PARTITION_EMIT_THRESHOLD;
        private int maxPageSize = Operator.TARGET_PAGE_SIZE / Long.SIZE;
        private int aggregationBatchSize = Operator.TARGET_PAGE_SIZE / Long.SIZE;

        public Builder groupSpecs(List<BlockHash.GroupSpec> groupSpecs) {
            this.groupSpecs = List.copyOf(groupSpecs);
            return this;
        }

        public Builder aggregators(List<AggregatorSpec> aggregators) {
            this.aggregators = aggregators;
            return this;
        }

        public Builder partitionCount(int partitionCount) {
            this.partitionCount = partitionCount;
            return this;
        }

        public Builder partitionConversionThreshold(int partitionConversionThreshold) {
            this.partitionConversionThreshold = partitionConversionThreshold;
            return this;
        }

        public Builder perPartitionEmitThreshold(int keysThreshold) {
            this.perPartitionEmitThreshold = keysThreshold;
            return this;
        }

        public Builder maxPageSize(int maxPageSize) {
            this.maxPageSize = maxPageSize;
            return this;
        }

        public Builder aggregationBatchSize(int aggregationBatchSize) {
            this.aggregationBatchSize = aggregationBatchSize;
            return this;
        }

        public Factory build() {
            return new Factory(this);
        }
    }

    public static class Factory implements OperatorFactory {
        private final List<Integer> groupChannels;
        private final List<BlockHash.GroupSpec> internalGroupSpecs;
        private final List<AggregatorSpec> aggregatorSpecs;
        private final List<GroupingAggregator.Factory> aggregatorFactories;
        private final List<List<Integer>> aggregatorRawChannels;
        private final int[] combinedChannelStart;
        private final int internalPageWidth;
        private final int partitionCount;
        private final int partitionConversionThreshold;
        private final int perPartitionEmitThreshold;
        private final int maxPageSize;
        private final int aggregationBatchSize;

        private Factory(Builder builder) {
            var mapping = AbstractPartitionedHashAggregationOperator.buildGroupChannelMapping(builder.groupSpecs);
            this.groupChannels = mapping.groupChannels();
            this.internalGroupSpecs = mapping.internalGroupSpecs();
            this.aggregatorSpecs = requireNonNull(builder.aggregators, "aggregators");

            List<GroupingAggregator.Factory> factories = new ArrayList<>(aggregatorSpecs.size());
            List<List<Integer>> rawChannelsList = new ArrayList<>(aggregatorSpecs.size());
            int[] combinedStart = new int[aggregatorSpecs.size()];
            int nextChannel = groupChannels.size(); // channels 0..groupChannels.size()-1 reserved for grouping keys
            for (int i = 0; i < aggregatorSpecs.size(); i++) {
                AggregatorSpec spec = aggregatorSpecs.get(i);
                AggregatorFunctionSupplier supplier = spec.supplier();
                int intermediateBlockCount = supplier.groupingIntermediateStateDesc().size();
                if (spec.rawChannels().size() > intermediateBlockCount) {
                    throw new IllegalArgumentException(
                        "aggregator ["
                            + supplier.describe()
                            + "] needs "
                            + spec.rawChannels().size()
                            + " raw channel(s) but only has "
                            + intermediateBlockCount
                            + " intermediate state field(s); PartitionedHashAggregationOperator requires "
                            + "intermediateBlockCount >= raw channel count so one channel list can serve both"
                    );
                }
                combinedStart[i] = nextChannel;
                List<Integer> combinedChannels = new ArrayList<>(intermediateBlockCount);
                for (int c = 0; c < intermediateBlockCount; c++) {
                    combinedChannels.add(nextChannel + c);
                }
                factories.add(new GroupingAggregator.Factory() {
                    @Override
                    public GroupingAggregator apply(DriverContext driverContext) {
                        return new GroupingAggregator(supplier.groupingAggregator(driverContext, combinedChannels), AggregatorMode.INITIAL);
                    }

                    @Override
                    public String describe() {
                        return supplier.describe();
                    }
                });
                rawChannelsList.add(spec.rawChannels());
                nextChannel += intermediateBlockCount;
            }
            this.aggregatorFactories = List.copyOf(factories);
            this.aggregatorRawChannels = List.copyOf(rawChannelsList);
            this.combinedChannelStart = combinedStart;
            this.internalPageWidth = nextChannel;

            this.partitionCount = builder.partitionCount;
            this.partitionConversionThreshold = builder.partitionConversionThreshold;
            this.perPartitionEmitThreshold = builder.perPartitionEmitThreshold;
            this.maxPageSize = builder.maxPageSize;
            this.aggregationBatchSize = builder.aggregationBatchSize;
        }

        @Override
        public PartitionedHashAggregationOperator get(DriverContext driverContext) {
            return new PartitionedHashAggregationOperator(
                groupChannels,
                internalGroupSpecs,
                aggregatorFactories,
                aggregatorRawChannels,
                combinedChannelStart,
                internalPageWidth,
                partitionCount,
                partitionConversionThreshold,
                perPartitionEmitThreshold,
                maxPageSize,
                aggregationBatchSize,
                driverContext
            );
        }

        @Override
        public String describe() {
            return "PartitionedHashAggregationOperator[partitionCount = "
                + partitionCount
                + ", aggs = "
                + aggregatorSpecs.stream().map(s -> s.supplier().describe()).collect(joining(", "))
                + "]";
        }
    }

    private final List<GroupingAggregator.Factory> aggregatorFactories;
    private final int partitionConversionThreshold;
    private final int perPartitionEmitThreshold;
    private final int aggregationBatchSize;

    /** Non-null until {@link #convertToPartitioned} (or a multi-valued key) replaces it. */
    private HashAggregationOperator legacyOp;

    private long routingNanos;
    /** Subset of {@link #routingNanos} spent during the one-time conversion in {@link #convertToPartitioned}. */
    private long conversionNanos;
    /** Hash + aggregation nanos from ops that have already been closed. */
    private long savedHashNanos;
    private long savedAggNanos;
    private int pagesProcessed;
    private long rowsReceived;
    private long rowsEmitted;
    /** Non-null once converted from the legacy table. */
    private HashAggregationOperator[] partitionOps;
    /** Set once a multi-valued key is observed; conversion is never attempted again after that. */
    private boolean permanentlyUnpartitioned;
    /**
     * Set at the start of {@link #emitFinal}: true if this operator converted to partitioned mode
     * and wasn't subsequently reverted. Used by {@link #toString()} and {@link #status()} after
     * ops are nulled out.
     */
    private boolean emittedFromPartitioned;

    @SuppressWarnings("this-escape")
    PartitionedHashAggregationOperator(
        List<Integer> groupChannels,
        List<BlockHash.GroupSpec> internalGroupSpecs,
        List<GroupingAggregator.Factory> aggregatorFactories,
        List<List<Integer>> aggregatorRawChannels,
        int[] combinedChannelStart,
        int internalPageWidth,
        int partitionCount,
        int partitionConversionThreshold,
        int perPartitionEmitThreshold,
        int maxPageSize,
        int aggregationBatchSize,
        DriverContext driverContext
    ) {
        super(
            groupChannels,
            internalGroupSpecs,
            aggregatorRawChannels,
            combinedChannelStart,
            internalPageWidth,
            partitionCount,
            maxPageSize,
            driverContext
        );
        if (partitionCount <= 0) {
            throw new IllegalArgumentException("partitionCount must be greater than 0; got " + partitionCount);
        }
        if (partitionConversionThreshold <= 0) {
            throw new IllegalArgumentException("partitionConversionThreshold must be greater than 0; got " + partitionConversionThreshold);
        }
        this.aggregatorFactories = aggregatorFactories;
        this.partitionConversionThreshold = partitionConversionThreshold;
        this.perPartitionEmitThreshold = perPartitionEmitThreshold;
        this.aggregationBatchSize = aggregationBatchSize;
        boolean success = false;
        try {
            this.legacyOp = newOp();
            success = true;
        } finally {
            if (success == false) {
                close();
            }
        }
    }

    @Override
    public void addInput(Page page) {
        try {
            checkState(needsInput(), "Operator is already finishing");
            requireNonNull(page, "page is null");
            Page internal = toInternalLayout(page);
            if (partitionOps == null) {
                legacyOp.processPage(internal);
                if (permanentlyUnpartitioned == false && legacyOp.blockHash.numKeys() >= partitionConversionThreshold) {
                    if (hasMultiValuedKeys(page)) {
                        permanentlyUnpartitioned = true;
                    } else {
                        convertToPartitioned();
                    }
                }
            } else if (hasMultiValuedKeys(page)) {
                if (legacyOp == null) {
                    legacyOp = newOp();
                }
                legacyOp.processPage(internal);
            } else {
                addToPartitions(internal);
                maybeEmitPartitions();
            }
        } finally {
            page.releaseBlocks();
            pagesProcessed++;
            rowsReceived += page.getPositionCount();
        }
    }

    @Override
    public Page getOutput() {
        if (output == null) {
            return null;
        }
        if (output.hasNext() == false) {
            output.close();
            output = null;
            return null;
        }
        Page page = output.next();
        rowsEmitted += page.getPositionCount();
        return page;
    }

    @Override
    public void finish() {
        if (finishCalled) {
            return;
        }
        finishCalled = true;
        emitFinal();
    }

    @Override
    public boolean isFinished() {
        return finishCalled && output == null;
    }

    @Override
    public boolean canProduceMoreDataWithoutExtraInput() {
        return output != null;
    }

    @Override
    public void close() {
        Releasables.close(legacyOp, probeHash, output);
        if (partitionOps != null) {
            Releasables.close(partitionOps);
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(getClass().getSimpleName()).append("[");
        if (partitionOps != null) {
            sb.append("partitionCount=").append(partitionCount);
            if (legacyOp != null) {
                sb.append(", mvOverflow=").append(legacyOp.blockHash);
            }
        } else if (legacyOp != null) {
            sb.append("legacy=").append(legacyOp.blockHash);
        } else if (permanentlyUnpartitioned) {
            sb.append("unpartitioned");
        } else if (emittedFromPartitioned) {
            sb.append("partitioned");
        } else {
            sb.append("legacy");
        }
        sb.append("]");
        return sb.toString();
    }

    // ---- legacy (pre-conversion) processing ----

    /**
     * Converts the legacy single table into {@link #partitionCount} independent partitions,
     * non-destructively: evaluates the legacy table to an intermediate page, splits that page by
     * partition, and folds each split into its partition via the existing
     * evaluate-intermediate/{@code addIntermediateInput} merge primitive - no new merge code, and
     * no group ids are lost (unlike the destructive periodic-early-emit reset).
     */
    private void convertToPartitioned() {
        int cappedBatchSize = Math.min(aggregationBatchSize, Operator.TARGET_PAGE_SIZE);
        // Try the specialized hash first: BytesRefBlockHash, for example, exposes a router for
        // single-column BYTES_REF keys. If it has no router (e.g. LongIntAdaptiveBlockHash for
        // multi-column fixed-width schemas), fall back to PackedValuesBlockHash.
        BlockHash routingHash = BlockHash.build(internalGroupSpecs, driverContext.blockFactory(), cappedBatchSize, false);
        if (routingHash.router() == null) {
            routingHash.close();
            routingHash = BlockHash.buildPackedValuesBlockHash(internalGroupSpecs, driverContext.blockFactory(), cappedBatchSize);
        }
        if (routingHash.router() == null) {
            routingHash.close();
            permanentlyUnpartitioned = true;
            return;
        }
        probeHash = routingHash;
        HashAggregationOperator[] newPartitions = new HashAggregationOperator[partitionCount];
        for (int p = 0; p < partitionCount; p++) {
            newPartitions[p] = newOp();
        }
        long routingBefore = routingNanos;
        try (ReleasableIterator<Page> intermediatePages = evaluateOp(legacyOp, Integer.MAX_VALUE)) {
            while (intermediatePages.hasNext()) {
                try (Page intermediatePage = intermediatePages.next()) {
                    distributeIntermediatePage(intermediatePage, newPartitions);
                }
            }
        }
        conversionNanos = routingNanos - routingBefore;
        saveOpTiming(legacyOp);
        legacyOp.close();
        legacyOp = null;
        partitionOps = newPartitions;
    }

    /**
     * Used to convert legacyOp to multiple partitions. Distributes a single intermediate page
     * across {@code targets} by partition hash, folding each slice into the target via
     * {@link #mergeIntermediateIntoTable}.
     */
    private void distributeIntermediatePage(Page intermediatePage, HashAggregationOperator[] targets) {
        splitByPartitionAndDispatch(intermediatePage, targets.length, (p, subPage) -> mergeIntermediateIntoTable(targets[p], subPage));
    }

    /**
     * Folds an intermediate page (grouping keys at channels 0..keyCount-1, per-aggregator
     * intermediate state after that — the standard intermediate page layout) into {@code table}:
     * re-hash the key columns against {@code table}'s own {@link BlockHash} to get fresh local
     * group ids, then fan those out to each aggregator's {@code addIntermediateInput} — the same
     * primitive an {@code INTERMEDIATE}-mode {@link HashAggregationOperator} uses for cross-node
     * merges, reused here regardless of these aggregators' own {@link AggregatorMode}.
     */
    private void mergeIntermediateIntoTable(HashAggregationOperator op, Page page) {
        List<GroupingAggregatorFunction.AddInput> prepared = new ArrayList<>(op.aggregators.size());
        try {
            for (GroupingAggregator aggregator : op.aggregators) {
                GroupingAggregatorFunction.AddInput addInput = aggregator.aggregatorFunction()
                    .prepareProcessIntermediateInputPage(op.blockHash, page);
                if (addInput != null) {
                    prepared.add(addInput);
                }
            }
            op.blockHash.add(page, new FanOutAddInput(prepared));
        } finally {
            Releasables.closeExpectNoException(Releasables.wrap(prepared));
        }
    }

    // ---- steady-state partitioned processing ----

    /** Routes each row to its owning partition and feeds each partition's sub-page to its own table. */
    private void addToPartitions(Page page) {
        splitByPartitionAndDispatch(page, partitionCount, (p, subPage) -> partitionOps[p].processPage(subPage));
    }

    private boolean hasMultiValuedKeys(Page page) {
        for (int k = 0; k < groupChannels.size(); k++) {
            Block keyBlock = page.getBlock(groupChannels.get(k));
            if (keyBlock.asVector() == null) {
                int positions = keyBlock.getPositionCount();
                for (int i = 0; i < positions; i++) {
                    if (keyBlock.getValueCount(i) > 1) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    // ---- per-partition periodic early emit ----

    private void maybeEmitPartitions() {
        List<TaggedPageSource> sources = null;
        for (int p = 0; p < partitionCount; p++) {
            HashAggregationOperator op = partitionOps[p];
            if (shouldEmitPartition(op)) {
                if (sources == null) {
                    sources = new ArrayList<>();
                }
                int partitionIndex = p;
                ReleasableIterator<Page> pages = evaluateOp(op, maxPageSize);
                sources.add(new TaggedPageSource(p, resetPartitionOnClose(pages, partitionIndex)));
            }
        }
        if (sources != null) {
            output = new PartitionedOutputIterator(sources);
        }
    }

    private boolean shouldEmitPartition(HashAggregationOperator op) {
        return op.blockHash.numKeys() >= perPartitionEmitThreshold;
    }

    /**
     * Wraps {@code delegate} so that once its pages are fully consumed and it's closed, the
     * operator at {@code partitionOps[partitionIndex]} is closed and replaced with a fresh one.
     * Safe to reset only once {@code delegate} itself is closed/exhausted, since later pages of
     * a multi-page result still read the operator's {@link BlockHash} lazily (for {@code getKeys});
     * {@code getOutput} only closes an exhausted iterator, so this always fires at the right time,
     * whether through normal drainage or the operator being closed early.
     */
    private ReleasableIterator<Page> resetPartitionOnClose(ReleasableIterator<Page> delegate, int partitionIndex) {
        return new ReleasableIterator<>() {
            @Override
            public boolean hasNext() {
                return delegate.hasNext();
            }

            @Override
            public Page next() {
                return delegate.next();
            }

            @Override
            public void close() {
                delegate.close();
                saveOpTiming(partitionOps[partitionIndex]);
                partitionOps[partitionIndex].close();
                partitionOps[partitionIndex] = newOp();
            }
        };
    }

    // ---- finish() ----

    private void emitFinal() {
        emittedFromPartitioned = (partitionOps != null);
        List<TaggedPageSource> sources = new ArrayList<>();
        if (legacyOp != null) {
            HashAggregationOperator op = legacyOp;
            legacyOp = null;
            saveOpTiming(op);
            if (op.blockHash.numKeys() > 0) {
                sources.add(new TaggedPageSource(NONE_PARTITION, closeOpOnClose(evaluateOp(op, maxPageSize), op)));
            } else {
                op.close();
            }
        }
        if (partitionOps != null) {
            for (int p = 0; p < partitionCount; p++) {
                HashAggregationOperator op = partitionOps[p];
                saveOpTiming(op);
                if (op.blockHash.numKeys() > 0) {
                    sources.add(new TaggedPageSource(p, closeOpOnClose(evaluateOp(op, maxPageSize), op)));
                } else {
                    op.close();
                }
            }
            partitionOps = null;
        }
        if (sources.isEmpty() == false) {
            output = new PartitionedOutputIterator(sources);
        }
    }

    // ---- shared operator helpers ----

    /**
     * Builds a child {@link HashAggregationOperator} with a per-partition block hash and
     * auto-emit disabled ({@code partialEmitKeysThreshold = MAX_VALUE}). This operator manages
     * its own periodic emit externally via {@link #shouldEmitPartition} and
     * {@link #evaluateOp}.
     */
    private HashAggregationOperator newOp() {
        return new HashAggregationOperator(
            AggregatorMode.INITIAL,
            aggregatorFactories,
            () -> BlockHash.build(internalGroupSpecs, driverContext.blockFactory(), aggregationBatchSize, false),
            Integer.MAX_VALUE,
            1.0,
            maxPageSize,
            null,
            driverContext
        );
    }

    /**
     * Splits {@code page} into per-partition sub-pages via an O(N+P) bucket-sort pass
     * (count → prefix-sum → scatter) and calls {@code action} for each non-empty partition.
     * Sub-pages are physically contiguous copies of the relevant rows, improving sequential
     * access patterns on the target {@link BlockHash} compared to scatter-gather.
     * <p>
     * Null grouping-key rows are routed to {@link #NULL_PARTITION}. {@link BlockHash.Router#fillPartitions}
     * hoists per-page {@link Page#getBlock} calls outside the inner loop when no key column has nulls.
     * </p>
     */
    private void splitByPartitionAndDispatch(Page page, int nPartitions, PartitionAction action) {
        int positions = page.getPositionCount();
        int[] partitionOf = new int[positions];
        int[] counts = new int[nPartitions];
        long routingStart = System.nanoTime();
        fillPartitionAssignments(page, nPartitions, partitionOf, counts);
        BucketSort sorted = sortPositionsByPartition(partitionOf, counts, nPartitions);
        routingNanos += System.nanoTime() - routingStart;
        for (int p = 0; p < nPartitions; p++) {
            int start = sorted.offsets()[p], end = sorted.offsets()[p + 1];
            if (start == end) {
                continue;
            }
            try (Page subPage = page.filter(false, sorted.sortedPositions(), start, end - start)) {
                action.accept(p, subPage);
            }
        }
    }

    @FunctionalInterface
    private interface PartitionAction {
        void accept(int partition, Page subPage);
    }

    private void saveOpTiming(HashAggregationOperator op) {
        HashAggregationOperator.Status s = (HashAggregationOperator.Status) op.status();
        savedHashNanos += s.hashNanos();
        savedAggNanos += s.aggregationNanos();
    }

    @Override
    public Operator.Status status() {
        long hashNanos = savedHashNanos, aggregationNanos = savedAggNanos;
        if (partitionOps != null) {
            for (HashAggregationOperator op : partitionOps) {
                if (op != null) {
                    HashAggregationOperator.Status s = (HashAggregationOperator.Status) op.status();
                    hashNanos += s.hashNanos();
                    aggregationNanos += s.aggregationNanos();
                }
            }
        }
        if (legacyOp != null) {
            HashAggregationOperator.Status s = (HashAggregationOperator.Status) legacyOp.status();
            hashNanos += s.hashNanos();
            aggregationNanos += s.aggregationNanos();
        }
        boolean isPartitioned = emittedFromPartitioned || partitionOps != null;
        return new Status(
            routingNanos - conversionNanos,
            conversionNanos,
            hashNanos,
            aggregationNanos,
            pagesProcessed,
            rowsReceived,
            rowsEmitted,
            isPartitioned
        );
    }

    public static class Status implements Operator.Status {
        public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
            Operator.Status.class,
            "partitioned_hash_agg",
            Status::new
        );

        private final long routingNanos;
        private final long conversionNanos;
        private final long hashNanos;
        private final long aggregationNanos;
        private final int pagesProcessed;
        private final long rowsReceived;
        private final long rowsEmitted;
        private final boolean partitionedMode;

        public Status(
            long routingNanos,
            long conversionNanos,
            long hashNanos,
            long aggregationNanos,
            int pagesProcessed,
            long rowsReceived,
            long rowsEmitted,
            boolean partitionedMode
        ) {
            this.routingNanos = routingNanos;
            this.conversionNanos = conversionNanos;
            this.hashNanos = hashNanos;
            this.aggregationNanos = aggregationNanos;
            this.pagesProcessed = pagesProcessed;
            this.rowsReceived = rowsReceived;
            this.rowsEmitted = rowsEmitted;
            this.partitionedMode = partitionedMode;
        }

        public Status(StreamInput in) throws IOException {
            routingNanos = in.readVLong();
            conversionNanos = in.readVLong();
            hashNanos = in.readVLong();
            aggregationNanos = in.readVLong();
            pagesProcessed = in.readVInt();
            rowsReceived = in.readVLong();
            rowsEmitted = in.readVLong();
            partitionedMode = in.readBoolean();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVLong(routingNanos);
            out.writeVLong(conversionNanos);
            out.writeVLong(hashNanos);
            out.writeVLong(aggregationNanos);
            out.writeVInt(pagesProcessed);
            out.writeVLong(rowsReceived);
            out.writeVLong(rowsEmitted);
            out.writeBoolean(partitionedMode);
        }

        @Override
        public String getWriteableName() {
            return ENTRY.name;
        }

        /**
         * Nanoseconds spent in steady-state bucket-sort routing (fillPartitions + prefix-sum + scatter),
         * excluding the one-time conversion pass; see {@link #conversionNanos()}.
         */
        public long routingNanos() {
            return routingNanos;
        }

        /**
         * Nanoseconds spent routing the legacy table's intermediate output to partition operators
         * during the one-time conversion in {@code convertToPartitioned}. Subset of the total
         * routing work; reported separately so it doesn't inflate the per-page routing cost.
         */
        public long conversionNanos() {
            return conversionNanos;
        }

        /** Nanoseconds inner partition operators spent hashing grouping keys, summed across all partitions. */
        public long hashNanos() {
            return hashNanos;
        }

        /** Nanoseconds inner partition operators spent running aggregations, summed across all partitions. */
        public long aggregationNanos() {
            return aggregationNanos;
        }

        /** Count of pages this operator has processed. */
        public int pagesProcessed() {
            return pagesProcessed;
        }

        /** Count of rows this operator has received as input. */
        public long rowsReceived() {
            return rowsReceived;
        }

        /** Count of rows this operator has emitted as output. */
        public long rowsEmitted() {
            return rowsEmitted;
        }

        /** True if this operator ran (at least partially) in partitioned mode. */
        public boolean partitionedMode() {
            return partitionedMode;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Status status = (Status) o;
            return routingNanos == status.routingNanos
                && conversionNanos == status.conversionNanos
                && hashNanos == status.hashNanos
                && aggregationNanos == status.aggregationNanos
                && pagesProcessed == status.pagesProcessed
                && rowsReceived == status.rowsReceived
                && rowsEmitted == status.rowsEmitted
                && partitionedMode == status.partitionedMode;
        }

        @Override
        public int hashCode() {
            return java.util.Objects.hash(
                routingNanos,
                conversionNanos,
                hashNanos,
                aggregationNanos,
                pagesProcessed,
                rowsReceived,
                rowsEmitted,
                partitionedMode
            );
        }

        @Override
        public TransportVersion getMinimalSupportedVersion() {
            return TransportVersion.minimumCompatible();
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("partitioned_mode", partitionedMode);
            builder.field("routing_nanos", routingNanos);
            if (builder.humanReadable()) {
                builder.field("routing_time", TimeValue.timeValueNanos(routingNanos));
            }
            builder.field("conversion_nanos", conversionNanos);
            if (builder.humanReadable()) {
                builder.field("conversion_time", TimeValue.timeValueNanos(conversionNanos));
            }
            builder.field("hash_nanos", hashNanos);
            if (builder.humanReadable()) {
                builder.field("hash_time", TimeValue.timeValueNanos(hashNanos));
            }
            builder.field("aggregation_nanos", aggregationNanos);
            if (builder.humanReadable()) {
                builder.field("aggregation_time", TimeValue.timeValueNanos(aggregationNanos));
            }
            builder.field("pages_processed", pagesProcessed);
            builder.field("rows_received", rowsReceived);
            builder.field("rows_emitted", rowsEmitted);
            return builder.endObject();
        }
    }

    /** Fans a single group-id batch out to every prepared aggregator's {@code AddInput}. */
    private static final class FanOutAddInput implements GroupingAggregatorFunction.AddInput {
        private final List<GroupingAggregatorFunction.AddInput> prepared;

        FanOutAddInput(List<GroupingAggregatorFunction.AddInput> prepared) {
            this.prepared = prepared;
        }

        @Override
        public void add(int positionOffset, IntArrayBlock groupIds) {
            for (GroupingAggregatorFunction.AddInput p : prepared) {
                p.add(positionOffset, groupIds);
            }
        }

        @Override
        public void add(int positionOffset, IntBigArrayBlock groupIds) {
            for (GroupingAggregatorFunction.AddInput p : prepared) {
                p.add(positionOffset, groupIds);
            }
        }

        @Override
        public void add(int positionOffset, IntVector groupIds) {
            for (GroupingAggregatorFunction.AddInput p : prepared) {
                p.add(positionOffset, groupIds);
            }
        }

        @Override
        public void close() {}
    }

    private record TaggedPageSource(int partitionId, ReleasableIterator<Page> pages) {}

    /**
     * Concatenates each triggered partition's pages into the single {@link #output} iterator
     * {@code getOutput()} drains, tagging each page with its partition id as it goes.
     */
    private final class PartitionedOutputIterator implements ReleasableIterator<Page> {
        private final Deque<TaggedPageSource> sources;

        PartitionedOutputIterator(List<TaggedPageSource> sources) {
            this.sources = new ArrayDeque<>(sources);
        }

        @Override
        public boolean hasNext() {
            while (sources.isEmpty() == false && sources.peekFirst().pages().hasNext() == false) {
                sources.pollFirst().pages().close();
            }
            return sources.isEmpty() == false;
        }

        @Override
        public Page next() {
            if (hasNext() == false) {
                throw new NoSuchElementException();
            }
            TaggedPageSource source = sources.peekFirst();
            int partitionId = source.partitionId();
            Page page = source.pages().next();
            if (partitionId != NONE_PARTITION) {
                Page tagged = page.withPartitionId(partitionId);
                page.releaseBlocks();
                return tagged;
            }
            return page;
        }

        @Override
        public void close() {
            for (TaggedPageSource source : sources) {
                source.pages().close();
            }
        }
    }
}
