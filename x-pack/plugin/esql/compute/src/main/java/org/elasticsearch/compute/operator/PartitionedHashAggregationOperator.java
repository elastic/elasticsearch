/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.compute.aggregation.AggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.AggregatorMode;
import org.elasticsearch.compute.aggregation.GroupingAggregator;
import org.elasticsearch.compute.aggregation.GroupingAggregatorEvaluationContext;
import org.elasticsearch.compute.aggregation.GroupingAggregatorFunction;
import org.elasticsearch.compute.aggregation.blockhash.BlockHash;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.IntArrayBlock;
import org.elasticsearch.compute.data.IntBigArrayBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.ReleasableIterator;
import org.elasticsearch.core.Releasables;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.List;
import java.util.NoSuchElementException;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;

/**
 * Aggregates raw input {@link Page}s into many output rows, grouping by a single {@code LONG}
 * column, by routing rows across {@code N} independent, otherwise-completely-unmodified
 * {@code (BlockHash, List<GroupingAggregator>)} pairs ("partitions") rather than one big shared
 * table. See {@code scratch/partitioned-hash-aggregation-design.md} for the full design; this is
 * Phase 2's real-implementation build order step 3.
 * <p>
 *     Starts as a single ordinary table (the "legacy" table) and converts, non-destructively,
 *     into {@code N} partitions once that table's key count crosses
 *     {@code partitionConversionThreshold}. Steady-state routing uses a two-phase bucket-sort:
 *     a routing-only {@link BlockHash} ({@code probeHash}, always a {@code PackedValuesBlockHash})
 *     computes a partition ID per row via {@link BlockHash.Router#partitionHashOfRow}; a single
 *     scatter pass then places positions into contiguous per-partition ranges; and for each
 *     non-empty partition a filtered sub-page (a physical copy of just that partition's rows) is
 *     fed to the partition's own table via {@code blockHash.add()} — typically the faster
 *     {@code LongIntAdaptiveBlockHash}, which does not need to expose a router itself.
 * </p>
 * <p>
 *     Only supports {@link AggregatorMode#INITIAL} (raw input, partial output): partitioning is a
 *     data-node-local optimization over raw rows, not something the coordinator-side merge (a
 *     later phase) needs to know about.
 * </p>
 * <p>
 *     Multi-valued grouping keys break the one-row-one-partition assumption bucket-sort routing
 *     depends on. The first time one is observed, this operator permanently reverts to the legacy
 *     single-table behavior (draining every partition back into one table), rather than ever
 *     misrouting a row.
 * </p>
 */
public class PartitionedHashAggregationOperator implements Operator {

    public static final int DEFAULT_PARTITION_COUNT = 32;
    public static final int DEFAULT_PARTITION_CONVERSION_THRESHOLD = 5_000;
    public static final int DEFAULT_PER_PARTITION_EMIT_THRESHOLD = 10_000;
    public static final double DEFAULT_PER_PARTITION_EMIT_UNIQUENESS_THRESHOLD = 0.1;

    /** The fixed partition every null grouping key is routed to, rather than being hashed. */
    private static final int NULL_PARTITION = 0;

    /** {@link #outputPartition()} value for pages emitted from the legacy (pre-conversion) table. */
    public static final int NONE_PARTITION = -1;

    private static final GroupingAggregatorPageBuilder.CustomizeSelected NO_CUSTOMIZATION = (aggregator, selected) -> {
        selected.incRef();
        return selected;
    };

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
        private double perPartitionEmitUniquenessThreshold = DEFAULT_PER_PARTITION_EMIT_UNIQUENESS_THRESHOLD;
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

        public Builder perPartitionEmit(int keysThreshold, double uniquenessThreshold) {
            this.perPartitionEmitThreshold = keysThreshold;
            this.perPartitionEmitUniquenessThreshold = uniquenessThreshold;
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
        private final double perPartitionEmitUniquenessThreshold;
        private final int maxPageSize;
        private final int aggregationBatchSize;

        private Factory(Builder builder) {
            List<BlockHash.GroupSpec> externalSpecs = requireNonNull(builder.groupSpecs, "groupSpecs");
            if (externalSpecs.isEmpty()) {
                throw new IllegalArgumentException("groupSpecs must not be empty");
            }
            List<Integer> channels = new ArrayList<>(externalSpecs.size());
            List<BlockHash.GroupSpec> internalSpecs = new ArrayList<>(externalSpecs.size());
            for (int k = 0; k < externalSpecs.size(); k++) {
                channels.add(externalSpecs.get(k).channel());
                internalSpecs.add(new BlockHash.GroupSpec(k, externalSpecs.get(k).elementType()));
            }
            this.groupChannels = List.copyOf(channels);
            this.internalGroupSpecs = List.copyOf(internalSpecs);
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
            this.perPartitionEmitUniquenessThreshold = builder.perPartitionEmitUniquenessThreshold;
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
                perPartitionEmitUniquenessThreshold,
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

    private final List<Integer> groupChannels;
    private final List<BlockHash.GroupSpec> internalGroupSpecs;
    private final List<GroupingAggregator.Factory> aggregatorFactories;
    private final List<List<Integer>> aggregatorRawChannels;
    private final int[] combinedChannelStart;
    private final int internalPageWidth;
    private final int partitionCount;
    private final int partitionConversionThreshold;
    private final int perPartitionEmitThreshold;
    private final double perPartitionEmitUniquenessThreshold;
    private final int maxPageSize;
    private final int aggregationBatchSize;
    private final DriverContext driverContext;

    /** Non-null until {@link #convertToPartitioned} (or a multi-valued key) replaces it. */
    private Table legacy;
    /** Non-null once converted from the legacy table. */
    private Table[] partitions;
    /**
     * Routing-only hash, non-null once {@link #convertToPartitioned} succeeds. Uses
     * {@link BlockHash#buildPackedValuesBlockHash} (which has a {@link BlockHash.Router} for any
     * fixed-width key schema) solely to compute partition IDs via
     * {@link BlockHash.Router#partitionHashOfRow}. Never used for aggregation — the actual
     * partition tables use the faster {@link BlockHash#build} result.
     */
    private BlockHash probeHash;
    /** Set once a multi-valued key is observed; conversion is never attempted again after that. */
    private boolean permanentlyUnpartitioned;

    private boolean finished;
    private ReleasableIterator<Page> output;
    private int currentOutputPartition = NONE_PARTITION;

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
        double perPartitionEmitUniquenessThreshold,
        int maxPageSize,
        int aggregationBatchSize,
        DriverContext driverContext
    ) {
        if (partitionCount <= 0) {
            throw new IllegalArgumentException("partitionCount must be greater than 0; got " + partitionCount);
        }
        if (partitionConversionThreshold <= 0) {
            throw new IllegalArgumentException("partitionConversionThreshold must be greater than 0; got " + partitionConversionThreshold);
        }
        this.groupChannels = groupChannels;
        this.internalGroupSpecs = internalGroupSpecs;
        this.aggregatorFactories = aggregatorFactories;
        this.aggregatorRawChannels = aggregatorRawChannels;
        this.combinedChannelStart = combinedChannelStart;
        this.internalPageWidth = internalPageWidth;
        this.partitionCount = partitionCount;
        this.partitionConversionThreshold = partitionConversionThreshold;
        this.perPartitionEmitThreshold = perPartitionEmitThreshold;
        this.perPartitionEmitUniquenessThreshold = perPartitionEmitUniquenessThreshold;
        this.maxPageSize = maxPageSize;
        this.aggregationBatchSize = aggregationBatchSize;
        this.driverContext = driverContext;
        boolean success = false;
        try {
            this.legacy = newTable(aggregationBatchSize);
            success = true;
        } finally {
            if (success == false) {
                close();
            }
        }
    }

    /**
     * The partition the page most recently returned by {@link #getOutput} belongs to, or
     * {@link #NONE_PARTITION} if it came from the (pre-conversion) legacy table. Exposed for a
     * future wiring step that tags emitted pages with this partition id on the wire; unused
     * internally beyond that.
     */
    protected int outputPartition() {
        return currentOutputPartition;
    }

    @Override
    public boolean needsInput() {
        return output == null && finished == false;
    }

    @Override
    public void addInput(Page page) {
        try {
            checkState(needsInput(), "Operator is already finishing");
            requireNonNull(page, "page is null");
            if (partitions != null && permanentlyUnpartitioned == false && hasMultiValuedKeys(page)) {
                revertToLegacy();
            }
            Page internal = toInternalLayout(page);
            if (partitions == null) {
                addToLegacy(internal);
                if (permanentlyUnpartitioned == false && legacy.blockHash.numKeys() >= partitionConversionThreshold) {
                    convertToPartitioned();
                }
            } else {
                addToPartitions(internal);
                maybeEmitPartitions();
            }
        } finally {
            page.releaseBlocks();
        }
    }

    /**
     * Rearranges {@code page} into this operator's internal channel convention: the grouping key
     * at channel 0, then each aggregator's raw value(s) at {@link #combinedChannelStart}[i]. Every
     * table's aggregators are built with channels wide enough to also read their own intermediate
     * state (channel 0's group id excluded) at those same positions, so the exact same aggregator
     * instances can be fed either raw pages (via {@code addGather}, reading only the first
     * {@code rawChannels.size()} of their channels) or this operator's own intermediate pages
     * (reading all of them) - no separate "raw" vs. "intermediate" aggregator instances needed.
     * <p>
     *     Builds a new {@link Page} that references the original blocks directly (no value
     *     copies); never {@link Page#close}/{@link Page#releaseBlocks} it - {@code page} itself
     *     remains the sole owner and is released by {@link #addInput}'s caller.
     * </p>
     */
    private Page toInternalLayout(Page page) {
        Block[] blocks = new Block[internalPageWidth];
        // Page's constructor asserts every block's position count, so slots beyond an
        // aggregator's raw channel count (only meaningful for intermediate consumption, never
        // read during raw processing) still need *some* valid block; the first key block is as
        // good as any other already-at-hand, always-correct-position-count placeholder.
        Arrays.fill(blocks, page.getBlock(groupChannels.get(0)));
        for (int k = 0; k < groupChannels.size(); k++) {
            blocks[k] = page.getBlock(groupChannels.get(k));
        }
        for (int a = 0; a < aggregatorRawChannels.size(); a++) {
            List<Integer> rawChannels = aggregatorRawChannels.get(a);
            int base = combinedChannelStart[a];
            for (int j = 0; j < rawChannels.size(); j++) {
                blocks[base + j] = page.getBlock(rawChannels.get(j));
            }
        }
        return new Page(blocks);
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
        return output.next();
    }

    @Override
    public void finish() {
        if (finished) {
            return;
        }
        finished = true;
        emitFinal();
    }

    @Override
    public boolean isFinished() {
        return finished && output == null;
    }

    @Override
    public boolean canProduceMoreDataWithoutExtraInput() {
        return output != null;
    }

    @Override
    public void close() {
        Releasables.close(legacy, probeHash, output);
        if (partitions != null) {
            Releasables.close(partitions);
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(getClass().getSimpleName()).append("[");
        if (partitions != null) {
            sb.append("partitionCount=").append(partitionCount);
        } else if (legacy != null) {
            sb.append("legacy=").append(legacy.blockHash);
        } else {
            sb.append("emitting");
        }
        sb.append("]");
        return sb.toString();
    }

    // ---- legacy (pre-conversion) processing ----

    private void addToLegacy(Page page) {
        List<GroupingAggregatorFunction.AddInput> prepared = new ArrayList<>(legacy.aggregators.size());
        try {
            for (GroupingAggregator aggregator : legacy.aggregators) {
                GroupingAggregatorFunction.AddInput addInput = aggregator.prepareProcessPage(legacy.blockHash, page);
                if (addInput != null) {
                    prepared.add(addInput);
                }
            }
            legacy.blockHash.add(page, new FanOutAddInput(prepared));
        } finally {
            Releasables.closeExpectNoException(Releasables.wrap(prepared));
        }
        legacy.rowsAddedInCurrentBatch += page.getPositionCount();
    }

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
        Table[] newPartitions = new Table[partitionCount];
        for (int p = 0; p < partitionCount; p++) {
            newPartitions[p] = newPartitionTable(aggregationBatchSize);
        }
        try (ReleasableIterator<Page> intermediatePages = evaluateToIntermediate(legacy)) {
            while (intermediatePages.hasNext()) {
                try (Page intermediatePage = intermediatePages.next()) {
                    distributeIntermediatePage(intermediatePage, newPartitions);
                }
            }
        }
        legacy.close();
        legacy = null;
        partitions = newPartitions;
    }

    /**
     * Distributes a single intermediate page across {@code targets} by partition hash. Uses a
     * single O(N+P) bucket-sort pass (count → prefix-sum → scatter) to collect each partition's
     * positions, then creates a filtered sub-page per partition and merges it into that target
     * via {@link #mergeIntermediateIntoTable}. The filtered sub-pages are physically reordered
     * copies — sequential reads on the target table's BlockHash improve cache utilisation
     * compared to a scatter-gather approach.
     */
    private void distributeIntermediatePage(Page intermediatePage, Table[] targets) {
        int positions = intermediatePage.getPositionCount();
        int keyCount = groupChannels.size();
        BlockHash.Router probeRouter = probeHash.router();
        int[] partitionOf = new int[positions];
        int[] counts = new int[targets.length];
        for (int i = 0; i < positions; i++) {
            boolean anyNull = false;
            for (int k = 0; k < keyCount; k++) {
                if (intermediatePage.getBlock(k).isNull(i)) {
                    anyNull = true;
                    break;
                }
            }
            partitionOf[i] = anyNull ? NULL_PARTITION : Math.floorMod(probeRouter.partitionHashOfRow(intermediatePage, i), targets.length);
            counts[partitionOf[i]]++;
        }
        int[] offsets = new int[targets.length + 1];
        for (int p = 0; p < targets.length; p++) {
            offsets[p + 1] = offsets[p] + counts[p];
        }
        int[] cursor = offsets.clone();
        int[] sortedPositions = new int[positions];
        for (int i = 0; i < positions; i++) {
            sortedPositions[cursor[partitionOf[i]]++] = i;
        }
        for (int p = 0; p < targets.length; p++) {
            int start = offsets[p], end = offsets[p + 1];
            if (start == end) {
                continue;
            }
            try (Page subPage = intermediatePage.filter(false, Arrays.copyOfRange(sortedPositions, start, end))) {
                mergeIntermediateIntoTable(targets[p], subPage);
            }
        }
    }

    /**
     * Folds an intermediate page (grouping key at channel 0, per-aggregator intermediate state
     * after that - the standard intermediate page layout) into {@code table}: re-hash the key
     * column against {@code table}'s own {@link BlockHash} to get fresh local group ids, then fan
     * those out to each aggregator's {@code addIntermediateInput} - the same primitive an
     * {@code INTERMEDIATE}-mode {@link HashAggregationOperator} uses for cross-node merges,
     * reused here regardless of these aggregators' own {@link AggregatorMode}.
     */
    private void mergeIntermediateIntoTable(Table table, Page page) {
        List<GroupingAggregatorFunction.AddInput> prepared = new ArrayList<>(table.aggregators.size());
        try {
            for (GroupingAggregator aggregator : table.aggregators) {
                GroupingAggregatorFunction.AddInput addInput = aggregator.aggregatorFunction()
                    .prepareProcessIntermediateInputPage(table.blockHash, page);
                if (addInput != null) {
                    prepared.add(addInput);
                }
            }
            table.blockHash.add(page, new FanOutAddInput(prepared));
        } finally {
            Releasables.closeExpectNoException(Releasables.wrap(prepared));
        }
    }

    // ---- steady-state partitioned processing ----

    /**
     * Routes each row to its owning partition, then physically bucket-sorts the page data so that
     * each partition's rows are contiguous before insertion. For each partition we:
     * <ol>
     *   <li>Compute a partition ID per row via {@code probeHash.router().partitionHashOfRow} (one
     *       call per row — the routing hash computed here is <em>not</em> reused for table
     *       insertion, so there is no redundant re-hash).</li>
     *   <li>Bucket-sort positions into a per-partition contiguous range in O(N+P) time.</li>
     *   <li>Create a filtered sub-page (physically copying just that partition's rows) and feed it
     *       to {@code partitions[p].blockHash.add()} — the fastest available hash for this schema
     *       (typically {@code LongIntAdaptiveBlockHash}), with sequential rather than scattered
     *       block reads.</li>
     * </ol>
     */
    private void addToPartitions(Page page) {
        int positions = page.getPositionCount();
        int keyCount = groupChannels.size();
        BlockHash.Router probeRouter = probeHash.router();
        int[] partitionOf = new int[positions];
        int[] counts = new int[partitionCount];
        // Hoist block lookups out of the tight per-row loop.
        Block[] keyBlocks = new Block[keyCount];
        boolean anyBlockHasNulls = false;
        for (int k = 0; k < keyCount; k++) {
            keyBlocks[k] = page.getBlock(k);
            if (keyBlocks[k].asVector() == null) {
                anyBlockHasNulls = true;
            }
        }
        if (anyBlockHasNulls) {
            for (int i = 0; i < positions; i++) {
                boolean anyNull = false;
                for (int k = 0; k < keyCount; k++) {
                    if (keyBlocks[k].isNull(i)) {
                        anyNull = true;
                        break;
                    }
                }
                partitionOf[i] = anyNull ? NULL_PARTITION : Math.floorMod(probeRouter.partitionHashOfRow(page, i), partitionCount);
                counts[partitionOf[i]]++;
            }
        } else {
            for (int i = 0; i < positions; i++) {
                partitionOf[i] = Math.floorMod(probeRouter.partitionHashOfRow(page, i), partitionCount);
                counts[partitionOf[i]]++;
            }
        }
        int[] offsets = new int[partitionCount + 1];
        for (int p = 0; p < partitionCount; p++) {
            offsets[p + 1] = offsets[p] + counts[p];
        }
        int[] cursor = offsets.clone();
        int[] sortedPositions = new int[positions];
        for (int i = 0; i < positions; i++) {
            sortedPositions[cursor[partitionOf[i]]++] = i;
        }
        for (int p = 0; p < partitionCount; p++) {
            int start = offsets[p], end = offsets[p + 1];
            if (start == end) {
                continue;
            }
            Table table = partitions[p];
            try (Page subPage = page.filter(false, Arrays.copyOfRange(sortedPositions, start, end))) {
                List<GroupingAggregatorFunction.AddInput> prepared = new ArrayList<>(table.aggregators.size());
                try {
                    for (GroupingAggregator aggregator : table.aggregators) {
                        GroupingAggregatorFunction.AddInput addInput = aggregator.prepareProcessPage(table.blockHash, subPage);
                        if (addInput != null) {
                            prepared.add(addInput);
                        }
                    }
                    table.blockHash.add(subPage, new FanOutAddInput(prepared));
                } finally {
                    Releasables.closeExpectNoException(Releasables.wrap(prepared));
                }
                table.rowsAddedInCurrentBatch += (end - start);
            }
        }
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

    /**
     * Permanently falls back to single-table behavior: drains every partition's contents back
     * into one fresh legacy table (the same evaluate-intermediate merge primitive as conversion,
     * in reverse), since a multi-valued key showed up and bucket-sort routing can't handle it.
     */
    private void revertToLegacy() {
        permanentlyUnpartitioned = true;
        Table newLegacy = newTable(aggregationBatchSize);
        boolean success = false;
        try {
            for (Table partition : partitions) {
                if (partition.blockHash.numKeys() > 0) {
                    drainTableInto(partition, newLegacy);
                }
            }
            success = true;
        } finally {
            BlockHash ph = probeHash;
            probeHash = null;
            Releasables.close(ph);
            Releasables.close(partitions);
            partitions = null;
            if (success) {
                legacy = newLegacy;
            } else {
                newLegacy.close();
            }
        }
    }

    private void drainTableInto(Table source, Table destination) {
        try (ReleasableIterator<Page> pages = evaluateToIntermediate(source)) {
            while (pages.hasNext()) {
                try (Page intermediatePage = pages.next()) {
                    mergeIntermediateIntoTable(destination, intermediatePage);
                }
            }
        }
    }

    // ---- per-partition periodic early emit ----

    private void maybeEmitPartitions() {
        List<TaggedPageSource> sources = null;
        for (int p = 0; p < partitionCount; p++) {
            Table table = partitions[p];
            if (shouldEmitPartition(table)) {
                if (sources == null) {
                    sources = new ArrayList<>();
                }
                int partitionIndex = p;
                ReleasableIterator<Page> pages = evaluateToIntermediate(table, maxPageSize);
                sources.add(new TaggedPageSource(p, resetPartitionOnClose(pages, partitionIndex)));
            }
        }
        if (sources != null) {
            output = new PartitionedOutputIterator(sources);
        }
    }

    private boolean shouldEmitPartition(Table table) {
        if (table.rowsAddedInCurrentBatch == 0) {
            return false;
        }
        int numKeys = table.blockHash.numKeys();
        if (numKeys < perPartitionEmitThreshold) {
            return false;
        }
        return table.rowsAddedInCurrentBatch * perPartitionEmitUniquenessThreshold <= numKeys;
    }

    /**
     * Wraps {@code delegate} so that once its pages are fully consumed and it's closed, the table
     * at {@code partitions[partitionIndex]} is closed and replaced with a fresh one - the
     * destructive per-partition generalization of
     * {@code HashAggregationOperator#maybeReinitializeAfterPeriodicallyEmitted}. Safe to reset
     * only once {@code delegate} itself is closed/exhausted, since later pages of a multi-page
     * result still read the table's {@link BlockHash} lazily (for {@code getKeys}); {@code
     * getOutput} only closes an exhausted iterator, so this always fires at the right time,
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
                partitions[partitionIndex].close();
                partitions[partitionIndex] = newPartitionTable(aggregationBatchSize);
            }
        };
    }

    /** Wraps {@code delegate} so that closing it also closes {@code table} (never rebuilt). */
    private ReleasableIterator<Page> closeTableOnClose(ReleasableIterator<Page> delegate, Table table) {
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
                Releasables.close(delegate::close, table);
            }
        };
    }

    // ---- finish() ----

    private void emitFinal() {
        List<TaggedPageSource> sources = new ArrayList<>();
        if (legacy != null) {
            Table table = legacy;
            legacy = null;
            if (table.blockHash.numKeys() > 0) {
                sources.add(new TaggedPageSource(NONE_PARTITION, closeTableOnClose(evaluateToIntermediate(table, maxPageSize), table)));
            } else {
                table.close();
            }
        } else {
            for (int p = 0; p < partitionCount; p++) {
                Table table = partitions[p];
                if (table.blockHash.numKeys() > 0) {
                    sources.add(new TaggedPageSource(p, closeTableOnClose(evaluateToIntermediate(table, maxPageSize), table)));
                } else {
                    table.close();
                }
            }
            partitions = null;
        }
        if (sources.isEmpty() == false) {
            output = new PartitionedOutputIterator(sources);
        }
    }

    // ---- shared table helpers ----

    private Table newTable(int emitBatchSize) {
        // Every page these tables ever see (raw or intermediate) is in this operator's internal
        // layout, which places grouping keys at channels 0..groupChannels.size()-1 - see toInternalLayout.
        BlockHash blockHash = BlockHash.build(internalGroupSpecs, driverContext.blockFactory(), emitBatchSize, false);
        boolean success = false;
        try {
            Table table = new Table(blockHash, buildAggregators());
            success = true;
            return table;
        } finally {
            if (success == false) {
                blockHash.close();
            }
        }
    }

    /**
     * Builds a partition table for aggregation. Routing (which partition a row belongs to) is
     * handled separately by {@link #probeHash}, so the table's {@link BlockHash} does not need
     * to expose a {@link BlockHash.Router} — we can always use the fastest available hash (e.g.
     * {@code LongIntAdaptiveBlockHash}) without a PackedValues fallback.
     */
    private Table newPartitionTable(int emitBatchSize) {
        BlockHash blockHash = BlockHash.build(internalGroupSpecs, driverContext.blockFactory(), emitBatchSize, false);
        boolean success = false;
        try {
            Table table = new Table(blockHash, buildAggregators());
            success = true;
            return table;
        } finally {
            if (success == false) {
                blockHash.close();
            }
        }
    }

    private List<GroupingAggregator> buildAggregators() {
        List<GroupingAggregator> result = new ArrayList<>(aggregatorFactories.size());
        boolean success = false;
        try {
            for (GroupingAggregator.Factory f : aggregatorFactories) {
                result.add(f.apply(driverContext));
            }
            success = true;
            return result;
        } finally {
            if (success == false) {
                Releasables.close(result);
            }
        }
    }

    /**
     * Evaluates {@code table} to a page reflecting its current, unmodified contents (its
     * grouping key column, followed by each aggregator's intermediate state) - the same
     * evaluate-to-page flow {@link HashAggregationOperator} uses, just interpreted here as an
     * "intermediate representation to re-consume elsewhere" rather than as final output. Table
     * stays alive/unmodified; the caller decides when (if ever) to close it.
     */
    private ReleasableIterator<Page> evaluateToIntermediate(Table table) {
        return evaluateToIntermediate(table, Integer.MAX_VALUE);
    }

    private ReleasableIterator<Page> evaluateToIntermediate(Table table, int maxPageSizeForThisEmit) {
        var pageBuilder = new GroupingAggregatorPageBuilder(table.blockHash, table.aggregators, maxPageSizeForThisEmit, NO_CUSTOMIZATION);
        return pageBuilder.build(new GroupingAggregatorEvaluationContext(driverContext));
    }

    protected static void checkState(boolean condition, String msg) {
        if (condition == false) {
            throw new IllegalArgumentException(msg);
        }
    }

    private static final class Table implements Releasable {
        BlockHash blockHash;
        List<GroupingAggregator> aggregators;
        long rowsAddedInCurrentBatch;

        Table(BlockHash blockHash, List<GroupingAggregator> aggregators) {
            this.blockHash = blockHash;
            this.aggregators = aggregators;
        }

        @Override
        public void close() {
            Releasables.close(blockHash, () -> Releasables.close(aggregators));
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
     * {@code getOutput()} drains, updating {@link #currentOutputPartition} as it advances from
     * one partition's pages to the next.
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
            currentOutputPartition = source.partitionId();
            Page page = source.pages().next();
            if (currentOutputPartition != NONE_PARTITION) {
                Page tagged = page.withPartitionId(currentOutputPartition);
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
