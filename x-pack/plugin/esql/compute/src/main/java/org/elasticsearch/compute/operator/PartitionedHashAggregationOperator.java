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
import org.elasticsearch.compute.data.ElementType;
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

    public static final int DEFAULT_PARTITION_COUNT = 8;
    public static final int DEFAULT_PARTITION_CONVERSION_THRESHOLD = 100_000;
    public static final int DEFAULT_PER_PARTITION_EMIT_THRESHOLD = 50_000;
    public static final double DEFAULT_PER_PARTITION_EMIT_UNIQUENESS_THRESHOLD = 0.1;

    /**
     * Returns true if the given group specs can be routed across partitions at runtime.
     * <p>
     * Single-column schemas are always routable: every single-column {@link BlockHash}
     * implementation (Long, Int, Double, BytesRef, …) exposes a {@link BlockHash.Router}.
     * Multi-column schemas use {@code PackedValuesBlockHash}, whose router requires all keys
     * to be fixed-width (no {@link ElementType#BYTES_REF}). When this returns false the planner
     * should skip PHAO+PHMO and fall through to the plain {@link HashAggregationOperator} instead.
     * </p>
     */
    public static boolean canPartition(List<BlockHash.GroupSpec> groupSpecs) {
        if (groupSpecs.size() <= 1) {
            return true;
        }
        return groupSpecs.stream().noneMatch(gs -> gs.elementType() == ElementType.BYTES_REF);
    }

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
    private HashAggregationOperator legacyOp;
    /** Non-null once converted from the legacy table. */
    private HashAggregationOperator[] partitionOps;
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
            this.legacyOp = newOp();
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
            if (partitionOps != null && permanentlyUnpartitioned == false && hasMultiValuedKeys(page)) {
                revertToLegacy();
            }
            Page internal = toInternalLayout(page);
            if (partitionOps == null) {
                legacyOp.processPage(internal);
                if (permanentlyUnpartitioned == false && legacyOp.blockHash.numKeys() >= partitionConversionThreshold) {
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
     * instances can be fed either raw pages (reading only the first
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
        } else if (legacyOp != null) {
            sb.append("legacy=").append(legacyOp.blockHash);
        } else {
            sb.append("emitting");
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
        try (ReleasableIterator<Page> intermediatePages = evaluateToIntermediate(legacyOp)) {
            while (intermediatePages.hasNext()) {
                try (Page intermediatePage = intermediatePages.next()) {
                    distributeIntermediatePage(intermediatePage, newPartitions);
                }
            }
        }
        legacyOp.close();
        legacyOp = null;
        partitionOps = newPartitions;
    }

    /**
     * Distributes a single intermediate page across {@code targets} by partition hash. Uses a
     * single O(N+P) bucket-sort pass (count → prefix-sum → scatter) to collect each partition's
     * positions, then creates a filtered sub-page per partition and merges it into that target
     * via {@link #mergeIntermediateIntoTable}. The filtered sub-pages are physically reordered
     * copies — sequential reads on the target table's BlockHash improve cache utilisation
     * compared to a scatter-gather approach.
     */
    private void distributeIntermediatePage(Page intermediatePage, HashAggregationOperator[] targets) {
        int positions = intermediatePage.getPositionCount();
        int keyCount = groupChannels.size();
        BlockHash.Router probeRouter = probeHash.router();
        int[] partitionOf = new int[positions];
        int[] counts = new int[targets.length];
        probeRouter.fillPartitions(intermediatePage, positions, keyCount, targets.length, NULL_PARTITION, partitionOf, counts);
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
            try (Page subPage = intermediatePage.filter(false, sortedPositions, start, end - start)) {
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

    /**
     * Routes each row to its owning partition, then bucket-sorts positions so each partition's rows
     * are contiguous before insertion. For each partition we:
     * <ol>
     *   <li>Compute a partition ID per row. When no key column has nulls,
     *       {@link BlockHash.Router#fillPartitions} is used: implementations can hoist
     *       {@link Page#getBlock} calls outside the inner loop (O(1) per page instead of O(N)).
     *       Rows with a null key land in {@link #NULL_PARTITION} regardless.</li>
     *   <li>Bucket-sort positions into a per-partition contiguous range in O(N+P) time.</li>
     *   <li>Create a filtered sub-page via {@link Page#filter(boolean, int[], int, int)},
     *       passing the pre-sorted positions array with an offset and length to avoid a
     *       per-partition {@link java.util.Arrays#copyOfRange} allocation.</li>
     *   <li>Feed the sub-page to {@code partitions[p].blockHash.add()} — the fastest available
     *       hash for this schema (typically {@code LongIntAdaptiveBlockHash}).</li>
     * </ol>
     */
    private void addToPartitions(Page page) {
        int positions = page.getPositionCount();
        int keyCount = groupChannels.size();
        BlockHash.Router probeRouter = probeHash.router();
        int[] partitionOf = new int[positions];
        int[] counts = new int[partitionCount];
        probeRouter.fillPartitions(page, positions, keyCount, partitionCount, NULL_PARTITION, partitionOf, counts);
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
            try (Page subPage = page.filter(false, sortedPositions, start, end - start)) {
                partitionOps[p].processPage(subPage);
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
     * into one fresh legacy operator (the same evaluate-intermediate merge primitive as conversion,
     * in reverse), since a multi-valued key showed up and bucket-sort routing can't handle it.
     */
    private void revertToLegacy() {
        permanentlyUnpartitioned = true;
        HashAggregationOperator newLegacy = newOp();
        boolean success = false;
        try {
            for (HashAggregationOperator partition : partitionOps) {
                if (partition.blockHash.numKeys() > 0) {
                    drainOpInto(partition, newLegacy);
                }
            }
            success = true;
        } finally {
            BlockHash ph = probeHash;
            probeHash = null;
            Releasables.close(ph);
            Releasables.close(partitionOps);
            partitionOps = null;
            if (success) {
                legacyOp = newLegacy;
            } else {
                newLegacy.close();
            }
        }
    }

    private void drainOpInto(HashAggregationOperator source, HashAggregationOperator destination) {
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
            HashAggregationOperator op = partitionOps[p];
            if (shouldEmitPartition(op)) {
                if (sources == null) {
                    sources = new ArrayList<>();
                }
                int partitionIndex = p;
                ReleasableIterator<Page> pages = evaluateToIntermediate(op, maxPageSize);
                sources.add(new TaggedPageSource(p, resetPartitionOnClose(pages, partitionIndex)));
            }
        }
        if (sources != null) {
            output = new PartitionedOutputIterator(sources);
        }
    }

    private boolean shouldEmitPartition(HashAggregationOperator op) {
        if (op.rowsAddedInCurrentBatch == 0) {
            return false;
        }
        int numKeys = op.blockHash.numKeys();
        if (numKeys < perPartitionEmitThreshold) {
            return false;
        }
        return op.rowsAddedInCurrentBatch * perPartitionEmitUniquenessThreshold <= numKeys;
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
                partitionOps[partitionIndex].close();
                partitionOps[partitionIndex] = newOp();
            }
        };
    }

    /** Wraps {@code delegate} so that closing it also closes {@code op} (never rebuilt). */
    private ReleasableIterator<Page> closeOpOnClose(ReleasableIterator<Page> delegate, HashAggregationOperator op) {
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
                Releasables.close(delegate::close, op);
            }
        };
    }

    // ---- finish() ----

    private void emitFinal() {
        List<TaggedPageSource> sources = new ArrayList<>();
        if (legacyOp != null) {
            HashAggregationOperator op = legacyOp;
            legacyOp = null;
            if (op.blockHash.numKeys() > 0) {
                sources.add(new TaggedPageSource(NONE_PARTITION, closeOpOnClose(evaluateToIntermediate(op, maxPageSize), op)));
            } else {
                op.close();
            }
        } else {
            for (int p = 0; p < partitionCount; p++) {
                HashAggregationOperator op = partitionOps[p];
                if (op.blockHash.numKeys() > 0) {
                    sources.add(new TaggedPageSource(p, closeOpOnClose(evaluateToIntermediate(op, maxPageSize), op)));
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
     * {@link #evaluateToIntermediate}.
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
     * Evaluates {@code op}'s current contents to intermediate pages - the same flow
     * {@link HashAggregationOperator} uses for output, reused here as an
     * "intermediate representation to re-consume elsewhere". The operator stays alive/unmodified;
     * the caller decides when (if ever) to close it.
     */
    private ReleasableIterator<Page> evaluateToIntermediate(HashAggregationOperator op) {
        return evaluateToIntermediate(op, Integer.MAX_VALUE);
    }

    private ReleasableIterator<Page> evaluateToIntermediate(HashAggregationOperator op, int maxPageSizeForThisEmit) {
        var pageBuilder = new GroupingAggregatorPageBuilder(op.blockHash, op.aggregators, maxPageSizeForThisEmit, NO_CUSTOMIZATION);
        return pageBuilder.build(new GroupingAggregatorEvaluationContext(driverContext));
    }

    protected static void checkState(boolean condition, String msg) {
        if (condition == false) {
            throw new IllegalArgumentException(msg);
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
