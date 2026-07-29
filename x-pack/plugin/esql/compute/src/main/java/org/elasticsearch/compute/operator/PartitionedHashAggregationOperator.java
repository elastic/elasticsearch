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
import org.elasticsearch.compute.aggregation.blockhash.BlockHash;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.ReleasableIterator;
import org.elasticsearch.core.Releasables;

import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;

import static java.util.stream.Collectors.joining;

/**
 * Aggregates raw input {@link Page}s into partitioned intermediate output using a single hash table.
 * <p>
 *     Accumulates all input in one inherited hash table. When the table's unique key count reaches
 *     {@link #emitKeysThreshold}, evaluates the table to intermediate pages, splits each page by
 *     {@code hash(key) % partitionCount}, tags each sub-page with its partition id, emits those
 *     tagged pages, then resets the table to accumulate again.
 * </p>
 * <p>
 *     Only supports {@link AggregatorMode#INITIAL} (raw input, partial output).
 * </p>
 * <p>
 *     The companion {@link PartitionedHashMergeOperator} receives the tagged pages and merges
 *     each partition independently in a background worker.
 * </p>
 */
public class PartitionedHashAggregationOperator extends HashAggregationOperator {

    public static final int DEFAULT_PARTITION_COUNT = 8;

    /**
     * Default threshold for triggering an intermediate emit. At ~24–32 bytes per entry (key +
     * aggregator state), 500k entries sits in the L3 cache range (~12–16 MB) on typical server
     * hardware, limiting cache thrashing without over-fragmenting output into many small pages.
     */
    public static final int DEFAULT_EMIT_KEYS_THRESHOLD = 500_000;

    /**
     * Returns true if the given group specs support output-side partitioning.
     * <p>
     *     TopN hashes accumulate at most {@code limit} groups — never enough to trigger an emit.
     *     Categorize hashes use semantic equality incompatible with key-space partitioning.
     * </p>
     */
    public static boolean canPartition(List<BlockHash.GroupSpec> groupSpecs) {
        if (groupSpecs.stream().anyMatch(gs -> gs.topNDef() != null || gs.isCategorize())) {
            return false;
        }
        if (groupSpecs.size() <= 1) {
            return true;
        }
        return groupSpecs.stream().allMatch(gs -> switch (gs.elementType()) {
            case BOOLEAN, INT, LONG, DOUBLE, BYTES_REF -> true;
            default -> false;
        });
    }

    /** Partition tag value for untagged pages; retained for {@link PartitionedHashMergeOperator} compatibility. */
    public static final int NONE_PARTITION = -1;

    /**
     * An aggregator plus the raw-input channel(s) it reads from. The channel list must not exceed
     * the aggregator's intermediate state block count so the same list can serve both raw-input and
     * intermediate-state reads during the evaluate-intermediate / {@code addIntermediateInput} path
     * in {@link PartitionedHashMergeOperator}.
     */
    public record AggregatorSpec(AggregatorFunctionSupplier supplier, List<Integer> rawChannels) {}

    public static class Builder {
        private List<BlockHash.GroupSpec> groupSpecs;
        private List<AggregatorSpec> aggregators;
        private int partitionCount = DEFAULT_PARTITION_COUNT;
        private int emitKeysThreshold = DEFAULT_EMIT_KEYS_THRESHOLD;
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

        public Builder emitKeysThreshold(int emitKeysThreshold) {
            this.emitKeysThreshold = emitKeysThreshold;
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
        private final int emitKeysThreshold;
        private final int maxPageSize;
        private final int aggregationBatchSize;

        private Factory(Builder builder) {
            var mapping = AbstractPartitionedHashAggregationOperator.buildGroupChannelMapping(builder.groupSpecs);
            this.groupChannels = mapping.groupChannels();
            this.internalGroupSpecs = mapping.internalGroupSpecs();
            this.aggregatorSpecs = java.util.Objects.requireNonNull(builder.aggregators, "aggregators");

            List<GroupingAggregator.Factory> factories = new ArrayList<>(aggregatorSpecs.size());
            List<List<Integer>> rawChannelsList = new ArrayList<>(aggregatorSpecs.size());
            int[] combinedStart = new int[aggregatorSpecs.size()];
            int nextChannel = groupChannels.size();
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
            this.emitKeysThreshold = builder.emitKeysThreshold;
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
                emitKeysThreshold,
                maxPageSize,
                aggregationBatchSize,
                driverContext
            );
        }

        @Override
        public String describe() {
            return "PartitionedHashAggregationOperator[partitionCount="
                + partitionCount
                + ", emitKeysThreshold="
                + emitKeysThreshold
                + ", aggs="
                + aggregatorSpecs.stream().map(s -> s.supplier().describe()).collect(joining(", "))
                + "]";
        }
    }

    // ---- Instance fields (beyond those inherited from HashAggregationOperator) ----

    private final List<Integer> groupChannels;
    private final List<BlockHash.GroupSpec> internalGroupSpecs;
    /** Per-aggregator raw-input channel indices; used by {@link #toRawInternalLayout}. */
    private final List<List<Integer>> aggregatorChannels;
    private final int[] combinedChannelStart;
    private final int internalPageWidth;
    private final int partitionCount;
    private final int emitKeysThreshold;
    /** Routing-only hash for computing {@code hash(key) % partitionCount}; never used for aggregation. */
    private BlockHash probeHash;

    @SuppressWarnings("this-escape")
    PartitionedHashAggregationOperator(
        List<Integer> groupChannels,
        List<BlockHash.GroupSpec> internalGroupSpecs,
        List<GroupingAggregator.Factory> aggregatorFactories,
        List<List<Integer>> aggregatorRawChannels,
        int[] combinedChannelStart,
        int internalPageWidth,
        int partitionCount,
        int emitKeysThreshold,
        int maxPageSize,
        int aggregationBatchSize,
        DriverContext driverContext
    ) {
        super(
            AggregatorMode.INITIAL,
            aggregatorFactories,
            () -> BlockHash.build(internalGroupSpecs, driverContext.blockFactory(), aggregationBatchSize, false),
            Integer.MAX_VALUE, // shouldEmitPartialResultsPeriodically() always returns false; this value is never used
            1.0,
            maxPageSize,
            null,
            driverContext
        );
        if (partitionCount <= 0) {
            throw new IllegalArgumentException("partitionCount must be greater than 0; got " + partitionCount);
        }
        if (emitKeysThreshold <= 0) {
            throw new IllegalArgumentException("emitKeysThreshold must be greater than 0; got " + emitKeysThreshold);
        }
        this.groupChannels = groupChannels;
        this.internalGroupSpecs = internalGroupSpecs;
        this.aggregatorChannels = aggregatorRawChannels;
        this.combinedChannelStart = combinedChannelStart;
        this.internalPageWidth = internalPageWidth;
        this.partitionCount = partitionCount;
        this.emitKeysThreshold = emitKeysThreshold;
        boolean success = false;
        try {
            this.probeHash = buildProbeHash(aggregationBatchSize);
            success = true;
        } finally {
            if (success == false) {
                close();
            }
        }
    }

    private BlockHash buildProbeHash(int batchSize) {
        int size = Math.min(batchSize, Operator.TARGET_PAGE_SIZE);
        BlockHash hash = BlockHash.build(internalGroupSpecs, driverContext.blockFactory(), size, false);
        if (hash.router() == null) {
            hash.close();
            hash = BlockHash.buildPackedValuesBlockHash(internalGroupSpecs, driverContext.blockFactory(), size);
        }
        return hash;
    }

    /**
     * Suppresses HAO's two-condition self-emit check (key count + uniqueness ratio). This operator
     * drives its own emit threshold — a simple key-count gate — from {@link #addInput} directly.
     */
    @Override
    protected boolean shouldEmitPartialResultsPeriodically() {
        return false;
    }

    @Override
    public void addInput(Page page) {
        List<Block> ownedPlaceholders = new ArrayList<>();
        try {
            Page internal = toRawInternalLayout(page, ownedPlaceholders);
            processPage(internal);
            if (blockHash.numKeys() >= emitKeysThreshold) {
                emit();
            }
        } finally {
            Releasables.closeExpectNoException(ownedPlaceholders.toArray(Block[]::new));
            page.releaseBlocks();
            pagesProcessed++;
            rowsReceived += page.getPositionCount();
        }
    }

    /**
     * Evaluates the accumulated table to intermediate pages, splits each by
     * {@code hash(key) % partitionCount}, and tags each non-empty partition slice with its
     * partition id. Called at the emit threshold (mid-stream) and at finish (final drain).
     * The inherited {@link HashAggregationOperator#maybeReinitializeAfterPeriodicallyEmitted()}
     * resets the hash table and aggregators at the start of the next {@link #processPage} call.
     */
    @Override
    protected void emit() {
        if (rowsAddedInCurrentBatch == 0) {
            return;
        }
        List<Page> taggedPages = new ArrayList<>();
        long emitStart = System.nanoTime();
        try {
            if (blockHash.numKeys() > 0) {
                var pageBuilder = new GroupingAggregatorPageBuilder(blockHash, aggregators, Integer.MAX_VALUE, this::customizeSelected);
                try (ReleasableIterator<Page> pages = pageBuilder.build(new GroupingAggregatorEvaluationContext(driverContext))) {
                    while (pages.hasNext()) {
                        try (Page page = pages.next()) {
                            appendTaggedPages(page, taggedPages);
                        }
                    }
                }
            }
        } finally {
            rowsAddedInCurrentBatch = 0;
            emitNanos += System.nanoTime() - emitStart;
            emitCount++;
        }
        if (taggedPages.isEmpty() == false) {
            output = new PageListIterator(taggedPages);
        }
    }

    @Override
    public void close() {
        Releasables.close(probeHash, () -> super.close());
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "[partitionCount=" + partitionCount + ", emitKeysThreshold=" + emitKeysThreshold + "]";
    }

    /**
     * Builds an internal-layout page for raw {@link AggregatorMode#INITIAL} input, remapping
     * external channels to internal positions and filling placeholder slots (those beyond an
     * aggregator's raw channel count) with constant non-null blocks.
     * <p>
     *     This prevents INITIAL-mode aggregators from misreading the placeholder as "all-null
     *     input": COUNT(*) is created with {@code combinedChannels} (so {@code countAll == false}),
     *     and reads from its first combined channel during raw processing. If that channel held an
     *     all-null group key (as produced by ENRICH with no matches), COUNT would return null from
     *     {@code prepareProcessRawInputPage} and count 0 instead of the correct row count.
     * </p>
     * <p>
     *     Caller must close every block appended to {@code owned} after the page has been
     *     consumed — those blocks are not referenced by the original page and would otherwise leak.
     * </p>
     */
    private Page toRawInternalLayout(Page page, List<Block> owned) {
        Block[] blocks = new Block[internalPageWidth];
        for (int k = 0; k < groupChannels.size(); k++) {
            blocks[k] = page.getBlock(groupChannels.get(k));
        }
        for (int a = 0; a < aggregatorChannels.size(); a++) {
            List<Integer> channels = aggregatorChannels.get(a);
            int base = combinedChannelStart[a];
            for (int j = 0; j < channels.size(); j++) {
                blocks[base + j] = page.getBlock(channels.get(j));
            }
            int totalCount = (a + 1 < combinedChannelStart.length ? combinedChannelStart[a + 1] : internalPageWidth) - base;
            for (int j = channels.size(); j < totalCount; j++) {
                // Placeholder slot: fill with a non-null constant so INITIAL-mode aggregators
                // that read this slot (e.g. COUNT(*)) count all rows rather than 0.
                Block sentinel = driverContext.blockFactory().newConstantIntBlockWith(0, page.getPositionCount());
                owned.add(sentinel);
                blocks[base + j] = sentinel;
            }
        }
        return new Page(blocks);
    }

    /**
     * Splits {@code intermediatePage} by partition hash and appends a tagged sub-page for each
     * non-empty partition to {@code out}. Ownership of the sub-pages is transferred to {@code out};
     * the caller retains (and is responsible for releasing) {@code intermediatePage}.
     */
    private void appendTaggedPages(Page intermediatePage, List<Page> out) {
        int positions = intermediatePage.getPositionCount();
        int[] partitionOf = new int[positions];
        int[] counts = new int[partitionCount];
        AbstractPartitionedHashAggregationOperator.fillPartitionAssignments(
            probeHash,
            internalGroupSpecs.size(),
            intermediatePage,
            partitionCount,
            partitionOf,
            counts
        );
        AbstractPartitionedHashAggregationOperator.BucketSort sorted = AbstractPartitionedHashAggregationOperator.sortPositionsByPartition(
            partitionOf,
            counts,
            partitionCount
        );
        for (int p = 0; p < partitionCount; p++) {
            int start = sorted.offsets()[p], end = sorted.offsets()[p + 1];
            if (start == end) {
                continue;
            }
            Page subPage = intermediatePage.filter(false, sorted.sortedPositions(), start, end - start);
            Page tagged = subPage.withPartitionId(p);
            subPage.releaseBlocks();
            out.add(tagged);
        }
    }

    /**
     * Iterates over a pre-collected list of tagged pages, releasing unconsumed pages on close.
     */
    private static final class PageListIterator implements ReleasableIterator<Page> {
        private final List<Page> pages;
        private int index;

        PageListIterator(List<Page> pages) {
            this.pages = pages;
        }

        @Override
        public boolean hasNext() {
            return index < pages.size();
        }

        @Override
        public Page next() {
            if (hasNext() == false) {
                throw new NoSuchElementException();
            }
            return pages.get(index++);
        }

        @Override
        public void close() {
            for (int i = index; i < pages.size(); i++) {
                pages.get(i).releaseBlocks();
            }
        }
    }
}
