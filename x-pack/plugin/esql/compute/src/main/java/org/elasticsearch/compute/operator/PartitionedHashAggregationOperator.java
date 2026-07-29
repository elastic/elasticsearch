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
import org.elasticsearch.compute.aggregation.blockhash.BlockHash;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.ReleasableIterator;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;

/**
 * Aggregates raw input {@link Page}s into partitioned intermediate output using a single hash table.
 * <p>
 *     Accumulates all input in one {@link HashAggregationOperator} table. When the table's unique
 *     key count reaches {@link #emitKeysThreshold}, evaluates the table to intermediate pages,
 *     splits each page by {@code hash(key) % partitionCount}, tags each sub-page with its
 *     partition id, emits those tagged pages, then resets the table to accumulate again.
 * </p>
 * <p>
 *     Only supports {@link AggregatorMode#INITIAL} (raw input, partial output).
 * </p>
 * <p>
 *     The companion {@link PartitionedHashMergeOperator} receives the tagged pages and merges
 *     each partition independently in a background worker.
 * </p>
 */
public class PartitionedHashAggregationOperator extends AbstractPartitionedHashAggregationOperator {

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
            this.aggregatorSpecs = requireNonNull(builder.aggregators, "aggregators");

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

    private final List<GroupingAggregator.Factory> aggregatorFactories;
    private final int emitKeysThreshold;
    private final int aggregationBatchSize;

    /** The single accumulation table. Replaced by a fresh instance after each intermediate emit. */
    private HashAggregationOperator singleOp;

    private long emitNanos;
    private long emitCount;
    private long savedHashNanos;
    private long savedAggNanos;
    private int pagesProcessed;
    private long rowsReceived;
    private long rowsEmitted;

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
        if (emitKeysThreshold <= 0) {
            throw new IllegalArgumentException("emitKeysThreshold must be greater than 0; got " + emitKeysThreshold);
        }
        this.aggregatorFactories = aggregatorFactories;
        this.emitKeysThreshold = emitKeysThreshold;
        this.aggregationBatchSize = aggregationBatchSize;
        boolean success = false;
        try {
            this.singleOp = newOp();
            this.probeHash = buildProbeHash();
            success = true;
        } finally {
            if (success == false) {
                close();
            }
        }
    }

    /**
     * Builds the routing-only {@link BlockHash} used at emit time to assign each output row to a
     * partition. Tries the specialized hash first (exposes a {@link BlockHash.Router} for the schema),
     * then falls back to {@code PackedValuesBlockHash} for multi-column schemas that need it.
     */
    private BlockHash buildProbeHash() {
        int batchSize = Math.min(aggregationBatchSize, Operator.TARGET_PAGE_SIZE);
        BlockHash hash = BlockHash.build(internalGroupSpecs, driverContext.blockFactory(), batchSize, false);
        if (hash.router() == null) {
            hash.close();
            hash = BlockHash.buildPackedValuesBlockHash(internalGroupSpecs, driverContext.blockFactory(), batchSize);
        }
        return hash;
    }

    @Override
    public void addInput(Page page) {
        try {
            checkState(needsInput(), "Operator is already finishing");
            requireNonNull(page, "page is null");
            List<Block> ownedPlaceholders = new ArrayList<>();
            try {
                Page internal = toRawInternalLayout(page, ownedPlaceholders);
                singleOp.processPage(internal);
                if (singleOp.blockHash.numKeys() >= emitKeysThreshold) {
                    emitIntermediate();
                }
            } finally {
                Releasables.closeExpectNoException(ownedPlaceholders.toArray(Block[]::new));
            }
        } finally {
            page.releaseBlocks();
            pagesProcessed++;
            rowsReceived += page.getPositionCount();
        }
    }

    /**
     * Builds an internal-layout page for raw {@link AggregatorMode#INITIAL} input, similar to
     * {@link #toInternalLayout} but replacing placeholder slots (those beyond an aggregator's
     * raw channel count) with constant non-null blocks rather than the group key block.
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
        emitIntermediate();
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
        Releasables.close(singleOp, probeHash, output);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "[partitionCount=" + partitionCount + ", emitKeysThreshold=" + emitKeysThreshold + "]";
    }

    /**
     * Evaluates the single table to tagged intermediate pages and resets the table. Each
     * intermediate page is split by {@code hash(key) % partitionCount}; each non-empty partition
     * gets its own sub-page tagged with its partition id. Called at emit threshold (mid-stream)
     * and at finish (final drain).
     */
    private void emitIntermediate() {
        if (singleOp.blockHash.numKeys() == 0) {
            return;
        }
        List<Page> taggedPages = new ArrayList<>();
        long emitStart = System.nanoTime();
        try {
            // Use Integer.MAX_VALUE so all groups come out in a single pass; the per-partition
            // sub-pages produced by the split are each naturally bounded to ~numKeys/partitionCount rows.
            try (ReleasableIterator<Page> pages = evaluateOp(singleOp, Integer.MAX_VALUE)) {
                while (pages.hasNext()) {
                    try (Page page = pages.next()) {
                        appendTaggedPages(page, taggedPages);
                    }
                }
            }
        } finally {
            emitNanos += System.nanoTime() - emitStart;
            emitCount++;
        }
        saveOpTiming(singleOp);
        singleOp.close();
        singleOp = finishCalled ? null : newOp();
        if (taggedPages.isEmpty() == false) {
            output = new PageListIterator(taggedPages);
        }
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
        fillPartitionAssignments(intermediatePage, partitionCount, partitionOf, counts);
        BucketSort sorted = sortPositionsByPartition(partitionOf, counts, partitionCount);
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

    private void saveOpTiming(HashAggregationOperator op) {
        HashAggregationOperator.Status s = (HashAggregationOperator.Status) op.status();
        savedHashNanos += s.hashNanos();
        savedAggNanos += s.aggregationNanos();
    }

    private HashAggregationOperator newOp() {
        return new HashAggregationOperator(
            AggregatorMode.INITIAL,
            aggregatorFactories,
            () -> BlockHash.build(internalGroupSpecs, driverContext.blockFactory(), aggregationBatchSize, false),
            Integer.MAX_VALUE, // disable self-emit; this operator manages its own emit threshold
            1.0,
            maxPageSize,
            null,
            driverContext
        );
    }

    @Override
    public Operator.Status status() {
        long hashNanos = savedHashNanos;
        long aggNanos = savedAggNanos;
        if (singleOp != null) {
            HashAggregationOperator.Status s = (HashAggregationOperator.Status) singleOp.status();
            hashNanos += s.hashNanos();
            aggNanos += s.aggregationNanos();
        }
        return new Status(emitNanos, emitCount, hashNanos, aggNanos, pagesProcessed, rowsReceived, rowsEmitted);
    }

    public static class Status implements Operator.Status {
        public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
            Operator.Status.class,
            "partitioned_hash_agg",
            Status::new
        );

        private final long emitNanos;
        private final long emitCount;
        private final long hashNanos;
        private final long aggregationNanos;
        private final int pagesProcessed;
        private final long rowsReceived;
        private final long rowsEmitted;

        public Status(
            long emitNanos,
            long emitCount,
            long hashNanos,
            long aggregationNanos,
            int pagesProcessed,
            long rowsReceived,
            long rowsEmitted
        ) {
            this.emitNanos = emitNanos;
            this.emitCount = emitCount;
            this.hashNanos = hashNanos;
            this.aggregationNanos = aggregationNanos;
            this.pagesProcessed = pagesProcessed;
            this.rowsReceived = rowsReceived;
            this.rowsEmitted = rowsEmitted;
        }

        public Status(StreamInput in) throws IOException {
            emitNanos = in.readVLong();
            emitCount = in.readVLong();
            hashNanos = in.readVLong();
            aggregationNanos = in.readVLong();
            pagesProcessed = in.readVInt();
            rowsReceived = in.readVLong();
            rowsEmitted = in.readVLong();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVLong(emitNanos);
            out.writeVLong(emitCount);
            out.writeVLong(hashNanos);
            out.writeVLong(aggregationNanos);
            out.writeVInt(pagesProcessed);
            out.writeVLong(rowsReceived);
            out.writeVLong(rowsEmitted);
        }

        @Override
        public String getWriteableName() {
            return ENTRY.name;
        }

        @Override
        public TransportVersion getMinimalSupportedVersion() {
            return TransportVersion.minimumCompatible();
        }

        public long emitNanos() {
            return emitNanos;
        }

        public long emitCount() {
            return emitCount;
        }

        public long hashNanos() {
            return hashNanos;
        }

        public long aggregationNanos() {
            return aggregationNanos;
        }

        public int pagesProcessed() {
            return pagesProcessed;
        }

        public long rowsReceived() {
            return rowsReceived;
        }

        public long rowsEmitted() {
            return rowsEmitted;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Status status = (Status) o;
            return emitNanos == status.emitNanos
                && emitCount == status.emitCount
                && hashNanos == status.hashNanos
                && aggregationNanos == status.aggregationNanos
                && pagesProcessed == status.pagesProcessed
                && rowsReceived == status.rowsReceived
                && rowsEmitted == status.rowsEmitted;
        }

        @Override
        public int hashCode() {
            return java.util.Objects.hash(emitNanos, emitCount, hashNanos, aggregationNanos, pagesProcessed, rowsReceived, rowsEmitted);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("emit_nanos", emitNanos);
            if (builder.humanReadable()) {
                builder.field("emit_time", TimeValue.timeValueNanos(emitNanos));
            }
            builder.field("emit_count", emitCount);
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
