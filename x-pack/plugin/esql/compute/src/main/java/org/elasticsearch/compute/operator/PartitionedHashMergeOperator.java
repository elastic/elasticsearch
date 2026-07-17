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
 * Coordinator-side merge operator for partitioned hash aggregation (Phase 3).
 *
 * <p>Receives intermediate {@link Page}s from data nodes — some tagged with a
 * {@link Page#partitionId()} by {@link PartitionedHashAggregationOperator}, others untagged
 * (from shards that never converted to partitioned mode). Produces final aggregated output.
 *
 * <h2>Lifecycle</h2>
 * <ol>
 *   <li><b>Pre-promotion</b> – all incoming pages, tagged or not, accumulate in a single
 *       {@code NONE} table (INTERMEDIATE-mode aggregators). On first tagged page the Driver
 *       calls {@link #tryPromote}, which allocates one FINAL-mode worker table per partition.</li>
 *   <li><b>Post-promotion</b> – tagged pages route directly to their owning worker table by
 *       partition id; untagged pages continue accumulating in the NONE table.</li>
 *   <li><b>Finish</b> – the NONE table's contents are distributed to workers via the same
 *       evaluate-intermediate → split-by-partition-hash → addIntermediateInput primitive used
 *       by {@link PartitionedHashAggregationOperator}'s conversion step; each worker then
 *       evaluates to final output (disjoint, no k-way merge required).</li>
 * </ol>
 *
 * <p>Only supports {@link AggregatorMode#FINAL}: the coordinator receives INITIAL/INTERMEDIATE
 * output from data nodes and produces final results for the user.
 */
public class PartitionedHashMergeOperator implements Operator {

    /** Partition assigned to rows whose grouping key contains a null. */
    private static final int NULL_PARTITION = 0;

    private static final GroupingAggregatorPageBuilder.CustomizeSelected NO_CUSTOMIZATION = (aggregator, selected) -> {
        selected.incRef();
        return selected;
    };

    /**
     * Aggregator specification: supplier plus the <em>external</em> intermediate-state channels
     * that appear in pages arriving from the exchange (key columns excluded).
     */
    public record AggregatorSpec(AggregatorFunctionSupplier supplier, List<Integer> channels) {}

    // ---- Builder / Factory ----

    public static class Builder {
        private List<BlockHash.GroupSpec> groupSpecs;
        private List<AggregatorSpec> aggregators;
        private int partitionCount = PartitionedHashAggregationOperator.DEFAULT_PARTITION_COUNT;
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
        private final List<GroupingAggregator.Factory> noneAggFactories;
        private final List<GroupingAggregator.Factory> workerAggFactories;
        private final int[] combinedChannelStart;
        private final int internalPageWidth;
        private final int partitionCount;
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

            List<GroupingAggregator.Factory> noneFactories = new ArrayList<>(aggregatorSpecs.size());
            List<GroupingAggregator.Factory> workerFactories = new ArrayList<>(aggregatorSpecs.size());
            int[] combinedStart = new int[aggregatorSpecs.size()];
            int nextChannel = groupChannels.size();
            for (int i = 0; i < aggregatorSpecs.size(); i++) {
                AggregatorSpec spec = aggregatorSpecs.get(i);
                int intermediateBlockCount = spec.supplier().groupingIntermediateStateDesc().size();
                List<Integer> internalChannels = new ArrayList<>(intermediateBlockCount);
                for (int c = 0; c < intermediateBlockCount; c++) {
                    internalChannels.add(nextChannel + c);
                }
                combinedStart[i] = nextChannel;
                List<Integer> frozenChannels = List.copyOf(internalChannels);
                noneFactories.add(new GroupingAggregator.Factory() {
                    @Override
                    public GroupingAggregator apply(DriverContext driverContext) {
                        return new GroupingAggregator(
                            spec.supplier().groupingAggregator(driverContext, frozenChannels),
                            AggregatorMode.INTERMEDIATE
                        );
                    }

                    @Override
                    public String describe() {
                        return spec.supplier().describe();
                    }
                });
                workerFactories.add(new GroupingAggregator.Factory() {
                    @Override
                    public GroupingAggregator apply(DriverContext driverContext) {
                        return new GroupingAggregator(
                            spec.supplier().groupingAggregator(driverContext, frozenChannels),
                            AggregatorMode.FINAL
                        );
                    }

                    @Override
                    public String describe() {
                        return spec.supplier().describe();
                    }
                });
                nextChannel += intermediateBlockCount;
            }
            this.noneAggFactories = List.copyOf(noneFactories);
            this.workerAggFactories = List.copyOf(workerFactories);
            this.combinedChannelStart = combinedStart;
            this.internalPageWidth = nextChannel;
            this.partitionCount = builder.partitionCount;
            this.maxPageSize = builder.maxPageSize;
            this.aggregationBatchSize = builder.aggregationBatchSize;
        }

        @Override
        public PartitionedHashMergeOperator get(DriverContext driverContext) {
            return new PartitionedHashMergeOperator(
                groupChannels,
                internalGroupSpecs,
                aggregatorSpecs,
                noneAggFactories,
                workerAggFactories,
                combinedChannelStart,
                internalPageWidth,
                partitionCount,
                maxPageSize,
                aggregationBatchSize,
                driverContext
            );
        }

        @Override
        public String describe() {
            return "PartitionedHashMergeOperator[partitionCount="
                + partitionCount
                + ", aggs="
                + aggregatorSpecs.stream().map(s -> s.supplier().describe()).collect(joining(", "))
                + "]";
        }
    }

    // ---- Instance fields ----

    private final List<Integer> groupChannels;
    private final List<BlockHash.GroupSpec> internalGroupSpecs;
    private final List<AggregatorSpec> aggregatorSpecs;
    private final List<GroupingAggregator.Factory> noneAggFactories;
    private final List<GroupingAggregator.Factory> workerAggFactories;
    private final int[] combinedChannelStart;
    private final int internalPageWidth;
    private final int partitionCount;
    private final int maxPageSize;
    private final int aggregationBatchSize;
    private final DriverContext driverContext;

    /** Accumulates all untagged pages, and all pages before promotion. INTERMEDIATE mode. */
    private Table noneTable;
    /** One FINAL-mode table per partition; null until promoted. */
    private Table[] workerTables;

    /** True once {@link #tryPromote} has created {@link #workerTables}. */
    private boolean promoted;
    /** Set in {@link #addInput} when a tagged page is seen before promotion. */
    private boolean needsPromotion;

    private boolean finished;
    private ReleasableIterator<Page> output;

    @SuppressWarnings("this-escape")
    PartitionedHashMergeOperator(
        List<Integer> groupChannels,
        List<BlockHash.GroupSpec> internalGroupSpecs,
        List<AggregatorSpec> aggregatorSpecs,
        List<GroupingAggregator.Factory> noneAggFactories,
        List<GroupingAggregator.Factory> workerAggFactories,
        int[] combinedChannelStart,
        int internalPageWidth,
        int partitionCount,
        int maxPageSize,
        int aggregationBatchSize,
        DriverContext driverContext
    ) {
        this.groupChannels = groupChannels;
        this.internalGroupSpecs = internalGroupSpecs;
        this.aggregatorSpecs = aggregatorSpecs;
        this.noneAggFactories = noneAggFactories;
        this.workerAggFactories = workerAggFactories;
        this.combinedChannelStart = combinedChannelStart;
        this.internalPageWidth = internalPageWidth;
        this.partitionCount = partitionCount;
        this.maxPageSize = maxPageSize;
        this.aggregationBatchSize = aggregationBatchSize;
        this.driverContext = driverContext;
        boolean success = false;
        try {
            this.noneTable = newTable(noneAggFactories);
            success = true;
        } finally {
            if (success == false) {
                close();
            }
        }
    }

    // ---- Operator interface ----

    @Override
    public boolean needsInput() {
        return output == null && finished == false;
    }

    @Override
    public void addInput(Page page) {
        try {
            checkState(needsInput(), "Operator is already finishing");
            requireNonNull(page, "page is null");
            Page internal = toInternalLayout(page);
            Integer partitionId = page.partitionId();
            if (promoted == false || partitionId == null) {
                mergeIntermediateIntoTable(noneTable, internal);
                if (partitionId != null) {
                    needsPromotion = true;
                }
            } else {
                assert partitionId >= 0 && partitionId < partitionCount
                    : "partitionId " + partitionId + " out of range [0, " + partitionCount + ")";
                mergeIntermediateIntoTable(workerTables[partitionId], internal);
            }
        } finally {
            page.releaseBlocks();
        }
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
    public boolean isFinished() {
        return finished && output == null;
    }

    @Override
    public boolean canProduceMoreDataWithoutExtraInput() {
        return output != null;
    }

    @Override
    public Operator tryPromote(DriverContext driverContext) {
        if (needsPromotion && promoted == false) {
            workerTables = new Table[partitionCount];
            boolean success = false;
            try {
                for (int p = 0; p < partitionCount; p++) {
                    workerTables[p] = newTable(workerAggFactories);
                }
                success = true;
            } finally {
                if (success == false) {
                    if (workerTables != null) {
                        Releasables.close(workerTables);
                    }
                    workerTables = null;
                }
            }
            promoted = true;
            needsPromotion = false;
        }
        return this;
    }

    @Override
    public void close() {
        Releasables.close(noneTable, output);
        if (workerTables != null) {
            Releasables.close(workerTables);
        }
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "[promoted=" + promoted + ", partitionCount=" + partitionCount + "]";
    }

    // ---- finish() helpers ----

    private void emitFinal() {
        if (promoted) {
            // 1. Distribute the NONE table's intermediate contents to the right partition workers.
            reconcileNone();
            // 2. Collect non-empty workers; evaluate each to final output.
            List<ReleasableIterator<Page>> parts = new ArrayList<>();
            for (int p = 0; p < workerTables.length; p++) {
                Table worker = workerTables[p];
                workerTables[p] = null;
                if (worker.blockHash.numKeys() > 0) {
                    parts.add(closeTableOnClose(evaluateTable(worker), worker));
                } else {
                    worker.close();
                }
            }
            workerTables = null;
            if (parts.isEmpty() == false) {
                output = new ConcatenatingPageIterator(parts);
            }
        } else {
            // Non-promoted path: single FINAL worker, merge NONE's INTERMEDIATE output into it.
            Table finalTable = newTable(workerAggFactories);
            boolean success = false;
            try {
                try (ReleasableIterator<Page> nonePages = evaluateTable(noneTable)) {
                    while (nonePages.hasNext()) {
                        try (Page p = nonePages.next()) {
                            mergeIntermediateIntoTable(finalTable, p);
                        }
                    }
                }
                success = true;
            } finally {
                noneTable.close();
                noneTable = null;
                if (success == false) {
                    finalTable.close();
                }
            }
            if (finalTable.blockHash.numKeys() > 0) {
                output = closeTableOnClose(evaluateTable(finalTable), finalTable);
            } else {
                finalTable.close();
            }
        }
    }

    /**
     * Evaluates the NONE table (INTERMEDIATE mode → intermediate pages), splits each intermediate
     * page by partition hash, and merges each split into the owning worker table (FINAL mode).
     * Closes and nulls {@link #noneTable} on return.
     */
    private void reconcileNone() {
        if (noneTable.blockHash.numKeys() == 0) {
            noneTable.close();
            noneTable = null;
            return;
        }
        try (ReleasableIterator<Page> nonePages = evaluateTable(noneTable)) {
            while (nonePages.hasNext()) {
                try (Page p = nonePages.next()) {
                    distributeIntermediatePage(p);
                }
            }
        }
        noneTable.close();
        noneTable = null;
    }

    /**
     * Distributes a single intermediate page from the NONE table across {@link #workerTables}
     * by computing each row's partition using the first worker's {@link BlockHash.Router}. The
     * page's key columns are at channels 0..keyCount-1 (internal layout from {@link #evaluateTable}).
     */
    private void distributeIntermediatePage(Page intermediatePage) {
        int positions = intermediatePage.getPositionCount();
        int keyCount = internalGroupSpecs.size();
        BlockHash.Router probeRouter = workerTables[0].blockHash.router();
        if (probeRouter == null) {
            // Router unsupported for this grouping shape: merge everything into partition 0.
            mergeIntermediateIntoTable(workerTables[NULL_PARTITION], intermediatePage);
            return;
        }
        int[] partitionOf = new int[positions];
        for (int i = 0; i < positions; i++) {
            boolean anyNull = false;
            for (int k = 0; k < keyCount; k++) {
                if (intermediatePage.getBlock(k).isNull(i)) {
                    anyNull = true;
                    break;
                }
            }
            partitionOf[i] = anyNull ? NULL_PARTITION : Math.floorMod(probeRouter.partitionHashOfRow(intermediatePage, i), partitionCount);
        }
        for (int p = 0; p < partitionCount; p++) {
            int count = 0;
            for (int i = 0; i < positions; i++) {
                if (partitionOf[i] == p) {
                    count++;
                }
            }
            if (count == 0) {
                continue;
            }
            int[] filterPositions = new int[count];
            int idx = 0;
            for (int i = 0; i < positions; i++) {
                if (partitionOf[i] == p) {
                    filterPositions[idx++] = i;
                }
            }
            try (Page subPage = intermediatePage.filter(false, filterPositions)) {
                mergeIntermediateIntoTable(workerTables[p], subPage);
            }
        }
    }

    /**
     * Merges an intermediate-format page into {@code table} by re-hashing the key column(s) to
     * get local group ids, then feeding each aggregator's intermediate state to
     * {@code prepareProcessIntermediateInputPage}. Works regardless of {@code table}'s
     * aggregator mode (INTERMEDIATE or FINAL), since both consume intermediate state.
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

    /**
     * Evaluates {@code table} to pages reflecting its current state. For INTERMEDIATE-mode
     * tables this produces intermediate pages suitable for re-merging; for FINAL-mode tables
     * this produces final output pages.
     */
    private ReleasableIterator<Page> evaluateTable(Table table) {
        var pageBuilder = new GroupingAggregatorPageBuilder(table.blockHash, table.aggregators, maxPageSize, NO_CUSTOMIZATION);
        return pageBuilder.build(new GroupingAggregatorEvaluationContext(driverContext));
    }

    /**
     * Rearranges {@code page} from the exchange's external channel layout to this operator's
     * internal layout: grouping keys at channels 0..keyCount-1, each aggregator's intermediate
     * state blocks at {@link #combinedChannelStart}[i]..combinedChannelStart[i]+blockCount-1.
     * Never releases the original blocks; {@code page} remains the sole owner.
     */
    private Page toInternalLayout(Page page) {
        Block[] blocks = new Block[internalPageWidth];
        Arrays.fill(blocks, page.getBlock(groupChannels.get(0)));
        for (int k = 0; k < groupChannels.size(); k++) {
            blocks[k] = page.getBlock(groupChannels.get(k));
        }
        for (int a = 0; a < aggregatorSpecs.size(); a++) {
            List<Integer> externalChannels = aggregatorSpecs.get(a).channels();
            int base = combinedChannelStart[a];
            for (int j = 0; j < externalChannels.size(); j++) {
                blocks[base + j] = page.getBlock(externalChannels.get(j));
            }
        }
        return new Page(blocks);
    }

    // ---- Table helpers ----

    private Table newTable(List<GroupingAggregator.Factory> factories) {
        BlockHash blockHash = BlockHash.build(internalGroupSpecs, driverContext.blockFactory(), aggregationBatchSize, false);
        boolean success = false;
        try {
            List<GroupingAggregator> aggregators = new ArrayList<>(factories.size());
            for (GroupingAggregator.Factory f : factories) {
                aggregators.add(f.apply(driverContext));
            }
            Table table = new Table(blockHash, aggregators);
            success = true;
            return table;
        } finally {
            if (success == false) {
                blockHash.close();
            }
        }
    }

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

    private static void checkState(boolean condition, String msg) {
        if (condition == false) {
            throw new IllegalArgumentException(msg);
        }
    }

    // ---- Inner types ----

    private static final class Table implements Releasable {
        final BlockHash blockHash;
        final List<GroupingAggregator> aggregators;

        Table(BlockHash blockHash, List<GroupingAggregator> aggregators) {
            this.blockHash = blockHash;
            this.aggregators = aggregators;
        }

        @Override
        public void close() {
            Releasables.close(blockHash, () -> Releasables.close(aggregators));
        }
    }

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

    /**
     * Lazily concatenates multiple {@link ReleasableIterator}s of pages without modification,
     * closing each source when exhausted.
     */
    private static final class ConcatenatingPageIterator implements ReleasableIterator<Page> {
        private final Deque<ReleasableIterator<Page>> sources;

        ConcatenatingPageIterator(List<ReleasableIterator<Page>> sources) {
            this.sources = new ArrayDeque<>(sources);
        }

        @Override
        public boolean hasNext() {
            while (sources.isEmpty() == false && sources.peekFirst().hasNext() == false) {
                sources.pollFirst().close();
            }
            return sources.isEmpty() == false;
        }

        @Override
        public Page next() {
            if (hasNext() == false) {
                throw new NoSuchElementException();
            }
            return sources.peekFirst().next();
        }

        @Override
        public void close() {
            for (ReleasableIterator<Page> source : sources) {
                source.close();
            }
        }
    }
}
