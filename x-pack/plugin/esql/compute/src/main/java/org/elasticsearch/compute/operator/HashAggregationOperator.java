/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.compute.Describable;
import org.elasticsearch.compute.aggregation.AggregatorMode;
import org.elasticsearch.compute.aggregation.FilteredGroupingAggregatorFunction;
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
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.analysis.AnalysisRegistry;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;

/**
 * Aggregates input {@link Page}s into many output rows, grouping by some values.
 * <p>
 *     Receives input pages until we call {@link #finish()}, telling it
 *     there are no more pages. Each aggregation is an instance
 *     of {@link GroupingAggregatorFunction}.
 * </p>
 * <p>
 *     For
 *     {@snippet lang="esql" :
 *     | STATS MIN(discovered), MAX(discovered) BY class
 *     }
 *     this'd look like:
 * </p>
 * {@snippet lang="txt" :
 * Before first page:
 *                                     ┌─────────────────┬─────────────────┬──────────┐
 *                                     │ MIN(discovered) │ MAX(discovered) │ class    │
 *                                     ├─────────────────┼─────────────────┼──────────┤
 *                                     └─────────────────┴─────────────────┴──────────┘
 *
 * First page:
 * ┌──────┬──────────┬────────────┐    ┌─────────────────┬─────────────────┬──────────┐
 * │  ref │ class    │ discovered │    │ MIN(discovered) │ MAX(discovered) │ class    │
 * ├──────┼──────────┼────────────┤    ├─────────────────┼─────────────────┼──────────┤
 * │  079 │ Euclid   │ 1988-01-01 │ -> │ 1988-01-01      │ 1993-01-01      │ Euclid   │
 * │  173 │ Euclid   │ 1993-01-01 │    │ 1967-01-01      │ 1967-01-01      │ Keter    │
 * │ 1313 │ Keter    │ 1967-01-01 │    │ 1991-01-01      │ 1991-01-01      │ Safe     │
 * │ 1981 │ Safe     │ 1991-01-01 │    └─────────────────┴─────────────────┴──────────┘
 * └──────┴──────────┴────────────┘
 *
 * Second page:
 * ┌──────┬──────────┬────────────┐    ┌─────────────────┬─────────────────┬──────────┐
 * │  ref │ class    │ discovered │    │ MIN(discovered) │ MAX(discovered) │ class    │
 * ├──────┼──────────┼────────────┤    ├─────────────────┼─────────────────┼──────────┤
 * │ 2317 │ Keter    │ 1922-01-01 │ -> │ 1988-01-01      │ 2010-01-01      │ Euclid   │
 * │ 2639 │ Euclid   │ 2010-01-01 │    │ 1922-01-01      │ 2016-04-28      │ Keter    │
 * │ 2935 │ Keter    │ 2016-04-28 │    │ 1991-01-01      │ 1991-01-01      │ Safe     │
 * │ 3000 │ Thaumiel │ 1971-01-01 │    │ 1971-01-01      │ 1971-01-01      │ Thaumiel │
 * └──────┴──────────┴────────────┘    └─────────────────┴─────────────────┴──────────┘
 *
 * Third page:
 * ┌──────┬──────────┬────────────┐    ┌─────────────────┬─────────────────┬──────────┐
 * │  ref │ class    │ discovered │    │ MIN(discovered) │ MAX(discovered) │ class    │
 * ├──────┼──────────┼────────────┤    ├─────────────────┼─────────────────┼──────────┤
 * │ 3001 │ Euclid   │ 2000-01-02 │ -> │ 1988-01-01      │ 2010-01-01      │ Euclid   │
 * │ 4999 │ Keter    │ 1998-11-27 │    │ 1922-01-01      │ 2016-04-28      │ Keter    │
 * │ 5000 │ Safe     │ 2020-12-04 │    │ 1991-01-01      │ 2020-12-04      │ Safe     │
 * └──────┴──────────┴────────────┘    │ 1971-01-01      │ 1971-01-01      │ Thaumiel │
 *                                     └─────────────────┴─────────────────┴──────────┘
 *
 * finish():
 *                                     ┌─────────────────┬─────────────────┬──────────┐
 *                                     │ MIN(discovered) │ MAX(discovered) │ class    │
 *                                     ├─────────────────┼─────────────────┼──────────┤
 *                                     │ 1988-01-01      │ 2010-01-01      │ Euclid   │
 *                                     │ 1922-01-01      │ 2016-04-28      │ Keter    │
 *                                     │ 1991-01-01      │ 2020-12-04      │ Safe     │
 *                                     │ 1971-01-01      │ 1971-01-01      │ Thaumiel │
 *                                     └─────────────────┴─────────────────┴──────────┘
 * }
 * <p>
 *     Aggregations can also filter which rows they receive using
 *     {@link FilteredGroupingAggregatorFunction}. In ESQL that looks like:
 *     {@snippet lang="esql" :
 *     | STATS min3 = MIN(discovered) WHERE LENGTH(ref) == 3,
 *             max3 = MAX(discovered) WHERE LENGTH(ref) == 3,
 *             min4 = MIN(discovered) WHERE LENGTH(ref) == 4
 *          BY class
 *     }
 *     this'd look like:
 * </p>
 * {@snippet lang="txt" :
 * Before first page:
 *                                     ┌────────────┬────────────┬────────────┬──────────┐
 *                                     │ min3       │ max3       │ min4       │ class    │
 *                                     ├────────────┼────────────┼────────────┼──────────┤
 *                                     └────────────┴────────────┴────────────┴──────────┘
 *
 * First page:
 * ┌──────┬──────────┬────────────┐    ┌────────────┬────────────┬────────────┬──────────┐
 * │  ref │ class    │ discovered │    │ min3       │ max3       │ min4       │ class    │
 * ├──────┼──────────┼────────────┤    ├────────────┼────────────┼────────────┼──────────┤
 * │  079 │ Euclid   │ 1988-01-01 │ -> │ 1988-01-01 │ 1993-01-01 │ null       │ Euclid   │
 * │  173 │ Euclid   │ 1993-01-01 │    │ null       │ null       │ 1967-01-01 │ Keter    │
 * │ 1313 │ Keter    │ 1967-01-01 │    │ null       │ null       │ 1991-01-01 │ Safe     │
 * │ 1981 │ Safe     │ 1991-01-01 │    └────────────┴────────────┴────────────┴──────────┘
 * └──────┴──────────┴────────────┘
 *
 * Second page:
 * ┌──────┬──────────┬────────────┐    ┌────────────┬────────────┬────────────┬──────────┐
 * │  ref │ class    │ discovered │    │ min3       │ max3       │ min4       │ class    │
 * ├──────┼──────────┼────────────┤    ├────────────┼────────────┼────────────┼──────────┤
 * │ 2317 │ Keter    │ 1922-01-01 │ -> │ 1988-01-01 │ 1993-01-01 │ 2010-01-01 │ Euclid   │
 * │ 2639 │ Euclid   │ 2010-01-01 │    │ null       │ null       │ 1922-01-01 │ Keter    │
 * │ 2935 │ Keter    │ 2016-04-28 │    │ null       │ null       │ 1991-01-01 │ Safe     │
 * │ 3000 │ Thaumiel │ 1971-01-01 │    │ null       │ null       │ 1971-01-01 │ Thaumiel │
 * └──────┴──────────┴────────────┘    └────────────┴────────────┴────────────┴──────────┘
 *
 * Third page:
 * ┌──────┬──────────┬────────────┐    ┌────────────┬────────────┬────────────┬──────────┐
 * │  ref │ class    │ discovered │    │ min3       │ max3       │ min4       │ class    │
 * ├──────┼──────────┼────────────┤    ├────────────┼────────────┼────────────┼──────────┤
 * │ 3001 │ Euclid   │ 2000-01-02 │ -> │ 1988-01-01 │ 1993-01-01 │ 2000-01-02 │ Euclid   │
 * │ 4999 │ Keter    │ 1998-11-27 │    │ null       │ null       │ 1922-01-01 │ Keter    │
 * │ 5000 │ Safe     │ 2020-12-04 │    │ null       │ null       │ 1991-01-01 │ Safe     │
 * └──────┴──────────┴────────────┘    │ null       │ null       │ 1971-01-01 │ Thaumiel │
 *                                     └────────────┴────────────┴────────────┴──────────┘
 *
 * finish():
 *                                     ┌────────────┬────────────┬────────────┬──────────┐
 *                                     │ min3       │ max3       │ min4       │ class    │
 *                                     ├────────────┼────────────┼────────────┼──────────┤
 *                                     │ 1988-01-01 │ 1993-01-01 │ 2000-01-02 │ Euclid   │
 *                                     │ null       │ null       │ 1922-01-01 │ Keter    │
 *                                     │ null       │ null       │ 1991-01-01 │ Safe     │
 *                                     │ null       │ null       │ 1971-01-01 │ Thaumiel │
 *                                     └────────────┴────────────┴────────────┴──────────┘
 * }
 */
public class HashAggregationOperator implements Operator {

    public static final int DEFAULT_PARTIAL_EMIT_KEYS_THRESHOLD = 100_000;
    public static final double DEFAULT_PARTIAL_EMIT_UNIQUENESS_THRESHOLD = 0.1;

    /**
     * Builder for {@link HashAggregationOperator}. {@link #groups(List)}, {@link #mode(AggregatorMode)},
     * and {@link #aggregators(List)} are required. The other parameters default to reasonable values
     * <strong>for tests</strong>. In production, set them all.
     */
    public static class Builder {
        private List<BlockHash.GroupSpec> groups;
        private AggregatorMode aggregatorMode;
        private List<GroupingAggregator.Factory> aggregators;
        private int partialEmitKeysThreshold = DEFAULT_PARTIAL_EMIT_KEYS_THRESHOLD;
        private double partialEmitUniquenessThreshold = DEFAULT_PARTIAL_EMIT_UNIQUENESS_THRESHOLD;
        private int maxPageSize = Operator.TARGET_PAGE_SIZE / Long.SIZE;
        private int aggregationBatchSize = Operator.TARGET_PAGE_SIZE / Long.SIZE;
        private AnalysisRegistry analysisRegistry;

        public Builder groups(List<BlockHash.GroupSpec> groups) {
            this.groups = groups;
            return this;
        }

        public Builder mode(AggregatorMode aggregatorMode) {
            this.aggregatorMode = aggregatorMode;
            return this;
        }

        public Builder aggregators(List<GroupingAggregator.Factory> aggregators) {
            this.aggregators = aggregators;
            return this;
        }

        public Builder partialEmit(int keysThreshold, double uniquenessThreshold) {
            this.partialEmitKeysThreshold = keysThreshold;
            this.partialEmitUniquenessThreshold = uniquenessThreshold;
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

        public Builder analysisRegistry(AnalysisRegistry analysisRegistry) {
            this.analysisRegistry = analysisRegistry;
            return this;
        }

        public Factory build() {
            return new Factory(this);
        }
    }

    public static class Factory implements OperatorFactory {
        private final List<BlockHash.GroupSpec> groups;
        private final AggregatorMode aggregatorMode;
        private final List<GroupingAggregator.Factory> aggregators;
        private final int partialEmitKeysThreshold;
        private final double partialEmitUniquenessThreshold;
        private final int maxPageSize;
        private final int aggregationBatchSize;
        private final AnalysisRegistry analysisRegistry;

        protected Factory(Builder builder) {
            this.groups = requireNonNull(builder.groups, "groups");
            this.aggregatorMode = requireNonNull(builder.aggregatorMode, "aggregatorMode");
            this.aggregators = requireNonNull(builder.aggregators, "aggregators");
            this.partialEmitKeysThreshold = builder.partialEmitKeysThreshold;
            this.partialEmitUniquenessThreshold = builder.partialEmitUniquenessThreshold;
            this.maxPageSize = builder.maxPageSize;
            this.aggregationBatchSize = builder.aggregationBatchSize;
            this.analysisRegistry = builder.analysisRegistry;
        }

        @Override
        public final HashAggregationOperator get(DriverContext driverContext) {
            if (groups.stream().anyMatch(BlockHash.GroupSpec::isCategorize)) {
                return new HashAggregationOperator(
                    aggregatorMode,
                    aggregators,
                    () -> wrapBlockHash(
                        driverContext,
                        BlockHash.buildCategorizeBlockHash(
                            groups,
                            aggregatorMode,
                            driverContext.blockFactory(),
                            analysisRegistry,
                            maxPageSize
                        )
                    ),
                    Integer.MAX_VALUE, // disable partial emit for CATEGORIZE. it doesn't support it.
                    1.0,
                    Integer.MAX_VALUE, // disable splitting aggs pages for CATEGORIZE. it doesn't support it.
                    driverContext
                );
            }
            return new HashAggregationOperator(
                aggregatorMode,
                aggregators,
                () -> wrapBlockHash(driverContext, BlockHash.build(groups, driverContext.blockFactory(), aggregationBatchSize, false)),
                partialEmitKeysThreshold,
                partialEmitUniquenessThreshold,
                maxPageSize,
                driverContext
            );
        }

        protected BlockHash wrapBlockHash(DriverContext driverContext, BlockHash hash) {
            return hash;
        }

        @Override
        public final String describe() {
            return "HashAggregationOperator[mode = "
                + "<not-needed>"
                + ", aggs = "
                + aggregators.stream().map(Describable::describe).collect(joining(", "))
                + "]";
        }
    }

    protected final Supplier<BlockHash> blockHashSupplier;
    protected final AggregatorMode aggregatorMode;
    protected final List<GroupingAggregator.Factory> aggregatorFactories;
    protected final List<GroupingAggregator> aggregators;
    protected final int partialEmitKeysThreshold;
    protected final double partialEmitUniquenessThreshold;

    protected final DriverContext driverContext;

    // The blockHash and aggregators can be re-initialized when partial results are emitted periodically
    protected BlockHash blockHash;
    private boolean finished;
    protected ReleasableIterator<Page> output;

    /**
     * Maximum number of rows per output page.
     */
    private final int maxPageSize;

    /**
     * Nanoseconds this operator has spent hashing grouping keys.
     */
    private long hashNanos;
    /**
     * Nanoseconds this operator has spent running the aggregations.
     */
    private long aggregationNanos;
    /**
     * Count of pages this operator has processed.
     */
    private int pagesProcessed;
    /**
     * Count of rows this operator has received.
     */
    private long rowsReceived;
    /**
     * Count of rows this operator has emitted.
     */
    private long rowsEmitted;

    /**
     * Total nanos for emitting the output
     */
    protected long emitNanos;

    protected long emitCount;

    protected long rowsAddedInCurrentBatch;

    /**
     * Build the operator. Instead of calling this directly, build the {@link Builder},
     * then the {@link Builder#build()} then {@link Factory#get}.
     */
    @SuppressWarnings("this-escape")
    public HashAggregationOperator(
        AggregatorMode aggregatorMode,
        List<GroupingAggregator.Factory> aggregatorFactories,
        Supplier<BlockHash> blockHashSupplier,
        int partialEmitKeysThreshold,
        double partialEmitUniquenessThreshold,
        int maxPageSize,
        DriverContext driverContext
    ) {
        if (partialEmitKeysThreshold <= 0) {
            throw new IllegalArgumentException("partialEmitKeysThreshold must be greater than 0; got " + partialEmitKeysThreshold);
        }
        this.aggregatorMode = aggregatorMode;
        this.partialEmitKeysThreshold = partialEmitKeysThreshold;
        this.partialEmitUniquenessThreshold = partialEmitUniquenessThreshold;
        this.maxPageSize = maxPageSize;
        this.driverContext = driverContext;
        this.aggregatorFactories = aggregatorFactories;
        this.blockHashSupplier = blockHashSupplier;
        this.aggregators = new ArrayList<>();
        boolean success = false;
        try {
            this.blockHash = blockHashSupplier.get();
            for (GroupingAggregator.Factory a : aggregatorFactories) {
                var groupingAggregator = a.apply(driverContext);
                assert groupingAggregator.mode() == aggregatorMode : groupingAggregator.mode() + " != " + aggregatorMode;
                this.aggregators.add(groupingAggregator);
            }
            success = true;
        } finally {
            if (success == false) {
                close();
            }
        }
    }

    @Override
    public boolean needsInput() {
        return output == null && finished == false;
    }

    @Override
    public void addInput(Page page) {
        try {
            maybeReinitializeAfterPeriodicallyEmitted();
            List<GroupingAggregatorFunction.AddInput> prepared = new ArrayList<>(aggregators.size());
            class AddInput implements GroupingAggregatorFunction.AddInput {
                long hashStart = System.nanoTime();
                long aggStart;

                @Override
                public void add(int positionOffset, IntArrayBlock groupIds) {
                    startAggEndHash();
                    for (GroupingAggregatorFunction.AddInput p : prepared) {
                        p.add(positionOffset, groupIds);
                    }
                    end();
                }

                @Override
                public void add(int positionOffset, IntBigArrayBlock groupIds) {
                    startAggEndHash();
                    for (GroupingAggregatorFunction.AddInput p : prepared) {
                        p.add(positionOffset, groupIds);
                    }
                    end();
                }

                @Override
                public void add(int positionOffset, IntVector groupIds) {
                    startAggEndHash();
                    for (GroupingAggregatorFunction.AddInput p : prepared) {
                        p.add(positionOffset, groupIds);
                    }
                    end();
                }

                private void startAggEndHash() {
                    aggStart = System.nanoTime();
                    hashNanos += aggStart - hashStart;
                }

                private void end() {
                    hashStart = System.nanoTime();
                    aggregationNanos += hashStart - aggStart;
                }

                @Override
                public void close() {
                    Releasables.closeExpectNoException(Releasables.wrap(prepared));
                }
            }
            try (AddInput add = new AddInput()) {
                checkState(needsInput(), "Operator is already finishing");
                requireNonNull(page, "page is null");

                for (GroupingAggregator aggregator : aggregators) {
                    GroupingAggregatorFunction.AddInput p = aggregator.prepareProcessPage(blockHash, page);
                    if (p != null) {
                        prepared.add(p);
                    }
                }

                // TODO we can skip the page *entirely* if we know we don't need "empty" results.
                blockHash.add(page, add);
                hashNanos += System.nanoTime() - add.hashStart;
            }
            rowsAddedInCurrentBatch += page.getPositionCount();
            if (shouldEmitPartialResultsPeriodically()) {
                emit();
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
        Page p = output.next();
        rowsEmitted += p.getPositionCount();
        return p;
    }

    @Override
    public void finish() {
        if (finished) {
            return;
        }
        finished = true;
        emit();
    }

    private void maybeReinitializeAfterPeriodicallyEmitted() {
        if (rowsReceived > 0 && rowsAddedInCurrentBatch == 0) {
            blockHash.close();
            blockHash = null;
            blockHash = blockHashSupplier.get();
            for (int i = 0; i < aggregators.size(); i++) {
                Releasables.close(aggregators.set(i, aggregatorFactories.get(i).apply(driverContext)));
            }
        }
    }

    protected void emit() {
        if (rowsAddedInCurrentBatch == 0) {
            return;
        }
        int[] aggBlockCounts = aggregators.stream().mapToInt(GroupingAggregator::evaluateBlockCount).toArray();
        long startInNanos = System.nanoTime();
        PreparedForEvaluation prepared = new PreparedForEvaluation();
        try {
            if (prepared.selected.keys.getPositionCount() <= maxPageSize) {
                output = ReleasableIterator.single(prepared.buildPage(prepared.selected, aggBlockCounts));
            } else {
                output = new MultiPageResult(prepared, aggBlockCounts);
                prepared = null; // Prepared has moved into the output
            }
        } finally {
            rowsAddedInCurrentBatch = 0;
            Releasables.close(prepared);
            emitNanos += System.nanoTime() - startInNanos;
            emitCount++;
        }
    }

    /**
     * Customize the {@code selected} groupIds that are sent to the agg's
     * {@link GroupingAggregatorFunction#prepareEvaluateIntermediate} and
     * {@link GroupingAggregatorFunction#prepareEvaluateFinal}. TSDB uses
     * this to do less work later on.
     */
    protected IntVector customizeSelected(GroupingAggregator aggregator, IntVector selected) {
        selected.incRef();
        return selected;
    }

    protected boolean shouldEmitPartialResultsPeriodically() {
        if (aggregatorMode.isOutputPartial() == false) {
            return false;
        }
        if (rowsAddedInCurrentBatch == 0) {
            return false;
        }
        final int numKeys = blockHash.numKeys();
        if (numKeys < partialEmitKeysThreshold) {
            return false;
        }
        return rowsAddedInCurrentBatch * partialEmitUniquenessThreshold <= numKeys;
    }

    protected GroupingAggregatorEvaluationContext evaluationContext(BlockHash blockHash) {
        return new GroupingAggregatorEvaluationContext(driverContext);
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
        Releasables.close(blockHash, () -> Releasables.close(aggregators), output);
    }

    @Override
    public Operator.Status status() {
        return new Status(hashNanos, aggregationNanos, pagesProcessed, rowsReceived, rowsEmitted, emitNanos, emitCount);
    }

    protected static void checkState(boolean condition, String msg) {
        if (condition == false) {
            throw new IllegalArgumentException(msg);
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(this.getClass().getSimpleName()).append("[");
        sb.append("blockHash=").append(blockHash).append(", ");
        sb.append("aggregators=").append(aggregators);
        sb.append("]");
        return sb.toString();
    }

    public static class Status implements Operator.Status {
        public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
            Operator.Status.class,
            "hashagg",
            Status::new
        );

        private static final TransportVersion ESQL_HASH_OPERATOR_STATUS_OUTPUT_TIME = TransportVersion.fromName(
            "esql_hash_operator_status_output_time"
        );

        private static final TransportVersion ESQL_HASH_OPERATOR_STATUS_EMIT_COUNT = TransportVersion.fromName(
            "esql_hash_operator_status_emit_count"
        );

        /**
         * Nanoseconds this operator has spent hashing grouping keys.
         */
        private final long hashNanos;
        /**
         * Nanoseconds this operator has spent running the aggregations.
         */
        private final long aggregationNanos;
        /**
         * Count of pages this operator has processed.
         */
        private final int pagesProcessed;
        /**
         * Count of rows this operator has received.
         */
        private final long rowsReceived;
        /**
         * Count of rows this operator has emitted.
         */
        private final long rowsEmitted;

        private final long emitNanos;

        protected final long emitCount;

        /**
         * Build.
         *
         * @param hashNanos        Nanoseconds this operator has spent hashing grouping keys.
         * @param aggregationNanos Nanoseconds this operator has spent running the aggregations.
         * @param pagesProcessed   Count of pages this operator has processed.
         * @param rowsReceived     Count of rows this operator has received.
         * @param rowsEmitted      Count of rows this operator has emitted.
         * @param emitNanos        Nanoseconds this operator has spent emitting the output.
         * @param emitCount        Count of times this operator has emitted output.
         */
        public Status(
            long hashNanos,
            long aggregationNanos,
            int pagesProcessed,
            long rowsReceived,
            long rowsEmitted,
            long emitNanos,
            long emitCount
        ) {
            this.hashNanos = hashNanos;
            this.aggregationNanos = aggregationNanos;
            this.pagesProcessed = pagesProcessed;
            this.rowsReceived = rowsReceived;
            this.rowsEmitted = rowsEmitted;
            this.emitNanos = emitNanos;
            this.emitCount = emitCount;
        }

        protected Status(StreamInput in) throws IOException {
            hashNanos = in.readVLong();
            aggregationNanos = in.readVLong();
            pagesProcessed = in.readVInt();
            rowsReceived = in.readVLong();
            rowsEmitted = in.readVLong();
            if (in.getTransportVersion().supports(ESQL_HASH_OPERATOR_STATUS_OUTPUT_TIME)) {
                emitNanos = in.readVLong();
            } else {
                emitNanos = 0;
            }
            if (in.getTransportVersion().supports(ESQL_HASH_OPERATOR_STATUS_EMIT_COUNT)) {
                emitCount = in.readVLong();
            } else {
                emitCount = 0;
            }
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVLong(hashNanos);
            out.writeVLong(aggregationNanos);
            out.writeVInt(pagesProcessed);
            out.writeVLong(rowsReceived);
            out.writeVLong(rowsEmitted);
            if (out.getTransportVersion().supports(ESQL_HASH_OPERATOR_STATUS_OUTPUT_TIME)) {
                out.writeVLong(emitNanos);
            }
            if (out.getTransportVersion().supports(ESQL_HASH_OPERATOR_STATUS_EMIT_COUNT)) {
                out.writeVLong(emitCount);
            }
        }

        @Override
        public String getWriteableName() {
            return ENTRY.name;
        }

        /**
         * Nanoseconds this operator has spent hashing grouping keys.
         */
        public long hashNanos() {
            return hashNanos;
        }

        /**
         * Nanoseconds this operator has spent running the aggregations.
         */
        public long aggregationNanos() {
            return aggregationNanos;
        }

        /**
         * Count of pages this operator has processed.
         */
        public int pagesProcessed() {
            return pagesProcessed;
        }

        /**
         * Count of rows this operator has received.
         */
        public long rowsReceived() {
            return rowsReceived;
        }

        /**
         * Count of rows this operator has emitted.
         */
        public long rowsEmitted() {
            return rowsEmitted;
        }

        /**
         * Nanoseconds this operator has spent emitting the output.
         */
        public long emitNanos() {
            return emitNanos;
        }

        /**
         * Count of times this operator has emitted output.
         */
        public long emitCount() {
            return emitCount;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
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
            if (emitCount > 0) {
                builder.field("emit_count", emitCount);
            }
            builder.field("emit_nanos", emitNanos);
            if (builder.humanReadable()) {
                builder.field("emit_time", TimeValue.timeValueNanos(emitNanos));
            }
            return builder.endObject();

        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Status status = (Status) o;
            return hashNanos == status.hashNanos
                && aggregationNanos == status.aggregationNanos
                && pagesProcessed == status.pagesProcessed
                && rowsReceived == status.rowsReceived
                && rowsEmitted == status.rowsEmitted
                && emitNanos == status.emitNanos
                && emitCount == status.emitCount;
        }

        @Override
        public int hashCode() {
            return Objects.hash(hashNanos, aggregationNanos, pagesProcessed, rowsReceived, rowsEmitted, emitNanos, emitCount);
        }

        @Override
        public String toString() {
            return Strings.toString(this);
        }

        @Override
        public TransportVersion getMinimalSupportedVersion() {
            return TransportVersion.minimumCompatible();
        }
    }

    /**
     * Returns many pages of results from aggregations. Works by breaking chunks off
     * of the {@code selected} and {@code keys}.
     * <p>
     *     This is a step towards a system that breaks rows off of the {@link BlockHash}
     *     itself. Right now, the {@link BlockHash} implementations returns all results
     *     at once so the best we can do is break pieces off. But soon! Soon we can make
     *     them smarter.
     * </p>
     */
    class MultiPageResult implements ReleasableIterator<Page> {
        private final PreparedForEvaluation prepared;
        private final int[] aggBlockCounts;

        private int rowOffset = 0;

        MultiPageResult(PreparedForEvaluation prepared, int[] aggBlockCounts) {
            this.prepared = prepared;
            this.aggBlockCounts = aggBlockCounts;
        }

        @Override
        public boolean hasNext() {
            return rowOffset < prepared.selected.keys.getPositionCount();
        }

        @Override
        public Page next() {
            long startInNanos = System.nanoTime();
            int endOffset = Math.min(maxPageSize + rowOffset, prepared.selected.keys.getPositionCount());
            try (Selected selectedInThisPage = prepared.selected.slice(rowOffset, endOffset)) {
                Page output = prepared.buildPage(selectedInThisPage, aggBlockCounts);
                rowOffset = endOffset;
                return output;
            } finally {
                emitNanos += System.nanoTime() - startInNanos;
            }
        }

        @Override
        public void close() {
            prepared.close();
        }
    }

    private class PreparedForEvaluation implements Releasable {
        private final GroupingAggregatorEvaluationContext ctx;
        private final Selected selected;
        private final List<GroupingAggregatorFunction.PreparedForEvaluation> preparedAggregators;

        private PreparedForEvaluation() {
            int count = aggregators.size();
            GroupingAggregatorEvaluationContext ctx = evaluationContext(blockHash);
            Selected selected = null;
            List<GroupingAggregatorFunction.PreparedForEvaluation> preparedAggregators = new ArrayList<>(count);
            boolean success = false;
            try {
                selected = new Selected(blockHash.nonEmpty(), new IntVector[count]);
                for (int a = 0; a < count; a++) {
                    selected.aggs[a] = customizeSelected(aggregators.get(a), selected.keys);
                    preparedAggregators.add(aggregators.get(a).prepareForEvaluate(selected.aggs[a], ctx));
                }
                success = true;
            } finally {
                if (success == false) {
                    Releasables.close(ctx, selected, Releasables.wrap(preparedAggregators));
                }
            }
            this.ctx = ctx;
            this.selected = selected;
            this.preparedAggregators = preparedAggregators;
        }

        /**
         * Build a page or results.
         * @param selectedInPage The subset of {@link #selected} for this page. If we're
         *                       emitting a single page then this is {@code ==} to {@link #selected}.
         */
        Page buildPage(Selected selectedInPage, int[] aggBlockCounts) {
            Block[] keys = blockHash.getKeys(selectedInPage.keys);
            Block[] blocks = new Block[keys.length + Arrays.stream(aggBlockCounts).sum()];
            System.arraycopy(keys, 0, blocks, 0, keys.length);
            try {
                int blockOffset = keys.length;
                for (int i = 0; i < preparedAggregators.size(); i++) {
                    var aggregator = preparedAggregators.get(i);
                    aggregator.evaluate(blocks, blockOffset, selectedInPage.aggs[i]);
                    blockOffset += aggBlockCounts[i];
                }
                Page result = new Page(blocks);
                blocks = null;
                return result;
            } finally {
                if (blocks != null) {
                    Releasables.close(blocks);
                }
            }
        }

        @Override
        public void close() {
            Releasables.close(ctx, selected, Releasables.wrap(preparedAggregators));
        }
    }

    private record Selected(IntVector keys, IntVector[] aggs) implements Releasable {
        public Selected slice(int beginInclude, int endExclusive) {
            Selected result = new Selected(keys.slice(beginInclude, endExclusive), new IntVector[aggs.length]);
            try {
                for (int a = 0; a < aggs.length; a++) {
                    result.aggs[a] = aggs[a].slice(beginInclude, endExclusive);
                }
                Selected r = result;
                result = null;
                return r;
            } finally {
                Releasables.close(result);
            }

        }

        @Override
        public void close() {
            Releasables.close(keys, Releasables.wrap(aggs));
        }
    }
}
