/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.bytes.PagedBytesBuilder;
import org.elasticsearch.common.bytes.PagedBytesCursor;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BitArray;
import org.elasticsearch.compute.Describable;
import org.elasticsearch.compute.aggregation.AggregatorMode;
import org.elasticsearch.compute.aggregation.FilteredGroupingAggregatorFunction;
import org.elasticsearch.compute.aggregation.GroupingAggregator;
import org.elasticsearch.compute.aggregation.GroupingAggregatorEvaluationContext;
import org.elasticsearch.compute.aggregation.GroupingAggregatorFunction;
import org.elasticsearch.compute.aggregation.blockhash.BlockHash;
import org.elasticsearch.compute.aggregation.blockhash.PartitionedBlockHash;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.IntArrayBlock;
import org.elasticsearch.compute.data.IntBigArrayBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.data.PartitionedAggregationBlock;
import org.elasticsearch.compute.spill.QueryBreaker;
import org.elasticsearch.compute.spill.QueryScope;
import org.elasticsearch.compute.spill.SpillStatus;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.ReleasableIterator;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.analysis.AnalysisRegistry;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Executor;
import java.util.function.Function;

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
    public static final long DEFAULT_PARTITIONING_MEMORY_THRESHOLD = 16L * 1024 * 1024;
    public static final int DEFAULT_PARTITIONING_NUM_KEYS_THRESHOLD = 400_000;

    // TODO: Push down LIMIT only
    public record TopAggregation(int aggregatorIndex, boolean asc, int limit) {}

    public record ParallelConfig(
        Executor executor,
        int numWorkers,
        int pagesPerWorker,
        int partitionKeysThreshold,
        long partitioningMemoryThreshold
    ) {
        public ParallelConfig(Executor executor, int numWorkers, int pagesPerWorker, int partitionKeysThreshold) {
            this(executor, numWorkers, pagesPerWorker, partitionKeysThreshold, Long.MAX_VALUE);
        }
    }

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
        private TopAggregation topAggregation;
        private boolean allowPartitionedOutput;
        private ParallelConfig parallelConfig;

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

        public Builder topAggregation(TopAggregation topAggregation) {
            this.topAggregation = topAggregation;
            return this;
        }

        public Builder parallelConfig(ParallelConfig parallelConfig) {
            this.parallelConfig = parallelConfig;
            return this;
        }

        public Builder allowPartitionedOutput(boolean allowPartitionedOutput) {
            this.allowPartitionedOutput = allowPartitionedOutput;
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
        private final TopAggregation topAggregation;
        private final ParallelConfig parallelConfig;
        private final boolean allowPartitionedOutput;

        protected Factory(Builder builder) {
            this.groups = requireNonNull(builder.groups, "groups");
            this.aggregatorMode = requireNonNull(builder.aggregatorMode, "aggregatorMode");
            this.aggregators = requireNonNull(builder.aggregators, "aggregators");
            this.partialEmitKeysThreshold = builder.partialEmitKeysThreshold;
            this.partialEmitUniquenessThreshold = builder.partialEmitUniquenessThreshold;
            this.maxPageSize = builder.maxPageSize;
            this.aggregationBatchSize = builder.aggregationBatchSize;
            this.analysisRegistry = builder.analysisRegistry;
            this.topAggregation = builder.topAggregation;
            this.parallelConfig = builder.parallelConfig;
            this.allowPartitionedOutput = builder.allowPartitionedOutput;
        }

        @Override
        public final HashAggregationOperator get(DriverContext driverContext) {
            if (groups.stream().anyMatch(BlockHash.GroupSpec::isCategorize)) {
                return new HashAggregationOperator(
                    aggregatorMode,
                    aggregators,
                    dc -> wrapBlockHash(
                        dc,
                        BlockHash.buildCategorizeBlockHash(groups, aggregatorMode, dc.blockFactory(), analysisRegistry, maxPageSize)
                    ),
                    Integer.MAX_VALUE, // disable partial emit for CATEGORIZE. it doesn't support it.
                    1.0,
                    Integer.MAX_VALUE, // disable splitting aggs pages for CATEGORIZE. it doesn't support it.
                    topAggregation,
                    driverContext,
                    parallelConfig,
                    allowPartitionedOutput
                );
            }
            return new HashAggregationOperator(
                aggregatorMode,
                aggregators,
                dc -> wrapBlockHash(dc, BlockHash.build(groups, dc.blockFactory(), aggregationBatchSize, false)),
                partialEmitKeysThreshold,
                partialEmitUniquenessThreshold,
                maxPageSize,
                topAggregation,
                driverContext,
                parallelConfig,
                allowPartitionedOutput,
                groups,
                0,
                0
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

    protected final Function<DriverContext, BlockHash> blockHashSupplier;
    protected final AggregatorMode aggregatorMode;
    protected final List<GroupingAggregator.Factory> aggregatorFactories;
    protected final List<GroupingAggregator> aggregators;
    protected final int partialEmitKeysThreshold;
    protected final double partialEmitUniquenessThreshold;

    protected final DriverContext driverContext;
    private final boolean supportPartitioning;
    private final int partitioningRowThreshold;
    private long partitioningMemoryThreshold;
    private final ParallelConfig parallelConfig;

    // The blockHash and aggregators can be re-initialized when partial results are emitted periodically
    protected BlockHash blockHash;
    private boolean finished;
    protected ReleasableIterator<Page> output;
    private Page pendingInputPage;

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

    private final TopAggregation topAggregation;
    private final boolean partitionedPartialOutput;
    private boolean emittedPartitionedOutput;
    final List<PartitionedAggregationBlock> partitionedAggregationBlocks = new ArrayList<>();

    protected long rowsAddedInCurrentBatch;

    // ---- spilling: the freeze route; every field below is the driver thread's
    /** The group specs, which the route's key encoder needs; an operator built without them stays resident. */
    @Nullable
    private final List<BlockHash.GroupSpec> groups;
    /** The query's scope when it spills and this operator may freeze, null otherwise: today's paths then run untouched. */
    @Nullable
    private final QueryScope scope;
    /** The overflow and the replay, present exactly when {@link #scope} is. */
    @Nullable
    private final SpillReplay spill;
    private final int depth;
    private final int seed;
    /** The groups present at the freeze: a looked-up id outside this set is a key the hash reserved, not one it holds. */
    @Nullable
    private BitArray frozenGroups;
    @Nullable
    private GroupKeyEncoder keyEncoder;
    /** A page admitted after the freeze while the writer was still being created; routed when it exists. */
    @Nullable
    private Page held;
    private long groupsFrozen;
    /** Where the lookup of a page after the freeze chunks its ids. */
    private static final ByteSizeValue LOOKUP_TARGET_BYTES = ByteSizeValue.ofBytes(Operator.TARGET_PAGE_SIZE);

    /**
     * Build the operator. Instead of calling this directly, build the {@link Builder},
     * then the {@link Builder#build()} then {@link Factory#get}. An operator built this way, without its group
     * specs, keeps every group resident.
     */
    public HashAggregationOperator(
        AggregatorMode aggregatorMode,
        List<GroupingAggregator.Factory> aggregatorFactories,
        Function<DriverContext, BlockHash> blockHashSupplier,
        int partialEmitKeysThreshold,
        double partialEmitUniquenessThreshold,
        int maxPageSize,
        TopAggregation topAggregation,
        DriverContext driverContext,
        ParallelConfig parallelConfig,
        boolean allowPartitionedOutput
    ) {
        this(
            aggregatorMode,
            aggregatorFactories,
            blockHashSupplier,
            partialEmitKeysThreshold,
            partialEmitUniquenessThreshold,
            maxPageSize,
            topAggregation,
            driverContext,
            parallelConfig,
            allowPartitionedOutput,
            null,
            0,
            0
        );
    }

    /**
     * @param groups the group specs, for the freeze route's key encoder; null keeps the operator resident
     * @param depth how many freezes this operator's input went through already; zero for the planned operator
     * @param seed changes the partition routing at each depth, so a partition that freezes again splits differently
     */
    @SuppressWarnings("this-escape")
    HashAggregationOperator(
        AggregatorMode aggregatorMode,
        List<GroupingAggregator.Factory> aggregatorFactories,
        Function<DriverContext, BlockHash> blockHashSupplier,
        int partialEmitKeysThreshold,
        double partialEmitUniquenessThreshold,
        int maxPageSize,
        TopAggregation topAggregation,
        DriverContext driverContext,
        ParallelConfig parallelConfig,
        boolean allowPartitionedOutput,
        @Nullable List<BlockHash.GroupSpec> groups,
        int depth,
        int seed
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
        this.topAggregation = topAggregation;
        this.parallelConfig = parallelConfig;
        this.partitioningRowThreshold = parallelConfig != null ? parallelConfig.partitionKeysThreshold : Integer.MAX_VALUE;
        this.partitioningMemoryThreshold = parallelConfig != null ? parallelConfig.partitioningMemoryThreshold : Long.MAX_VALUE;
        this.groups = groups;
        this.depth = depth;
        this.seed = seed;
        boolean success = false;
        try {
            this.blockHash = blockHashSupplier.apply(driverContext);
            for (GroupingAggregator.Factory a : aggregatorFactories) {
                var groupingAggregator = a.apply(driverContext);
                assert groupingAggregator.mode() == aggregatorMode : groupingAggregator.mode() + " != " + aggregatorMode;
                this.aggregators.add(groupingAggregator);
            }
            this.supportPartitioning = parallelConfig != null
                && blockHash instanceof PartitionedBlockHash
                && PartitionedBlockHash.supportPartitioning()
                && aggregators.stream().allMatch(a -> a.aggregatorFunction().supportPartitioning());
            this.partitionedPartialOutput = this.supportPartitioning && allowPartitionedOutput && aggregatorMode.isOutputPartial();
            QueryScope queryScope = driverContext.queryScope();
            this.scope = groups != null && SpillReplay.applies(queryScope) && freezeEligible() ? queryScope : null;
            // the replay's host is this operator; it reads nothing of it before the constructor is done
            this.spill = this.scope == null
                ? null
                : new SpillReplay(driverContext, this.scope, new ReplayHost(), "hash-aggregation", depth, seed);
            success = true;
        } finally {
            if (success == false) {
                close();
            }
        }
    }

    HashAggregationOperator spawnWorker() {
        assert parallelConfig != null : "parallel config is not set for workers";
        DriverContext mainDriverContext = this.driverContext;
        var workerBlockFactory = mainDriverContext.createChildBlockFactory();
        boolean success = false;
        try {
            var workerDriverContext = new DriverContext(
                mainDriverContext.bigArrays(),
                workerBlockFactory,
                mainDriverContext.localBreakerSettings()
            );
            HashAggregationOperator operator = new HashAggregationOperator(
                aggregatorMode,
                aggregatorFactories,
                blockHashSupplier,
                partialEmitKeysThreshold,
                partialEmitUniquenessThreshold,
                maxPageSize,
                topAggregation,
                workerDriverContext,
                parallelConfig,
                false
            ) {
                @Override
                public void close() {
                    Releasables.close(
                        super::close,
                        () -> mainDriverContext.releaseChildBlockFactory(workerBlockFactory),
                        mainDriverContext::removeAsyncAction
                    );
                }
            };
            mainDriverContext.addAsyncAction();
            success = true;
            return operator;
        } finally {
            if (success == false) {
                mainDriverContext.releaseChildBlockFactory(workerBlockFactory);
            }
        }
    }

    @Override
    public Operator tryPromote(DriverContext driverContext) {
        // an operator that may freeze does not partition, so neither condition holds for it: a promotion could not
        // take over rows on disk
        if (partitionedAggregationBlocks.isEmpty() == false
            || (supportPartitioning && aggregatorMode.isOutputPartial() == false && partitioningThresholdReached())) {
            var parallelOp = new ParallelHashAggregationOperator(parallelConfig, this);
            Releasables.close(this);
            return parallelOp;
        }
        return this;
    }

    @Override
    public boolean needsInput() {
        return output == null
            && pendingInputPage == null
            && finished == false
            && held == null
            && (spill == null || spill.appendsBlocked().listener().isDone());
    }

    /** Which of the conditions of {@link #needsInput} do not hold, for the message of a page handed over regardless. */
    private String inputState() {
        return "output pending ["
            + (output != null)
            + "], input pending ["
            + (pendingInputPage != null)
            + "], finishing ["
            + finished
            + "], page held ["
            + (held != null)
            + "], appends blocked ["
            + (spill != null && spill.appendsBlocked().listener().isDone() == false)
            + "]";
    }

    @Override
    public void addInput(Page page) {
        // will be promoted to the parallel operator
        if (page.getBlockCount() == 1 && page.getBlock(0) instanceof PartitionedAggregationBlock pb) {
            if (spill != null) {
                // only an operator whose functions all partition sends them, and its receiver partitions as well
                page.releaseBlocks();
                throw new IllegalStateException("an aggregation that may freeze received partitioned blocks");
            }
            pb.mustIncRef();
            partitionedAggregationBlocks.add(pb);
            page.close();
            return;
        }
        // the appends bound is advisory: the driver consults it through needsInput before it hands a page over, and a
        // page handed over is routed or held whatever the count stands at, so the operator never refuses one over it
        checkState(
            output == null && pendingInputPage == null && finished == false && held == null,
            "Operator cannot take input: " + inputState()
        );
        requireNonNull(page, "page is null");
        if (spill != null) {
            addInputSpilling(page);
            return;
        }
        addInputResident(page);
    }

    private void addInputResident(Page page) {
        final boolean toEmit;
        boolean success = false;
        try {
            toEmit = canEmitCurrentBatchPeriodically() && periodicEmitThresholdReached() && prepareForPeriodicEmit(page);
            success = true;
        } finally {
            if (success == false) {
                page.releaseBlocks();
            }
        }
        if (toEmit) {
            pendingInputPage = page;
            emit();
        } else {
            processInputPage(page);
        }
    }

    private void processInputPage(Page page) {
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
                    assert assertGroupAssignments(page, positionOffset, groupIds);
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
        } finally {
            pagesProcessed++;
            rowsReceived += page.getPositionCount();
            page.releaseBlocks();
        }
    }

    @Override
    public Page getOutput() {
        Page out = nextOutputPage();
        if (out == null) {
            if (pendingInputPage != null) {
                try {
                    processInputPage(pendingInputPage);
                } finally {
                    this.pendingInputPage = null;
                }
                if (finished) {
                    emit();
                    out = nextOutputPage();
                }
            }
            if (out == null && spill != null) {
                out = outputSpilling();
            }
        }
        return out;
    }

    private Page nextOutputPage() {
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
        // a page held for the writer has rows of the frozen groups to merge first: the route emits once it has gone
        if (pendingInputPage == null && held == null) {
            emit();
        }
        if (spill != null) {
            spill.maybeStartFinishing(true, held != null);
        }
    }

    private void maybeReinitializeAfterPeriodicallyEmitted() {
        if (rowsReceived > 0 && rowsAddedInCurrentBatch == 0) {
            clearCurrentBatch();
        }
    }

    void clearCurrentBatch() {
        rowsAddedInCurrentBatch = 0;
        if (blockHash instanceof PartitionedBlockHash partitionedBlockHash) {
            partitionedBlockHash.clear();
        } else {
            blockHash.close();
            blockHash = null;
            blockHash = blockHashSupplier.apply(driverContext);
        }
        for (int i = 0; i < aggregators.size(); i++) {
            Releasables.close(aggregators.set(i, aggregatorFactories.get(i).apply(driverContext)));
        }
    }

    protected void emit() {
        if (rowsAddedInCurrentBatch == 0) {
            return;
        }
        long startInNanos = System.nanoTime();
        PreparedForEvaluation prepared = null;
        try {
            if (shouldEmitPartitionedPartialOutput()) {
                var partitionedBlock = PartitionedHashAggregations.splitToPartitionedBlock(driverContext.breaker(), this);
                output = ReleasableIterator.single(new Page(partitionedBlock));
                emittedPartitionedOutput = true;
            } else {
                int[] aggBlockCounts = aggregators.stream().mapToInt(GroupingAggregator::evaluateBlockCount).toArray();
                prepared = new PreparedForEvaluation();
                if (prepared.selected.keys.getPositionCount() <= maxPageSize) {
                    output = ReleasableIterator.single(prepared.buildPage(prepared.selected, aggBlockCounts));
                } else {
                    output = new MultiPageResult(prepared, aggBlockCounts);
                    prepared = null; // Prepared has moved into the output
                }
            }
        } finally {
            rowsAddedInCurrentBatch = 0;
            Releasables.close(prepared);
            emitNanos += System.nanoTime() - startInNanos;
            emitCount++;
        }
    }

    private boolean shouldEmitPartitionedPartialOutput() {
        if (partitionedPartialOutput == false) {
            return false;
        }
        // Once partitioned, keep subsequent output partitioned.
        if (emittedPartitionedOutput) {
            return true;
        }
        // Partition when the current batch reaches the threshold.
        if (partitioningThresholdReached()) {
            return true;
        }
        // For the final batch, partition at 75% of the threshold.
        return finished
            && (blockHash.numKeys() >= Math.toIntExact((partitioningRowThreshold * 3L + 3L) / 4L)
                || estimatedBytesForPartitioning() >= partitioningMemoryThreshold - partitioningMemoryThreshold / 4);
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

    protected boolean assertGroupAssignments(Page page, int positionOffset, IntVector groupIds) {
        return true;
    }

    /**
     * Selects which group ids ("keys") to emit, given the full set of non-empty groups. The default emits every group.
     * Subclasses can override to emit a subset: the time-series operator emits only the groups aligned to the output
     * time bucket, while still exposing the full group set to window aggregators through the evaluation context.
     * <p>
     * The returned vector is owned by the caller. {@code allKeys} remains owned by the caller and is released right after
     * this method returns, so an implementation that needs to retain it (e.g. by stashing it in {@code ctx}) must
     * increment its reference count.
     */
    protected IntVector selectedKeysForEmit(GroupingAggregatorEvaluationContext ctx, IntVector allKeys) {
        allKeys.incRef();
        return allKeys;
    }

    /**
     * Whether the current batch may be emitted before the incoming page is added to it. Partial output can always be
     * emitted early because the final aggregation re-merges groups that span batches.
     */
    protected boolean canEmitCurrentBatchPeriodically() {
        return aggregatorMode.isOutputPartial();
    }

    private boolean prepareForPeriodicEmit(Page incomingPage) {
        for (GroupingAggregator aggregator : aggregators) {
            if (aggregator.aggregatorFunction().prepareForPeriodicEmit(incomingPage) == false) {
                return false;
            }
        }
        return true;
    }

    private boolean periodicEmitThresholdReached() {
        if (spill != null && spill.frozen()) {
            // the frozen groups are what the route keeps resident; an emit would drop them and route every row
            return false;
        }
        if (rowsAddedInCurrentBatch == 0) {
            return false;
        }
        final int numKeys = blockHash.numKeys();
        if (partitionedPartialOutput) {
            return partitioningThresholdReached();
        } else {
            if (numKeys < partialEmitKeysThreshold) {
                return false;
            }
            return rowsAddedInCurrentBatch * partialEmitUniquenessThreshold <= numKeys;
        }
    }

    protected GroupingAggregatorEvaluationContext evaluationContext(BlockHash blockHash) {
        return new GroupingAggregatorEvaluationContext(driverContext);
    }

    boolean partitioningThresholdReached() {
        return blockHash.numKeys() >= partitioningRowThreshold || estimatedBytesForPartitioning() >= partitioningMemoryThreshold;
    }

    /**
     * Raises the memory threshold a worker splits at, never lowering it: the parallel operator calls it once its
     * registry spills, so that generations grow with the query's budget rather than with its input.
     */
    void raisePartitioningMemoryThreshold(long threshold) {
        if (threshold > partitioningMemoryThreshold) {
            partitioningMemoryThreshold = threshold;
        }
    }

    private long estimatedBytesForPartitioning() {
        if (blockHash instanceof PartitionedBlockHash partitioned) {
            return partitioned.estimatedBytesForPartitioning();
        } else {
            assert false : "expected a partitioned block hash; got " + blockHash;
            return -1;
        }
    }

    @Override
    public boolean isFinished() {
        if (finished == false || output != null || pendingInputPage != null) {
            return false;
        }
        return spill == null || spill.frozen() == false || (held == null && spill.replayDone());
    }

    @Override
    public boolean canProduceMoreDataWithoutExtraInput() {
        return output != null || pendingInputPage != null || held != null || (spill != null && spill.frozen() && finished);
    }

    @Override
    public IsBlockedResult isBlocked() {
        if (spill == null || output != null) {
            return NOT_BLOCKED;
        }
        return spill.blocked(held != null);
    }

    @Override
    public void close() {
        Releasable heldRelease = held == null ? () -> {} : held::releaseBlocks;
        Releasables.close(
            blockHash,
            () -> Releasables.close(aggregators),
            output,
            pendingInputPage,
            heldRelease,
            spill,
            frozenGroups,
            keyEncoder,
            Releasables.wrap(partitionedAggregationBlocks)
        );
    }

    @Override
    public Operator.Status status() {
        List<Status.ExtraStatus> extra = spill != null && spill.frozen()
            ? List.of(new FreezeStatus(groupsFrozen, spill.rowsSpilled(), spill.partitionsReplayed(), spill.spillStatus()))
            : List.of();
        return new Status(hashNanos, aggregationNanos, pagesProcessed, rowsReceived, rowsEmitted, emitNanos, emitCount, extra);
    }

    // ---- the freeze route

    /**
     * Whether this operator may freeze its groups: it consumes intermediate pages, whose keys are scalar, its hash
     * looks keys up without inserting them and every function keeps a bounded state per group; a top-N pushdown
     * selects among the groups it holds, which a replay by partition would not see as one set. An operator that
     * partitions, every function with a splitter under a parallel configuration, leaves spilling to the parallel
     * operator's slice route: a promotion could not take over rows on disk, and the partitioned blocks its input may
     * carry hold no row to route.
     */
    boolean freezeEligible() {
        return aggregatorMode.isInputPartial()
            && topAggregation == null
            && supportPartitioning == false
            && blockHash.supportsLookup()
            && aggregators.stream().allMatch(a -> a.aggregatorFunction().freezeEligible());
    }

    /**
     * The input path under a scope that spills: resident until the query would cross its budget, or the reservation
     * before a page fails with headroom in hand, then frozen. After the freeze a row of a group the hash holds merges
     * into it, a row of any other group goes to a partition's run.
     */
    private void addInputSpilling(Page page) {
        // the page is the operator's from here on: a throw before it was admitted, routed or held releases it
        boolean handedOver = false;
        try {
            spill.checkSpillFailure();
            if (spill.frozen()) {
                handedOver = true;
                routeOrHold(page);
                return;
            }
            spill.maybeAcquireHeadroom();
            spill.resolveHeadroom();
            // what a page may add: its keys and their states, about the page itself again
            long estimate = 2 * page.ramBytesUsedByBlocks();
            if (spill.shouldFreeze(estimate)) {
                freeze("the query would cross its budget");
                handedOver = true;
                routeOrHold(page);
                return;
            }
            // the reservation before the page is the backstop at a safe point: it fails here, with the hash and the
            // states untouched, rather than in the middle of the page; with headroom in hand that failure is the
            // freeze, without it the query fails as it does today
            QueryBreaker.View view = driverContext.queryView();
            boolean reserved = false;
            if (view != null) {
                try {
                    view.reserve(estimate, "hash aggregation page");
                    reserved = true;
                } catch (CircuitBreakingException e) {
                    if (spill.mayFreeze()) {
                        freeze("the reservation before the page failed");
                        handedOver = true;
                        routeOrHold(page);
                        return;
                    }
                    throw e;
                }
            }
            try {
                handedOver = true;
                addInputResident(page);
            } finally {
                if (reserved) {
                    view.releaseReserve();
                }
            }
        } catch (RuntimeException e) {
            if (handedOver == false) {
                page.releaseBlocks();
            }
            throw e;
        }
    }

    private void freeze(String reason) {
        frozenGroups = blockHash.seenGroupIds(driverContext.bigArrays());
        groupsFrozen = blockHash.numKeys();
        int[] channels = new int[groups.size()];
        int widest = 0;
        for (int i = 0; i < channels.length; i++) {
            channels[i] = groups.get(i).channel();
            widest = Math.max(widest, channels[i]);
        }
        List<ElementType> types = new ArrayList<>(Collections.nCopies(widest + 1, (ElementType) null));
        for (BlockHash.GroupSpec group : groups) {
            types.set(group.channel(), group.elementType());
        }
        BlockFactory blockFactory = driverContext.blockFactory();
        PagedBytesBuilder row = new PagedBytesBuilder(blockFactory.bigArrays().recycler(), blockFactory.breaker(), "group-key-encoder", 64);
        keyEncoder = new GroupKeyEncoder(channels, types, row);
        spill.freeze(reason, groupsFrozen);
    }

    private void routeOrHold(Page page) {
        boolean ready;
        try {
            ready = spill.writerReady();
        } catch (RuntimeException e) {
            page.releaseBlocks();
            throw e;
        }
        if (ready) {
            route(page);
        } else {
            held = page;
        }
    }

    /**
     * The frozen loop: a row of a group the hash holds merges into it through the aggregators' intermediate input, a
     * row of any other group goes to its partition's run. Membership is the frozen set's, not the lookup's: a hash
     * that reserves an id for null answers it for a null key it has never seen.
     */
    private void route(Page page) {
        long start = System.nanoTime();
        try {
            int positionCount = page.getPositionCount();
            int[] present = new int[positionCount];
            int[] presentIds = new int[positionCount];
            int presentCount = 0;
            int[][] byPartition = new int[SpillReplay.PARTITIONS][];
            int[] partitionCounts = new int[SpillReplay.PARTITIONS];
            int positionOffset = 0;
            try (ReleasableIterator<IntBlock> lookups = blockHash.lookup(page, LOOKUP_TARGET_BYTES)) {
                while (lookups.hasNext()) {
                    try (IntBlock ids = lookups.next()) {
                        for (int p = 0; p < ids.getPositionCount(); p++) {
                            int pos = positionOffset + p;
                            if (ids.isNull(p) == false) {
                                if (ids.getValueCount(p) != 1) {
                                    throw new IllegalStateException("a key with several values in an intermediate page");
                                }
                                int id = ids.getInt(ids.getFirstValueIndex(p));
                                if (frozenGroups.get(id)) {
                                    present[presentCount] = pos;
                                    presentIds[presentCount++] = id;
                                    continue;
                                }
                            }
                            PagedBytesCursor key = keyEncoder.encode(page, pos);
                            int partition = spill.partitionOf(key);
                            int[] positions = byPartition[partition];
                            if (positions == null) {
                                positions = new int[8];
                                byPartition[partition] = positions;
                            } else if (partitionCounts[partition] == positions.length) {
                                positions = Arrays.copyOf(positions, positions.length * 2);
                                byPartition[partition] = positions;
                            }
                            positions[partitionCounts[partition]++] = pos;
                        }
                        positionOffset += ids.getPositionCount();
                    }
                }
            }
            hashNanos += System.nanoTime() - start;
            if (presentCount > 0) {
                long aggStart = System.nanoTime();
                Page presentPage = presentCount == positionCount ? page.shallowCopy() : page.filter(false, present, 0, presentCount);
                try (
                    IntVector ids = driverContext.blockFactory().newIntArrayVector(Arrays.copyOf(presentIds, presentCount), presentCount)
                ) {
                    for (GroupingAggregator aggregator : aggregators) {
                        try (GroupingAggregatorFunction.AddInput add = aggregator.prepareProcessPage(blockHash, presentPage)) {
                            if (add != null) {
                                add.add(0, ids);
                            }
                        }
                    }
                } finally {
                    presentPage.releaseBlocks();
                }
                rowsAddedInCurrentBatch += presentCount;
                aggregationNanos += System.nanoTime() - aggStart;
            }
            for (int partition = 0; partition < SpillReplay.PARTITIONS; partition++) {
                if (partitionCounts[partition] == 0) {
                    continue;
                }
                Page part = page.filter(false, byPartition[partition], 0, partitionCounts[partition]);
                try {
                    spill.append(partition, part);
                } finally {
                    part.releaseBlocks();
                }
            }
        } finally {
            pagesProcessed++;
            rowsReceived += page.getPositionCount();
            page.releaseBlocks();
        }
    }

    /**
     * The output after the frozen state's own: a held page routed once the writer exists, then the replay's pages.
     */
    @Nullable
    private Page outputSpilling() {
        spill.checkSpillFailure();
        if (held != null) {
            if (spill.writerReady() == false) {
                return null;
            }
            Page page = held;
            held = null;
            route(page);
            spill.maybeStartFinishing(finished, false);
            if (finished) {
                // the frozen groups, with the held page's rows of them merged, go out before the replay
                emit();
                return nextOutputPage();
            }
        }
        if (spill.frozen() == false || finished == false || output != null) {
            return null;
        }
        Page out = spill.advanceReplay();
        if (out != null) {
            rowsEmitted += out.getPositionCount();
        }
        return out;
    }

    private void releaseFrozenState() {
        Releasables.close(blockHash, () -> Releasables.close(aggregators), frozenGroups, keyEncoder);
        blockHash = null;
        aggregators.clear();
        frozenGroups = null;
        keyEncoder = null;
    }

    SpillStatus spillStatus() {
        return spill == null ? SpillStatus.NONE : spill.spillStatus();
    }

    /**
     * What the replay needs of this operator: its size, a child for a partition, the frozen state's release and a
     * child's counters. The size is an estimate: the keys as the partitioned hash measures them, or a few dozen bytes
     * per group, and a few words of state per group and aggregator.
     */
    private final class ReplayHost implements SpillReplay.Host {
        @Override
        public long ownBytes() {
            long keys = blockHash instanceof PartitionedBlockHash partitioned
                ? partitioned.estimatedBytesForPartitioning()
                : blockHash.numKeys() * 32L;
            return keys + blockHash.numKeys() * 16L * aggregators.size();
        }

        @Override
        public Operator newReplay(int depth, int seed) {
            return new HashAggregationOperator(
                aggregatorMode,
                aggregatorFactories,
                blockHashSupplier,
                partialEmitKeysThreshold,
                partialEmitUniquenessThreshold,
                maxPageSize,
                topAggregation,
                driverContext,
                null,
                false,
                groups,
                depth,
                seed
            );
        }

        @Override
        public void releaseFrozenState() {
            HashAggregationOperator.this.releaseFrozenState();
        }

        @Override
        public SpillStatus spillStatusOf(Operator replay) {
            return ((HashAggregationOperator) replay).spillStatus();
        }
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
        public static final ProfileSchema<Status> PROFILE_SCHEMA = ProfileSchema.of(
            Status.class,
            (builder, status, params) -> status.writeProfile(builder, params)
        )
            .required("hash_nanos", ProfileSchema.integer())
            .optional("hash_time", ProfileSchema.string())
            .required("aggregation_nanos", ProfileSchema.integer())
            .optional("aggregation_time", ProfileSchema.string())
            .required("pages_processed", ProfileSchema.integer())
            .required("rows_received", ProfileSchema.integer())
            .required("rows_emitted", ProfileSchema.integer())
            .optional("emit_count", ProfileSchema.integer())
            .required("emit_nanos", ProfileSchema.integer())
            .optional("emit_time", ProfileSchema.string())
            .optional("partitioning", ParallelHashAggregationOperator.PartitioningStatus.partitioningSchema())
            .optional("freeze", FreezeStatus.freezeSchema())
            .build();

        private static final TransportVersion ESQL_HASH_OPERATOR_STATUS_OUTPUT_TIME = TransportVersion.fromName(
            "esql_hash_operator_status_output_time"
        );

        private static final TransportVersion ESQL_HASH_OPERATOR_STATUS_EMIT_COUNT = TransportVersion.fromName(
            "esql_hash_operator_status_emit_count"
        );

        private static final TransportVersion ESQL_HASH_OPERATOR_STATUS_EXTRA_FIELDS = TransportVersion.fromName(
            "esql_hash_operator_status_extra_fields"
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

        protected final List<ExtraStatus> extraFields;

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
         * @param extraFields      the extra status such as time-series or partitioning can be attached
         */
        public Status(
            long hashNanos,
            long aggregationNanos,
            int pagesProcessed,
            long rowsReceived,
            long rowsEmitted,
            long emitNanos,
            long emitCount,
            List<ExtraStatus> extraFields
        ) {
            this.hashNanos = hashNanos;
            this.aggregationNanos = aggregationNanos;
            this.pagesProcessed = pagesProcessed;
            this.rowsReceived = rowsReceived;
            this.rowsEmitted = rowsEmitted;
            this.emitNanos = emitNanos;
            this.emitCount = emitCount;
            this.extraFields = extraFields;
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
            if (in.getTransportVersion().supports(ESQL_HASH_OPERATOR_STATUS_EXTRA_FIELDS)) {
                extraFields = in.readNamedWriteableCollectionAsList(Status.ExtraStatus.class);
            } else {
                extraFields = List.of();
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
            if (out.getTransportVersion().supports(ESQL_HASH_OPERATOR_STATUS_EXTRA_FIELDS)) {
                out.writeNamedWriteableCollection(extraFields);
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

        public List<ExtraStatus> extraFields() {
            return extraFields;
        }

        private XContentBuilder writeProfile(XContentBuilder builder, Params params) throws IOException {
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
            for (ExtraStatus ef : extraFields) {
                ef.toXContent(builder, params);
            }
            return builder.endObject();
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            return PROFILE_SCHEMA.write(builder, this, params);
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
                && emitCount == status.emitCount
                && Objects.equals(extraFields, status.extraFields);
        }

        @Override
        public int hashCode() {
            return Objects.hash(hashNanos, aggregationNanos, pagesProcessed, rowsReceived, rowsEmitted, emitNanos, emitCount, extraFields);
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
                final IntVector keys;
                if (aggregatorMode.isOutputPartial() == false && topAggregation != null) {
                    // push down TopN by selecting a subset of keys
                    try (var allKeys = blockHash.nonEmpty()) {
                        keys = aggregators.get(topAggregation.aggregatorIndex())
                            .aggregatorFunction()
                            .selectTopN(allKeys, topAggregation.limit(), topAggregation.asc());
                    }
                } else {
                    try (var allKeys = blockHash.nonEmpty()) {
                        keys = selectedKeysForEmit(ctx, allKeys);
                    }
                }
                selected = new Selected(keys, new IntVector[count]);
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

    /**
     * What a frozen operator reports: the groups it froze, the rows that went to disk, the partitions replayed and the
     * shared spill counters. Present in the status only once the operator froze, which a scope that spills makes
     * possible, so a node without the record never reads it.
     */
    public static class FreezeStatus extends Status.ExtraStatus {
        public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
            Status.ExtraStatus.class,
            "hashagg_freeze",
            FreezeStatus::new
        );
        private static final ProfileSchema<FreezeStatus> FREEZE_SCHEMA = SpillStatus.addTo(
            ProfileSchema.of(FreezeStatus.class, (builder, status, params) -> builder)
        )
            .required("groups_frozen", ProfileSchema.integer())
            .required("rows_spilled", ProfileSchema.integer())
            .required("partitions_replayed", ProfileSchema.integer())
            .build();
        public static final ProfileSchema<FreezeStatus> PROFILE_SCHEMA = ProfileSchema.fragment(
            FreezeStatus.class,
            (builder, status, params) -> {
                status.writeProfile(builder);
                return builder;
            }
        ).required("freeze", FREEZE_SCHEMA.schema()).build();

        static Map<String, Object> freezeSchema() {
            return FREEZE_SCHEMA.schema();
        }

        private final long groupsFrozen;
        private final long rowsSpilled;
        private final int partitionsReplayed;
        private final SpillStatus spill;

        public FreezeStatus(long groupsFrozen, long rowsSpilled, int partitionsReplayed, SpillStatus spill) {
            this.groupsFrozen = groupsFrozen;
            this.rowsSpilled = rowsSpilled;
            this.partitionsReplayed = partitionsReplayed;
            this.spill = spill;
        }

        FreezeStatus(StreamInput in) throws IOException {
            this(in.readVLong(), in.readVLong(), in.readVInt(), SpillStatus.readFrom(in));
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVLong(groupsFrozen);
            out.writeVLong(rowsSpilled);
            out.writeVInt(partitionsReplayed);
            spill.writeTo(out);
        }

        public long groupsFrozen() {
            return groupsFrozen;
        }

        public long rowsSpilled() {
            return rowsSpilled;
        }

        public int partitionsReplayed() {
            return partitionsReplayed;
        }

        public SpillStatus spill() {
            return spill;
        }

        @Override
        public String getWriteableName() {
            return ENTRY.name;
        }

        @Override
        protected void toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
            PROFILE_SCHEMA.write(builder, this, params);
        }

        private void writeProfile(XContentBuilder builder) throws IOException {
            builder.startObject("freeze");
            builder.field("groups_frozen", groupsFrozen);
            builder.field("rows_spilled", rowsSpilled);
            builder.field("partitions_replayed", partitionsReplayed);
            spill.toXContent(builder, ToXContent.EMPTY_PARAMS);
            builder.endObject();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            FreezeStatus other = (FreezeStatus) o;
            return groupsFrozen == other.groupsFrozen
                && rowsSpilled == other.rowsSpilled
                && partitionsReplayed == other.partitionsReplayed
                && spill.equals(other.spill);
        }

        @Override
        public int hashCode() {
            return Objects.hash(groupsFrozen, rowsSpilled, partitionsReplayed, spill);
        }
    }
}
