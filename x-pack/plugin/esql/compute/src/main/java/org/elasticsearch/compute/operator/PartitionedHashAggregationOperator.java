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
import org.elasticsearch.compute.data.Page;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.IntUnaryOperator;

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
    public static final int DEFAULT_EMIT_KEYS_THRESHOLD = 400_000;

    /**
     * Default key-count threshold for deciding whether to partition the final output when no
     * intermediate emit has occurred. Below this threshold the operator emits an untagged page,
     * which {@link PartitionedHashMergeOperator} handles on its driver thread without spawning
     * workers. Above it the operator partitions just as it would for an intermediate emit.
     */
    public static final int DEFAULT_PARTITION_THRESHOLD = 10_000;

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

    /** An aggregator plus the raw-input channel(s) it reads from on the data node. */
    public record AggregatorSpec(AggregatorFunctionSupplier supplier, List<Integer> rawChannels) {}

    public static class Builder {
        private List<BlockHash.GroupSpec> groupSpecs;
        private List<AggregatorSpec> aggregators;
        private int partitionCount = DEFAULT_PARTITION_COUNT;
        private int emitKeysThreshold = DEFAULT_EMIT_KEYS_THRESHOLD;
        private int partitionThreshold = DEFAULT_PARTITION_THRESHOLD;
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

        public Builder partitionThreshold(int partitionThreshold) {
            this.partitionThreshold = partitionThreshold;
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
        private final List<BlockHash.GroupSpec> groupSpecs;
        private final List<AggregatorSpec> aggregatorSpecs;
        private final List<GroupingAggregator.Factory> aggregatorFactories;
        private final int partitionCount;
        private final int emitKeysThreshold;
        private final int partitionThreshold;
        private final int maxPageSize;
        private final int aggregationBatchSize;

        private Factory(Builder builder) {
            this.groupSpecs = Objects.requireNonNull(builder.groupSpecs, "groupSpecs");
            this.aggregatorSpecs = Objects.requireNonNull(builder.aggregators, "aggregators");

            List<GroupingAggregator.Factory> factories = new ArrayList<>(aggregatorSpecs.size());
            for (AggregatorSpec spec : aggregatorSpecs) {
                AggregatorFunctionSupplier supplier = spec.supplier();
                List<Integer> rawChannels = List.copyOf(spec.rawChannels());
                factories.add(supplier.groupingAggregatorFactory(AggregatorMode.INITIAL, rawChannels));
            }
            this.aggregatorFactories = List.copyOf(factories);

            this.partitionCount = builder.partitionCount;
            this.emitKeysThreshold = builder.emitKeysThreshold;
            this.partitionThreshold = builder.partitionThreshold;
            this.maxPageSize = builder.maxPageSize;
            this.aggregationBatchSize = builder.aggregationBatchSize;
        }

        @Override
        public PartitionedHashAggregationOperator get(DriverContext driverContext) {
            return new PartitionedHashAggregationOperator(
                groupSpecs,
                aggregatorFactories,
                partitionCount,
                emitKeysThreshold,
                partitionThreshold,
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
                + ", partitionThreshold="
                + partitionThreshold
                + ", aggs="
                + aggregatorSpecs.stream().map(s -> s.supplier().describe()).collect(joining(", "))
                + "]";
        }
    }

    // ---- Instance fields (beyond those inherited from HashAggregationOperator) ----

    private final int partitionCount;
    private final int emitKeysThreshold;
    private final int partitionThreshold;
    /**
     * Set to {@code true} on the first intermediate emit; once set, all subsequent emits (including
     * the final one) are partitioned regardless of key count.
     */
    private boolean usePartitioning;

    @SuppressWarnings("this-escape")
    PartitionedHashAggregationOperator(
        List<BlockHash.GroupSpec> groupSpecs,
        List<GroupingAggregator.Factory> aggregatorFactories,
        int partitionCount,
        int emitKeysThreshold,
        int partitionThreshold,
        int maxPageSize,
        int aggregationBatchSize,
        DriverContext driverContext
    ) {
        super(
            AggregatorMode.INITIAL,
            aggregatorFactories,
            () -> BlockHash.build(groupSpecs, driverContext.blockFactory(), aggregationBatchSize, false),
            Integer.MAX_VALUE, // shouldEmitPartialResultsPeriodically() is overridden so this parameter is not used
            1.0,
            maxPageSize,
            null,
            driverContext,
            null // INITIAL-mode PHAO does not promote
        );
        if (partitionCount <= 0) {
            throw new IllegalArgumentException("partitionCount must be greater than 0; got " + partitionCount);
        }
        if (emitKeysThreshold <= 0) {
            throw new IllegalArgumentException("emitKeysThreshold must be greater than 0; got " + emitKeysThreshold);
        }
        this.partitionCount = partitionCount;
        this.emitKeysThreshold = emitKeysThreshold;
        this.partitionThreshold = partitionThreshold;
    }

    /**
     * Replaces HAO's two-condition self-emit check (key count + uniqueness ratio) with a simpler
     * key-count gate: emit when the number of accumulated keys reaches {@link #emitKeysThreshold}.
     */
    @Override
    protected boolean shouldEmitPartialResultsPeriodically() {
        return blockHash.numKeys() >= emitKeysThreshold;
    }

    /**
     * Evaluates the accumulated table to intermediate pages and either partitions them (tagging each
     * sub-page with its partition id) or emits them untagged for the small-query path.
     * <p>
     *     {@link #usePartitioning} is latched to {@code true} when the key count meets or exceeds
     *     {@link #partitionThreshold}. Once latched, all subsequent emits — including the final one —
     *     also partition, so the coordinator always sees a consistent stream of tagged pages.
     * </p>
     */
    @Override
    protected void emit() {
        if (rowsAddedInCurrentBatch == 0) {
            return;
        }
        long emitStart = System.nanoTime();
        try {
            int numKeys = blockHash.numKeys();
            if (numKeys > 0) {
                usePartitioning |= numKeys >= partitionThreshold;
                var pageBuilder = new GroupingAggregatorPageBuilder(blockHash, aggregators, Integer.MAX_VALUE, this::customizeSelected);
                var groupingAggEvaluationContext = new GroupingAggregatorEvaluationContext(driverContext);
                if (usePartitioning) {
                    IntUnaryOperator partitioner = blockHash.partitioner(partitionCount);
                    assert partitioner != null : "partitioner is null";
                    output = pageBuilder.buildPartitioned(partitionCount, partitioner, groupingAggEvaluationContext);
                } else {
                    output = pageBuilder.build(groupingAggEvaluationContext);
                }
            }
        } finally {
            rowsAddedInCurrentBatch = 0;
            emitNanos += System.nanoTime() - emitStart;
            emitCount++;
        }
    }

    @Override
    public String toString() {
        return getClass().getSimpleName()
            + "[partitionCount="
            + partitionCount
            + ", emitKeysThreshold="
            + emitKeysThreshold
            + ", partitionThreshold="
            + partitionThreshold
            + "]";
    }

}
