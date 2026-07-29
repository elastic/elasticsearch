/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.compute.aggregation.GroupingAggregatorEvaluationContext;
import org.elasticsearch.compute.aggregation.blockhash.BlockHash;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.ReleasableIterator;
import org.elasticsearch.core.Releasables;

import java.util.ArrayList;
import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * Shared base for {@link PartitionedHashMergeOperator}.
 *
 * <p>Holds the routing hash, output iterator, and helper methods for operator evaluation,
 * partition assignment, and state checks.
 */
abstract class AbstractPartitionedHashAggregationOperator implements Operator {

    /** Partition that every null grouping key is routed to. */
    static final int NULL_PARTITION = 0;

    static final GroupingAggregatorPageBuilder.CustomizeSelected NO_CUSTOMIZATION = (aggregator, selected) -> {
        selected.incRef();
        return selected;
    };

    // ---- Shared instance fields ----

    /** Number of grouping keys. */
    protected final int keyCount;
    protected final int partitionCount;
    protected final int maxPageSize;
    protected final DriverContext driverContext;

    /**
     * Routing-only hash used to compute partition IDs via {@link BlockHash.Router#partitionHashOfRow};
     * never used for aggregation.
     */
    protected BlockHash probeHash;
    /** Iterator of output pages; non-null while the operator has output to drain. */
    protected ReleasableIterator<Page> output;
    /** Set to {@code true} once {@link #finish()} has been called. */
    protected boolean finishCalled;

    protected AbstractPartitionedHashAggregationOperator(int keyCount, int partitionCount, int maxPageSize, DriverContext driverContext) {
        this.keyCount = keyCount;
        this.partitionCount = partitionCount;
        this.maxPageSize = maxPageSize;
        this.driverContext = driverContext;
    }

    @Override
    public boolean needsInput() {
        return output == null && finishCalled == false;
    }

    /**
     * Evaluates {@code op}'s current aggregator state to pages using {@code maxPageSizeOverride}
     * as the page-size cap. Pass {@link Integer#MAX_VALUE} for intermediate-only evaluations with
     * no size limit (used when re-ingesting results internally); pass {@link #maxPageSize} for
     * normal emission.
     */
    protected final ReleasableIterator<Page> evaluateOp(HashAggregationOperator op, int maxPageSizeOverride) {
        var pageBuilder = new GroupingAggregatorPageBuilder(op.blockHash, op.aggregators, maxPageSizeOverride, NO_CUSTOMIZATION);
        return pageBuilder.build(new GroupingAggregatorEvaluationContext(driverContext));
    }

    // ---- Shared routing helpers ----

    /**
     * Assigns each row in {@code page} to a partition, filling {@code partitionOf[i]} with the
     * partition index for row {@code i} and incrementing {@code counts[partition]} for each row.
     * Null grouping keys are routed to {@link #NULL_PARTITION}.
     * When no {@link BlockHash.Router} is available for the current grouping shape, all rows are
     * routed to {@link #NULL_PARTITION}.
     */
    protected static void fillPartitionAssignments(
        BlockHash probeHash,
        int keyCount,
        Page page,
        int nPartitions,
        int[] partitionOf,
        int[] counts
    ) {
        BlockHash.Router router = probeHash.router();
        if (router == null) {
            counts[NULL_PARTITION] = page.getPositionCount();
            // partitionOf is already all zeros (NULL_PARTITION) — Java default
            return;
        }
        router.fillPartitions(page, page.getPositionCount(), keyCount, nPartitions, NULL_PARTITION, partitionOf, counts);
    }

    /**
     * Stable bucket-sort of row indices by partition. Given {@code partitionOf[i]} (the partition
     * for each row) and {@code counts[p]} (number of rows in each partition), returns a
     * {@link BucketSort} containing:
     * <ul>
     *   <li>{@code offsets[p]} — the start index in {@code sortedPositions} for partition {@code p}</li>
     *   <li>{@code sortedPositions} — row indices grouped by partition, in original row order within each partition</li>
     * </ul>
     */
    protected static BucketSort sortPositionsByPartition(int[] partitionOf, int[] counts, int nPartitions) {
        int[] offsets = new int[nPartitions + 1];
        for (int p = 0; p < nPartitions; p++) {
            offsets[p + 1] = offsets[p] + counts[p];
        }
        int[] cursor = offsets.clone();
        int[] sortedPositions = new int[partitionOf.length];
        for (int i = 0; i < partitionOf.length; i++) {
            sortedPositions[cursor[partitionOf[i]]++] = i;
        }
        return new BucketSort(offsets, sortedPositions);
    }

    /** Result of {@link #sortPositionsByPartition}: per-partition offsets and sorted row indices. */
    protected record BucketSort(int[] offsets, int[] sortedPositions) {}

    // ---- Shared statics ----

    protected static void checkState(boolean condition, String msg) {
        if (condition == false) {
            throw new IllegalArgumentException(msg);
        }
    }

    /** Wraps {@code delegate} so that closing the iterator also closes {@code op}. */
    static ReleasableIterator<Page> closeOpOnClose(ReleasableIterator<Page> delegate, HashAggregationOperator op) {
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
                Releasables.close(delegate, op);
            }
        };
    }

    /**
     * Validates {@code externalSpecs} and derives internal group specs with channels remapped
     * to 0..keyCount-1. Used by {@link PartitionedHashMergeOperator.Factory} to build the
     * internal blockHash configuration for noneOp and workerOps.
     */
    protected static List<BlockHash.GroupSpec> buildInternalGroupSpecs(List<BlockHash.GroupSpec> externalSpecs) {
        requireNonNull(externalSpecs, "groupSpecs");
        if (externalSpecs.isEmpty()) {
            throw new IllegalArgumentException("groupSpecs must not be empty");
        }
        List<BlockHash.GroupSpec> internalSpecs = new ArrayList<>(externalSpecs.size());
        for (int k = 0; k < externalSpecs.size(); k++) {
            internalSpecs.add(new BlockHash.GroupSpec(k, externalSpecs.get(k).elementType()));
        }
        return List.copyOf(internalSpecs);
    }
}
