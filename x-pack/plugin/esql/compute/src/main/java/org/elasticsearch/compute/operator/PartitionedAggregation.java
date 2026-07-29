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

import java.util.ArrayList;
import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * Static utilities shared by {@link PartitionedHashAggregationOperator} and
 * {@link PartitionedHashMergeOperator}.
 */
final class PartitionedAggregation {

    private PartitionedAggregation() {}

    /** Partition that every null grouping key is routed to. */
    static final int NULL_PARTITION = 0;

    static final GroupingAggregatorPageBuilder.CustomizeSelected NO_CUSTOMIZATION = (aggregator, selected) -> {
        selected.incRef();
        return selected;
    };

    /** Result of {@link #sortPositionsByPartition}: per-partition offsets and sorted row indices. */
    record BucketSort(int[] offsets, int[] sortedPositions) {}

    /**
     * Assigns each row in {@code page} to a partition, filling {@code partitionOf[i]} with the
     * partition index for row {@code i} and incrementing {@code counts[partition]} for each row.
     * Null grouping keys are routed to {@link #NULL_PARTITION}.
     * When no {@link BlockHash.Router} is available for the current grouping shape, all rows are
     * routed to {@link #NULL_PARTITION}.
     */
    static void fillPartitionAssignments(BlockHash probeHash, int keyCount, Page page, int nPartitions, int[] partitionOf, int[] counts) {
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
    static BucketSort sortPositionsByPartition(int[] partitionOf, int[] counts, int nPartitions) {
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

    /**
     * Validates {@code externalSpecs} and derives internal group specs with channels remapped
     * to 0..keyCount-1. Used by {@link PartitionedHashMergeOperator.Factory} to build the
     * internal blockHash configuration for noneOp and workerOps.
     */
    static List<BlockHash.GroupSpec> buildInternalGroupSpecs(List<BlockHash.GroupSpec> externalSpecs) {
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

    /**
     * Evaluates {@code op}'s current aggregator state to pages using {@code maxPageSizeOverride}
     * as the page-size cap. Pass {@link Integer#MAX_VALUE} for intermediate-only evaluations with
     * no size limit (used when re-ingesting results internally); pass the operator's
     * {@code maxPageSize} for normal emission.
     */
    static ReleasableIterator<Page> evaluateOp(HashAggregationOperator op, int maxPageSizeOverride, DriverContext driverContext) {
        var pageBuilder = new GroupingAggregatorPageBuilder(op.blockHash, op.aggregators, maxPageSizeOverride, NO_CUSTOMIZATION);
        return pageBuilder.build(new GroupingAggregatorEvaluationContext(driverContext));
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
                org.elasticsearch.core.Releasables.close(delegate, op);
            }
        };
    }

    static void checkState(boolean condition, String msg) {
        if (condition == false) {
            throw new IllegalArgumentException(msg);
        }
    }
}
