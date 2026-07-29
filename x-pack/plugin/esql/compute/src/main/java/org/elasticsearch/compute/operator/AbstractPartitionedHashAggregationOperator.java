/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.compute.aggregation.GroupingAggregatorEvaluationContext;
import org.elasticsearch.compute.aggregation.blockhash.BlockHash;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.ReleasableIterator;
import org.elasticsearch.core.Releasables;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * Shared base for {@link PartitionedHashAggregationOperator} and {@link PartitionedHashMergeOperator}.
 *
 * <p>Holds the channel-mapping fields, routing hash, output iterator, and helper methods
 * both operators use for page layout remapping, operator evaluation, and state checks.
 * Both operators compose N+1 {@link HashAggregationOperator} instances internally and use
 * {@link #toInternalLayout} to normalize external channels to a compact internal layout.
 */
abstract class AbstractPartitionedHashAggregationOperator implements Operator {

    /** Partition that every null grouping key is routed to. */
    static final int NULL_PARTITION = 0;

    static final GroupingAggregatorPageBuilder.CustomizeSelected NO_CUSTOMIZATION = (aggregator, selected) -> {
        selected.incRef();
        return selected;
    };

    // ---- Shared instance fields ----

    /** External channel index for each grouping key. */
    protected final List<Integer> groupChannels;
    /** Remapped group specs with channels 0..keyCount-1 (the internal convention). */
    protected final List<BlockHash.GroupSpec> internalGroupSpecs;
    /**
     * Per-aggregator list of external channel indices used by {@link #toInternalLayout}.
     * {@link PartitionedHashAggregationOperator} passes raw-input channels;
     * {@link PartitionedHashMergeOperator} passes intermediate-state channels.
     */
    protected final List<List<Integer>> aggregatorChannels;
    /** Offset into the internal page where each aggregator's state blocks begin. */
    protected final int[] combinedChannelStart;
    /** Total number of blocks in an internal-layout page. */
    protected final int internalPageWidth;
    protected final int partitionCount;
    protected final int maxPageSize;
    protected final DriverContext driverContext;

    /**
     * Routing-only hash used to compute partition IDs via {@link BlockHash.Router#partitionHashOfRow};
     * never used for aggregation. {@link PartitionedHashAggregationOperator} sets it lazily in
     * {@code convertToPartitioned}; {@link PartitionedHashMergeOperator} sets it in the constructor.
     */
    protected BlockHash probeHash;
    /** Iterator of output pages; non-null while the operator has output to drain. */
    protected ReleasableIterator<Page> output;
    /** Set to {@code true} once {@link #finish()} has been called. */
    protected boolean finishCalled;

    protected AbstractPartitionedHashAggregationOperator(
        List<Integer> groupChannels,
        List<BlockHash.GroupSpec> internalGroupSpecs,
        List<List<Integer>> aggregatorChannels,
        int[] combinedChannelStart,
        int internalPageWidth,
        int partitionCount,
        int maxPageSize,
        DriverContext driverContext
    ) {
        this.groupChannels = groupChannels;
        this.internalGroupSpecs = internalGroupSpecs;
        this.aggregatorChannels = aggregatorChannels;
        this.combinedChannelStart = combinedChannelStart;
        this.internalPageWidth = internalPageWidth;
        this.partitionCount = partitionCount;
        this.maxPageSize = maxPageSize;
        this.driverContext = driverContext;
    }

    @Override
    public boolean needsInput() {
        return output == null && finishCalled == false;
    }

    /**
     * Rearranges {@code page} into the internal channel convention: grouping keys at channels
     * 0..keyCount-1, each aggregator's blocks at {@link #combinedChannelStart}[i] onward.
     * <p>
     *     Builds a new {@link Page} that references the original blocks directly (no value
     *     copies). Never {@link Page#close}/{@link Page#releaseBlocks} it — {@code page} itself
     *     remains the sole owner. Slots beyond an aggregator's actual channel count (valid only
     *     for intermediate consumption) are filled with the first key block so that
     *     {@code Page}'s position-count assertion always holds.
     * </p>
     */
    protected final Page toInternalLayout(Page page) {
        Block[] blocks = new Block[internalPageWidth];
        Arrays.fill(blocks, page.getBlock(groupChannels.get(0)));
        for (int k = 0; k < groupChannels.size(); k++) {
            blocks[k] = page.getBlock(groupChannels.get(k));
        }
        for (int a = 0; a < aggregatorChannels.size(); a++) {
            List<Integer> channels = aggregatorChannels.get(a);
            int base = combinedChannelStart[a];
            for (int j = 0; j < channels.size(); j++) {
                blocks[base + j] = page.getBlock(channels.get(j));
            }
        }
        return new Page(blocks);
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
                Releasables.close(delegate::close, op);
            }
        };
    }

    /**
     * Validates {@code externalSpecs}, then derives the internal channel mapping:
     * {@code groupChannels} (external channel per key) and {@code internalGroupSpecs}
     * (remapped so key channels are 0..keyCount-1). Both factories use this as the first
     * step of their constructors to eliminate the duplicated spec-derivation loop.
     */
    protected static GroupChannelMapping buildGroupChannelMapping(List<BlockHash.GroupSpec> externalSpecs) {
        requireNonNull(externalSpecs, "groupSpecs");
        if (externalSpecs.isEmpty()) {
            throw new IllegalArgumentException("groupSpecs must not be empty");
        }
        List<Integer> channels = new ArrayList<>(externalSpecs.size());
        List<BlockHash.GroupSpec> internalSpecs = new ArrayList<>(externalSpecs.size());
        for (int k = 0; k < externalSpecs.size(); k++) {
            channels.add(externalSpecs.get(k).channel());
            internalSpecs.add(new BlockHash.GroupSpec(k, externalSpecs.get(k).elementType()));
        }
        return new GroupChannelMapping(List.copyOf(channels), List.copyOf(internalSpecs));
    }

    protected record GroupChannelMapping(List<Integer> groupChannels, List<BlockHash.GroupSpec> internalGroupSpecs) {}
}
