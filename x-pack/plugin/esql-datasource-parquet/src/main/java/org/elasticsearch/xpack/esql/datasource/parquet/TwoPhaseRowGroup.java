/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.parquet;

import org.elasticsearch.compute.data.Block;
import org.elasticsearch.core.Releasables;

import java.util.List;

/**
 * Per-row-group state for the two-phase late-materialization decode flow.
 *
 * <p>Phase 1 of {@link OptimizedParquetColumnIterator#advanceRowGroup()} fully decodes the
 * predicate columns batch-by-batch (using the same {@code batchSize} the consumer will see),
 * evaluates the filter, and stashes the per-batch survivor positions and the survivor-only
 * predicate {@link Block}s here. Phase 2 then fetches only the projection columns and decodes
 * them lazily when the consumer pulls each batch.
 *
 * <p>Storing the predicate blocks per batch keeps memory usage bounded by survivor count
 * (typically tiny for a selective filter), avoids re-decoding predicate columns once the
 * filter is known, and lets the iterator emit each batch in its original {@code rowsToRead}
 * granularity so downstream operators see the same shape they would under single-phase.
 */
final class TwoPhaseRowGroup {

    /**
     * Compacted predicate-column {@link Block}s per batch, in batch order. Slot {@code [batch][col]}
     * is the survivor-only block for column {@code col} in batch {@code batch}, or {@code null} for
     * columns that are not predicate columns. Ownership is transferred to this object on
     * construction; {@link #close()} closes any blocks that have not been emitted yet.
     */
    private final List<Block[]> predicateBlockBatches;
    /**
     * Per-batch survivor positions within the original (pre-compaction) batch row indices.
     * Slot {@code i} is {@code null} when either every row in batch {@code i} survives the
     * filter (caller treats this as identity) or no row survives (size encoded in
     * {@link #survivorCountsPerBatch}); a non-null array otherwise lists the surviving row
     * offsets in ascending order.
     */
    private final List<int[]> survivorPositionsPerBatch;
    /**
     * Per-batch survivor counts. When {@link #survivorPositionsPerBatch} slot {@code i} is
     * non-null this equals its length; when null the count is either {@code sourceRowsPerBatch[i]}
     * (all-survive) or {@code 0} (none-survive).
     */
    private final int[] survivorCountsPerBatch;
    /** Per-batch source row counts (number of rows decoded in batch {@code i}, before filtering). */
    private final int[] sourceRowsPerBatch;
    /** Total surviving rows across all batches in this row group. */
    private final long totalSurvivors;
    /** Projection-column page read store keyed for the projection-only columns. */
    private final PrefetchedPageReadStore projectionStore;
    /** Survivor row ranges across the row group, used to seed projection {@code PageColumnReader}s. */
    private final RowRanges survivorRowRanges;
    /** Cursor into {@link #predicateBlockBatches} tracking the next batch to emit. */
    private int nextBatchIndex;

    TwoPhaseRowGroup(
        List<Block[]> predicateBlockBatches,
        List<int[]> survivorPositionsPerBatch,
        int[] survivorCountsPerBatch,
        int[] sourceRowsPerBatch,
        long totalSurvivors,
        PrefetchedPageReadStore projectionStore,
        RowRanges survivorRowRanges
    ) {
        assert predicateBlockBatches.size() == survivorPositionsPerBatch.size();
        assert predicateBlockBatches.size() == survivorCountsPerBatch.length;
        assert predicateBlockBatches.size() == sourceRowsPerBatch.length;
        this.predicateBlockBatches = predicateBlockBatches;
        this.survivorPositionsPerBatch = survivorPositionsPerBatch;
        this.survivorCountsPerBatch = survivorCountsPerBatch;
        this.sourceRowsPerBatch = sourceRowsPerBatch;
        this.totalSurvivors = totalSurvivors;
        this.projectionStore = projectionStore;
        this.survivorRowRanges = survivorRowRanges;
    }

    int batchCount() {
        return predicateBlockBatches.size();
    }

    int nextBatchIndex() {
        return nextBatchIndex;
    }

    boolean hasMoreBatches() {
        return nextBatchIndex < predicateBlockBatches.size();
    }

    int currentSourceRows() {
        return sourceRowsPerBatch[nextBatchIndex];
    }

    int currentSurvivorCount() {
        return survivorCountsPerBatch[nextBatchIndex];
    }

    int[] currentSurvivorPositions() {
        return survivorPositionsPerBatch.get(nextBatchIndex);
    }

    /**
     * Returns the predicate block array for the current batch, transferring ownership of every
     * non-null entry to the caller. Subsequent reads of the same batch return arrays whose
     * predicate slots are {@code null}, since the blocks now belong to the caller. The cursor
     * is advanced as a side effect, so a single call serves a single batch.
     */
    Block[] takeCurrentPredicateBlocks() {
        Block[] batch = predicateBlockBatches.get(nextBatchIndex);
        // Replace the owned references with null so close() does not double-release any block
        // that is now held by the caller's emitted Page.
        Block[] handed = batch.clone();
        for (int i = 0; i < batch.length; i++) {
            batch[i] = null;
        }
        nextBatchIndex++;
        return handed;
    }

    long totalSurvivors() {
        return totalSurvivors;
    }

    PrefetchedPageReadStore projectionStore() {
        return projectionStore;
    }

    RowRanges survivorRowRanges() {
        return survivorRowRanges;
    }

    /**
     * Closes any predicate blocks that were not handed out. The projection store is not closed
     * here — the iterator's {@code rowGroup} field owns it (or a wrapper around it) and closes
     * it via {@link PrefetchedPageReadStore#close()} on row-group advance.
     */
    void close() {
        for (Block[] batch : predicateBlockBatches) {
            if (batch == null) {
                continue;
            }
            Releasables.closeExpectNoException(batch);
        }
    }
}
