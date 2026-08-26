/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.parser.ParsingException;

import java.util.Arrays;

/**
 * Per-iterator helper for columnar readers (Parquet, ORC) implementing {@code error_mode: skip_row}.
 *
 * <h2>What this solves</h2>
 * A columnar batch presents all column values for the same set of rows simultaneously; a cell-level
 * coercion failure (numeric overflow, unparseable date string) must therefore drop the <em>whole row</em>
 * from the emitted page, not just null-fill the failing cell. This class accumulates per-cell failure
 * positions across all decoded columns for a batch and, at the emit point, applies {@link Block#filter}
 * to compact the page to the surviving (non-failed) rows. It also enforces the error budget
 * ({@code max_errors} / {@code max_error_ratio}) cumulatively across all batches for the iterator's
 * lifetime.
 *
 * <h2>Lifecycle</h2>
 * <ol>
 *   <li>Create once per iterator via {@link #forPolicy} — returns {@code null} for non-{@code SKIP_ROW}
 *       modes so callers hold a nullable reference and pay zero overhead on the hot non-skip path.</li>
 *   <li>Per batch: call {@link #beginBatch(int)} with the number of source rows in the batch, then
 *       call {@link #markFailed(int)} for every coercion failure position (idempotent: multiple column
 *       failures on the same row count as one dropped row).</li>
 *   <li>At the emit point: call {@link #filterBlocks(Block[], BlockFactory)} to compact the blocks and
 *       remove all failed rows; call {@link #addToTotals(int, int)} to update the cumulative budget
 *       counters; call {@link #checkBudget()} to throw a {@link ParsingException} (HTTP 400) if the
 *       configured error budget is exceeded.</li>
 * </ol>
 *
 * <h2>Coordinate spaces</h2>
 * All positions passed to {@link #markFailed} must be in the same coordinate space as the blocks
 * passed to {@link #filterBlocks} — typically the batch-level page coordinates where position 0 is
 * the first row in the current batch. Callers on paths that pre-filter rows (late-materialization,
 * two-phase predicate evaluation) must perform the coordinate transform before calling this helper.
 *
 * <h2>Exception contract</h2>
 * Budget-exceeded failures surface as {@link ParsingException} (an HTTP 400 client-data error), matching
 * {@code CsvFormatReader.checkBudget}. NDJSON diverges with {@code EsqlIllegalArgumentException}; this
 * class uses {@link ParsingException} so a third variant is never introduced.
 */
public final class ColumnarRowDropHelper {

    private final ErrorPolicy policy;
    private final SkipWarnings warnings;
    private final String fileLocation;

    /** Cumulative error count (dropped rows) across all batches this iterator has processed. */
    private long errorCount;
    /** Cumulative source-row count across all batches this iterator has processed. */
    private long rowCount;

    /** Number of positions in the current batch. Set by {@link #beginBatch}. */
    private int batchSize;
    /**
     * Per-batch failure flags. Index = position in the current batch; {@code true} = at least one
     * coercion failure in that row (across any column). Allocated lazily and reused across batches
     * to avoid per-batch allocation in the hot path.
     */
    private boolean[] failed;
    /** Count of distinct failed positions in the current batch. */
    private int failedCount;

    private ColumnarRowDropHelper(ErrorPolicy policy, SkipWarnings warnings, String fileLocation) {
        this.policy = policy;
        this.warnings = warnings;
        this.fileLocation = fileLocation;
    }

    /**
     * Factory method: returns a new helper when the error policy is {@link ErrorPolicy.Mode#SKIP_ROW},
     * or {@code null} for any other mode. Callers store the result as a nullable field; a null reference
     * means no helper is active and all per-batch logic should be skipped.
     */
    @Nullable
    public static ColumnarRowDropHelper forPolicy(@Nullable ErrorPolicy policy, @Nullable SkipWarnings warnings, String fileLocation) {
        if (policy != null && policy.mode() == ErrorPolicy.Mode.SKIP_ROW) {
            return new ColumnarRowDropHelper(policy, warnings != null ? warnings : new SkipWarnings(fileLocation), fileLocation);
        }
        return null;
    }

    /**
     * Resets the per-batch state for a new batch of {@code positions} rows. Must be called before
     * any {@link #markFailed} calls for a new batch.
     */
    public void beginBatch(int positions) {
        this.batchSize = positions;
        this.failedCount = 0;
        if (failed == null || failed.length < positions) {
            failed = new boolean[positions];
        } else {
            Arrays.fill(failed, 0, positions, false);
        }
    }

    /**
     * Marks {@code position} as having a coercion failure in the current batch. Idempotent: calling
     * multiple times for the same position (failures in different columns for the same row) counts
     * as one dropped row.
     */
    public void markFailed(int position) {
        assert position >= 0 && position < batchSize : "position " + position + " out of batch [0, " + batchSize + ")";
        if (failed[position] == false) {
            failed[position] = true;
            failedCount++;
        }
    }

    /** Returns {@code true} if at least one position has been marked failed in the current batch. */
    public boolean hasFailures() {
        return failedCount > 0;
    }

    /** Number of distinct positions marked failed in the current batch. */
    public int failedCount() {
        return failedCount;
    }

    /**
     * Filters the blocks to retain only non-failed positions. Ownership of each block in {@code blocks}
     * is transferred to this method: each block is either closed (and replaced with a new filtered
     * block) or returned as-is if {@link #hasFailures()} is false.
     *
     * <p>Does NOT update cumulative error/row totals or check the budget; call
     * {@link #addToTotals} and {@link #checkBudget} separately.
     *
     * @param blocks block array whose elements are owned by this call (may contain {@code null} slots
     *               that the caller will fill with constant-null blocks afterwards)
     * @param blockFactory factory for replacement blocks (used when all rows are dropped)
     * @return the same array with each non-null block replaced by its filtered version;
     *         null slots are left unchanged
     */
    public Block[] filterBlocks(Block[] blocks, BlockFactory blockFactory) {
        if (failedCount == 0) {
            return blocks;
        }
        int survivorCount = batchSize - failedCount;
        try {
            if (survivorCount == 0) {
                for (int col = 0; col < blocks.length; col++) {
                    Block b = blocks[col];
                    if (b == null) {
                        continue;
                    }
                    b.close();
                    blocks[col] = blockFactory.newConstantNullBlock(0);
                }
            } else {
                int[] survivors = buildSurvivorPositions(survivorCount);
                for (int col = 0; col < blocks.length; col++) {
                    Block b = blocks[col];
                    if (b == null) {
                        continue;
                    }
                    Block filtered = b.filter(false, survivors);
                    b.close();
                    blocks[col] = filtered;
                }
            }
        } catch (RuntimeException e) {
            // Null slots before closing so any re-close by the caller (e.g. Releasables.closeExpectNoException)
            // sees null and skips them, keeping "already released" off the suppressed list of the real error.
            for (int i = 0; i < blocks.length; i++) {
                Block b = blocks[i];
                if (b != null) {
                    blocks[i] = null;
                    try {
                        b.close();
                    } catch (RuntimeException closeEx) {
                        e.addSuppressed(closeEx);
                    }
                }
            }
            throw e;
        }
        return blocks;
    }

    /**
     * Builds the sorted survivor-position array: indices in [0, {@link #batchSize}) where
     * {@code failed[i]} is false, in ascending order.
     */
    private int[] buildSurvivorPositions(int survivorCount) {
        int[] survivors = new int[survivorCount];
        int si = 0;
        for (int i = 0; i < batchSize; i++) {
            if (failed[i] == false) {
                survivors[si++] = i;
            }
        }
        return survivors;
    }

    /**
     * Updates cumulative error and row totals. Call once per batch after all per-batch accounting
     * is complete.
     *
     * @param sourceRows the number of source rows in the batch (before any coercion-failure filtering)
     * @param errors     the total number of dropped rows in this batch (may differ from
     *                   {@link #failedCount()} when predicate-column failures on the late-materialization
     *                   path are counted separately)
     */
    public void addToTotals(int sourceRows, int errors) {
        this.rowCount += sourceRows;
        this.errorCount += errors;
    }

    /**
     * Checks whether the error budget has been exceeded and throws a {@link ParsingException}
     * (HTTP 400 — client-data problem) if so. Emits a budget-exceeded warning before throwing,
     * matching {@code CsvFormatReader.checkBudget}'s contract so clients see which batch tripped
     * the limit even when the request fails.
     */
    public void checkBudget() {
        if (policy.isBudgetExceeded(errorCount, rowCount)) {
            warnings.add(
                "Columnar error budget exceeded at ["
                    + fileLocation
                    + "]: ["
                    + errorCount
                    + "] dropped rows in ["
                    + rowCount
                    + "] decoded rows, maximum ["
                    + policy.maxErrors()
                    + "] errors or ratio ["
                    + policy.maxErrorRatio()
                    + "]"
            );
            throw new ParsingException(
                Source.EMPTY,
                "Error budget exceeded: [{}] dropped rows in [{}] decoded rows in [{}]; " + "maximum allowed is [{}] errors or [{}] ratio",
                errorCount,
                rowCount,
                fileLocation,
                policy.maxErrors(),
                policy.maxErrorRatio()
            );
        }
    }
}
