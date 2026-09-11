/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.parser.ParsingException;

/**
 * Shared per-read error budget for cross-file schema adaptation.
 * <p>
 * Carries the cumulative error and row counts across both the columnar reader
 * (Parquet, ORC) and {@code SchemaAdaptingIterator}, so a single
 * {@code max_errors} / {@code max_error_ratio} budget is enforced against the
 * combined total rather than checked twice independently (once for reader drops,
 * once for reconciliation-cast drops in the adapter).
 * <p>
 * One instance per file read, created in
 * {@code AsyncExternalSourceOperatorFactory} before any reader or adapter is
 * constructed. Passed to the reader through {@link FormatReadContext} and
 * directly to the adapter via
 * {@link ColumnarRowDropHelper#forSharedBudget(SharedErrorBudget)}.
 * <p>
 * Not thread-safe. A single file read is always single-threaded.
 */
public final class SharedErrorBudget {

    private final ErrorPolicy policy;
    private final String fileLocation;
    private long errorCount;
    private long rowCount;

    private SharedErrorBudget(ErrorPolicy policy, String fileLocation) {
        this.policy = policy;
        this.fileLocation = fileLocation;
    }

    /**
     * Returns a new budget for the given policy and file, or {@code null} when the policy is not
     * {@link ErrorPolicy.Mode#SKIP_ROW} (no budget tracking needed).
     */
    @Nullable
    public static SharedErrorBudget forPolicy(@Nullable ErrorPolicy policy, String fileLocation) {
        if (policy != null && policy.mode() == ErrorPolicy.Mode.SKIP_ROW) {
            return new SharedErrorBudget(policy, fileLocation);
        }
        return null;
    }

    /**
     * Charges a completed reader batch: {@code rows} source rows processed, {@code errors} of which
     * were dropped. Only the reader (or reader-side budget holder such as
     * {@code ListCorruptionHandler}) should call this. Adapter-side drops use
     * {@link #addErrors(int)}.
     */
    public void addReaderBatch(int rows, int errors) {
        this.rowCount += rows;
        this.errorCount += errors;
    }

    /**
     * Charges {@code errors} additional errors without incrementing the row count. Used by the
     * adapter ({@code SchemaAdaptingIterator}) and by Parquet list-corruption detection
     * ({@code ListCorruptionHandler.recoveredOrphan}). The source rows for these errors were
     * already counted by the reader via {@link #addReaderBatch}.
     */
    public void addErrors(int errors) {
        this.errorCount += errors;
    }

    /**
     * Ensures the row count is at least {@code minRows}. Called by Parquet list-corruption
     * detection which may discover errors mid-stream before a batch is complete.
     */
    public void ensureRowsAtLeast(long minRows) {
        this.rowCount = Math.max(this.rowCount, minRows);
    }

    public ErrorPolicy policy() {
        return policy;
    }

    public String fileLocation() {
        return fileLocation;
    }

    public long errorCount() {
        return errorCount;
    }

    public long rowCount() {
        return rowCount;
    }

    /** Returns {@code true} if the configured budget is exceeded. */
    public boolean isBudgetExceeded() {
        return policy.isBudgetExceeded(errorCount, rowCount);
    }

    /**
     * Checks the budget and throws a {@link ParsingException} (HTTP 400) if exceeded.
     *
     * @param warnings   optional warning sink; receives the budget-exceeded line before the exception is thrown
     * @param errorKind  describes what the error count covers, in plural form (e.g. {@code "dropped rows"})
     */
    public void checkBudget(@Nullable SkipWarnings warnings, String errorKind) {
        if (policy.isBudgetExceeded(errorCount, rowCount)) {
            if (warnings != null) {
                warnings.add(budgetExceededWarning(policy, fileLocation, errorCount, rowCount, errorKind));
            }
            throw new ParsingException(
                Source.EMPTY,
                "Error budget exceeded: [{}] {} in [{}] decoded rows in [{}]; maximum allowed is [{}] errors or [{}] ratio",
                errorCount,
                errorKind,
                rowCount,
                fileLocation,
                policy.maxErrors(),
                policy.maxErrorRatio()
            );
        }
    }

    /**
     * Formats the budget-exceeded warning line. Shared with
     * {@link ColumnarRowDropHelper#budgetExceededWarning} for backward compatibility with call
     * sites that still use the helper's static method.
     */
    public static String budgetExceededWarning(ErrorPolicy policy, String fileLocation, long errorCount, long rowCount, String errorKind) {
        return "Columnar error budget exceeded at ["
            + fileLocation
            + "]: ["
            + errorCount
            + "] "
            + errorKind
            + " in ["
            + rowCount
            + "] decoded rows, maximum ["
            + policy.maxErrors()
            + "] errors or ratio ["
            + policy.maxErrorRatio()
            + "]";
    }
}
