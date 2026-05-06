/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

import java.util.List;

/**
 * Immutable context for a single {@link FormatReader#read} or {@link FormatReader#readAsync} call.
 * Bundles all per-read execution parameters that were previously spread across 12+ method overloads.
 * <p>
 * Format-specific configuration (delimiter, encoding, etc.) lives on the reader instance via
 * {@link FormatReader#withConfig}. Per-query optimizer hints (pushed filters) live on the reader
 * instance via {@link FormatReader#withPushedFilter}. This context carries only the parameters
 * that may vary per file or per split within a single query execution.
 *
 * @param projectedColumns columns to read. {@code null} means "no projection info available — read
 *                         every column" (backward compatibility default). An <em>empty</em> list
 *                         means "the optimizer pruned every column" (e.g. {@code COUNT(*)}); format
 *                         readers may take a fast path that skips type conversion and emits row-
 *                         count-only {@link org.elasticsearch.compute.data.Page Page}s.
 * @param batchSize        target number of rows per page
 * @param rowLimit         maximum total rows to return ({@link FormatReader#NO_LIMIT} for unlimited)
 * @param errorPolicy      how to handle malformed rows
 * @param firstSplit       whether this is the first split for the file (consistent with {@code lastSplit};
 *                         format-agnostic replacement for the legacy {@code skipFirstLine} parameter)
 * @param lastSplit        whether this is the last split for the file (affects trailing-record handling)
 * @param recordAligned    whether the split starts at a record boundary (no leading partial record).
 *                         When {@code false}, line-oriented readers may need to skip a leading partial line
 *                         (e.g. bzip2 / zstd-indexed macro-splits). When {@code true}, the split is known
 *                         to start exactly on a record boundary (e.g. streaming-parallel chunks sliced on
 *                         {@code \n}). Has no effect on the first split.
 */
public record FormatReadContext(
    List<String> projectedColumns,
    int batchSize,
    int rowLimit,
    ErrorPolicy errorPolicy,
    boolean firstSplit,
    boolean lastSplit,
    boolean recordAligned
) {

    /**
     * Creates a minimal context for the common non-split case. Leaves {@code errorPolicy} as
     * {@code null} so the reader falls back to its own default — typically the policy resolved
     * from the user's {@code WITH} options via {@link FormatReader#withConfig}, or the
     * {@link FormatReader#defaultErrorPolicy()} when no user options are set. Callers that need
     * to override the policy should use {@link #builder()} or {@link #withErrorPolicy(ErrorPolicy)}.
     */
    public static FormatReadContext of(List<String> projectedColumns, int batchSize) {
        return new FormatReadContext(projectedColumns, batchSize, FormatReader.NO_LIMIT, null, true, true, false);
    }

    /**
     * Returns a copy with a different row limit.
     */
    public FormatReadContext withRowLimit(int limit) {
        return new FormatReadContext(projectedColumns, batchSize, limit, errorPolicy, firstSplit, lastSplit, recordAligned);
    }

    /**
     * Returns a copy with a different error policy.
     */
    public FormatReadContext withErrorPolicy(ErrorPolicy policy) {
        return new FormatReadContext(projectedColumns, batchSize, rowLimit, policy, firstSplit, lastSplit, recordAligned);
    }

    /**
     * Returns a copy configured for a split-based read.
     */
    public FormatReadContext withSplit(boolean first, boolean last) {
        return new FormatReadContext(projectedColumns, batchSize, rowLimit, errorPolicy, first, last, recordAligned);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {
        private List<String> projectedColumns;
        private int batchSize;
        private int rowLimit = FormatReader.NO_LIMIT;
        // Defaults to null so the reader falls back to its own resolved policy
        // (typically the WITH-options policy from withConfig). Callers that want to override
        // explicitly should call errorPolicy(...).
        private ErrorPolicy errorPolicy = null;
        private boolean firstSplit = true;
        private boolean lastSplit = true;
        private boolean recordAligned = false;

        private Builder() {}

        public Builder projectedColumns(List<String> projectedColumns) {
            this.projectedColumns = projectedColumns;
            return this;
        }

        public Builder batchSize(int batchSize) {
            this.batchSize = batchSize;
            return this;
        }

        public Builder rowLimit(int rowLimit) {
            this.rowLimit = rowLimit;
            return this;
        }

        public Builder errorPolicy(ErrorPolicy errorPolicy) {
            this.errorPolicy = errorPolicy;
            return this;
        }

        public Builder firstSplit(boolean firstSplit) {
            this.firstSplit = firstSplit;
            return this;
        }

        public Builder lastSplit(boolean lastSplit) {
            this.lastSplit = lastSplit;
            return this;
        }

        /**
         * Marks the split as starting at a record boundary so line-oriented readers can skip the
         * "trim leading partial record" workaround used for byte-range macro-splits.
         */
        public Builder recordAligned(boolean recordAligned) {
            this.recordAligned = recordAligned;
            return this;
        }

        public FormatReadContext build() {
            if (batchSize <= 0) {
                throw new IllegalArgumentException("batchSize must be positive, got: " + batchSize);
            }
            return new FormatReadContext(projectedColumns, batchSize, rowLimit, errorPolicy, firstSplit, lastSplit, recordAligned);
        }
    }
}
