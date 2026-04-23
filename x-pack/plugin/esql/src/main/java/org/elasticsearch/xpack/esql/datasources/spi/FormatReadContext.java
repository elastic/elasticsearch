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
 * @param projectedColumns columns to read (null or empty means all columns)
 * @param batchSize        target number of rows per page
 * @param rowLimit         maximum total rows to return ({@link FormatReader#NO_LIMIT} for unlimited)
 * @param errorPolicy      how to handle malformed rows
 * @param firstSplit       whether this is the first split for the file (consistent with {@code lastSplit};
 *                         format-agnostic replacement for the legacy {@code skipFirstLine} parameter)
 * @param lastSplit        whether this is the last split for the file (affects trailing-record handling)
 */
public record FormatReadContext(
    List<String> projectedColumns,
    int batchSize,
    int rowLimit,
    ErrorPolicy errorPolicy,
    boolean firstSplit,
    boolean lastSplit
) {

    /**
     * Creates a minimal context for the common non-split case.
     */
    public static FormatReadContext of(List<String> projectedColumns, int batchSize) {
        return new FormatReadContext(projectedColumns, batchSize, FormatReader.NO_LIMIT, ErrorPolicy.STRICT, true, true);
    }

    /**
     * Returns a copy with a different row limit.
     */
    public FormatReadContext withRowLimit(int limit) {
        return new FormatReadContext(projectedColumns, batchSize, limit, errorPolicy, firstSplit, lastSplit);
    }

    /**
     * Returns a copy with a different error policy.
     */
    public FormatReadContext withErrorPolicy(ErrorPolicy policy) {
        return new FormatReadContext(projectedColumns, batchSize, rowLimit, policy, firstSplit, lastSplit);
    }

    /**
     * Returns a copy configured for a split-based read.
     */
    public FormatReadContext withSplit(boolean first, boolean last) {
        return new FormatReadContext(projectedColumns, batchSize, rowLimit, errorPolicy, first, last);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {
        private List<String> projectedColumns;
        private int batchSize;
        private int rowLimit = FormatReader.NO_LIMIT;
        private ErrorPolicy errorPolicy = ErrorPolicy.STRICT;
        private boolean firstSplit = true;
        private boolean lastSplit = true;

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

        public FormatReadContext build() {
            if (batchSize <= 0) {
                throw new IllegalArgumentException("batchSize must be positive, got: " + batchSize);
            }
            return new FormatReadContext(projectedColumns, batchSize, rowLimit, errorPolicy, firstSplit, lastSplit);
        }
    }
}
