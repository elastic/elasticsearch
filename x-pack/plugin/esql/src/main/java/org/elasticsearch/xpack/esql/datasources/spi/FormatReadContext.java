/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

import org.elasticsearch.xpack.esql.core.expression.Attribute;

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
 * @param skipFirstLine    whether to skip the first line (used in split-based reads where the
 *                         split starts mid-record)
 * @param lastSplit        whether this is the last split for the file (affects trailing-record handling)
 * @param resolvedAttributes pre-resolved schema attributes (non-null in split reads where the schema
 *                           was resolved from the first split)
 */
public record FormatReadContext(
    List<String> projectedColumns,
    int batchSize,
    int rowLimit,
    ErrorPolicy errorPolicy,
    boolean skipFirstLine,
    boolean lastSplit,
    List<Attribute> resolvedAttributes
) {

    /**
     * Creates a minimal context for the common non-split case.
     */
    public static FormatReadContext of(List<String> projectedColumns, int batchSize) {
        return new FormatReadContext(projectedColumns, batchSize, FormatReader.NO_LIMIT, ErrorPolicy.STRICT, false, false, null);
    }

    /**
     * Returns a copy with a different row limit.
     */
    public FormatReadContext withRowLimit(int limit) {
        return new FormatReadContext(projectedColumns, batchSize, limit, errorPolicy, skipFirstLine, lastSplit, resolvedAttributes);
    }

    /**
     * Returns a copy with a different error policy.
     */
    public FormatReadContext withErrorPolicy(ErrorPolicy policy) {
        return new FormatReadContext(projectedColumns, batchSize, rowLimit, policy, skipFirstLine, lastSplit, resolvedAttributes);
    }

    /**
     * Returns a copy configured for a split-based read.
     */
    public FormatReadContext withSplit(boolean skip, boolean last, List<Attribute> attrs) {
        return new FormatReadContext(projectedColumns, batchSize, rowLimit, errorPolicy, skip, last, attrs);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {
        private List<String> projectedColumns;
        private int batchSize;
        private int rowLimit = FormatReader.NO_LIMIT;
        private ErrorPolicy errorPolicy = ErrorPolicy.STRICT;
        private boolean skipFirstLine;
        private boolean lastSplit;
        private List<Attribute> resolvedAttributes;

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

        public Builder skipFirstLine(boolean skipFirstLine) {
            this.skipFirstLine = skipFirstLine;
            return this;
        }

        public Builder lastSplit(boolean lastSplit) {
            this.lastSplit = lastSplit;
            return this;
        }

        public Builder resolvedAttributes(List<Attribute> resolvedAttributes) {
            this.resolvedAttributes = resolvedAttributes;
            return this;
        }

        public FormatReadContext build() {
            return new FormatReadContext(projectedColumns, batchSize, rowLimit, errorPolicy, skipFirstLine, lastSplit, resolvedAttributes);
        }
    }
}
