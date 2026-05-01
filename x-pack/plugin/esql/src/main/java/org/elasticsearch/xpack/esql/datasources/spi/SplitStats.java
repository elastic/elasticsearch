/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

import org.elasticsearch.core.Nullable;

/**
 * Per-split statistics used by the optimizer for aggregate pushdown and filter pruning.
 * Implemented by {@link org.elasticsearch.xpack.esql.datasources.SplitStats} for
 * individual file splits and by {@link org.elasticsearch.xpack.esql.datasources.MergedSplitStats}
 * for composite splits that aggregate statistics lazily from their children.
 * <p>
 * Unknown numeric values are represented as {@code -1}; unknown object values as {@code null}.
 * This matches the sentinel convention used throughout the datasources layer
 * (see {@link ExternalSplit#estimatedSizeInBytes()}).
 */
public interface SplitStats {

    /** Total row count for this split. Always {@code >= 0}. */
    long rowCount();

    /**
     * Uncompressed size of the data in this split, in bytes.
     * Returns {@code -1} if unknown.
     */
    long sizeInBytes();

    /**
     * On-disk (compressed) size of this split, in bytes.
     * Returns {@code -1} if unknown.
     */
    default long compressedSizeInBytes() {
        return -1;
    }

    /**
     * Number of null values in the named column, or {@code -1} if unknown or the
     * column is not present in this split's statistics.
     */
    long columnNullCount(String name);

    /**
     * Minimum value for the named column, or {@code null} if unknown or the column
     * is not present in this split's statistics.
     */
    @Nullable
    Object columnMin(String name);

    /**
     * Maximum value for the named column, or {@code null} if unknown or the column
     * is not present in this split's statistics.
     */
    @Nullable
    Object columnMax(String name);

    /**
     * Uncompressed size in bytes for the named column, or {@code -1} if unknown or the
     * column is not present in this split's statistics.
     */
    default long columnSizeBytes(String name) {
        return -1;
    }
}
