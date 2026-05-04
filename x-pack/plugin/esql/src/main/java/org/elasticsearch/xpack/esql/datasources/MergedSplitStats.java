/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.core.Nullable;

import java.util.List;

/**
 * Lazily merges statistics from a list of child {@link org.elasticsearch.xpack.esql.datasources.spi.SplitStats} instances.
 * Used by {@link CoalescedSplit#splitStats()} to expose aggregate statistics for
 * composite splits without eagerly materializing a full {@link SplitStats}.
 * <p>
 * Each accessor method computes its result on demand by iterating the children.
 * This is appropriate because the optimizer calls these methods at most a few times
 * per planning phase, and the child count is typically small (a few dozen splits).
 */
public final class MergedSplitStats implements org.elasticsearch.xpack.esql.datasources.spi.SplitStats {

    private final List<org.elasticsearch.xpack.esql.datasources.spi.SplitStats> children;

    public MergedSplitStats(List<org.elasticsearch.xpack.esql.datasources.spi.SplitStats> children) {
        if (children == null || children.isEmpty()) {
            throw new IllegalArgumentException("children cannot be null or empty");
        }
        this.children = List.copyOf(children);
    }

    /** Returns the list of child stats that this instance merges. */
    public List<org.elasticsearch.xpack.esql.datasources.spi.SplitStats> children() {
        return children;
    }

    @Override
    public long rowCount() {
        long total = 0;
        for (org.elasticsearch.xpack.esql.datasources.spi.SplitStats child : children) {
            total += child.rowCount();
        }
        return total;
    }

    /**
     * Returns the sum of children's uncompressed sizes, or {@code -1} if any child
     * reports an unknown size. A single unknown child poisons the aggregate because
     * we cannot give a meaningful partial sum to the optimizer.
     */
    @Override
    public long sizeInBytes() {
        long total = 0;
        for (org.elasticsearch.xpack.esql.datasources.spi.SplitStats child : children) {
            long s = child.sizeInBytes();
            if (s < 0) {
                return -1;
            }
            total += s;
        }
        return total;
    }

    /**
     * Returns the sum of children's compressed sizes, or {@code -1} if any child
     * reports an unknown compressed size.
     */
    @Override
    public long compressedSizeInBytes() {
        long total = 0;
        for (org.elasticsearch.xpack.esql.datasources.spi.SplitStats child : children) {
            long s = child.compressedSizeInBytes();
            if (s < 0) {
                return -1;
            }
            total += s;
        }
        return total;
    }

    /**
     * Returns the sum of null counts across children for the named column.
     * Returns {@code -1} if any child returns an unknown null count for the column,
     * or if the column is not present in any child.
     */
    @Override
    public long columnNullCount(String name) {
        long total = 0;
        for (org.elasticsearch.xpack.esql.datasources.spi.SplitStats child : children) {
            long nc = child.columnNullCount(name);
            if (nc < 0) {
                return -1;
            }
            total += nc;
        }
        return total;
    }

    /**
     * Returns the minimum of children's min values for the named column.
     * Returns {@code null} if any child returns null for the column min, or if
     * the column is not present in any child. Cross-type numeric widening is applied
     * via {@link SplitStats#mergedMin}.
     */
    @Override
    @Nullable
    public Object columnMin(String name) {
        Object result = null;
        for (org.elasticsearch.xpack.esql.datasources.spi.SplitStats child : children) {
            Object childMin = child.columnMin(name);
            if (childMin == null) {
                // Unknown min in any child poisons the aggregate
                return null;
            }
            result = SplitStats.mergedMin(result, childMin);
            if (result == null) {
                // Incompatible types — clear the stat
                return null;
            }
        }
        return result;
    }

    /**
     * Returns the maximum of children's max values for the named column.
     * Returns {@code null} if any child returns null for the column max, or if
     * the column is not present in any child. Cross-type numeric widening is applied
     * via {@link SplitStats#mergedMax}.
     */
    @Override
    @Nullable
    public Object columnMax(String name) {
        Object result = null;
        for (org.elasticsearch.xpack.esql.datasources.spi.SplitStats child : children) {
            Object childMax = child.columnMax(name);
            if (childMax == null) {
                // Unknown max in any child poisons the aggregate
                return null;
            }
            result = SplitStats.mergedMax(result, childMax);
            if (result == null) {
                // Incompatible types — clear the stat
                return null;
            }
        }
        return result;
    }

    /**
     * Returns the sum of per-column sizes across children, or {@code -1} if any child
     * returns an unknown size for the column.
     */
    @Override
    public long columnSizeBytes(String name) {
        long total = 0;
        for (org.elasticsearch.xpack.esql.datasources.spi.SplitStats child : children) {
            long sz = child.columnSizeBytes(name);
            if (sz < 0) {
                return -1;
            }
            total += sz;
        }
        return total;
    }

    @Override
    public String toString() {
        return "MergedSplitStats[children=" + children.size() + "]";
    }
}
