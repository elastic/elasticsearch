/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.lucene.query;

import org.elasticsearch.index.mapper.DateFieldMapper;

/**
 * The competitive bound of a top N on its first sort key, shared by the operators of a data-node request across its batches
 * of shards. The rows behind the bound are part of the output, so a shard whose values of that key can't beat it can't add
 * to the top N, whatever its other keys.
 * <p>
 * Only the operators that sort on every key of the request's top N use it. Local optimization drops the keys that are
 * constant within a batch, such as a {@code constant_keyword}, but they still order the rows of different batches.
 * <p>
 * A top N pushed to Lucene offers the last value of each full per-shard top N, and skips the shards that can't beat it.
 */
public final class SharedSortBound {
    private final int sortKeys;

    private String field;
    private boolean ascending;
    private DateFieldMapper.Resolution resolution;
    private long value;
    private boolean offered;
    private boolean disabled;

    /**
     * @param sortKeys the number of sort keys of the request's top N
     */
    public SharedSortBound(int sortKeys) {
        this.sortKeys = sortKeys;
    }

    /**
     * Whether an operator that sorts on {@code keys} sort keys shares this bound.
     */
    public boolean sharedWith(int keys) {
        return keys == sortKeys;
    }

    /**
     * Offers the first sort key, on {@code field}, of the last row of a full top N.
     */
    public synchronized void offer(String field, boolean ascending, DateFieldMapper.Resolution resolution, long value) {
        if (disabled) {
            return;
        }
        if (offered == false) {
            this.field = field;
            this.ascending = ascending;
            this.resolution = resolution;
            this.value = value;
            offered = true;
            return;
        }
        if (sameKey(field, ascending, resolution) == false) {
            // Values that don't compare with each other
            disabled = true;
            return;
        }
        this.value = ascending ? Math.min(this.value, value) : Math.max(this.value, value);
    }

    /**
     * Whether a shard whose values of {@code field} are within {@code [min, max]} can't beat the bound.
     */
    public synchronized boolean canSkip(String field, boolean ascending, DateFieldMapper.Resolution resolution, long min, long max) {
        if (disabled || offered == false || sameKey(field, ascending, resolution) == false) {
            return false;
        }
        return ascending ? min > value : max < value;
    }

    private boolean sameKey(String field, boolean ascending, DateFieldMapper.Resolution resolution) {
        return this.field.equals(field) && this.ascending == ascending && this.resolution == resolution;
    }

    /**
     * Stops sharing the bound, for instance because the rows behind it may never be delivered.
     */
    public synchronized void invalidate() {
        disabled = true;
    }
}
