/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.lucene.query;

import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.compute.operator.topn.SharedMinCompetitive;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.index.mapper.DateFieldMapper;

import java.util.List;

/**
 * The competitive bound of a top N on a single field, shared by the operators of a data-node request across its batches of
 * shards. The rows behind the bound are part of the output, so a shard whose values can't beat it can't add to the top N.
 * <p>
 * A top N pushed to Lucene offers the last value of each full per-shard top N, and skips the shards that can't beat it. A
 * top N that runs after the source keeps its {@link SharedMinCompetitive} here for the whole request, so the sources of the
 * later batches filter with it from the start.
 */
public final class SharedSortBound implements Releasable {
    private Boolean ascending;
    private DateFieldMapper.Resolution resolution;
    private long value;
    private boolean disabled;

    private SharedMinCompetitive.Supplier minCompetitive;
    private SharedMinCompetitive heldMinCompetitive;

    /**
     * Offers the last value of a full top N.
     */
    public synchronized void offer(boolean ascending, DateFieldMapper.Resolution resolution, long value) {
        if (disabled) {
            return;
        }
        if (this.ascending == null) {
            this.ascending = ascending;
            this.resolution = resolution;
            this.value = value;
            return;
        }
        if (this.ascending != ascending || this.resolution != resolution) {
            // Values that don't compare with each other
            disabled = true;
            return;
        }
        this.value = ascending ? Math.min(this.value, value) : Math.max(this.value, value);
    }

    /**
     * Whether a shard whose values are within {@code [min, max]} can't beat the bound.
     */
    public synchronized boolean canSkip(boolean ascending, DateFieldMapper.Resolution resolution, long min, long max) {
        if (disabled || this.ascending == null || this.ascending != ascending || this.resolution != resolution) {
            return false;
        }
        return ascending ? min > value : max < value;
    }

    /**
     * The {@link SharedMinCompetitive} of a top N with these keys, kept for the whole request, or {@code null} if the request
     * already has one for other keys.
     */
    @Nullable
    public synchronized SharedMinCompetitive.Supplier minCompetitive(CircuitBreaker breaker, List<SharedMinCompetitive.KeyConfig> keys) {
        if (minCompetitive == null) {
            minCompetitive = new SharedMinCompetitive.Supplier(breaker, keys);
            // Keeps the bound between batches, when no operator holds it
            heldMinCompetitive = minCompetitive.get();
        }
        return minCompetitive.keyConfigs().equals(keys) ? minCompetitive : null;
    }

    @Override
    public synchronized void close() {
        Releasables.close(heldMinCompetitive);
        heldMinCompetitive = null;
    }
}
