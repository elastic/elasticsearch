/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.action.search;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.index.store.DirectoryMetrics;
import org.elasticsearch.search.SearchPhaseResult;
import org.elasticsearch.search.SearchShardTarget;

import java.util.function.Consumer;

/**
 * A base action listener that ensures shard target and shard index is set on all responses
 * received by this listener.
 */
abstract class SearchActionListener<T extends SearchPhaseResult> implements ActionListener<T> {

    private static final Consumer<DirectoryMetrics> NOOP_ACCUMULATOR = m -> {};

    final int requestIndex;
    private final SearchShardTarget searchShardTarget;
    private final Consumer<DirectoryMetrics> directoryMetricsAccumulator;

    /**
     * Creates a listener that does not accumulate directory metrics. Use this when the
     * caller is not the coordinator merging metrics across shards (e.g. a data-node-side
     * batched listener).
     */
    protected SearchActionListener(SearchShardTarget searchShardTarget, int shardIndex) {
        this(searchShardTarget, shardIndex, NOOP_ACCUMULATOR);
    }

    protected SearchActionListener(
        SearchShardTarget searchShardTarget,
        int shardIndex,
        Consumer<DirectoryMetrics> directoryMetricsAccumulator
    ) {
        assert shardIndex >= 0 : "shard index must be positive";
        this.searchShardTarget = searchShardTarget;
        this.requestIndex = shardIndex;
        this.directoryMetricsAccumulator = directoryMetricsAccumulator;
    }

    @Override
    public final void onResponse(T response) {
        response.setShardIndex(requestIndex);
        setSearchShardTarget(response);
        directoryMetricsAccumulator.accept(response.getDirectoryMetrics());
        innerOnResponse(response);
    }

    protected void setSearchShardTarget(T response) { // some impls need to override this
        response.setSearchShardTarget(searchShardTarget);
    }

    protected abstract void innerOnResponse(T response);
}
