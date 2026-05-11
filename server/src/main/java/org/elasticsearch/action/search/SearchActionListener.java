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
 * received by this listener, and accumulates any directory-level metrics carried by the
 * response into a caller-supplied accumulator. Centralizing the metrics hook here means
 * every shard-level search phase that uses this listener picks up accumulation
 * automatically, without having to remember to call it from each {@code innerOnResponse}.
 * <p>
 * Phase results that don't open a reader (e.g. can-match, open-PIT) inherit the default
 * {@link SearchPhaseResult#getDirectoryMetrics()} which returns {@link DirectoryMetrics#EMPTY},
 * so they contribute nothing to the accumulator.
 * <p>
 * Callers must supply an accumulator explicitly or a noop
 * when accumulation is handled elsewhere (e.g. the coordinator-side onShardResult path,
 * or a data-node-side batched listener whose results are accumulated on the coordinator) and
 * justify the choice with an inline comment at the call site.
 */
abstract class SearchActionListener<T extends SearchPhaseResult> implements ActionListener<T> {

    static final Consumer<DirectoryMetrics> NOOP_ACCUMULATOR = m -> {};

    final int requestIndex;
    private final SearchShardTarget searchShardTarget;
    private final Consumer<DirectoryMetrics> directoryMetricsAccumulator;

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
