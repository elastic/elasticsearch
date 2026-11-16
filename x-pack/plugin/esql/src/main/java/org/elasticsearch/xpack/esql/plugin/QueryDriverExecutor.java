/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.compute.operator.ComputeExecutor;
import org.elasticsearch.compute.operator.DriverExecutor;
import org.elasticsearch.search.scheduling.QueryScheduler;
import org.elasticsearch.tasks.TaskId;

import java.util.List;
import java.util.concurrent.Executor;

/**
 * Runs the drivers of a query on a {@link ComputeExecutor}, through the query scheduler of the node both when they start
 * and when they resume, so the search waiting of their shards counts them. The query phase and the fetch phase run their
 * drivers through it.
 */
public final class QueryDriverExecutor implements DriverExecutor {
    private final QueryScheduler.QueryExecutor executor;
    /** The scheduler passes a wrapped {@link AbstractRunnable} on as an {@link AbstractRunnable}, so the cast holds. */
    private final Executor resumed;

    /**
     * @param shards the shards the drivers work on, each listed once
     */
    public QueryDriverExecutor(
        QueryScheduler scheduler,
        ComputeExecutor computeExecutor,
        TaskId queryId,
        List<QueryScheduler.SearchedShard> shards
    ) {
        this.executor = scheduler.executor(queryId, computeExecutor, shards);
        this.resumed = task -> computeExecutor.executeResumed((AbstractRunnable) task);
    }

    @Override
    public void execute(Runnable task) {
        executor.execute(task);
    }

    @Override
    public void executeResumed(AbstractRunnable task) {
        executor.execute(task, resumed);
    }
}
