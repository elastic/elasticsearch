/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.fetch;

import org.elasticsearch.compute.operator.DriverExecutor;
import org.elasticsearch.search.scheduling.QueryScheduler;
import org.elasticsearch.tasks.TaskId;

import java.util.List;

/**
 * Runs the drivers of fetch requests like the drivers of the query phase: on the worker pool of ES|QL, through the query
 * scheduler of this node, so the search waiting of their shards counts them.
 */
@FunctionalInterface
public interface FetchDriverExecutors {
    /**
     * An executor for tasks of the query {@code queryId} that work on {@code shards}, each listed once.
     */
    DriverExecutor forQuery(TaskId queryId, List<QueryScheduler.SearchedShard> shards);
}
