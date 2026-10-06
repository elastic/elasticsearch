/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.planner;

import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.xpack.esql.plan.physical.FetchSourceExec;

/**
 * Gives {@link LocalExecutionPlanner} the source of a fetch plan, on the node that loads the documents one fetch request
 * names. Only the planner of a fetch request has one, so every other planner refuses a fetch plan.
 */
@FunctionalInterface
public interface FetchSourceProvider {
    /**
     * The source of {@code exec}. Each driver of the fetch plan loads one shard of the request.
     *
     * @param maxPageSize the most rows in one page
     */
    FetchSource fetchSource(FetchSourceExec exec, int maxPageSize);

    /**
     * @param factory builds the source of each driver
     * @param drivers how many drivers run the fetch plan, one per shard
     */
    record FetchSource(SourceOperator.SourceOperatorFactory factory, int drivers) {}

    /**
     * For planners that never run a fetch plan, which is every planner but the one of a fetch request.
     */
    FetchSourceProvider UNSUPPORTED = (exec, maxPageSize) -> { throw new IllegalStateException("this planner can't run a fetch plan"); };
}
