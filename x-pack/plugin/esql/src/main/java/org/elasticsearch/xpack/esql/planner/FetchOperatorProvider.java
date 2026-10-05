/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.planner;

import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.xpack.esql.plan.physical.FetchExec;

import java.util.List;

/**
 * Gives {@link LocalExecutionPlanner} the operator of each {@link FetchExec}. The planner calls nothing else of the fetch
 * runtime, so the runtime can change without touching the planner.
 */
@FunctionalInterface
public interface FetchOperatorProvider {
    /**
     * The operator that loads the fetched attributes of {@code exec} for the rows of its input pages, and appends them
     * as columns after the input columns.
     *
     * @param docRefChannel the input channel that holds the document references
     * @param fetchedTypes  the element type of each fetched column
     */
    Operator.OperatorFactory fetchOperator(FetchExec exec, int docRefChannel, List<ElementType> fetchedTypes);

    /**
     * For planners that never meet the fetch phase, like the ones many tests build.
     */
    FetchOperatorProvider UNSUPPORTED = (exec, docRefChannel, fetchedTypes) -> {
        throw new IllegalStateException("this planner can't plan the fetch phase");
    };
}
