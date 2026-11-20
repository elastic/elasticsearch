/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.planner;

import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.compute.operator.topn.DocVectorEncoder;
import org.elasticsearch.xpack.esql.plan.physical.FetchExec;

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
     * @param docChannel the input channel that holds the documents of the rows
     */
    Operator.OperatorFactory fetchOperator(FetchExec exec, int docChannel);

    /**
     * Whether the {@code _doc} rows a TopN of this plan keeps left the nodes that read them. Each such TopN numbers their
     * origins in a table of its own, see {@link DocVectorEncoder#detached()}. {@code false} where documents never leave
     * the node that read them.
     */
    default boolean receivesDetachedDocuments() {
        return false;
    }

    /**
     * For planners that never meet the fetch phase, like the ones many tests build.
     */
    FetchOperatorProvider UNSUPPORTED = (exec, docChannel) -> {
        throw new IllegalStateException("this planner can't plan the fetch phase");
    };
}
