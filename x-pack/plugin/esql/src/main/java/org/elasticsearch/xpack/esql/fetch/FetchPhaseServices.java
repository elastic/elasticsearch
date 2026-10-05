/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.fetch;

import org.elasticsearch.xpack.esql.planner.FetchOperatorProvider;

/**
 * The entry point to the runtime of the fetch phase. ES|QL code outside this package gets the operators of the fetch
 * phase from it, so the runtime can change without touching that code.
 */
@FunctionalInterface
public interface FetchPhaseServices {
    /**
     * Builds the operators of the fetch phase.
     */
    FetchOperatorProvider operatorProvider();

    /**
     * Plans each {@link org.elasticsearch.xpack.esql.plan.physical.FetchExec} into a {@link FetchOperator}.
     */
    static FetchPhaseServices create() {
        FetchOperatorProvider operators = (exec, docRefChannel, fetchedTypes) -> new FetchOperator.Factory(docRefChannel, fetchedTypes);
        return () -> operators;
    }
}
