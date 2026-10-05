/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.fetch;

import org.elasticsearch.xpack.esql.planner.FetchOperatorProvider;

/**
 * The runtime of the fetch phase, as the rest of ES|QL sees it. Everything outside this package goes through it, so the
 * runtime can change behind it.
 */
@FunctionalInterface
public interface FetchPhaseServices {
    /**
     * Builds the operators of the fetch phase.
     */
    FetchOperatorProvider operatorProvider();

    /**
     * For nodes and tests without the runtime. Planning a fetch phase fails.
     */
    FetchPhaseServices NOOP = () -> FetchOperatorProvider.UNSUPPORTED;
}
