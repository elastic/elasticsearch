/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.planner;

import java.util.Objects;

/**
 * The services from outside the planner that {@link LocalExecutionPlanner} builds operators with.
 *
 * @param fetch the operators of the fetch phase
 */
public record PlannerServices(FetchOperatorProvider fetch) {
    public PlannerServices {
        Objects.requireNonNull(fetch, "fetch");
    }

    /**
     * For planners that never meet the fetch phase.
     */
    public static final PlannerServices NONE = new PlannerServices(FetchOperatorProvider.UNSUPPORTED);
}
