/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.planner;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.remotefetch.RemoteFetchService;

import java.util.Objects;

/**
 * The services from outside the planner that {@link LocalExecutionPlanner} builds operators with.
 *
 * @param remoteFetch the transport of the remote fetch prototype, {@code null} when the planner never meets it
 * @param fetch       the operators of the fetch phase
 */
public record PlannerServices(@Nullable RemoteFetchService remoteFetch, FetchOperatorProvider fetch) {
    public PlannerServices {
        Objects.requireNonNull(fetch, "fetch");
    }

    /**
     * For planners that meet neither.
     */
    public static final PlannerServices NONE = new PlannerServices(null, FetchOperatorProvider.UNSUPPORTED);
}
