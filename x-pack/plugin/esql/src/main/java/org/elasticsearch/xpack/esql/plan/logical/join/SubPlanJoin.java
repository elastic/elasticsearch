/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical.join;

import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalRelation;

/**
 * A join whose right side is executed as a sub-plan before the result is inlined.
 */
public interface SubPlanJoin {

    /**
     * Returns the independently executed plan.
     */
    LogicalPlan right();

    /**
     * Replaces this join with a plan that consumes the materialized sub-plan result.
     */
    LogicalPlan inlineSubqueryResult(LocalRelation result, SubqueryResultContext context);
}
