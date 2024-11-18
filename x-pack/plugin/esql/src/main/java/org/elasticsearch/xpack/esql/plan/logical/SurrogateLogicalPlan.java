/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical;

/**
 * Interface signaling to the planner that the declaring plan should be replaced with the surrogate plan.
 * This usually occurs for predefined commands that get "normalized" into a more generic form.
 * @see org.elasticsearch.xpack.esql.expression.SurrogateExpression
 */
public interface SurrogateLogicalPlan {
    /**
     * Returns the plan to be replaced with.
     */
    LogicalPlan surrogate();
}
