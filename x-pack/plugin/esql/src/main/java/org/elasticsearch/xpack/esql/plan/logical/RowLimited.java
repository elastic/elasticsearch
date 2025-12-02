/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical;

/**
 * Interface for logical plans that enforce a row limit.
 * Plans implementing this interface have a maximum number of rows they can handle,
 * which may be enforced during plan transformation or execution.
 *
 * <p>
 * Practically it means that a LIMIT to the plan children.
 */
public interface RowLimited {
    /**
     * Returns the maximum number of rows this plan can produce.
     */
    int maxRows();
}
