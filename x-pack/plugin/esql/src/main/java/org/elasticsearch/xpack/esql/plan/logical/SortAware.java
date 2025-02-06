/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical;

/**
 * This interface is intended to check redundancy of a previous SORT.
 *
 * Eg. if a MY_COMMAND that implements this interface is used after a sort, and if dependsOnInputOrder() = false,
 * then we can assume that
 * <p>
 * <code>
 * | SORT x | MY_COMMAND
 * </code>
 * <p>
 * is equivalent to
 * <p>
 * <code>
 * | MY_COMMAND
 * </code>
 * <p>
 *
 * In all the other cases, eg. if the command does not implement this interface, or if dependsOnInputOrder() = true
 * then we assume that the SORT is still relevant and cannot be pruned.
 */
public interface SortAware {

    /**
     * Returns true if the logic of this command could depend on the order of the input.
     */
    default boolean dependsOnInputOrder() {
        return false;
    }
}
