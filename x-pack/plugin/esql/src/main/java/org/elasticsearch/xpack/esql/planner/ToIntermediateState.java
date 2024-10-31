/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.planner;

import org.elasticsearch.compute.aggregation.IntermediateStateDesc;

import java.util.List;

/**
 * Expressions that have a mapping to {@link org.elasticsearch.compute.aggregation.IntermediateStateDesc}s.
 */
public interface ToIntermediateState {
    /**
     * Returns the intermediate state descriptions for this expression.
     * <p>
     *     If null, the default method of {@link AggregateMapper} will be used to get them.
     * </p>
     */
    default List<IntermediateStateDesc> intermediateState(boolean grouping) {
        return null;
    }
}
