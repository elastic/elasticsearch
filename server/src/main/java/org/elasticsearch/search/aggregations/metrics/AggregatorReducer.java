/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.metrics;

import org.elasticsearch.core.Releasable;
import org.elasticsearch.search.aggregations.InternalAggregation;

/**
 *  Interface for reducing aggregations to a single one.
 */
public interface AggregatorReducer extends Releasable {

    /**
     * Adds an aggregation for reduction.
     */
    void accept(InternalAggregation aggregation);

    /**
     * returns the final aggregation.
     */
    InternalAggregation get();

    @Override
    default void close() {}
}
