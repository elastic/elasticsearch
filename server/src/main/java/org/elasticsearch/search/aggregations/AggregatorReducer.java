/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.aggregations;

import org.elasticsearch.core.Releasable;

/**
 *  Interface for reducing aggregations to a single one.
 */
public interface AggregatorReducer extends Releasable {

    /**
     * Adds an aggregation for reduction. In <b>most</b> cases, the assumption will be the all given
     * aggregations are of the same type (the same type as this aggregation).
     */
    void accept(InternalAggregation aggregation);

    /**
     * returns the final aggregation.
     */
    InternalAggregation get();

    @Override
    default void close() {}
}
