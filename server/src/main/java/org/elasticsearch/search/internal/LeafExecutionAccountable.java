/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.internal;

import org.apache.lucene.search.Query;
import org.elasticsearch.core.Nullable;

/**
 * A {@link Query} that knows the execution memory its own scorers allocate per leaf, such as a doc id set it materializes.
 * {@link ExecutionCharge#forQuery} recognises only Lucene's queries by their type, so this is how a query defined outside
 * {@code server} gets the same treatment: {@link ContextIndexSearcher} charges what the estimator reports to the request circuit
 * breaker when the scorer is built, settles the charge down to the doc id set the scorer keeps, and releases it once the leaf is done.
 */
public interface LeafExecutionAccountable {

    /**
     * Called each time a searcher with a request circuit breaker creates this query's weight, so the estimator can capture what it
     * needs from the query up front and read only leaf statistics per leaf.
     *
     * @return the charge for this query's execution memory, or {@code null} if it allocates none worth charging
     */
    @Nullable
    ExecutionCharge executionCharge();
}
