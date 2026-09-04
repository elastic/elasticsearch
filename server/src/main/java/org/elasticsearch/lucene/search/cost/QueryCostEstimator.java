/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.lucene.search.cost;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.query.SearchExecutionContext;

/**
 * Upper bound on the bytes a query clause should reserve from the request circuit breaker.
 */
public interface QueryCostEstimator {

    long estimate();

    /** Charge {@link #estimate()} to the breaker on {@code ctx}. No-op if unavailable or non-positive. */
    default void charge(@Nullable SearchExecutionContext ctx, String label) {
        if (ctx == null || ctx.getCircuitBreaker() == null) {
            return;
        }
        long bytes = estimate();
        if (bytes <= 0L) {
            return;
        }
        ctx.addCircuitBreakerMemory(bytes, label);
    }
}
