/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.lucene.search;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.query.SearchExecutionContext;

/**
 * Conservative, parameter-driven estimate of the additional bytes a single query clause
 * should reserve from the request circuit breaker's rewrite pool, on top of the per-clause
 * constant. Implementations over-estimate rather than under-estimate.
 */
public interface QueryCostEstimator {

    /** Upper bound on the additional bytes to charge for this clause. */
    long estimate();

    /**
     * Charge {@link #estimate()} to the rewrite-scoped circuit-breaker pool on {@code ctx},
     * tagged with {@code label}. No-op when the context, its breaker, or the estimate is
     * non-positive.
     */
    default void chargeRewrite(@Nullable SearchExecutionContext ctx, String label) {
        if (ctx == null || ctx.getCircuitBreaker() == null) {
            return;
        }
        long bytes = estimate();
        if (bytes <= 0L) {
            return;
        }
        ctx.addRewriteCircuitBreakerMemory(bytes, label);
    }
}
