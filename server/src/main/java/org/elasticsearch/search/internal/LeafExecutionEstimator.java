/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.internal;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.ScorerSupplier;

import java.io.IOException;

/**
 * Sizes the execution memory that one query allocates to score one leaf, such as the doc id set a points query materializes.
 * {@link ExecutionBreakerWeight} calls it just before the scorer is built and charges the result to the request circuit breaker
 * until the leaf is done. It must return an upper bound, since nothing corrects the charge later.
 */
@FunctionalInterface
interface LeafExecutionEstimator {

    /**
     * @param ctx      the leaf about to be scored
     * @param inner    the query's own scorer supplier for {@code ctx}, whose {@link ScorerSupplier#cost()} may be read
     * @param leadCost the lead cost the scorer is built with, {@link Long#MAX_VALUE} for bulk scoring
     * @return the bytes to charge for {@code ctx}, {@code 0} if the scorer allocates nothing worth charging
     */
    long bytesForLeaf(LeafReaderContext ctx, ScorerSupplier inner, long leadCost) throws IOException;
}
