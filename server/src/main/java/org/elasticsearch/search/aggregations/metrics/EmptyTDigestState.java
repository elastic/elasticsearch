/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.aggregations.metrics;

import org.elasticsearch.common.breaker.NoopCircuitBreaker;

public final class EmptyTDigestState extends TDigestState {
    public EmptyTDigestState() {
        // Use the sorting implementation to minimize memory allocation.
        super(new NoopCircuitBreaker("empty-tdigest-state-noop-breaker"), Type.SORTING, 1.0D);
    }

    @Override
    public void add(double x, long w) {
        throw new UnsupportedOperationException("Immutable Empty TDigest");
    }

    @Override
    public void add(double x) {
        throw new UnsupportedOperationException("Immutable Empty TDigest");
    }

    @Override
    public void add(TDigestState other) {
        throw new UnsupportedOperationException("Immutable Empty TDigest");
    }
}
