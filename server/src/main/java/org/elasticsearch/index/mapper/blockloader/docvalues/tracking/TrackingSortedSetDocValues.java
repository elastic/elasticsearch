/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper.blockloader.docvalues.tracking;

import org.apache.lucene.index.SortedSetDocValues;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.Releasable;

/**
 * Wraps a {@link SortedSetDocValues}, reserving some space in a {@link CircuitBreaker}
 * while it is live.
 */
public class TrackingSortedSetDocValues implements Releasable {
    private final CircuitBreaker breaker;
    private final ByteSizeValue size;
    private final SortedSetDocValues docValues;

    TrackingSortedSetDocValues(CircuitBreaker breaker, ByteSizeValue size, SortedSetDocValues docValues) {
        this.breaker = breaker;
        this.size = size;
        this.docValues = docValues;
    }

    public SortedSetDocValues docValues() {
        return docValues;
    }

    public CircuitBreaker breaker() {
        return breaker;
    }

    @Override
    public void close() {
        breaker.addWithoutBreaking(-size.getBytes());
    }
}
