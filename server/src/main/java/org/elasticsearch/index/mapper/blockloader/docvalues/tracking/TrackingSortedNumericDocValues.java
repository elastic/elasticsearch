/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper.blockloader.docvalues.tracking;

import org.apache.lucene.index.SortedNumericDocValues;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.core.Releasable;

import static org.elasticsearch.index.mapper.blockloader.docvalues.tracking.TrackingNumericDocValues.ESTIMATED_SIZE;

public class TrackingSortedNumericDocValues implements Releasable {
    private final CircuitBreaker breaker;
    private final SortedNumericDocValues docValues;

    TrackingSortedNumericDocValues(CircuitBreaker breaker, SortedNumericDocValues docValues) {
        this.breaker = breaker;
        this.docValues = docValues;
    }

    public SortedNumericDocValues docValues() {
        return docValues;
    }

    @Override
    public void close() {
        breaker.addWithoutBreaking(-ESTIMATED_SIZE);
    }
}
