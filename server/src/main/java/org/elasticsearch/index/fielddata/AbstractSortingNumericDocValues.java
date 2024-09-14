/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.fielddata;

import org.apache.lucene.search.DocIdSetIterator;

import java.io.IOException;
import java.util.function.LongConsumer;

/**
 * Base implementation that throws an {@link IOException} for the
 * {@link DocIdSetIterator} APIs. This impl is safe to use for sorting and
 * aggregations, which only use {@link #advanceExact(int)} and
 * {@link #docValueCount()} and {@link #nextValue()}.
 */
public abstract class AbstractSortingNumericDocValues extends SortingNumericDocValues {

    public AbstractSortingNumericDocValues() {
        super();
    }

    public AbstractSortingNumericDocValues(LongConsumer circuitBreakerConsumer) {
        super(circuitBreakerConsumer);
    }

    @Override
    public int docID() {
        throw new UnsupportedOperationException();
    }

    @Override
    public int nextDoc() throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public int advance(int target) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public long cost() {
        throw new UnsupportedOperationException();
    }

}
