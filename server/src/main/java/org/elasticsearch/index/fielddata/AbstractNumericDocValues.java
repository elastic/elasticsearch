/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.fielddata;

import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.search.DocIdSetIterator;

import java.io.IOException;

/**
 * Base implementation that throws an {@link IOException} for the
 * {@link DocIdSetIterator} APIs. This impl is safe to use for sorting and
 * aggregations, which only use {@link #advanceExact(int)} and
 * {@link #longValue()}.
 */
public abstract class AbstractNumericDocValues extends NumericDocValues {

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
