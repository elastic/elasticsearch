/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.fielddata;

import org.apache.lucene.index.BinaryDocValues;

/**
 * Base implementation. This impl is safe to use for sorting and
 * aggregations, which only use {@link #advanceExact(int)} and
 * {@link #binaryValue()}.
 */
public abstract class AbstractBinaryDocValues extends BinaryDocValues {

    @Override
    public int docID() {
        throw new UnsupportedOperationException();
    }

    @Override
    public int nextDoc() {
        throw new UnsupportedOperationException();
    }

    @Override
    public int advance(int target) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long cost() {
        throw new UnsupportedOperationException();
    }

}
