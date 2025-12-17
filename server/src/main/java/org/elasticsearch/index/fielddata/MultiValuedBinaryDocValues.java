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
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;

/**
 * Wrapper around {@link BinaryDocValues} to decode the typical multivalued encoding
 */
public class MultiValuedBinaryDocValues extends BinaryDocValues {

    private final BinaryDocValues values;
    private final NumericDocValues counts;

    MultiValuedBinaryDocValues(BinaryDocValues values, NumericDocValues counts) {
        this.values = values;
        this.counts = counts;
    }

    public static BinaryDocValues from(BinaryDocValues values, NumericDocValues counts) {
        return new MultiValuedBinaryDocValues(values, counts);
    }

    @Override
    public BytesRef binaryValue() throws IOException {
        return values.binaryValue();
    }

    @Override
    public boolean advanceExact(int target) throws IOException {
        counts.advanceExact(target);
        return values.advanceExact(target);
    }

    @Override
    public int docID() {
        return values.docID();
    }

    @Override
    public int nextDoc() throws IOException {
        return values.nextDoc();
    }

    @Override
    public int advance(int target) throws IOException {
        counts.advance(target);
        return counts.advance(target);
    }

    @Override
    public long cost() {
        return counts.cost() + values.cost();
    }
}
