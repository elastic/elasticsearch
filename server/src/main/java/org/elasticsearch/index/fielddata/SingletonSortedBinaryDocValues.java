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
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.index.codec.tsdb.es819.BinaryDocValuesSkipperEntry;
import org.elasticsearch.index.codec.tsdb.es819.BinaryDocValuesSkipperProducer;

import java.io.IOException;

final class SingletonSortedBinaryDocValues extends SortedBinaryDocValues implements BinaryDocValuesSkipperProducer {

    private final BinaryDocValues in;

    SingletonSortedBinaryDocValues(BinaryDocValues in) {
        this.in = in;
    }

    @Override
    public boolean advanceExact(int doc) throws IOException {
        return in.advanceExact(doc);
    }

    @Override
    public int docValueCount() {
        return 1;
    }

    @Override
    public BytesRef nextValue() throws IOException {
        return in.binaryValue();
    }

    @Override
    public BinaryDocValuesSkipperEntry getBinarySkipper() {
        if (in instanceof BinaryDocValuesSkipperProducer) {
            return ((BinaryDocValuesSkipperProducer) in).getBinarySkipper();
        }
        return null;
    }

    public BinaryDocValues getBinaryDocValues() {
        return in;
    }

}
