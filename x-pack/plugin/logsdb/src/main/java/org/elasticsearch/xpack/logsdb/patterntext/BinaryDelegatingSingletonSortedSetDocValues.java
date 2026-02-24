/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logsdb.patterntext;

import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;

/**
 * Wrap a SortedSetDocValues in a BinaryDocValues interface. Requires that each doc only have a single value.
 */
final class BinaryDelegatingSingletonSortedSetDocValues extends BinaryDocValues {
    private final SortedSetDocValues sortedSetDocValues;

    BinaryDelegatingSingletonSortedSetDocValues(SortedSetDocValues sortedSetDocValues) {
        this.sortedSetDocValues = sortedSetDocValues;
    }

    @Override
    public BytesRef binaryValue() throws IOException {
        assert sortedSetDocValues.docValueCount() == 1;
        return sortedSetDocValues.lookupOrd(sortedSetDocValues.nextOrd());
    }

    @Override
    public boolean advanceExact(int target) throws IOException {
        return sortedSetDocValues.advanceExact(target);
    }

    @Override
    public int docID() {
        return sortedSetDocValues.docID();
    }

    @Override
    public int nextDoc() throws IOException {
        return sortedSetDocValues.nextDoc();
    }

    @Override
    public int advance(int target) throws IOException {
        return sortedSetDocValues.advance(target);
    }

    @Override
    public long cost() {
        return sortedSetDocValues.cost();
    }
}
