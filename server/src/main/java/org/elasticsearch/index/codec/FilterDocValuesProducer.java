/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec;

import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;

import java.io.IOException;

/**
 * Implementation that allows wrapping another {@link DocValuesProducer} and alter behaviour of the wrapped instance.
 */
public abstract class FilterDocValuesProducer extends DocValuesProducer {
    private final DocValuesProducer in;

    protected FilterDocValuesProducer(DocValuesProducer in) {
        this.in = in;
    }

    @Override
    public NumericDocValues getNumeric(FieldInfo field) throws IOException {
        return in.getNumeric(field);
    }

    @Override
    public BinaryDocValues getBinary(FieldInfo field) throws IOException {
        return in.getBinary(field);
    }

    @Override
    public SortedDocValues getSorted(FieldInfo field) throws IOException {
        return in.getSorted(field);
    }

    @Override
    public SortedNumericDocValues getSortedNumeric(FieldInfo field) throws IOException {
        return in.getSortedNumeric(field);
    }

    @Override
    public SortedSetDocValues getSortedSet(FieldInfo field) throws IOException {
        return in.getSortedSet(field);
    }

    @Override
    public void checkIntegrity() throws IOException {
        in.checkIntegrity();
    }

    @Override
    public void close() throws IOException {
        in.close();
    }

    public DocValuesProducer getIn() {
        return in;
    }
}
