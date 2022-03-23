/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.vectors.query;

import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.Version;

import java.io.IOException;

public class BinaryDenseVectorDocValuesField extends DenseVectorDocValuesField {

    protected final BinaryDocValues input;
    protected final Version indexVersion;
    protected final int dims;
    protected BytesRef value;

    public BinaryDenseVectorDocValuesField(BinaryDocValues input, String name, int dims, Version indexVersion) {
        super(name);
        this.input = input;
        this.indexVersion = indexVersion;
        this.dims = dims;
    }

    @Override
    public void setNextDocId(int docId) throws IOException {
        if (input.advanceExact(docId)) {
            value = input.binaryValue();
        } else {
            value = null;
        }
    }

    @Override
    public DenseVectorScriptDocValues toScriptDocValues() {
        return new DenseVectorScriptDocValues(this, dims);
    }

    @Override
    public boolean isEmpty() {
        return value == null;
    }

    @Override
    public DenseVector get() {
        if (isEmpty()) {
            return DenseVector.EMPTY;
        }

        return new BinaryDenseVector(value, dims, indexVersion);
    }

    @Override
    public DenseVector get(DenseVector defaultValue) {
        if (isEmpty()) {
            return defaultValue;
        }
        return new BinaryDenseVector(value, dims, indexVersion);
    }

    @Override
    public DenseVector getInternal() {
        return get(null);
    }
}
