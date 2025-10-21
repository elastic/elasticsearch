/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.script.field.vectors;

import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper.ElementType;
import org.elasticsearch.index.mapper.vectors.DenseVectorScriptDocValues;

import java.io.IOException;

public class ByteBinaryDenseVectorDocValuesField extends DenseVectorDocValuesField {

    protected final BinaryDocValues input;
    protected final int dims;
    protected final byte[] vectorValue;
    protected boolean decoded;
    protected BytesRef value;

    public ByteBinaryDenseVectorDocValuesField(BinaryDocValues input, String name, ElementType elementType, int dims) {
        super(name, elementType);
        this.input = input;
        this.dims = dims;
        this.vectorValue = new byte[dims];
    }

    @Override
    public void setNextDocId(int docId) throws IOException {
        decoded = false;
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

    protected DenseVector getVector() {
        return new ByteBinaryDenseVector(vectorValue, value, dims);
    }

    @Override
    public DenseVector get() {
        if (isEmpty()) {
            return DenseVector.EMPTY;
        }
        decodeVectorIfNecessary();
        return getVector();
    }

    @Override
    public DenseVector get(DenseVector defaultValue) {
        if (isEmpty()) {
            return defaultValue;
        }
        decodeVectorIfNecessary();
        return getVector();
    }

    @Override
    public DenseVector getInternal() {
        return get(null);
    }

    private void decodeVectorIfNecessary() {
        if (decoded == false && value != null) {
            System.arraycopy(value.bytes, value.offset, vectorValue, 0, dims);
            decoded = true;
        }
    }
}
