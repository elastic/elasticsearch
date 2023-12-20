/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.script.field.vectors;

import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper.ElementType;
import org.elasticsearch.index.mapper.vectors.DenseVectorScriptDocValues;
import org.elasticsearch.index.mapper.vectors.VectorEncoderDecoder;

import java.io.IOException;

public class BinaryDenseVectorDocValuesField extends DenseVectorDocValuesField {

    private final BinaryDocValues input;
    private final float[] vectorValue;
    private final IndexVersion indexVersion;
    private boolean decoded;
    private final int dims;
    private BytesRef value;

    public BinaryDenseVectorDocValuesField(
        BinaryDocValues input,
        String name,
        ElementType elementType,
        int dims,
        IndexVersion indexVersion
    ) {
        super(name, elementType);
        this.input = input;
        this.indexVersion = indexVersion;
        this.dims = dims;
        this.vectorValue = new float[dims];
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

    @Override
    public DenseVector get() {
        if (isEmpty()) {
            return DenseVector.EMPTY;
        }
        decodeVectorIfNecessary();
        return new BinaryDenseVector(vectorValue, value, dims, indexVersion);
    }

    @Override
    public DenseVector get(DenseVector defaultValue) {
        if (isEmpty()) {
            return defaultValue;
        }
        decodeVectorIfNecessary();
        return new BinaryDenseVector(vectorValue, value, dims, indexVersion);
    }

    @Override
    public DenseVector getInternal() {
        return get(null);
    }

    private void decodeVectorIfNecessary() {
        if (decoded == false && value != null) {
            VectorEncoderDecoder.decodeDenseVector(indexVersion, value, vectorValue);
            decoded = true;
        }
    }
}
