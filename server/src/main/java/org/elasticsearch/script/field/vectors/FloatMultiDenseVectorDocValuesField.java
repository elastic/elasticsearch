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
import org.elasticsearch.index.mapper.vectors.MultiDenseVectorScriptDocValues;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.FloatBuffer;
import java.util.Iterator;

public class FloatMultiDenseVectorDocValuesField extends MultiDenseVectorDocValuesField {

    private final BinaryDocValues input;
    private final BinaryDocValues magnitudes;
    private boolean decoded;
    private final int dims;
    private BytesRef value;
    private BytesRef magnitudesValue;
    private FloatVectorIterator vectorValues;
    private int numVectors;
    private float[] buffer;

    public FloatMultiDenseVectorDocValuesField(
        BinaryDocValues input,
        BinaryDocValues magnitudes,
        String name,
        ElementType elementType,
        int dims
    ) {
        super(name, elementType);
        this.input = input;
        this.magnitudes = magnitudes;
        this.dims = dims;
        this.buffer = new float[dims];
    }

    @Override
    public void setNextDocId(int docId) throws IOException {
        decoded = false;
        if (input.advanceExact(docId)) {
            boolean magnitudesFound = magnitudes.advanceExact(docId);
            assert magnitudesFound;

            value = input.binaryValue();
            assert value.length % (Float.BYTES * dims) == 0;
            numVectors = value.length / (Float.BYTES * dims);
            magnitudesValue = magnitudes.binaryValue();
            assert magnitudesValue.length == (Float.BYTES * numVectors);
        } else {
            value = null;
            magnitudesValue = null;
            numVectors = 0;
        }
    }

    @Override
    public MultiDenseVectorScriptDocValues toScriptDocValues() {
        return new MultiDenseVectorScriptDocValues(this, dims);
    }

    @Override
    public boolean isEmpty() {
        return value == null;
    }

    @Override
    public MultiDenseVector get() {
        if (isEmpty()) {
            return MultiDenseVector.EMPTY;
        }
        decodeVectorIfNecessary();
        return new FloatMultiDenseVector(vectorValues, magnitudesValue, numVectors, dims);
    }

    @Override
    public MultiDenseVector get(MultiDenseVector defaultValue) {
        if (isEmpty()) {
            return defaultValue;
        }
        decodeVectorIfNecessary();
        return new FloatMultiDenseVector(vectorValues, magnitudesValue, numVectors, dims);
    }

    @Override
    public MultiDenseVector getInternal() {
        return get(null);
    }

    @Override
    public int size() {
        return value == null ? 0 : value.length / (Float.BYTES * dims);
    }

    private void decodeVectorIfNecessary() {
        if (decoded == false && value != null) {
            vectorValues = new FloatVectorIterator(value, buffer, numVectors);
            decoded = true;
        }
    }

    static class FloatVectorIterator implements Iterator<float[]> {
        private final float[] buffer;
        private final FloatBuffer vectorValues;
        private final int size;
        private int idx = 0;

        FloatVectorIterator(BytesRef vectorValues, float[] buffer, int size) {
            assert vectorValues.length == (buffer.length * Float.BYTES * size);
            this.vectorValues = ByteBuffer.wrap(vectorValues.bytes, vectorValues.offset, vectorValues.length)
                .order(ByteOrder.LITTLE_ENDIAN)
                .asFloatBuffer();
            this.size = size;
            this.buffer = buffer;
        }

        @Override
        public boolean hasNext() {
            return idx < size;
        }

        @Override
        public float[] next() {
            if (hasNext() == false) {
                throw new IllegalArgumentException("No more elements in the iterator");
            }
            vectorValues.get(buffer);
            idx++;
            return buffer;
        }
    }
}
