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
import java.util.Iterator;

public class ByteMultiDenseVectorDocValuesField extends MultiDenseVectorDocValuesField {

    protected final BinaryDocValues input;
    private final BinaryDocValues magnitudes;
    protected final int dims;
    protected int numVecs;
    protected VectorIterator<byte[]> vectorValue;
    protected boolean decoded;
    protected BytesRef value;
    protected BytesRef magnitudesValue;
    private byte[] buffer;

    public ByteMultiDenseVectorDocValuesField(
        BinaryDocValues input,
        BinaryDocValues magnitudes,
        String name,
        ElementType elementType,
        int dims
    ) {
        super(name, elementType);
        this.input = input;
        this.dims = dims;
        this.buffer = new byte[dims];
        this.magnitudes = magnitudes;
    }

    @Override
    public void setNextDocId(int docId) throws IOException {
        decoded = false;
        if (input.advanceExact(docId)) {
            boolean magnitudesFound = magnitudes.advanceExact(docId);
            assert magnitudesFound;
            value = input.binaryValue();
            assert value.length % dims == 0;
            numVecs = value.length / dims;
            magnitudesValue = magnitudes.binaryValue();
            assert magnitudesValue.length == (numVecs * Float.BYTES);
        } else {
            value = null;
            magnitudesValue = null;
            vectorValue = null;
            numVecs = 0;
        }
    }

    @Override
    public MultiDenseVectorScriptDocValues toScriptDocValues() {
        return new MultiDenseVectorScriptDocValues(this, dims);
    }

    protected MultiDenseVector getVector() {
        return new ByteMultiDenseVector(vectorValue, magnitudesValue, numVecs, dims);
    }

    @Override
    public MultiDenseVector get() {
        if (isEmpty()) {
            return MultiDenseVector.EMPTY;
        }
        decodeVectorIfNecessary();
        return getVector();
    }

    @Override
    public MultiDenseVector get(MultiDenseVector defaultValue) {
        if (isEmpty()) {
            return defaultValue;
        }
        decodeVectorIfNecessary();
        return getVector();
    }

    @Override
    public MultiDenseVector getInternal() {
        return get(null);
    }

    private void decodeVectorIfNecessary() {
        if (decoded == false && value != null) {
            vectorValue = new ByteVectorIterator(value, buffer, numVecs);
            decoded = true;
        }
    }

    @Override
    public int size() {
        return value == null ? 0 : value.length / dims;
    }

    @Override
    public boolean isEmpty() {
        return value == null;
    }

    static class ByteVectorIterator implements VectorIterator<byte[]> {
        private final byte[] buffer;
        private final BytesRef vectorValues;
        private final int size;
        private int idx = 0;

        ByteVectorIterator(BytesRef vectorValues, byte[] buffer, int size) {
            assert vectorValues.length == (buffer.length * size);
            this.vectorValues = vectorValues;
            this.size = size;
            this.buffer = buffer;
        }

        @Override
        public boolean hasNext() {
            return idx < size;
        }

        @Override
        public byte[] next() {
            if (hasNext() == false) {
                throw new IllegalArgumentException("No more elements in the iterator");
            }
            System.arraycopy(vectorValues.bytes, vectorValues.offset + idx * buffer.length, buffer, 0, buffer.length);
            idx++;
            return buffer;
        }

        @Override
        public Iterator<byte[]> copy() {
            return new ByteVectorIterator(vectorValues, new byte[buffer.length], size);
        }

        @Override
        public void reset() {
            idx = 0;
        }
    }
}
