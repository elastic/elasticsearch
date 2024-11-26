/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.script.field.vectors;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.VectorUtil;
import org.elasticsearch.index.mapper.vectors.VectorEncoderDecoder;

import java.util.Arrays;
import java.util.Iterator;

public class ByteMultiDenseVector implements MultiDenseVector {

    protected final VectorIterator<byte[]> vectorValues;
    protected final int numVecs;
    protected final int dims;

    private float[] magnitudes;
    private final BytesRef magnitudesBytes;

    public ByteMultiDenseVector(VectorIterator<byte[]> vectorValues, BytesRef magnitudesBytes, int numVecs, int dims) {
        assert magnitudesBytes.length == numVecs * Float.BYTES;
        this.vectorValues = vectorValues;
        this.numVecs = numVecs;
        this.dims = dims;
        this.magnitudesBytes = magnitudesBytes;
    }

    @Override
    public float maxSimDotProduct(float[][] query) {
        throw new UnsupportedOperationException("use [float maxSimDotProduct(byte[][] queryVector)] instead");
    }

    @Override
    public float maxSimDotProduct(byte[][] query) {
        vectorValues.reset();
        float[] maxes = new float[query.length];
        Arrays.fill(maxes, Float.NEGATIVE_INFINITY);
        while (vectorValues.hasNext()) {
            byte[] vv = vectorValues.next();
            for (int i = 0; i < query.length; i++) {
                maxes[i] = Math.max(maxes[i], VectorUtil.dotProduct(query[i], vv));
            }
        }
        float sum = 0;
        for (float m : maxes) {
            sum += m;
        }
        return sum;
    }

    @Override
    public float maxSimInvHamming(byte[][] query) {
        vectorValues.reset();
        int bitCount = dims * Byte.SIZE;
        float[] maxes = new float[query.length];
        Arrays.fill(maxes, Float.NEGATIVE_INFINITY);
        while (vectorValues.hasNext()) {
            byte[] vv = vectorValues.next();
            for (int i = 0; i < query.length; i++) {
                maxes[i] = Math.max(maxes[i], ((bitCount - VectorUtil.xorBitCount(vv, query[i])) / (float) bitCount));
            }
        }
        float sum = 0;
        for (float m : maxes) {
            sum += m;
        }
        return sum;
    }

    @Override
    public Iterator<float[]> getVectors() {
        return new ByteToFloatIteratorWrapper(vectorValues.copy(), dims);
    }

    @Override
    public float[] getMagnitudes() {
        if (magnitudes == null) {
            magnitudes = VectorEncoderDecoder.getMultiMagnitudes(magnitudesBytes);
        }
        return magnitudes;
    }

    @Override
    public boolean isEmpty() {
        return false;
    }

    @Override
    public int getDims() {
        return dims;
    }

    @Override
    public int size() {
        return numVecs;
    }

    static class ByteToFloatIteratorWrapper implements Iterator<float[]> {
        private final Iterator<byte[]> byteIterator;
        private final float[] buffer;
        private final int dims;

        ByteToFloatIteratorWrapper(Iterator<byte[]> byteIterator, int dims) {
            this.byteIterator = byteIterator;
            this.buffer = new float[dims];
            this.dims = dims;
        }

        @Override
        public boolean hasNext() {
            return byteIterator.hasNext();
        }

        @Override
        public float[] next() {
            byte[] next = byteIterator.next();
            for (int i = 0; i < dims; i++) {
                buffer[i] = next[i];
            }
            return buffer;
        }
    }
}
