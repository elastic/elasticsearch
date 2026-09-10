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
import org.elasticsearch.simdvec.ESVectorUtil;
import org.elasticsearch.simdvec.MultiByteVectorsSource;

import java.util.Arrays;
import java.util.Iterator;

public class ByteRankVectors implements RankVectors, MultiByteVectorsSource {

    protected final VectorIterator<byte[]> vectorValues;
    protected final int numVecs;
    protected final int dims;

    private float[] magnitudes;
    private final BytesRef magnitudesBytes;
    private final BytesRef vectorBytes;
    private float[] scoresScratch = new float[0];
    private float[] maxesScratch = new float[0];

    public ByteRankVectors(VectorIterator<byte[]> vectorValues, BytesRef magnitudesBytes, int numVecs, int dims) {
        this(vectorValues, magnitudesBytes, numVecs, dims, null);
    }

    public ByteRankVectors(VectorIterator<byte[]> vectorValues, BytesRef magnitudesBytes, int numVecs, int dims, BytesRef vectorBytes) {
        assert magnitudesBytes.length == numVecs * Float.BYTES;
        this.vectorValues = vectorValues;
        this.numVecs = numVecs;
        this.dims = dims;
        this.magnitudesBytes = magnitudesBytes;
        this.vectorBytes = vectorBytes;
    }

    @Override
    public float maxSimDotProduct(float[][] query) {
        throw new UnsupportedOperationException("use [float maxSimDotProduct(byte[][] queryVector)] instead");
    }

    @Override
    public float maxSimDotProduct(byte[][] query) {
        float[] scores = ensureScoresScratch();
        return ESVectorUtil.maxSimDotProduct(this, query, scores);
    }

    @Override
    public float maxSimInvHamming(byte[][] query) {
        vectorValues.reset();
        int bitCount = dims * Byte.SIZE;
        float[] maxes = ensureMaxesScratch(query.length);
        Arrays.fill(maxes, 0, query.length, Float.NEGATIVE_INFINITY);
        while (vectorValues.hasNext()) {
            byte[] vv = vectorValues.next();
            for (int i = 0; i < query.length; i++) {
                maxes[i] = Math.max(maxes[i], ((bitCount - VectorUtil.xorBitCount(vv, query[i])) / (float) bitCount));
            }
        }
        return ESVectorUtil.sum(maxes, query.length);
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

    @Override
    public BytesRef vectorBytes() {
        return vectorBytes;
    }

    @Override
    public int vectorCount() {
        return numVecs;
    }

    @Override
    public int vectorDims() {
        return dims;
    }

    @Override
    public int vectorByteSize() {
        return dims;
    }

    @Override
    public Iterator<byte[]> vectorValues() {
        return vectorValues.copy();
    }

    private float[] ensureScoresScratch() {
        if (scoresScratch.length < numVecs) {
            scoresScratch = new float[numVecs];
        }
        return scoresScratch;
    }

    private float[] ensureMaxesScratch(int queryCount) {
        if (maxesScratch.length < queryCount) {
            maxesScratch = new float[queryCount];
        }
        return maxesScratch;
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
