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
import org.elasticsearch.index.codec.vectors.BFloat16;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper.ElementType;
import org.elasticsearch.simdvec.ESVectorUtil;
import org.elasticsearch.simdvec.MultiFloatVectorsSource;

import java.util.Iterator;

import static org.elasticsearch.index.mapper.vectors.VectorEncoderDecoder.getMultiMagnitudes;

public class FloatRankVectors implements RankVectors, MultiFloatVectorsSource {

    private final BytesRef magnitudes;
    private float[] magnitudesArray = null;
    private final int dims;
    private final int numVectors;
    private final VectorIterator<float[]> vectorValues;
    private final BytesRef vectorBytes;
    private final ElementType elementType;
    private float[] scoresScratch = new float[0];

    public FloatRankVectors(VectorIterator<float[]> decodedDocVector, BytesRef magnitudes, int numVectors, int dims) {
        this(decodedDocVector, magnitudes, numVectors, dims, null, ElementType.FLOAT);
    }

    public FloatRankVectors(
        VectorIterator<float[]> decodedDocVector,
        BytesRef magnitudes,
        int numVectors,
        int dims,
        BytesRef vectorBytes,
        ElementType elementType
    ) {
        assert magnitudes.length == numVectors * Float.BYTES;
        this.vectorValues = decodedDocVector;
        this.magnitudes = magnitudes;
        this.numVectors = numVectors;
        this.dims = dims;
        this.vectorBytes = vectorBytes;
        this.elementType = elementType;
    }

    @Override
    public float maxSimDotProduct(float[][] query) {
        float[] scores = ensureScoresScratch();
        return ESVectorUtil.maxSimDotProduct(this, query, scores);
    }

    @Override
    public float maxSimDotProduct(byte[][] query) {
        throw new UnsupportedOperationException("use [float maxSimDotProduct(float[][] queryVector)] instead");
    }

    @Override
    public float maxSimInvHamming(byte[][] query) {
        throw new UnsupportedOperationException("hamming distance is not supported for float vectors");
    }

    @Override
    public Iterator<float[]> getVectors() {
        return vectorValues.copy();
    }

    @Override
    public float[] getMagnitudes() {
        if (magnitudesArray == null) {
            magnitudesArray = getMultiMagnitudes(magnitudes);
        }
        return magnitudesArray;
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
        return numVectors;
    }

    @Override
    public BytesRef vectorBytes() {
        return vectorBytes;
    }

    @Override
    public int vectorCount() {
        return numVectors;
    }

    @Override
    public int vectorDims() {
        return dims;
    }

    @Override
    public int vectorByteSize() {
        return dims * (elementType == ElementType.BFLOAT16 ? BFloat16.BYTES : Float.BYTES);
    }

    public ElementType elementType() {
        return elementType;
    }

    @Override
    public Iterator<float[]> vectorValues() {
        return vectorValues.copy();
    }

    private float[] ensureScoresScratch() {
        if (scoresScratch.length < numVectors) {
            scoresScratch = new float[numVectors];
        }
        return scoresScratch;
    }

}
