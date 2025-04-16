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

import java.util.Arrays;
import java.util.Iterator;

import static org.elasticsearch.index.mapper.vectors.VectorEncoderDecoder.getMultiMagnitudes;

public class FloatRankVectors implements RankVectors {

    private final BytesRef magnitudes;
    private float[] magnitudesArray = null;
    private final int dims;
    private final int numVectors;
    private final VectorIterator<float[]> vectorValues;

    public FloatRankVectors(VectorIterator<float[]> decodedDocVector, BytesRef magnitudes, int numVectors, int dims) {
        assert magnitudes.length == numVectors * Float.BYTES;
        this.vectorValues = decodedDocVector;
        this.magnitudes = magnitudes;
        this.numVectors = numVectors;
        this.dims = dims;
    }

    @Override
    public float maxSimDotProduct(float[][] query) {
        vectorValues.reset();
        float[] maxes = new float[query.length];
        Arrays.fill(maxes, Float.NEGATIVE_INFINITY);
        while (vectorValues.hasNext()) {
            float[] vv = vectorValues.next();
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
}
