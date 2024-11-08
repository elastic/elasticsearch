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
import org.elasticsearch.index.mapper.vectors.VectorEncoderDecoder;

import java.util.List;

public class ByteMultiDenseVector implements MultiDenseVector {

    protected final List<byte[]> vectorValues;
    protected final int dims;

    private List<float[]> floatDocVectors;
    private float[] magnitudes;
    private final BytesRef magnitudesBytes;

    public ByteMultiDenseVector(List<byte[]> vectorValues, BytesRef magnitudesBytes, int dims) {
        this.vectorValues = vectorValues;
        this.dims = dims;
        this.magnitudesBytes = magnitudesBytes;
    }

    @Override
    public List<float[]> getVectors() {
        if (floatDocVectors == null) {
            floatDocVectors = new java.util.ArrayList<>(vectorValues.size());
            for (byte[] vectorValue : vectorValues) {
                float[] floatDocVector = new float[dims];
                for (int i = 0; i < dims; i++) {
                    floatDocVector[i] = vectorValue[i];
                }
                floatDocVectors.add(floatDocVector);
            }
        }
        return floatDocVectors;
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
        return vectorValues.size();
    }
}
