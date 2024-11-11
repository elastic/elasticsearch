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

import java.util.Iterator;

import static org.elasticsearch.index.mapper.vectors.VectorEncoderDecoder.getMultiMagnitudes;

public class FloatMultiDenseVector implements MultiDenseVector {

    private final BytesRef magnitudes;
    private float[] magnitudesArray = null;
    private final int dims;
    private final int numVectors;
    private final Iterator<float[]> decodedDocVector;

    public FloatMultiDenseVector(Iterator<float[]> decodedDocVector, BytesRef magnitudes, int numVectors, int dims) {
        assert magnitudes.length == numVectors * Float.BYTES;
        this.decodedDocVector = decodedDocVector;
        this.magnitudes = magnitudes;
        this.numVectors = numVectors;
        this.dims = dims;
    }

    @Override
    public Iterator<float[]> getVectors() {
        return decodedDocVector;
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
