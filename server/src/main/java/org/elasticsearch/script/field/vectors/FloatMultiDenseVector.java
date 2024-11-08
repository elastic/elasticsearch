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

import java.util.List;

public class FloatMultiDenseVector implements MultiDenseVector {

    private final BytesRef magnitudes;
    private boolean decodedMagnitude;
    private final int dims;
    private final List<float[]> decodedDocVector;
    private List<Float> decodedMagnitudes;

    public FloatMultiDenseVector(List<float[]> decodedDocVector, List<Float> decodedMagnitudes, BytesRef docVector, BytesRef magnitudes, int dims) {
        assert decodedDocVector.size() == decodedMagnitudes.size();
        this.decodedDocVector = decodedDocVector;
        this.decodedMagnitudes = decodedMagnitudes;
        this.docVector = docVector;
        this.magnitudes = magnitudes;
        this.dims = dims;
    }

    @Override
    public List<float[]> getVectors() {
        return decodedDocVector;
    }

    @Override
    public List<Float> getMagnitudes() {
        return decodedMagnitudes;
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
        return decodedDocVector.size();
    }
}
