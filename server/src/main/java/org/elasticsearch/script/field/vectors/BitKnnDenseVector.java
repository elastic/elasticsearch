/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.script.field.vectors;

import java.util.List;

public class BitKnnDenseVector extends ByteKnnDenseVector {

    public BitKnnDenseVector(byte[] vector) {
        super(vector);
    }

    @Override
    public void checkDimensions(int qvDims) {
        if (qvDims != docVector.length) {
            throw new IllegalArgumentException(
                "The query vector has a different number of dimensions ["
                    + qvDims * Byte.SIZE
                    + "] than the document vectors ["
                    + docVector.length * Byte.SIZE
                    + "]."
            );
        }
    }

    @Override
    public float getMagnitude() {
        if (magnitudeCalculated == false) {
            magnitude = DenseVector.getBitMagnitude(docVector, docVector.length);
            magnitudeCalculated = true;
        }
        return magnitude;
    }

    @Override
    public int l1Norm(byte[] queryVector) {
        return hamming(queryVector);
    }

    @Override
    public double l1Norm(List<Number> queryVector) {
        return hamming(queryVector);
    }

    @Override
    public double l2Norm(byte[] queryVector) {
        return Math.sqrt(hamming(queryVector));
    }

    @Override
    public double l2Norm(List<Number> queryVector) {
        return Math.sqrt(hamming(queryVector));
    }

    @Override
    public int dotProduct(byte[] queryVector) {
        throw new UnsupportedOperationException("dotProduct is not supported for bit vectors.");
    }

    @Override
    public double cosineSimilarity(float[] queryVector, boolean normalizeQueryVector) {
        throw new UnsupportedOperationException("cosineSimilarity is not supported for bit vectors.");
    }

    @Override
    public double dotProduct(List<Number> queryVector) {
        throw new UnsupportedOperationException("dotProduct is not supported for bit vectors.");
    }

    @Override
    public double cosineSimilarity(byte[] queryVector, float qvMagnitude) {
        throw new UnsupportedOperationException("cosineSimilarity is not supported for bit vectors.");
    }

    @Override
    public double cosineSimilarity(List<Number> queryVector) {
        throw new UnsupportedOperationException("cosineSimilarity is not supported for bit vectors.");
    }

    @Override
    public double dotProduct(float[] queryVector) {
        throw new UnsupportedOperationException("dotProduct is not supported for bit vectors.");
    }

    @Override
    public int getDims() {
        return docVector.length * Byte.SIZE;
    }
}
