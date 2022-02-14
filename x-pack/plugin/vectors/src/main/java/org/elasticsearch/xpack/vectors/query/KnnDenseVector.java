/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.vectors.query;

import org.apache.lucene.util.VectorUtil;

import java.util.Arrays;

public class KnnDenseVector implements DenseVector {
    protected final float[] docVector;

    public KnnDenseVector(float[] docVector) {
        this.docVector = docVector;
    }

    @Override
    public float[] getVector() {
        // we need to copy the value, since {@link VectorValues} can reuse
        // the underlying array across documents
        return Arrays.copyOf(docVector, docVector.length);
    }

    @Override
    public float getMagnitude() {
        return DenseVector.getMagnitude(docVector);
    }

    @Override
    public double dotProduct(float[] queryVector) {
        return VectorUtil.dotProduct(docVector, queryVector);
    }

    @Override
    public double dotProduct(QueryVector queryVector) {
        double dotProduct = 0;
        for (int i = 0; i < docVector.length; i++) {
            dotProduct += docVector[i] * queryVector.get(i);
        }
        return dotProduct;
    }

    /**
     * dotProduct of doc vector and query vector that normalizes the query vector while performing the calculation.
     */
    protected double dotProduct(QueryVector queryVector, float qvMagnitude) {
        double dotProduct = 0;
        for (int i = 0; i < docVector.length; i++) {
            dotProduct += docVector[i] * (queryVector.get(i) / qvMagnitude);
        }
        return dotProduct;
    }

    protected double dotProduct(float[] queryVector, float qvMagnitude) {
        double dotProduct = 0;
        for (int i = 0; i < docVector.length; i++) {
            dotProduct += docVector[i] * (queryVector[i] / qvMagnitude);
        }
        return dotProduct;
    }

    @Override
    public double l1Norm(float[] queryVector) {
        double result = 0.0;
        for (int i = 0; i < docVector.length; i++) {
            result += Math.abs(docVector[i] - queryVector[i]);
        }
        return result;
    }

    @Override
    public double l1Norm(QueryVector queryVector) {
        double result = 0.0;
        for (int i = 0; i < docVector.length; i++) {
            result += Math.abs(docVector[i] - queryVector.get(i));
        }
        return result;
    }

    @Override
    public double l2Norm(float[] queryVector) {
        return Math.sqrt(VectorUtil.squareDistance(docVector, queryVector));
    }

    @Override
    public double l2Norm(QueryVector queryVector) {
        double l2norm = 0;
        for (int i = 0; i < docVector.length; i++) {
            double diff = docVector[i] - queryVector.get(i);
            l2norm += diff * diff;
        }
        return Math.sqrt(l2norm);
    }

    @Override
    public double cosineSimilarity(float[] queryVector, boolean normalizeQueryVector) {
        if (normalizeQueryVector) {
            return dotProduct(queryVector, DenseVector.getMagnitude(queryVector)) / getMagnitude();
        }

        return dotProduct(queryVector) / getMagnitude();
    }

    @Override
    public double cosineSimilarity(QueryVector queryVector) {
        return dotProduct(queryVector, queryVector.getMagnitude()) / getMagnitude();
    }

    @Override
    public boolean isEmpty() {
        return false;
    }

    @Override
    public int getDims() {
        return docVector.length;
    }

    @Override
    public int size() {
        return 1;
    }
}
