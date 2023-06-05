/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.script.field.vectors;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.VectorUtil;
import org.elasticsearch.Version;
import org.elasticsearch.index.mapper.vectors.VectorEncoderDecoder;

import java.util.List;

public class BinaryDenseVector implements DenseVector {

    private final BytesRef docVector;

    private final int dims;
    private final Version indexVersion;

    protected float[] decodedDocVector;

    public BinaryDenseVector(float[] decodedDocVector, BytesRef docVector, int dims, Version indexVersion) {
        this.decodedDocVector = decodedDocVector;
        this.docVector = docVector;
        this.indexVersion = indexVersion;
        this.dims = dims;
    }

    @Override
    public float[] getVector() {
        return decodedDocVector;
    }

    @Override
    public float getMagnitude() {
        return VectorEncoderDecoder.getMagnitude(indexVersion, docVector, decodedDocVector);
    }

    @Override
    public int dotProduct(byte[] queryVector) {
        throw new UnsupportedOperationException("use [double dotProduct(float[] queryVector)] instead");
    }

    @Override
    public double dotProduct(float[] queryVector) {
        return VectorUtil.dotProduct(decodedDocVector, queryVector);
    }

    @Override
    public double dotProduct(List<Number> queryVector) {
        double dotProduct = 0;
        for (int i = 0; i < queryVector.size(); i++) {
            dotProduct += decodedDocVector[i] * queryVector.get(i).floatValue();
        }
        return dotProduct;
    }

    @Override
    public int l1Norm(byte[] queryVector) {
        throw new UnsupportedOperationException("use [double l1Norm(float[] queryVector)] instead");
    }

    @Override
    public double l1Norm(float[] queryVector) {
        double l1norm = 0;
        for (int i = 0; i < queryVector.length; i++) {
            l1norm += Math.abs(queryVector[i] - decodedDocVector[i]);
        }
        return l1norm;
    }

    @Override
    public double l1Norm(List<Number> queryVector) {
        double l1norm = 0;
        for (int i = 0; i < queryVector.size(); i++) {
            l1norm += Math.abs(queryVector.get(i).floatValue() - decodedDocVector[i]);
        }
        return l1norm;
    }

    @Override
    public double l2Norm(byte[] queryVector) {
        throw new UnsupportedOperationException("use [double l2Norm(float[] queryVector)] instead");
    }

    @Override
    public double l2Norm(float[] queryVector) {
        return Math.sqrt(VectorUtil.squareDistance(queryVector, decodedDocVector));
    }

    @Override
    public double l2Norm(List<Number> queryVector) {
        double l2norm = 0;
        for (int i = 0; i < queryVector.size(); i++) {
            double diff = decodedDocVector[i] - queryVector.get(i).floatValue();
            l2norm += diff * diff;
        }
        return Math.sqrt(l2norm);
    }

    @Override
    public double cosineSimilarity(byte[] queryVector, float qvMagnitude) {
        throw new UnsupportedOperationException("use [double cosineSimilarity(float[] queryVector, boolean normalizeQueryVector)] instead");
    }

    @Override
    public double cosineSimilarity(float[] queryVector, boolean normalizeQueryVector) {
        if (normalizeQueryVector) {
            return dotProduct(queryVector) / (DenseVector.getMagnitude(queryVector) * getMagnitude());
        }
        return dotProduct(queryVector) / getMagnitude();
    }

    @Override
    public double cosineSimilarity(List<Number> queryVector) {
        return dotProduct(queryVector) / (DenseVector.getMagnitude(queryVector) * getMagnitude());
    }

    @Override
    public int size() {
        return 1;
    }

    @Override
    public boolean isEmpty() {
        return false;
    }

    @Override
    public int getDims() {
        return dims;
    }
}
