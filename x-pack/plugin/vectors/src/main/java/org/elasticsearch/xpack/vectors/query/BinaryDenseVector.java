/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.vectors.query;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.Version;
import org.elasticsearch.xpack.vectors.mapper.VectorEncoderDecoder;

import java.nio.ByteBuffer;

public class BinaryDenseVector implements DenseVector {
    protected final BytesRef docVector;
    protected final int dims;
    protected final Version indexVersion;

    protected float[] decodedDocVector;
    protected float magnitude = -1.0f;

    public BinaryDenseVector(BytesRef docVector, int dims, Version indexVersion) {
        this.docVector = docVector;
        this.indexVersion = indexVersion;
        this.dims = dims;
    }

    @Override
    public float[] getVector() {
        if (decodedDocVector == null) {
            decodedDocVector = new float[dims];
            VectorEncoderDecoder.decodeDenseVector(docVector, decodedDocVector);
        }
        return decodedDocVector;
    }

    @Override
    public float getMagnitude() {
        if (magnitude != -1.0f) {
            return magnitude;
        }
        magnitude = VectorEncoderDecoder.getMagnitude(indexVersion, docVector);
        return magnitude;
    }

    @Override
    public double dotProduct(float[] queryVector) {
        ByteBuffer byteBuffer = wrap(docVector);

        double dotProduct = 0;
        for (float v : queryVector) {
            dotProduct += byteBuffer.getFloat() * v;
        }
        return dotProduct;
    }

    @Override
    public double dotProduct(QueryVector queryVector) {
        ByteBuffer byteBuffer = wrap(docVector);

        double dotProduct = 0;
        for (int i = 0; i < queryVector.size(); i++) {
            dotProduct += byteBuffer.getFloat() * queryVector.get(i);
        }
        return dotProduct;
    }

    /**
     * dotProduct of doc vector and query vector that normalizes the query vector while performing the calculation.
     */
    protected double dotProduct(QueryVector qv, float qvMagnitude) {
        ByteBuffer byteBuffer = wrap(docVector);

        double dotProduct = 0;
        for (int i = 0; i < qv.size(); i++) {
            dotProduct += byteBuffer.getFloat() * (qv.get(i) / qvMagnitude);
        }
        return dotProduct;
    }

    @Override
    public double l1Norm(float[] queryVector) {
        ByteBuffer byteBuffer = wrap(docVector);

        double l1norm = 0;
        for (float v : queryVector) {
            l1norm += Math.abs(v - byteBuffer.getFloat());
        }
        return l1norm;
    }

    @Override
    public double l1Norm(QueryVector queryVector) {
        ByteBuffer byteBuffer = wrap(docVector);

        double l1norm = 0;
        for (int i = 0; i < queryVector.size(); i++) {
            l1norm += Math.abs(queryVector.get(i) - byteBuffer.getFloat());
        }
        return l1norm;
    }

    @Override
    public double l2Norm(float[] queryVector) {
        ByteBuffer byteBuffer = wrap(docVector);
        double l2norm = 0;
        for (float queryValue : queryVector) {
            double diff = byteBuffer.getFloat() - queryValue;
            l2norm += diff * diff;
        }
        return Math.sqrt(l2norm);
    }

    @Override
    public double l2Norm(QueryVector queryVector) {
        ByteBuffer byteBuffer = wrap(docVector);
        double l2norm = 0;
        for (int i = 0; i < queryVector.size(); i++) {
            double diff = byteBuffer.getFloat() - queryVector.get(i);
            l2norm += diff * diff;
        }
        return Math.sqrt(l2norm);
    }

    @Override
    public double cosineSimilarity(float[] queryVector) {
        return dotProduct(queryVector) / getMagnitude();
    }

    @Override
    public double cosineSimilarity(QueryVector queryVector) {
        return dotProduct(queryVector, queryVector.getMagnitude()) / getMagnitude();
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

    protected static ByteBuffer wrap(BytesRef dv) {
        return ByteBuffer.wrap(dv.bytes, dv.offset, dv.length);
    }
}
