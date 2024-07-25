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
import org.elasticsearch.core.SuppressForbidden;

import java.nio.ByteBuffer;
import java.util.List;

public class ByteBinaryDenseVector implements DenseVector {

    public static final int MAGNITUDE_BYTES = 4;

    private final BytesRef docVector;
    private final byte[] vectorValue;
    protected final int dims;

    private float[] floatDocVector;
    private boolean magnitudeDecoded;
    private float magnitude;

    public ByteBinaryDenseVector(byte[] vectorValue, BytesRef docVector, int dims) {
        this.docVector = docVector;
        this.dims = dims;
        this.vectorValue = vectorValue;
    }

    @Override
    public float[] getVector() {
        if (floatDocVector == null) {
            floatDocVector = new float[dims];
            for (int i = 0; i < dims; i++) {
                floatDocVector[i] = vectorValue[i];
            }
        }
        return floatDocVector;
    }

    @Override
    public float getMagnitude() {
        if (magnitudeDecoded == false) {
            magnitude = ByteBuffer.wrap(docVector.bytes, docVector.offset + dims, MAGNITUDE_BYTES).getFloat();
            magnitudeDecoded = true;
        }
        return magnitude;
    }

    @Override
    public int dotProduct(byte[] queryVector) {
        return VectorUtil.dotProduct(queryVector, vectorValue);
    }

    @Override
    public double dotProduct(float[] queryVector) {
        throw new UnsupportedOperationException("use [int dotProduct(byte[] queryVector)] instead");
    }

    @Override
    public double dotProduct(List<Number> queryVector) {
        int result = 0;
        for (int i = 0; i < queryVector.size(); i++) {
            result += vectorValue[i] * queryVector.get(i).intValue();
        }
        return result;
    }

    @SuppressForbidden(reason = "used only for bytes so it cannot overflow")
    private static int abs(int value) {
        return Math.abs(value);
    }

    @Override
    public int l1Norm(byte[] queryVector) {
        int result = 0;
        for (int i = 0; i < queryVector.length; i++) {
            result += abs(vectorValue[i] - queryVector[i]);
        }
        return result;
    }

    @Override
    public double l1Norm(float[] queryVector) {
        throw new UnsupportedOperationException("use [int l1Norm(byte[] queryVector)] instead");
    }

    @Override
    public double l1Norm(List<Number> queryVector) {
        int result = 0;
        for (int i = 0; i < queryVector.size(); i++) {
            result += abs(vectorValue[i] - queryVector.get(i).intValue());
        }
        return result;
    }

    @Override
    public int hamming(byte[] queryVector) {
        return ESVectorUtil.xorBitCount(queryVector, vectorValue);
    }

    @Override
    public int hamming(List<Number> queryVector) {
        int distance = 0;
        for (int i = 0; i < queryVector.size(); i++) {
            distance += Integer.bitCount((queryVector.get(i).intValue() ^ vectorValue[i]) & 0xFF);
        }
        return distance;
    }

    @Override
    public double l2Norm(byte[] queryVector) {
        return Math.sqrt(VectorUtil.squareDistance(queryVector, vectorValue));
    }

    @Override
    public double l2Norm(float[] queryVector) {
        throw new UnsupportedOperationException("use [double l2Norm(byte[] queryVector)] instead");
    }

    @Override
    public double l2Norm(List<Number> queryVector) {
        int result = 0;
        for (int i = 0; i < queryVector.size(); i++) {
            int diff = vectorValue[i] - queryVector.get(i).intValue();
            result += diff * diff;
        }
        return Math.sqrt(result);
    }

    @Override
    public double cosineSimilarity(byte[] queryVector, float qvMagnitude) {
        return dotProduct(queryVector) / (qvMagnitude * getMagnitude());
    }

    @Override
    public double cosineSimilarity(float[] queryVector, boolean normalizeQueryVector) {
        throw new UnsupportedOperationException("use [double cosineSimilarity(byte[] queryVector, float qvMagnitude)] instead");
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
