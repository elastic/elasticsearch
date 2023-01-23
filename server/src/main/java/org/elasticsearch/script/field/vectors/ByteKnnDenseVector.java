/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.script.field.vectors;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.core.SuppressForbidden;

import java.util.List;

public class ByteKnnDenseVector implements DenseVector {

    protected final BytesRef docVector;

    protected float[] floatDocVector;
    protected boolean magnitudeCalculated = false;
    protected float magnitude;

    public ByteKnnDenseVector(BytesRef vector) {
        this.docVector = vector;
    }

    @Override
    public float[] getVector() {
        // TODO it would be really nice if we didn't transform the `byte[]` arrays to `float[]`
        if (floatDocVector == null) {
            floatDocVector = new float[docVector.length];

            int i = 0;
            int j = docVector.offset;

            while (i < docVector.length) {
                floatDocVector[i++] = docVector.bytes[j++];
            }
        }

        return floatDocVector;
    }

    @Override
    public float getMagnitude() {
        if (magnitudeCalculated == false) {
            magnitude = DenseVector.getMagnitude(docVector, docVector.length);
            magnitudeCalculated = true;
        }
        return magnitude;
    }

    @Override
    public int dotProduct(byte[] queryVector) {
        int result = 0;
        int i = 0;
        int j = docVector.offset;
        while (i < docVector.length) {
            result += docVector.bytes[j++] * queryVector[i++];
        }
        return result;
    }

    @Override
    public double dotProduct(float[] queryVector) {
        throw new UnsupportedOperationException("use [int dotProduct(byte[] queryVector)] instead");
    }

    @Override
    public double dotProduct(List<Number> queryVector) {
        int result = 0;
        int i = 0;
        int j = docVector.offset;
        while (i < docVector.length) {
            result += docVector.bytes[j++] * queryVector.get(i++).intValue();
        }
        return result;
    }

    @SuppressForbidden(reason = "used only for bytes so it cannot overflow")
    private int abs(int value) {
        return Math.abs(value);
    }

    @Override
    public int l1Norm(byte[] queryVector) {
        int result = 0;
        int i = 0;
        int j = docVector.offset;
        while (i < docVector.length) {
            result += abs(docVector.bytes[j++] - queryVector[i++]);
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
        int i = 0;
        int j = docVector.offset;
        while (i < docVector.length) {
            result += abs(docVector.bytes[j++] - queryVector.get(i++).intValue());
        }
        return result;
    }

    @Override
    public double l2Norm(byte[] queryVector) {
        int result = 0;
        int i = 0;
        int j = docVector.offset;
        while (i < docVector.length) {
            int diff = docVector.bytes[j++] - queryVector[i++];
            result += diff * diff;
        }
        return Math.sqrt(result);
    }

    @Override
    public double l2Norm(float[] queryVector) {
        throw new UnsupportedOperationException("use [double l2Norm(byte[] queryVector)] instead");
    }

    @Override
    public double l2Norm(List<Number> queryVector) {
        int result = 0;
        int i = 0;
        int j = docVector.offset;
        while (i < docVector.length) {
            int diff = docVector.bytes[j++] - queryVector.get(i++).intValue();
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
