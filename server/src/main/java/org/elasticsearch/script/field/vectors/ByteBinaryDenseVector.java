/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.script.field.vectors;

import org.apache.lucene.util.BytesRef;

import java.nio.ByteBuffer;
import java.util.List;

public class ByteBinaryDenseVector implements DenseVector {

    protected final BytesRef docVector;
    protected final int dims;

    protected float[] floatDocVector;
    protected byte[] byteDocVector;
    protected boolean magnitudeDecoded;
    protected float magnitude;

    public ByteBinaryDenseVector(BytesRef docVector, int dims) {
        this.docVector = docVector;
        this.dims = dims;
    }

    @Override
    public float[] getVector() {
        if (floatDocVector == null) {
            floatDocVector = new float[dims];

            int i = 0;
            int j = docVector.offset;
            while (i < docVector.length) {
                floatDocVector[i++] = docVector.bytes[j++];
            }
        }

        return floatDocVector;
    }

    @Override
    public byte[] asBytes() {
        if (byteDocVector == null) {
            byteDocVector = new byte[dims];
            System.arraycopy(docVector.bytes, docVector.offset, byteDocVector, 0, dims);
        }

        return byteDocVector;
    }

    @Override
    public float getMagnitude() {
        if (magnitudeDecoded == false) {
            magnitude = ByteBuffer.wrap(docVector.bytes, docVector.offset + dims, docVector.length).getFloat();
            magnitudeDecoded = true;
        }
        return magnitude;
    }

    @Override
    public double dotProduct(byte[] queryVector) {
        int result = 0;
        int i = 0;
        int j = docVector.offset;
        while (i < dims) {
            result +=  docVector.bytes[j++] * queryVector[i++];
        }
        return result;
    }

    @Override
    public double dotProduct(float[] queryVector) {
        int result = 0;
        int i = 0;
        int j = docVector.offset;
        while (i < docVector.length) {
            result +=  docVector.bytes[j++] * (int)queryVector[i++];
        }
        return result;
    }

    @Override
    public double dotProduct(List<Number> queryVector) {
        int result = 0;
        int i = 0;
        int j = docVector.offset;
        while (i < dims) {
            result +=  docVector.bytes[j++] * queryVector.get(i++).intValue();
        }
        return result;
    }

    @Override
    public double l1Norm(byte[] queryVector) {
        int result = 0;
        int i = 0;
        int j = docVector.offset;
        while (i < dims) {
            result += Math.abs(docVector.bytes[j++] - queryVector[i++]);
        }
        return result;
    }

    @Override
    public double l1Norm(float[] queryVector) {
        int result = 0;
        int i = 0;
        int j = docVector.offset;
        while (i < dims) {
            result += Math.abs(docVector.bytes[j++] - (int)queryVector[i++]);
        }
        return result;
    }

    @Override
    public double l1Norm(List<Number> queryVector) {
        int result = 0;
        int i = 0;
        int j = docVector.offset;
        while (i < dims) {
            result += Math.abs(docVector.bytes[j++] - queryVector.get(i++).intValue());
        }
        return result;
    }

    @Override
    public double l2Norm(byte[] queryVector) {
        int result = 0;
        int i = 0;
        int j = docVector.offset;
        while (i < dims) {
            int diff = docVector.bytes[j++] - queryVector[i++];
            result += diff * diff;
        }
        return Math.sqrt(result);
    }

    @Override
    public double l2Norm(float[] queryVector) {
        int result = 0;
        int i = 0;
        int j = docVector.offset;
        while (i < dims) {
            int diff = docVector.bytes[j++] - (int)queryVector[i++];
            result += diff * diff;
        }
        return Math.sqrt(result);
    }

    @Override
    public double l2Norm(List<Number> queryVector) {
        int result = 0;
        int i = 0;
        int j = docVector.offset;
        while (i < dims) {
            int diff = docVector.bytes[j++] - queryVector.get(i++).intValue();
            result += diff * diff;
        }
        return Math.sqrt(result);
    }

    @Override
    public double cosineSimilarity(byte[] queryVector, boolean normalizeQueryVector) {
        if (normalizeQueryVector) {
            return dotProduct(queryVector) / (DenseVector.getMagnitude(queryVector) * getMagnitude());
        }
        return dotProduct(queryVector) / getMagnitude();
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
