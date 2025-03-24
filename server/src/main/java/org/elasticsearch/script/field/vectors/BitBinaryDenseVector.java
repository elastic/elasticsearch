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

import static org.elasticsearch.simdvec.ESVectorUtil.andBitCount;
import static org.elasticsearch.simdvec.ESVectorUtil.ipByteBit;
import static org.elasticsearch.simdvec.ESVectorUtil.ipFloatBit;

public class BitBinaryDenseVector extends ByteBinaryDenseVector {

    public BitBinaryDenseVector(byte[] vectorValue, BytesRef docVector, int dims) {
        super(vectorValue, docVector, dims);
    }

    @Override
    public void checkDimensions(int qvDims) {
        if (qvDims != dims) {
            throw new IllegalArgumentException(
                "The query vector has a different number of dimensions ["
                    + qvDims * Byte.SIZE
                    + "] than the document vectors ["
                    + dims * Byte.SIZE
                    + "]."
            );
        }
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
        if (queryVector.length == vectorValue.length) {
            // assume that the query vector is a bit vector and do a bitwise AND
            return andBitCount(vectorValue, queryVector);
        }
        return ipByteBit(queryVector, vectorValue);
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
        return ipFloatBit(queryVector, vectorValue);
    }

    @Override
    public int getDims() {
        return dims * Byte.SIZE;
    }
}
