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
import org.apache.lucene.util.VectorUtil;
import org.elasticsearch.simdvec.ESVectorUtil;

import java.util.Iterator;

public class BitMultiDenseVector extends ByteMultiDenseVector {
    public BitMultiDenseVector(Iterator<byte[]> vectorValues, BytesRef magnitudesBytes, int numVecs, int dims) {
        super(vectorValues, magnitudesBytes, numVecs, dims);
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
    public float maxSimDotProduct(float[][] query) {
        float[] sums = new float[query.length];
        while (vectorValues.hasNext()) {
            byte[] vv = vectorValues.next();
            for (int i = 0; i < query.length; i++) {
                sums[i] += ESVectorUtil.ipFloatBit(query[i], vv);
            }
        }
        float max = -Float.MAX_VALUE;
        for (float s : sums) {
            max = Math.max(max, s);
        }
        return max;
    }

    @Override
    public float maxSimDotProduct(byte[][] query) {
        float[] sums = new float[query.length];
        if (query[0].length == dims) {
            while (vectorValues.hasNext()) {
                byte[] vv = vectorValues.next();
                for (int i = 0; i < query.length; i++) {
                    sums[i] += ESVectorUtil.andBitCount(query[i], vv);
                }
            }
        } else {
            while (vectorValues.hasNext()) {
                byte[] vv = vectorValues.next();
                for (int i = 0; i < query.length; i++) {
                    sums[i] += ESVectorUtil.ipByteBit(query[i], vv);
                }
            }
        }
        float max = -Float.MAX_VALUE;
        for (float s : sums) {
            max = Math.max(max, s);
        }
        return max;
    }

    @Override
    public float maxSimInvHamming(byte[][] query) {
        int bitCount = this.getDims();
        float[] sums = new float[query.length];
        while (vectorValues.hasNext()) {
            byte[] vv = vectorValues.next();
            for (int i = 0; i < query.length; i++) {
                sums[i] += ((bitCount - VectorUtil.xorBitCount(vv, query[i])) / (float) bitCount);
            }
        }
        float max = -Float.MAX_VALUE;
        for (float s : sums) {
            max = Math.max(s, max);
        }
        return max;
    }

    @Override
    public int getDims() {
        return dims * Byte.SIZE;
    }
}
