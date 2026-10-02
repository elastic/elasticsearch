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
import org.elasticsearch.simdvec.ESVectorUtil;

import java.util.Arrays;

public class BitRankVectors extends ByteRankVectors {
    public BitRankVectors(VectorIterator<byte[]> vectorValues, BytesRef magnitudesBytes, int numVecs, int dims) {
        this(vectorValues, magnitudesBytes, numVecs, dims, null);
    }

    public BitRankVectors(VectorIterator<byte[]> vectorValues, BytesRef magnitudesBytes, int numVecs, int dims, BytesRef vectorBytes) {
        super(vectorValues, magnitudesBytes, numVecs, dims, vectorBytes);
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
        vectorValues.reset();
        float[] maxes = ensureMaxesScratch(query.length);
        Arrays.fill(maxes, 0, query.length, Float.NEGATIVE_INFINITY);
        while (vectorValues.hasNext()) {
            byte[] vv = vectorValues.next();
            for (int i = 0; i < query.length; i++) {
                maxes[i] = Math.max(maxes[i], ESVectorUtil.ipFloatBit(query[i], vv));
            }
        }
        return ESVectorUtil.sum(maxes, query.length);
    }

    @Override
    public float maxSimDotProduct(byte[][] query) {
        vectorValues.reset();
        float[] maxes = ensureMaxesScratch(query.length);
        Arrays.fill(maxes, 0, query.length, Float.NEGATIVE_INFINITY);
        if (query[0].length == dims) {
            while (vectorValues.hasNext()) {
                byte[] vv = vectorValues.next();
                for (int i = 0; i < query.length; i++) {
                    maxes[i] = Math.max(maxes[i], ESVectorUtil.andBitCount(query[i], vv));
                }
            }
        } else {
            while (vectorValues.hasNext()) {
                byte[] vv = vectorValues.next();
                for (int i = 0; i < query.length; i++) {
                    maxes[i] = Math.max(maxes[i], ESVectorUtil.ipByteBit(query[i], vv));
                }
            }
        }
        return ESVectorUtil.sum(maxes, query.length);
    }

    // maxSimInvHamming is not overridden: the inherited implementation already normalizes by the bit count,
    // since ByteRankVectors derives it from [dims * Byte.SIZE], which is exactly this class' [getDims()].

    @Override
    public int getDims() {
        return dims * Byte.SIZE;
    }
}
