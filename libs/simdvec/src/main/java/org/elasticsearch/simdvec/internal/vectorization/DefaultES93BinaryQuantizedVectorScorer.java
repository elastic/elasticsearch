/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.simdvec.internal.vectorization;

import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.store.IndexInput;
import org.elasticsearch.simdvec.ES93BinaryQuantizedVectorScorer;
import org.elasticsearch.simdvec.ESVectorUtil;

import java.io.IOException;
import java.nio.ByteBuffer;

public class DefaultES93BinaryQuantizedVectorScorer extends ES93BinaryQuantizedVectorScorer {

    protected final IndexInput slice;
    private int lastOrd = -1;

    float[] correctiveValues;
    byte[] binaryValue;
    ByteBuffer byteBuffer;
    int quantizedComponentSum;

    public DefaultES93BinaryQuantizedVectorScorer(IndexInput slice, int dimension, int numBytes) {
        super(dimension, numBytes);
        this.slice = slice;
    }

    @Override
    public float score(
        byte[] q,
        float queryLowerInterval,
        float queryUpperInterval,
        int queryQuantizedComponentSum,
        float queryAdditionalCorrection,
        VectorSimilarityFunction similarityFunction,
        float centroidDp,
        int targetOrd
    ) throws IOException {
        var d = values(targetOrd);
        float qcDist = ESVectorUtil.ipByteBinByte(q, d);
        return applyCorrections(
            dimensions,
            similarityFunction,
            centroidDp,
            qcDist,
            queryLowerInterval,
            queryUpperInterval,
            queryAdditionalCorrection,
            queryQuantizedComponentSum,
            correctiveValues[0],
            correctiveValues[1],
            correctiveValues[2],
            quantizedComponentSum
        );
    }

    private byte[] values(int targetOrd) throws IOException {
        if (binaryValue == null) {
            this.correctiveValues = new float[3];
            this.byteBuffer = ByteBuffer.allocate(numBytes);
            this.binaryValue = byteBuffer.array();
        }
        if (lastOrd == targetOrd) {
            return binaryValue;
        }
        slice.seek((long) targetOrd * byteSize);
        slice.readBytes(byteBuffer.array(), byteBuffer.arrayOffset(), numBytes);
        slice.readFloats(correctiveValues, 0, 3);
        quantizedComponentSum = Short.toUnsignedInt(slice.readShort());
        lastOrd = targetOrd;
        return binaryValue;
    }
}
