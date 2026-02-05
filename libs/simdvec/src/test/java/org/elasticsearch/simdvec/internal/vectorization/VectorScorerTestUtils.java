/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.simdvec.internal.vectorization;

import org.apache.lucene.store.IndexOutput;
import org.elasticsearch.index.codec.vectors.BQVectorUtils;
import org.elasticsearch.index.codec.vectors.OptimizedScalarQuantizer;
import org.elasticsearch.simdvec.ESVectorUtil;

import java.io.IOException;

public class VectorScorerTestUtils {

    public record VectorData(byte[] vector, float lowerInterval, float upperInterval, float additionalCorrection, short quantizedComponentSum) {}

    private static final byte queryBits = 4;

    private VectorScorerTestUtils() {}

    public static VectorData createBinarizedIndexData(
        float[] values,
        float[] centroid,
        OptimizedScalarQuantizer binaryQuantizer,
        int dimension
    ) {
        int discretizedDimension = BQVectorUtils.discretize(dimension, 64);

        int[] quantizationScratch = new int[dimension];
        byte[] toIndex = new byte[discretizedDimension / 8];

        float[] scratch = new float[dimension];

        OptimizedScalarQuantizer.QuantizationResult r = binaryQuantizer.scalarQuantize(
            values,
            scratch,
            quantizationScratch,
            (byte)1,
            centroid
        );
        // pack and store document bit vector
        ESVectorUtil.packAsBinary(quantizationScratch, toIndex);

        assert r.quantizedComponentSum() >= 0 && r.quantizedComponentSum() <= 0xffff;

        return new VectorData(toIndex, r.lowerInterval(), r.upperInterval(),
            r.additionalCorrection(), (short) r.quantizedComponentSum());
    }

    static VectorData createBinarizedQueryData(
        float[] floatVectorValues,
        float[] centroid,
        OptimizedScalarQuantizer binaryQuantizer,
        int dimension
    ) {
        int discretizedDimension = BQVectorUtils.discretize(dimension, 64);

        int[] quantizationScratch = new int[dimension];
        byte[] toQuery = new byte[(discretizedDimension / 8) * queryBits];

        float[] scratch = new float[dimension];

        OptimizedScalarQuantizer.QuantizationResult r = binaryQuantizer.scalarQuantize(
            floatVectorValues,
            scratch,
            quantizationScratch,
            queryBits,
            centroid
        );

        // pack and store the 4bit query vector
        ESVectorUtil.transposeHalfByte(quantizationScratch, toQuery);
        assert r.quantizedComponentSum() >= 0 && r.quantizedComponentSum() <= 0xffff;

        return new VectorData(toQuery, r.lowerInterval(), r.upperInterval(),
            r.additionalCorrection(), (short) r.quantizedComponentSum());
    }

    static void writeBinarizedVectorData(IndexOutput binarizedQueryData, VectorData vectorData) throws IOException {
        binarizedQueryData.writeBytes(vectorData.vector, vectorData.vector.length);
        binarizedQueryData.writeInt(Float.floatToIntBits(vectorData.lowerInterval()));
        binarizedQueryData.writeInt(Float.floatToIntBits(vectorData.upperInterval()));
        binarizedQueryData.writeInt(Float.floatToIntBits(vectorData.additionalCorrection()));
        binarizedQueryData.writeShort(vectorData.quantizedComponentSum());
    }
}
