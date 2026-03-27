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
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.VectorUtil;
import org.elasticsearch.index.codec.vectors.OptimizedScalarQuantizer;
import org.elasticsearch.index.codec.vectors.diskbbq.next.ESNextDiskBBQVectorsFormat;
import org.elasticsearch.simdvec.ESVectorUtil;

import java.io.IOException;
import java.util.Random;

public class VectorScorerTestUtils {

    public record OSQVectorData(
        byte[] quantizedVector,
        float lowerInterval,
        float upperInterval,
        float additionalCorrection,
        int quantizedComponentSum
    ) {}

    public static OSQVectorData createOSQIndexData(
        float[] values,
        float[] centroid,
        OptimizedScalarQuantizer quantizer,
        int dimensions,
        byte indexBits,
        int vectorPackedLengthInBytes
    ) {

        final float[] residualScratch = new float[dimensions];
        final int[] scratch = new int[dimensions];
        final byte[] qVector = new byte[vectorPackedLengthInBytes];

        OptimizedScalarQuantizer.QuantizationResult result = quantizer.scalarQuantize(
            values,
            residualScratch,
            scratch,
            indexBits,
            centroid
        );
        ESNextDiskBBQVectorsFormat.QuantEncoding.fromBits(indexBits).pack(scratch, qVector);

        return new OSQVectorData(
            qVector,
            result.lowerInterval(),
            result.upperInterval(),
            result.additionalCorrection(),
            result.quantizedComponentSum()
        );
    }

    public static OSQVectorData createOSQQueryData(
        float[] query,
        float[] centroid,
        OptimizedScalarQuantizer quantizer,
        int dimensions,
        byte queryBits,
        int queryVectorPackedLengthInBytes
    ) {
        final float[] residualScratch = new float[dimensions];
        final int[] scratch = new int[dimensions];

        OptimizedScalarQuantizer.QuantizationResult queryCorrections = quantizer.scalarQuantize(
            query,
            residualScratch,
            scratch,
            queryBits,
            centroid
        );
        final byte[] quantizeQuery = new byte[queryVectorPackedLengthInBytes];
        ESVectorUtil.transposeHalfByte(scratch, quantizeQuery);

        return new OSQVectorData(
            quantizeQuery,
            queryCorrections.lowerInterval(),
            queryCorrections.upperInterval(),
            queryCorrections.additionalCorrection(),
            queryCorrections.quantizedComponentSum()
        );
    }

    public static void writeSingleOSQVectorData(IndexOutput out, OSQVectorData vectorData) throws IOException {
        out.writeBytes(vectorData.quantizedVector(), 0, vectorData.quantizedVector().length);
        out.writeInt(Float.floatToIntBits(vectorData.lowerInterval()));
        out.writeInt(Float.floatToIntBits(vectorData.upperInterval()));
        out.writeInt(Float.floatToIntBits(vectorData.additionalCorrection()));
        out.writeInt(vectorData.quantizedComponentSum());
    }

    public static void writeBulkOSQVectorData(int bulkSize, IndexOutput out, VectorScorerTestUtils.OSQVectorData[] vectors)
        throws IOException {
        for (int j = 0; j < bulkSize; j++) {
            out.writeBytes(vectors[j].quantizedVector(), 0, vectors[j].quantizedVector().length);
        }
        writeCorrections(vectors, out);
    }

    private static void writeCorrections(VectorScorerTestUtils.OSQVectorData[] corrections, IndexOutput out) throws IOException {
        for (var correction : corrections) {
            out.writeInt(Float.floatToIntBits(correction.lowerInterval()));
        }
        for (var correction : corrections) {
            out.writeInt(Float.floatToIntBits(correction.upperInterval()));
        }
        for (var correction : corrections) {
            out.writeInt(correction.quantizedComponentSum());
        }
        for (var correction : corrections) {
            out.writeInt(Float.floatToIntBits(correction.additionalCorrection()));
        }
    }

    public static void randomVector(Random random, float[] vector, VectorSimilarityFunction vectorSimilarityFunction) {
        for (int i = 0; i < vector.length; i++) {
            vector[i] = random.nextFloat();
        }
        if (vectorSimilarityFunction != VectorSimilarityFunction.EUCLIDEAN) {
            VectorUtil.l2normalize(vector);
        }
    }
}
