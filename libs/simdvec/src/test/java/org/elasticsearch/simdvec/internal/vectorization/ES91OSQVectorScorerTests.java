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
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.store.NIOFSDirectory;
import org.apache.lucene.util.VectorUtil;
import org.elasticsearch.index.codec.vectors.BQVectorUtils;
import org.elasticsearch.index.codec.vectors.OptimizedScalarQuantizer;
import org.elasticsearch.simdvec.ES91Int4VectorsScorer;
import org.elasticsearch.simdvec.ES91OSQVectorsScorer;
import org.elasticsearch.simdvec.ESVectorUtil;

import java.io.IOException;

public class ES91OSQVectorScorerTests extends BaseVectorizationTests {

    public void testQuantizeScore() throws Exception {
        final int dimensions = random().nextInt(1, 2000);
        final int length = BQVectorUtils.discretize(dimensions, 64) / 8;
        final int numVectors = random().nextInt(1, 100);
        final byte[] vector = new byte[length];
        try (Directory dir = newRandomDirectory()) {
            try (IndexOutput out = dir.createOutput("tests.bin", IOContext.DEFAULT)) {
                for (int i = 0; i < numVectors; i++) {
                    random().nextBytes(vector);
                    out.writeBytes(vector, 0, length);
                }
            }
            final byte[] query = new byte[4 * length];
            random().nextBytes(query);
            try (IndexInput in = dir.openInput("tests.bin", IOContext.DEFAULT)) {
                // Work on a slice that has just the right number of bytes to make the test fail with an
                // index-out-of-bounds in case the implementation reads more than the allowed number of
                // padding bytes.
                final IndexInput slice = in.slice("test", 0, (long) length * numVectors);
                final ES91OSQVectorsScorer defaultScorer = defaultProvider().newES91OSQVectorsScorer(slice, dimensions);
                final ES91OSQVectorsScorer panamaScorer = maybePanamaProvider().newES91OSQVectorsScorer(in, dimensions);
                for (int i = 0; i < numVectors; i++) {
                    assertEquals(defaultScorer.quantizeScore(query), panamaScorer.quantizeScore(query));
                    assertEquals(in.getFilePointer(), slice.getFilePointer());
                }
                assertEquals((long) length * numVectors, slice.getFilePointer());
            }
        }
    }

    public void testScore() throws Exception {
        final int maxDims = random().nextInt(1, 1000) * 2;
        final int dimensions = random().nextInt(1, maxDims);
        final int length = BQVectorUtils.discretize(dimensions, 64) / 8;
        final int numVectors = random().nextInt(10, 50);
        float[][] vectors = new float[numVectors][dimensions];
        final int[] scratch = new int[dimensions];
        final float[] residualScratch = new float[dimensions];
        final byte[] qVector = new byte[length];
        final float[] centroid = new float[dimensions];
        VectorSimilarityFunction similarityFunction = randomFrom(VectorSimilarityFunction.values());
        randomVector(centroid, similarityFunction);
        OptimizedScalarQuantizer quantizer = new OptimizedScalarQuantizer(similarityFunction);
        int padding = random().nextInt(100);
        byte[] paddingBytes = new byte[padding];
        try (Directory dir = newRandomDirectory()) {
            try (IndexOutput out = dir.createOutput("testScore.bin", IOContext.DEFAULT)) {
                random().nextBytes(paddingBytes);
                out.writeBytes(paddingBytes, 0, padding);
                for (float[] vector : vectors) {
                    randomVector(vector, similarityFunction);
                    OptimizedScalarQuantizer.QuantizationResult result = quantizer.scalarQuantize(
                        vector,
                        residualScratch,
                        scratch,
                        (byte) 1,
                        centroid
                    );
                    ESVectorUtil.packAsBinary(scratch, qVector);
                    out.writeBytes(qVector, 0, qVector.length);
                    out.writeInt(Float.floatToIntBits(result.lowerInterval()));
                    out.writeInt(Float.floatToIntBits(result.upperInterval()));
                    out.writeInt(Float.floatToIntBits(result.additionalCorrection()));
                    out.writeShort((short) result.quantizedComponentSum());
                }
            }
            final float[] query = new float[dimensions];
            randomVector(query, similarityFunction);
            OptimizedScalarQuantizer.QuantizationResult queryCorrections = quantizer.scalarQuantize(
                query,
                residualScratch,
                scratch,
                (byte) 4,
                centroid
            );
            final byte[] quantizeQuery = new byte[4 * length];
            ESVectorUtil.transposeHalfByte(scratch, quantizeQuery);
            final float centroidDp = VectorUtil.dotProduct(centroid, centroid);
            final float[] floatScratch = new float[3];
            try (IndexInput in = dir.openInput("testScore.bin", IOContext.DEFAULT)) {
                in.seek(padding);
                assertEquals(in.length(), padding + (long) numVectors * (length + 14));
                final IndexInput slice = in.slice("test", in.getFilePointer(), (long) (length + 14) * numVectors);
                // Work on a slice that has just the right number of bytes to make the test fail with an
                // index-out-of-bounds in case the implementation reads more than the allowed number of
                // padding bytes.
                for (int i = 0; i < numVectors; i++) {
                    final ES91OSQVectorsScorer defaultScorer = defaultProvider().newES91OSQVectorsScorer(slice, dimensions);
                    final ES91OSQVectorsScorer panamaScorer = maybePanamaProvider().newES91OSQVectorsScorer(in, dimensions);
                    long qDist = defaultScorer.quantizeScore(quantizeQuery);
                    slice.readFloats(floatScratch, 0, 3);
                    int quantizedComponentSum = slice.readShort();
                    float defaulScore = defaultScorer.score(
                        queryCorrections.lowerInterval(),
                        queryCorrections.upperInterval(),
                        queryCorrections.quantizedComponentSum(),
                        queryCorrections.additionalCorrection(),
                        similarityFunction,
                        centroidDp,
                        floatScratch[0],
                        floatScratch[1],
                        quantizedComponentSum,
                        floatScratch[2],
                        qDist
                    );
                    qDist = panamaScorer.quantizeScore(quantizeQuery);
                    in.readFloats(floatScratch, 0, 3);
                    quantizedComponentSum = in.readShort();
                    float panamaScore = panamaScorer.score(
                        queryCorrections.lowerInterval(),
                        queryCorrections.upperInterval(),
                        queryCorrections.quantizedComponentSum(),
                        queryCorrections.additionalCorrection(),
                        similarityFunction,
                        centroidDp,
                        floatScratch[0],
                        floatScratch[1],
                        quantizedComponentSum,
                        floatScratch[2],
                        qDist
                    );
                    assertEquals(defaulScore, panamaScore, 1e-2f);
                    assertEquals(((long) (i + 1) * (length + 14)), slice.getFilePointer());
                    assertEquals(padding + ((long) (i + 1) * (length + 14)), in.getFilePointer());
                }
            }
        }
    }

    public void testScoreBulk() throws Exception {
        final int maxDims = random().nextInt(1, 1000) * 2;
        final int dimensions = random().nextInt(1, maxDims);
        final int length = BQVectorUtils.discretize(dimensions, 64) / 8;
        final int numVectors = ES91OSQVectorsScorer.BULK_SIZE * random().nextInt(1, 10);
        float[][] vectors = new float[numVectors][dimensions];
        final int[] scratch = new int[dimensions];
        final float[] residualScratch = new float[dimensions];
        final byte[] qVector = new byte[length];
        final float[] centroid = new float[dimensions];
        VectorSimilarityFunction similarityFunction = randomFrom(VectorSimilarityFunction.values());
        randomVector(centroid, similarityFunction);
        OptimizedScalarQuantizer quantizer = new OptimizedScalarQuantizer(similarityFunction);
        int padding = random().nextInt(100);
        byte[] paddingBytes = new byte[padding];
        try (Directory dir = newRandomDirectory()) {
            try (IndexOutput out = dir.createOutput("testScore.bin", IOContext.DEFAULT)) {
                random().nextBytes(paddingBytes);
                out.writeBytes(paddingBytes, 0, padding);
                int limit = numVectors - ES91OSQVectorsScorer.BULK_SIZE + 1;
                OptimizedScalarQuantizer.QuantizationResult[] results =
                    new OptimizedScalarQuantizer.QuantizationResult[ES91Int4VectorsScorer.BULK_SIZE];
                for (int i = 0; i < limit; i += ES91OSQVectorsScorer.BULK_SIZE) {
                    for (int j = 0; j < ES91Int4VectorsScorer.BULK_SIZE; j++) {
                        randomVector(vectors[i + j], similarityFunction);
                        results[j] = quantizer.scalarQuantize(vectors[i + j], residualScratch, scratch, (byte) 1, centroid);
                        ESVectorUtil.packAsBinary(scratch, qVector);
                        out.writeBytes(qVector, 0, qVector.length);
                    }
                    writeCorrections(results, out);
                }
            }
            final float[] query = new float[dimensions];
            randomVector(query, similarityFunction);
            OptimizedScalarQuantizer.QuantizationResult queryCorrections = quantizer.scalarQuantize(
                query,
                residualScratch,
                scratch,
                (byte) 4,
                centroid
            );
            final byte[] quantizeQuery = new byte[4 * length];
            ESVectorUtil.transposeHalfByte(scratch, quantizeQuery);
            final float centroidDp = VectorUtil.dotProduct(centroid, centroid);
            final float[] scoresDefault = new float[ES91OSQVectorsScorer.BULK_SIZE];
            final float[] scoresPanama = new float[ES91OSQVectorsScorer.BULK_SIZE];
            try (IndexInput in = dir.openInput("testScore.bin", IOContext.DEFAULT)) {
                in.seek(padding);
                assertEquals(in.length(), padding + (long) numVectors * (length + 14));
                // Work on a slice that has just the right number of bytes to make the test fail with an
                // index-out-of-bounds in case the implementation reads more than the allowed number of
                // padding bytes.
                for (int i = 0; i < numVectors; i += ES91OSQVectorsScorer.BULK_SIZE) {
                    final IndexInput slice = in.slice("test", in.getFilePointer(), (long) (length + 14) * ES91OSQVectorsScorer.BULK_SIZE);
                    final ES91OSQVectorsScorer defaultScorer = defaultProvider().newES91OSQVectorsScorer(slice, dimensions);
                    final ES91OSQVectorsScorer panamaScorer = maybePanamaProvider().newES91OSQVectorsScorer(in, dimensions);
                    float defaultMaxScore = defaultScorer.scoreBulk(
                        quantizeQuery,
                        queryCorrections.lowerInterval(),
                        queryCorrections.upperInterval(),
                        queryCorrections.quantizedComponentSum(),
                        queryCorrections.additionalCorrection(),
                        similarityFunction,
                        centroidDp,
                        scoresDefault
                    );
                    float panamaMaxScore = panamaScorer.scoreBulk(
                        quantizeQuery,
                        queryCorrections.lowerInterval(),
                        queryCorrections.upperInterval(),
                        queryCorrections.quantizedComponentSum(),
                        queryCorrections.additionalCorrection(),
                        similarityFunction,
                        centroidDp,
                        scoresPanama
                    );
                    assertEquals(defaultMaxScore, panamaMaxScore, 1e-2f);
                    for (int j = 0; j < ES91OSQVectorsScorer.BULK_SIZE; j++) {
                        assertEquals(scoresDefault[j], scoresPanama[j], 1e-2f);
                    }
                    assertEquals(((long) (ES91OSQVectorsScorer.BULK_SIZE) * (length + 14)), slice.getFilePointer());
                    assertEquals(padding + ((long) (i + ES91OSQVectorsScorer.BULK_SIZE) * (length + 14)), in.getFilePointer());
                }
            }
        }
    }

    private static void writeCorrections(OptimizedScalarQuantizer.QuantizationResult[] corrections, IndexOutput out) throws IOException {
        for (OptimizedScalarQuantizer.QuantizationResult correction : corrections) {
            out.writeInt(Float.floatToIntBits(correction.lowerInterval()));
        }
        for (OptimizedScalarQuantizer.QuantizationResult correction : corrections) {
            out.writeInt(Float.floatToIntBits(correction.upperInterval()));
        }
        for (OptimizedScalarQuantizer.QuantizationResult correction : corrections) {
            int targetComponentSum = correction.quantizedComponentSum();
            out.writeShort((short) targetComponentSum);
        }
        for (OptimizedScalarQuantizer.QuantizationResult correction : corrections) {
            out.writeInt(Float.floatToIntBits(correction.additionalCorrection()));
        }
    }

    private void randomVector(float[] vector, VectorSimilarityFunction vectorSimilarityFunction) {
        for (int i = 0; i < vector.length; i++) {
            vector[i] = random().nextFloat();
        }
        if (vectorSimilarityFunction != VectorSimilarityFunction.EUCLIDEAN) {
            VectorUtil.l2normalize(vector);
        }
    }

    private Directory newRandomDirectory() throws IOException {
        return randomBoolean() ? new MMapDirectory(createTempDir()) : new NIOFSDirectory(createTempDir());
    }
}
