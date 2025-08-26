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
import org.apache.lucene.util.VectorUtil;
import org.elasticsearch.index.codec.vectors.OptimizedScalarQuantizer;
import org.elasticsearch.simdvec.ES91Int4VectorsScorer;
import org.elasticsearch.simdvec.ES91OSQVectorsScorer;

import java.io.IOException;

import static org.hamcrest.Matchers.greaterThan;

public class ES91Int4VectorScorerTests extends BaseVectorizationTests {

    public void testInt4DotProduct() throws Exception {
        // only even dimensions are supported
        final int dimensions = random().nextInt(1, 1000) * 2;
        final int numVectors = random().nextInt(1, 100);
        final byte[] vector = new byte[dimensions];
        try (Directory dir = new MMapDirectory(createTempDir())) {
            try (IndexOutput out = dir.createOutput("tests.bin", IOContext.DEFAULT)) {
                for (int i = 0; i < numVectors; i++) {
                    for (int j = 0; j < dimensions; j++) {
                        vector[j] = (byte) random().nextInt(16); // 4-bit quantization
                    }
                    out.writeBytes(vector, 0, dimensions);
                }
            }
            final byte[] query = new byte[dimensions];
            for (int j = 0; j < dimensions; j++) {
                query[j] = (byte) random().nextInt(16); // 4-bit quantization
            }
            try (IndexInput in = dir.openInput("tests.bin", IOContext.DEFAULT)) {
                // Work on a slice that has just the right number of bytes to make the test fail with an
                // index-out-of-bounds in case the implementation reads more than the allowed number of
                // padding bytes.
                final IndexInput slice = in.slice("test", 0, (long) dimensions * numVectors);
                final IndexInput slice2 = in.slice("test2", 0, (long) dimensions * numVectors);
                final ES91Int4VectorsScorer defaultScorer = defaultProvider().newES91Int4VectorsScorer(slice, dimensions);
                final ES91Int4VectorsScorer panamaScorer = maybePanamaProvider().newES91Int4VectorsScorer(slice2, dimensions);
                for (int i = 0; i < numVectors; i++) {
                    in.readBytes(vector, 0, dimensions);
                    long val = VectorUtil.int4DotProduct(vector, query);
                    assertEquals(val, defaultScorer.int4DotProduct(query));
                    assertEquals(val, panamaScorer.int4DotProduct(query));
                    assertEquals(in.getFilePointer(), slice.getFilePointer());
                    assertEquals(in.getFilePointer(), slice2.getFilePointer());
                }
                assertEquals((long) dimensions * numVectors, in.getFilePointer());
            }
        }
    }

    public void testInt4Score() throws Exception {
        // only even dimensions are supported
        final int dimensions = random().nextInt(1, 1000) * 2;
        final int numVectors = random().nextInt(1, 100);
        final byte[] vector = new byte[dimensions];
        final byte[] corrections = new byte[14];
        try (Directory dir = new MMapDirectory(createTempDir())) {
            try (IndexOutput out = dir.createOutput("tests.bin", IOContext.DEFAULT)) {
                for (int i = 0; i < numVectors; i++) {
                    for (int j = 0; j < dimensions; j++) {
                        vector[j] = (byte) random().nextInt(16); // 4-bit quantization
                    }
                    out.writeBytes(vector, 0, dimensions);
                    random().nextBytes(corrections);
                    out.writeBytes(corrections, 0, corrections.length);
                }
            }
            final byte[] query = new byte[dimensions];
            for (int j = 0; j < dimensions; j++) {
                query[j] = (byte) random().nextInt(16); // 4-bit quantization
            }
            OptimizedScalarQuantizer.QuantizationResult queryCorrections = new OptimizedScalarQuantizer.QuantizationResult(
                random().nextFloat(),
                random().nextFloat(),
                random().nextFloat(),
                Short.toUnsignedInt((short) random().nextInt())
            );
            float centroidDp = random().nextFloat();
            VectorSimilarityFunction similarityFunction = randomFrom(VectorSimilarityFunction.values());
            try (IndexInput in = dir.openInput("tests.bin", IOContext.DEFAULT)) {
                // Work on a slice that has just the right number of bytes to make the test fail with an
                // index-out-of-bounds in case the implementation reads more than the allowed number of
                // padding bytes.
                final IndexInput slice = in.slice("test", 0, (long) (dimensions + 14) * numVectors);
                final ES91Int4VectorsScorer defaultScorer = defaultProvider().newES91Int4VectorsScorer(in, dimensions);
                final ES91Int4VectorsScorer panamaScorer = maybePanamaProvider().newES91Int4VectorsScorer(slice, dimensions);
                for (int i = 0; i < numVectors; i++) {
                    float scoreDefault = defaultScorer.score(
                        query,
                        queryCorrections.lowerInterval(),
                        queryCorrections.upperInterval(),
                        queryCorrections.quantizedComponentSum(),
                        queryCorrections.additionalCorrection(),
                        similarityFunction,
                        centroidDp
                    );
                    float scorePanama = panamaScorer.score(
                        query,
                        queryCorrections.lowerInterval(),
                        queryCorrections.upperInterval(),
                        queryCorrections.quantizedComponentSum(),
                        queryCorrections.additionalCorrection(),
                        similarityFunction,
                        centroidDp
                    );
                    assertEquals(scoreDefault, scorePanama, 0.001f);
                    assertEquals(in.getFilePointer(), slice.getFilePointer());
                }
                assertEquals((long) (dimensions + 14) * numVectors, in.getFilePointer());
            }
        }
    }

    public void testInt4ScoreBulk() throws Exception {
        // only even dimensions are supported
        final int dimensions = random().nextInt(1, 1000) * 2;
        final int numVectors = random().nextInt(1, 10) * ES91Int4VectorsScorer.BULK_SIZE;
        final float[][] vectors = new float[numVectors][dimensions];
        final int[] quantizedScratch = new int[dimensions];
        final byte[] quantizeVector = new byte[dimensions];
        final float[] centroid = new float[dimensions];
        VectorSimilarityFunction similarityFunction = randomFrom(VectorSimilarityFunction.values());
        for (int i = 0; i < dimensions; i++) {
            centroid[i] = random().nextFloat();
        }
        if (similarityFunction != VectorSimilarityFunction.EUCLIDEAN) {
            VectorUtil.l2normalize(centroid);
        }

        OptimizedScalarQuantizer quantizer = new OptimizedScalarQuantizer(similarityFunction);
        try (Directory dir = new MMapDirectory(createTempDir())) {
            try (IndexOutput out = dir.createOutput("tests.bin", IOContext.DEFAULT)) {
                OptimizedScalarQuantizer.QuantizationResult[] results =
                    new OptimizedScalarQuantizer.QuantizationResult[ES91Int4VectorsScorer.BULK_SIZE];
                for (int i = 0; i < numVectors; i += ES91Int4VectorsScorer.BULK_SIZE) {
                    for (int j = 0; j < ES91Int4VectorsScorer.BULK_SIZE; j++) {
                        for (int k = 0; k < dimensions; k++) {
                            vectors[i + j][k] = random().nextFloat();
                        }
                        if (similarityFunction != VectorSimilarityFunction.EUCLIDEAN) {
                            VectorUtil.l2normalize(vectors[i + j]);
                        }
                        results[j] = quantizer.scalarQuantize(vectors[i + j].clone(), quantizedScratch, (byte) 4, centroid);
                        for (int k = 0; k < dimensions; k++) {
                            quantizeVector[k] = (byte) quantizedScratch[k];
                        }
                        out.writeBytes(quantizeVector, 0, dimensions);
                    }
                    writeCorrections(results, out);
                }
            }
            final float[] query = new float[dimensions];
            final byte[] quantizeQuery = new byte[dimensions];
            for (int j = 0; j < dimensions; j++) {
                query[j] = random().nextFloat();
            }
            if (similarityFunction != VectorSimilarityFunction.EUCLIDEAN) {
                VectorUtil.l2normalize(query);
            }
            OptimizedScalarQuantizer.QuantizationResult queryCorrections = quantizer.scalarQuantize(
                query.clone(),
                quantizedScratch,
                (byte) 4,
                centroid
            );
            for (int j = 0; j < dimensions; j++) {
                quantizeQuery[j] = (byte) quantizedScratch[j];
            }
            float centroidDp = VectorUtil.dotProduct(centroid, centroid);

            try (IndexInput in = dir.openInput("tests.bin", IOContext.DEFAULT)) {
                // Work on a slice that has just the right number of bytes to make the test fail with an
                // index-out-of-bounds in case the implementation reads more than the allowed number of
                // padding bytes.
                final IndexInput slice = in.slice("test", 0, (long) (dimensions + 14) * numVectors);
                final ES91Int4VectorsScorer defaultScorer = defaultProvider().newES91Int4VectorsScorer(in, dimensions);
                final ES91Int4VectorsScorer panamaScorer = maybePanamaProvider().newES91Int4VectorsScorer(slice, dimensions);
                float[] scoresDefault = new float[ES91Int4VectorsScorer.BULK_SIZE];
                float[] scoresPanama = new float[ES91Int4VectorsScorer.BULK_SIZE];
                for (int i = 0; i < numVectors; i += ES91Int4VectorsScorer.BULK_SIZE) {
                    defaultScorer.scoreBulk(
                        quantizeQuery,
                        queryCorrections.lowerInterval(),
                        queryCorrections.upperInterval(),
                        queryCorrections.quantizedComponentSum(),
                        queryCorrections.additionalCorrection(),
                        similarityFunction,
                        centroidDp,
                        scoresDefault
                    );
                    panamaScorer.scoreBulk(
                        quantizeQuery,
                        queryCorrections.lowerInterval(),
                        queryCorrections.upperInterval(),
                        queryCorrections.quantizedComponentSum(),
                        queryCorrections.additionalCorrection(),
                        similarityFunction,
                        centroidDp,
                        scoresPanama
                    );
                    for (int j = 0; j < ES91OSQVectorsScorer.BULK_SIZE; j++) {
                        assertEquals(scoresDefault[j], scoresPanama[j], 1e-2f);
                        float realSimilarity = similarityFunction.compare(vectors[i + j], query);
                        float accuracy = realSimilarity > scoresDefault[j]
                            ? scoresDefault[j] / realSimilarity
                            : realSimilarity / scoresDefault[j];
                        assertThat(accuracy, greaterThan(0.90f));
                    }
                    assertEquals(in.getFilePointer(), slice.getFilePointer());
                }
                assertEquals((long) (dimensions + 14) * numVectors, in.getFilePointer());
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
            assert targetComponentSum >= 0 && targetComponentSum <= 0xffff;
            out.writeShort((short) targetComponentSum);
        }
        for (OptimizedScalarQuantizer.QuantizationResult correction : corrections) {
            out.writeInt(Float.floatToIntBits(correction.additionalCorrection()));
        }
    }
}
