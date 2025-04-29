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
import org.apache.lucene.util.quantization.OptimizedScalarQuantizer;

import static org.hamcrest.Matchers.lessThan;

public class ES91OSQVectorScorerTests extends BaseVectorizationTests {

    public void testQuantizeScore() throws Exception {
        final int dimensions = random().nextInt(1, 2000);
        final int length = OptimizedScalarQuantizer.discretize(dimensions, 64) / 8;
        final int numVectors = random().nextInt(1, 100);
        final byte[] vector = new byte[length];
        try (Directory dir = new MMapDirectory(createTempDir())) {
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
        final int maxDims = 512;
        final int dimensions = random().nextInt(1, maxDims);
        final int length = OptimizedScalarQuantizer.discretize(dimensions, 64) / 8;
        final int numVectors = ES91OSQVectorsScorer.BULK_SIZE * random().nextInt(1, 10);
        final byte[] vector = new byte[length];
        int padding = random().nextInt(100);
        byte[] paddingBytes = new byte[padding];
        try (Directory dir = new MMapDirectory(createTempDir())) {
            try (IndexOutput out = dir.createOutput("testScore.bin", IOContext.DEFAULT)) {
                random().nextBytes(paddingBytes);
                out.writeBytes(paddingBytes, 0, padding);
                for (int i = 0; i < numVectors; i++) {
                    random().nextBytes(vector);
                    out.writeBytes(vector, 0, length);
                    float lower = random().nextFloat();
                    float upper = random().nextFloat() + lower / 2;
                    float additionalCorrection = random().nextFloat();
                    int targetComponentSum = randomIntBetween(0, dimensions / 2);
                    out.writeInt(Float.floatToIntBits(lower));
                    out.writeInt(Float.floatToIntBits(upper));
                    out.writeShort((short) targetComponentSum);
                    out.writeInt(Float.floatToIntBits(additionalCorrection));
                }
            }
            final byte[] query = new byte[4 * length];
            random().nextBytes(query);
            float lower = random().nextFloat();
            OptimizedScalarQuantizer.QuantizationResult result = new OptimizedScalarQuantizer.QuantizationResult(
                lower,
                random().nextFloat() + lower / 2,
                random().nextFloat(),
                randomIntBetween(0, dimensions * 2)
            );
            final float centroidDp = random().nextFloat();
            final float[] scores1 = new float[ES91OSQVectorsScorer.BULK_SIZE];
            final float[] scores2 = new float[ES91OSQVectorsScorer.BULK_SIZE];
            for (VectorSimilarityFunction similarityFunction : VectorSimilarityFunction.values()) {
                try (IndexInput in = dir.openInput("testScore.bin", IOContext.DEFAULT)) {
                    in.seek(padding);
                    assertEquals(in.length(), padding + (long) numVectors * (length + 14));
                    // Work on a slice that has just the right number of bytes to make the test fail with an
                    // index-out-of-bounds in case the implementation reads more than the allowed number of
                    // padding bytes.
                    for (int i = 0; i < numVectors; i += ES91OSQVectorsScorer.BULK_SIZE) {
                        final IndexInput slice = in.slice(
                            "test",
                            in.getFilePointer(),
                            (long) (length + 14) * ES91OSQVectorsScorer.BULK_SIZE
                        );
                        final ES91OSQVectorsScorer defaultScorer = defaultProvider().newES91OSQVectorsScorer(slice, dimensions);
                        final ES91OSQVectorsScorer panamaScorer = maybePanamaProvider().newES91OSQVectorsScorer(in, dimensions);
                        defaultScorer.scoreBulk(query, result, similarityFunction, centroidDp, scores1);
                        panamaScorer.scoreBulk(query, result, similarityFunction, centroidDp, scores2);
                        for (int j = 0; j < ES91OSQVectorsScorer.BULK_SIZE; j++) {
                            if (scores1[j] > (maxDims * Short.MAX_VALUE)) {
                                int diff = (int) (scores1[j] - scores2[j]);
                                assertThat("defaultScores: " + scores1[j] + " bulkScores: " + scores2[j], Math.abs(diff), lessThan(65));
                            } else if (scores1[j] > (maxDims * Byte.MAX_VALUE)) {
                                int diff = (int) (scores1[j] - scores2[j]);
                                assertThat("defaultScores: " + scores1[j] + " bulkScores: " + scores2[j], Math.abs(diff), lessThan(9));
                            } else {
                                assertEquals(scores1[j], scores2[j], 1e-2f);
                            }
                        }
                        assertEquals(((long) (ES91OSQVectorsScorer.BULK_SIZE) * (length + 14)), slice.getFilePointer());
                        assertEquals(padding + ((long) (i + ES91OSQVectorsScorer.BULK_SIZE) * (length + 14)), in.getFilePointer());
                    }
                }
            }
        }
    }
}
