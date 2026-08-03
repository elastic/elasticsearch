/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.simdvec.internal.vectorization;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.stream.IntStream;

import static org.elasticsearch.simdvec.internal.vectorization.VectorScorerTestUtils.createBinarizedIndexData;
import static org.elasticsearch.simdvec.internal.vectorization.VectorScorerTestUtils.createBinarizedQueryData;
import static org.elasticsearch.simdvec.internal.vectorization.VectorScorerTestUtils.writeBinarizedVectorData;

public class ES93BinaryQuantizedVectorScorerTests extends BaseVectorizationTests {

    public enum DirectoryType {
        NIOFS,
        MMAP
    }

    private final DirectoryType directoryType;

    private final VectorSimilarityFunction similarityFunction;

    public ES93BinaryQuantizedVectorScorerTests(DirectoryType directoryType, VectorSimilarityFunction similarityFunction) {
        this.directoryType = directoryType;
        this.similarityFunction = similarityFunction;
    }

    private Directory newParametrizedDirectory() throws IOException {
        return switch (directoryType) {
            case NIOFS -> new NIOFSDirectory(createTempDir());
            case MMAP -> new MMapDirectory(createTempDir());
        };
    }

    private void createTestFile(
        Directory dir,
        int numVectors,
        float[] vectorValues,
        float[] centroid,
        OptimizedScalarQuantizer quantizer,
        int dims
    ) throws IOException {
        try (IndexOutput out = dir.createOutput("testScore.bin", IOContext.DEFAULT)) {
            for (int i = 0; i < numVectors; i++) {
                VectorScorerTestUtils.randomVector(random(), vectorValues, similarityFunction);
                var indexData = createBinarizedIndexData(vectorValues, centroid, quantizer, dims);
                writeBinarizedVectorData(out, indexData);
            }
        }
    }

    public void testScore() throws IOException {
        final int dims = random().nextInt(1, 2000);
        final int numVectors = random().nextInt(10, 50);

        final float[] centroid = new float[dims];
        VectorScorerTestUtils.randomVector(random(), centroid, similarityFunction);
        final float centroidDp = VectorUtil.dotProduct(centroid, centroid);

        float[] vectorValues = new float[dims];
        OptimizedScalarQuantizer quantizer = new OptimizedScalarQuantizer(similarityFunction);

        try (Directory dir = newParametrizedDirectory()) {
            createTestFile(dir, numVectors, vectorValues, centroid, quantizer, dims);

            VectorScorerTestUtils.randomVector(random(), vectorValues, similarityFunction);
            var queryData = createBinarizedQueryData(vectorValues, centroid, quantizer, dims);

            try (IndexInput in = dir.openInput("testScore.bin", IOContext.DEFAULT)) {
                final int vectorLengthInBytes = BQVectorUtils.discretize(dims, 64) / 8;
                final int perVectorBytes = vectorLengthInBytes + 14;
                assertEquals(in.length(), (long) numVectors * perVectorBytes);

                final var defaultScorer = defaultProvider().newES93BinaryQuantizedVectorScorer(in, dims, vectorLengthInBytes);
                final var panamaScorer = maybePanamaProvider().newES93BinaryQuantizedVectorScorer(in, dims, vectorLengthInBytes);

                for (int i = 0; i < numVectors; i++) {
                    var defaultScore = defaultScorer.score(
                        queryData.vector(),
                        queryData.lowerInterval(),
                        queryData.upperInterval(),
                        queryData.quantizedComponentSum(),
                        queryData.additionalCorrection(),
                        similarityFunction,
                        centroidDp,
                        i
                    );
                    var panamaScore = panamaScorer.score(
                        queryData.vector(),
                        queryData.lowerInterval(),
                        queryData.upperInterval(),
                        queryData.quantizedComponentSum(),
                        queryData.additionalCorrection(),
                        similarityFunction,
                        centroidDp,
                        i
                    );

                    assertEquals(defaultScore, panamaScore, 1e-2f);
                }
            }
        }
    }

    public void testBulkScore() throws IOException {
        final int dims = random().nextInt(1, 2000);
        final int numVectors = random().nextInt(10, 50);

        final float[] centroid = new float[dims];
        VectorScorerTestUtils.randomVector(random(), centroid, similarityFunction);
        final float centroidDp = VectorUtil.dotProduct(centroid, centroid);

        float[] vectorValues = new float[dims];
        OptimizedScalarQuantizer quantizer = new OptimizedScalarQuantizer(similarityFunction);

        try (Directory dir = newParametrizedDirectory()) {
            createTestFile(dir, numVectors, vectorValues, centroid, quantizer, dims);

            VectorScorerTestUtils.randomVector(random(), vectorValues, similarityFunction);
            var queryData = createBinarizedQueryData(vectorValues, centroid, quantizer, dims);

            try (IndexInput in = dir.openInput("testScore.bin", IOContext.DEFAULT)) {
                final int vectorLengthInBytes = BQVectorUtils.discretize(dims, 64) / 8;
                final int perVectorBytes = vectorLengthInBytes + 14;
                assertEquals(in.length(), (long) numVectors * perVectorBytes);

                final var defaultScorer = defaultProvider().newES93BinaryQuantizedVectorScorer(in, dims, vectorLengthInBytes);
                final var panamaScorer = maybePanamaProvider().newES93BinaryQuantizedVectorScorer(in, dims, vectorLengthInBytes);

                final float[] scoresDefault = new float[numVectors];
                final float[] scoresPanama = new float[numVectors];
                var nodeList = new ArrayList<>(IntStream.range(0, numVectors).boxed().toList());
                Collections.shuffle(nodeList, random());
                final int[] nodes = nodeList.stream().mapToInt(Integer::intValue).toArray();

                float defaultMaxScore = defaultScorer.scoreBulk(
                    queryData.vector(),
                    queryData.lowerInterval(),
                    queryData.upperInterval(),
                    queryData.quantizedComponentSum(),
                    queryData.additionalCorrection(),
                    similarityFunction,
                    centroidDp,
                    nodes,
                    scoresDefault,
                    numVectors
                );
                float panamaMaxScore = panamaScorer.scoreBulk(
                    queryData.vector(),
                    queryData.lowerInterval(),
                    queryData.upperInterval(),
                    queryData.quantizedComponentSum(),
                    queryData.additionalCorrection(),
                    similarityFunction,
                    centroidDp,
                    nodes,
                    scoresPanama,
                    numVectors
                );

                assertEquals(defaultMaxScore, panamaMaxScore, 1e-2f);
                assertArrayEqualsPercent(scoresDefault, scoresPanama, 0.05f);
            }
        }
    }

    /**
     * Regression test: when quantization loss makes the reconstructed squared
     * euclidean distance slightly negative, the default Java scorer must still
     * produce a score in [0, 1] — matching the native scorer. The bug was that
     * applyCorrections clamped *after* normalisation (1/(1+d)) instead of
     * before, so a small negative distance d produced 1/(1+d) > 1.
     *
     * Uses hand-crafted correction values written directly into the index file
     * so the test is deterministic and platform-independent.
     *
     * With the values below (lowerInterval=0.1, upperInterval=0.1, additionalCorrection=0.5,
     * componentSum=0, all-zero quantized vectors), the scorer computes:
     *   base_score = ax*ay*dims = 0.1 * 0.1 * 64 = 0.64
     *   distance   = 0.5 + 0.5 - 2*0.64 = -0.28
     * Before the fix: normalizeDistanceToUnitInterval(-0.28) = 1/(1-0.28) ≈ 1.389, which is > 1.
     * After the fix:  normalizeDistanceToUnitInterval(max(-0.28, 0)) = 1/(1+0) = 1.0.
     */
    public void testEuclideanScoreBoundedWhenQuantizationLossProducesNegativeDistance() throws IOException {
        if (similarityFunction != VectorSimilarityFunction.EUCLIDEAN) {
            return;
        }
        final int dims = 64;
        final int vectorLengthInBytes = BQVectorUtils.discretize(dims, 64) / 8;
        final int perVectorBytes = vectorLengthInBytes + 14;  // (Float.BYTES * 3) + Short.BYTES

        // Craft index data: all-zero quantized vector bytes, then corrections
        // that will produce a negative euclidean distance.
        final float indexLowerInterval = 0.1f;
        final float indexUpperInterval = 0.1f;  // lx = upper - lower = 0
        final float indexAdditionalCorrection = 0.5f;
        final short indexComponentSum = 0;

        // Craft query data: same shape, all-zero 4-bit quantized bytes.
        final float queryLowerInterval = 0.1f;
        final float queryUpperInterval = 0.1f;  // ly = (upper - lower) * FOUR_BIT_SCALE = 0
        final float queryAdditionalCorrection = 0.5f;
        final short queryComponentSum = 0;

        byte[] indexVector = new byte[vectorLengthInBytes];  // all zeros
        int queryVectorLength = (BQVectorUtils.discretize(dims, 64) / 8) * 4;  // B_QUERY = 4
        byte[] queryVector = new byte[queryVectorLength];  // all zeros

        try (Directory dir = newParametrizedDirectory()) {
            try (IndexOutput out = dir.createOutput("testScore.bin", IOContext.DEFAULT)) {
                out.writeBytes(indexVector, indexVector.length);
                out.writeInt(Float.floatToIntBits(indexLowerInterval));
                out.writeInt(Float.floatToIntBits(indexUpperInterval));
                out.writeInt(Float.floatToIntBits(indexAdditionalCorrection));
                out.writeShort(indexComponentSum);
            }

            try (IndexInput in = dir.openInput("testScore.bin", IOContext.DEFAULT)) {
                var defaultScorer = defaultProvider().newES93BinaryQuantizedVectorScorer(in, dims, vectorLengthInBytes);
                var panamaScorer = maybePanamaProvider().newES93BinaryQuantizedVectorScorer(in, dims, vectorLengthInBytes);

                float defaultScore = defaultScorer.score(
                    queryVector,
                    queryLowerInterval,
                    queryUpperInterval,
                    Short.toUnsignedInt(queryComponentSum),
                    queryAdditionalCorrection,
                    similarityFunction,
                    0f,
                    0
                );
                float panamaScore = panamaScorer.score(
                    queryVector,
                    queryLowerInterval,
                    queryUpperInterval,
                    Short.toUnsignedInt(queryComponentSum),
                    queryAdditionalCorrection,
                    similarityFunction,
                    0f,
                    0
                );

                assertTrue("EUCLIDEAN score from default scorer must be <= 1.0, got " + defaultScore, defaultScore <= 1.0f);
                assertTrue("EUCLIDEAN score from panama scorer must be <= 1.0, got " + panamaScore, panamaScore <= 1.0f);
                assertEquals(defaultScore, panamaScore, 1e-2f);
            }
        }
    }

    @ParametersFactory
    public static Iterable<Object[]> parametersFactory() {
        return () -> Arrays.stream(DirectoryType.values())
            .flatMap(d -> Arrays.stream(VectorSimilarityFunction.values()).map(f -> new Object[] { d, f }))
            .iterator();
    }
}
