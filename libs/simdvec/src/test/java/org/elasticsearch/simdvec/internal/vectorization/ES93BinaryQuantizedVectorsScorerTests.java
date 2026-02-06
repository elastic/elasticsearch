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
import java.util.Arrays;
import java.util.stream.IntStream;

import static org.elasticsearch.simdvec.internal.vectorization.VectorScorerTestUtils.createBinarizedIndexData;
import static org.elasticsearch.simdvec.internal.vectorization.VectorScorerTestUtils.createBinarizedQueryData;
import static org.elasticsearch.simdvec.internal.vectorization.VectorScorerTestUtils.writeBinarizedVectorData;

public class ES93BinaryQuantizedVectorsScorerTests extends BaseVectorizationTests {

    public enum DirectoryType {
        NIOFS,
        MMAP
    }

    private final DirectoryType directoryType;

    private final VectorSimilarityFunction similarityFunction;

    public ES93BinaryQuantizedVectorsScorerTests(DirectoryType directoryType, VectorSimilarityFunction similarityFunction) {
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
                randomVector(vectorValues, similarityFunction);
                var indexData = createBinarizedIndexData(vectorValues, centroid, quantizer, dims);
                writeBinarizedVectorData(out, indexData);
            }
        }
    }

    public void testScore() throws IOException {
        final int dims = random().nextInt(1, 2000);
        final int numVectors = random().nextInt(10, 50);

        final float[] centroid = new float[dims];
        randomVector(centroid, similarityFunction);
        final float centroidDp = VectorUtil.dotProduct(centroid, centroid);

        float[] vectorValues = new float[dims];
        OptimizedScalarQuantizer quantizer = new OptimizedScalarQuantizer(similarityFunction);

        try (Directory dir = newParametrizedDirectory()) {
            createTestFile(dir, numVectors, vectorValues, centroid, quantizer, dims);

            randomVector(vectorValues, similarityFunction);
            var queryData = createBinarizedQueryData(vectorValues, centroid, quantizer, dims);

            try (IndexInput in = dir.openInput("testScore.bin", IOContext.DEFAULT)) {
                final int vectorLengthInBytes = BQVectorUtils.discretize(dims, 64) / 8;
                final int perVectorBytes = vectorLengthInBytes + 14;
                assertEquals(in.length(), (long) numVectors * perVectorBytes);

                final var defaultScorer = defaultProvider().newES93BinaryQuantizedVectorsScorer(in, dims, vectorLengthInBytes);
                final var panamaScorer = maybePanamaProvider().newES93BinaryQuantizedVectorsScorer(in, dims, vectorLengthInBytes);

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
        randomVector(centroid, similarityFunction);
        final float centroidDp = VectorUtil.dotProduct(centroid, centroid);

        float[] vectorValues = new float[dims];
        OptimizedScalarQuantizer quantizer = new OptimizedScalarQuantizer(similarityFunction);

        try (Directory dir = newParametrizedDirectory()) {
            createTestFile(dir, numVectors, vectorValues, centroid, quantizer, dims);

            randomVector(vectorValues, similarityFunction);
            var queryData = createBinarizedQueryData(vectorValues, centroid, quantizer, dims);

            try (IndexInput in = dir.openInput("testScore.bin", IOContext.DEFAULT)) {
                final int vectorLengthInBytes = BQVectorUtils.discretize(dims, 64) / 8;
                final int perVectorBytes = vectorLengthInBytes + 14;
                assertEquals(in.length(), (long) numVectors * perVectorBytes);

                final var defaultScorer = defaultProvider().newES93BinaryQuantizedVectorsScorer(in, dims, vectorLengthInBytes);
                final var panamaScorer = maybePanamaProvider().newES93BinaryQuantizedVectorsScorer(in, dims, vectorLengthInBytes);

                final float[] scoresDefault = new float[numVectors];
                final float[] scoresPanama = new float[numVectors];
                final int[] nodes = IntStream.range(0, numVectors).map(x -> randomInt(numVectors - 1)).toArray();

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

    @ParametersFactory
    public static Iterable<Object[]> parametersFactory() {
        return () -> Arrays.stream(DirectoryType.values())
            .flatMap(d -> Arrays.stream(VectorSimilarityFunction.values()).map(f -> new Object[] { d, f }))
            .iterator();
    }
}
