/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.simdvec.internal;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.apache.lucene.codecs.hnsw.FlatVectorsScorer;
import org.apache.lucene.index.ByteVectorValues;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.KnnVectorValues;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.elasticsearch.simdvec.BaseVectorizationTests;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.IntStream;

public class ESDefaultFlatVectorScorerTests extends BaseVectorizationTests {

    private static final float DELTA = 1e-3f;
    private static final float DELTA_PERCENT = 0.05f;

    // we're just testing arrays here, don't need to go into directorys/inputs/etc

    private static final FlatVectorsScorer defaultScorer = defaultProvider().getVectorScorerFactory().newFlatVectorsScorer();
    private static final FlatVectorsScorer nativeScorer = nativeProvider().getVectorScorerFactory().newFlatVectorsScorer();

    private final VectorSimilarityFunction function;

    public ESDefaultFlatVectorScorerTests(VectorSimilarityFunction function) {
        this.function = function;
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        return Arrays.stream(VectorSimilarityFunction.values()).map(f -> new Object[] { f })::iterator;
    }

    public void testFloatScorerSupplier() throws IOException {
        testScorerSupplier(randomFloatVectors());
    }

    public void testByteScorerSupplier() throws IOException {
        testScorerSupplier(randomByteVectors());
    }

    private void testScorerSupplier(KnnVectorValues values) throws IOException {
        var def = defaultScorer.getRandomVectorScorerSupplier(function, values.copy()).scorer();
        var nat = nativeScorer.getRandomVectorScorerSupplier(function, values.copy()).scorer();

        int numToScore = randomInt(values.size());
        // single scores
        float[] singleScores = new float[numToScore];
        int scoringOrd = randomInt(values.size() - 1);
        def.setScoringOrdinal(scoringOrd);
        nat.setScoringOrdinal(scoringOrd);

        int[] ords = new int[numToScore];
        for (int s = 0; s < numToScore; s++) {
            ords[s] = randomInt(values.size() - 1);

            singleScores[s] = def.score(ords[s]);
            float natScore = nat.score(ords[s]);
            assertEquals(singleScores[s], natScore, DELTA);
        }

        // bulk scores
        float[] defBulkScores = new float[numToScore];
        def.bulkScore(ords, defBulkScores, numToScore);
        assertArrayEqualsPercent(singleScores, defBulkScores, DELTA_PERCENT);

        float[] natBulkScores = new float[numToScore];
        nat.bulkScore(ords, natBulkScores, numToScore);
        assertArrayEqualsPercent(singleScores, natBulkScores, DELTA_PERCENT);
    }

    public void testFloatScorer() throws IOException {
        FloatVectorValues values = randomFloatVectors();
        float[] queryVector = generateRandomFloatVector(values.dimension());

        var def = defaultScorer.getRandomVectorScorer(function, values.copy(), queryVector);
        var nat = nativeScorer.getRandomVectorScorer(function, values.copy(), queryVector);

        int numToScore = randomInt(values.size());
        // single scores
        float[] singleScores = new float[numToScore];

        int[] ords = new int[numToScore];
        for (int s = 0; s < numToScore; s++) {
            ords[s] = randomInt(values.size() - 1);

            singleScores[s] = def.score(ords[s]);
            float natScore = nat.score(ords[s]);
            assertEquals(singleScores[s], natScore, DELTA);
        }

        // bulk scores
        float[] defBulkScores = new float[numToScore];
        def.bulkScore(ords, defBulkScores, numToScore);
        assertArrayEqualsPercent(singleScores, defBulkScores, DELTA_PERCENT);

        float[] natBulkScores = new float[numToScore];
        nat.bulkScore(ords, natBulkScores, numToScore);
        assertArrayEqualsPercent(singleScores, natBulkScores, DELTA_PERCENT);
    }

    public void testByteScorer() throws IOException {
        ByteVectorValues values = randomByteVectors();
        byte[] queryVector = randomByteArrayOfLength(values.dimension());

        var def = defaultScorer.getRandomVectorScorer(function, values.copy(), queryVector);
        var nat = nativeScorer.getRandomVectorScorer(function, values.copy(), queryVector);

        int numToScore = randomInt(values.size());
        // single scores
        float[] singleScores = new float[numToScore];

        int[] ords = new int[numToScore];
        for (int s = 0; s < numToScore; s++) {
            ords[s] = randomInt(values.size() - 1);

            singleScores[s] = def.score(ords[s]);
            float natScore = nat.score(ords[s]);
            assertEquals(singleScores[s], natScore, DELTA);
        }

        // bulk scores
        float[] defBulkScores = new float[numToScore];
        def.bulkScore(ords, defBulkScores, numToScore);
        assertArrayEqualsPercent(singleScores, defBulkScores, DELTA_PERCENT);

        float[] natBulkScores = new float[numToScore];
        nat.bulkScore(ords, natBulkScores, numToScore);
        assertArrayEqualsPercent(singleScores, natBulkScores, DELTA_PERCENT);
    }

    private static FloatVectorValues randomFloatVectors() {
        int num = randomIntBetween(2, 50);
        int dims = randomIntBetween(1, 1025);

        List<float[]> floats = IntStream.rangeClosed(0, num).mapToObj(n -> generateRandomFloatVector(dims)).toList();
        if (rarely()) {
            // return a FloatVectorValues that does require copying
            class CopyingFloatVectorValues extends FloatVectorValues {
                private final float[] scratch = new float[dims];

                @Override
                public float[] vectorValue(int ord) throws IOException {
                    System.arraycopy(floats.get(ord), 0, scratch, 0, dims);
                    return scratch;
                }

                @Override
                public FloatVectorValues copy() throws IOException {
                    return new CopyingFloatVectorValues();
                }

                @Override
                public int dimension() {
                    return dims;
                }

                @Override
                public int size() {
                    return num;
                }
            }
            return new CopyingFloatVectorValues();
        } else {
            return FloatVectorValues.fromFloats(floats, dims);
        }
    }

    private static float[] generateRandomFloatVector(int dims) {
        float[] vector = new float[dims];
        for (int i = 0; i < dims; i++) {
            vector[i] = randomFloat() * 2f - 1f;
        }
        return vector;
    }

    private static ByteVectorValues randomByteVectors() {
        int num = randomIntBetween(2, 50);
        int dims = randomIntBetween(1, 1025);

        List<byte[]> bytes = IntStream.rangeClosed(0, num).mapToObj(n -> randomByteArrayOfLength(dims)).toList();
        if (rarely()) {
            // return a ByteVectorValues that does require copying
            class CopyingByteVectorValues extends ByteVectorValues {
                private final byte[] scratch = new byte[dims];

                @Override
                public byte[] vectorValue(int ord) throws IOException {
                    System.arraycopy(bytes.get(ord), 0, scratch, 0, dims);
                    return scratch;
                }

                @Override
                public ByteVectorValues copy() throws IOException {
                    return new CopyingByteVectorValues();
                }

                @Override
                public int dimension() {
                    return dims;
                }

                @Override
                public int size() {
                    return num;
                }
            }
            return new CopyingByteVectorValues();
        } else {
            return ByteVectorValues.fromBytes(bytes, dims);
        }
    }
}
