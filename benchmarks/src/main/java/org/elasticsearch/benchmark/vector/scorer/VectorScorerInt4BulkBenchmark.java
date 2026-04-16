/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.vector.scorer;

import org.apache.lucene.codecs.lucene104.QuantizedByteVectorValues;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.quantization.OptimizedScalarQuantizer;
import org.elasticsearch.simdvec.VectorSimilarityType;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Setup;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

import static org.elasticsearch.benchmark.vector.scorer.BenchmarkUtils.getScorerFactoryOrDie;
import static org.elasticsearch.benchmark.vector.scorer.BenchmarkUtils.lucene104ScoreSupplier;
import static org.elasticsearch.benchmark.vector.scorer.BenchmarkUtils.lucene104Scorer;
import static org.elasticsearch.benchmark.vector.scorer.BenchmarkUtils.supportsHeapSegments;
import static org.elasticsearch.benchmark.vector.scorer.Int4BenchmarkUtils.VECTOR_DATA_FILE;
import static org.elasticsearch.benchmark.vector.scorer.Int4BenchmarkUtils.createI4ScalarQueryScorer;
import static org.elasticsearch.benchmark.vector.scorer.Int4BenchmarkUtils.createI4ScalarScorer;
import static org.elasticsearch.benchmark.vector.scorer.Int4BenchmarkUtils.generateCentroid;
import static org.elasticsearch.benchmark.vector.scorer.Int4BenchmarkUtils.generateCorrectiveTerms;
import static org.elasticsearch.benchmark.vector.scorer.Int4BenchmarkUtils.quantizeQuery;
import static org.elasticsearch.benchmark.vector.scorer.Int4BenchmarkUtils.writeI4VectorData;
import static org.elasticsearch.nativeaccess.Int4TestUtils.packNibbles;
import static org.elasticsearch.simdvec.ESVectorUtil.dotProduct;
import static org.elasticsearch.simdvec.internal.vectorization.VectorScorerTestUtils.createDenseInt4VectorValues;
import static org.elasticsearch.simdvec.internal.vectorization.VectorScorerTestUtils.randomInt4Bytes;

/**
 * Benchmark that compares bulk scoring of int4 packed-nibble quantized vectors:
 * scalar vs Lucene's Lucene104ScalarQuantizedVectorScorer vs native,
 * across sequential and random access patterns.
 * Run with ./gradlew -p benchmarks run --args 'VectorScorerInt4BulkBenchmark'
 */
public class VectorScorerInt4BulkBenchmark extends VectorScorerBulkBenchmark {

    @Param({ "128", "1500", "130000" })
    public int numVectors;

    @Param
    public VectorImplementation implementation;

    @Param({ "DOT_PRODUCT", "EUCLIDEAN" })
    public VectorSimilarityType function;

    static class VectorData extends VectorScorerBulkBenchmark.VectorData {
        final byte[][] packedVectors;
        final OptimizedScalarQuantizer.QuantizationResult[] corrections;
        final float[] centroid;
        final float centroidDp;
        final float[] queryVector;

        VectorData(int dims, int numVectors, int numVectorsToScore, Random random) {
            super(numVectors, numVectorsToScore, random);

            packedVectors = new byte[numVectors][];
            for (int v = 0; v < numVectors; v++) {
                byte[] unpacked = new byte[dims];
                randomInt4Bytes(random, unpacked);
                packedVectors[v] = packNibbles(unpacked);
            }
            corrections = generateCorrectiveTerms(dims, numVectors);
            centroid = generateCentroid(dims);
            centroidDp = dotProduct(centroid, centroid);

            queryVector = randomFloatArray(random, dims);
        }

        @Override
        void writeVectorData(Directory directory) throws IOException {
            writeI4VectorData(directory, packedVectors, corrections);
        }

        @Override
        String vectorDataFile() {
            return VECTOR_DATA_FILE;
        }
    }

    @Setup
    public void setup() throws IOException {
        setup(new VectorData(dims, numVectors, Math.min(numVectors, 20_000), ThreadLocalRandom.current()));
    }

    void setup(VectorData vectorData) throws IOException {
        setup(vectorData, numVectors);
    }

    @Override
    void createScorers(IndexInput in, VectorScorerBulkBenchmark.VectorData vectorData) throws IOException {
        VectorSimilarityFunction similarityFunction = function.function();
        VectorData vd = (VectorData) vectorData;
        QuantizedByteVectorValues values = createDenseInt4VectorValues(
            dims,
            numVectors,
            vd.centroid,
            vd.centroidDp,
            in,
            similarityFunction
        );

        switch (implementation) {
            case SCALAR:
                scorer = createI4ScalarScorer(values, similarityFunction);
                queryScorer = createI4ScalarQueryScorer(values, similarityFunction, vd.queryVector);
                break;
            case LUCENE:
                scorer = lucene104ScoreSupplier(values, similarityFunction).scorer();
                if (supportsHeapSegments()) {
                    queryScorer = lucene104Scorer(values, similarityFunction, vd.queryVector);
                }
                break;
            case NATIVE:
                var factory = getScorerFactoryOrDie();
                scorer = factory.getInt4VectorScorerSupplier(function, in, values).orElseThrow().scorer();
                if (supportsHeapSegments()) {
                    var qQuery = quantizeQuery(values, similarityFunction, vd.queryVector);
                    queryScorer = factory.getInt4VectorScorer(
                        similarityFunction,
                        values,
                        qQuery.unpackedQuery(),
                        qQuery.corrections().lowerInterval(),
                        qQuery.corrections().upperInterval(),
                        qQuery.corrections().additionalCorrection(),
                        qQuery.corrections().quantizedComponentSum()
                    ).orElseThrow();
                }
                break;
        }
    }
}
