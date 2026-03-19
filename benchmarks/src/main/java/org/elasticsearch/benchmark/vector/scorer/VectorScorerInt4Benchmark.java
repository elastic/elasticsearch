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
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.util.hnsw.RandomVectorScorer;
import org.apache.lucene.util.hnsw.UpdateableRandomVectorScorer;
import org.apache.lucene.util.quantization.OptimizedScalarQuantizer;
import org.elasticsearch.benchmark.Utils;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.simdvec.VectorSimilarityType;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

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
 * Benchmark that compares int4 packed-nibble quantized vector similarity scoring:
 * scalar vs Lucene's Lucene104ScalarQuantizedVectorScorer vs native.
 * Run with ./gradlew -p benchmarks run --args 'VectorScorerInt4Benchmark'
 */
@Fork(value = 1, jvmArgsPrepend = { "--add-modules=jdk.incubator.vector" })
@Warmup(iterations = 3, time = 3)
@Measurement(iterations = 5, time = 3)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Thread)
public class VectorScorerInt4Benchmark {

    static {
        Utils.configureBenchmarkLogging();
    }

    @Param({ "96", "768", "1024" })
    public int dims;
    public static int numVectors = 2;

    @Param
    public VectorImplementation implementation;

    @Param({ "DOT_PRODUCT", "EUCLIDEAN" })
    public VectorSimilarityType function;

    private Path path;
    private Directory dir;
    private IndexInput in;

    private UpdateableRandomVectorScorer scorer;
    private RandomVectorScorer queryScorer;

    static class VectorData {
        final byte[][] packedVectors;
        final OptimizedScalarQuantizer.QuantizationResult[] corrections;
        final float[] centroid;
        final float centroidDp;
        final float[] queryVector;

        VectorData(int dims) {
            packedVectors = new byte[numVectors][];
            ThreadLocalRandom random = ThreadLocalRandom.current();
            for (int v = 0; v < numVectors; v++) {
                byte[] unpacked = new byte[dims];
                randomInt4Bytes(random, unpacked);
                packedVectors[v] = packNibbles(unpacked);
            }
            corrections = generateCorrectiveTerms(dims, numVectors);
            centroid = generateCentroid(dims);
            centroidDp = dotProduct(centroid, centroid);
            queryVector = new float[dims];
            for (int i = 0; i < dims; i++) {
                queryVector[i] = random.nextFloat();
            }
        }
    }

    @Setup
    public void setup() throws IOException {
        setup(new VectorData(dims));
    }

    void setup(VectorData vectorData) throws IOException {
        VectorSimilarityFunction similarityFunction = function.function();

        path = Files.createTempDirectory("Int4ScorerBenchmark");
        dir = new MMapDirectory(path);
        writeI4VectorData(dir, vectorData.packedVectors, vectorData.corrections);
        in = dir.openInput(VECTOR_DATA_FILE, IOContext.DEFAULT);

        QuantizedByteVectorValues values = createDenseInt4VectorValues(
            dims,
            numVectors,
            vectorData.centroid,
            vectorData.centroidDp,
            in,
            similarityFunction
        );

        switch (implementation) {
            case SCALAR:
                scorer = createI4ScalarScorer(values, similarityFunction);
                queryScorer = createI4ScalarQueryScorer(values, similarityFunction, vectorData.queryVector);
                break;
            case LUCENE:
                scorer = lucene104ScoreSupplier(values, similarityFunction).scorer();
                if (supportsHeapSegments()) {
                    queryScorer = lucene104Scorer(values, similarityFunction, vectorData.queryVector);
                }
                break;
            case NATIVE:
                var factory = getScorerFactoryOrDie();
                scorer = factory.getInt4VectorScorerSupplier(function, in, values).orElseThrow().scorer();
                if (supportsHeapSegments()) {
                    var qQuery = quantizeQuery(values, similarityFunction, vectorData.queryVector);
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

        scorer.setScoringOrdinal(0);
    }

    @TearDown
    public void teardown() throws IOException {
        IOUtils.close(in, dir);
        IOUtils.rm(path);
    }

    @Benchmark
    public float score() throws IOException {
        return scorer.score(1);
    }

    @Benchmark
    public float scoreQuery() throws IOException {
        return queryScorer.score(1);
    }
}
