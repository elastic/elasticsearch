/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.benchmark.vector.scorer;

import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.store.NIOFSDirectory;
import org.apache.lucene.util.VectorUtil;
import org.elasticsearch.benchmark.Utils;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.index.codec.vectors.BQVectorUtils;
import org.elasticsearch.simdvec.ES93BinaryQuantizedVectorScorer;
import org.elasticsearch.simdvec.internal.vectorization.DefaultES93BinaryQuantizedVectorScorer;
import org.elasticsearch.simdvec.internal.vectorization.ESVectorizationProvider;
import org.elasticsearch.simdvec.internal.vectorization.VectorScorerTestUtils;
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
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import static org.elasticsearch.simdvec.internal.vectorization.VectorScorerTestUtils.createBinarizedIndexData;
import static org.elasticsearch.simdvec.internal.vectorization.VectorScorerTestUtils.createBinarizedQueryData;
import static org.elasticsearch.simdvec.internal.vectorization.VectorScorerTestUtils.randomVector;
import static org.elasticsearch.simdvec.internal.vectorization.VectorScorerTestUtils.writeBinarizedVectorData;

@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
// first iteration is complete garbage, so make sure we really warmup
@Warmup(iterations = 4, time = 1)
// real iterations. not useful to spend tons of time here, better to fork more
@Measurement(iterations = 5, time = 1)
// engage some noise reduction
@Fork(value = 1, jvmArgsPrepend = { "--add-modules=jdk.incubator.vector" })
public class VectorScorerBQBenchmark {

    static {
        Utils.configureBenchmarkLogging();
    }

    public enum DirectoryType {
        NIO,
        MMAP
    }

    public enum VectorImplementation {
        SCALAR,
        VECTORIZED
    }

    @Param({ "384", "768", "1024" })
    public int dims;

    @Param
    public VectorImplementation implementation;

    @Param
    public DirectoryType directoryType;

    @Param
    public VectorSimilarityFunction similarityFunction;

    // TODO: parametric?
    static final int NUM_VECTORS = 1024;
    static final int NUM_QUERIES = 10;

    VectorScorerTestUtils.VectorData[] queries;
    float centroidDp;

    ES93BinaryQuantizedVectorScorer scorer;

    Directory directory;
    IndexInput input;

    float[] scratchScores;
    int[] sequentialNodes;
    int[] randomNodes;

    record VectorData(
        VectorScorerTestUtils.VectorData[] indexVectors,
        VectorScorerTestUtils.VectorData[] queries,
        float centroidDp,
        int[] randomNodes
    ) {}

    static VectorData generateRandomVectorData(
        Random random,
        int dims,
        int numVectors,
        int numQueries,
        VectorSimilarityFunction similarityFunction
    ) {
        final float[] centroid = new float[dims];
        randomVector(random, centroid, similarityFunction);

        var quantizer = new org.elasticsearch.index.codec.vectors.OptimizedScalarQuantizer(similarityFunction);
        float[] vectorValues = new float[dims];

        VectorScorerTestUtils.VectorData[] indexVectors = new VectorScorerTestUtils.VectorData[numVectors];
        for (int i = 0; i < numVectors; i++) {
            randomVector(random, vectorValues, similarityFunction);
            indexVectors[i] = createBinarizedIndexData(vectorValues, centroid, quantizer, dims);
        }

        VectorScorerTestUtils.VectorData[] queries = new VectorScorerTestUtils.VectorData[numQueries];
        for (int i = 0; i < numQueries; i++) {
            randomVector(random, vectorValues, similarityFunction);
            queries[i] = createBinarizedQueryData(vectorValues, centroid, quantizer, dims);
        }

        int[] randomNodes = IntStream.range(0, numVectors).map(x -> random.nextInt(0, numVectors)).toArray();

        return new VectorData(indexVectors, queries, VectorUtil.dotProduct(centroid, centroid), randomNodes);
    }

    @Setup
    public void setup() throws IOException {
        setup(generateRandomVectorData(new Random(123), dims, NUM_VECTORS, NUM_QUERIES, similarityFunction));
    }

    void setup(VectorData data) throws IOException {
        int indexVectorLengthInBytes = BQVectorUtils.discretize(dims, 64) / 8;

        directory = switch (directoryType) {
            case MMAP -> new MMapDirectory(Files.createTempDirectory("vectorDataMmap"));
            case NIO -> new NIOFSDirectory(Files.createTempDirectory("vectorDataNFIOS"));
        };

        try (IndexOutput out = directory.createOutput("vectors", IOContext.DEFAULT)) {
            for (var indexData : data.indexVectors) {
                writeBinarizedVectorData(out, indexData);
            }
        }

        this.queries = data.queries;
        this.centroidDp = data.centroidDp;
        this.randomNodes = data.randomNodes;

        input = directory.openInput("vectors", IOContext.DEFAULT);

        scorer = switch (implementation) {
            case SCALAR -> new DefaultES93BinaryQuantizedVectorScorer(input, dims, indexVectorLengthInBytes);
            case VECTORIZED -> ESVectorizationProvider.getInstance()
                .newES93BinaryQuantizedVectorScorer(input, dims, indexVectorLengthInBytes);
        };
        scratchScores = new float[NUM_VECTORS];
        sequentialNodes = IntStream.range(0, NUM_VECTORS).toArray();
    }

    @TearDown
    public void teardown() throws IOException {
        IOUtils.close(directory, input);
    }

    @Benchmark
    public float[] scoreSequential() throws IOException {
        float[] results = new float[NUM_QUERIES * NUM_VECTORS];
        for (int j = 0; j < NUM_QUERIES; j++) {
            input.seek(0);
            for (int i = 0; i < NUM_VECTORS; i++) {
                float score = scorer.score(
                    queries[j].vector(),
                    queries[j].lowerInterval(),
                    queries[j].upperInterval(),
                    queries[j].quantizedComponentSum(),
                    queries[j].additionalCorrection(),
                    similarityFunction,
                    centroidDp,
                    i
                );
                results[j * NUM_VECTORS + i] = score;
            }
        }
        return results;
    }

    @Benchmark
    public float[] scoreRandom() throws IOException {
        float[] results = new float[NUM_QUERIES * NUM_VECTORS];
        for (int j = 0; j < NUM_QUERIES; j++) {
            input.seek(0);
            for (int i = 0; i < NUM_VECTORS; i++) {
                float score = scorer.score(
                    queries[j].vector(),
                    queries[j].lowerInterval(),
                    queries[j].upperInterval(),
                    queries[j].quantizedComponentSum(),
                    queries[j].additionalCorrection(),
                    similarityFunction,
                    centroidDp,
                    randomNodes[i]
                );
                results[j * NUM_VECTORS + i] = score;
            }
        }
        return results;
    }

    @Benchmark
    public float[] bulkScoreSequential() throws IOException {
        float[] results = new float[NUM_QUERIES * NUM_VECTORS];
        for (int j = 0; j < NUM_QUERIES; j++) {
            input.seek(0);
            scorer.scoreBulk(
                queries[j].vector(),
                queries[j].lowerInterval(),
                queries[j].upperInterval(),
                queries[j].quantizedComponentSum(),
                queries[j].additionalCorrection(),
                similarityFunction,
                centroidDp,
                sequentialNodes,
                scratchScores,
                NUM_VECTORS
            );
            System.arraycopy(scratchScores, 0, results, j * NUM_VECTORS, scratchScores.length);
        }
        return results;
    }

    @Benchmark
    public float[] bulkScoreRandom() throws IOException {
        float[] results = new float[NUM_QUERIES * NUM_VECTORS];
        for (int j = 0; j < NUM_QUERIES; j++) {
            input.seek(0);
            scorer.scoreBulk(
                queries[j].vector(),
                queries[j].lowerInterval(),
                queries[j].upperInterval(),
                queries[j].quantizedComponentSum(),
                queries[j].additionalCorrection(),
                similarityFunction,
                centroidDp,
                randomNodes,
                scratchScores,
                NUM_VECTORS
            );
            System.arraycopy(scratchScores, 0, results, j * NUM_VECTORS, scratchScores.length);
        }
        return results;
    }
}
