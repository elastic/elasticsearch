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
import org.elasticsearch.common.logging.LogConfigurator;
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
        LogConfigurator.configureESLogging(); // native access requires logging to be initialized
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
    public int numVectors = 1024;
    int numQueries = 10;

    int indexVectorLengthInBytes;

    VectorScorerTestUtils.VectorData[] queries;
    float centroidDp;

    ES93BinaryQuantizedVectorScorer scorer;

    Directory directory;
    IndexInput input;

    float[] scratchScores;
    int[] sequentialNodes;
    int[] randomNodes;

    @Setup
    public void setup() throws IOException {
        setup(new Random(123));
    }

    private void createTestFile(
        Random random,
        Directory dir,
        int numVectors,
        float[] vectorValues,
        float[] centroid,
        org.elasticsearch.index.codec.vectors.OptimizedScalarQuantizer quantizer,
        int dims
    ) throws IOException {
        try (IndexOutput out = dir.createOutput("vectors", IOContext.DEFAULT)) {
            for (int i = 0; i < numVectors; i++) {
                randomVector(random, vectorValues, similarityFunction);
                var indexData = createBinarizedIndexData(vectorValues, centroid, quantizer, dims);
                writeBinarizedVectorData(out, indexData);
            }
        }
    }

    void setup(Random random) throws IOException {
        this.indexVectorLengthInBytes = BQVectorUtils.discretize(dims, 64) / 8;

        directory = switch (directoryType) {
            case MMAP -> new MMapDirectory(Files.createTempDirectory("vectorDataMmap"));
            case NIO -> new NIOFSDirectory(Files.createTempDirectory("vectorDataNFIOS"));
        };

        final float[] centroid = new float[dims];
        randomVector(random, centroid, similarityFunction);
        centroidDp = VectorUtil.dotProduct(centroid, centroid);

        float[] vectorValues = new float[dims];
        var quantizer = new org.elasticsearch.index.codec.vectors.OptimizedScalarQuantizer(similarityFunction);

        createTestFile(random, directory, numVectors, vectorValues, centroid, quantizer, dims);
        queries = new VectorScorerTestUtils.VectorData[numQueries];
        for (int i = 0; i < numQueries; i++) {
            randomVector(random, vectorValues, similarityFunction);
            queries[i] = createBinarizedQueryData(vectorValues, centroid, quantizer, dims);
        }

        input = directory.openInput("vectors", IOContext.DEFAULT);

        scorer = switch (implementation) {
            case SCALAR -> new DefaultES93BinaryQuantizedVectorScorer(input, dims, indexVectorLengthInBytes);
            case VECTORIZED -> ESVectorizationProvider.getInstance()
                .newES93BinaryQuantizedVectorScorer(input, dims, indexVectorLengthInBytes);
        };
        scratchScores = new float[numVectors];
        sequentialNodes = IntStream.range(0, numVectors).toArray();
        randomNodes = IntStream.range(0, numVectors).map(x -> random.nextInt(0, numVectors)).toArray();
    }

    @TearDown
    public void teardown() throws IOException {
        IOUtils.close(directory, input);
    }

    @Benchmark
    public float[] scoreSequential() throws IOException {
        float[] results = new float[numQueries * numVectors];
        for (int j = 0; j < numQueries; j++) {
            input.seek(0);
            for (int i = 0; i < numVectors; i++) {
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
                results[j * numVectors + i] = score;
            }
        }
        return results;
    }

    @Benchmark
    public float[] scoreRandom() throws IOException {
        float[] results = new float[numQueries * numVectors];
        for (int j = 0; j < numQueries; j++) {
            input.seek(0);
            for (int i = 0; i < numVectors; i++) {
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
                results[j * numVectors + i] = score;
            }
        }
        return results;
    }

    @Benchmark
    public float[] bulkScoreSequential() throws IOException {
        float[] results = new float[numQueries * numVectors];
        for (int j = 0; j < numQueries; j++) {
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
                numVectors
            );
            System.arraycopy(scratchScores, 0, results, j * numVectors, scratchScores.length);
        }
        return results;
    }

    @Benchmark
    public float[] bulkScoreRandom() throws IOException {
        float[] results = new float[numQueries * numVectors];
        for (int j = 0; j < numQueries; j++) {
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
                numVectors
            );
            System.arraycopy(scratchScores, 0, results, j * numVectors, scratchScores.length);
        }
        return results;
    }
}
