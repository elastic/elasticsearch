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
import org.apache.lucene.util.quantization.OptimizedScalarQuantizer;
import org.elasticsearch.common.logging.LogConfigurator;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.index.codec.vectors.diskbbq.next.ESNextDiskBBQVectorsFormat;
import org.elasticsearch.simdvec.ESNextOSQVectorsScorer;
import org.elasticsearch.simdvec.internal.vectorization.ESVectorizationProvider;
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

@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
// first iteration is complete garbage, so make sure we really warmup
@Warmup(iterations = 4, time = 1)
// real iterations. not useful to spend tons of time here, better to fork more
@Measurement(iterations = 5, time = 1)
// engage some noise reduction
@Fork(value = 1, jvmArgsPrepend = { "--add-modules=jdk.incubator.vector" })
public class VectorScorerOSQBenchmark {

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

    @Param({ "1", "2", "4", "7" })
    public int bits;

    int bulkSize = ESNextOSQVectorsScorer.BULK_SIZE;

    @Param
    public VectorImplementation implementation;

    @Param
    public DirectoryType directoryType;

    public int numVectors = ESNextOSQVectorsScorer.BULK_SIZE * 10;
    int numQueries = 10;

    int length;

    byte[][] binaryVectors;
    byte[][] binaryQueries;
    OptimizedScalarQuantizer.QuantizationResult result;
    float centroidDp;

    byte[] scratch;
    ESNextOSQVectorsScorer scorer;
    ScorerAdapter scorerAdapter;

    Directory directory;
    IndexInput input;

    float[] scratchScores;
    float[] corrections;

    @Setup
    public void setup() throws IOException {
        setup(new Random(123));
    }

    void setup(Random random) throws IOException {
        this.length = switch (bits) {
            case 1 -> ESNextDiskBBQVectorsFormat.QuantEncoding.ONE_BIT_4BIT_QUERY.getDocPackedLength(dims);
            case 2 -> ESNextDiskBBQVectorsFormat.QuantEncoding.TWO_BIT_4BIT_QUERY.getDocPackedLength(dims);
            case 4 -> ESNextDiskBBQVectorsFormat.QuantEncoding.FOUR_BIT_SYMMETRIC.getDocPackedLength(dims);
            case 7 -> ESNextDiskBBQVectorsFormat.QuantEncoding.SEVEN_BIT_SYMMETRIC.getDocPackedLength(dims);
            default -> throw new IllegalArgumentException("Unsupported bits: " + bits);
        };

        binaryVectors = new byte[numVectors][length];
        for (byte[] binaryVector : binaryVectors) {
            random.nextBytes(binaryVector);
        }

        directory = switch (directoryType) {
            case MMAP -> new MMapDirectory(Files.createTempDirectory("vectorDataMmap"));
            case NIO -> new NIOFSDirectory(Files.createTempDirectory("vectorDataNFIOS"));
        };

        try (IndexOutput output = directory.createOutput("vectors", IOContext.DEFAULT)) {
            int correctionBytesLength = bits == 7 ? 16 * bulkSize : 14 * bulkSize;
            byte[] correctionBytes = new byte[correctionBytesLength];
            for (int i = 0; i < numVectors; i += bulkSize) {
                for (int j = 0; j < bulkSize; j++) {
                    output.writeBytes(binaryVectors[i + j], 0, binaryVectors[i + j].length);
                }
                random.nextBytes(correctionBytes);
                output.writeBytes(correctionBytes, 0, correctionBytes.length);
            }
        }
        input = directory.openInput("vectors", IOContext.DEFAULT);
        int binaryQueryLength = switch (bits) {
            case 1 -> ESNextDiskBBQVectorsFormat.QuantEncoding.ONE_BIT_4BIT_QUERY.getQueryPackedLength(dims);
            case 2 -> ESNextDiskBBQVectorsFormat.QuantEncoding.TWO_BIT_4BIT_QUERY.getQueryPackedLength(dims);
            case 4 -> ESNextDiskBBQVectorsFormat.QuantEncoding.FOUR_BIT_SYMMETRIC.getQueryPackedLength(dims);
            case 7 -> ESNextDiskBBQVectorsFormat.QuantEncoding.SEVEN_BIT_SYMMETRIC.getQueryPackedLength(dims);
            default -> throw new IllegalArgumentException("Unsupported bits: " + bits);
        };

        binaryQueries = new byte[numVectors][binaryQueryLength];
        for (byte[] binaryQuery : binaryQueries) {
            random.nextBytes(binaryQuery);
        }
        result = new OptimizedScalarQuantizer.QuantizationResult(
            random.nextFloat(),
            random.nextFloat(),
            random.nextFloat(),
            Short.toUnsignedInt((short) random.nextInt())
        );
        centroidDp = random.nextFloat();

        scratch = new byte[length];
        final int docBits;
        final int queryBits = switch (bits) {
            case 1 -> {
                docBits = 1;
                yield 4;
            }
            case 2 -> {
                docBits = 2;
                yield 4;
            }
            case 4 -> {
                docBits = 4;
                yield 4;
            }
            case 7 -> {
                docBits = 7;
                yield 7;
            }
            default -> throw new IllegalArgumentException("Unsupported bits: " + bits);
        };
        scorer = switch (implementation) {
            case SCALAR -> new ESNextOSQVectorsScorer(input, (byte) queryBits, (byte) docBits, dims, length);
            case VECTORIZED -> ESVectorizationProvider.getInstance()
                .newESNextOSQVectorsScorer(input, (byte) queryBits, (byte) docBits, dims, length, bulkSize);
        };
        scorerAdapter = bits == 7 ? new Int7ScorerAdapter() : new OsqScorerAdapter();
        scratchScores = new float[bulkSize];
        corrections = new float[3];
    }

    @TearDown
    public void teardown() throws IOException {
        IOUtils.close(directory, input);
    }

    @Benchmark
    public float[] score() throws IOException {
        float[] results = new float[numQueries * numVectors];
        for (int j = 0; j < numQueries; j++) {
            input.seek(0);
            for (int i = 0; i < numVectors; i++) {
                results[j * numVectors + i] = scorerAdapter.score(binaryQueries[j]);
            }
        }
        return results;
    }

    @Benchmark
    public float[] bulkScore() throws IOException {
        float[] results = new float[numQueries * numVectors];
        for (int j = 0; j < numQueries; j++) {
            input.seek(0);
            for (int i = 0; i < numVectors; i += scratchScores.length) {
                scorerAdapter.scoreBulk(binaryQueries[j], scratchScores);
                System.arraycopy(scratchScores, 0, results, j * numVectors + i, scratchScores.length);
            }
        }
        return results;
    }

    private interface ScorerAdapter {
        float score(byte[] query) throws IOException;

        void scoreBulk(byte[] query, float[] scores) throws IOException;
    }

    private class OsqScorerAdapter implements ScorerAdapter {
        @Override
        public float score(byte[] query) throws IOException {
            float qDist = scorer.quantizeScore(query);
            input.readFloats(corrections, 0, corrections.length);
            int addition = Short.toUnsignedInt(input.readShort());
            return scorer.score(
                result.lowerInterval(),
                result.upperInterval(),
                result.quantizedComponentSum(),
                result.additionalCorrection(),
                VectorSimilarityFunction.EUCLIDEAN,
                centroidDp,
                corrections[0],
                corrections[1],
                addition,
                corrections[2],
                qDist
            );
        }

        @Override
        public void scoreBulk(byte[] query, float[] scores) throws IOException {
            scorer.scoreBulk(
                query,
                result.lowerInterval(),
                result.upperInterval(),
                result.quantizedComponentSum(),
                result.additionalCorrection(),
                VectorSimilarityFunction.EUCLIDEAN,
                centroidDp,
                scores
            );
        }
    }

    private class Int7ScorerAdapter implements ScorerAdapter {
        @Override
        public float score(byte[] query) throws IOException {
            return scorer.score(
                query,
                result.lowerInterval(),
                result.upperInterval(),
                result.quantizedComponentSum(),
                result.additionalCorrection(),
                VectorSimilarityFunction.EUCLIDEAN,
                centroidDp
            );
        }

        @Override
        public void scoreBulk(byte[] query, float[] scores) throws IOException {
            scorer.scoreBulk(
                query,
                result.lowerInterval(),
                result.upperInterval(),
                result.quantizedComponentSum(),
                result.additionalCorrection(),
                VectorSimilarityFunction.EUCLIDEAN,
                centroidDp,
                scores,
                scores.length
            );
        }
    }
}
