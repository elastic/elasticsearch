/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.benchmark.vector.scorer;

import org.apache.lucene.codecs.CodecUtil;
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
import org.elasticsearch.index.codec.vectors.diskbbq.next.ESNextDiskBBQVectorsFormat;
import org.elasticsearch.simdvec.ESNextOSQVectorsScorer;
import org.elasticsearch.simdvec.internal.vectorization.ESVectorizationProvider;
import org.elasticsearch.simdvec.internal.vectorization.VectorScorerTestUtils;
import org.elasticsearch.xpack.searchablesnapshots.store.SearchableSnapshotDirectoryFactory;
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
import java.util.Random;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.simdvec.internal.vectorization.VectorScorerTestUtils.createOSQIndexData;
import static org.elasticsearch.simdvec.internal.vectorization.VectorScorerTestUtils.createOSQQueryData;
import static org.elasticsearch.simdvec.internal.vectorization.VectorScorerTestUtils.randomVector;
import static org.elasticsearch.simdvec.internal.vectorization.VectorScorerTestUtils.writeBulkOSQVectorData;

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
        Utils.configureBenchmarkLogging();
    }

    public enum DirectoryType {
        NIO,
        MMAP,
        SNAP
    }

    public enum VectorImplementation {
        SCALAR,
        VECTORIZED
    }

    @Param({ "384", "768", "1024" })
    public int dims;

    @Param({ "1", "2", "4", "7" })
    public byte bits;

    @Param
    public VectorImplementation implementation;

    @Param
    public DirectoryType directoryType;

    @Param
    public VectorSimilarityFunction similarityFunction;

    static final int BULK_SIZE = ESNextOSQVectorsScorer.BULK_SIZE;
    static final int NUM_VECTORS = ESNextOSQVectorsScorer.BULK_SIZE * 10;
    static final int NUM_QUERIES = 10;

    VectorScorerTestUtils.OSQVectorData[] binaryQueries;
    float centroidDp;

    byte[] scratch;
    ESNextOSQVectorsScorer scorer;

    Path tempDir;
    Directory directory;
    IndexInput input;

    float[] scratchScores;

    record VectorData(
        VectorScorerTestUtils.OSQVectorData[] indexVectors,
        VectorScorerTestUtils.OSQVectorData[] queries,
        int binaryIndexLength,
        float centroidDp
    ) {}

    static VectorData generateRandomVectorData(
        Random random,
        int dims,
        byte bits,
        int numVectors,
        VectorSimilarityFunction similarityFunction
    ) {
        int binaryIndexLength = ESNextDiskBBQVectorsFormat.QuantEncoding.fromBits(bits).getDocPackedLength(dims);

        final float[] centroid = new float[dims];
        randomVector(random, centroid, similarityFunction);

        var quantizer = new org.elasticsearch.index.codec.vectors.OptimizedScalarQuantizer(similarityFunction);

        VectorScorerTestUtils.OSQVectorData[] indexVectors = new VectorScorerTestUtils.OSQVectorData[numVectors];
        for (int i = 0; i < numVectors; i++) {
            var vector = new float[dims];
            randomVector(random, vector, similarityFunction);
            indexVectors[i] = createOSQIndexData(vector, centroid, quantizer, dims, bits, binaryIndexLength);
        }

        int binaryQueryLength = ESNextDiskBBQVectorsFormat.QuantEncoding.fromBits(bits).getQueryPackedLength(dims);
        byte queryBits = bits == 7 ? (byte) 7 : (byte) 4;
        VectorScorerTestUtils.OSQVectorData[] queryVectors = new VectorScorerTestUtils.OSQVectorData[numVectors];
        var query = new float[dims];
        for (int i = 0; i < numVectors; i++) {
            randomVector(random, query, similarityFunction);
            queryVectors[i] = createOSQQueryData(query, centroid, quantizer, dims, queryBits, binaryQueryLength);
        }

        return new VectorData(indexVectors, queryVectors, binaryIndexLength, VectorUtil.dotProduct(centroid, centroid));
    }

    @Setup
    public void setup() throws IOException {
        setup(generateRandomVectorData(new Random(123), dims, bits, NUM_VECTORS, similarityFunction));
    }

    void setup(VectorData data) throws IOException {
        this.directory = switch (directoryType) {
            case MMAP -> new MMapDirectory(createTempDirectory("vectorDataMmap"));
            case NIO -> new NIOFSDirectory(createTempDirectory("vectorDataNFIOS"));
            case SNAP -> SearchableSnapshotDirectoryFactory.newDirectory(createTempDirectory("vectorDataSNAP"));
        };

        try (IndexOutput output = directory.createOutput("vectors", IOContext.DEFAULT)) {
            for (int i = 0; i < NUM_VECTORS; i += BULK_SIZE) {
                writeBulkOSQVectorData(BULK_SIZE, output, data.indexVectors, i);
            }
            CodecUtil.writeFooter(output);
        }
        this.input = directory.openInput("vectors", IOContext.DEFAULT);

        this.binaryQueries = data.queries;
        this.centroidDp = data.centroidDp;

        this.scratch = new byte[data.binaryIndexLength];
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
            case SCALAR -> new ESNextOSQVectorsScorer(input, (byte) queryBits, (byte) docBits, dims, data.binaryIndexLength);
            case VECTORIZED -> ESVectorizationProvider.getInstance()
                .newESNextOSQVectorsScorer(input, (byte) queryBits, (byte) docBits, dims, data.binaryIndexLength, BULK_SIZE);
        };
        scratchScores = new float[BULK_SIZE];
    }

    Path createTempDirectory(String name) throws IOException {
        tempDir = Files.createTempDirectory(name);
        return tempDir;
    }

    @TearDown
    public void teardown() throws IOException {
        IOUtils.close(directory, input);
    }

    @Benchmark
    public float[] score() throws IOException {
        float[] results = new float[NUM_QUERIES * NUM_VECTORS];

        float[] lowerIntervals = new float[BULK_SIZE];
        float[] upperIntervals = new float[BULK_SIZE];
        int[] sums = new int[BULK_SIZE];
        float[] additional = new float[BULK_SIZE];

        for (int j = 0; j < NUM_QUERIES; j++) {
            input.seek(0);
            for (int i = 0; i < NUM_VECTORS; i += BULK_SIZE) {
                scorer.quantizeScoreBulk(binaryQueries[j].quantizedVector(), BULK_SIZE, scratchScores);
                input.readFloats(lowerIntervals, 0, BULK_SIZE);
                input.readFloats(upperIntervals, 0, BULK_SIZE);
                input.readInts(sums, 0, BULK_SIZE);
                input.readFloats(additional, 0, BULK_SIZE);

                for (int b = 0; b < BULK_SIZE; b++) {
                    float score = scorer.score(
                        binaryQueries[j].lowerInterval(),
                        binaryQueries[j].upperInterval(),
                        binaryQueries[j].quantizedComponentSum(),
                        binaryQueries[j].additionalCorrection(),
                        similarityFunction,
                        centroidDp,
                        lowerIntervals[b],
                        upperIntervals[b],
                        sums[b],
                        additional[b],
                        scratchScores[b]
                    );
                    results[j * NUM_VECTORS + i + b] = score;
                }
            }
        }
        return results;
    }

    @Benchmark
    public float[] bulkScore() throws IOException {
        float[] results = new float[NUM_QUERIES * NUM_VECTORS];
        for (int j = 0; j < NUM_QUERIES; j++) {
            input.seek(0);
            for (int i = 0; i < NUM_VECTORS; i += scratchScores.length) {
                scorer.scoreBulk(
                    binaryQueries[j].quantizedVector(),
                    binaryQueries[j].lowerInterval(),
                    binaryQueries[j].upperInterval(),
                    binaryQueries[j].quantizedComponentSum(),
                    binaryQueries[j].additionalCorrection(),
                    similarityFunction,
                    centroidDp,
                    scratchScores
                );
                System.arraycopy(scratchScores, 0, results, j * NUM_VECTORS + i, scratchScores.length);
            }
        }
        return results;
    }
}
