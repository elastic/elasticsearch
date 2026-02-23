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
import org.elasticsearch.common.logging.LogConfigurator;
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
        LogConfigurator.configureESLogging(); // native access requires logging to be initialized
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

    @Param({ "1", "2", "4" })
    public byte bits;

    int bulkSize = ESNextOSQVectorsScorer.BULK_SIZE;

    @Param
    public VectorImplementation implementation;

    @Param
    public DirectoryType directoryType;

    @Param
    public VectorSimilarityFunction similarityFunction;

    public int numVectors = ESNextOSQVectorsScorer.BULK_SIZE * 10;
    int numQueries = 10;

    int length;

    VectorScorerTestUtils.OSQVectorData[] binaryQueries;
    float centroidDp;

    byte[] scratch;
    ESNextOSQVectorsScorer scorer;

    Path tempDir;
    Directory directory;
    IndexInput input;

    float[] scratchScores;
    float[] corrections;

    @Setup
    public void setup() throws IOException {
        setup(new Random(123));
    }

    void setup(Random random) throws IOException {
        this.length = ESNextDiskBBQVectorsFormat.QuantEncoding.fromBits(bits).getDocPackedLength(dims);

        final float[] centroid = new float[dims];
        randomVector(random, centroid, similarityFunction);

        var quantizer = new org.elasticsearch.index.codec.vectors.OptimizedScalarQuantizer(similarityFunction);

        directory = switch (directoryType) {
            case MMAP -> new MMapDirectory(createTempDirectory("vectorDataMmap"));
            case NIO -> new NIOFSDirectory(createTempDirectory("vectorDataNFIOS"));
            case SNAP -> SearchableSnapshotDirectoryFactory.newDirectory(createTempDirectory("vectorDataSNAP"));
        };

        try (IndexOutput output = directory.createOutput("vectors", IOContext.DEFAULT)) {
            VectorScorerTestUtils.OSQVectorData[] vectors = new VectorScorerTestUtils.OSQVectorData[bulkSize];
            for (int i = 0; i < numVectors; i += bulkSize) {
                for (int j = 0; j < bulkSize; j++) {
                    var vector = new float[dims];
                    randomVector(random, vector, similarityFunction);
                    vectors[j] = createOSQIndexData(vector, centroid, quantizer, dims, bits, length);
                }
                writeBulkOSQVectorData(bulkSize, output, vectors);
            }
            CodecUtil.writeFooter(output);
        }
        input = directory.openInput("vectors", IOContext.DEFAULT);
        int binaryQueryLength = ESNextDiskBBQVectorsFormat.QuantEncoding.fromBits(bits).getQueryPackedLength(dims);

        binaryQueries = new VectorScorerTestUtils.OSQVectorData[numVectors];
        var query = new float[dims];
        for (int i = 0; i < numVectors; ++i) {
            randomVector(random, query, similarityFunction);
            binaryQueries[i] = createOSQQueryData(query, centroid, quantizer, dims, (byte) 4, binaryQueryLength);
        }
        centroidDp = VectorUtil.dotProduct(centroid, centroid);

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
            default -> throw new IllegalArgumentException("Unsupported bits: " + bits);
        };
        scorer = switch (implementation) {
            case SCALAR -> new ESNextOSQVectorsScorer(input, (byte) queryBits, (byte) docBits, dims, length);
            case VECTORIZED -> ESVectorizationProvider.getInstance()
                .newESNextOSQVectorsScorer(input, (byte) queryBits, (byte) docBits, dims, length, bulkSize);
        };
        scratchScores = new float[bulkSize];
        corrections = new float[3];
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
        float[] results = new float[numQueries * numVectors];
        for (int j = 0; j < numQueries; j++) {
            input.seek(0);
            for (int i = 0; i < numVectors; i++) {
                float qDist = scorer.quantizeScore(binaryQueries[j].quantizedVector());
                input.readFloats(corrections, 0, corrections.length);
                int addition = Short.toUnsignedInt(input.readShort());
                float score = scorer.score(
                    binaryQueries[j].lowerInterval(),
                    binaryQueries[j].upperInterval(),
                    binaryQueries[j].quantizedComponentSum(),
                    binaryQueries[j].additionalCorrection(),
                    similarityFunction,
                    centroidDp,
                    corrections[0],
                    corrections[1],
                    addition,
                    corrections[2],
                    qDist
                );
                results[j * numVectors + i] = score;
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
                System.arraycopy(scratchScores, 0, results, j * numVectors + i, scratchScores.length);
            }
        }
        return results;
    }
}
