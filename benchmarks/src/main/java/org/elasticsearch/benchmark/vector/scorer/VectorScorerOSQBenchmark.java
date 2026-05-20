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
import org.elasticsearch.index.codec.vectors.diskbbq.es94.ES940DiskBBQVectorsFormat;
import org.elasticsearch.simdvec.ES940OSQVectorsScorer;
import org.elasticsearch.simdvec.ESVectorizationProvider;
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

/**
 * Benchmarks for {@link ES940OSQVectorsScorer} as used by the DiskBBQ readers
 * ({@code ES9{20,40}DiskBBQVectorsReader}, {@code ESNextDiskBBQVectorsReader}).
 *
 * <p>Methods are split into two groups:
 * <ul>
 *   <li><b>{@code score*}</b> — production paths. These mirror the dispatch in the DiskBBQ
 *       readers (see {@code ES940DiskBBQVectorsReader#visit}): {@link #scoreBulk} for the
 *       all-pass / no-filter case, {@link #scoreBulkFilteredDense} /
 *       {@link #scoreBulkFilteredSparse} for partial filters, and
 *       {@link #scoreIndividualFilteredOne} for the "exactly one doc passes" case.</li>
 *   <li><b>{@code controlScore*}</b> — control / baseline benchmarks not used in production,
 *       kept to isolate the per-call cost of the dot-product kernel and to compare against the
 *       prod paths (e.g. what would per-vector scoring cost if used everywhere).</li>
 * </ul>
 *
 * <p>JMH filter patterns (the leading {@code \.} anchors at the method-name boundary):
 * <pre>
 *   # prod only
 *   ./gradlew :benchmarks:jmh -Pargs='VectorScorerOSQBenchmark\.score'
 *   # control only
 *   ./gradlew :benchmarks:jmh -Pargs='VectorScorerOSQBenchmark\.controlScore'
 *   # all, exclude control
 *   ./gradlew :benchmarks:jmh -Pargs='VectorScorerOSQBenchmark -e controlScore'
 * </pre>
 */
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
        PANAMA,
        NATIVE
    }

    @Param({ "96", "128", "192", "256", "384", "768", "1024" })
    public int dims;

    @Param({ "1", "2", "4", "7" })
    public byte bits;

    @Param({ "STRIPED", "PACKED_NIBBLE" })
    public ES940OSQVectorsScorer.SymmetricInt4Encoding int4Encoding;

    @Param
    public VectorImplementation implementation;

    @Param
    public DirectoryType directoryType;

    @Param
    public VectorSimilarityFunction similarityFunction;

    static final int BULK_SIZE = ES940OSQVectorsScorer.BULK_SIZE;
    static final int NUM_VECTORS = ES940OSQVectorsScorer.BULK_SIZE * 10;
    static final int NUM_QUERIES = 10;

    VectorScorerTestUtils.OSQVectorData[] binaryQueries;
    float centroidDp;

    byte[] scratch;
    ES940OSQVectorsScorer scorer;

    Path tempDir;
    Directory directory;
    IndexInput input;

    float[] scratchScores;
    int[] denseOffsets;
    int denseOffsetsCount;
    int[] sparseOffsets;
    int sparseOffsetsCount;
    static final int[] SINGLE_OFFSET = new int[] { 0 };

    record VectorData(
        VectorScorerTestUtils.OSQVectorData[] indexVectors,
        VectorScorerTestUtils.OSQVectorData[] queries,
        int binaryIndexLength,
        float centroidDp,
        int[] denseOffsets,
        int denseOffsetsCount,
        int[] sparseOffsets,
        int sparseOffsetsCount
    ) {}

    private static ES940OSQVectorsScorer.SymmetricInt4Encoding resolveInt4Encoding(
        byte bits,
        ES940OSQVectorsScorer.SymmetricInt4Encoding int4Encoding
    ) {
        return bits == 4 ? int4Encoding : ES940OSQVectorsScorer.SymmetricInt4Encoding.STRIPED;
    }

    private static int docPackedLength(int dims, byte bits, ES940OSQVectorsScorer.SymmetricInt4Encoding int4Encoding) {
        if (bits == 4 && int4Encoding == ES940OSQVectorsScorer.SymmetricInt4Encoding.STRIPED) {
            int discretized = ES940DiskBBQVectorsFormat.QuantEncoding.fromBits(bits).discretizedDimensions(dims);
            return 4 * ((discretized + 7) / 8);
        }
        return ES940DiskBBQVectorsFormat.QuantEncoding.fromBits(bits).getDocPackedLength(dims);
    }

    private static int queryPackedLength(int dims, byte bits, ES940OSQVectorsScorer.SymmetricInt4Encoding int4Encoding) {
        if (bits == 4 && int4Encoding == ES940OSQVectorsScorer.SymmetricInt4Encoding.STRIPED) {
            return docPackedLength(dims, bits, int4Encoding);
        }
        return ES940DiskBBQVectorsFormat.QuantEncoding.fromBits(bits).getQueryPackedLength(dims);
    }

    static VectorData generateRandomVectorData(
        Random random,
        int dims,
        byte bits,
        ES940OSQVectorsScorer.SymmetricInt4Encoding int4Encoding,
        VectorSimilarityFunction similarityFunction
    ) {
        ES940OSQVectorsScorer.SymmetricInt4Encoding resolvedEncoding = resolveInt4Encoding(bits, int4Encoding);
        int binaryIndexLength = docPackedLength(dims, bits, resolvedEncoding);

        final float[] centroid = new float[dims];
        randomVector(random, centroid, similarityFunction);

        var quantizer = new org.elasticsearch.index.codec.vectors.OptimizedScalarQuantizer(similarityFunction);

        VectorScorerTestUtils.OSQVectorData[] indexVectors = new VectorScorerTestUtils.OSQVectorData[VectorScorerOSQBenchmark.NUM_VECTORS];
        for (int i = 0; i < VectorScorerOSQBenchmark.NUM_VECTORS; i++) {
            var vector = new float[dims];
            randomVector(random, vector, similarityFunction);
            indexVectors[i] = createOSQIndexData(vector, centroid, quantizer, dims, bits, binaryIndexLength, resolvedEncoding);
        }

        int binaryQueryLength = queryPackedLength(dims, bits, resolvedEncoding);
        byte queryBits = bits == 7 ? (byte) 7 : (byte) 4;
        VectorScorerTestUtils.OSQVectorData[] queryVectors = new VectorScorerTestUtils.OSQVectorData[VectorScorerOSQBenchmark.NUM_VECTORS];
        var query = new float[dims];
        for (int i = 0; i < VectorScorerOSQBenchmark.NUM_VECTORS; i++) {
            randomVector(random, query, similarityFunction);
            queryVectors[i] = createOSQQueryData(query, centroid, quantizer, dims, queryBits, binaryQueryLength, bits, resolvedEncoding);
        }

        var denseOffsetsCount = BULK_SIZE - 3;
        var denseOffsets = VectorScorerTestUtils.generateFilteredOffsets(random, BULK_SIZE, 3);

        var sparseOffsetsCount = 3;
        var sparseOffsets = VectorScorerTestUtils.generateFilteredOffsets(random, BULK_SIZE, BULK_SIZE - 3);

        return new VectorData(
            indexVectors,
            queryVectors,
            binaryIndexLength,
            VectorUtil.dotProduct(centroid, centroid),
            denseOffsets,
            denseOffsetsCount,
            sparseOffsets,
            sparseOffsetsCount
        );
    }

    private float scoreFilteredIndividually(int queryIndex, int[] offsets, int offsetsCount) throws IOException {
        float maxScore = Float.NEGATIVE_INFINITY;
        var query = binaryQueries[queryIndex];
        int offsetIndex = 0;
        for (int j = 0; j < BULK_SIZE; j++) {
            if (offsetIndex < offsetsCount && offsets[offsetIndex] == j) {
                offsetIndex++;
                float qcDist = scorer.quantizeScore(query.quantizedVector());
                scratchScores[j] = qcDist;
            } else {
                scratchScores[j] = 0;
                input.skipBytes(scratch.length);
            }
        }

        float[] lowerIntervals = new float[BULK_SIZE];
        float[] upperIntervals = new float[BULK_SIZE];
        int[] sums = new int[BULK_SIZE];
        float[] additional = new float[BULK_SIZE];

        input.readFloats(lowerIntervals, 0, BULK_SIZE);
        input.readFloats(upperIntervals, 0, BULK_SIZE);
        input.readInts(sums, 0, BULK_SIZE);
        input.readFloats(additional, 0, BULK_SIZE);

        offsetIndex = 0;
        for (int b = 0; b < BULK_SIZE; b++) {
            if (offsetIndex < offsetsCount && offsets[offsetIndex] == b) {
                offsetIndex++;
                float score = scorer.applyCorrectionsIndividually(
                    query.lowerInterval(),
                    query.upperInterval(),
                    query.quantizedComponentSum(),
                    query.additionalCorrection(),
                    similarityFunction,
                    centroidDp,
                    lowerIntervals[b],
                    upperIntervals[b],
                    sums[b],
                    additional[b],
                    scratchScores[b]
                );
                scratchScores[b] = score;
                if (score > maxScore) {
                    maxScore = score;
                }
            } else {
                scratchScores[b] = 0;
            }
        }
        return maxScore;
    }

    @Setup
    public void setup() throws IOException {
        setup(generateRandomVectorData(new Random(123), dims, bits, int4Encoding, similarityFunction));
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
        ES940OSQVectorsScorer.SymmetricInt4Encoding resolvedEncoding = resolveInt4Encoding(bits, int4Encoding);
        this.scorer = switch (implementation) {
            case SCALAR -> ESVectorizationProvider.lookup(false, false)
                .getVectorScorerFactory()
                .newES940OSQVectorsScorer(
                    input,
                    (byte) queryBits,
                    (byte) docBits,
                    dims,
                    data.binaryIndexLength,
                    ES940OSQVectorsScorer.BULK_SIZE,
                    resolvedEncoding
                );
            case PANAMA -> ESVectorizationProvider.lookup(true, false)
                .getVectorScorerFactory()
                .newES940OSQVectorsScorer(
                    input,
                    (byte) queryBits,
                    (byte) docBits,
                    dims,
                    data.binaryIndexLength,
                    BULK_SIZE,
                    resolvedEncoding
                );
            case NATIVE -> ESVectorizationProvider.lookup(true, true)
                .getVectorScorerFactory()
                .newES940OSQVectorsScorer(
                    input,
                    (byte) queryBits,
                    (byte) docBits,
                    dims,
                    data.binaryIndexLength,
                    BULK_SIZE,
                    resolvedEncoding
                );
        };
        this.scratchScores = new float[BULK_SIZE];
        this.denseOffsets = data.denseOffsets();
        this.denseOffsetsCount = data.denseOffsetsCount();
        this.sparseOffsets = data.sparseOffsets();
        this.sparseOffsetsCount = data.sparseOffsetsCount();
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
    public float[] controlScoreIndividual() throws IOException {
        float[] results = new float[NUM_QUERIES * NUM_VECTORS];

        float[] lowerIntervals = new float[BULK_SIZE];
        float[] upperIntervals = new float[BULK_SIZE];
        int[] sums = new int[BULK_SIZE];
        float[] additional = new float[BULK_SIZE];

        // Control benchmark: pure per-vector scoring in a loop.
        // For each chunk of BULK_SIZE vectors we issue BULK_SIZE single-vector quantizeScore calls
        // (no bulk dot-product amortization), then read the corrections in bulk and apply them
        // one-by-one in Java. This isolates the per-call overhead of the dot-product kernel and
        // serves as a baseline against scoreBulk (fused native bulk).
        // Note: corrections are still bulk-read per chunk because the on-disk layout is
        // [BULK_SIZE x vectors | BULK_SIZE x lowerIntervals | ... | BULK_SIZE x additional].
        for (int j = 0; j < NUM_QUERIES; j++) {
            input.seek(0);
            for (int i = 0; i < NUM_VECTORS; i += BULK_SIZE) {
                for (int b = 0; b < BULK_SIZE; b++) {
                    scratchScores[b] = scorer.quantizeScore(binaryQueries[j].quantizedVector());
                }
                input.readFloats(lowerIntervals, 0, BULK_SIZE);
                input.readFloats(upperIntervals, 0, BULK_SIZE);
                input.readInts(sums, 0, BULK_SIZE);
                input.readFloats(additional, 0, BULK_SIZE);

                for (int b = 0; b < BULK_SIZE; b++) {
                    float score = scorer.applyCorrectionsIndividually(
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
    public float[] scoreBulk() throws IOException {
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

    @Benchmark
    public float[] controlScoreBulkFilteredOne() throws IOException {
        float[] results = new float[NUM_QUERIES * NUM_VECTORS];
        for (int j = 0; j < NUM_QUERIES; j++) {
            input.seek(0);
            for (int i = 0; i < NUM_VECTORS; i += scratchScores.length) {
                scorer.scoreBulkOffsets(
                    binaryQueries[j].quantizedVector(),
                    binaryQueries[j].lowerInterval(),
                    binaryQueries[j].upperInterval(),
                    binaryQueries[j].quantizedComponentSum(),
                    binaryQueries[j].additionalCorrection(),
                    similarityFunction,
                    centroidDp,
                    SINGLE_OFFSET,
                    1,
                    scratchScores,
                    BULK_SIZE
                );
                System.arraycopy(scratchScores, 0, results, j * NUM_VECTORS + i, scratchScores.length);
            }
        }
        return results;
    }

    @Benchmark
    public float[] scoreIndividualFilteredOne() throws IOException {
        float[] results = new float[NUM_QUERIES * NUM_VECTORS];
        for (int j = 0; j < NUM_QUERIES; j++) {
            input.seek(0);
            for (int i = 0; i < NUM_VECTORS; i += scratchScores.length) {
                scoreFilteredIndividually(j, SINGLE_OFFSET, 1);
                System.arraycopy(scratchScores, 0, results, j * NUM_VECTORS + i, scratchScores.length);
            }
        }
        return results;
    }

    @Benchmark
    public float[] scoreBulkFilteredDense() throws IOException {
        float[] results = new float[NUM_QUERIES * NUM_VECTORS];
        for (int j = 0; j < NUM_QUERIES; j++) {
            input.seek(0);
            for (int i = 0; i < NUM_VECTORS; i += scratchScores.length) {
                scorer.scoreBulkOffsets(
                    binaryQueries[j].quantizedVector(),
                    binaryQueries[j].lowerInterval(),
                    binaryQueries[j].upperInterval(),
                    binaryQueries[j].quantizedComponentSum(),
                    binaryQueries[j].additionalCorrection(),
                    similarityFunction,
                    centroidDp,
                    denseOffsets,
                    denseOffsetsCount,
                    scratchScores,
                    BULK_SIZE
                );
                System.arraycopy(scratchScores, 0, results, j * NUM_VECTORS + i, scratchScores.length);
            }
        }
        return results;
    }

    @Benchmark
    public float[] controlScoreIndividualFilteredDense() throws IOException {
        float[] results = new float[NUM_QUERIES * NUM_VECTORS];
        for (int j = 0; j < NUM_QUERIES; j++) {
            input.seek(0);
            for (int i = 0; i < NUM_VECTORS; i += scratchScores.length) {
                scoreFilteredIndividually(j, denseOffsets, denseOffsetsCount);
                System.arraycopy(scratchScores, 0, results, j * NUM_VECTORS + i, scratchScores.length);
            }
        }
        return results;
    }

    @Benchmark
    public float[] scoreBulkFilteredSparse() throws IOException {
        float[] results = new float[NUM_QUERIES * NUM_VECTORS];
        for (int j = 0; j < NUM_QUERIES; j++) {
            input.seek(0);
            for (int i = 0; i < NUM_VECTORS; i += scratchScores.length) {
                scorer.scoreBulkOffsets(
                    binaryQueries[j].quantizedVector(),
                    binaryQueries[j].lowerInterval(),
                    binaryQueries[j].upperInterval(),
                    binaryQueries[j].quantizedComponentSum(),
                    binaryQueries[j].additionalCorrection(),
                    similarityFunction,
                    centroidDp,
                    sparseOffsets,
                    sparseOffsetsCount,
                    scratchScores,
                    BULK_SIZE
                );
                System.arraycopy(scratchScores, 0, results, j * NUM_VECTORS + i, scratchScores.length);
            }
        }
        return results;
    }

    @Benchmark
    public float[] controlScoreIndividualFilteredSparse() throws IOException {
        float[] results = new float[NUM_QUERIES * NUM_VECTORS];
        for (int j = 0; j < NUM_QUERIES; j++) {
            input.seek(0);
            for (int i = 0; i < NUM_VECTORS; i += BULK_SIZE) {
                scoreFilteredIndividually(j, sparseOffsets, sparseOffsetsCount);
                System.arraycopy(scratchScores, 0, results, j * NUM_VECTORS + i, scratchScores.length);
            }
        }
        return results;
    }
}
