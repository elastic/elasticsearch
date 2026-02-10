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
import org.elasticsearch.index.codec.vectors.diskbbq.next.ESNextDiskBBQVectorsFormat;
import org.elasticsearch.simdvec.ESNextOSQVectorsScorer;
import org.elasticsearch.simdvec.ESVectorUtil;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

public class ESNextOSQVectorsScorerTests extends BaseVectorizationTests {

    private final DirectoryType directoryType;
    private final byte indexBits;
    private final VectorSimilarityFunction similarityFunction;
    private static final byte queryBits = 4;

    public enum DirectoryType {
        NIOFS,
        MMAP
    }

    public ESNextOSQVectorsScorerTests(DirectoryType directoryType, byte indexBits, VectorSimilarityFunction similarityFunction) {
        this.directoryType = directoryType;
        this.indexBits = indexBits;
        this.similarityFunction = similarityFunction;
    }

    public void testQuantizeScore() throws Exception {

        final int dimensions = random().nextInt(1, 2000);
        final int length2 = BQVectorUtils.discretize(dimensions, 64) / 8;

        final int length = switch (indexBits) {
            case 1 -> ESNextDiskBBQVectorsFormat.QuantEncoding.ONE_BIT_4BIT_QUERY.getDocPackedLength(dimensions);
            case 2 -> ESNextDiskBBQVectorsFormat.QuantEncoding.TWO_BIT_4BIT_QUERY.getDocPackedLength(dimensions);
            case 4 -> ESNextDiskBBQVectorsFormat.QuantEncoding.FOUR_BIT_SYMMETRIC.getDocPackedLength(dimensions);
            default -> throw new IllegalArgumentException("Unsupported bits: " + indexBits);
        };

        final int numVectors = random().nextInt(1, 100);
        final byte[] vector = new byte[length];

        final int queryBytes = length * (queryBits / indexBits);

        try (Directory dir = newParametrizedDirectory()) {
            try (IndexOutput out = dir.createOutput("tests.bin", IOContext.DEFAULT)) {
                for (int i = 0; i < numVectors; i++) {
                    random().nextBytes(vector);
                    out.writeBytes(vector, 0, length);
                }
            }
            final byte[] query = new byte[queryBytes];
            random().nextBytes(query);
            try (IndexInput in = dir.openInput("tests.bin", IOContext.DEFAULT)) {
                // Work on a slice that has just the right number of bytes to make the test fail with an
                // index-out-of-bounds in case the implementation reads more than the allowed number of
                // padding bytes.
                final IndexInput slice = in.slice("test", 0, (long) length * numVectors);
                final var defaultScorer = defaultProvider().newESNextOSQVectorsScorer(
                    slice,
                    queryBits,
                    indexBits,
                    dimensions,
                    length,
                    ESNextOSQVectorsScorer.BULK_SIZE
                );
                final var panamaScorer = maybePanamaProvider().newESNextOSQVectorsScorer(
                    in,
                    queryBits,
                    indexBits,
                    dimensions,
                    length,
                    ESNextOSQVectorsScorer.BULK_SIZE
                );
                for (int i = 0; i < numVectors; i++) {
                    assertEquals(defaultScorer.quantizeScore(query), panamaScorer.quantizeScore(query));
                    assertEquals(in.getFilePointer(), slice.getFilePointer());
                }
                assertEquals((long) length * numVectors, slice.getFilePointer());
            }
        }
    }

    public void testScore() throws Exception {
        final int maxDims = random().nextInt(1, 1000) * 2;
        final int dimensions = random().nextInt(1, maxDims);

        final int length = switch (indexBits) {
            case 1 -> ESNextDiskBBQVectorsFormat.QuantEncoding.ONE_BIT_4BIT_QUERY.getDocPackedLength(dimensions);
            case 2 -> ESNextDiskBBQVectorsFormat.QuantEncoding.TWO_BIT_4BIT_QUERY.getDocPackedLength(dimensions);
            case 4 -> ESNextDiskBBQVectorsFormat.QuantEncoding.FOUR_BIT_SYMMETRIC.getDocPackedLength(dimensions);
            default -> throw new IllegalArgumentException("Unsupported bits: " + indexBits);
        };

        final int queryBytes = length * (queryBits / indexBits);

        final int numVectors = random().nextInt(10, 50);
        float[][] vectors = new float[numVectors][dimensions];
        final int[] scratch = new int[dimensions];
        final float[] residualScratch = new float[dimensions];
        final byte[] qVector = new byte[length];
        final float[] centroid = new float[dimensions];
        randomVector(centroid, similarityFunction);
        OptimizedScalarQuantizer quantizer = new OptimizedScalarQuantizer(similarityFunction);
        int padding = random().nextInt(100);
        byte[] paddingBytes = new byte[padding];
        try (Directory dir = newParametrizedDirectory()) {
            try (IndexOutput out = dir.createOutput("testScore.bin", IOContext.DEFAULT)) {
                random().nextBytes(paddingBytes);
                out.writeBytes(paddingBytes, 0, padding);
                for (float[] vector : vectors) {
                    randomVector(vector, similarityFunction);
                    OptimizedScalarQuantizer.QuantizationResult result = quantizer.scalarQuantize(
                        vector,
                        residualScratch,
                        scratch,
                        (byte) 1,
                        centroid
                    );
                    ESVectorUtil.packAsBinary(scratch, qVector);
                    out.writeBytes(qVector, 0, qVector.length);
                    out.writeInt(Float.floatToIntBits(result.lowerInterval()));
                    out.writeInt(Float.floatToIntBits(result.upperInterval()));
                    out.writeInt(Float.floatToIntBits(result.additionalCorrection()));
                    out.writeInt(result.quantizedComponentSum());
                }
            }
            final float[] query = new float[dimensions];
            randomVector(query, similarityFunction);
            OptimizedScalarQuantizer.QuantizationResult queryCorrections = quantizer.scalarQuantize(
                query,
                residualScratch,
                scratch,
                queryBits,
                centroid
            );
            final byte[] quantizeQuery = new byte[queryBytes];
            ESVectorUtil.transposeHalfByte(scratch, quantizeQuery);
            final float centroidDp = VectorUtil.dotProduct(centroid, centroid);
            final float[] floatScratch = new float[3];
            try (IndexInput in = dir.openInput("testScore.bin", IOContext.DEFAULT)) {
                in.seek(padding);
                final int perVectorBytes = length + 16;
                assertEquals(in.length(), padding + (long) numVectors * perVectorBytes);
                final IndexInput slice = in.slice("test", in.getFilePointer(), (long) perVectorBytes * numVectors);
                // Work on a slice that has just the right number of bytes to make the test fail with an
                // index-out-of-bounds in case the implementation reads more than the allowed number of
                // padding bytes.
                for (int i = 0; i < numVectors; i++) {
                    final var defaultScorer = defaultProvider().newESNextOSQVectorsScorer(
                        slice,
                        queryBits,
                        indexBits,
                        dimensions,
                        length,
                        ESNextOSQVectorsScorer.BULK_SIZE
                    );
                    final var panamaScorer = maybePanamaProvider().newESNextOSQVectorsScorer(
                        in,
                        queryBits,
                        indexBits,
                        dimensions,
                        length,
                        ESNextOSQVectorsScorer.BULK_SIZE
                    );
                    long qDist = defaultScorer.quantizeScore(quantizeQuery);
                    slice.readFloats(floatScratch, 0, 3);
                    int quantizedComponentSum = slice.readInt();
                    float defaulScore = defaultScorer.score(
                        queryCorrections.lowerInterval(),
                        queryCorrections.upperInterval(),
                        queryCorrections.quantizedComponentSum(),
                        queryCorrections.additionalCorrection(),
                        similarityFunction,
                        centroidDp,
                        floatScratch[0],
                        floatScratch[1],
                        quantizedComponentSum,
                        floatScratch[2],
                        qDist
                    );
                    qDist = panamaScorer.quantizeScore(quantizeQuery);
                    in.readFloats(floatScratch, 0, 3);
                    quantizedComponentSum = in.readInt();
                    float panamaScore = panamaScorer.score(
                        queryCorrections.lowerInterval(),
                        queryCorrections.upperInterval(),
                        queryCorrections.quantizedComponentSum(),
                        queryCorrections.additionalCorrection(),
                        similarityFunction,
                        centroidDp,
                        floatScratch[0],
                        floatScratch[1],
                        quantizedComponentSum,
                        floatScratch[2],
                        qDist
                    );
                    assertEquals(defaulScore, panamaScore, 1e-2f);
                    assertEquals(((long) (i + 1) * perVectorBytes), slice.getFilePointer());
                    assertEquals(padding + ((long) (i + 1) * perVectorBytes), in.getFilePointer());
                }
            }
        }
    }

    public void testScoreBulk() throws Exception {
        doTestScoreBulk(ESNextOSQVectorsScorer.BULK_SIZE);
    }

    public void testScoreBulkNonAlignedBulkSize() throws Exception {
        // Pick a bulkSize that is NOT a multiple of 8, so that the tail path in
        // applyCorrectionsIndividually is exercised for both 128-bit (species length 4)
        // and 256-bit (species length 8) vector implementations.
        final int bulkSize = randomIntBetween(1, ESNextOSQVectorsScorer.BULK_SIZE - 1) | 1; // ensure odd
        doTestScoreBulk(bulkSize);
    }

    private void doTestScoreBulk(int bulkSize) throws Exception {
        final int maxDims = random().nextInt(1, 1000) * 2;
        final int dimensions = random().nextInt(1, maxDims);

        final int length = switch (indexBits) {
            case 1 -> ESNextDiskBBQVectorsFormat.QuantEncoding.ONE_BIT_4BIT_QUERY.getDocPackedLength(dimensions);
            case 2 -> ESNextDiskBBQVectorsFormat.QuantEncoding.TWO_BIT_4BIT_QUERY.getDocPackedLength(dimensions);
            case 4 -> ESNextDiskBBQVectorsFormat.QuantEncoding.FOUR_BIT_SYMMETRIC.getDocPackedLength(dimensions);
            default -> throw new IllegalArgumentException("Unsupported bits: " + indexBits);
        };
        final int queryBytes = length * (queryBits / indexBits);

        final int numVectors = bulkSize * random().nextInt(1, 10);
        float[][] vectors = new float[numVectors][dimensions];
        final int[] scratch = new int[dimensions];
        final float[] residualScratch = new float[dimensions];
        final byte[] qVector = new byte[length];
        final float[] centroid = new float[dimensions];
        VectorSimilarityFunction similarityFunction = randomFrom(VectorSimilarityFunction.values());
        randomVector(centroid, similarityFunction);
        OptimizedScalarQuantizer quantizer = new OptimizedScalarQuantizer(similarityFunction);
        int padding = random().nextInt(100);
        byte[] paddingBytes = new byte[padding];
        try (Directory dir = newParametrizedDirectory()) {
            try (IndexOutput out = dir.createOutput("testScore.bin", IOContext.DEFAULT)) {
                random().nextBytes(paddingBytes);
                out.writeBytes(paddingBytes, 0, padding);
                OptimizedScalarQuantizer.QuantizationResult[] results = new OptimizedScalarQuantizer.QuantizationResult[bulkSize];
                for (int i = 0; i < numVectors; i += bulkSize) {
                    for (int j = 0; j < bulkSize; j++) {
                        randomVector(vectors[i + j], similarityFunction);
                        results[j] = quantizer.scalarQuantize(vectors[i + j], residualScratch, scratch, (byte) 1, centroid);
                        ESVectorUtil.packAsBinary(scratch, qVector);
                        out.writeBytes(qVector, 0, qVector.length);
                    }
                    writeCorrections(results, out);
                }
            }
            final float[] query = new float[dimensions];
            randomVector(query, similarityFunction);
            OptimizedScalarQuantizer.QuantizationResult queryCorrections = quantizer.scalarQuantize(
                query,
                residualScratch,
                scratch,
                (byte) 4,
                centroid
            );
            final byte[] quantizeQuery = new byte[queryBytes];
            ESVectorUtil.transposeHalfByte(scratch, quantizeQuery);
            final float centroidDp = VectorUtil.dotProduct(centroid, centroid);
            final float[] scoresDefault = new float[ESNextOSQVectorsScorer.BULK_SIZE];
            final float[] scoresPanama = new float[ESNextOSQVectorsScorer.BULK_SIZE];
            try (IndexInput in = dir.openInput("testScore.bin", IOContext.DEFAULT)) {
                in.seek(padding);
                final int perVectorBytes = length + 16;
                assertEquals(in.length(), padding + (long) numVectors * perVectorBytes);
                // Work on a slice that has just the right number of bytes to make the test fail with an
                // index-out-of-bounds in case the implementation reads more than the allowed number of
                // padding bytes.
                for (int i = 0; i < numVectors; i += bulkSize) {
                    final IndexInput slice = in.slice("test", in.getFilePointer(), (long) perVectorBytes * bulkSize);
                    final var defaultScorer = defaultProvider().newESNextOSQVectorsScorer(
                        slice,
                        queryBits,
                        indexBits,
                        dimensions,
                        length,
                        ESNextOSQVectorsScorer.BULK_SIZE
                    );
                    final var panamaScorer = maybePanamaProvider().newESNextOSQVectorsScorer(
                        in,
                        queryBits,
                        indexBits,
                        dimensions,
                        length,
                        ESNextOSQVectorsScorer.BULK_SIZE
                    );
                    float defaultMaxScore = defaultScorer.scoreBulk(
                        quantizeQuery,
                        queryCorrections.lowerInterval(),
                        queryCorrections.upperInterval(),
                        queryCorrections.quantizedComponentSum(),
                        queryCorrections.additionalCorrection(),
                        similarityFunction,
                        centroidDp,
                        scoresDefault,
                        bulkSize
                    );
                    float panamaMaxScore = panamaScorer.scoreBulk(
                        quantizeQuery,
                        queryCorrections.lowerInterval(),
                        queryCorrections.upperInterval(),
                        queryCorrections.quantizedComponentSum(),
                        queryCorrections.additionalCorrection(),
                        similarityFunction,
                        centroidDp,
                        scoresPanama,
                        bulkSize
                    );
                    assertEquals(defaultMaxScore, panamaMaxScore, 1e-2f);
                    for (int j = 0; j < bulkSize; j++) {
                        assertEquals(scoresDefault[j], scoresPanama[j], 1e-2f);
                    }
                    assertEquals(((long) bulkSize * perVectorBytes), slice.getFilePointer());
                    assertEquals(padding + ((long) (i + bulkSize) * perVectorBytes), in.getFilePointer());
                }
            }
        }
    }

    private static void writeCorrections(OptimizedScalarQuantizer.QuantizationResult[] corrections, IndexOutput out) throws IOException {
        for (OptimizedScalarQuantizer.QuantizationResult correction : corrections) {
            out.writeInt(Float.floatToIntBits(correction.lowerInterval()));
        }
        for (OptimizedScalarQuantizer.QuantizationResult correction : corrections) {
            out.writeInt(Float.floatToIntBits(correction.upperInterval()));
        }
        for (OptimizedScalarQuantizer.QuantizationResult correction : corrections) {
            out.writeInt(correction.quantizedComponentSum());
        }
        for (OptimizedScalarQuantizer.QuantizationResult correction : corrections) {
            out.writeInt(Float.floatToIntBits(correction.additionalCorrection()));
        }
    }

    private void randomVector(float[] vector, VectorSimilarityFunction vectorSimilarityFunction) {
        for (int i = 0; i < vector.length; i++) {
            vector[i] = random().nextFloat();
        }
        if (vectorSimilarityFunction != VectorSimilarityFunction.EUCLIDEAN) {
            VectorUtil.l2normalize(vector);
        }
    }

    private Directory newParametrizedDirectory() throws IOException {
        return switch (directoryType) {
            case NIOFS -> new NIOFSDirectory(createTempDir());
            case MMAP -> new MMapDirectory(createTempDir());
        };
    }

    @ParametersFactory
    public static Iterable<Object[]> parametersFactory() {
        return () -> Stream.of((byte) 1, (byte) 2, (byte) 4)
            .flatMap(i -> Arrays.stream(DirectoryType.values()).map(f -> List.of(f, i)))
            .flatMap(p -> Arrays.stream(VectorSimilarityFunction.values()).map(f -> Stream.concat(p.stream(), Stream.of(f)).toArray()))
            .iterator();
    }
}
