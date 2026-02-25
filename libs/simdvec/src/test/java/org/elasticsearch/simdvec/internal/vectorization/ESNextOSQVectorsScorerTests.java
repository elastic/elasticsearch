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

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.store.NIOFSDirectory;
import org.apache.lucene.util.VectorUtil;
import org.elasticsearch.index.codec.vectors.OptimizedScalarQuantizer;
import org.elasticsearch.index.codec.vectors.diskbbq.next.ESNextDiskBBQVectorsFormat;
import org.elasticsearch.simdvec.ESNextOSQVectorsScorer;
import org.elasticsearch.xpack.searchablesnapshots.store.SearchableSnapshotDirectoryFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

import static org.elasticsearch.simdvec.internal.vectorization.VectorScorerTestUtils.createOSQIndexData;
import static org.elasticsearch.simdvec.internal.vectorization.VectorScorerTestUtils.createOSQQueryData;
import static org.elasticsearch.simdvec.internal.vectorization.VectorScorerTestUtils.randomVector;
import static org.elasticsearch.simdvec.internal.vectorization.VectorScorerTestUtils.writeBulkOSQVectorData;
import static org.elasticsearch.simdvec.internal.vectorization.VectorScorerTestUtils.writeSingleOSQVectorData;

public class ESNextOSQVectorsScorerTests extends BaseVectorizationTests {

    private final DirectoryType directoryType;
    private final byte indexBits;
    private final VectorSimilarityFunction similarityFunction;
    private static final byte queryBits = 4;

    public enum DirectoryType {
        NIOFS,
        MMAP,
        SNAP
    }

    public ESNextOSQVectorsScorerTests(DirectoryType directoryType, byte indexBits, VectorSimilarityFunction similarityFunction) {
        this.directoryType = directoryType;
        this.indexBits = indexBits;
        this.similarityFunction = similarityFunction;
    }

    public void testQuantizeScore() throws Exception {

        final int dimensions = random().nextInt(1, 2000);
        final int numVectors = random().nextInt(1, 100);

        final int length = ESNextDiskBBQVectorsFormat.QuantEncoding.fromBits(indexBits).getDocPackedLength(dimensions);

        final byte[] vector = new byte[length];

        final int queryBytes = length * (queryBits / indexBits);

        try (Directory dir = newParametrizedDirectory()) {
            try (IndexOutput out = dir.createOutput("tests.bin", IOContext.DEFAULT)) {
                for (int i = 0; i < numVectors; i++) {
                    random().nextBytes(vector);
                    out.writeBytes(vector, 0, length);
                }
                CodecUtil.writeFooter(out);
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
        final int numVectors = random().nextInt(10, 50);

        final int indexVectorPackedLengthInBytes = ESNextDiskBBQVectorsFormat.QuantEncoding.fromBits(indexBits)
            .getDocPackedLength(dimensions);

        final float[] centroid = new float[dimensions];
        randomVector(random(), centroid, similarityFunction);
        OptimizedScalarQuantizer quantizer = new OptimizedScalarQuantizer(similarityFunction);
        int padding = random().nextInt(100);
        byte[] paddingBytes = new byte[padding];
        try (Directory dir = newParametrizedDirectory()) {
            try (IndexOutput out = dir.createOutput("testScore.bin", IOContext.DEFAULT)) {
                random().nextBytes(paddingBytes);
                out.writeBytes(paddingBytes, 0, padding);

                float[] vector = new float[dimensions];
                for (int i = 0; i < numVectors; i++) {
                    randomVector(random(), vector, similarityFunction);
                    var vectorData = createOSQIndexData(vector, centroid, quantizer, dimensions, indexBits, indexVectorPackedLengthInBytes);
                    writeSingleOSQVectorData(out, vectorData);
                }
                CodecUtil.writeFooter(out);
            }
            final float[] query = new float[dimensions];
            randomVector(random(), query, similarityFunction);

            final int queryVectorPackedLengthInBytes = indexVectorPackedLengthInBytes * (queryBits / indexBits);
            var queryData = createOSQQueryData(query, centroid, quantizer, dimensions, queryBits, queryVectorPackedLengthInBytes);

            final float centroidDp = VectorUtil.dotProduct(centroid, centroid);
            final float[] floatScratch = new float[3];
            try (IndexInput in = dir.openInput("testScore.bin", IOContext.DEFAULT)) {
                in.seek(padding);
                final int perVectorBytes = indexVectorPackedLengthInBytes + 16;
                assertEquals(in.length(), padding + (long) numVectors * perVectorBytes + CodecUtil.footerLength());
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
                        indexVectorPackedLengthInBytes,
                        ESNextOSQVectorsScorer.BULK_SIZE
                    );
                    final var panamaScorer = maybePanamaProvider().newESNextOSQVectorsScorer(
                        in,
                        queryBits,
                        indexBits,
                        dimensions,
                        indexVectorPackedLengthInBytes,
                        ESNextOSQVectorsScorer.BULK_SIZE
                    );
                    long qDist = defaultScorer.quantizeScore(queryData.quantizedVector());
                    slice.readFloats(floatScratch, 0, 3);
                    int quantizedComponentSum = slice.readInt();
                    float defaultScore = defaultScorer.score(
                        queryData.lowerInterval(),
                        queryData.upperInterval(),
                        queryData.quantizedComponentSum(),
                        queryData.additionalCorrection(),
                        similarityFunction,
                        centroidDp,
                        floatScratch[0],
                        floatScratch[1],
                        quantizedComponentSum,
                        floatScratch[2],
                        qDist
                    );
                    qDist = panamaScorer.quantizeScore(queryData.quantizedVector());
                    in.readFloats(floatScratch, 0, 3);
                    quantizedComponentSum = in.readInt();
                    float panamaScore = panamaScorer.score(
                        queryData.lowerInterval(),
                        queryData.upperInterval(),
                        queryData.quantizedComponentSum(),
                        queryData.additionalCorrection(),
                        similarityFunction,
                        centroidDp,
                        floatScratch[0],
                        floatScratch[1],
                        quantizedComponentSum,
                        floatScratch[2],
                        qDist
                    );
                    assertEquals(defaultScore, panamaScore, 1e-2f);
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
        final int numVectors = bulkSize * random().nextInt(1, 10);

        final int indexVectorPackedLengthInBytes = ESNextDiskBBQVectorsFormat.QuantEncoding.fromBits(indexBits)
            .getDocPackedLength(dimensions);

        final float[] centroid = new float[dimensions];
        randomVector(random(), centroid, similarityFunction);

        OptimizedScalarQuantizer quantizer = new OptimizedScalarQuantizer(similarityFunction);
        int padding = random().nextInt(100);
        byte[] paddingBytes = new byte[padding];
        try (Directory dir = newParametrizedDirectory()) {
            try (IndexOutput out = dir.createOutput("testScore.bin", IOContext.DEFAULT)) {
                random().nextBytes(paddingBytes);
                out.writeBytes(paddingBytes, 0, padding);

                var vectors = new VectorScorerTestUtils.OSQVectorData[bulkSize];

                for (int i = 0; i < numVectors; i += bulkSize) {
                    for (int j = 0; j < bulkSize; j++) {
                        var vector = new float[dimensions];
                        randomVector(random(), vector, similarityFunction);
                        vectors[j] = createOSQIndexData(vector, centroid, quantizer, dimensions, indexBits, indexVectorPackedLengthInBytes);
                    }
                    writeBulkOSQVectorData(bulkSize, out, vectors);
                }
                CodecUtil.writeFooter(out);
            }
            final float[] query = new float[dimensions];
            randomVector(random(), query, similarityFunction);
            final int queryVectorPackedLengthInBytes = indexVectorPackedLengthInBytes * (queryBits / indexBits);
            var queryData = createOSQQueryData(query, centroid, quantizer, dimensions, queryBits, queryVectorPackedLengthInBytes);

            final float centroidDp = VectorUtil.dotProduct(centroid, centroid);
            final float[] scoresDefault = new float[ESNextOSQVectorsScorer.BULK_SIZE];
            final float[] scoresPanama = new float[ESNextOSQVectorsScorer.BULK_SIZE];
            try (IndexInput in = dir.openInput("testScore.bin", IOContext.DEFAULT)) {
                in.seek(padding);
                final int perVectorBytes = indexVectorPackedLengthInBytes + 16;
                assertEquals(in.length(), padding + (long) numVectors * perVectorBytes + CodecUtil.footerLength());
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
                        indexVectorPackedLengthInBytes,
                        ESNextOSQVectorsScorer.BULK_SIZE
                    );
                    final var panamaScorer = maybePanamaProvider().newESNextOSQVectorsScorer(
                        in,
                        queryBits,
                        indexBits,
                        dimensions,
                        indexVectorPackedLengthInBytes,
                        ESNextOSQVectorsScorer.BULK_SIZE
                    );
                    float defaultMaxScore = defaultScorer.scoreBulk(
                        queryData.quantizedVector(),
                        queryData.lowerInterval(),
                        queryData.upperInterval(),
                        queryData.quantizedComponentSum(),
                        queryData.additionalCorrection(),
                        similarityFunction,
                        centroidDp,
                        scoresDefault,
                        bulkSize
                    );
                    float panamaMaxScore = panamaScorer.scoreBulk(
                        queryData.quantizedVector(),
                        queryData.lowerInterval(),
                        queryData.upperInterval(),
                        queryData.quantizedComponentSum(),
                        queryData.additionalCorrection(),
                        similarityFunction,
                        centroidDp,
                        scoresPanama,
                        bulkSize
                    );
                    assertEquals(defaultMaxScore, panamaMaxScore, 1e-2f);
                    assertArrayEqualsPercent(scoresDefault, scoresPanama, 0.05f, 1e-2f);
                    assertEquals(((long) bulkSize * perVectorBytes), slice.getFilePointer());
                    assertEquals(padding + ((long) (i + bulkSize) * perVectorBytes), in.getFilePointer());
                }
            }
        }
    }

    /**
     * Regression test: verifies that the vectorized scorer correctly handles -Infinity raw scores
     * for MAXIMUM_INNER_PRODUCT. Passing Float.NEGATIVE_INFINITY as queryAdditionalCorrection
     * (with all-zero corrections) forces every element's raw score to -Infinity before
     * scaleMaxInnerProductScore is applied. The correct result is 0.0 for all elements.
     * <p>
     * This catches the AVX-512 bug where {@code _mm512_fpclass_ps_mask(res, 0x40)} (Negative Finite)
     * failed to classify -Infinity as negative, causing the positive branch ({@code 1 + res = -Infinity})
     * to be used instead of the negative branch ({@code 1/(1 - res) = 0}).
     */
    public void testScoreBulkWithNegativeInfinityScore() throws Exception {
        final int dimensions = 768;
        final int bulkSize = ESNextOSQVectorsScorer.BULK_SIZE;

        final int length = ESNextDiskBBQVectorsFormat.QuantEncoding.fromBits(indexBits).getDocPackedLength(dimensions);
        final int queryBytes = length * (queryBits / indexBits);

        try (Directory dir = newParametrizedDirectory()) {
            try (IndexOutput out = dir.createOutput("testNegInf.bin", IOContext.DEFAULT)) {
                byte[] vector = new byte[length];
                for (int i = 0; i < bulkSize; i++) {
                    random().nextBytes(vector);
                    out.writeBytes(vector, 0, length);
                }
                // All-zero corrections: zero bytes are interpreted identically regardless of byte order
                byte[] zeroCorrections = new byte[16 * bulkSize];
                out.writeBytes(zeroCorrections, 0, zeroCorrections.length);
                CodecUtil.writeFooter(out);
            }

            byte[] query = new byte[queryBytes];
            random().nextBytes(query);

            float[] scoresDefault = new float[bulkSize];
            float[] scoresPanama = new float[bulkSize];

            try (IndexInput in = dir.openInput("testNegInf.bin", IOContext.DEFAULT)) {
                final long dataLength = (long) bulkSize * length + 16L * bulkSize;
                final IndexInput slice = in.slice("test", 0, dataLength);
                final var defaultScorer = defaultProvider().newESNextOSQVectorsScorer(
                    slice,
                    queryBits,
                    indexBits,
                    dimensions,
                    length,
                    bulkSize
                );
                final var panamaScorer = maybePanamaProvider().newESNextOSQVectorsScorer(
                    in,
                    queryBits,
                    indexBits,
                    dimensions,
                    length,
                    bulkSize
                );

                // Pass Float.NEGATIVE_INFINITY as queryAdditionalCorrection.
                // With all-zero corrections and zero query intervals, the base score is zero,
                // and adding -Infinity makes every element's total raw score -Infinity.
                float defaultMaxScore = defaultScorer.scoreBulk(
                    query,
                    0f,
                    0f,
                    0,
                    Float.NEGATIVE_INFINITY,
                    similarityFunction,
                    0f,
                    scoresDefault
                );
                float panamaMaxScore = panamaScorer.scoreBulk(
                    query,
                    0f,
                    0f,
                    0,
                    Float.NEGATIVE_INFINITY,
                    similarityFunction,
                    0f,
                    scoresPanama
                );

                assertEquals(defaultMaxScore, panamaMaxScore, 1e-2f);
                for (int j = 0; j < bulkSize; j++) {
                    assertEquals("score mismatch at index " + j, scoresDefault[j], scoresPanama[j], 1e-2f);
                }
                assertEquals(dataLength, slice.getFilePointer());
                assertEquals(dataLength, in.getFilePointer());
            }
        }
    }

    private Directory newParametrizedDirectory() throws IOException {
        return switch (directoryType) {
            case NIOFS -> new NIOFSDirectory(createTempDir());
            case MMAP -> new MMapDirectory(createTempDir());
            case SNAP -> SearchableSnapshotDirectoryFactory.newDirectory(createTempDir());
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
