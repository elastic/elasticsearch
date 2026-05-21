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
import org.elasticsearch.index.codec.vectors.diskbbq.es94.ES940DiskBBQVectorsFormat;
import org.elasticsearch.simdvec.BaseVectorizationTests;
import org.elasticsearch.simdvec.ES940OSQVectorsScorer;
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

public class ES940OSQVectorsScorerTests extends BaseVectorizationTests {

    private final DirectoryType directoryType;
    private final byte indexBits;
    private final byte queryBits;
    private final VectorSimilarityFunction similarityFunction;
    private final ES940OSQVectorsScorer.SymmetricInt4Encoding int4Encoding;

    public enum DirectoryType {
        NIOFS,
        MMAP,
        SNAP
    }

    public ES940OSQVectorsScorerTests(
        DirectoryType directoryType,
        byte indexBits,
        byte queryBits,
        ES940OSQVectorsScorer.SymmetricInt4Encoding int4Encoding,
        VectorSimilarityFunction similarityFunction
    ) {
        this.directoryType = directoryType;
        this.indexBits = indexBits;
        this.queryBits = queryBits;
        this.int4Encoding = int4Encoding;
        this.similarityFunction = similarityFunction;
    }

    private ES940OSQVectorsScorer.SymmetricInt4Encoding int4Encoding() {
        return int4Encoding;
    }

    private int docPackedLength(int dimensions) {
        if (indexBits == 4 && int4Encoding == ES940OSQVectorsScorer.SymmetricInt4Encoding.STRIPED) {
            int discretized = ES940DiskBBQVectorsFormat.QuantEncoding.fromBits(indexBits).discretizedDimensions(dimensions);
            return 4 * ((discretized + 7) / 8);
        }
        return ES940DiskBBQVectorsFormat.QuantEncoding.fromBits(indexBits).getDocPackedLength(dimensions);
    }

    private int queryPackedLength(int dimensions) {
        if (indexBits == 4 && int4Encoding == ES940OSQVectorsScorer.SymmetricInt4Encoding.STRIPED) {
            return docPackedLength(dimensions);
        }
        return ES940DiskBBQVectorsFormat.QuantEncoding.fromBits(indexBits).getQueryPackedLength(dimensions);
    }

    public void testQuantizeScore() throws Exception {

        final int dimensions = random().nextInt(1, 2000);
        final int numVectors = random().nextInt(1, 100);

        final int length = docPackedLength(dimensions);

        final byte[] vector = new byte[length];
        final int queryBytes = queryPackedLength(dimensions);

        try (Directory dir = newParametrizedDirectory()) {
            try (IndexOutput out = dir.createOutput("tests.bin", IOContext.DEFAULT)) {
                for (int i = 0; i < numVectors; i++) {
                    random().nextBytes(vector);
                    if (indexBits == 7) clampTo7Bit(vector, dimensions);
                    out.writeBytes(vector, 0, length);
                }
                CodecUtil.writeFooter(out);
            }
            final byte[] query = new byte[queryBytes];
            random().nextBytes(query);
            if (indexBits == 4 && int4Encoding == ES940OSQVectorsScorer.SymmetricInt4Encoding.PACKED_NIBBLE) {
                clampTo4Bit(query);
            }
            if (indexBits == 7) clampTo7Bit(query, dimensions);
            try (IndexInput in = dir.openInput("tests.bin", IOContext.DEFAULT)) {
                // Work on a slice that has just the right number of bytes to make the test fail with an
                // index-out-of-bounds in case the implementation reads more than the allowed number of
                // padding bytes.
                IndexInput slice = in.slice("test", 0, (long) length * numVectors);
                ES940OSQVectorsScorer defaultScorer = defaultProvider().getVectorScorerFactory()
                    .newES940OSQVectorsScorer(
                        slice,
                        queryBits,
                        indexBits,
                        dimensions,
                        length,
                        ES940OSQVectorsScorer.BULK_SIZE,
                        int4Encoding()
                    );
                ES940OSQVectorsScorer panamaScorer = panamaProvider().getVectorScorerFactory()
                    .newES940OSQVectorsScorer(
                        in.clone(),
                        queryBits,
                        indexBits,
                        dimensions,
                        length,
                        ES940OSQVectorsScorer.BULK_SIZE,
                        int4Encoding()
                    );
                ES940OSQVectorsScorer nativeScorer = nativeProvider().getVectorScorerFactory()
                    .newES940OSQVectorsScorer(
                        in,
                        queryBits,
                        indexBits,
                        dimensions,
                        length,
                        ES940OSQVectorsScorer.BULK_SIZE,
                        int4Encoding()
                    );
                for (int i = 0; i < numVectors; i++) {
                    long expectedScore = defaultScorer.quantizeScore(query);
                    assertEquals(expectedScore, panamaScorer.quantizeScore(query));
                    assertEquals(expectedScore, nativeScorer.quantizeScore(query));
                    assertEquals(in.getFilePointer(), slice.getFilePointer());
                }
                assertEquals((long) length * numVectors, slice.getFilePointer());
                assertEquals((long) length * numVectors, in.getFilePointer());
            }
        }
    }

    public void testScore() throws Exception {
        final int maxDims = random().nextInt(1, 1000) * 2;
        final int dimensions = random().nextInt(1, maxDims);
        final int numVectors = random().nextInt(10, 50);

        final int indexVectorPackedLengthInBytes = docPackedLength(dimensions);

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
                    var vectorData = createOSQIndexData(
                        vector,
                        centroid,
                        quantizer,
                        dimensions,
                        indexBits,
                        indexVectorPackedLengthInBytes,
                        int4Encoding
                    );
                    writeSingleOSQVectorData(out, vectorData);
                }
                CodecUtil.writeFooter(out);
            }
            final float[] query = new float[dimensions];
            randomVector(random(), query, similarityFunction);

            final int queryVectorPackedLengthInBytes = queryPackedLength(dimensions);
            var queryData = createOSQQueryData(
                query,
                centroid,
                quantizer,
                dimensions,
                queryBits,
                queryVectorPackedLengthInBytes,
                indexBits,
                int4Encoding
            );

            final float centroidDp = VectorUtil.dotProduct(centroid, centroid);
            final float[] floatScratch = new float[3];
            try (IndexInput in = dir.openInput("testScore.bin", IOContext.DEFAULT)) {
                in.seek(padding);
                final int perVectorBytes = indexVectorPackedLengthInBytes + 16;
                assertEquals(in.length(), padding + (long) numVectors * perVectorBytes + CodecUtil.footerLength());
                final IndexInput slice = in.slice("test", in.getFilePointer(), (long) perVectorBytes * numVectors);
                IndexInput panamaIn = in.clone();
                IndexInput nativeIn = in;
                // Work on a slice that has just the right number of bytes to make the test fail with an
                // index-out-of-bounds in case the implementation reads more than the allowed number of
                // padding bytes.
                for (int i = 0; i < numVectors; i++) {
                    final var defaultScorer = defaultProvider().getVectorScorerFactory()
                        .newES940OSQVectorsScorer(
                            slice,
                            queryBits,
                            indexBits,
                            dimensions,
                            indexVectorPackedLengthInBytes,
                            ES940OSQVectorsScorer.BULK_SIZE,
                            int4Encoding()
                        );
                    final var panamaScorer = panamaProvider().getVectorScorerFactory()
                        .newES940OSQVectorsScorer(
                            panamaIn,
                            queryBits,
                            indexBits,
                            dimensions,
                            indexVectorPackedLengthInBytes,
                            ES940OSQVectorsScorer.BULK_SIZE,
                            int4Encoding()
                        );
                    final var nativeScorer = nativeProvider().getVectorScorerFactory()
                        .newES940OSQVectorsScorer(
                            nativeIn,
                            queryBits,
                            indexBits,
                            dimensions,
                            indexVectorPackedLengthInBytes,
                            ES940OSQVectorsScorer.BULK_SIZE,
                            int4Encoding()
                        );
                    long qDist = defaultScorer.quantizeScore(queryData.quantizedVector());
                    slice.readFloats(floatScratch, 0, 3);
                    int quantizedComponentSum = slice.readInt();
                    float defaultScore = defaultScorer.applyCorrectionsIndividually(
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
                    panamaIn.readFloats(floatScratch, 0, 3);
                    quantizedComponentSum = panamaIn.readInt();
                    float panamaScore = panamaScorer.applyCorrectionsIndividually(
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

                    qDist = nativeScorer.quantizeScore(queryData.quantizedVector());
                    nativeIn.readFloats(floatScratch, 0, 3);
                    quantizedComponentSum = nativeIn.readInt();
                    float nativeScore = nativeScorer.applyCorrectionsIndividually(
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
                    assertEquals(defaultScore, nativeScore, 1e-2f);
                    assertEquals(padding + ((long) (i + 1) * perVectorBytes), nativeIn.getFilePointer());
                }
            }
        }
    }

    public void testScoreBulk() throws Exception {
        doTestScoreBulk(ES940OSQVectorsScorer.BULK_SIZE);
    }

    public void testScoreBulkNonAlignedBulkSize() throws Exception {
        // Pick a bulkSize that is NOT a multiple of 8, so that the tail path in
        // applyCorrectionsIndividually is exercised for both 128-bit (species length 4)
        // and 256-bit (species length 8) vector implementations.
        final int bulkSize = randomIntBetween(1, ES940OSQVectorsScorer.BULK_SIZE - 1) | 1; // ensure odd
        doTestScoreBulk(bulkSize);
    }

    private void doTestScoreBulk(int bulkSize) throws Exception {
        final int maxDims = random().nextInt(1, 1000) * 2;
        final int dimensions = random().nextInt(1, maxDims);
        final int numVectors = bulkSize * random().nextInt(1, 10);

        final int indexVectorPackedLengthInBytes = docPackedLength(dimensions);

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
                        vectors[j] = createOSQIndexData(
                            vector,
                            centroid,
                            quantizer,
                            dimensions,
                            indexBits,
                            indexVectorPackedLengthInBytes,
                            int4Encoding
                        );
                    }
                    writeBulkOSQVectorData(bulkSize, out, vectors);
                }
                CodecUtil.writeFooter(out);
            }
            final float[] query = new float[dimensions];
            randomVector(random(), query, similarityFunction);
            final int queryVectorPackedLengthInBytes = queryPackedLength(dimensions);
            var queryData = createOSQQueryData(
                query,
                centroid,
                quantizer,
                dimensions,
                queryBits,
                queryVectorPackedLengthInBytes,
                indexBits,
                int4Encoding
            );

            final float centroidDp = VectorUtil.dotProduct(centroid, centroid);
            final float[] scoresDefault = new float[ES940OSQVectorsScorer.BULK_SIZE];
            final float[] scoresPanama = new float[ES940OSQVectorsScorer.BULK_SIZE];
            final float[] scoresNative = new float[ES940OSQVectorsScorer.BULK_SIZE];
            try (IndexInput in = dir.openInput("testScore.bin", IOContext.DEFAULT)) {
                in.seek(padding);
                final int perVectorBytes = indexVectorPackedLengthInBytes + 16;
                assertEquals(in.length(), padding + (long) numVectors * perVectorBytes + CodecUtil.footerLength());
                // Work on a slice that has just the right number of bytes to make the test fail with an
                // index-out-of-bounds in case the implementation reads more than the allowed number of
                // padding bytes.
                for (int i = 0; i < numVectors; i += bulkSize) {
                    final IndexInput slice = in.slice("test", in.getFilePointer(), (long) perVectorBytes * bulkSize);
                    final var defaultScorer = defaultProvider().getVectorScorerFactory()
                        .newES940OSQVectorsScorer(
                            slice,
                            queryBits,
                            indexBits,
                            dimensions,
                            indexVectorPackedLengthInBytes,
                            ES940OSQVectorsScorer.BULK_SIZE,
                            int4Encoding()
                        );
                    final var panamaScorer = panamaProvider().getVectorScorerFactory()
                        .newES940OSQVectorsScorer(
                            in.clone(),
                            queryBits,
                            indexBits,
                            dimensions,
                            indexVectorPackedLengthInBytes,
                            ES940OSQVectorsScorer.BULK_SIZE,
                            int4Encoding()
                        );
                    final var nativeScorer = nativeProvider().getVectorScorerFactory()
                        .newES940OSQVectorsScorer(
                            in,
                            queryBits,
                            indexBits,
                            dimensions,
                            indexVectorPackedLengthInBytes,
                            ES940OSQVectorsScorer.BULK_SIZE,
                            int4Encoding()
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
                    float nativeMaxScore = nativeScorer.scoreBulk(
                        queryData.quantizedVector(),
                        queryData.lowerInterval(),
                        queryData.upperInterval(),
                        queryData.quantizedComponentSum(),
                        queryData.additionalCorrection(),
                        similarityFunction,
                        centroidDp,
                        scoresNative,
                        bulkSize
                    );
                    assertEquals(defaultMaxScore, nativeMaxScore, 1e-2f);
                    assertArrayEqualsPercent(scoresDefault, scoresNative, 0.05f, 1e-2f);

                    assertEquals(((long) bulkSize * perVectorBytes), slice.getFilePointer());
                    assertEquals(padding + ((long) (i + bulkSize) * perVectorBytes), in.getFilePointer());
                }
            }
        }
    }

    public void testScoreBulkOffsets() throws Exception {
        final int bulkSize = ES940OSQVectorsScorer.BULK_SIZE;
        int filtered = random().nextInt(0, bulkSize);
        final int[] offsets = VectorScorerTestUtils.generateFilteredOffsets(random(), bulkSize, filtered);
        var offsetsCount = bulkSize - filtered;
        doTestScoreBulkOffsets(offsets, offsetsCount, bulkSize);
    }

    public void testScoreBulkOffsetsOneVector() throws Exception {
        final int bulkSize = ES940OSQVectorsScorer.BULK_SIZE;
        int filtered = bulkSize - 1;
        final int[] offsets = VectorScorerTestUtils.generateFilteredOffsets(random(), bulkSize, filtered);
        assert offsets.length == 1;
        doTestScoreBulkOffsets(offsets, 1, bulkSize);
    }

    public void testScoreBulkOffsetsAllVectors() throws Exception {
        final int bulkSize = ES940OSQVectorsScorer.BULK_SIZE;
        int filtered = 0;
        final int[] offsets = VectorScorerTestUtils.generateFilteredOffsets(random(), bulkSize, filtered);
        assert offsets.length == bulkSize;
        doTestScoreBulkOffsets(offsets, bulkSize, bulkSize);
    }

    public void testScoreBulkOffsetsAllVectorsButOne() throws Exception {
        final int bulkSize = ES940OSQVectorsScorer.BULK_SIZE;
        int filtered = 1;
        final int[] offsets = VectorScorerTestUtils.generateFilteredOffsets(random(), bulkSize, filtered);
        assert offsets.length == bulkSize - 1;
        doTestScoreBulkOffsets(offsets, bulkSize - 1, bulkSize);
    }

    public void testScoreBulkOffsetsTail() throws Exception {
        final int bulkSize = ES940OSQVectorsScorer.BULK_SIZE;
        int tailSize = random().nextInt(1, bulkSize);
        int filtered = random().nextInt(0, tailSize);
        final int[] offsets = VectorScorerTestUtils.generateFilteredOffsets(random(), tailSize, filtered);
        var offsetsCount = tailSize - filtered;
        doTestScoreBulkOffsets(offsets, offsetsCount, tailSize);
    }

    private void doTestScoreBulkOffsets(int[] offsets, int offsetsCount, int count) throws Exception {
        final int bulkSize = ES940OSQVectorsScorer.BULK_SIZE;
        final int maxDims = random().nextInt(1, 1000) * 2;
        final int dimensions = random().nextInt(1, maxDims);
        final int numVectors = count * random().nextInt(1, 10);

        final int indexVectorPackedLengthInBytes = docPackedLength(dimensions);

        final float[] centroid = new float[dimensions];
        randomVector(random(), centroid, similarityFunction);

        OptimizedScalarQuantizer quantizer = new OptimizedScalarQuantizer(similarityFunction);
        int padding = random().nextInt(100);
        byte[] paddingBytes = new byte[padding];
        try (Directory dir = newParametrizedDirectory()) {
            try (IndexOutput out = dir.createOutput("testScore.bin", IOContext.DEFAULT)) {
                random().nextBytes(paddingBytes);
                out.writeBytes(paddingBytes, 0, padding);

                var vectors = new VectorScorerTestUtils.OSQVectorData[count];

                for (int i = 0; i < numVectors; i += count) {
                    for (int j = 0; j < count; j++) {
                        var vector = new float[dimensions];
                        randomVector(random(), vector, similarityFunction);
                        vectors[j] = createOSQIndexData(
                            vector,
                            centroid,
                            quantizer,
                            dimensions,
                            indexBits,
                            indexVectorPackedLengthInBytes,
                            int4Encoding
                        );
                    }
                    writeBulkOSQVectorData(count, out, vectors);
                }
                CodecUtil.writeFooter(out);
            }
            final float[] query = new float[dimensions];
            randomVector(random(), query, similarityFunction);
            final int queryVectorPackedLengthInBytes = queryPackedLength(dimensions);
            var queryData = createOSQQueryData(
                query,
                centroid,
                quantizer,
                dimensions,
                queryBits,
                queryVectorPackedLengthInBytes,
                indexBits,
                int4Encoding
            );

            final float centroidDp = VectorUtil.dotProduct(centroid, centroid);

            final float[] scoresDefault = new float[bulkSize];
            final float[] scoresPanama = new float[bulkSize];
            final float[] scoresNative = new float[bulkSize];
            try (IndexInput in = dir.openInput("testScore.bin", IOContext.DEFAULT)) {
                in.seek(padding);
                final int perVectorBytes = indexVectorPackedLengthInBytes + 16;
                assertEquals(in.length(), padding + (long) numVectors * perVectorBytes + CodecUtil.footerLength());
                // Work on a slice that has just the right number of bytes to make the test fail with an
                // index-out-of-bounds in case the implementation reads more than the allowed number of
                // padding bytes.
                for (int i = 0; i < numVectors; i += count) {
                    final IndexInput slice = in.slice("test", in.getFilePointer(), (long) perVectorBytes * count);
                    final var defaultScorer = defaultProvider().getVectorScorerFactory()
                        .newES940OSQVectorsScorer(
                            slice,
                            queryBits,
                            indexBits,
                            dimensions,
                            indexVectorPackedLengthInBytes,
                            bulkSize,
                            int4Encoding()
                        );
                    final var panamaScorer = panamaProvider().getVectorScorerFactory()
                        .newES940OSQVectorsScorer(
                            in.clone(),
                            queryBits,
                            indexBits,
                            dimensions,
                            indexVectorPackedLengthInBytes,
                            bulkSize,
                            int4Encoding()
                        );
                    final var nativeScorer = nativeProvider().getVectorScorerFactory()
                        .newES940OSQVectorsScorer(
                            in,
                            queryBits,
                            indexBits,
                            dimensions,
                            indexVectorPackedLengthInBytes,
                            bulkSize,
                            int4Encoding()
                        );
                    float defaultMaxScore = defaultScorer.scoreBulkOffsets(
                        queryData.quantizedVector(),
                        queryData.lowerInterval(),
                        queryData.upperInterval(),
                        queryData.quantizedComponentSum(),
                        queryData.additionalCorrection(),
                        similarityFunction,
                        centroidDp,
                        offsets,
                        offsetsCount,
                        scoresDefault,
                        count
                    );
                    float panamaMaxScore = panamaScorer.scoreBulkOffsets(
                        queryData.quantizedVector(),
                        queryData.lowerInterval(),
                        queryData.upperInterval(),
                        queryData.quantizedComponentSum(),
                        queryData.additionalCorrection(),
                        similarityFunction,
                        centroidDp,
                        offsets,
                        offsetsCount,
                        scoresPanama,
                        count
                    );
                    assertEquals(defaultMaxScore, panamaMaxScore, 1e-2f);
                    assertArrayEqualsPercent(Arrays.copyOf(scoresDefault, count), Arrays.copyOf(scoresPanama, count), 0.05f, 1e-2f);
                    float nativeMaxScore = nativeScorer.scoreBulkOffsets(
                        queryData.quantizedVector(),
                        queryData.lowerInterval(),
                        queryData.upperInterval(),
                        queryData.quantizedComponentSum(),
                        queryData.additionalCorrection(),
                        similarityFunction,
                        centroidDp,
                        offsets,
                        offsetsCount,
                        scoresNative,
                        count
                    );
                    assertEquals(defaultMaxScore, nativeMaxScore, 1e-2f);
                    assertArrayEqualsPercent(Arrays.copyOf(scoresDefault, count), Arrays.copyOf(scoresNative, count), 0.05f, 1e-2f);

                    assertEquals(((long) count * perVectorBytes), slice.getFilePointer());
                    assertEquals(padding + ((long) (i + count) * perVectorBytes), in.getFilePointer());

                    assertFilteredNotScored(count, offsets, scoresDefault);
                }
            }
        }
    }

    private static void assertFilteredNotScored(int count, int[] offsets, float[] scoresDefault) {
        for (int j = 0; j < count; j++) {
            if (Arrays.binarySearch(offsets, j) < 0) {
                assertEquals(0.0f, scoresDefault[j], 0.0f);
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
        final int bulkSize = ES940OSQVectorsScorer.BULK_SIZE;

        final int length = docPackedLength(dimensions);
        final int queryBytes = queryPackedLength(dimensions);

        try (Directory dir = newParametrizedDirectory()) {
            try (IndexOutput out = dir.createOutput("testNegInf.bin", IOContext.DEFAULT)) {
                byte[] vector = new byte[length];
                for (int i = 0; i < bulkSize; i++) {
                    random().nextBytes(vector);
                    if (indexBits == 7) clampTo7Bit(vector, dimensions);
                    out.writeBytes(vector, 0, length);
                }
                // All-zero corrections: zero bytes are interpreted identically regardless of byte order
                byte[] zeroCorrections = new byte[16 * bulkSize];
                out.writeBytes(zeroCorrections, 0, zeroCorrections.length);
                CodecUtil.writeFooter(out);
            }

            byte[] query = new byte[queryBytes];
            random().nextBytes(query);
            if (indexBits == 4 && int4Encoding == ES940OSQVectorsScorer.SymmetricInt4Encoding.PACKED_NIBBLE) {
                clampTo4Bit(query);
            }
            if (indexBits == 7) clampTo7Bit(query, dimensions);

            float[] scoresDefault = new float[bulkSize];
            float[] scoresPanama = new float[bulkSize];
            float[] scoresNative = new float[bulkSize];

            try (IndexInput in = dir.openInput("testNegInf.bin", IOContext.DEFAULT)) {
                final long dataLength = (long) bulkSize * length + 16L * bulkSize;
                final IndexInput slice = in.slice("test", 0, dataLength);
                final var defaultScorer = defaultProvider().getVectorScorerFactory()
                    .newES940OSQVectorsScorer(slice, queryBits, indexBits, dimensions, length, bulkSize, int4Encoding());
                final var panamaScorer = panamaProvider().getVectorScorerFactory()
                    .newES940OSQVectorsScorer(in.clone(), queryBits, indexBits, dimensions, length, bulkSize, int4Encoding());
                final var nativeScorer = panamaProvider().getVectorScorerFactory()
                    .newES940OSQVectorsScorer(in, queryBits, indexBits, dimensions, length, bulkSize, int4Encoding());

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

                float nativeMaxScore = nativeScorer.scoreBulk(
                    query,
                    0f,
                    0f,
                    0,
                    Float.NEGATIVE_INFINITY,
                    similarityFunction,
                    0f,
                    scoresNative
                );
                assertEquals(defaultMaxScore, nativeMaxScore, 1e-2f);
                for (int j = 0; j < bulkSize; j++) {
                    assertEquals("score mismatch at index " + j, scoresDefault[j], scoresNative[j], 1e-2f);
                }

                assertEquals(dataLength, slice.getFilePointer());
                assertEquals(dataLength, in.getFilePointer());
            }
        }
    }

    private static void clampTo4Bit(byte[] vector) {
        for (int i = 0; i < vector.length; i++) {
            vector[i] = (byte) (vector[i] & 0x0F);
        }
    }

    private static void clampTo7Bit(byte[] vector, int dimensions) {
        for (int i = 0; i < dimensions; i++) {
            vector[i] = (byte) (vector[i] & 0x7F);
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
        var bitCombinations = List.of(
            List.of((byte) 1, (byte) 4, ES940OSQVectorsScorer.SymmetricInt4Encoding.STRIPED),
            List.of((byte) 2, (byte) 4, ES940OSQVectorsScorer.SymmetricInt4Encoding.STRIPED),
            List.of((byte) 4, (byte) 4, ES940OSQVectorsScorer.SymmetricInt4Encoding.STRIPED),
            List.of((byte) 4, (byte) 4, ES940OSQVectorsScorer.SymmetricInt4Encoding.PACKED_NIBBLE),
            List.of((byte) 7, (byte) 7, ES940OSQVectorsScorer.SymmetricInt4Encoding.STRIPED)
        );
        return () -> bitCombinations.stream()
            .flatMap(bits -> Arrays.stream(DirectoryType.values()).map(d -> List.of(d, bits.get(0), bits.get(1), bits.get(2))))
            .flatMap(p -> Arrays.stream(VectorSimilarityFunction.values()).map(f -> Stream.concat(p.stream(), Stream.of(f)).toArray()))
            .iterator();
    }
}
