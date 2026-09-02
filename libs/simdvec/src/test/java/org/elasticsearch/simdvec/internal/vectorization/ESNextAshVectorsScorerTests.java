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
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.store.NIOFSDirectory;
import org.elasticsearch.simdvec.AshScorer;
import org.elasticsearch.simdvec.BaseVectorizationTests;
import org.elasticsearch.xpack.searchablesnapshots.store.SearchableSnapshotDirectoryFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static org.elasticsearch.simdvec.ES940OSQVectorsScorer.BULK_SIZE;

/**
 * Tests for {@link org.elasticsearch.simdvec.ESNextAshVectorsScorer} and its Panama/native-accelerated subclasses.
 * <p>
 * For each (DirectoryType, bitsPerDim, queryBitsPerDim) combination, writes random
 * packed ASH data to an IndexInput, creates scorers via {@link org.elasticsearch.simdvec.VectorScorerFactory},
 * and asserts that the scalar, Panama, and (for integer path) native tiers produce matching results.
 */
public class ESNextAshVectorsScorerTests extends BaseVectorizationTests {

    private final DirectoryType directoryType;
    private final int bitsPerDim;
    private final int queryBitsPerDim;

    public enum DirectoryType {
        NIOFS,
        MMAP,
        SNAP
    }

    public ESNextAshVectorsScorerTests(DirectoryType directoryType, int bitsPerDim, int queryBitsPerDim) {
        this.directoryType = directoryType;
        this.bitsPerDim = bitsPerDim;
        this.queryBitsPerDim = queryBitsPerDim;
    }

    @ParametersFactory
    public static Iterable<Object[]> parametersFactory() {
        // (bitsPerDim, queryBitsPerDim): float path (queryBitsPerDim==0) and integer path
        // Also include small nDims cases (below 128 bits / 16 planeBytes) to exercise fall-through to scalar
        var bitCombinations = List.of(
            List.of(1, 0),  // float path
            List.of(1, 4),  // original integer combinations
            List.of(2, 4),
            List.of(4, 4),  // newly accelerated
            List.of(3, 4),  // Panama only (no native)
            List.of(8, 4),  // Panama only (no native)
            List.of(1, 1),  // queryBitsPerDim == 1
            List.of(2, 1)   // queryBitsPerDim == 1 with multi-plane doc
        );
        return () -> bitCombinations.stream()
            .flatMap(bits -> Arrays.stream(DirectoryType.values()).map(d -> new Object[] { d, bits.get(0), bits.get(1) }))
            .iterator();
    }

    /**
     * Tests that the Panama scorer produces the same raw dot products as the scalar scorer
     * for a bulk of random vectors.
     */
    public void testScoreBulk() throws Exception {
        int nDims = randomIntBetween(16, 512);
        int planeBytes = (nDims + 7) / 8;
        int packedCodeBytes = bitsPerDim * planeBytes;
        int numVectors = BULK_SIZE * randomIntBetween(1, 4);

        byte[][] packedCodes = new byte[numVectors][packedCodeBytes];
        for (int i = 0; i < numVectors; i++) {
            random().nextBytes(packedCodes[i]);
        }

        float[] queryTransformed = new float[nDims];
        for (int j = 0; j < nDims; j++) {
            queryTransformed[j] = (float) random().nextGaussian();
        }

        byte[] queryQuantized = null;
        if (queryBitsPerDim > 0) {
            queryQuantized = new byte[queryBitsPerDim * planeBytes];
            random().nextBytes(queryQuantized);
        }

        try (Directory dir = newParametrizedDirectory()) {
            try (IndexOutput out = dir.createOutput("test_ash.bin", IOContext.DEFAULT)) {
                for (int i = 0; i < numVectors; i++) {
                    out.writeBytes(packedCodes[i], 0, packedCodeBytes);
                }
                CodecUtil.writeFooter(out);
            }

            try (IndexInput in = dir.openInput("test_ash.bin", IOContext.DEFAULT)) {
                long dataLength = (long) packedCodeBytes * numVectors;

                float[] scalarScores = new float[BULK_SIZE];
                float[] panamaScores = new float[BULK_SIZE];
                float[] nativeScores = new float[BULK_SIZE];

                if (queryBitsPerDim > 0) {
                    // Integer path: compare scalar, Panama, and native
                    IndexInput scalarSlice = in.slice("scalar", 0, dataLength);
                    AshScorer<byte[]> scalarScorer = defaultProvider().getVectorScorerFactory()
                        .newESNextAshIntegerVectorsScorer(scalarSlice, nDims, bitsPerDim, queryBitsPerDim);
                    IndexInput panamaInput = in.clone();
                    AshScorer<byte[]> panamaScorer = panamaProvider().getVectorScorerFactory()
                        .newESNextAshIntegerVectorsScorer(panamaInput, nDims, bitsPerDim, queryBitsPerDim);
                    IndexInput nativeInput = in.clone();
                    AshScorer<byte[]> nativeScorer = nativeProvider().getVectorScorerFactory()
                        .newESNextAshIntegerVectorsScorer(nativeInput, nDims, bitsPerDim, queryBitsPerDim);

                    for (int offset = 0; offset < numVectors; offset += BULK_SIZE) {
                        int blockSize = Math.min(BULK_SIZE, numVectors - offset);
                        scalarScorer.scoreBulk(queryQuantized, scalarScores, blockSize);
                        panamaScorer.scoreBulk(queryQuantized, panamaScores, blockSize);
                        nativeScorer.scoreBulk(queryQuantized, nativeScores, blockSize);
                        assertScoresMatch("scalar vs panama", scalarScores, panamaScores, blockSize, offset);
                        assertScoresMatch("scalar vs native", scalarScores, nativeScores, blockSize, offset);
                    }
                    assertEquals(dataLength, scalarSlice.getFilePointer());
                    assertEquals(dataLength, panamaInput.getFilePointer());
                    assertEquals(dataLength, nativeInput.getFilePointer());
                } else {
                    // Float path: compare scalar and Panama only
                    IndexInput scalarSlice = in.slice("scalar", 0, dataLength);
                    AshScorer<float[]> scalarScorer = defaultProvider().getVectorScorerFactory()
                        .newESNextAshFloatVectorsScorer(scalarSlice, nDims, bitsPerDim);
                    IndexInput panamaInput = in.clone();
                    AshScorer<float[]> panamaScorer = panamaProvider().getVectorScorerFactory()
                        .newESNextAshFloatVectorsScorer(panamaInput, nDims, bitsPerDim);

                    for (int offset = 0; offset < numVectors; offset += BULK_SIZE) {
                        int blockSize = Math.min(BULK_SIZE, numVectors - offset);
                        scalarScorer.scoreBulk(queryTransformed, scalarScores, blockSize);
                        panamaScorer.scoreBulk(queryTransformed, panamaScores, blockSize);
                        assertScoresMatch("scalar vs panama", scalarScores, panamaScores, blockSize, offset);
                    }
                    assertEquals(dataLength, scalarSlice.getFilePointer());
                    assertEquals(dataLength, panamaInput.getFilePointer());
                }
            }
        }
    }

    /**
     * Tests scoring a tail block (smaller than BULK_SIZE) to exercise non-aligned paths.
     */
    public void testScoreBulkTail() throws Exception {
        int nDims = randomIntBetween(16, 256);
        int planeBytes = (nDims + 7) / 8;
        int packedCodeBytes = bitsPerDim * planeBytes;
        int tailSize = randomIntBetween(1, BULK_SIZE - 1);

        byte[][] packedCodes = new byte[tailSize][packedCodeBytes];
        for (int i = 0; i < tailSize; i++) {
            random().nextBytes(packedCodes[i]);
        }

        float[] queryTransformed = new float[nDims];
        for (int j = 0; j < nDims; j++) {
            queryTransformed[j] = (float) random().nextGaussian();
        }
        byte[] queryQuantized = null;
        if (queryBitsPerDim > 0) {
            queryQuantized = new byte[queryBitsPerDim * planeBytes];
            random().nextBytes(queryQuantized);
        }

        try (Directory dir = newParametrizedDirectory()) {
            try (IndexOutput out = dir.createOutput("test_ash_tail.bin", IOContext.DEFAULT)) {
                for (int i = 0; i < tailSize; i++) {
                    out.writeBytes(packedCodes[i], 0, packedCodeBytes);
                }
                CodecUtil.writeFooter(out);
            }

            try (IndexInput in = dir.openInput("test_ash_tail.bin", IOContext.DEFAULT)) {
                long dataLength = (long) packedCodeBytes * tailSize;

                float[] scalarScores = new float[BULK_SIZE];
                float[] panamaScores = new float[BULK_SIZE];
                float[] nativeScores = new float[BULK_SIZE];

                if (queryBitsPerDim > 0) {
                    IndexInput scalarSlice = in.slice("scalar", 0, dataLength);
                    AshScorer<byte[]> scalarScorer = defaultProvider().getVectorScorerFactory()
                        .newESNextAshIntegerVectorsScorer(scalarSlice, nDims, bitsPerDim, queryBitsPerDim);
                    IndexInput panamaInput = in.clone();
                    AshScorer<byte[]> panamaScorer = panamaProvider().getVectorScorerFactory()
                        .newESNextAshIntegerVectorsScorer(panamaInput, nDims, bitsPerDim, queryBitsPerDim);
                    IndexInput nativeInput = in.clone();
                    AshScorer<byte[]> nativeScorer = nativeProvider().getVectorScorerFactory()
                        .newESNextAshIntegerVectorsScorer(nativeInput, nDims, bitsPerDim, queryBitsPerDim);

                    scalarScorer.scoreBulk(queryQuantized, scalarScores, tailSize);
                    panamaScorer.scoreBulk(queryQuantized, panamaScores, tailSize);
                    nativeScorer.scoreBulk(queryQuantized, nativeScores, tailSize);
                    assertScoresMatch("scalar vs panama", scalarScores, panamaScores, tailSize, 0);
                    assertScoresMatch("scalar vs native", scalarScores, nativeScores, tailSize, 0);
                    assertEquals(dataLength, scalarSlice.getFilePointer());
                    assertEquals(dataLength, panamaInput.getFilePointer());
                    assertEquals(dataLength, nativeInput.getFilePointer());
                } else {
                    IndexInput scalarSlice = in.slice("scalar", 0, dataLength);
                    AshScorer<float[]> scalarScorer = defaultProvider().getVectorScorerFactory()
                        .newESNextAshFloatVectorsScorer(scalarSlice, nDims, bitsPerDim);
                    IndexInput panamaInput = in.clone();
                    AshScorer<float[]> panamaScorer = panamaProvider().getVectorScorerFactory()
                        .newESNextAshFloatVectorsScorer(panamaInput, nDims, bitsPerDim);

                    scalarScorer.scoreBulk(queryTransformed, scalarScores, tailSize);
                    panamaScorer.scoreBulk(queryTransformed, panamaScores, tailSize);
                    assertScoresMatch("scalar vs panama", scalarScores, panamaScores, tailSize, 0);
                    assertEquals(dataLength, scalarSlice.getFilePointer());
                    assertEquals(dataLength, panamaInput.getFilePointer());
                }
            }
        }
    }

    /**
     * Tests with nDims below 128 ({@code planeBytes < 16}) to exercise the fall-through to scalar in PanamaBBQDotProduct.
     */
    public void testScoreBulkSmallDims() throws Exception {
        // nDims in [1,15] gives planeBytes in [1,2], both below the Panama 16-byte threshold
        int nDims = randomIntBetween(1, 15);
        int planeBytes = (nDims + 7) / 8;
        int packedCodeBytes = bitsPerDim * planeBytes;
        int numVectors = randomIntBetween(1, BULK_SIZE);

        byte[][] packedCodes = new byte[numVectors][packedCodeBytes];
        for (int i = 0; i < numVectors; i++) {
            random().nextBytes(packedCodes[i]);
        }

        float[] queryTransformed = new float[nDims];
        for (int j = 0; j < nDims; j++) {
            queryTransformed[j] = (float) random().nextGaussian();
        }
        byte[] queryQuantized = null;
        if (queryBitsPerDim > 0) {
            queryQuantized = new byte[queryBitsPerDim * planeBytes];
            random().nextBytes(queryQuantized);
        }

        try (Directory dir = newParametrizedDirectory()) {
            try (IndexOutput out = dir.createOutput("test_ash_small.bin", IOContext.DEFAULT)) {
                for (int i = 0; i < numVectors; i++) {
                    out.writeBytes(packedCodes[i], 0, packedCodeBytes);
                }
                CodecUtil.writeFooter(out);
            }

            try (IndexInput in = dir.openInput("test_ash_small.bin", IOContext.DEFAULT)) {
                long dataLength = (long) packedCodeBytes * numVectors;
                float[] scalarScores = new float[BULK_SIZE];
                float[] panamaScores = new float[BULK_SIZE];

                if (queryBitsPerDim > 0) {
                    IndexInput scalarSlice = in.slice("scalar", 0, dataLength);
                    AshScorer<byte[]> scalarScorer = defaultProvider().getVectorScorerFactory()
                        .newESNextAshIntegerVectorsScorer(scalarSlice, nDims, bitsPerDim, queryBitsPerDim);
                    IndexInput panamaInput = in.clone();
                    AshScorer<byte[]> panamaScorer = panamaProvider().getVectorScorerFactory()
                        .newESNextAshIntegerVectorsScorer(panamaInput, nDims, bitsPerDim, queryBitsPerDim);

                    scalarScorer.scoreBulk(queryQuantized, scalarScores, numVectors);
                    panamaScorer.scoreBulk(queryQuantized, panamaScores, numVectors);
                    assertScoresMatch("scalar vs panama (small dims)", scalarScores, panamaScores, numVectors, 0);
                } else {
                    IndexInput scalarSlice = in.slice("scalar", 0, dataLength);
                    AshScorer<float[]> scalarScorer = defaultProvider().getVectorScorerFactory()
                        .newESNextAshFloatVectorsScorer(scalarSlice, nDims, bitsPerDim);
                    IndexInput panamaInput = in.clone();
                    AshScorer<float[]> panamaScorer = panamaProvider().getVectorScorerFactory()
                        .newESNextAshFloatVectorsScorer(panamaInput, nDims, bitsPerDim);

                    scalarScorer.scoreBulk(queryTransformed, scalarScores, numVectors);
                    panamaScorer.scoreBulk(queryTransformed, panamaScores, numVectors);
                    assertScoresMatch("scalar vs panama (small dims)", scalarScores, panamaScores, numVectors, 0);
                }
            }
        }
    }

    /**
     * Tests that the single-vector score() method produces the same result as scoreBulk() with blockSize=1.
     */
    public void testSingleScoreMatchesBulk() throws Exception {
        int nDims = randomIntBetween(16, 256);
        int planeBytes = (nDims + 7) / 8;
        int packedCodeBytes = bitsPerDim * planeBytes;

        byte[] packedCode = new byte[packedCodeBytes];
        random().nextBytes(packedCode);

        float[] queryTransformed = new float[nDims];
        for (int j = 0; j < nDims; j++) {
            queryTransformed[j] = (float) random().nextGaussian();
        }
        byte[] queryQuantized = null;
        if (queryBitsPerDim > 0) {
            queryQuantized = new byte[queryBitsPerDim * planeBytes];
            random().nextBytes(queryQuantized);
        }

        try (Directory dir = newParametrizedDirectory()) {
            // Write the same vector twice (one for single, one for bulk)
            try (IndexOutput out = dir.createOutput("test_ash_single.bin", IOContext.DEFAULT)) {
                out.writeBytes(packedCode, 0, packedCodeBytes);
                out.writeBytes(packedCode, 0, packedCodeBytes);
                CodecUtil.writeFooter(out);
            }

            try (IndexInput in = dir.openInput("test_ash_single.bin", IOContext.DEFAULT)) {
                if (queryBitsPerDim > 0) {
                    IndexInput singleInput = in.clone();
                    AshScorer<byte[]> singleScorer = panamaProvider().getVectorScorerFactory()
                        .newESNextAshIntegerVectorsScorer(singleInput, nDims, bitsPerDim, queryBitsPerDim);

                    IndexInput bulkInput = in.clone();
                    AshScorer<byte[]> bulkScorer = panamaProvider().getVectorScorerFactory()
                        .newESNextAshIntegerVectorsScorer(bulkInput, nDims, bitsPerDim, queryBitsPerDim);

                    float singleScore = singleScorer.score(queryQuantized);
                    float[] bulkScores = new float[1];
                    bulkScorer.scoreBulk(queryQuantized, bulkScores, 1);
                    assertEquals("Single vs bulk mismatch", singleScore, bulkScores[0], 0f);
                } else {
                    IndexInput singleInput = in.clone();
                    AshScorer<float[]> singleScorer = panamaProvider().getVectorScorerFactory()
                        .newESNextAshFloatVectorsScorer(singleInput, nDims, bitsPerDim);

                    IndexInput bulkInput = in.clone();
                    AshScorer<float[]> bulkScorer = panamaProvider().getVectorScorerFactory()
                        .newESNextAshFloatVectorsScorer(bulkInput, nDims, bitsPerDim);

                    float singleScore = singleScorer.score(queryTransformed);
                    float[] bulkScores = new float[1];
                    bulkScorer.scoreBulk(queryTransformed, bulkScores, 1);
                    assertEquals("Single vs bulk mismatch", singleScore, bulkScores[0], 0f);
                }
            }
        }
    }

    private void assertScoresMatch(String label, float[] expected, float[] actual, int count, int baseOffset) {
        for (int j = 0; j < count; j++) {
            assertEquals(
                label
                    + " mismatch at vector "
                    + (baseOffset + j)
                    + " (directoryType="
                    + directoryType
                    + ", D"
                    + bitsPerDim
                    + "Q"
                    + queryBitsPerDim
                    + ")",
                expected[j],
                actual[j],
                1e-3f
            );
        }
    }

    private Directory newParametrizedDirectory() throws IOException {
        return switch (directoryType) {
            case NIOFS -> new NIOFSDirectory(createTempDir());
            case MMAP -> new MMapDirectory(createTempDir());
            case SNAP -> SearchableSnapshotDirectoryFactory.newDirectory(createTempDir());
        };
    }
}
