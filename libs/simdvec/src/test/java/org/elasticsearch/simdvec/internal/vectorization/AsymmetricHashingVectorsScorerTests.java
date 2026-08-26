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
import org.elasticsearch.simdvec.AsymmetricHashingVectorsScorer;
import org.elasticsearch.simdvec.BaseVectorizationTests;
import org.elasticsearch.simdvec.ESVectorUtil;
import org.elasticsearch.xpack.searchablesnapshots.store.SearchableSnapshotDirectoryFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static org.elasticsearch.simdvec.ES940OSQVectorsScorer.BULK_SIZE;

/**
 * Tests for {@link AsymmetricHashingVectorsScorer} and its Panama-accelerated
 * subclass {@link MemorySegmentASHVectorsScorer}.
 * <p>
 * Mirrors the structure of {@link ES940OSQVectorsScorerTests}: for each
 * (DirectoryType, bitsPerDim, queryBitsPerDim) combination, writes random
 * packed ASH data to an IndexInput, creates both scalar and Panama scorers,
 * and asserts they produce matching raw dot products.
 */
public class AsymmetricHashingVectorsScorerTests extends BaseVectorizationTests {

    private final DirectoryType directoryType;
    private final int bitsPerDim;
    private final int queryBitsPerDim;

    public enum DirectoryType {
        NIOFS,
        MMAP,
        SNAP
    }

    public AsymmetricHashingVectorsScorerTests(DirectoryType directoryType, int bitsPerDim, int queryBitsPerDim) {
        this.directoryType = directoryType;
        this.bitsPerDim = bitsPerDim;
        this.queryBitsPerDim = queryBitsPerDim;
    }

    @ParametersFactory
    public static Iterable<Object[]> parametersFactory() {
        // (bitsPerDim, queryBitsPerDim): D1Q0 (float), D1Q4 (integer), D2Q4 (integer)
        var bitCombinations = List.of(List.of(1, 0), List.of(1, 4), List.of(2, 4));
        return () -> bitCombinations.stream()
            .flatMap(bits -> Arrays.stream(DirectoryType.values()).map(d -> new Object[] { d, bits.get(0), bits.get(1) }))
            .iterator();
    }

    /**
     * Tests that the Panama scorer produces the same raw dot products as the scalar scorer
     * for a bulk of random vectors.
     */
    public void testScoreBulk() throws Exception {
        // nDims must be a multiple of 8 for bit-plane packing to work cleanly
        int nDims = (randomIntBetween(16, 512) + 7) & ~7;
        int planeBytes = nDims / 8;
        int packedCodeBytes = bitsPerDim * planeBytes;
        int numVectors = BULK_SIZE * randomIntBetween(1, 4);

        // Generate random packed codes for each vector
        byte[][] packedCodes = new byte[numVectors][packedCodeBytes];
        for (int i = 0; i < numVectors; i++) {
            random().nextBytes(packedCodes[i]);
        }

        // Generate random query
        float[] queryTransformed = new float[nDims];
        for (int j = 0; j < nDims; j++) {
            queryTransformed[j] = (float) random().nextGaussian();
        }

        // For integer path: generate quantized query in bit-plane format
        byte[] queryQuantized = null;
        if (queryBitsPerDim > 0) {
            queryQuantized = new byte[queryBitsPerDim * planeBytes];
            random().nextBytes(queryQuantized);
        }

        try (Directory dir = newParametrizedDirectory()) {
            // Write packed codes to an IndexOutput
            try (IndexOutput out = dir.createOutput("test_ash.bin", IOContext.DEFAULT)) {
                for (int i = 0; i < numVectors; i++) {
                    out.writeBytes(packedCodes[i], 0, packedCodeBytes);
                }
                CodecUtil.writeFooter(out);
            }

            try (IndexInput in = dir.openInput("test_ash.bin", IOContext.DEFAULT)) {
                long dataLength = (long) packedCodeBytes * numVectors;
                // Scalar scorer: use a slice with exact length to catch out-of-bounds reads
                IndexInput scalarSlice = in.slice("scalar", 0, dataLength);
                AsymmetricHashingVectorsScorer scalarScorer = new AsymmetricHashingVectorsScorer(scalarSlice, nDims, bitsPerDim);

                // Panama scorer: use a clone (shares underlying file, independent pointer)
                IndexInput panamaInput = in.clone();
                AsymmetricHashingVectorsScorer panamaScorer = ESVectorUtil.getASHVectorsScorer(
                    panamaInput,
                    nDims,
                    bitsPerDim,
                    queryBitsPerDim
                );

                float[] scalarScores = new float[BULK_SIZE];
                float[] panamaScores = new float[BULK_SIZE];

                // Process in bulk chunks
                for (int offset = 0; offset < numVectors; offset += BULK_SIZE) {
                    int blockSize = Math.min(BULK_SIZE, numVectors - offset);
                    if (queryBitsPerDim > 0) {
                        scalarScorer.scoreIntegerBulk(queryQuantized, queryBitsPerDim, scalarScores, blockSize);
                        panamaScorer.scoreIntegerBulk(queryQuantized, queryBitsPerDim, panamaScores, blockSize);
                    } else {
                        scalarScorer.scoreFloatBulk(queryTransformed, scalarScores, blockSize);
                        panamaScorer.scoreFloatBulk(queryTransformed, panamaScores, blockSize);
                    }

                    for (int j = 0; j < blockSize; j++) {
                        assertEquals(
                            "Mismatch at vector "
                                + (offset + j)
                                + " (directoryType="
                                + directoryType
                                + ", D"
                                + bitsPerDim
                                + "Q"
                                + queryBitsPerDim
                                + ")",
                            scalarScores[j],
                            panamaScores[j],
                            1e-3f
                        );
                    }
                }

                // Verify both scorers advanced by exactly the right number of bytes
                assertEquals(dataLength, scalarSlice.getFilePointer());
                assertEquals(dataLength, panamaInput.getFilePointer());
            }
        }
    }

    /**
     * Tests scoring a tail block (smaller than BULK_SIZE) to exercise non-aligned paths.
     */
    public void testScoreBulkTail() throws Exception {
        int nDims = (randomIntBetween(16, 256) + 7) & ~7;
        int planeBytes = nDims / 8;
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
                IndexInput scalarSlice = in.slice("scalar", 0, dataLength);
                AsymmetricHashingVectorsScorer scalarScorer = new AsymmetricHashingVectorsScorer(scalarSlice, nDims, bitsPerDim);

                IndexInput panamaInput = in.clone();
                AsymmetricHashingVectorsScorer panamaScorer = ESVectorUtil.getASHVectorsScorer(
                    panamaInput,
                    nDims,
                    bitsPerDim,
                    queryBitsPerDim
                );

                float[] scalarScores = new float[BULK_SIZE];
                float[] panamaScores = new float[BULK_SIZE];

                if (queryBitsPerDim > 0) {
                    scalarScorer.scoreIntegerBulk(queryQuantized, queryBitsPerDim, scalarScores, tailSize);
                    panamaScorer.scoreIntegerBulk(queryQuantized, queryBitsPerDim, panamaScores, tailSize);
                } else {
                    scalarScorer.scoreFloatBulk(queryTransformed, scalarScores, tailSize);
                    panamaScorer.scoreFloatBulk(queryTransformed, panamaScores, tailSize);
                }

                for (int j = 0; j < tailSize; j++) {
                    assertEquals("Tail mismatch at vector " + j, scalarScores[j], panamaScores[j], 1e-3f);
                }

                assertEquals(dataLength, scalarSlice.getFilePointer());
                assertEquals(dataLength, panamaInput.getFilePointer());
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

}
