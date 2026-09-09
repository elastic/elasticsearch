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
import org.elasticsearch.common.util.ArrayUtils;
import org.elasticsearch.simdvec.BaseVectorizationTests;
import org.elasticsearch.xpack.searchablesnapshots.store.SearchableSnapshotDirectoryFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static org.hamcrest.Matchers.either;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notANumber;

public class BBQDotProductTests extends BaseVectorizationTests {

    private static final int BULK_SIZE = 32;

    public enum DirectoryType {
        NIOFS,
        MMAP,
        SNAP
    }

    public enum Implementation {
        SCALAR,
        PANAMA,
        NATIVE
    }

    private final DirectoryType directoryType;
    private final int docBits;
    private final int queryBits;

    public BBQDotProductTests(DirectoryType directoryType, int docBits, int queryBits) {
        this.directoryType = directoryType;
        this.docBits = docBits;
        this.queryBits = queryBits;
    }

    @ParametersFactory
    public static Iterable<Object[]> parametersFactory() {
        List<Object[]> bitCombinations = List.of(
            new Object[] { 1, 1 },
            new Object[] { 1, 4 },
            new Object[] { 2, 1 },
            new Object[] { 2, 2 },
            new Object[] { 2, 4 },
            new Object[] { 3, 4 },
            new Object[] { 4, 4 },
            new Object[] { 8, 4 },
            new Object[] { 8, 8 }
        );
        return () -> bitCombinations.stream()
            .flatMap(bits -> Arrays.stream(DirectoryType.values()).map(d -> ArrayUtils.prepend(d, bits)))
            .iterator();
    }

    private BBQDotProduct getImpl(Implementation impl, IndexInput input, int nDims) {
        return switch (impl) {
            case SCALAR -> BBQDotProduct.create(input, nDims, docBits, queryBits);
            case PANAMA -> PanamaBBQDotProduct.create(input, nDims, docBits, queryBits);
            case NATIVE -> NativeBBQDotProduct.create(input, nDims, docBits, queryBits);
        };
    }

    public void testBulkScoring() throws Exception {
        int nDims = randomDims();
        int planeBytes = BBQDotProduct.planeBytes(nDims);
        int count = BULK_SIZE * randomIntBetween(1, 4) - randomIntBetween(0, BULK_SIZE - 1);

        byte[] query = randomPackedVector(queryBits, planeBytes, nDims);
        byte[][] vectors = new byte[count][];
        for (int i = 0; i < count; i++) {
            vectors[i] = randomPackedVector(docBits, planeBytes, nDims);
        }

        try (Directory dir = newParametrizedDirectory()) {
            write(dir, vectors);
            try (IndexInput in = dir.openInput("vecs.bin", IOContext.DEFAULT)) {

                Long consumed = null;
                for (Implementation impl : Implementation.values()) {
                    IndexInput read = in.clone();
                    BBQDotProduct dotProduct = getImpl(impl, read, nDims);
                    float[] scores = new float[BULK_SIZE];

                    for (int offset = 0; offset < count; offset += BULK_SIZE) {
                        int blockSize = Math.min(BULK_SIZE, count - offset);
                        dotProduct.dotProductBulk(query, blockSize, scores);
                        for (int j = 0; j < blockSize; j++) {
                            float expected = basicBitImplementation(query, vectors[offset + j], planeBytes, nDims);
                            assertThat(describe(impl + ", vector " + (offset + j)), scores[j], equalTo(expected));
                        }
                    }

                    if (consumed == null) {
                        consumed = read.getFilePointer();
                    } else {
                        assertThat(describe(impl.toString()), read.getFilePointer(), equalTo(consumed));
                    }
                }
            }
        }
    }

    public void testSingleMatchesBulk() throws Exception {
        int nDims = randomDims();
        int planeBytes = BBQDotProduct.planeBytes(nDims);

        byte[] query = randomPackedVector(queryBits, planeBytes, nDims);
        byte[][] vectors = new byte[][] { randomPackedVector(docBits, planeBytes, nDims) };

        try (Directory dir = newParametrizedDirectory()) {
            write(dir, vectors);
            try (IndexInput in = dir.openInput("vecs.bin", IOContext.DEFAULT)) {
                long expected = basicBitImplementation(query, vectors[0], planeBytes, nDims);

                for (Implementation impl : Implementation.values()) {
                    BBQDotProduct single = getImpl(impl, in.clone(), nDims);
                    BBQDotProduct bulk = getImpl(impl, in.clone(), nDims);

                    float[] bulkScores = new float[1];
                    bulk.dotProductBulk(query, 1, bulkScores);
                    assertThat(describe(impl.toString()), single.dotProduct(query), equalTo(expected));
                    assertThat(describe(impl.toString()), bulkScores[0], equalTo((float) expected));
                }
            }
        }
    }

    public void testBulkOffsets() throws Exception {
        int nDims = randomDims();
        int planeBytes = BBQDotProduct.planeBytes(nDims);
        int count = randomIntBetween(1, BULK_SIZE);

        byte[] query = randomPackedVector(queryBits, planeBytes, nDims);
        byte[][] vectors = new byte[count][];
        for (int i = 0; i < count; i++) {
            vectors[i] = randomPackedVector(docBits, planeBytes, nDims);
        }
        int[] offsets = new int[BULK_SIZE];
        int offsetsCount = 0;
        for (int i = 0; i < count; i++) {
            if (randomBoolean()) {
                offsets[offsetsCount++] = i;
            }
        }
        if (offsetsCount == 0) {
            // Ensure there's always *something* to score
            offsets[offsetsCount++] = randomIntBetween(0, count - 1);
        }

        try (Directory dir = newParametrizedDirectory()) {
            write(dir, vectors);
            try (IndexInput in = dir.openInput("vecs.bin", IOContext.DEFAULT)) {
                for (Implementation impl : Implementation.values()) {
                    IndexInput read = in.clone();
                    BBQDotProduct dotProduct = getImpl(impl, read, nDims);

                    float[] scores = new float[BULK_SIZE];
                    Arrays.fill(scores, Float.NaN);
                    dotProduct.dotProductBulkOffsets(query, offsets, offsetsCount, scores, count);

                    int next = 0;
                    for (int i = 0; i < count; i++) {
                        boolean scored = next < offsetsCount && offsets[next] == i;
                        if (scored) {
                            long expected = basicBitImplementation(query, vectors[i], planeBytes, nDims);
                            assertThat(describe(impl + ", vector " + i), scores[i], equalTo((float) expected));
                            next++;
                        } else {
                            assertThat(describe(impl.toString()), (double) scores[i], either(notANumber()).or(is(0.0)));
                        }
                    }

                    // should have read all the data regardless
                    long consumed = (long) count * docBits * planeBytes;
                    assertThat(describe(impl.toString()), read.getFilePointer(), equalTo(consumed));
                }
            }
        }
    }

    public void testTierSelectionFollowsSupport() throws Exception {
        int nDims = randomDims();
        int planeBytes = BBQDotProduct.planeBytes(nDims);

        try (Directory dir = newParametrizedDirectory()) {
            write(dir, new byte[][] { randomPackedVector(docBits, planeBytes, nDims) });
            try (IndexInput in = dir.openInput("vecs.bin", IOContext.DEFAULT)) {
                IndexInput panamaIn = in.clone();
                BBQDotProduct panama = PanamaBBQDotProduct.create(panamaIn, nDims, docBits, queryBits);
                if (PanamaBBQDotProduct.supports(panamaIn, docBits, queryBits, planeBytes)) {
                    assertThat(describe("Panama"), panama, instanceOf(PanamaBBQDotProduct.class));
                } else {
                    assertThat(describe("Panama"), panama, not(instanceOf(PanamaBBQDotProduct.class)));
                }

                IndexInput nativeIn = in.clone();
                BBQDotProduct nativ = NativeBBQDotProduct.create(nativeIn, nDims, docBits, queryBits);
                if (NativeBBQDotProduct.supports(nativeIn, docBits, queryBits)) {
                    assertThat(describe("native"), nativ, instanceOf(NativeBBQDotProduct.class));
                } else {
                    assertThat(describe("native"), nativ, not(instanceOf(NativeBBQDotProduct.class)));
                }
            }
        }
    }

    public void testRejectsOutOfRangeConfiguration() {
        expectThrows(IllegalArgumentException.class, () -> BBQDotProduct.create(null, 128, 0, queryBits));
        expectThrows(IllegalArgumentException.class, () -> BBQDotProduct.create(null, 128, BBQDotProduct.MAX_BITS + 1, queryBits));
        expectThrows(IllegalArgumentException.class, () -> BBQDotProduct.create(null, 128, docBits, 0));
        expectThrows(IllegalArgumentException.class, () -> BBQDotProduct.create(null, 128, docBits, BBQDotProduct.MAX_BITS + 1));
        expectThrows(IllegalArgumentException.class, () -> BBQDotProduct.create(null, 0, docBits, queryBits));
    }

    private long basicBitImplementation(byte[] query, byte[] data, int planeBytes, int nDims) {
        // basic bit-by-bit implementation, for sanity checking
        long dot = 0;
        for (int i = 0; i < nDims; i++) {
            int byteIndex = i >>> 3;
            int bitIndex = 7 - (i & 7);
            int queryLevel = 0;
            for (int p = 0; p < queryBits; p++) {
                queryLevel |= ((query[p * planeBytes + byteIndex] >> bitIndex) & 1) << p;
            }
            int docLevel = 0;
            for (int p = 0; p < docBits; p++) {
                docLevel |= ((data[p * planeBytes + byteIndex] >> bitIndex) & 1) << p;
            }
            dot += (long) queryLevel * docLevel;
        }
        return dot;
    }

    /**
     * Creates random bytes, but zeros bits that don't represent any dimensions.
     */
    private static byte[] randomPackedVector(int bitsPerDim, int planeBytes, int nDims) {
        byte[] bytes = randomByteArrayOfLength(bitsPerDim * planeBytes);
        int padding = planeBytes * Byte.SIZE - nDims;
        if (padding > 0) {
            byte mask = (byte) (0xFF << padding);
            for (int p = 0; p < bitsPerDim; p++) {
                bytes[(p + 1) * planeBytes - 1] &= mask;
            }
        }
        return bytes;
    }

    private static int randomDims() {
        return randomIntBetween(8, 1024);
    }

    private static void write(Directory dir, byte[][] bytes) throws IOException {
        try (IndexOutput out = dir.createOutput("vecs.bin", IOContext.DEFAULT)) {
            for (byte[] vector : bytes) {
                out.writeBytes(vector, 0, vector.length);
            }
            CodecUtil.writeFooter(out);
        }
    }

    private String describe(String what) {
        return what + " (directoryType=" + directoryType + ", D" + docBits + "Q" + queryBits + ")";
    }

    private Directory newParametrizedDirectory() throws IOException {
        return switch (directoryType) {
            case NIOFS -> new NIOFSDirectory(createTempDir());
            case MMAP -> new MMapDirectory(createTempDir());
            case SNAP -> SearchableSnapshotDirectoryFactory.newDirectory(createTempDir());
        };
    }
}
