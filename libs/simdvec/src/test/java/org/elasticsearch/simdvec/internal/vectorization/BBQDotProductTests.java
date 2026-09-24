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
import org.elasticsearch.simdvec.BBQEncoding;
import org.elasticsearch.simdvec.BaseVectorizationTests;
import org.elasticsearch.xpack.searchablesnapshots.store.SearchableSnapshotDirectoryFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static org.hamcrest.Matchers.either;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
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
    private final BBQEncoding encoding;

    public BBQDotProductTests(DirectoryType directoryType, BBQEncoding encoding) {
        this.directoryType = directoryType;
        this.encoding = encoding;
    }

    @ParametersFactory
    public static Iterable<Object[]> parametersFactory() {
        List<BBQEncoding> bitCombinations = List.of(
            new BBQEncoding(1, 1),
            new BBQEncoding(1, 4),
            new BBQEncoding(2, 2),
            new BBQEncoding(2, 4),
            new BBQEncoding(3, 4),
            new BBQEncoding(4, 4),
            new BBQEncoding(4, 8),
            new BBQEncoding(8, 8)
        );
        return () -> bitCombinations.stream()
            .flatMap(e -> Arrays.stream(DirectoryType.values()).map(d -> new Object[] { d, e }))
            .iterator();
    }

    private BBQDotProduct getImpl(Implementation impl, IndexInput input, int nDims) {
        return switch (impl) {
            case SCALAR -> BBQDotProduct.create(input, nDims, encoding);
            case PANAMA -> PanamaBBQDotProduct.create(input, nDims, encoding);
            case NATIVE -> NativeBBQDotProduct.create(input, nDims, encoding);
        };
    }

    public void testBulkScoring() throws Exception {
        int nDims = randomDims();
        int planeBytes = BBQDotProduct.planeBytes(nDims);
        int count = BULK_SIZE * randomIntBetween(1, 4) - randomIntBetween(0, BULK_SIZE - 1);

        byte[] query = randomPackedVector(encoding.queryBits(), planeBytes, nDims);
        byte[][] vectors = new byte[count][];
        for (int i = 0; i < count; i++) {
            vectors[i] = randomPackedVector(encoding.dataBits(), planeBytes, nDims);
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

        byte[] query = randomPackedVector(encoding.queryBits(), planeBytes, nDims);
        byte[][] vectors = new byte[][] { randomPackedVector(encoding.dataBits(), planeBytes, nDims) };

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

        byte[] query = randomPackedVector(encoding.queryBits(), planeBytes, nDims);
        byte[][] vectors = new byte[count][];
        for (int i = 0; i < count; i++) {
            vectors[i] = randomPackedVector(encoding.dataBits(), planeBytes, nDims);
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
                    long consumed = (long) count * encoding.dataBits() * planeBytes;
                    assertThat(describe(impl.toString()), read.getFilePointer(), equalTo(consumed));
                }
            }
        }
    }

    public void testRejectsOutOfRangeConfiguration() {
        expectThrows(IllegalArgumentException.class, () -> new BBQEncoding(0, encoding.queryBits()));
        expectThrows(IllegalArgumentException.class, () -> new BBQEncoding(Byte.SIZE + 1, encoding.queryBits()));
        expectThrows(IllegalArgumentException.class, () -> new BBQEncoding(encoding.dataBits(), 0));
        expectThrows(IllegalArgumentException.class, () -> new BBQEncoding(encoding.dataBits(), Byte.SIZE + 1));
        expectThrows(IllegalArgumentException.class, () -> BBQDotProduct.create(null, 0, encoding));
    }

    private long basicBitImplementation(byte[] query, byte[] data, int planeBytes, int nDims) {
        // basic bit-by-bit implementation, for sanity checking
        long dot = 0;
        for (int i = 0; i < nDims; i++) {
            int byteIndex = i >>> 3;
            int bitIndex = 7 - (i & 7);
            int queryLevel = 0;
            for (int p = 0; p < encoding.queryBits(); p++) {
                queryLevel |= ((query[p * planeBytes + byteIndex] >> bitIndex) & 1) << p;
            }
            int docLevel = 0;
            for (int p = 0; p < encoding.dataBits(); p++) {
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
        return what + " (directoryType=" + directoryType + ", " + encoding + ")";
    }

    private Directory newParametrizedDirectory() throws IOException {
        return switch (directoryType) {
            case NIOFS -> new NIOFSDirectory(createTempDir());
            case MMAP -> new MMapDirectory(createTempDir());
            case SNAP -> SearchableSnapshotDirectoryFactory.newDirectory(createTempDir());
        };
    }
}
