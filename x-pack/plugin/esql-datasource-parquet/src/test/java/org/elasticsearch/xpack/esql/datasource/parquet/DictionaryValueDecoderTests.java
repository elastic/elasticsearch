/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.parquet;

import org.apache.lucene.util.BytesRef;
import org.apache.parquet.bytes.BytesUtils;
import org.apache.parquet.bytes.HeapByteBufferAllocator;
import org.apache.parquet.column.Dictionary;
import org.apache.parquet.column.values.rle.RunLengthBitPackingHybridEncoder;
import org.apache.parquet.io.api.Binary;
import org.elasticsearch.test.ESTestCase;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * Unit tests for {@link DictionaryValueDecoder}, including the dictionary-index fast path
 * used by {@link PageColumnReader} to emit {@link org.elasticsearch.compute.data.OrdinalBytesRefBlock}.
 */
public class DictionaryValueDecoderTests extends ESTestCase {

    public void testReadIndicesShortRun() throws IOException {
        int[] expected = { 0, 0, 0, 0, 0, 0, 0, 0 };
        DictionaryValueDecoder decoder = decoderFor(expected, /* bitWidth= */ 4);

        int[] indices = new int[expected.length];
        decoder.readIndices(indices, 0, expected.length);

        assertArrayEquals(expected, indices);
    }

    public void testReadIndicesMixedRunAndPacked() throws IOException {
        int[] expected = { 0, 1, 2, 3, 4, 5, 6, 7, 7, 7, 7, 7, 0, 0, 1, 2, 3, 5 };
        DictionaryValueDecoder decoder = decoderFor(expected, /* bitWidth= */ 3);

        int[] indices = new int[expected.length];
        decoder.readIndices(indices, 0, expected.length);

        assertArrayEquals(expected, indices);
    }

    public void testReadIndicesWithOffset() throws IOException {
        int[] expected = { 1, 2, 3, 4, 5, 6, 7, 0 };
        DictionaryValueDecoder decoder = decoderFor(expected, /* bitWidth= */ 3);

        int[] indices = new int[expected.length + 5];
        Arrays.fill(indices, -1);
        decoder.readIndices(indices, 5, expected.length);

        for (int i = 0; i < 5; i++) {
            assertEquals("slot before offset must be untouched", -1, indices[i]);
        }
        for (int i = 0; i < expected.length; i++) {
            assertEquals("slot at offset " + (5 + i), expected[i], indices[5 + i]);
        }
    }

    public void testReadIndicesRandom() throws IOException {
        int dictSize = randomIntBetween(2, 64);
        int bitWidth = BytesUtils.getWidthFromMaxInt(dictSize - 1);
        int count = randomIntBetween(50, 5000);
        int[] expected = new int[count];
        for (int i = 0; i < count; i++) {
            expected[i] = randomIntBetween(0, dictSize - 1);
        }
        DictionaryValueDecoder decoder = decoderFor(expected, bitWidth);

        int[] indices = new int[count];
        decoder.readIndices(indices, 0, count);

        assertArrayEquals(expected, indices);
    }

    public void testReadIndicesAndReadBinariesAgreeOnSameStream() throws IOException {
        // For the same encoded index stream, both paths must produce equivalent results
        // (readBinaries resolves indices through the dictionary; readIndices returns the indices).
        String[] dict = { "alpha", "beta", "gamma", "delta" };
        int[] expectedIndices = new int[200];
        for (int i = 0; i < expectedIndices.length; i++) {
            expectedIndices[i] = randomIntBetween(0, dict.length - 1);
        }

        DictionaryValueDecoder indexDecoder = decoderFor(expectedIndices, /* bitWidth= */ 2);
        int[] indices = new int[expectedIndices.length];
        indexDecoder.readIndices(indices, 0, expectedIndices.length);

        DictionaryValueDecoder binaryDecoder = decoderFor(expectedIndices, /* bitWidth= */ 2);
        BytesRef[] resolved = new BytesRef[expectedIndices.length];
        Dictionary fakeDict = new BinaryDictionary(dict);
        binaryDecoder.readBinaries(resolved, 0, expectedIndices.length, fakeDict);

        for (int i = 0; i < expectedIndices.length; i++) {
            assertEquals("index@" + i, expectedIndices[i], indices[i]);
            BytesRef expected = new BytesRef(dict[expectedIndices[i]]);
            assertEquals("resolved@" + i, expected, resolved[i]);
        }
    }

    public void testGetDictionaryBytesRefsCachesAcrossCalls() {
        String[] dict = { "one", "two", "three" };
        Dictionary fakeDict = new BinaryDictionary(dict);
        DictionaryValueDecoder decoder = new DictionaryValueDecoder();

        BytesRef[] first = decoder.getDictionaryBytesRefs(fakeDict);
        BytesRef[] second = decoder.getDictionaryBytesRefs(fakeDict);

        assertSame("dictionary array should be cached and returned by reference", first, second);
        assertEquals(dict.length, first.length);
        for (int i = 0; i < dict.length; i++) {
            assertEquals(new BytesRef(dict[i]), first[i]);
        }
    }

    public void testGetDictionaryBytesRefsBeforeReadBinariesIsAvailable() {
        // The dictionary array must be lazily initialized on first access without requiring
        // readBinaries to have been called.
        String[] dict = { "x", "y" };
        Dictionary fakeDict = new BinaryDictionary(dict);
        DictionaryValueDecoder decoder = new DictionaryValueDecoder();

        BytesRef[] entries = decoder.getDictionaryBytesRefs(fakeDict);
        assertEquals(2, entries.length);
        assertEquals(new BytesRef("x"), entries[0]);
        assertEquals(new BytesRef("y"), entries[1]);
    }

    public void testReadBinariesAndGetDictionaryBytesRefsShareCache() throws IOException {
        // A subsequent readBinaries call must reuse the same cached dictionary array.
        int[] expectedIndices = { 0, 1, 0, 1 };
        String[] dict = { "left", "right" };
        Dictionary fakeDict = new BinaryDictionary(dict);

        DictionaryValueDecoder decoder = decoderFor(expectedIndices, /* bitWidth= */ 1);
        BytesRef[] entries = decoder.getDictionaryBytesRefs(fakeDict);

        BytesRef[] resolved = new BytesRef[expectedIndices.length];
        decoder.readBinaries(resolved, 0, expectedIndices.length, fakeDict);

        for (int i = 0; i < expectedIndices.length; i++) {
            assertSame("readBinaries must return the cached entry", entries[expectedIndices[i]], resolved[i]);
        }
    }

    // --- helpers ---

    private static DictionaryValueDecoder decoderFor(int[] indices, int bitWidth) throws IOException {
        ByteBuffer encoded = encodeRle(indices, bitWidth);
        DictionaryValueDecoder decoder = new DictionaryValueDecoder();
        decoder.init(encoded);
        return decoder;
    }

    /**
     * Encodes {@code indices} using Parquet's RLE/bit-packed hybrid encoding and prepends
     * the 1-byte bit-width header that {@link DictionaryValueDecoder#init(ByteBuffer)} expects.
     */
    private static ByteBuffer encodeRle(int[] indices, int bitWidth) throws IOException {
        RunLengthBitPackingHybridEncoder encoder = new RunLengthBitPackingHybridEncoder(
            bitWidth,
            64,
            1024,
            HeapByteBufferAllocator.getInstance()
        );
        for (int v : indices) {
            encoder.writeInt(v);
        }
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        out.write(bitWidth & 0xFF);
        encoder.toBytes().writeAllTo(out);
        return ByteBuffer.wrap(out.toByteArray());
    }

    /**
     * Minimal {@link Dictionary} implementation for testing. The real Parquet
     * {@code Dictionary} subclasses (e.g. {@code PlainBinaryDictionary}) require an
     * encoded {@code DictionaryPage} plus a {@code ColumnDescriptor} to be instantiated,
     * which is far heavier than what these unit tests need. Here we extend
     * {@link Dictionary} directly and only override the two methods exercised by
     * {@link DictionaryValueDecoder}: {@code getMaxId} and {@code decodeToBinary}.
     */
    private static final class BinaryDictionary extends Dictionary {
        private final Binary[] entries;

        BinaryDictionary(String[] values) {
            super(null);
            this.entries = new Binary[values.length];
            for (int i = 0; i < values.length; i++) {
                this.entries[i] = Binary.fromString(values[i]);
            }
        }

        @Override
        public int getMaxId() {
            return entries.length - 1;
        }

        @Override
        public Binary decodeToBinary(int id) {
            return entries[id];
        }
    }
}
