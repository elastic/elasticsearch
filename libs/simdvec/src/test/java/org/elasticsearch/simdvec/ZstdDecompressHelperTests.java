/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.simdvec;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterIndexInput;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.lucene.store.ByteArrayIndexInput;
import org.elasticsearch.common.lucene.store.DirectAccessIndexInput;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.core.DirectAccessInput;
import org.elasticsearch.nativeaccess.NativeAccess;
import org.elasticsearch.nativeaccess.Zstd;
import org.elasticsearch.test.ESTestCase;
import org.junit.BeforeClass;

import java.io.IOException;
import java.nio.ByteBuffer;

import static org.elasticsearch.simdvec.internal.vectorization.JdkFeatures.SUPPORTS_HEAP_SEGMENTS;

public class ZstdDecompressHelperTests extends ESTestCase {

    static NativeAccess nativeAccess;
    static Zstd zstd;

    @BeforeClass
    public static void checkNative() {
        nativeAccess = NativeAccess.instance();
        zstd = nativeAccess.getZstd();
        assumeTrue("native zstd required", zstd != null);
    }

    // --- Source resolution path tests ---

    // MSAI fast path: MMapDirectory provides a MemorySegmentAccessInput, so decompression
    // reads the source directly from the mmap'd segment without any copy.
    public void testDecompressViaMemorySegmentAccessInput() throws Exception {
        byte[] original = randomByteArrayOfLength(randomIntBetween(1, 8192));
        byte[] compressed = compressRaw(original);

        try (Directory dir = new MMapDirectory(createTempDir())) {
            writeData(dir, "test.bin", compressed);
            try (IndexInput in = dir.openInput("test.bin", IOContext.DEFAULT)) {
                BytesRef bytes = new BytesRef();
                int result = ZstdDecompressHelper.decompress(
                    in,
                    compressed.length,
                    original.length,
                    0,
                    original.length,
                    bytes,
                    nativeAccess,
                    zstd,
                    new byte[4096]
                );
                assertEquals(original.length, result);
                assertArrayEquals(original, BytesRef.deepCopyOf(bytes).bytes);
                assertEquals(compressed.length, in.getFilePointer());
            }
        }
    }

    // DAI fast path: DirectAccessIndexInput provides a direct ByteBuffer slice, so
    // decompression wraps it as a MemorySegment and avoids the heap copy loop.
    public void testDecompressViaDirectAccessInput() throws Exception {
        byte[] original = randomByteArrayOfLength(randomIntBetween(1, 8192));
        byte[] compressed = compressRaw(original);

        IndexInput rawIn = new ByteArrayIndexInput("test", compressed);
        IndexInput directIn = new DirectAccessIndexInput("direct", rawIn, compressed, nativeAccess);
        BytesRef bytes = new BytesRef();
        int result = ZstdDecompressHelper.decompress(
            directIn,
            compressed.length,
            original.length,
            0,
            original.length,
            bytes,
            nativeAccess,
            zstd,
            new byte[4096]
        );
        assertEquals(original.length, result);
        assertArrayEquals(original, BytesRef.deepCopyOf(bytes).bytes);
        assertEquals(compressed.length, directIn.getFilePointer());
    }

    // Plain DataInput fallback: no MSAI or DAI, so decompression copies the compressed
    // bytes into a native buffer via the copyBuffer loop before calling zstd.
    public void testDecompressViaPlainDataInput() throws Exception {
        byte[] original = randomByteArrayOfLength(randomIntBetween(1, 8192));
        byte[] compressed = compressRaw(original);

        IndexInput plainIn = new ByteArrayIndexInput("test", compressed);
        BytesRef bytes = new BytesRef();
        int result = ZstdDecompressHelper.decompress(
            plainIn,
            compressed.length,
            original.length,
            0,
            original.length,
            bytes,
            nativeAccess,
            zstd,
            new byte[4096]
        );
        assertEquals(original.length, result);
        assertArrayEquals(original, BytesRef.deepCopyOf(bytes).bytes);
        assertEquals(compressed.length, plainIn.getFilePointer());
    }

    // DAI is present but withByteBufferSlice returns false, forcing the fallback
    // through copyAndDecompress as if the input were a plain DataInput.
    public void testDecompressFallbackWhenDirectAccessUnavailable() throws Exception {
        byte[] original = randomByteArrayOfLength(randomIntBetween(1, 8192));
        byte[] compressed = compressRaw(original);

        IndexInput rawIn = new ByteArrayIndexInput("test", compressed);
        IndexInput failingIn = new FailingDirectAccessIndexInput("failing-dai", rawIn);

        BytesRef bytes = new BytesRef();
        int result = ZstdDecompressHelper.decompress(
            failingIn,
            compressed.length,
            original.length,
            0,
            original.length,
            bytes,
            nativeAccess,
            zstd,
            new byte[4096]
        );
        assertEquals(original.length, result);
        assertArrayEquals(original, BytesRef.deepCopyOf(bytes).bytes);
        assertEquals(compressed.length, failingIn.getFilePointer());
    }

    // --- Sub-range extraction tests ---

    // MSAI path with offset/length: decompresses the full block, then the BytesRef
    // window should cover only the requested sub-range of the original data.
    public void testSubRangeViaMemorySegmentAccessInput() throws Exception {
        byte[] original = randomByteArrayOfLength(randomIntBetween(100, 8192));
        byte[] compressed = compressRaw(original);
        int offset = randomIntBetween(0, original.length / 2);
        int length = randomIntBetween(1, original.length - offset);

        try (Directory dir = new MMapDirectory(createTempDir())) {
            writeData(dir, "test.bin", compressed);
            try (IndexInput in = dir.openInput("test.bin", IOContext.DEFAULT)) {
                BytesRef bytes = new BytesRef();
                ZstdDecompressHelper.decompress(
                    in,
                    compressed.length,
                    original.length,
                    offset,
                    length,
                    bytes,
                    nativeAccess,
                    zstd,
                    new byte[4096]
                );
                assertSubRange(original, offset, length, bytes);
                assertEquals(compressed.length, in.getFilePointer());
            }
        }
    }

    // DAI path with offset/length: same sub-range contract as MSAI but exercising
    // the DirectAccessInput source resolution branch.
    public void testSubRangeViaDirectAccessInput() throws Exception {
        byte[] original = randomByteArrayOfLength(randomIntBetween(100, 8192));
        byte[] compressed = compressRaw(original);
        int offset = randomIntBetween(0, original.length / 2);
        int length = randomIntBetween(1, original.length - offset);

        IndexInput rawIn = new ByteArrayIndexInput("test", compressed);
        IndexInput directIn = new DirectAccessIndexInput("direct", rawIn, compressed, nativeAccess);
        BytesRef bytes = new BytesRef();
        ZstdDecompressHelper.decompress(
            directIn,
            compressed.length,
            original.length,
            offset,
            length,
            bytes,
            nativeAccess,
            zstd,
            new byte[4096]
        );
        assertSubRange(original, offset, length, bytes);
        assertEquals(compressed.length, directIn.getFilePointer());
    }

    // Plain fallback with offset/length: the copy-based path must still produce the
    // correct sub-range in the BytesRef output.
    public void testSubRangeViaPlainDataInput() throws Exception {
        byte[] original = randomByteArrayOfLength(randomIntBetween(100, 8192));
        byte[] compressed = compressRaw(original);
        int offset = randomIntBetween(0, original.length / 2);
        int length = randomIntBetween(1, original.length - offset);

        IndexInput plainIn = new ByteArrayIndexInput("test", compressed);
        BytesRef bytes = new BytesRef();
        ZstdDecompressHelper.decompress(
            plainIn,
            compressed.length,
            original.length,
            offset,
            length,
            bytes,
            nativeAccess,
            zstd,
            new byte[4096]
        );
        assertSubRange(original, offset, length, bytes);
        assertEquals(compressed.length, plainIn.getFilePointer());
    }

    // --- BytesRef.offset contract tests ---

    // On JDK 22+ (decompressToHeap), bytes.offset should equal the requested offset
    // because the full block is decompressed directly into the byte array.
    public void testBytesRefOffsetMatchesRequestOnJdk22Plus() throws Exception {
        assumeTrue("heap segment path requires JDK 22+", SUPPORTS_HEAP_SEGMENTS);

        byte[] original = randomByteArrayOfLength(randomIntBetween(100, 4096));
        byte[] compressed = compressRaw(original);
        int offset = randomIntBetween(1, original.length / 2);
        int length = randomIntBetween(1, original.length - offset);

        IndexInput plainIn = new ByteArrayIndexInput("test", compressed);
        BytesRef bytes = new BytesRef();
        ZstdDecompressHelper.decompress(
            plainIn,
            compressed.length,
            original.length,
            offset,
            length,
            bytes,
            nativeAccess,
            zstd,
            new byte[4096]
        );

        assertEquals(offset, bytes.offset);
        assertEquals(length, bytes.length);
        assertEquals(compressed.length, plainIn.getFilePointer());
    }

    // On JDK 21 (decompressToNative), bytes.offset must be 0 because the sub-range
    // is copied from the native buffer into the start of the byte array.
    public void testBytesRefOffsetIsZeroOnJdk21() throws Exception {
        assumeFalse("native buffer path requires JDK 21", SUPPORTS_HEAP_SEGMENTS);

        byte[] original = randomByteArrayOfLength(randomIntBetween(100, 4096));
        byte[] compressed = compressRaw(original);
        int offset = randomIntBetween(1, original.length / 2);
        int length = randomIntBetween(1, original.length - offset);

        IndexInput plainIn = new ByteArrayIndexInput("test", compressed);
        BytesRef bytes = new BytesRef();
        ZstdDecompressHelper.decompress(
            plainIn,
            compressed.length,
            original.length,
            offset,
            length,
            bytes,
            nativeAccess,
            zstd,
            new byte[4096]
        );

        assertEquals(0, bytes.offset);
        assertEquals(length, bytes.length);
        assertEquals(compressed.length, plainIn.getFilePointer());
    }

    // --- Edge case: copyBuffer sizing ---

    // Compressed data larger than the copyBuffer exercises the while loop in
    // copyAndDecompress, which must iterate multiple times to fill the source buffer.
    public void testLargeDataExceedingCopyBuffer() throws Exception {
        byte[] original = randomByteArrayOfLength(randomIntBetween(8192, 16384));
        byte[] compressed = compressRaw(original);

        IndexInput plainIn = new ByteArrayIndexInput("test", compressed);
        BytesRef bytes = new BytesRef();
        int result = ZstdDecompressHelper.decompress(
            plainIn,
            compressed.length,
            original.length,
            0,
            original.length,
            bytes,
            nativeAccess,
            zstd,
            new byte[4096]
        );
        assertEquals(original.length, result);
        assertArrayEquals(original, BytesRef.deepCopyOf(bytes).bytes);
        assertEquals(compressed.length, plainIn.getFilePointer());
    }

    // A tiny copyBuffer (7 bytes) stresses the loop boundary in copyAndDecompress,
    // ensuring partial reads are assembled correctly before decompression.
    public void testSmallCopyBuffer() throws Exception {
        byte[] original = randomByteArrayOfLength(randomIntBetween(100, 4096));
        byte[] compressed = compressRaw(original);

        IndexInput plainIn = new ByteArrayIndexInput("test", compressed);
        BytesRef bytes = new BytesRef();
        int result = ZstdDecompressHelper.decompress(
            plainIn,
            compressed.length,
            original.length,
            0,
            original.length,
            bytes,
            nativeAccess,
            zstd,
            new byte[7]
        );
        assertEquals(original.length, result);
        assertArrayEquals(original, BytesRef.deepCopyOf(bytes).bytes);
        assertEquals(compressed.length, plainIn.getFilePointer());
    }

    // --- helpers ---

    private byte[] compressRaw(byte[] data) {
        int bound = zstd.compressBound(data.length);
        try (var src = nativeAccess.newConfinedBuffer(data.length); var dst = nativeAccess.newConfinedBuffer(bound)) {
            src.buffer().put(0, data);
            int compressedLen = zstd.compress(dst, src, randomIntBetween(-3, 9));
            byte[] result = new byte[compressedLen];
            dst.buffer().get(0, result, 0, compressedLen);
            return result;
        }
    }

    private static void assertSubRange(byte[] original, int offset, int length, BytesRef bytes) {
        assertEquals(length, bytes.length);
        byte[] expected = new byte[length];
        System.arraycopy(original, offset, expected, 0, length);
        byte[] actual = new byte[length];
        System.arraycopy(bytes.bytes, bytes.offset, actual, 0, length);
        assertArrayEquals(expected, actual);
    }

    private static void writeData(Directory dir, String name, byte[] data) throws IOException {
        try (IndexOutput out = dir.createOutput(name, IOContext.DEFAULT)) {
            out.writeBytes(data, 0, data.length);
        }
    }

    static class FailingDirectAccessIndexInput extends FilterIndexInput implements DirectAccessInput {

        FailingDirectAccessIndexInput(String resourceDescription, IndexInput delegate) {
            super(resourceDescription, delegate);
        }

        @Override
        public boolean withByteBufferSlice(long offset, long length, CheckedConsumer<ByteBuffer, IOException> action) {
            return false;
        }

        @Override
        public boolean withByteBufferSlices(long[] offsets, int length, int count, CheckedConsumer<ByteBuffer[], IOException> action) {
            return false;
        }

        @Override
        public IndexInput clone() {
            return new FailingDirectAccessIndexInput("clone(" + toString() + ")", in.clone());
        }

        @Override
        public IndexInput slice(String sliceDescription, long offset, long length) throws IOException {
            return new FailingDirectAccessIndexInput(sliceDescription, in.slice(sliceDescription, offset, length));
        }
    }
}
