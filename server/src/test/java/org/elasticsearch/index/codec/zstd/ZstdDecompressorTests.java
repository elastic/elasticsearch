/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.zstd;

import org.apache.lucene.codecs.compressing.Compressor;
import org.apache.lucene.codecs.compressing.Decompressor;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.store.ByteBuffersDataInput;
import org.apache.lucene.store.ByteBuffersDataOutput;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.FilterIndexInput;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.lucene.store.ByteArrayIndexInput;
import org.elasticsearch.common.lucene.store.DirectAccessIndexInput;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.core.DirectAccessInput;
import org.elasticsearch.index.codec.zstd.ZstdCompressionMode.ZstdDecompressor;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.function.IntFunction;

public class ZstdDecompressorTests extends ESTestCase {

    // Exercises the DirectAccessInput fast path where withMemorySegmentSlice succeeds and the
    // compressed data is passed directly to zstd without an intermediate heap copy.
    public void testDecompressViaDirectAccess() throws IOException {
        byte[] original = randomByteArrayOfLength(randomIntBetween(1, 8192));
        byte[] compressed = compress(original);

        Decompressor decompressor = new ZstdCompressionMode(1).newDecompressor();
        IndexInput rawIn = new ByteArrayIndexInput("test", compressed);
        IndexInput directIn = new DirectAccessIndexInput("direct", rawIn, compressed);

        BytesRef result = new BytesRef();
        decompressor.decompress(directIn, original.length, 0, original.length, result);

        assertArrayEquals(original, BytesRef.deepCopyOf(result).bytes);
    }

    // Exercises the fallback when withMemorySegmentSlice throws AlreadyClosedException (e.g. the blob
    // cache region was evicted mid-read). Decompression must still succeed via copyAndDecompress.
    public void testDecompressFallbackWhenDirectAccessThrowsAlreadyClosed() throws IOException {
        byte[] original = randomByteArrayOfLength(randomIntBetween(1, 8192));
        byte[] compressed = compress(original);

        Decompressor decompressor = new ZstdCompressionMode(1).newDecompressor();
        IndexInput rawIn = new ByteArrayIndexInput("test", compressed);
        IndexInput throwingDirectIn = new ThrowingAlreadyClosedDirectAccessIndexInput("throwing-dai", rawIn);

        BytesRef result = new BytesRef();
        decompressor.decompress(throwingDirectIn, original.length, 0, original.length, result);

        assertArrayEquals(original, BytesRef.deepCopyOf(result).bytes);
    }

    // Exercises the fallback when the input implements DirectAccessInput but withMemorySegmentSlice
    // returns false, forcing decompression through the copy-based copyAndDecompress path.
    public void testDecompressFallbackWhenDirectAccessUnavailable() throws IOException {
        byte[] original = randomByteArrayOfLength(randomIntBetween(1, 8192));
        byte[] compressed = compress(original);

        Decompressor decompressor = new ZstdCompressionMode(1).newDecompressor();
        IndexInput rawIn = new ByteArrayIndexInput("test", compressed);
        IndexInput failingDirectIn = new FailingDirectAccessIndexInput("failing-dai", rawIn);

        BytesRef result = new BytesRef();
        decompressor.decompress(failingDirectIn, original.length, 0, original.length, result);

        assertArrayEquals(original, BytesRef.deepCopyOf(result).bytes);
    }

    // Exercises the plain DataInput path where the input does not implement DirectAccessInput,
    // so decompression always goes through the existing copy-based path.
    public void testDecompressWithPlainDataInput() throws IOException {
        byte[] original = randomByteArrayOfLength(randomIntBetween(1, 8192));
        byte[] compressed = compress(original);

        Decompressor decompressor = new ZstdCompressionMode(1).newDecompressor();
        IndexInput plainIn = new ByteArrayIndexInput("test", compressed);

        BytesRef result = new BytesRef();
        decompressor.decompress(plainIn, original.length, 0, original.length, result);

        assertArrayEquals(original, BytesRef.deepCopyOf(result).bytes);
    }

    // Verifies that offset and length parameters correctly extract a sub-range of the
    // decompressed output when using the DirectAccessInput fast path.
    public void testDecompressSubRangeViaDirectAccess() throws IOException {
        byte[] original = randomByteArrayOfLength(randomIntBetween(100, 8192));
        byte[] compressed = compress(original);

        int offset = randomIntBetween(0, original.length / 2);
        int length = randomIntBetween(1, original.length - offset);

        Decompressor decompressor = new ZstdCompressionMode(1).newDecompressor();
        IndexInput rawIn = new ByteArrayIndexInput("test", compressed);
        IndexInput directIn = new DirectAccessIndexInput("direct", rawIn, compressed);

        BytesRef result = new BytesRef();
        decompressor.decompress(directIn, original.length, offset, length, result);

        assertEquals(length, result.length);
        byte[] expected = new byte[length];
        System.arraycopy(original, offset, expected, 0, length);
        assertArrayEquals(expected, BytesRef.deepCopyOf(result).bytes);
    }

    // Checks that the file pointer is positioned at the end of the compressed data after
    // decompression via the direct path, which uses an explicit seek rather than sequential reads.
    public void testFilePointerAdvancedAfterDirectAccessDecompress() throws IOException {
        byte[] original = randomByteArrayOfLength(randomIntBetween(1, 4096));
        byte[] compressed = compress(original);

        Decompressor decompressor = new ZstdCompressionMode(1).newDecompressor();
        IndexInput rawIn = new ByteArrayIndexInput("test", compressed);
        IndexInput directIn = new DirectAccessIndexInput("direct", rawIn, compressed);

        BytesRef result = new BytesRef();
        decompressor.decompress(directIn, original.length, 0, original.length, result);

        assertEquals(compressed.length, directIn.getFilePointer());
    }

    // Verifies the direct path: decompressDirect writes into bytes.bytes and sets offset/length.
    public void testDecompressDirect() throws IOException {
        byte[] original = randomByteArrayOfLength(randomIntBetween(100, 8192));
        byte[] compressed = compress(original);

        int offset = randomIntBetween(0, original.length / 2);
        int length = randomIntBetween(1, original.length - offset);

        ZstdDecompressor decompressor = newDecompressor();
        DataInput in = new ByteArrayIndexInput("test", compressed);
        int compressedLength = in.readVInt();

        BytesRef bytes = new BytesRef(new byte[original.length]);
        decompressor.decompressDirect(in, compressedLength, original.length, offset, length, bytes);

        assertEquals(offset, bytes.offset);
        assertEquals(length, bytes.length);
        byte[] expected = new byte[length];
        System.arraycopy(original, offset, expected, 0, length);
        byte[] actual = new byte[length];
        System.arraycopy(bytes.bytes, bytes.offset, actual, 0, length);
        assertArrayEquals(expected, actual);
    }

    // Verifies that decompressSlice uses a temp buffer and copies only the needed range.
    public void testDecompressSlice() throws IOException {
        byte[] original = randomByteArrayOfLength(randomIntBetween(100, 8192));
        byte[] compressed = compress(original);

        int offset = randomIntBetween(1, original.length / 2);
        int length = randomIntBetween(1, original.length - offset);

        ZstdDecompressor decompressor = newDecompressor();
        DataInput in = new ByteArrayIndexInput("test", compressed);
        int compressedLength = in.readVInt();

        BytesRef bytes = new BytesRef();
        decompressor.decompressSlice(in, compressedLength, original.length, offset, length, bytes);

        assertEquals(0, bytes.offset);
        assertEquals(length, bytes.length);
        byte[] expected = new byte[length];
        System.arraycopy(original, offset, expected, 0, length);
        assertArrayEquals(expected, BytesRef.deepCopyOf(bytes).bytes);
    }

    // Verifies that decompressSlice does not grow bytes.bytes beyond what is needed.
    public void testDecompressSliceDoesNotRetainFullChunk() throws IOException {
        byte[] original = randomByteArrayOfLength(randomIntBetween(1000, 8192));
        byte[] compressed = compress(original);

        int length = randomIntBetween(1, original.length / 4);
        int offset = randomIntBetween(0, original.length - length);

        ZstdDecompressor decompressor = newDecompressor();
        DataInput in = new ByteArrayIndexInput("test", compressed);
        int compressedLength = in.readVInt();

        BytesRef bytes = new BytesRef();
        decompressor.decompressSlice(in, compressedLength, original.length, offset, length, bytes);

        assertTrue("bytes.bytes should be sized to length, not originalLength", bytes.bytes.length < original.length);
    }

    // Verifies that the full-decompress fast path is chosen when offset==0 and length==originalLength.
    public void testFullDecompressPath() throws IOException {
        byte[] original = randomByteArrayOfLength(randomIntBetween(1, 8192));
        byte[] compressed = compress(original);

        Decompressor decompressor = new ZstdCompressionMode(1).newDecompressor();
        IndexInput in = new ByteArrayIndexInput("test", compressed);

        BytesRef result = new BytesRef();
        decompressor.decompress(in, original.length, 0, original.length, result);

        assertEquals(0, result.offset);
        assertEquals(original.length, result.length);
        assertArrayEquals(original, BytesRef.deepCopyOf(result).bytes);
    }

    // Verifies the reuse path: when bytes.bytes is already large enough, decompressDirect is used.
    public void testReusePathWhenBufferAlreadyLargeEnough() throws IOException {
        byte[] original = randomByteArrayOfLength(randomIntBetween(100, 4096));
        byte[] compressed = compress(original);

        int offset = randomIntBetween(1, original.length / 2);
        int length = randomIntBetween(1, original.length - offset);

        Decompressor decompressor = new ZstdCompressionMode(1).newDecompressor();
        IndexInput in = new ByteArrayIndexInput("test", compressed);

        // Pre-allocate a buffer larger than originalLength to trigger the reuse path
        BytesRef result = new BytesRef(new byte[original.length + 1024]);
        decompressor.decompress(in, original.length, offset, length, result);

        assertEquals(offset, result.offset);
        assertEquals(length, result.length);
        byte[] expected = new byte[length];
        System.arraycopy(original, offset, expected, 0, length);
        byte[] actual = new byte[length];
        System.arraycopy(result.bytes, result.offset, actual, 0, length);
        assertArrayEquals(expected, actual);
    }

    // Verifies copyAndDecompress: plain DataInput with no IndexInput features.
    public void testCopyAndDecompress() throws IOException {
        byte[] original = randomByteArrayOfLength(randomIntBetween(1, 8192));
        byte[] compressed = compress(original);

        DataInput in = new ByteArrayIndexInput("test", compressed);
        int compressedLength = in.readVInt();

        byte[] dst = new byte[original.length];
        MemorySegment dstSegment = MemorySegment.ofArray(dst);
        int decompressedLen = ZstdDecompressor.copyAndDecompress(in, compressedLength, dstSegment);

        assertEquals(original.length, decompressedLen);
        assertArrayEquals(original, dst);
    }

    // Verifies checkLength throws CorruptIndexException on mismatch.
    public void testCheckLengthThrowsOnMismatch() {
        DataInput dummyIn = new ByteArrayIndexInput("test", new byte[0]);
        expectThrows(org.apache.lucene.index.CorruptIndexException.class, () -> ZstdDecompressor.checkLength(99, 100, dummyIn));
    }

    // Verifies checkLength does not throw when lengths match.
    public void testCheckLengthPassesOnMatch() throws Exception {
        DataInput dummyIn = new ByteArrayIndexInput("test", new byte[0]);
        ZstdDecompressor.checkLength(100, 100, dummyIn);
    }

    // Partial reads from a DataInput that is not an IndexInput have no file pointer to identify the block, so they are never cached.
    public void testPartialReadsFromDataInputAreNotCached() throws IOException {
        byte[] original = randomByteArrayOfLength(randomIntBetween(100, 8192));
        byte[] compressed = compress(original);

        ZstdDecompressor decompressor = newDecompressor();
        ByteArrayDataInput in = new ByteArrayDataInput(compressed);
        int reads = randomIntBetween(2, 5);
        for (int i = 0; i < reads; i++) {
            in.reset(compressed);
            int offset = randomIntBetween(1, original.length / 2);
            int length = randomIntBetween(1, original.length - offset);
            BytesRef bytes = new BytesRef();
            decompressor.decompress(in, original.length, offset, length, bytes);
            assertArrayEquals(Arrays.copyOfRange(original, offset, offset + length), BytesRef.deepCopyOf(bytes).bytes);
            assertEquals(compressed.length, in.getPosition());
            assertFalse(decompressor.hasCachedBlock());
            assertEquals(0, decompressor.retainedBytes());
        }
    }

    // Caching a block decompresses it through the same paths as other reads, including the fallbacks for a DirectAccessInput
    // that has no direct access or that throws AlreadyClosedException. Each read must leave the input after its block.
    public void testCachedReadsWithDirectAccessFallbacks() throws IOException {
        byte[][] blocks = { randomByteArrayOfLength(randomIntBetween(100, 8192)), randomByteArrayOfLength(randomIntBetween(100, 8192)) };
        byte[] first = compress(blocks[0]);
        byte[] second = compress(blocks[1]);
        byte[] compressed = new byte[first.length + second.length];
        System.arraycopy(first, 0, compressed, 0, first.length);
        System.arraycopy(second, 0, compressed, first.length, second.length);
        long[] filePointers = { 0, first.length, compressed.length };

        IndexInput rawIn = new ByteArrayIndexInput("test", compressed);
        IndexInput in = switch (randomIntBetween(0, 3)) {
            case 0 -> rawIn;
            case 1 -> new DirectAccessIndexInput("direct", rawIn, compressed);
            case 2 -> new FailingDirectAccessIndexInput("failing-dai", rawIn);
            case 3 -> new ThrowingAlreadyClosedDirectAccessIndexInput("throwing-dai", rawIn);
            default -> throw new AssertionError();
        };
        ZstdDecompressor decompressor = newDecompressor();
        for (int block : new int[] { 0, 0, 0, 1, 1, 0, 1 }) {
            int offset = randomIntBetween(1, blocks[block].length / 2);
            int length = randomIntBetween(1, blocks[block].length - offset);
            in.seek(filePointers[block]);
            BytesRef bytes = new BytesRef();
            decompressor.decompress(in, blocks[block].length, offset, length, bytes);
            assertArrayEquals(Arrays.copyOfRange(blocks[block], offset, offset + length), BytesRef.deepCopyOf(bytes).bytes);
            assertEquals(filePointers[block + 1], in.getFilePointer());
        }
        assertTrue(decompressor.hasCachedBlock());
    }

    // If reading a block fails while it is being cached, no block is left cached and the next read decompresses again.
    public void testReadFailureWhileCachingLeavesNoCachedBlock() throws IOException {
        byte[] original = randomByteArrayOfLength(randomIntBetween(100, 8192));
        byte[] compressed = compress(original);
        FailingReadIndexInput in = new FailingReadIndexInput(new ByteArrayIndexInput("test", compressed));
        ZstdDecompressor decompressor = newDecompressor();

        int offset = randomIntBetween(1, original.length / 2);
        int length = randomIntBetween(1, original.length - offset);
        byte[] expected = Arrays.copyOfRange(original, offset, offset + length);
        BytesRef bytes = new BytesRef();
        in.seek(0);
        decompressor.decompress(in, original.length, offset, length, bytes);
        assertArrayEquals(expected, BytesRef.deepCopyOf(bytes).bytes);
        assertFalse(decompressor.hasCachedBlock());

        in.fail = true;
        in.seek(0);
        expectThrows(IOException.class, () -> decompressor.decompress(in, original.length, offset, length, new BytesRef()));
        assertFalse(decompressor.hasCachedBlock());

        in.fail = false;
        in.seek(0);
        bytes = new BytesRef();
        decompressor.decompress(in, original.length, offset, length, bytes);
        assertArrayEquals(expected, BytesRef.deepCopyOf(bytes).bytes);
        assertEquals(compressed.length, in.getFilePointer());
        assertTrue(decompressor.hasCachedBlock());
    }

    private byte[] compress(byte[] data) throws IOException {
        ByteBuffersDataOutput output = new ByteBuffersDataOutput();
        Compressor compressor = new ZstdCompressionMode(1).newCompressor();
        compressor.compress(new ByteBuffersDataInput(List.of(ByteBuffer.wrap(data))), output);
        compressor.close();
        return output.toArrayCopy();
    }

    private static ZstdDecompressor newDecompressor() {
        return (ZstdDecompressor) new ZstdCompressionMode(1).newDecompressor();
    }

    /**
     * An IndexInput that implements DirectAccessInput but always throws AlreadyClosedException from
     * withMemorySegmentSlice, simulating a blob cache region being evicted mid-read.
     */
    static class ThrowingAlreadyClosedDirectAccessIndexInput extends FilterIndexInput implements DirectAccessInput {

        ThrowingAlreadyClosedDirectAccessIndexInput(String resourceDescription, IndexInput delegate) {
            super(resourceDescription, delegate);
        }

        @Override
        public boolean withMemorySegmentSlice(
            long offset,
            long length,
            IntFunction<MemorySegment> scratch,
            CheckedConsumer<MemorySegment, IOException> action
        ) {
            throw new AlreadyClosedException("no free region found");
        }

        @Override
        public boolean withSliceAddresses(
            long[] offsets,
            int length,
            int count,
            MemorySegment addressesScratch,
            CheckedConsumer<MemorySegment, IOException> action
        ) {
            throw new AlreadyClosedException("no free region found");
        }

        @Override
        public IndexInput clone() {
            return new ThrowingAlreadyClosedDirectAccessIndexInput("clone(" + toString() + ")", in.clone());
        }

        @Override
        public IndexInput slice(String sliceDescription, long offset, long length) throws IOException {
            return new ThrowingAlreadyClosedDirectAccessIndexInput(sliceDescription, in.slice(sliceDescription, offset, length));
        }
    }

    /**
     * An IndexInput whose bulk reads throw an IOException while {@link #fail} is set, simulating a read error in the middle of a block.
     */
    static class FailingReadIndexInput extends FilterIndexInput {

        boolean fail;

        FailingReadIndexInput(IndexInput delegate) {
            super("failing-read", delegate);
        }

        @Override
        public void readBytes(byte[] b, int offset, int len) throws IOException {
            if (fail) {
                throw new IOException("simulated read failure");
            }
            super.readBytes(b, offset, len);
        }
    }

    /**
     * An IndexInput that implements DirectAccessInput but always returns false,
     * simulating an input where direct access is not available.
     */
    static class FailingDirectAccessIndexInput extends FilterIndexInput implements DirectAccessInput {

        FailingDirectAccessIndexInput(String resourceDescription, IndexInput delegate) {
            super(resourceDescription, delegate);
        }

        @Override
        public boolean withMemorySegmentSlice(
            long offset,
            long length,
            IntFunction<MemorySegment> scratch,
            CheckedConsumer<MemorySegment, IOException> action
        ) {
            return false;
        }

        @Override
        public boolean withSliceAddresses(
            long[] offsets,
            int length,
            int count,
            MemorySegment addressesScratch,
            CheckedConsumer<MemorySegment, IOException> action
        ) {
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
