/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.zstd;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.compressing.Compressor;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.store.ByteBuffersDataInput;
import org.apache.lucene.store.ByteBuffersDataOutput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.store.NIOFSDirectory;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.lucene.store.DirectAccessIndexInput;
import org.elasticsearch.index.codec.zstd.ZstdCompressionMode.ZstdDecompressor;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

import static org.hamcrest.Matchers.lessThanOrEqualTo;

// Tests ZstdDecompressor against real Directory implementations (NIOFS, MMap, DirectAccessInput)
// to exercise the different IndexInput code paths: heap copy, memory-mapped, and DirectAccessInput.
public class ZstdDecompressorDirectoryTests extends ESTestCase {

    public enum DirectoryType {
        NIOFS,
        MMAP,
        DIRECT_ACCESS
    }

    private final DirectoryType directoryType;

    public ZstdDecompressorDirectoryTests(DirectoryType directoryType) {
        this.directoryType = directoryType;
    }

    @ParametersFactory
    public static Iterable<Object[]> parametersFactory() {
        return () -> Arrays.stream(DirectoryType.values()).map(d -> new Object[] { d }).iterator();
    }

    private Directory newParametrizedDirectory() throws IOException {
        return switch (directoryType) {
            case NIOFS -> new NIOFSDirectory(createTempDir());
            case MMAP -> new MMapDirectory(createTempDir());
            case DIRECT_ACCESS -> new DirectAccessDirectory(new NIOFSDirectory(createTempDir()));
        };
    }

    /**
     * A Directory wrapper whose openInput returns a DirectAccessIndexInput, exercising the
     * DirectAccessInput code path in ZstdDecompressor without needing searchable-snapshots.
     */
    private static class DirectAccessDirectory extends FilterDirectory {
        DirectAccessDirectory(Directory delegate) {
            super(delegate);
        }

        @Override
        public IndexInput openInput(String name, IOContext context) throws IOException {
            IndexInput delegate = in.openInput(name, context);
            byte[] data = new byte[(int) in.fileLength(name)];
            try (IndexInput reader = in.openInput(name, IOContext.READONCE)) {
                reader.readBytes(data, 0, data.length);
            }
            return new DirectAccessIndexInput("direct-access(" + name + ")", delegate, data);
        }
    }

    // Decompresses the full block and verifies all bytes match.
    public void testFullDecompress() throws IOException {
        byte[] original = randomByteArrayOfLength(randomIntBetween(1, 16384));
        try (Directory dir = newParametrizedDirectory()) {
            writeCompressed(dir, "data", original);
            try (IndexInput in = dir.openInput("data", IOContext.DEFAULT)) {
                ZstdDecompressor decompressor = newDecompressor();
                BytesRef result = new BytesRef();
                decompressor.decompress(in, original.length, 0, original.length, result);
                assertEquals(0, result.offset);
                assertEquals(original.length, result.length);
                assertArrayEquals(original, BytesRef.deepCopyOf(result).bytes);
            }
        }
    }

    // Decompresses a sub-range (offset, length) and verifies only the requested slice is returned.
    public void testSubRangeDecompress() throws IOException {
        byte[] original = randomByteArrayOfLength(randomIntBetween(200, 16384));
        int offset = randomIntBetween(0, original.length / 2);
        int length = randomIntBetween(1, original.length - offset);

        try (Directory dir = newParametrizedDirectory()) {
            writeCompressed(dir, "data", original);
            try (IndexInput in = dir.openInput("data", IOContext.DEFAULT)) {
                ZstdDecompressor decompressor = newDecompressor();
                BytesRef result = new BytesRef();
                decompressor.decompress(in, original.length, offset, length, result);
                assertEquals(length, result.length);
                byte[] expected = new byte[length];
                System.arraycopy(original, offset, expected, 0, length);
                assertArrayEquals(expected, BytesRef.deepCopyOf(result).bytes);
            }
        }
    }

    // Verifies the reuse path: when bytes.bytes is already large enough, decompressDirect is used
    // and the result is a view (offset, length) into the existing buffer.
    public void testReusePath() throws IOException {
        byte[] original = randomByteArrayOfLength(randomIntBetween(200, 16384));
        int offset = randomIntBetween(1, original.length / 2);
        int length = randomIntBetween(1, original.length - offset);

        try (Directory dir = newParametrizedDirectory()) {
            writeCompressed(dir, "data", original);
            try (IndexInput in = dir.openInput("data", IOContext.DEFAULT)) {
                for (int overSize : new int[] { 0, 512, randomIntBetween(1, 512) }) {
                    in.seek(0L);
                    ZstdDecompressor decompressor = newDecompressor();
                    BytesRef result = new BytesRef(new byte[original.length + overSize]);
                    final byte[] originalBytesRefArray = result.bytes;
                    decompressor.decompress(in, original.length, offset, length, result);
                    assertEquals(offset, result.offset);
                    assertEquals(length, result.length);
                    byte[] expected = new byte[length];
                    System.arraycopy(original, offset, expected, 0, length);
                    byte[] actual = new byte[length];
                    System.arraycopy(result.bytes, result.offset, actual, 0, length);
                    assertArrayEquals(expected, actual);
                    // reuse should retain the same byte array instance
                    assertTrue(result.bytes == originalBytesRefArray);
                }
            }
        }
    }

    // Verifies that the file pointer advances past the compressed data after decompression.
    public void testFilePointerAdvanced() throws IOException {
        byte[] original = randomByteArrayOfLength(randomIntBetween(1, 4096));
        try (Directory dir = newParametrizedDirectory()) {
            writeCompressed(dir, "data", original);
            try (IndexInput in = dir.openInput("data", IOContext.DEFAULT)) {
                ZstdDecompressor decompressor = newDecompressor();
                BytesRef result = new BytesRef();
                decompressor.decompress(in, original.length, 0, original.length, result);
                assertEquals(in.length() - CodecUtil.footerLength(), in.getFilePointer());
            }
        }
    }

    // Verifies that the BytesRef buffer is correctly reused across multiple decompressions.
    public void testMultipleDecompressionsReuseBuffer() throws IOException {
        byte[] data1 = randomByteArrayOfLength(randomIntBetween(100, 4096));
        byte[] data2 = randomByteArrayOfLength(randomIntBetween(100, 4096));

        ZstdDecompressor decompressor = newDecompressor();
        BytesRef result = new BytesRef();

        try (Directory dir1 = newParametrizedDirectory()) {
            writeCompressed(dir1, "data1", data1);
            try (IndexInput in1 = dir1.openInput("data1", IOContext.DEFAULT)) {
                decompressor.decompress(in1, data1.length, 0, data1.length, result);
                assertArrayEquals(data1, BytesRef.deepCopyOf(result).bytes);
            }
        }

        try (Directory dir2 = newParametrizedDirectory()) {
            writeCompressed(dir2, "data2", data2);
            try (IndexInput in2 = dir2.openInput("data2", IOContext.DEFAULT)) {
                decompressor.decompress(in2, data2.length, 0, data2.length, result);
                assertArrayEquals(data2, BytesRef.deepCopyOf(result).bytes);
            }
        }
    }

    // Reads several parts of one block in a row, like the documents of a stored fields chunk, then another block, then the first
    // block again. The block is decompressed off-heap on the first read and kept on heap from the second one.
    public void testRepeatedPartialReads() throws IOException {
        byte[][] blocks = randomBlocks(2);
        try (Directory dir = newParametrizedDirectory()) {
            long[] filePointers = writeBlocks(dir, "data", blocks);
            try (IndexInput in = dir.openInput("data", IOContext.DEFAULT)) {
                ZstdDecompressor decompressor = newDecompressor();
                assertPartialRead(decompressor, in, filePointers, blocks, 0);
                assertFalse(decompressor.hasCachedBlock());
                int moreReads = randomIntBetween(2, 10);
                for (int i = 0; i < moreReads; i++) {
                    assertPartialRead(decompressor, in, filePointers, blocks, 0);
                    assertTrue(decompressor.hasCachedBlock());
                }
                assertPartialRead(decompressor, in, filePointers, blocks, 1);
                assertPartialRead(decompressor, in, filePointers, blocks, 0);
                assertTrue(decompressor.hasCachedBlock());
                assertThat(decompressor.retainedBytes(), lessThanOrEqualTo(ArrayUtil.oversize(maxLength(blocks), Byte.BYTES)));
            }
        }
    }

    // An instance that never reads part of the same block twice in a row, as for a get or scattered hits, retains nothing.
    // Whole-block reads in between do not count.
    public void testScatteredReadsRetainNothing() throws IOException {
        byte[][] blocks = randomBlocks(randomIntBetween(2, 5));
        try (Directory dir = newParametrizedDirectory()) {
            long[] filePointers = writeBlocks(dir, "data", blocks);
            try (IndexInput in = dir.openInput("data", IOContext.DEFAULT)) {
                ZstdDecompressor decompressor = newDecompressor();
                int previous = -1;
                int reads = randomIntBetween(1, 20);
                for (int i = 0; i < reads; i++) {
                    final int last = previous;
                    int block = randomValueOtherThan(last, () -> randomIntBetween(0, blocks.length - 1));
                    assertPartialRead(decompressor, in, filePointers, blocks, block);
                    if (randomBoolean()) {
                        assertFullRead(decompressor, in, filePointers, blocks, randomIntBetween(0, blocks.length - 1));
                    }
                    assertFalse(decompressor.hasCachedBlock());
                    assertEquals(0, decompressor.retainedBytes());
                    previous = block;
                }
            }
        }
    }

    // Once an instance keeps blocks on heap it keeps every block it reads part of, so reads that alternate between blocks
    // replace the cached block each time and must still return the right bytes.
    public void testAlternatingBlocksAfterCaching() throws IOException {
        byte[][] blocks = randomBlocks(randomIntBetween(2, 4));
        try (Directory dir = newParametrizedDirectory()) {
            long[] filePointers = writeBlocks(dir, "data", blocks);
            try (IndexInput in = dir.openInput("data", IOContext.DEFAULT)) {
                ZstdDecompressor decompressor = newDecompressor();
                int first = randomIntBetween(0, blocks.length - 1);
                assertPartialRead(decompressor, in, filePointers, blocks, first);
                assertPartialRead(decompressor, in, filePointers, blocks, first);
                assertTrue(decompressor.hasCachedBlock());
                int reads = randomIntBetween(10, 50);
                for (int i = 0; i < reads; i++) {
                    if (randomBoolean()) {
                        assertFullRead(decompressor, in, filePointers, blocks, randomIntBetween(0, blocks.length - 1));
                    }
                    assertPartialRead(decompressor, in, filePointers, blocks, randomIntBetween(0, blocks.length - 1));
                    assertTrue(decompressor.hasCachedBlock());
                }
                assertThat(decompressor.retainedBytes(), lessThanOrEqualTo(ArrayUtil.oversize(maxLength(blocks), Byte.BYTES)));
            }
        }
    }

    // A block of the same length at the same file pointer of another input must not be served from the cached block. A clone of
    // the input holds the same bytes but is a different input too, and must be left positioned after the block.
    public void testOtherInputAtSameFilePointer() throws IOException {
        int length = randomIntBetween(100, 16384);
        byte[][] blocks = { randomByteArrayOfLength(length), randomByteArrayOfLength(randomIntBetween(100, 16384)) };
        byte[][] otherBlocks = { randomByteArrayOfLength(length), randomByteArrayOfLength(randomIntBetween(100, 16384)) };
        try (Directory dir = newParametrizedDirectory()) {
            long[] filePointers = writeBlocks(dir, "data", blocks);
            long[] otherFilePointers = writeBlocks(dir, "other", otherBlocks);
            assertEquals(filePointers[0], otherFilePointers[0]);
            try (IndexInput in = dir.openInput("data", IOContext.DEFAULT); IndexInput other = dir.openInput("other", IOContext.DEFAULT)) {
                ZstdDecompressor decompressor = newDecompressor();
                assertPartialRead(decompressor, in, filePointers, blocks, 0);
                assertPartialRead(decompressor, in, filePointers, blocks, 0);
                assertTrue(decompressor.hasCachedBlock());
                assertPartialRead(decompressor, other, otherFilePointers, otherBlocks, 0, 0, length - 1);
                assertPartialRead(decompressor, in, filePointers, blocks, 0, 0, length - 1);
                IndexInput clone = in.clone();
                assertPartialRead(decompressor, clone, filePointers, blocks, 0);
                assertPartialRead(decompressor, in, filePointers, blocks, 0);
                assertPartialRead(decompressor, clone, filePointers, blocks, 0);
            }
        }
    }

    // If decompressing a block fails while it is being cached, the partly overwritten buffer must not be served afterwards.
    public void testFailureWhileCachingLeavesNoStaleBlock() throws IOException {
        int length = randomIntBetween(100, 16384);
        byte[][] blocks = { randomByteArrayOfLength(length), randomByteArrayOfLength(length) };
        try (Directory dir = newParametrizedDirectory()) {
            long[] filePointers = writeBlocks(dir, "data", blocks);
            try (IndexInput in = dir.openInput("data", IOContext.DEFAULT)) {
                ZstdDecompressor decompressor = newDecompressor();
                assertPartialRead(decompressor, in, filePointers, blocks, 0);
                assertPartialRead(decompressor, in, filePointers, blocks, 0);
                assertTrue(decompressor.hasCachedBlock());
                // claim a longer block than was written: the second block is decompressed into the cached buffer, then rejected
                in.seek(filePointers[1]);
                expectThrows(CorruptIndexException.class, () -> decompressor.decompress(in, length + 1, 1, length - 1, new BytesRef()));
                assertFalse(decompressor.hasCachedBlock());
                assertPartialRead(decompressor, in, filePointers, blocks, 0, 0, length - 1);
                assertTrue(decompressor.hasCachedBlock());
            }
        }
    }

    // A clone of a decompressor starts without a cached block and caches independently of the original.
    public void testCloneStartsEmpty() throws IOException {
        byte[][] blocks = randomBlocks(2);
        try (Directory dir = newParametrizedDirectory()) {
            long[] filePointers = writeBlocks(dir, "data", blocks);
            try (IndexInput in = dir.openInput("data", IOContext.DEFAULT)) {
                ZstdDecompressor decompressor = newDecompressor();
                assertPartialRead(decompressor, in, filePointers, blocks, 0);
                assertPartialRead(decompressor, in, filePointers, blocks, 0);
                assertTrue(decompressor.hasCachedBlock());

                ZstdDecompressor clone = (ZstdDecompressor) decompressor.clone();
                assertFalse(clone.hasCachedBlock());
                assertEquals(0, clone.retainedBytes());
                IndexInput inClone = in.clone();
                assertPartialRead(clone, inClone, filePointers, blocks, 0);
                assertFalse(clone.hasCachedBlock());
                assertPartialRead(clone, inClone, filePointers, blocks, 1);
                assertPartialRead(clone, inClone, filePointers, blocks, 1);
                assertTrue(clone.hasCachedBlock());

                assertPartialRead(decompressor, in, filePointers, blocks, 0);
                assertTrue(decompressor.hasCachedBlock());
            }
        }
    }

    private static ZstdDecompressor newDecompressor() {
        return (ZstdDecompressor) new ZstdCompressionMode(1).newDecompressor();
    }

    private static byte[][] randomBlocks(int count) {
        byte[][] blocks = new byte[count][];
        for (int i = 0; i < count; i++) {
            blocks[i] = randomByteArrayOfLength(randomIntBetween(100, 16384));
        }
        return blocks;
    }

    private static int maxLength(byte[][] blocks) {
        return Arrays.stream(blocks).mapToInt(b -> b.length).max().orElseThrow();
    }

    private static void assertPartialRead(ZstdDecompressor decompressor, IndexInput in, long[] filePointers, byte[][] blocks, int block)
        throws IOException {
        int offset = randomIntBetween(0, blocks[block].length - 2);
        int length = randomIntBetween(1, blocks[block].length - offset - (offset == 0 ? 1 : 0));
        assertPartialRead(decompressor, in, filePointers, blocks, block, offset, length);
    }

    /**
     * Reads part of a block like Lucene's stored fields reader reads a document: it seeks to the block and passes a new
     * {@link BytesRef}. Checks the bytes and that the input is left positioned after the block.
     */
    private static void assertPartialRead(
        ZstdDecompressor decompressor,
        IndexInput in,
        long[] filePointers,
        byte[][] blocks,
        int block,
        int offset,
        int length
    ) throws IOException {
        byte[] original = blocks[block];
        assertFalse("not a partial read", offset == 0 && length == original.length);
        in.seek(filePointers[block]);
        BytesRef bytes = new BytesRef();
        decompressor.decompress(in, original.length, offset, length, bytes);
        assertEquals(length, bytes.length);
        assertArrayEquals(Arrays.copyOfRange(original, offset, offset + length), BytesRef.deepCopyOf(bytes).bytes);
        assertEquals(filePointers[block + 1], in.getFilePointer());
    }

    private static void assertFullRead(ZstdDecompressor decompressor, IndexInput in, long[] filePointers, byte[][] blocks, int block)
        throws IOException {
        byte[] original = blocks[block];
        in.seek(filePointers[block]);
        BytesRef bytes = new BytesRef();
        decompressor.decompress(in, original.length, 0, original.length, bytes);
        assertArrayEquals(original, BytesRef.deepCopyOf(bytes).bytes);
        assertEquals(filePointers[block + 1], in.getFilePointer());
    }

    /**
     * Writes the blocks one after the other, like a stored fields data file, and returns the file pointer of each block followed
     * by the file pointer after the last one.
     */
    private static long[] writeBlocks(Directory dir, String name, byte[]... blocks) throws IOException {
        long[] filePointers = new long[blocks.length + 1];
        try (
            IndexOutput out = dir.createOutput(name, IOContext.DEFAULT);
            Compressor compressor = new ZstdCompressionMode(1).newCompressor()
        ) {
            for (int i = 0; i < blocks.length; i++) {
                filePointers[i] = out.getFilePointer();
                compressor.compress(new ByteBuffersDataInput(List.of(ByteBuffer.wrap(blocks[i]))), out);
            }
            filePointers[blocks.length] = out.getFilePointer();
            CodecUtil.writeFooter(out);
        }
        return filePointers;
    }

    private static void writeCompressed(Directory dir, String name, byte[] data) throws IOException {
        ByteBuffersDataOutput compressedOutput = new ByteBuffersDataOutput();
        Compressor compressor = new ZstdCompressionMode(1).newCompressor();
        compressor.compress(new ByteBuffersDataInput(List.of(ByteBuffer.wrap(data))), compressedOutput);
        compressor.close();
        byte[] compressed = compressedOutput.toArrayCopy();

        try (IndexOutput out = dir.createOutput(name, IOContext.DEFAULT)) {
            out.writeBytes(compressed, compressed.length);
            CodecUtil.writeFooter(out);
        }
    }
}
