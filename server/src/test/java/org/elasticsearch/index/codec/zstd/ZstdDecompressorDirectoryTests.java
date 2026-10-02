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
import org.apache.lucene.store.ByteBuffersDataInput;
import org.apache.lucene.store.ByteBuffersDataOutput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.store.NIOFSDirectory;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.lucene.store.DirectAccessIndexInput;
import org.elasticsearch.index.codec.zstd.ZstdCompressionMode.ZstdDecompressor;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

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

    private static ZstdDecompressor newDecompressor() {
        return (ZstdDecompressor) new ZstdCompressionMode(1).newDecompressor();
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
