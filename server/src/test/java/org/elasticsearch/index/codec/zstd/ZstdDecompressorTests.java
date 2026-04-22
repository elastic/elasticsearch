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
import org.apache.lucene.store.ByteBuffersDataInput;
import org.apache.lucene.store.ByteBuffersDataOutput;
import org.apache.lucene.store.FilterIndexInput;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.lucene.store.ByteArrayIndexInput;
import org.elasticsearch.common.lucene.store.DirectAccessIndexInput;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.core.DirectAccessInput;
import org.elasticsearch.nativeaccess.NativeAccess;
import org.elasticsearch.test.ESTestCase;
import org.junit.BeforeClass;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

public class ZstdDecompressorTests extends ESTestCase {

    private static NativeAccess nativeAccess;

    @BeforeClass
    public static void checkNative() {
        nativeAccess = NativeAccess.instance();
        assumeTrue("native zstd required", nativeAccess.getZstd() != null);
    }

    // Exercises the DirectAccessInput fast path where withByteBufferSlice succeeds and the
    // compressed data is passed directly to zstd without an intermediate heap copy.
    public void testDecompressViaDirectAccess() throws IOException {
        byte[] original = randomByteArrayOfLength(randomIntBetween(1, 8192));
        byte[] compressed = compress(original);

        Decompressor decompressor = new ZstdCompressionMode(1).newDecompressor();
        IndexInput rawIn = new ByteArrayIndexInput("test", compressed);
        IndexInput directIn = new DirectAccessIndexInput("direct", rawIn, compressed, nativeAccess);

        BytesRef result = new BytesRef();
        decompressor.decompress(directIn, original.length, 0, original.length, result);

        assertArrayEquals(original, BytesRef.deepCopyOf(result).bytes);
    }

    // Exercises the fallback when withByteBufferSlice throws AlreadyClosedException (e.g. the blob
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

    // Exercises the fallback when the input implements DirectAccessInput but withByteBufferSlice
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
        IndexInput directIn = new DirectAccessIndexInput("direct", rawIn, compressed, nativeAccess);

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
        IndexInput directIn = new DirectAccessIndexInput("direct", rawIn, compressed, nativeAccess);

        BytesRef result = new BytesRef();
        decompressor.decompress(directIn, original.length, 0, original.length, result);

        assertEquals(compressed.length, directIn.getFilePointer());
    }

    private byte[] compress(byte[] data) throws IOException {
        ByteBuffersDataOutput output = new ByteBuffersDataOutput();
        Compressor compressor = new ZstdCompressionMode(1).newCompressor();
        compressor.compress(new ByteBuffersDataInput(List.of(ByteBuffer.wrap(data))), output);
        compressor.close();
        return output.toArrayCopy();
    }

    /**
     * An IndexInput that implements DirectAccessInput but always throws AlreadyClosedException from
     * withByteBufferSlice, simulating a blob cache region being evicted mid-read.
     */
    static class ThrowingAlreadyClosedDirectAccessIndexInput extends FilterIndexInput implements DirectAccessInput {

        ThrowingAlreadyClosedDirectAccessIndexInput(String resourceDescription, IndexInput delegate) {
            super(resourceDescription, delegate);
        }

        @Override
        public boolean withByteBufferSlice(long offset, long length, CheckedConsumer<ByteBuffer, IOException> action) {
            throw new AlreadyClosedException("no free region found");
        }

        @Override
        public boolean withByteBufferSlices(long[] offsets, int length, int count, CheckedConsumer<ByteBuffer[], IOException> action) {
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
     * An IndexInput that implements DirectAccessInput but always returns false,
     * simulating an input where direct access is not available.
     */
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
