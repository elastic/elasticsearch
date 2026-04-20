/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.parquet;

import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.compression.CompressionCodecFactory.BytesInputCompressor;
import org.apache.parquet.compression.CompressionCodecFactory.BytesInputDecompressor;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.nio.ByteBuffer;

public class PlainCompressionCodecFactoryTests extends ESTestCase {

    private PlainCompressionCodecFactory factory;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        factory = new PlainCompressionCodecFactory();
    }

    @Override
    public void tearDown() throws Exception {
        factory.release();
        super.tearDown();
    }

    public void testUncompressedRoundTrip() throws IOException {
        assertRoundTrip(CompressionCodecName.UNCOMPRESSED);
    }

    public void testSnappyRoundTrip() throws IOException {
        assertRoundTrip(CompressionCodecName.SNAPPY);
    }

    public void testGzipRoundTrip() throws IOException {
        assertRoundTrip(CompressionCodecName.GZIP);
    }

    public void testZstdRoundTrip() throws IOException {
        assertRoundTrip(CompressionCodecName.ZSTD);
    }

    public void testLz4RawRoundTrip() throws IOException {
        assertRoundTrip(CompressionCodecName.LZ4_RAW);
    }

    public void testLz4HadoopFramedUnsupported() {
        expectThrows(UnsupportedOperationException.class, () -> factory.getDecompressor(CompressionCodecName.LZ4));
        expectThrows(UnsupportedOperationException.class, () -> factory.getCompressor(CompressionCodecName.LZ4));
    }

    public void testDirectByteBufferDecompressionSnappy() throws IOException {
        assertDirectByteBufferRoundTrip(CompressionCodecName.SNAPPY);
    }

    public void testDirectByteBufferDecompressionGzip() throws IOException {
        assertDirectByteBufferRoundTrip(CompressionCodecName.GZIP);
    }

    public void testDirectByteBufferDecompressionZstd() throws IOException {
        assertDirectByteBufferRoundTrip(CompressionCodecName.ZSTD);
    }

    public void testDirectByteBufferDecompressionLz4Raw() throws IOException {
        assertDirectByteBufferRoundTrip(CompressionCodecName.LZ4_RAW);
    }

    private void assertDirectByteBufferRoundTrip(CompressionCodecName codec) throws IOException {
        BytesInputCompressor compressor = factory.getCompressor(codec);
        byte[] original = randomByteArrayOfLength(between(100, 4096));
        BytesInput compressed = compressor.compress(BytesInput.from(original));
        byte[] compressedBytes = compressed.toByteArray();

        ByteBuffer input = ByteBuffer.allocateDirect(compressedBytes.length);
        input.put(compressedBytes);
        input.flip();
        ByteBuffer output = ByteBuffer.allocateDirect(original.length);

        BytesInputDecompressor decompressor = factory.getDecompressor(codec);
        decompressor.decompress(input, compressedBytes.length, output, original.length);
        output.flip();

        byte[] result = new byte[original.length];
        output.get(result);
        assertArrayEquals("Direct ByteBuffer round-trip failed for " + codec, original, result);
    }

    public void testHeapByteBufferDecompression() throws IOException {
        for (CompressionCodecName codec : new CompressionCodecName[] {
            CompressionCodecName.SNAPPY,
            CompressionCodecName.GZIP,
            CompressionCodecName.ZSTD,
            CompressionCodecName.LZ4_RAW }) {

            BytesInputCompressor compressor = factory.getCompressor(codec);
            byte[] original = randomByteArrayOfLength(between(100, 4096));
            BytesInput compressed = compressor.compress(BytesInput.from(original));
            byte[] compressedBytes = compressed.toByteArray();

            ByteBuffer input = ByteBuffer.wrap(compressedBytes);
            ByteBuffer output = ByteBuffer.allocate(original.length);

            BytesInputDecompressor decompressor = factory.getDecompressor(codec);
            decompressor.decompress(input, compressedBytes.length, output, original.length);
            output.flip();

            byte[] result = new byte[original.length];
            output.get(result);
            assertArrayEquals("Heap ByteBuffer round-trip failed for " + codec, original, result);
        }
    }

    public void testDecompressorsCached() {
        BytesInputDecompressor d1 = factory.getDecompressor(CompressionCodecName.SNAPPY);
        BytesInputDecompressor d2 = factory.getDecompressor(CompressionCodecName.SNAPPY);
        assertSame(d1, d2);
    }

    public void testCompressorsCached() {
        BytesInputCompressor c1 = factory.getCompressor(CompressionCodecName.SNAPPY);
        BytesInputCompressor c2 = factory.getCompressor(CompressionCodecName.SNAPPY);
        assertSame(c1, c2);
    }

    public void testUnsupportedCodecThrows() {
        expectThrows(UnsupportedOperationException.class, () -> factory.getDecompressor(CompressionCodecName.BROTLI));
        expectThrows(UnsupportedOperationException.class, () -> factory.getCompressor(CompressionCodecName.BROTLI));
    }

    public void testReleaseClears() {
        factory.getDecompressor(CompressionCodecName.SNAPPY);
        factory.getCompressor(CompressionCodecName.GZIP);
        factory.release();
        BytesInputDecompressor d = factory.getDecompressor(CompressionCodecName.SNAPPY);
        assertNotNull(d);
    }

    private void assertRoundTrip(CompressionCodecName codec) throws IOException {
        BytesInputCompressor compressor = factory.getCompressor(codec);
        BytesInputDecompressor decompressor = factory.getDecompressor(codec);

        byte[] original = randomByteArrayOfLength(between(100, 4096));
        BytesInput compressed = compressor.compress(BytesInput.from(original));
        BytesInput decompressed = decompressor.decompress(compressed, original.length);

        assertArrayEquals("Round-trip failed for " + codec, original, decompressed.toByteArray());
    }
}
