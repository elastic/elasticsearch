/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.parquet;

import io.airlift.compress.lz4.Lz4Compressor;

import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.compression.CompressionCodecFactory.BytesInputCompressor;
import org.apache.parquet.compression.CompressionCodecFactory.BytesInputDecompressor;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasource.compress.PanamaZstd;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import static org.hamcrest.Matchers.containsString;

public class PlainCompressionCodecFactoryTests extends ESTestCase {

    private PlainCompressionCodecFactory factory;

    @Before
    public void initFactory() {
        factory = new PlainCompressionCodecFactory();
    }

    @After
    public void releaseFactory() {
        factory.release();
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

    /**
     * Round-trips a payload via a test-only Hadoop-framed LZ4 compressor and the production
     * decompressor wired into the factory. The compressor is intentionally not part of
     * {@link PlainCompressionCodecFactory} — legacy {@code LZ4} is read-only — but exercising the
     * decompressor against real Hadoop-framed bytes is the only way to lock in reader correctness
     * for customer files written during the deprecation window.
     */
    public void testLz4HadoopFramedRoundTrip() throws IOException {
        LegacyLz4HadoopFramedCodecFactory writeFactory = new LegacyLz4HadoopFramedCodecFactory();
        BytesInputCompressor compressor = writeFactory.getCompressor(CompressionCodecName.LZ4);
        BytesInputDecompressor decompressor = factory.getDecompressor(CompressionCodecName.LZ4);

        byte[] original = randomByteArrayOfLength(between(100, 4096));
        BytesInput compressed = compressor.compress(BytesInput.from(original));
        BytesInput decompressed = decompressor.decompress(compressed, original.length);
        assertArrayEquals(original, decompressed.toByteArray());
    }

    /**
     * Reader must still reject {@code getCompressor(LZ4)}: the parquet-format spec deprecated the
     * Hadoop-framed codec and ES|QL must not emit it. Acceptance criterion of the feature.
     */
    public void testLz4HadoopFramedCompressorStillUnsupported() {
        expectThrows(UnsupportedOperationException.class, () -> factory.getCompressor(CompressionCodecName.LZ4));
    }

    /**
     * Verifies the decompressor honors Hadoop's frame format when an outer block carries
     * multiple sub-blocks. Parquet-mr in practice emits one sub-block per page, but the Hadoop
     * frame format permits more; a writer that splits an outer block across sub-blocks must
     * still produce a readable file. The frame bytes here are hand-crafted to force the
     * sub-block loop in the decompressor.
     */
    public void testLz4HadoopFramedMultipleSubBlocks() throws IOException {
        byte[] partA = randomByteArrayOfLength(between(64, 256));
        byte[] partB = randomByteArrayOfLength(between(64, 256));
        byte[] partC = randomByteArrayOfLength(between(64, 256));
        byte[] framed = buildMultiSubBlockFrame(new byte[][] { partA, partB, partC });

        BytesInputDecompressor decompressor = factory.getDecompressor(CompressionCodecName.LZ4);
        int total = partA.length + partB.length + partC.length;
        BytesInput decompressed = decompressor.decompress(BytesInput.from(framed), total);
        byte[] out = decompressed.toByteArray();

        byte[] expected = new byte[total];
        System.arraycopy(partA, 0, expected, 0, partA.length);
        System.arraycopy(partB, 0, expected, partA.length, partB.length);
        System.arraycopy(partC, 0, expected, partA.length + partB.length, partC.length);
        assertArrayEquals(expected, out);
    }

    public void testLz4HadoopFramedMultipleSubBlocksDirectByteBuffer() throws IOException {
        byte[] partA = randomByteArrayOfLength(between(64, 256));
        byte[] partB = randomByteArrayOfLength(between(64, 256));
        byte[] framed = buildMultiSubBlockFrame(new byte[][] { partA, partB });

        ByteBuffer input = ByteBuffer.allocateDirect(framed.length);
        input.put(framed).flip();
        int total = partA.length + partB.length;
        ByteBuffer output = ByteBuffer.allocateDirect(total);

        BytesInputDecompressor decompressor = factory.getDecompressor(CompressionCodecName.LZ4);
        decompressor.decompress(input, framed.length, output, total);
        output.flip();
        byte[] result = new byte[total];
        output.get(result);

        byte[] expected = new byte[total];
        System.arraycopy(partA, 0, expected, 0, partA.length);
        System.arraycopy(partB, 0, expected, partA.length, partB.length);
        assertArrayEquals(expected, result);
    }

    /**
     * Decompressor must defend against a caller that flips the input {@link ByteBuffer} to
     * little-endian byte order: Hadoop's frame uses BE int32 headers unconditionally, so the
     * decompressor must read them in BE regardless of the buffer's configured order.
     */
    public void testLz4HadoopFramedLittleEndianBufferOrderIgnored() throws IOException {
        LegacyLz4HadoopFramedCodecFactory writeFactory = new LegacyLz4HadoopFramedCodecFactory();
        BytesInputCompressor compressor = writeFactory.getCompressor(CompressionCodecName.LZ4);
        byte[] original = randomByteArrayOfLength(between(100, 4096));
        byte[] framed = compressor.compress(BytesInput.from(original)).toByteArray();

        ByteBuffer input = ByteBuffer.allocate(framed.length).order(ByteOrder.LITTLE_ENDIAN);
        input.put(framed).flip();
        ByteBuffer output = ByteBuffer.allocate(original.length);

        BytesInputDecompressor decompressor = factory.getDecompressor(CompressionCodecName.LZ4);
        decompressor.decompress(input, framed.length, output, original.length);
        output.flip();
        byte[] result = new byte[original.length];
        output.get(result);
        assertArrayEquals(original, result);
    }

    public void testLz4HadoopFramedTruncatedOuterHeader() {
        BytesInputDecompressor decompressor = factory.getDecompressor(CompressionCodecName.LZ4);
        // Two bytes is short of the four-byte outer length header.
        byte[] truncated = new byte[] { 0x00, 0x00 };
        IOException e = expectThrows(IOException.class, () -> decompressor.decompress(BytesInput.from(truncated), 16));
        assertTrue(e.getMessage(), e.getMessage().contains("truncated outer length header"));
    }

    public void testLz4HadoopFramedTruncatedSubBlockHeader() {
        BytesInputDecompressor decompressor = factory.getDecompressor(CompressionCodecName.LZ4);
        // Outer length declares 32 bytes, but the sub-block header is truncated to two bytes.
        byte[] framed = new byte[] { 0x00, 0x00, 0x00, 0x20, 0x00, 0x00 };
        IOException e = expectThrows(IOException.class, () -> decompressor.decompress(BytesInput.from(framed), 32));
        assertTrue(e.getMessage(), e.getMessage().contains("truncated sub-block length header"));
    }

    public void testLz4HadoopFramedInvalidOuterLength() {
        BytesInputDecompressor decompressor = factory.getDecompressor(CompressionCodecName.LZ4);
        // Outer length of 0 is rejected — Hadoop streams treat it as EOF, but parquet pages
        // never legitimately carry an empty outer block in the middle of a frame.
        byte[] framed = new byte[] { 0x00, 0x00, 0x00, 0x00 };
        IOException e = expectThrows(IOException.class, () -> decompressor.decompress(BytesInput.from(framed), 16));
        assertTrue(e.getMessage(), e.getMessage().contains("invalid outer uncompressed length"));
    }

    public void testLz4HadoopFramedInvalidSubBlockLength() {
        BytesInputDecompressor decompressor = factory.getDecompressor(CompressionCodecName.LZ4);
        // Outer length declares 32 bytes; sub-block declares 200 bytes of compressed data but only
        // 4 trailing bytes follow. The decompressor must reject the over-long sub-block before
        // handing it to aircompressor.
        byte[] framed = new byte[] { 0x00, 0x00, 0x00, 0x20, 0x00, 0x00, 0x00, (byte) 0xC8, 0x01, 0x02, 0x03, 0x04 };
        IOException e = expectThrows(IOException.class, () -> decompressor.decompress(BytesInput.from(framed), 32));
        assertTrue(e.getMessage(), e.getMessage().contains("invalid sub-block compressed length"));
    }

    /**
     * Negative-path coverage for the {@code ByteBuffer} decompress overload, which has its own
     * frame parsing loop distinct from the {@code BytesInput} path. Truncating the outer header
     * here ensures the buffer-cursor restoration in the finally block doesn't leak invalid state
     * to callers on the error path.
     */
    public void testLz4HadoopFramedTruncatedOuterHeaderDirectByteBuffer() {
        BytesInputDecompressor decompressor = factory.getDecompressor(CompressionCodecName.LZ4);
        byte[] truncated = new byte[] { 0x00, 0x00 };
        ByteBuffer input = ByteBuffer.allocateDirect(truncated.length);
        input.put(truncated).flip();
        ByteBuffer output = ByteBuffer.allocateDirect(16);
        IOException e = expectThrows(IOException.class, () -> decompressor.decompress(input, truncated.length, output, 16));
        assertTrue(e.getMessage(), e.getMessage().contains("truncated outer length header"));
    }

    public void testLz4HadoopFramedTrailingBytesRejected() throws IOException {
        LegacyLz4HadoopFramedCodecFactory writeFactory = new LegacyLz4HadoopFramedCodecFactory();
        BytesInputCompressor compressor = writeFactory.getCompressor(CompressionCodecName.LZ4);
        byte[] original = randomByteArrayOfLength(between(100, 256));
        byte[] framed = compressor.compress(BytesInput.from(original)).toByteArray();

        // Append a stray byte after the legitimate frame. The reader must reject it: parquet pages
        // are sized exactly to the compressed payload and unconsumed trailing bytes signal corruption.
        byte[] withTrailing = new byte[framed.length + 1];
        System.arraycopy(framed, 0, withTrailing, 0, framed.length);
        withTrailing[framed.length] = (byte) 0xAB;

        BytesInputDecompressor decompressor = factory.getDecompressor(CompressionCodecName.LZ4);
        IOException e = expectThrows(IOException.class, () -> decompressor.decompress(BytesInput.from(withTrailing), original.length));
        assertTrue(e.getMessage(), e.getMessage().contains("trailing bytes after frame"));
    }

    public void testDirectByteBufferDecompressionLz4HadoopFramed() throws IOException {
        assertDirectByteBufferRoundTripWithLegacyLz4Writer();
    }

    private void assertDirectByteBufferRoundTripWithLegacyLz4Writer() throws IOException {
        LegacyLz4HadoopFramedCodecFactory writeFactory = new LegacyLz4HadoopFramedCodecFactory();
        BytesInputCompressor compressor = writeFactory.getCompressor(CompressionCodecName.LZ4);
        byte[] original = randomByteArrayOfLength(between(100, 4096));
        byte[] compressedBytes = compressor.compress(BytesInput.from(original)).toByteArray();

        ByteBuffer input = ByteBuffer.allocateDirect(compressedBytes.length);
        input.put(compressedBytes).flip();
        ByteBuffer output = ByteBuffer.allocateDirect(original.length);

        BytesInputDecompressor decompressor = factory.getDecompressor(CompressionCodecName.LZ4);
        decompressor.decompress(input, compressedBytes.length, output, original.length);
        output.flip();
        byte[] result = new byte[original.length];
        output.get(result);
        assertArrayEquals(original, result);
    }

    /**
     * Builds a Hadoop {@code BlockCompressorStream} frame from raw uncompressed payload parts,
     * emitting one outer block whose declared uncompressed length covers all parts and
     * {@code parts.length} sub-blocks back to back. Used to exercise the decompressor's
     * sub-block loop on hand-crafted inputs that the simpler test compressor would never emit.
     */
    private static byte[] buildMultiSubBlockFrame(byte[][] parts) throws IOException {
        Lz4Compressor lz4 = new Lz4Compressor();
        int totalUncompressed = 0;
        byte[][] compressed = new byte[parts.length][];
        for (int i = 0; i < parts.length; i++) {
            byte[] part = parts[i];
            totalUncompressed += part.length;
            byte[] buf = new byte[lz4.maxCompressedLength(part.length)];
            int len = lz4.compress(part, 0, part.length, buf, 0, buf.length);
            compressed[i] = new byte[len];
            System.arraycopy(buf, 0, compressed[i], 0, len);
        }
        int frameLen = 4;
        for (byte[] c : compressed) {
            frameLen += 4 + c.length;
        }
        byte[] framed = new byte[frameLen];
        ByteBuffer buf = ByteBuffer.wrap(framed); // BE by default
        buf.putInt(totalUncompressed);
        for (byte[] c : compressed) {
            buf.putInt(c.length);
            buf.put(c);
        }
        return framed;
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

    /**
     * Exercises the {@code BytesInput.from(byte[], int, int)} input shape (a {@code ByteArrayBytesInput}
     * with a non-zero offset). This is the dominant shape on the S3 prefetch path: column chunks
     * arrive as byte arrays that may be sliced from a larger backing array.
     *
     * <p>Regression test for the fast path that calls {@code BytesInput.toByteBuffer()} and
     * decompresses directly from the backing array, bypassing the BAOS-backed
     * {@code BytesInput.toByteArray()} on the hot path.
     */
    public void testSnappyDecompressByteArrayWithOffset() throws IOException {
        BytesInputCompressor compressor = factory.getCompressor(CompressionCodecName.SNAPPY);
        BytesInputDecompressor decompressor = factory.getDecompressor(CompressionCodecName.SNAPPY);

        byte[] original = randomByteArrayOfLength(between(100, 4096));
        byte[] compressed = compressor.compress(BytesInput.from(original)).toByteArray();

        // Embed the compressed payload in a larger backing array with a non-zero leading offset
        // and trailing slack, so BytesInput.from(byte[], offset, length) produces a non-trivial
        // ByteArrayBytesInput.
        int leadingSlack = between(1, 32);
        int trailingSlack = between(0, 32);
        byte[] backing = new byte[leadingSlack + compressed.length + trailingSlack];
        System.arraycopy(compressed, 0, backing, leadingSlack, compressed.length);

        BytesInput offsetInput = BytesInput.from(backing, leadingSlack, compressed.length);
        BytesInput decompressed = decompressor.decompress(offsetInput, original.length);
        assertArrayEquals(original, decompressed.toByteArray());
    }

    /**
     * Exercises {@link BytesInput#from(ByteBuffer, int, int)} with a heap-backed
     * {@link ByteBuffer}. This produces a {@code ByteBufferBytesInput}, whose
     * {@code toByteBuffer()} is overridden to return a no-copy slice that still has
     * {@code hasArray() == true}; the fast path should pick that up.
     */
    public void testSnappyDecompressFromHeapByteBuffer() throws IOException {
        BytesInputCompressor compressor = factory.getCompressor(CompressionCodecName.SNAPPY);
        BytesInputDecompressor decompressor = factory.getDecompressor(CompressionCodecName.SNAPPY);

        byte[] original = randomByteArrayOfLength(between(100, 4096));
        byte[] compressed = compressor.compress(BytesInput.from(original)).toByteArray();

        ByteBuffer compressedBuffer = ByteBuffer.wrap(compressed);
        BytesInput heapBufferInput = BytesInput.from(compressedBuffer, 0, compressed.length);

        BytesInput decompressed = decompressor.decompress(heapBufferInput, original.length);
        assertArrayEquals(original, decompressed.toByteArray());
    }

    /**
     * Exercises the fallback path: {@link BytesInput#from(ByteBuffer, int, int)} with a direct
     * (off-heap) {@link ByteBuffer}. The resulting {@code ByteBufferBytesInput#toByteBuffer()}
     * returns a slice with {@code hasArray() == false}, so the decompressor must fall back to
     * the byte-array path. This guarantees we still handle off-heap inputs correctly.
     */
    public void testSnappyDecompressFromDirectByteBuffer() throws IOException {
        BytesInputCompressor compressor = factory.getCompressor(CompressionCodecName.SNAPPY);
        BytesInputDecompressor decompressor = factory.getDecompressor(CompressionCodecName.SNAPPY);

        byte[] original = randomByteArrayOfLength(between(100, 4096));
        byte[] compressed = compressor.compress(BytesInput.from(original)).toByteArray();

        ByteBuffer compressedDirect = ByteBuffer.allocateDirect(compressed.length);
        compressedDirect.put(compressed).flip();
        BytesInput directBufferInput = BytesInput.from(compressedDirect, 0, compressed.length);

        BytesInput decompressed = decompressor.decompress(directBufferInput, original.length);
        assertArrayEquals(original, decompressed.toByteArray());
    }

    /**
     * Exercises the Panama FFI Zstd path with non-zero {@code position()} on both buffers, modeling
     * how parquet-mr feeds page slices that start inside larger column-chunk buffers. The Panama
     * call must use the absolute offsets passed by the codec (i.e. {@code input.position()} and
     * {@code output.position()}) without double-counting them. Regression test for the addressing
     * semantics of {@link org.elasticsearch.nativeaccess.Zstd#decompress(ByteBuffer, int, int, ByteBuffer, int, int)}.
     */
    public void testZstdDirectByteBufferDecompressionWithNonZeroPosition() throws IOException {
        assumeTrue("Panama Zstd binding required for this test", PanamaZstd.instance().isAvailable());
        BytesInputCompressor compressor = factory.getCompressor(CompressionCodecName.ZSTD);
        BytesInputDecompressor decompressor = factory.getDecompressor(CompressionCodecName.ZSTD);

        byte[] original = randomByteArrayOfLength(between(100, 4096));
        byte[] compressedBytes = compressor.compress(BytesInput.from(original)).toByteArray();

        int srcLeading = between(1, 64);
        int srcTrailing = between(0, 64);
        ByteBuffer input = ByteBuffer.allocateDirect(srcLeading + compressedBytes.length + srcTrailing);
        input.position(srcLeading);
        input.put(compressedBytes);
        input.position(srcLeading);
        input.limit(srcLeading + compressedBytes.length);

        int dstLeading = between(1, 64);
        int dstTrailing = between(0, 64);
        ByteBuffer output = ByteBuffer.allocateDirect(dstLeading + original.length + dstTrailing);
        output.position(dstLeading);
        output.limit(dstLeading + original.length);

        decompressor.decompress(input, compressedBytes.length, output, original.length);

        // After decompression the codec advances position by the consumed/produced bytes.
        assertEquals(srcLeading + compressedBytes.length, input.position());
        assertEquals(dstLeading + original.length, output.position());

        // Decompressed payload must land at the original dstLeading offset.
        byte[] result = new byte[original.length];
        for (int i = 0; i < original.length; i++) {
            result[i] = output.get(dstLeading + i);
        }
        assertArrayEquals(original, result);
    }

    /**
     * The Panama Zstd path validates the decompressed length equals the parquet page header's
     * {@code decompressedSize}. Inflating that claimed size beyond what the compressed frame holds
     * must surface as a clean {@link IOException} rather than partial/incorrect output.
     */
    public void testZstdDirectByteBufferRejectsLengthMismatch() throws IOException {
        assumeTrue("Panama Zstd binding required for this test", PanamaZstd.instance().isAvailable());
        BytesInputCompressor compressor = factory.getCompressor(CompressionCodecName.ZSTD);
        BytesInputDecompressor decompressor = factory.getDecompressor(CompressionCodecName.ZSTD);

        byte[] original = randomByteArrayOfLength(between(100, 4096));
        byte[] compressedBytes = compressor.compress(BytesInput.from(original)).toByteArray();

        ByteBuffer input = ByteBuffer.allocateDirect(compressedBytes.length);
        input.put(compressedBytes).flip();
        ByteBuffer output = ByteBuffer.allocateDirect(original.length + 1);

        // Inflate decompressedSize beyond the actual frame payload; libzstd writes exactly
        // original.length bytes and our wrapper must reject the mismatch.
        IOException ex = expectThrows(
            IOException.class,
            () -> decompressor.decompress(input, compressedBytes.length, output, original.length + 1)
        );
        assertThat(ex.getMessage(), containsString("Zstd decompression"));
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

    public void testReleaseIsNoop() {
        BytesInputDecompressor before = factory.getDecompressor(CompressionCodecName.SNAPPY);
        BytesInputCompressor compressorBefore = factory.getCompressor(CompressionCodecName.GZIP);
        factory.release();
        // The factory holds stateless adapters whose backing JNI libraries cannot be unloaded;
        // release() is a no-op and the cached instances remain valid for reuse.
        assertSame(before, factory.getDecompressor(CompressionCodecName.SNAPPY));
        assertSame(compressorBefore, factory.getCompressor(CompressionCodecName.GZIP));
    }

    /**
     * Regression test: the factory is shared across all driver threads of an ESQL query, so
     * concurrent calls to {@link PlainCompressionCodecFactory#getDecompressor} and
     * {@link PlainCompressionCodecFactory#getCompressor} must be safe and must return the same
     * cached instance per codec. An earlier version backed the lookup with a plain {@code HashMap}
     * + {@code computeIfAbsent}, which raced under load.
     */
    public void testConcurrentLookupReturnsSameInstance() {
        int threads = 16;
        int iterationsPerThread = 200;
        CompressionCodecName[] codecs = new CompressionCodecName[] {
            CompressionCodecName.UNCOMPRESSED,
            CompressionCodecName.SNAPPY,
            CompressionCodecName.GZIP,
            CompressionCodecName.ZSTD,
            CompressionCodecName.LZ4_RAW };

        startInParallel(threads, t -> {
            for (int i = 0; i < iterationsPerThread; i++) {
                CompressionCodecName codec = codecs[i % codecs.length];
                BytesInputDecompressor d = factory.getDecompressor(codec);
                BytesInputCompressor c = factory.getCompressor(codec);
                assertNotNull(d);
                assertNotNull(c);
                // Identity must hold even mid-race; the holder must hand out the same instance to all callers.
                assertSame(d, factory.getDecompressor(codec));
                assertSame(c, factory.getCompressor(codec));
            }
        });
        for (CompressionCodecName codec : codecs) {
            assertSame(factory.getDecompressor(codec), factory.getDecompressor(codec));
            assertSame(factory.getCompressor(codec), factory.getCompressor(codec));
        }
        // Legacy LZ4 has a decompressor only, but the LazyInitializable holder must still hand
        // out a single shared instance under contention.
        BytesInputDecompressor legacyLz4 = factory.getDecompressor(CompressionCodecName.LZ4);
        assertSame(legacyLz4, factory.getDecompressor(CompressionCodecName.LZ4));
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
