/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.bzip2;

import org.apache.commons.compress.compressors.bzip2.BZip2CompressorOutputStream;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * Unit tests for {@link Bzip2DecompressionCodec}.
 */
public class Bzip2DecompressionCodecTests extends ESTestCase {

    public void testNameAndExtensions() {
        Bzip2DecompressionCodec codec = new Bzip2DecompressionCodec(EsExecutors.DIRECT_EXECUTOR_SERVICE);
        assertEquals("bzip2", codec.name());
        assertTrue(codec.extensions().contains(".bz2"));
        assertTrue(codec.extensions().contains(".bz"));
    }

    public void testRoundTripDecompression() throws IOException {
        String original = "hello,world\n1,2\nbar,baz";
        byte[] compressed = bzip2(original.getBytes(StandardCharsets.UTF_8));

        Bzip2DecompressionCodec codec = new Bzip2DecompressionCodec(EsExecutors.DIRECT_EXECUTOR_SERVICE);
        try (InputStream decompressed = codec.decompress(new ByteArrayInputStream(compressed))) {
            byte[] result = decompressed.readAllBytes();
            assertEquals(original, new String(result, StandardCharsets.UTF_8));
        }
    }

    public void testInvalidBzip2Throws() throws IOException {
        byte[] invalidBzip2 = new byte[] { 0x00, 0x01, 0x02 };
        Bzip2DecompressionCodec codec = new Bzip2DecompressionCodec(EsExecutors.DIRECT_EXECUTOR_SERVICE);

        IOException e = expectThrows(IOException.class, () -> {
            try (InputStream ignored = codec.decompress(new ByteArrayInputStream(invalidBzip2))) {
                ignored.readAllBytes();
            }
        });
        assertNotNull(e.getMessage());
    }

    public void testEmptyInput() throws IOException {
        byte[] compressed = bzip2(new byte[0]);
        Bzip2DecompressionCodec codec = new Bzip2DecompressionCodec(EsExecutors.DIRECT_EXECUTOR_SERVICE);

        try (InputStream decompressed = codec.decompress(new ByteArrayInputStream(compressed))) {
            byte[] result = decompressed.readAllBytes();
            assertEquals(0, result.length);
        }
    }

    public void testMultiBlockContinuousMode() throws IOException {
        // Generate enough data to produce multiple bzip2 blocks (min block size = 100KB)
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 5000; i++) {
            sb.append("line_").append(i).append("_").append("x".repeat(50)).append("\n");
        }
        String original = sb.toString();
        byte[] compressed = bzip2(original.getBytes(StandardCharsets.UTF_8));

        Bzip2DecompressionCodec codec = new Bzip2DecompressionCodec(EsExecutors.DIRECT_EXECUTOR_SERVICE);
        try (InputStream decompressed = codec.decompress(new ByteArrayInputStream(compressed))) {
            String result = new String(decompressed.readAllBytes(), StandardCharsets.UTF_8);
            assertEquals("Multi-block decompression must match original", original, result);
        }
    }

    public void testDecompressHeaderValidation() throws IOException {
        // 4 bytes of non-bzip2 data: the header check reads up to 4 bytes and must reject either a
        // wrong magic or a block-size digit outside [1-9]. Also covers bytes that pass the 'BZ'
        // prefix but fail on the 'h' / digit bytes.
        Bzip2DecompressionCodec codec = new Bzip2DecompressionCodec(EsExecutors.DIRECT_EXECUTOR_SERVICE);
        for (byte[] notBzip2 : List.of(
            "XXXX".getBytes(StandardCharsets.UTF_8),
            "BZxx".getBytes(StandardCharsets.UTF_8),
            "BZh0".getBytes(StandardCharsets.UTF_8),
            new byte[] { 'B', 'Z' }
        )) {
            IOException e = expectThrows(IOException.class, () -> {
                try (var s = codec.decompress(new ByteArrayInputStream(notBzip2))) {
                    s.readAllBytes();
                }
            });
            assertNotNull(e.getMessage());
            assertTrue(
                "expected BZip2 header-validation message, got: " + e.getMessage(),
                e.getMessage().contains("BZip2") || e.getMessage().contains("BZh")
            );
        }
    }

    /** Rationale: some real-world .bz2 files are several bzip2 members concatenated; a single member must not truncate NDJSON. */
    public void testDecompressConcatenatedMembers() throws IOException {
        byte[] a = bzip2("a\n".getBytes(StandardCharsets.UTF_8));
        byte[] b = bzip2("b\n".getBytes(StandardCharsets.UTF_8));
        byte[] both = new byte[a.length + b.length];
        System.arraycopy(a, 0, both, 0, a.length);
        System.arraycopy(b, 0, both, a.length, b.length);

        Bzip2DecompressionCodec codec = new Bzip2DecompressionCodec(EsExecutors.DIRECT_EXECUTOR_SERVICE);
        try (InputStream in = codec.decompress(new ByteArrayInputStream(both))) {
            assertEquals("a\nb\n", new String(in.readAllBytes(), StandardCharsets.UTF_8));
        }
    }

    /**
     * Pins the BYBLOCK bit-scan invariant when concatenated members use different block sizes
     * (e.g. {@code BZh9} + {@code BZh1}). {@link CBZip2InputStream} in BYBLOCK mode fixes
     * {@code blockSize100k = 9} and never re-reads the next member's header, so the {@code Data}
     * buffer is over-allocated for a smaller-block member; decoding itself is capped by each
     * block's own metadata, so the round-trip must still be byte-for-byte.
     */
    public void testDecompressConcatenatedMembersMixedBlockSizes() throws IOException {
        byte[] a = bzip2("first-member\n".getBytes(StandardCharsets.UTF_8), 9);
        byte[] b = bzip2("second-smaller-block\n".getBytes(StandardCharsets.UTF_8), 1);
        byte[] both = new byte[a.length + b.length];
        System.arraycopy(a, 0, both, 0, a.length);
        System.arraycopy(b, 0, both, a.length, b.length);
        String expected = "first-member\nsecond-smaller-block\n";

        Bzip2DecompressionCodec codec = new Bzip2DecompressionCodec(EsExecutors.DIRECT_EXECUTOR_SERVICE);
        try (InputStream in = codec.decompress(new ByteArrayInputStream(both))) {
            assertEquals(expected, new String(in.readAllBytes(), StandardCharsets.UTF_8));
        }
        StorageObject object = new ByteArrayStorageObject(both);
        try (InputStream in = codec.decompressRange(object, 0, both.length)) {
            assertEquals(expected, new String(in.readAllBytes(), StandardCharsets.UTF_8));
        }
    }

    /** Full compressed span read via decompressRange (as with a single full-file FileSplit) must include all members. */
    public void testDecompressRangeFullObjectConcatenated() throws IOException {
        byte[] a = bzip2("a\n".getBytes(StandardCharsets.UTF_8));
        byte[] b = bzip2("b\n".getBytes(StandardCharsets.UTF_8));
        byte[] both = new byte[a.length + b.length];
        System.arraycopy(a, 0, both, 0, a.length);
        System.arraycopy(b, 0, both, a.length, b.length);

        StorageObject object = new ByteArrayStorageObject(both);
        Bzip2DecompressionCodec codec = new Bzip2DecompressionCodec(EsExecutors.DIRECT_EXECUTOR_SERVICE);
        try (InputStream in = codec.decompressRange(object, 0, both.length)) {
            assertEquals("a\nb\n", new String(in.readAllBytes(), StandardCharsets.UTF_8));
        }
    }

    private static byte[] bzip2(byte[] input) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (BZip2CompressorOutputStream bzip2Out = new BZip2CompressorOutputStream(baos)) {
            bzip2Out.write(input);
        }
        return baos.toByteArray();
    }

    private static byte[] bzip2(byte[] input, int blockSize100k) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (BZip2CompressorOutputStream bzip2Out = new BZip2CompressorOutputStream(baos, blockSize100k)) {
            bzip2Out.write(input);
        }
        return baos.toByteArray();
    }
}
