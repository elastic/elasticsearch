/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.snappy;

import io.airlift.compress.hadoop.HadoopOutputStream;
import io.airlift.compress.snappy.SnappyHadoopStreams;

import org.elasticsearch.test.ESTestCase;
import org.xerial.snappy.Snappy;
import org.xerial.snappy.SnappyFramedOutputStream;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.not;

/**
 * Unit tests for {@link SnappyDecompressionCodec}.
 */
public class SnappyDecompressionCodecTests extends ESTestCase {

    public void testNameAndExtensions() {
        SnappyDecompressionCodec codec = new SnappyDecompressionCodec();
        assertEquals("snappy", codec.name());
        assertEquals(1, codec.extensions().size());
        assertTrue(codec.extensions().contains(".snappy"));
    }

    public void testRoundTripXerialFraming() throws IOException {
        String original = "hello,world\n1,2\nbar,baz";
        byte[] compressed = xerialFrame(original.getBytes(StandardCharsets.UTF_8));

        SnappyDecompressionCodec codec = new SnappyDecompressionCodec();
        try (InputStream decompressed = codec.decompress(new ByteArrayInputStream(compressed))) {
            byte[] result = decompressed.readAllBytes();
            assertEquals(original, new String(result, StandardCharsets.UTF_8));
        }
    }

    public void testRoundTripHadoopFraming() throws IOException {
        String original = "hello,world\n1,2\nbar,baz\nthe,quick,brown,fox";
        byte[] compressed = hadoopFrame(original.getBytes(StandardCharsets.UTF_8));

        SnappyDecompressionCodec codec = new SnappyDecompressionCodec();
        try (InputStream decompressed = codec.decompress(new ByteArrayInputStream(compressed))) {
            byte[] result = decompressed.readAllBytes();
            assertEquals(original, new String(result, StandardCharsets.UTF_8));
        }
    }

    /**
     * Reproduces the framing the original bug report constructed by hand (BE int32 lengths +
     * a single raw {@code Snappy.compress} block). This is the on-disk shape that {@code snzip
     * -t hadoop-snappy} and ClickHouse text snappy exports produce.
     */
    public void testRoundTripHandCraftedHadoopBlock() throws IOException {
        byte[] payload = "hello,world\n1,2\n".getBytes(StandardCharsets.UTF_8);
        byte[] compressedBlock = Snappy.compress(payload);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (DataOutputStream dos = new DataOutputStream(out)) {
            dos.writeInt(payload.length);
            dos.writeInt(compressedBlock.length);
            dos.write(compressedBlock);
        }

        SnappyDecompressionCodec codec = new SnappyDecompressionCodec();
        try (InputStream decompressed = codec.decompress(new ByteArrayInputStream(out.toByteArray()))) {
            byte[] result = decompressed.readAllBytes();
            assertArrayEquals(payload, result);
        }
    }

    public void testUnknownFramingThrowsActionableError() {
        // First byte 0x42 matches neither xerial (0xff) nor Hadoop (0x00).
        byte[] garbage = new byte[] { 0x42, 0x01, 0x02, 0x03 };
        SnappyDecompressionCodec codec = new SnappyDecompressionCodec();

        IOException e = expectThrows(IOException.class, () -> {
            try (InputStream ignored = codec.decompress(new ByteArrayInputStream(garbage))) {
                ignored.readAllBytes();
            }
        });
        assertThat(e.getMessage(), containsString("unrecognized snappy stream framing"));
        assertThat(e.getMessage(), containsString("0x42"));
        assertThat(e.getMessage(), containsString("xerial"));
        assertThat(e.getMessage(), containsString("Hadoop"));
    }

    public void testEmptyInputXerial() throws IOException {
        byte[] compressed = xerialFrame(new byte[0]);
        SnappyDecompressionCodec codec = new SnappyDecompressionCodec();

        try (InputStream decompressed = codec.decompress(new ByteArrayInputStream(compressed))) {
            byte[] result = decompressed.readAllBytes();
            assertEquals(0, result.length);
        }
    }

    public void testEmptyStreamThrows() {
        // A zero-byte input routes to the xerial reader (the empty-stream branch); its constructor
        // rejects the input because the 10-byte stream identifier chunk is missing.
        SnappyDecompressionCodec codec = new SnappyDecompressionCodec();
        expectThrows(IOException.class, () -> codec.decompress(new ByteArrayInputStream(new byte[0])));
    }

    public void testTruncatedXerialStreamThrows() throws IOException {
        String original = "hello,world\n1,2\nbar,baz\nsome,more,data,here";
        byte[] compressed = xerialFrame(original.getBytes(StandardCharsets.UTF_8));
        byte[] truncated = Arrays.copyOf(compressed, compressed.length / 2);

        SnappyDecompressionCodec codec = new SnappyDecompressionCodec();
        expectThrows(IOException.class, () -> {
            try (InputStream decompressed = codec.decompress(new ByteArrayInputStream(truncated))) {
                decompressed.readAllBytes();
            }
        });
    }

    public void testTruncatedHadoopStreamThrows() throws IOException {
        String original = "hello,world\n1,2\nbar,baz\nsome,more,data,here";
        byte[] compressed = hadoopFrame(original.getBytes(StandardCharsets.UTF_8));
        // Truncate mid-block: keep the 8-byte header but drop most of the payload.
        byte[] truncated = Arrays.copyOf(compressed, Math.max(9, compressed.length / 2));

        SnappyDecompressionCodec codec = new SnappyDecompressionCodec();
        expectThrows(IOException.class, () -> {
            try (InputStream decompressed = codec.decompress(new ByteArrayInputStream(truncated))) {
                decompressed.readAllBytes();
            }
        });
    }

    /**
     * Hadoop snappy framing with a valid header but a corrupted compressed payload: aircompressor
     * raises {@code MalformedInputException} (unchecked) from inside the snappy decoder. The codec
     * must translate that into a checked {@link IOException} so callers that only catch
     * {@code IOException} don't crash, and the error message must name the Hadoop layer.
     */
    public void testMalformedHadoopPayloadTranslatedToIOException() throws IOException {
        // Hadoop framing with a header that passes aircompressor's sanity checks but a block whose
        // snappy payload is invalid. The block format is varint(uncompressedLen) followed by snappy
        // tag bytes. We use uncompressedLen=16 (0x10 fits in one varint byte) so the chunk-vs-block
        // length check passes; the remaining bytes are 0xff, which is an invalid snappy literal/copy
        // tag, so SnappyDecompressor throws aircompressor's unchecked MalformedInputException. The
        // codec must translate that into a checked IOException for callers that only catch IOException.
        byte[] block = new byte[] { 0x10, (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff };

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (DataOutputStream dos = new DataOutputStream(out)) {
            dos.writeInt(16);
            dos.writeInt(block.length);
            dos.write(block);
        }

        SnappyDecompressionCodec codec = new SnappyDecompressionCodec();
        IOException e = expectThrows(IOException.class, () -> {
            try (InputStream decompressed = codec.decompress(new ByteArrayInputStream(out.toByteArray()))) {
                decompressed.readAllBytes();
            }
        });
        assertThat(e.getMessage(), containsString("malformed Hadoop-snappy stream at offset"));
        assertNotNull(e.getCause());
        assertEquals("io.airlift.compress.MalformedInputException", e.getCause().getClass().getName());
    }

    /**
     * Reads via the single-argument {@code read(byte[])} path. {@link java.io.FilterInputStream}'s
     * default implementation of that method delegates straight to {@code in.read(b, 0, b.length)},
     * which would bypass the overridden three-argument read and let aircompressor's unchecked
     * {@code MalformedInputException} escape. Our wrapper overrides the single-argument form to
     * route through the overridden three-argument form, so the translation applies here too.
     */
    public void testMalformedHadoopPayloadTranslatedOnReadByteArrayPath() throws IOException {
        byte[] block = new byte[] { 0x10, (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff };
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (DataOutputStream dos = new DataOutputStream(out)) {
            dos.writeInt(16);
            dos.writeInt(block.length);
            dos.write(block);
        }

        SnappyDecompressionCodec codec = new SnappyDecompressionCodec();
        IOException e = expectThrows(IOException.class, () -> {
            try (InputStream decompressed = codec.decompress(new ByteArrayInputStream(out.toByteArray()))) {
                byte[] buf = new byte[64];
                // Use the single-arg read(byte[]) form explicitly, not readAllBytes() (which goes
                // through read(byte[], int, int)).
                while (decompressed.read(buf) != -1) {
                    // drain
                }
            }
        });
        assertThat(e.getMessage(), containsString("malformed Hadoop-snappy stream at offset"));
        assertEquals("io.airlift.compress.MalformedInputException", e.getCause().getClass().getName());
    }

    /**
     * Input whose first byte is {@code 0x00} routes to the Hadoop reader. If the rest of the
     * header is malformed the failure comes from the Hadoop reader, not from the
     * unrecognized-framing path; the two error surfaces are distinct on purpose.
     */
    public void testMalformedHadoopHeaderThrowsFromHadoopReader() {
        // Valid-looking BE int32 uncompressed length, then garbage where the BE int32 compressed
        // length and the snappy block should be.
        byte[] malformed = new byte[] { 0x00, 0x00, 0x00, 0x10, (byte) 0xde, (byte) 0xad, (byte) 0xbe, (byte) 0xef };

        SnappyDecompressionCodec codec = new SnappyDecompressionCodec();
        IOException e = expectThrows(IOException.class, () -> {
            try (InputStream decompressed = codec.decompress(new ByteArrayInputStream(malformed))) {
                decompressed.readAllBytes();
            }
        });
        // The message must NOT be the unrecognized-framing one — that path is reserved for
        // truly unknown first bytes. Hadoop-layer failures bubble up with their own wording.
        assertThat(e.getMessage() == null ? "" : e.getMessage(), not(containsString("unrecognized snappy stream framing")));
    }

    private static byte[] xerialFrame(byte[] input) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (SnappyFramedOutputStream snappyOut = new SnappyFramedOutputStream(baos)) {
            snappyOut.write(input);
        }
        return baos.toByteArray();
    }

    private static byte[] hadoopFrame(byte[] input) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (HadoopOutputStream snappyOut = new SnappyHadoopStreams().createOutputStream(baos)) {
            snappyOut.write(input, 0, input.length);
            snappyOut.finish();
        }
        return baos.toByteArray();
    }
}
