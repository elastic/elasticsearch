/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.parquet.docvalues;

import io.airlift.compress.snappy.SnappyCompressor;

import org.apache.parquet.format.CompressionCodec;
import org.elasticsearch.test.ESTestCase;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.zip.GZIPOutputStream;

public class PageDecompressorTests extends ESTestCase {

    private static byte[] compress(int codec, byte[] page) throws IOException {
        if (codec == CompressionCodec.UNCOMPRESSED.getValue()) {
            return page;
        }
        if (codec == CompressionCodec.SNAPPY.getValue()) {
            SnappyCompressor compressor = new SnappyCompressor();
            byte[] out = new byte[compressor.maxCompressedLength(page.length)];
            return Arrays.copyOf(out, compressor.compress(page, 0, page.length, out, 0, out.length));
        }
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (GZIPOutputStream gzip = new GZIPOutputStream(bytes)) {
            gzip.write(page);
        }
        return bytes.toByteArray();
    }

    private static int randomCodec() {
        return randomFrom(CompressionCodec.UNCOMPRESSED, CompressionCodec.SNAPPY, CompressionCodec.GZIP).getValue();
    }

    public void testDecompressIntoALongerBufferLeavesTheRestAlone() throws IOException {
        for (int i = 0; i < 50; i++) {
            int codec = randomCodec();
            // repetitive content compresses, and random content does not: use both
            byte[] page = randomBoolean() ? randomByteArrayOfLength(randomIntBetween(0, 5_000)) : new byte[randomIntBetween(0, 5_000)];
            byte[] compressed = compress(codec, page);
            // the compressed bytes sit in the middle of a longer array, as the page of a chunk does
            int before = randomIntBetween(0, 20);
            byte[] input = new byte[before + compressed.length + randomIntBetween(0, 20)];
            System.arraycopy(compressed, 0, input, before, compressed.length);
            byte[] output = new byte[page.length + randomIntBetween(0, 100)];
            Arrays.fill(output, (byte) 0x7F);

            PageDecompressor.decompressInto(codec, input, before, compressed.length, output, page.length);

            assertArrayEquals(page, Arrays.copyOf(output, page.length));
            for (int j = page.length; j < output.length; j++) {
                assertEquals("the byte [" + j + "] after the page must not be touched", (byte) 0x7F, output[j]);
            }
            assertArrayEquals(page, PageDecompressor.decompress(codec, input, before, compressed.length, page.length));
        }
    }

    public void testABufferThatIsTooShortIsRefused() throws IOException {
        byte[] page = randomByteArrayOfLength(100);
        int codec = randomCodec();
        byte[] compressed = compress(codec, page);
        expectThrows(
            IllegalArgumentException.class,
            () -> PageDecompressor.decompressInto(codec, compressed, 0, compressed.length, new byte[99], page.length)
        );
    }

    public void testASizeThatTheHeaderGetsWrongIsAnError() throws IOException {
        byte[] page = new byte[1_000];
        for (int codec : new int[] { CompressionCodec.UNCOMPRESSED.getValue(), CompressionCodec.SNAPPY.getValue() }) {
            byte[] compressed = compress(codec, page);
            expectThrows(
                IOException.class,
                () -> PageDecompressor.decompressInto(codec, compressed, 0, compressed.length, new byte[2_000], 1_500)
            );
        }
    }
}
