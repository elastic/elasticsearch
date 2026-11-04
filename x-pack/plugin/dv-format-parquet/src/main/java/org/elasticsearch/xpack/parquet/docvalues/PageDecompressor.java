/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.parquet.docvalues;

import io.airlift.compress.snappy.SnappyDecompressor;

import org.apache.parquet.format.CompressionCodec;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.zip.GZIPInputStream;

/** Decompresses the pages of a column chunk. Snappy comes from {@code aircompressor}, in pure Java. */
final class PageDecompressor {

    private PageDecompressor() {}

    /** Whether the reader can decompress chunks with this codec. */
    static boolean supports(int codec) {
        return codec == CompressionCodec.UNCOMPRESSED.getValue()
            || codec == CompressionCodec.SNAPPY.getValue()
            || codec == CompressionCodec.GZIP.getValue();
    }

    /**
     * Returns the uncompressed bytes of {@code length} bytes of {@code input} starting at {@code offset}. For {@code UNCOMPRESSED} that
     * is a copy-free view only when the caller asks for a copy, so the caller must not write to the result.
     */
    static byte[] decompress(int codec, byte[] input, int offset, int length, int uncompressedSize) throws IOException {
        if (codec == CompressionCodec.UNCOMPRESSED.getValue()) {
            if (length != uncompressedSize) {
                throw new IOException("an uncompressed page has [" + length + "] bytes, the header says [" + uncompressedSize + "]");
            }
            byte[] copy = new byte[length];
            System.arraycopy(input, offset, copy, 0, length);
            return copy;
        }
        byte[] output = new byte[uncompressedSize];
        if (codec == CompressionCodec.SNAPPY.getValue()) {
            int written;
            try {
                written = new SnappyDecompressor().decompress(input, offset, length, output, 0, uncompressedSize);
            } catch (RuntimeException e) {
                throw new IOException("failed to decompress a Snappy page", e);
            }
            if (written != uncompressedSize) {
                throw new IOException(
                    "a Snappy page has [" + written + "] bytes after decompression, the header says [" + uncompressedSize + "]"
                );
            }
            return output;
        }
        if (codec == CompressionCodec.GZIP.getValue()) {
            try (GZIPInputStream in = new GZIPInputStream(new ByteArrayInputStream(input, offset, length))) {
                int read = in.readNBytes(output, 0, uncompressedSize);
                if (read != uncompressedSize) {
                    throw new IOException(
                        "a GZIP page has [" + read + "] bytes after decompression, the header says [" + uncompressedSize + "]"
                    );
                }
                return output;
            }
        }
        throw new IOException("the compression codec [" + codec + "] is not supported yet");
    }
}
