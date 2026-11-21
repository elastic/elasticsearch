/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.parquet.write.file;

import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.compression.CompressionCodecFactory;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.elasticsearch.xpack.esql.datasource.compress.PanamaZstd;
import org.xerial.snappy.Snappy;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * The compression of the files that the engine writes, with no Hadoop: no compression, Snappy and ZSTD. The libraries are those of the
 * compression plugin that the Iceberg plugin extends ({@code snappy-java}, and ZSTD through the foreign function interface).
 *
 * <p>The engine reads only files with no compression (its own local files) and the footers of the others, so a decompressor exists
 * only for the files with no compression.
 */
public final class ParquetCodecs implements CompressionCodecFactory {

    private static final int ZSTD_LEVEL = 3;

    @Override
    public BytesInputCompressor getCompressor(CompressionCodecName codec) {
        return switch (codec) {
            case UNCOMPRESSED -> new Compressor(codec) {
                @Override
                public BytesInput compress(BytesInput bytes) {
                    return bytes;
                }
            };
            case SNAPPY -> new Compressor(codec) {
                @Override
                public BytesInput compress(BytesInput bytes) throws IOException {
                    return BytesInput.from(Snappy.compress(bytes.toByteArray()));
                }
            };
            case ZSTD -> new Compressor(codec) {
                private final PanamaZstd zstd = PanamaZstd.instance();

                @Override
                public BytesInput compress(BytesInput bytes) throws IOException {
                    final byte[] in = bytes.toByteArray();
                    final byte[] out = new byte[zstd.compressBound(in.length)];
                    try {
                        return BytesInput.from(out, 0, zstd.compressHeap(out, 0, out.length, in, 0, in.length, ZSTD_LEVEL));
                    } catch (RuntimeException e) {
                        throw new IOException("ZSTD compression failed", e);
                    }
                }
            };
            default -> throw new UnsupportedOperationException("the engine does not write the codec [" + codec + "]");
        };
    }

    @Override
    public BytesInputDecompressor getDecompressor(CompressionCodecName codec) {
        return switch (codec) {
            case UNCOMPRESSED -> new Decompressor() {
                @Override
                protected byte[] decompressBytes(byte[] compressed, int decompressedSize) {
                    return compressed;
                }
            };
            case SNAPPY -> new Decompressor() {
                @Override
                protected byte[] decompressBytes(byte[] compressed, int decompressedSize) throws IOException {
                    final byte[] out = new byte[decompressedSize];
                    final int written = Snappy.uncompress(compressed, 0, compressed.length, out, 0);
                    if (written != decompressedSize) {
                        throw new IOException("a Snappy page has [" + written + "] bytes and its header says [" + decompressedSize + "]");
                    }
                    return out;
                }
            };
            case ZSTD -> new Decompressor() {
                private final PanamaZstd zstd = PanamaZstd.instance();

                @Override
                protected byte[] decompressBytes(byte[] compressed, int decompressedSize) throws IOException {
                    final byte[] out = new byte[decompressedSize];
                    try {
                        final int written = zstd.decompressHeap(out, 0, out.length, compressed, 0, compressed.length);
                        if (written != decompressedSize) {
                            throw new IOException("a ZSTD page has [" + written + "] bytes and its header says [" + decompressedSize + "]");
                        }
                    } catch (RuntimeException e) {
                        throw new IOException("ZSTD decompression failed", e);
                    }
                    return out;
                }
            };
            default -> throw new UnsupportedOperationException("the engine does not read the codec [" + codec + "]");
        };
    }

    /** Reads the pages of the files that the engine itself writes (uncompressed local files, Snappy or ZSTD uploaded files). */
    private abstract static class Decompressor implements BytesInputDecompressor {
        protected abstract byte[] decompressBytes(byte[] compressed, int decompressedSize) throws IOException;

        @Override
        public BytesInput decompress(BytesInput bytes, int decompressedSize) throws IOException {
            return BytesInput.from(decompressBytes(bytes.toByteArray(), decompressedSize));
        }

        @Override
        public void decompress(ByteBuffer input, int compressedSize, ByteBuffer output, int decompressedSize) throws IOException {
            final byte[] compressed = new byte[compressedSize];
            input.get(compressed);
            output.put(decompressBytes(compressed, decompressedSize));
        }

        @Override
        public void release() {}
    }

    @Override
    public void release() {}

    private abstract static class Compressor implements BytesInputCompressor {
        private final CompressionCodecName codec;

        Compressor(CompressionCodecName codec) {
            this.codec = codec;
        }

        @Override
        public CompressionCodecName getCodecName() {
            return codec;
        }

        @Override
        public void release() {}
    }
}
