/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.parquet;

import io.airlift.compress.lz4.Lz4Compressor;

import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.compression.CompressionCodecFactory;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import java.io.IOException;

/**
 * Test-only codec factory that adds a writer for the deprecated Hadoop-framed
 * {@link CompressionCodecName#LZ4} codec on top of {@link PlainCompressionCodecFactory}.
 *
 * <p>The production factory deliberately exposes the legacy LZ4 codec as read-only — see
 * {@code PlainCompressionCodecFactory.Lz4HadoopFramedBytesDecompressor} — because the
 * parquet-format spec deprecated the codec in November 2021 (PARQUET-2032) and ES|QL should not
 * emit it. To exercise the reader against real legacy-LZ4 parquet files this test helper provides
 * a compressor that produces the exact same Hadoop {@code BlockCompressorStream} framing that
 * parquet-mr embedded when it wrote the deprecated codec, so the existing in-memory codec-sweep
 * tests can drive the new decompressor without checking in binary fixtures.
 *
 * <p>The frame format produced here is the inverse of the one parsed by the reader:
 *
 * <pre>
 * [outer uncompressed length: int32 big-endian]
 *   [sub-block compressed length: int32 big-endian]
 *   [sub-block compressed bytes: raw LZ4 block format]
 * </pre>
 *
 * <p>This compressor emits a single sub-block per outer block — sufficient for parquet round-trip
 * tests since column chunks are bounded per page and parquet-mr itself produced one sub-block in
 * the overwhelming majority of legacy-LZ4 files in the wild. The decompressor still supports
 * multi-sub-block inputs; that path is exercised directly in
 * {@link PlainCompressionCodecFactoryTests} by hand-crafted frame bytes rather than through this
 * helper.
 */
final class LegacyLz4HadoopFramedCodecFactory implements CompressionCodecFactory {

    private final PlainCompressionCodecFactory delegate;
    private final BytesInputCompressor legacyLz4Compressor;

    LegacyLz4HadoopFramedCodecFactory() {
        this.delegate = new PlainCompressionCodecFactory();
        this.legacyLz4Compressor = new HadoopFramedLz4Compressor();
    }

    @Override
    public BytesInputDecompressor getDecompressor(CompressionCodecName codecName) {
        return delegate.getDecompressor(codecName);
    }

    @Override
    public BytesInputCompressor getCompressor(CompressionCodecName codecName) {
        if (codecName == CompressionCodecName.LZ4) {
            return legacyLz4Compressor;
        }
        return delegate.getCompressor(codecName);
    }

    @Override
    public void release() {
        delegate.release();
    }

    /**
     * Test-only writer that mirrors Hadoop's {@code BlockCompressorStream} single-sub-block frame.
     * Production code intentionally never emits this format.
     */
    private static final class HadoopFramedLz4Compressor implements BytesInputCompressor {
        private final Lz4Compressor lz4 = new Lz4Compressor();

        @Override
        public BytesInput compress(BytesInput bytes) throws IOException {
            byte[] in = bytes.toByteArray();
            int uncompressedLen = in.length;
            byte[] rawCompressed = new byte[lz4.maxCompressedLength(uncompressedLen)];
            int compressedLen = lz4.compress(in, 0, uncompressedLen, rawCompressed, 0, rawCompressed.length);

            byte[] framed = new byte[4 + 4 + compressedLen];
            writeIntBE(framed, 0, uncompressedLen);
            writeIntBE(framed, 4, compressedLen);
            System.arraycopy(rawCompressed, 0, framed, 8, compressedLen);
            return BytesInput.from(framed);
        }

        @Override
        public CompressionCodecName getCodecName() {
            return CompressionCodecName.LZ4;
        }

        @Override
        public void release() {}

        private static void writeIntBE(byte[] buf, int off, int v) {
            buf[off] = (byte) (v >>> 24);
            buf[off + 1] = (byte) (v >>> 16);
            buf[off + 2] = (byte) (v >>> 8);
            buf[off + 3] = (byte) v;
        }
    }
}
