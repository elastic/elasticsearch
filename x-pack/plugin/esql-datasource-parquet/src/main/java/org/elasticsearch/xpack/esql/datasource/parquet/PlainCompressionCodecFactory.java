/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.parquet;

import io.airlift.compress.lz4.Lz4Compressor;
import io.airlift.compress.lz4.Lz4Decompressor;

import com.github.luben.zstd.Zstd;

import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.compression.CompressionCodecFactory;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.elasticsearch.common.CheckedSupplier;
import org.elasticsearch.common.util.LazyInitializable;
import org.xerial.snappy.Snappy;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.EnumMap;
import java.util.Map;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/**
 * Hadoop-free {@link CompressionCodecFactory} that delegates to native JNI libraries already on the
 * plugin classpath (snappy-java, zstd-jni, aircompressor for LZ4, JDK GZIP). Decompressors and
 * compressors are created lazily on first use so native libraries are only loaded when a Parquet
 * file actually uses that codec.
 *
 * <p>This replaces Parquet-MR's default {@code CodecFactory} which requires Hadoop's
 * {@code Configuration} and pulls in ~50MB of Hadoop JARs.
 *
 * <p>Each decompressor supports both heap and direct {@link ByteBuffer}s. When both buffers are
 * direct, the JNI fast path is used (zero-copy for Snappy and Zstd). When either buffer is heap,
 * the decompressor falls back to the byte-array path. Parquet-MR's
 * {@code ColumnChunkPageReadStore} calls the {@code ByteBuffer} overload only when the allocator
 * is direct and {@code useOffHeapDecryptBuffer} is enabled; the current read path uses
 * {@code HeapByteBufferAllocator} so the {@code BytesInput} path is the hot path today, but the
 * direct path is ready for when we switch to a direct allocator.
 *
 * <p>This factory is shared across all driver threads of a query, so {@link #getDecompressor} and
 * {@link #getCompressor} must be safe under concurrent access. Thread-safety is achieved without
 * losing laziness by:
 * <ul>
 *   <li>Building the per-codec lookup tables once in the constructor as immutable {@link EnumMap}s,
 *       so the hot path is a plain map read with no synchronization.</li>
 *   <li>Wrapping each entry in a {@link LazyInitializable} which uses double-checked locking to
 *       create the underlying (de)compressor on first use, so the JNI library backing each codec
 *       is loaded exactly once per codec regardless of how many threads race for it.</li>
 * </ul>
 *
 * <p>{@link #release()} is intentionally a no-op: the codec adapters here hold no resources that
 * can be released (the underlying JNI native libraries cannot be unloaded), so there is nothing
 * to clear. The method exists only because the {@link CompressionCodecFactory} SPI requires it.
 */
final class PlainCompressionCodecFactory implements CompressionCodecFactory {

    private final Map<CompressionCodecName, LazyInitializable<BytesInputDecompressor, RuntimeException>> decompressors;
    private final Map<CompressionCodecName, LazyInitializable<BytesInputCompressor, RuntimeException>> compressors;

    PlainCompressionCodecFactory() {
        Map<CompressionCodecName, LazyInitializable<BytesInputDecompressor, RuntimeException>> dec = new EnumMap<>(
            CompressionCodecName.class
        );
        dec.put(CompressionCodecName.UNCOMPRESSED, lazy(NoopDecompressor::new));
        dec.put(CompressionCodecName.SNAPPY, lazy(SnappyBytesDecompressor::new));
        dec.put(CompressionCodecName.GZIP, lazy(GzipBytesDecompressor::new));
        dec.put(CompressionCodecName.ZSTD, lazy(ZstdBytesDecompressor::new));
        dec.put(CompressionCodecName.LZ4_RAW, lazy(Lz4RawBytesDecompressor::new));
        this.decompressors = dec;

        Map<CompressionCodecName, LazyInitializable<BytesInputCompressor, RuntimeException>> com = new EnumMap<>(
            CompressionCodecName.class
        );
        com.put(CompressionCodecName.UNCOMPRESSED, lazy(NoopCompressor::new));
        com.put(CompressionCodecName.SNAPPY, lazy(SnappyBytesCompressor::new));
        com.put(CompressionCodecName.GZIP, lazy(GzipBytesCompressor::new));
        com.put(CompressionCodecName.ZSTD, lazy(ZstdBytesCompressor::new));
        com.put(CompressionCodecName.LZ4_RAW, lazy(Lz4RawBytesCompressor::new));
        this.compressors = com;
    }

    private static <T> LazyInitializable<T, RuntimeException> lazy(CheckedSupplier<T, RuntimeException> supplier) {
        return new LazyInitializable<>(supplier);
    }

    @Override
    public BytesInputDecompressor getDecompressor(CompressionCodecName codecName) {
        LazyInitializable<BytesInputDecompressor, RuntimeException> holder = decompressors.get(codecName);
        if (holder == null) {
            throw new UnsupportedOperationException("Unsupported Parquet decompression codec: " + codecName);
        }
        return holder.getOrCompute();
    }

    @Override
    public BytesInputCompressor getCompressor(CompressionCodecName codecName) {
        LazyInitializable<BytesInputCompressor, RuntimeException> holder = compressors.get(codecName);
        if (holder == null) {
            throw new UnsupportedOperationException("Unsupported Parquet compression codec: " + codecName);
        }
        return holder.getOrCompute();
    }

    @Override
    public void release() {
        // No-op: the codec adapters hold no resources, and the JNI native libraries backing them
        // cannot be unloaded. Implementing this purely to satisfy the parquet-mr SPI.
    }

    /**
     * Heap-buffer fallback: delegates the {@code ByteBuffer} overload to the byte-array
     * {@code BytesInput} path when JNI direct-buffer APIs are not available for the codec.
     */
    private static void decompressViaHeapCopy(
        BytesInputDecompressor self,
        ByteBuffer input,
        int compressedSize,
        ByteBuffer output,
        int decompressedSize
    ) throws IOException {
        int origLimit = input.limit();
        int origPos = input.position();
        input.limit(origPos + compressedSize);
        BytesInput decompressed = self.decompress(BytesInput.from(input), decompressedSize);
        output.put(decompressed.toByteBuffer());
        input.limit(origLimit);
        input.position(origPos + compressedSize);
    }

    // ------------------------------- decompressors -------------------------------

    private static class NoopDecompressor implements BytesInputDecompressor {
        @Override
        public BytesInput decompress(BytesInput bytes, int decompressedSize) {
            return bytes;
        }

        @Override
        public void decompress(ByteBuffer input, int compressedSize, ByteBuffer output, int decompressedSize) {
            int origLimit = input.limit();
            input.limit(input.position() + compressedSize);
            output.put(input);
            input.limit(origLimit);
        }

        @Override
        public void release() {}
    }

    /**
     * Snappy decompressor. The JNI {@code Snappy.uncompress(ByteBuffer, ByteBuffer)} requires both
     * buffers to be direct; heap buffers fall back to the byte-array path. The JNI call returns the
     * number of decompressed bytes written but does not advance the output buffer position, so we
     * advance it manually.
     */
    private static class SnappyBytesDecompressor implements BytesInputDecompressor {
        @Override
        public BytesInput decompress(BytesInput bytes, int decompressedSize) throws IOException {
            byte[] out = new byte[decompressedSize];
            Snappy.uncompress(bytes.toByteArray(), 0, (int) bytes.size(), out, 0);
            return BytesInput.from(out);
        }

        @Override
        public void decompress(ByteBuffer input, int compressedSize, ByteBuffer output, int decompressedSize) throws IOException {
            if (input.isDirect() && output.isDirect()) {
                int origLimit = input.limit();
                int origPos = input.position();
                input.limit(origPos + compressedSize);
                int written = Snappy.uncompress(input, output);
                output.position(output.position() + written);
                input.limit(origLimit);
                input.position(origPos + compressedSize);
            } else {
                decompressViaHeapCopy(this, input, compressedSize, output, decompressedSize);
            }
        }

        @Override
        public void release() {}
    }

    private static class GzipBytesDecompressor implements BytesInputDecompressor {
        @Override
        public BytesInput decompress(BytesInput bytes, int decompressedSize) throws IOException {
            byte[] out = new byte[decompressedSize];
            try (GZIPInputStream gis = new GZIPInputStream(bytes.toInputStream())) {
                int off = 0;
                while (off < decompressedSize) {
                    int read = gis.read(out, off, decompressedSize - off);
                    if (read < 0) {
                        throw new IOException("Premature end of GZIP stream: expected " + decompressedSize + " bytes, got " + off);
                    }
                    off += read;
                }
            }
            return BytesInput.from(out);
        }

        @Override
        public void decompress(ByteBuffer input, int compressedSize, ByteBuffer output, int decompressedSize) throws IOException {
            decompressViaHeapCopy(this, input, compressedSize, output, decompressedSize);
        }

        @Override
        public void release() {}
    }

    /**
     * Zstd decompressor. Uses {@code Zstd.decompressDirectByteBuffer} for direct buffers to get
     * explicit offset/length control. Heap buffers fall back to the byte-array path. The JNI call
     * returns the number of decompressed bytes written; we advance the output buffer position.
     */
    private static class ZstdBytesDecompressor implements BytesInputDecompressor {
        @Override
        public BytesInput decompress(BytesInput bytes, int decompressedSize) throws IOException {
            byte[] out = new byte[decompressedSize];
            try {
                Zstd.decompress(out, bytes.toByteArray());
            } catch (RuntimeException e) {
                throw new IOException("Zstd decompression failed", e);
            }
            return BytesInput.from(out);
        }

        @Override
        public void decompress(ByteBuffer input, int compressedSize, ByteBuffer output, int decompressedSize) throws IOException {
            if (input.isDirect() && output.isDirect()) {
                int inputPos = input.position();
                int outputPos = output.position();
                long written = Zstd.decompressDirectByteBuffer(output, outputPos, decompressedSize, input, inputPos, compressedSize);
                if (Zstd.isError(written)) {
                    throw new IOException("Zstd decompression failed: " + Zstd.getErrorName(written));
                }
                output.position(outputPos + (int) written);
                input.position(inputPos + compressedSize);
            } else {
                decompressViaHeapCopy(this, input, compressedSize, output, decompressedSize);
            }
        }

        @Override
        public void release() {}
    }

    /**
     * LZ4 raw decompressor. Aircompressor's {@code Lz4Decompressor} works with both heap and
     * direct {@code ByteBuffer}s, so no fallback is needed.
     */
    private static class Lz4RawBytesDecompressor implements BytesInputDecompressor {
        private final Lz4Decompressor lz4 = new Lz4Decompressor();

        @Override
        public BytesInput decompress(BytesInput bytes, int decompressedSize) throws IOException {
            byte[] in = bytes.toByteArray();
            byte[] out = new byte[decompressedSize];
            lz4.decompress(in, 0, in.length, out, 0, decompressedSize);
            return BytesInput.from(out);
        }

        @Override
        public void decompress(ByteBuffer input, int compressedSize, ByteBuffer output, int decompressedSize) throws IOException {
            int origLimit = input.limit();
            int origPos = input.position();
            input.limit(origPos + compressedSize);
            lz4.decompress(input, output);
            input.limit(origLimit);
            input.position(origPos + compressedSize);
        }

        @Override
        public void release() {}
    }

    // --------------------------------- compressors ---------------------------------

    private static class NoopCompressor implements BytesInputCompressor {
        @Override
        public BytesInput compress(BytesInput bytes) {
            return bytes;
        }

        @Override
        public CompressionCodecName getCodecName() {
            return CompressionCodecName.UNCOMPRESSED;
        }

        @Override
        public void release() {}
    }

    private static class SnappyBytesCompressor implements BytesInputCompressor {
        @Override
        public BytesInput compress(BytesInput bytes) throws IOException {
            byte[] in = bytes.toByteArray();
            return BytesInput.from(Snappy.compress(in));
        }

        @Override
        public CompressionCodecName getCodecName() {
            return CompressionCodecName.SNAPPY;
        }

        @Override
        public void release() {}
    }

    private static class GzipBytesCompressor implements BytesInputCompressor {
        @Override
        public BytesInput compress(BytesInput bytes) throws IOException {
            ByteArrayOutputStream baos = new ByteArrayOutputStream((int) bytes.size());
            try (GZIPOutputStream gos = new GZIPOutputStream(baos)) {
                bytes.writeAllTo(gos);
            }
            return BytesInput.from(baos.toByteArray());
        }

        @Override
        public CompressionCodecName getCodecName() {
            return CompressionCodecName.GZIP;
        }

        @Override
        public void release() {}
    }

    private static class ZstdBytesCompressor implements BytesInputCompressor {
        @Override
        public BytesInput compress(BytesInput bytes) throws IOException {
            byte[] in = bytes.toByteArray();
            return BytesInput.from(Zstd.compress(in));
        }

        @Override
        public CompressionCodecName getCodecName() {
            return CompressionCodecName.ZSTD;
        }

        @Override
        public void release() {}
    }

    private static class Lz4RawBytesCompressor implements BytesInputCompressor {
        private final Lz4Compressor lz4 = new Lz4Compressor();

        @Override
        public BytesInput compress(BytesInput bytes) throws IOException {
            byte[] in = bytes.toByteArray();
            byte[] out = new byte[lz4.maxCompressedLength(in.length)];
            int compressedLen = lz4.compress(in, 0, in.length, out, 0, out.length);
            return BytesInput.from(out, 0, compressedLen);
        }

        @Override
        public CompressionCodecName getCodecName() {
            return CompressionCodecName.LZ4_RAW;
        }

        @Override
        public void release() {}
    }
}
