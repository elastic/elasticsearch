/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.parquet;

import io.airlift.compress.MalformedInputException;
import io.airlift.compress.lz4.Lz4Compressor;
import io.airlift.compress.lz4.Lz4Decompressor;

import com.github.luben.zstd.Zstd;

import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.compression.CompressionCodecFactory;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.elasticsearch.common.CheckedSupplier;
import org.elasticsearch.common.util.LazyInitializable;
import org.elasticsearch.compute.data.UninitializedArrays;
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
        // Legacy Hadoop-framed LZ4 (CompressionCodecName.LZ4) is read-only — see
        // Lz4HadoopFramedBytesDecompressor for the rationale and frame format. No matching entry
        // is added to the compressors map: the codec is deprecated by the parquet-format spec
        // and ES|QL must not emit it. getCompressor(LZ4) continues to throw.
        dec.put(CompressionCodecName.LZ4, lazy(Lz4HadoopFramedBytesDecompressor::new));
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
     * Snappy decompressor. Two JNI overloads are used depending on input shape:
     * <ul>
     *   <li>{@code Snappy.uncompress(byte[], int, int, byte[], int)} when the compressed input is
     *       backed by a Java heap array (the common case for the prefetch path: column chunks
     *       arrive as {@link ByteBuffer#wrap(byte[], int, int)}-style {@code BytesInput}s).</li>
     *   <li>{@code Snappy.uncompress(ByteBuffer, ByteBuffer)} when both input and output are
     *       direct buffers — the only case where the JNI binding can avoid a copy.</li>
     * </ul>
     * The JNI call returns the number of decompressed bytes written but does not advance the
     * output buffer position; we advance it manually for the {@code ByteBuffer} overload.
     */
    private static class SnappyBytesDecompressor implements BytesInputDecompressor {
        @Override
        public BytesInput decompress(BytesInput bytes, int decompressedSize) throws IOException {
            byte[] out = UninitializedArrays.newByteArray(decompressedSize);
            // Fast path: avoid BytesInput.toByteArray() for byte-array- or heap-buffer-backed inputs
            // (the hot path for S3-prefetched column chunks). The default toByteArray() for those
            // BytesInput subtypes funnels bytes through a sized ByteArrayOutputStream, adding one
            // allocation and one System.arraycopy that the JNI Snappy binding does not need.
            ByteBuffer input = bytes.toByteBuffer();
            if (input.hasArray()) {
                Snappy.uncompress(input.array(), input.arrayOffset() + input.position(), input.remaining(), out, 0);
            } else {
                // Off-heap inputs (rare on this path) fall back to a single byte[] copy via
                // toByteArray(). The byte[] overload is preferred over the ByteBuffer overload
                // because the latter would also need to copy into a direct output buffer.
                byte[] in = bytes.toByteArray();
                Snappy.uncompress(in, 0, in.length, out, 0);
            }
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
            byte[] out = UninitializedArrays.newByteArray(decompressedSize);
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
            byte[] out = UninitializedArrays.newByteArray(decompressedSize);
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
            byte[] out = UninitializedArrays.newByteArray(decompressedSize);
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

    /**
     * Legacy Hadoop-framed LZ4 decompressor — reads files written with
     * {@link CompressionCodecName#LZ4} (the deprecated codec, distinct from {@code LZ4_RAW}).
     *
     * <p>This codec wraps raw LZ4 block-format payloads in Hadoop's {@code BlockCompressorStream}
     * framing — the same framing parquet-mr embeds when it writes the legacy codec. The framing
     * is:
     *
     * <pre>
     * [outer uncompressed length: int32 big-endian]
     *   one or more sub-blocks:
     *     [sub-block compressed length: int32 big-endian]
     *     [sub-block compressed bytes: raw LZ4 block format]
     * </pre>
     *
     * <p>Sub-blocks accumulate until the decompressed bytes written equal the outer uncompressed
     * length. In practice parquet-mr produces a single sub-block per column chunk page, but the
     * Hadoop frame format permits multiple sub-blocks and this decompressor honors it.
     *
     * <p>The implementation strips the Hadoop frame in plain Java and delegates each sub-block to
     * the existing aircompressor {@link Lz4Decompressor} — the same library used for
     * {@link CompressionCodecName#LZ4_RAW}. No Hadoop dependency is required, which is the entire
     * reason {@link PlainCompressionCodecFactory} exists: keep the ~50 MB Hadoop jar off the
     * runtime classpath.
     *
     * <p>This codec is deliberately read-only. The parquet-format spec deprecated it in November
     * 2021 in favor of {@code LZ4_RAW} (see PARQUET-2032); ES|QL accepts files written during the
     * deprecation window (notably ClickHouse {@code FORMAT Parquet} exports from v23.3 through
     * mid-2024, and Spark 3.0–3.4 with explicit {@code lz4} compression) but never emits the
     * deprecated codec itself. No entry is registered in the compressors map.
     */
    private static class Lz4HadoopFramedBytesDecompressor implements BytesInputDecompressor {
        private final Lz4Decompressor lz4 = new Lz4Decompressor();

        @Override
        public BytesInput decompress(BytesInput bytes, int decompressedSize) throws IOException {
            byte[] in = bytes.toByteArray();
            byte[] out = UninitializedArrays.newByteArray(decompressedSize);
            decompressHadoopFramed(in, 0, in.length, out, 0, decompressedSize);
            return BytesInput.from(out);
        }

        @Override
        public void decompress(ByteBuffer input, int compressedSize, ByteBuffer output, int decompressedSize) throws IOException {
            // The frame envelope and sub-block headers are parsed via ByteBuffer slicing, then each
            // sub-block is handed to aircompressor's ByteBuffer decompress overload — preserving the
            // direct-buffer fast path when both buffers are off-heap and avoiding any extra copy of
            // the compressed payload regardless of buffer kind. The byte-array fallback is reserved
            // for inputs that are neither direct nor heap-backed (rare).
            if (input.hasArray() == false && input.isDirect() == false) {
                decompressViaHeapCopy(this, input, compressedSize, output, decompressedSize);
                return;
            }
            int origLimit = input.limit();
            int origPos = input.position();
            int compressedEnd = origPos + compressedSize;
            input.limit(compressedEnd);
            try {
                int outWritten = 0;
                while (outWritten < decompressedSize) {
                    if (input.remaining() < 4) {
                        throw new IOException("Hadoop-framed LZ4: truncated outer length header");
                    }
                    // Read BE int32 independently of the buffer's current byte order — the parquet
                    // read path doesn't set order explicitly today, but defending against a caller
                    // that does prevents silent corruption.
                    int outerUncompressedLen = readIntBE(input);
                    if (outerUncompressedLen <= 0) {
                        throw new IOException("Hadoop-framed LZ4: invalid outer uncompressed length " + outerUncompressedLen);
                    }
                    if (outWritten + outerUncompressedLen > decompressedSize) {
                        throw new IOException(
                            "Hadoop-framed LZ4: outer length "
                                + outerUncompressedLen
                                + " at offset "
                                + outWritten
                                + " exceeds declared decompressed size "
                                + decompressedSize
                        );
                    }
                    int outerEnd = outWritten + outerUncompressedLen;
                    while (outWritten < outerEnd) {
                        if (input.remaining() < 4) {
                            throw new IOException("Hadoop-framed LZ4: truncated sub-block length header");
                        }
                        int subCompressedLen = readIntBE(input);
                        if (subCompressedLen <= 0 || subCompressedLen > input.remaining()) {
                            throw new IOException(
                                "Hadoop-framed LZ4: invalid sub-block compressed length "
                                    + subCompressedLen
                                    + " (remaining "
                                    + input.remaining()
                                    + ")"
                            );
                        }
                        // Slice the sub-block into its own ByteBuffer view and a same-kind sliced
                        // output view. Aircompressor consumes the entire source buffer up to its
                        // limit and advances the output buffer position by the number of bytes
                        // written; we then advance our cursors accordingly.
                        int subInPos = input.position();
                        ByteBuffer subIn = input.duplicate();
                        subIn.position(subInPos).limit(subInPos + subCompressedLen);
                        ByteBuffer subOut = output.duplicate();
                        int subOutPos = output.position() + outWritten;
                        subOut.position(subOutPos).limit(output.position() + outerEnd);
                        try {
                            lz4.decompress(subIn, subOut);
                        } catch (MalformedInputException e) {
                            throw new IOException("Hadoop-framed LZ4: malformed sub-block at output offset " + outWritten, e);
                        }
                        int written = subOut.position() - subOutPos;
                        if (written <= 0) {
                            throw new IOException("Hadoop-framed LZ4: sub-block decoded to 0 bytes");
                        }
                        outWritten += written;
                        input.position(subInPos + subCompressedLen);
                    }
                    if (outWritten != outerEnd) {
                        throw new IOException(
                            "Hadoop-framed LZ4: outer block underflow, expected " + outerEnd + " uncompressed bytes, got " + outWritten
                        );
                    }
                }
                if (input.position() != compressedEnd) {
                    throw new IOException(
                        "Hadoop-framed LZ4: trailing bytes after frame, " + (compressedEnd - input.position()) + " bytes unconsumed"
                    );
                }
                output.position(output.position() + outWritten);
            } finally {
                input.limit(origLimit);
                input.position(compressedEnd);
            }
        }

        @Override
        public void release() {}

        private void decompressHadoopFramed(byte[] in, int inOff, int inLen, byte[] out, int outOff, int outCapacity) throws IOException {
            int inEnd = inOff + inLen;
            int inPos = inOff;
            int outWritten = 0;
            while (outWritten < outCapacity) {
                if (inEnd - inPos < 4) {
                    throw new IOException("Hadoop-framed LZ4: truncated outer length header");
                }
                int outerUncompressedLen = readIntBE(in, inPos);
                inPos += 4;
                if (outerUncompressedLen <= 0) {
                    throw new IOException("Hadoop-framed LZ4: invalid outer uncompressed length " + outerUncompressedLen);
                }
                if (outWritten + outerUncompressedLen > outCapacity) {
                    throw new IOException(
                        "Hadoop-framed LZ4: outer length "
                            + outerUncompressedLen
                            + " at offset "
                            + outWritten
                            + " exceeds declared decompressed size "
                            + outCapacity
                    );
                }
                int outerEnd = outWritten + outerUncompressedLen;
                while (outWritten < outerEnd) {
                    if (inEnd - inPos < 4) {
                        throw new IOException("Hadoop-framed LZ4: truncated sub-block length header");
                    }
                    int subCompressedLen = readIntBE(in, inPos);
                    inPos += 4;
                    if (subCompressedLen <= 0 || subCompressedLen > inEnd - inPos) {
                        throw new IOException(
                            "Hadoop-framed LZ4: invalid sub-block compressed length "
                                + subCompressedLen
                                + " (remaining "
                                + (inEnd - inPos)
                                + ")"
                        );
                    }
                    int written;
                    try {
                        written = lz4.decompress(in, inPos, subCompressedLen, out, outOff + outWritten, outerEnd - outWritten);
                    } catch (MalformedInputException e) {
                        throw new IOException("Hadoop-framed LZ4: malformed sub-block at output offset " + outWritten, e);
                    }
                    if (written <= 0) {
                        throw new IOException("Hadoop-framed LZ4: sub-block decoded to 0 bytes");
                    }
                    outWritten += written;
                    inPos += subCompressedLen;
                }
                if (outWritten != outerEnd) {
                    throw new IOException(
                        "Hadoop-framed LZ4: outer block underflow, expected " + outerEnd + " uncompressed bytes, got " + outWritten
                    );
                }
            }
            if (inPos != inEnd) {
                throw new IOException("Hadoop-framed LZ4: trailing bytes after frame, " + (inEnd - inPos) + " bytes unconsumed");
            }
        }

        private static int readIntBE(byte[] buf, int off) {
            return ((buf[off] & 0xFF) << 24) | ((buf[off + 1] & 0xFF) << 16) | ((buf[off + 2] & 0xFF) << 8) | (buf[off + 3] & 0xFF);
        }

        private static int readIntBE(ByteBuffer in) {
            int b1 = in.get() & 0xFF;
            int b2 = in.get() & 0xFF;
            int b3 = in.get() & 0xFF;
            int b4 = in.get() & 0xFF;
            return (b1 << 24) | (b2 << 16) | (b3 << 8) | b4;
        }
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
            byte[] out = UninitializedArrays.newByteArray(lz4.maxCompressedLength(in.length));
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
