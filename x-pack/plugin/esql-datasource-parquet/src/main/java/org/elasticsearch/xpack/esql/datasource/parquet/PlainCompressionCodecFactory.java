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

import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.compression.CompressionCodecFactory;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.elasticsearch.common.CheckedSupplier;
import org.elasticsearch.common.util.LazyInitializable;
import org.elasticsearch.compute.data.UninitializedArrays;
import org.elasticsearch.xpack.esql.datasource.compress.PanamaZstd;
import org.xerial.snappy.Snappy;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.EnumMap;
import java.util.Map;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/**
 * Hadoop-free {@link CompressionCodecFactory} that delegates to native libraries already on the
 * plugin classpath. Zstd page decompression goes through {@link PanamaZstd}, the shared Panama
 * FFI binding to libzstd in {@code esql-datasource-compression-libs} (the same binding Lucene's
 * {@code ZstdCompressionMode} uses since 8.14), avoiding zstd-jni's
 * {@code GetPrimitiveArrayCritical} G1GC region pinning. Snappy and the LZ4 fallback still use
 * JNI (snappy-java, aircompressor); GZIP uses the JDK. Decompressors and compressors are created
 * lazily on first use so native libraries are only loaded when a Parquet file actually uses that
 * codec.
 *
 * <p>This replaces Parquet-MR's default {@code CodecFactory} which requires Hadoop's
 * {@code Configuration} and pulls in ~50MB of Hadoop JARs.
 *
 * <p>Each decompressor supports both heap and direct {@link ByteBuffer}s. When both buffers are
 * direct, the zero-copy fast path is taken: Snappy uses its JNI binding, Zstd routes through
 * {@link PanamaZstd} (the shared Panama FFI binding to libzstd, the same one Lucene's
 * {@code ZstdCompressionMode} uses, eliminating zstd-jni's {@code GetPrimitiveArrayCritical}
 * G1GC region pinning). When either buffer is heap, the decompressor falls back to the
 * byte-array path. Two call sites reach these decompressors:
 * <ul>
 *   <li>{@code PrefetchedPageReader} decompresses compressed pages onto a grow-only heap
 *       {@code byte[]} via {@link HeapDestDecompressor#decompressInto}. Uncompressed pages
 *       alias the prefetched I/O {@link BytesInput} and are not charged. The reusable dest's
 *       live capacity is charged to the request breaker until the reader closes.
 *       Snappy's heap JNI still uses {@code GetPrimitiveArrayCritical}; that is the cost of
 *       a heap destination versus glibc RSS from direct malloc. LZ4/Zstd do not take that
 *       JNI pin path. {@code decompress(BytesInput, int)} still allocates for parquet-mr's
 *       non-prefetched path and for dictionary pages.</li>
 *   <li>Parquet-MR's {@code ColumnChunkPageReadStore.ColumnChunkPageReader.readPage()} (the
 *       non-prefetched path) invokes only the {@code decompress(BytesInput, int)} overload — it
 *       never reaches the {@code ByteBuffer} overload, regardless of the allocator or the
 *       {@code useOffHeapDecryptBuffer} flag (which is a decryption-only flag). The decompressed
 *       page bytes on that path are therefore still allocated as a heap {@code byte[]} by the
 *       codec. The read-options allocator is therefore deliberately heap-backed (see
 *       {@code ParquetFormatReader.readOptionsBuilder}): nothing on that path hands its buffers
 *       to a native codec in place, so direct memory would buy no zero-copy while making release
 *       depend on garbage collection. Our {@link CircuitBreakerByteBufferAllocator} wrapper still
 *       accounts those allocations.
 *       Routing the non-prefetched decompression output through the {@code ByteBuffer} overload
 *       would require a parquet-mr change and is left as future work.</li>
 * </ul>
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
public final class PlainCompressionCodecFactory implements CompressionCodecFactory {

    private final Map<CompressionCodecName, LazyInitializable<BytesInputDecompressor, RuntimeException>> decompressors;
    private final Map<CompressionCodecName, LazyInitializable<BytesInputCompressor, RuntimeException>> compressors;

    public PlainCompressionCodecFactory() {
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

    /**
     * Heap decompress into a caller-owned dest. {@code dest.length} may exceed {@code destLen};
     * implementations write exactly {@code destLen} bytes at {@code dest[0..destLen)} and must
     * not treat {@code dest.length} as the output cap (Zstd's full-buffer {@code decompressHeap}
     * overload does that). {@link #decompress(BytesInput, int)} allocates a right-sized array and
     * delegates here so parquet-mr and dictionary pages keep working.
     *
     * <p>{@link NoopDecompressor} does not implement this: uncompressed pages never decompress.
     */
    interface HeapDestDecompressor extends BytesInputDecompressor {
        @Override
        default BytesInput decompress(BytesInput bytes, int decompressedSize) throws IOException {
            byte[] out = UninitializedArrays.newByteArray(decompressedSize);
            decompressInto(bytes, out, decompressedSize);
            return BytesInput.from(out);
        }

        void decompressInto(BytesInput compressed, byte[] dest, int destLen) throws IOException;
    }

    private static void requireDestCapacity(byte[] dest, int destLen) {
        if (destLen < 0 || destLen > dest.length) {
            throw new IllegalArgumentException(
                "decompress destLen [" + destLen + "] is out of range for dest.length [" + dest.length + "]"
            );
        }
    }

    private static void requireExactUncompressedSize(int written, int destLen, String codec) throws IOException {
        if (written != destLen) {
            throw new IOException(codec + " decompression produced " + written + " bytes, expected " + destLen + " from page header");
        }
    }

    // ------------------------------- decompressors -------------------------------

    /**
     * Pass-through decompressor for files written with {@link CompressionCodecName#UNCOMPRESSED}.
     *
     * <p>Visible at package level so {@link PrefetchedPageReader} can detect this case via
     * {@code instanceof} and return the compressed input as-is (no heap copy, no breaker charge)
     * instead of routing through {@code decompress(BytesInput, int)}. The marker check is a
     * narrow coupling to the only built-in pass-through codec.
     */
    static class NoopDecompressor implements BytesInputDecompressor {
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
     * Snappy decompressor. Heap {@code BytesInput} uses
     * {@code Snappy.uncompress(byte[], ...)} ({@code GetPrimitiveArrayCritical}). That G1 pin is
     * the cost of a heap destination versus glibc RSS from the old direct malloc path.
     * Direct-to-direct {@code Snappy.uncompress(ByteBuffer, ByteBuffer)} remains on the
     * {@code ByteBuffer} overload for tests and the parquet-mr SPI.
     */
    private static class SnappyBytesDecompressor implements HeapDestDecompressor {
        @Override
        public void decompressInto(BytesInput compressed, byte[] dest, int destLen) throws IOException {
            requireDestCapacity(dest, destLen);
            // JNI Snappy.uncompress uses dest.length as the write allowance. On a reused dest that
            // is larger than this page, a stream whose uncompressed size exceeds destLen would
            // clobber dest[destLen..). Check the stream length first, then uncompress.
            // Fast path: avoid BytesInput.toByteArray() for heap-buffer-backed inputs — the default
            // toByteArray() funnels through a sized ByteArrayOutputStream, adding one allocation
            // and one System.arraycopy that the JNI Snappy binding does not need.
            ByteBuffer input = compressed.toByteBuffer();
            if (input.hasArray()) {
                snappyUncompress(input.array(), input.arrayOffset() + input.position(), input.remaining(), dest, destLen);
            } else {
                byte[] in = compressed.toByteArray();
                snappyUncompress(in, 0, in.length, dest, destLen);
            }
        }

        private static void snappyUncompress(byte[] src, int srcOff, int srcLen, byte[] dest, int destLen) throws IOException {
            int declared = Snappy.uncompressedLength(src, srcOff, srcLen);
            if (declared != destLen) {
                throw new IOException("Snappy uncompressed length " + declared + " bytes, expected " + destLen + " from page header");
            }
            requireExactUncompressedSize(Snappy.uncompress(src, srcOff, srcLen, dest, 0), destLen, "Snappy");
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

    private static class GzipBytesDecompressor implements HeapDestDecompressor {
        @Override
        public void decompressInto(BytesInput compressed, byte[] dest, int destLen) throws IOException {
            requireDestCapacity(dest, destLen);
            try (GZIPInputStream gis = new GZIPInputStream(compressed.toInputStream())) {
                int off = 0;
                while (off < destLen) {
                    int read = gis.read(dest, off, destLen - off);
                    if (read < 0) {
                        throw new IOException("Premature end of GZIP stream: expected " + destLen + " bytes, got " + off);
                    }
                    off += read;
                }
                if (gis.read() >= 0) {
                    throw new IOException("GZIP stream produced more than " + destLen + " uncompressed bytes declared by the page header");
                }
            }
        }

        @Override
        public void decompress(ByteBuffer input, int compressedSize, ByteBuffer output, int decompressedSize) throws IOException {
            decompressViaHeapCopy(this, input, compressedSize, output, decompressedSize);
        }

        @Override
        public void release() {}
    }

    /**
     * Zstd decompressor.
     *
     * <p>The hot path for {@code PrefetchedPageReader} and parquet-mr's
     * {@code ColumnChunkPageReadStore} is {@code BytesInput}/{@code byte[]} via
     * {@link PanamaZstd#decompressHeap}, bound with {@code Linker.Option.critical(true)} so heap
     * segments cross into libzstd without an off-heap staging copy and without zstd-jni's G1
     * region pinning. Production readers use this {@code BytesInput} overload.
     * The {@code ByteBuffer} overload remains for the parquet-mr SPI and tests; it still uses a
     * direct-to-direct Panama path when both sides are direct.
     *
     * <p>When the Panama binding is unavailable on a platform ({@link PanamaZstd#isAvailable()}
     * returns {@code false}), both paths fail with the same {@link IllegalStateException} from
     * {@code PanamaZstd}. The previous second-tier zstd-jni direct-buffer fallback was removed
     * because by definition libs/native couldn't load libzstd, so the fallback would also fail —
     * it merely deferred the same error. Aligns with {@code ZstdDecompressionCodec}'s
     * hard-fail-on-construction stance.
     */
    private static class ZstdBytesDecompressor implements HeapDestDecompressor {
        private final PanamaZstd panamaZstd = PanamaZstd.instance();

        @Override
        public void decompressInto(BytesInput compressed, byte[] dest, int destLen) throws IOException {
            requireDestCapacity(dest, destLen);
            int written;
            try {
                // PanamaZstd.decompressHeap is critical(true): heap segments cross into libzstd
                // with no off-heap staging copy. Pass destLen as dstSize, not dest.length: a
                // reused dest may be larger than this page, and the two-arg decompressHeap(dest,
                // src) uses dest.length as the cap.
                ByteBuffer input = compressed.toByteBuffer();
                if (input.hasArray()) {
                    written = panamaZstd.decompressHeap(
                        dest,
                        0,
                        destLen,
                        input.array(),
                        input.arrayOffset() + input.position(),
                        input.remaining()
                    );
                } else {
                    byte[] src = compressed.toByteArray();
                    written = panamaZstd.decompressHeap(dest, 0, destLen, src, 0, src.length);
                }
            } catch (RuntimeException e) {
                throw new IOException("Zstd decompression failed", e);
            }
            requireExactUncompressedSize(written, destLen, "Zstd");
        }

        @Override
        public void decompress(ByteBuffer input, int compressedSize, ByteBuffer output, int decompressedSize) throws IOException {
            if (input.isDirect() && output.isDirect()) {
                int inputPos = input.position();
                int outputPos = output.position();
                int written;
                try {
                    written = panamaZstd.decompressDirect(output, outputPos, decompressedSize, input, inputPos, compressedSize);
                } catch (RuntimeException e) {
                    throw new IOException("Zstd decompression failed: " + e.getMessage(), e);
                }
                if (written != decompressedSize) {
                    throw new IOException(
                        "Zstd decompression produced " + written + " bytes, expected " + decompressedSize + " from page header"
                    );
                }
                output.position(outputPos + written);
                input.position(inputPos + compressedSize);
            } else {
                // At least one buffer is heap-backed: route through the BytesInput/byte[] path
                // which is now also Panama-backed (see decompress(BytesInput, int) above). The
                // previous "Panama unavailable, fall back to zstd-jni direct" branch is gone —
                // if libs/native could not load libzstd, both code paths fail identically, so
                // the fallback was masking the real failure mode.
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
    private static class Lz4RawBytesDecompressor implements HeapDestDecompressor {
        private final Lz4Decompressor lz4 = new Lz4Decompressor();

        @Override
        public void decompressInto(BytesInput compressed, byte[] dest, int destLen) throws IOException {
            requireDestCapacity(dest, destLen);
            ByteBuffer input = compressed.toByteBuffer();
            int written;
            if (input.hasArray()) {
                written = lz4.decompress(input.array(), input.arrayOffset() + input.position(), input.remaining(), dest, 0, destLen);
            } else {
                byte[] in = compressed.toByteArray();
                written = lz4.decompress(in, 0, in.length, dest, 0, destLen);
            }
            requireExactUncompressedSize(written, destLen, "LZ4_RAW");
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
    private static class Lz4HadoopFramedBytesDecompressor implements HeapDestDecompressor {
        private final Lz4Decompressor lz4 = new Lz4Decompressor();

        @Override
        public void decompressInto(BytesInput compressed, byte[] dest, int destLen) throws IOException {
            requireDestCapacity(dest, destLen);
            byte[] in = compressed.toByteArray();
            decompressHadoopFramed(in, 0, in.length, dest, 0, destLen);
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
                    // Compare against remaining space rather than adding to outWritten, so a crafted
                    // outerUncompressedLen near Integer.MAX_VALUE cannot wrap the sum to a negative
                    // value and bypass the bounds check. The loop guard guarantees
                    // outWritten < decompressedSize, so (decompressedSize - outWritten) is a
                    // non-negative int.
                    if (outerUncompressedLen > decompressedSize - outWritten) {
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
                // See the ByteBuffer overload — same overflow-safe rearrangement.
                if (outerUncompressedLen > outCapacity - outWritten) {
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
        // Default zstd compression level — matches zstd-jni's Zstd.compress(byte[]) default which
        // this method previously delegated to. parquet-mr's defaultCompressionCodecFactory uses 3
        // as well, so we preserve behavior for any caller that constructs a Parquet writer with
        // this factory. ESQL is a reader in production; this codec is only exercised by tests, but
        // we keep it on the Panama path so dropping zstd-jni from the implementation classpath is
        // clean (no orphan call site).
        private static final int DEFAULT_LEVEL = 3;
        private final PanamaZstd panamaZstd = PanamaZstd.instance();

        @Override
        public BytesInput compress(BytesInput bytes) throws IOException {
            byte[] in = bytes.toByteArray();
            byte[] out = new byte[panamaZstd.compressBound(in.length)];
            int written;
            try {
                written = panamaZstd.compressHeap(out, 0, out.length, in, 0, in.length, DEFAULT_LEVEL);
            } catch (RuntimeException e) {
                throw new IOException("Zstd compression failed", e);
            }
            return BytesInput.from(out, 0, written);
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
