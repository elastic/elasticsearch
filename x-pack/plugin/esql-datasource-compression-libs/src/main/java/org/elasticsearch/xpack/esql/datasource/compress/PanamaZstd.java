/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.compress;

import org.elasticsearch.nativeaccess.NativeAccess;
import org.elasticsearch.nativeaccess.Zstd;

import java.io.InputStream;
import java.nio.ByteBuffer;

/**
 * Zero-copy zstd decompression helper backed by Elasticsearch's Panama FFI binding to libzstd
 * (the same binding Lucene's {@code ZstdCompressionMode} uses since 8.14). Lives in the shared
 * {@code esql-datasource-compression-libs} plugin so format readers (Parquet today, ORC and
 * Iceberg later) can replace per-page {@code zstd-jni} JNI calls — and the
 * {@code GetPrimitiveArrayCritical} G1GC region pinning that comes with them — by delegating
 * to a single hot-path call through this class.
 *
 * <p><b>Why compression-libs is a named Java module.</b> The package
 * {@code org.elasticsearch.nativeaccess} is not a general-purpose plugin API — it also exposes
 * process limits, mlock, exec sandbox, systemd hooks, and raw memory mapping. Widening its
 * qualified-export to all modules would surface that whole surface to every plugin in the
 * system. Declaring this plugin as a named module ({@code org.elasticsearch.xpack.esql.datasource.compress})
 * lets {@code libs/native} grant a single, narrow qualified export to it and nothing else,
 * while extending plugins (parquet, snappy, zstd, future orc/iceberg) reach {@code PanamaZstd}
 * — and the bundled compression libraries (zstd-jni, snappy-java, aircompressor) — through
 * the standard {@code extendedPlugins} parent-first classloader delegation.
 *
 * <p><b>Zero-copy contract.</b> The direct-buffer overload accepts plain direct
 * {@link ByteBuffer}s with absolute offsets and sizes. The Panama path slices the underlying
 * memory segment at those absolute positions; no temporary buffer is allocated and neither
 * buffer's {@code position}/{@code limit} is mutated by the call. Callers manage cursors
 * themselves, matching parquet-mr's {@code BytesInputDecompressor} SPI and ORC's internal
 * {@code CompressionCodec} SPI.
 *
 * <p><b>Availability.</b> {@link NativeAccess#getZstd()} returns {@code null} on platforms where
 * the native binding could not be loaded ({@code NoopNativeAccess}). Callers must check
 * {@link #isAvailable()} (or null-check the resolved instance) and fall back to a heap-based
 * path if unavailable.
 *
 * <p><b>Threading.</b> {@code Zstd.decompress} is stateless (no shared decompressor context);
 * a single instance of this class is safe to share across all decompression threads.
 */
public final class PanamaZstd {

    private static final PanamaZstd INSTANCE = createInstance();

    private final Zstd zstd;

    PanamaZstd(Zstd zstd) {
        this.zstd = zstd;
    }

    /**
     * Resolve {@link NativeAccess#instance()} defensively so that an unexpected failure during
     * native-access bootstrap surfaces as {@link #isAvailable()} returning {@code false} (callers
     * route to their existing fallback path) rather than as a class-init failure that would make
     * every subsequent {@code PanamaZstd.instance()} call fail with {@code NoClassDefFoundError}.
     * In practice, {@link NativeAccess#instance()} throwing here would already mean the rest of the
     * node's native-access singletons are broken — this catch only guarantees the parquet reader
     * does not also brick on top of that. The failure surface for the caller is the standard
     * "no Panama" path; logging is deliberately omitted because that decision belongs to whatever
     * subsystem first noticed the native bootstrap failure.
     */
    private static PanamaZstd createInstance() {
        try {
            return new PanamaZstd(NativeAccess.instance().getZstd());
        } catch (Exception | LinkageError e) {
            return new PanamaZstd(null);
        }
    }

    /**
     * Returns the process-wide instance, resolved once at class init from {@link NativeAccess#instance()}.
     * Use {@link #isAvailable()} to check whether the native binding actually loaded on this platform
     * before invoking {@link #decompressDirect}.
     */
    public static PanamaZstd instance() {
        return INSTANCE;
    }

    /**
     * @return {@code true} if the native Panama zstd binding is available on this platform.
     *         When {@code false}, callers must route through their existing fallback path.
     */
    public boolean isAvailable() {
        return zstd != null;
    }

    /**
     * Decompress {@code srcSize} bytes starting at {@code srcOffset} of the direct
     * {@link ByteBuffer} {@code src} into {@code dstSize} bytes starting at {@code dstOffset} of
     * the direct {@link ByteBuffer} {@code dst}. Returns the number of decompressed bytes
     * actually written.
     *
     * <p>Both buffers must be direct. Both offsets are absolute (independent of the buffers'
     * {@code position}/{@code limit}, which are left unmodified).
     *
     * @throws IllegalStateException    if {@link #isAvailable()} is {@code false}
     * @throws IllegalArgumentException if either buffer is not direct, the offset/size pair is
     *                                  out of range, or the native call returns a zstd error
     */
    public int decompressDirect(ByteBuffer dst, int dstOffset, int dstSize, ByteBuffer src, int srcOffset, int srcSize) {
        if (zstd == null) {
            throw new IllegalStateException("Panama zstd binding is not available on this platform");
        }
        return zstd.decompress(dst, dstOffset, dstSize, src, srcOffset, srcSize);
    }

    /**
     * One-shot heap {@code byte[]} decompress for callers that hold the full compressed frame in a
     * Java array — parquet-mr's {@code BytesInputDecompressor.decompress(BytesInput, int)} cold
     * path is the primary consumer. Routed through Panama FFI's {@code critical(true)} downcall
     * so the heap segments cross into libzstd with no off-heap staging copy. Drop-in replacement
     * for {@code com.github.luben.zstd.Zstd.decompress(byte[], byte[])}.
     *
     * @throws IllegalStateException    if {@link #isAvailable()} is {@code false}
     * @throws IllegalArgumentException if the offset/size pair is out of range or the native call
     *                                  returns a zstd error
     */
    public int decompressHeap(byte[] dst, int dstOffset, int dstSize, byte[] src, int srcOffset, int srcSize) {
        if (zstd == null) {
            throw new IllegalStateException("Panama zstd binding is not available on this platform");
        }
        return zstd.decompress(dst, dstOffset, dstSize, src, srcOffset, srcSize);
    }

    /**
     * Full-buffer convenience overload of {@link #decompressHeap(byte[], int, int, byte[], int, int)}.
     * Decompresses {@code src[0..src.length)} into {@code dst[0..dst.length)}.
     */
    public int decompressHeap(byte[] dst, byte[] src) {
        return decompressHeap(dst, 0, dst.length, src, 0, src.length);
    }

    /**
     * One-shot heap {@code byte[]} compress at the given zstd {@code level}. Returns the number of
     * compressed bytes written into {@code dst[dstOffset .. dstOffset+returned)}. The caller must
     * size {@code dst} based on {@link Zstd#compressBound(int)}.
     *
     * @throws IllegalStateException    if {@link #isAvailable()} is {@code false}
     * @throws IllegalArgumentException if the offset/size pair is out of range or libzstd returns
     *                                  an error
     */
    public int compressHeap(byte[] dst, int dstOffset, int dstSize, byte[] src, int srcOffset, int srcSize, int level) {
        if (zstd == null) {
            throw new IllegalStateException("Panama zstd binding is not available on this platform");
        }
        return zstd.compress(dst, dstOffset, dstSize, src, srcOffset, srcSize, level);
    }

    /**
     * Returns the upper bound on the compressed size of an input of {@code srcLen} bytes. Mirrors
     * libzstd's {@code ZSTD_compressBound}. Required by parquet-mr's {@code BytesInputCompressor}
     * sizing logic, which has to allocate the output array up front.
     */
    public int compressBound(int srcLen) {
        if (zstd == null) {
            throw new IllegalStateException("Panama zstd binding is not available on this platform");
        }
        return zstd.compressBound(srcLen);
    }

    /**
     * Wrap {@code compressed} in a streaming zstd decompressing {@link InputStream}, backed by the
     * Panama {@code ZSTD_decompressStream} binding. Drop-in replacement for
     * {@code com.github.luben.zstd.ZstdInputStream} on the ESQL {@code .csv.zst} / {@code .ndjson.zstd}
     * codec paths.
     *
     * <p>Hard-fails if the native binding could not be loaded — the streaming codec assumes native
     * availability (matching Lucene's zstd codec) and there is no zstd-jni fallback on this path.
     *
     * @throws IllegalStateException if {@link #isAvailable()} returns {@code false}
     */
    public InputStream wrap(InputStream compressed) {
        if (zstd == null) {
            throw new IllegalStateException("Panama zstd binding is not available on this platform");
        }
        return new PanamaZstdInputStream(compressed, zstd);
    }
}
