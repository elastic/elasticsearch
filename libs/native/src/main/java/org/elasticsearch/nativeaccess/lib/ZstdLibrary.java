/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.nativeaccess.lib;

import org.elasticsearch.foreign.Critical;
import org.elasticsearch.foreign.Function;
import org.elasticsearch.foreign.LibrarySpecification;

import java.lang.foreign.MemorySegment;

/**
 * FFM binding for a subset of <a href="https://facebook.github.io/zstd/">libzstd</a>.
 *
 * <p>Java {@code long} parameters and return values map to C {@code size_t}, which is 8 bytes on every
 * 64-bit platform Elasticsearch ships on. Binding these as {@code int} would leave the upper bits of
 * the argument register undefined and cause libzstd to read garbage as part of the size — see
 * <a href="https://github.com/elastic/elasticsearch/pull/150690">#150690</a> for context.
 *
 * <p>The {@code *Heap} variants bind the same C symbol with {@link Critical @Critical} so the linker
 * can use the fast path when the caller passes on-heap memory segments.
 *
 * @see <a href="https://facebook.github.io/zstd/zstd_manual.html">zstd manual</a>
 */
@LibrarySpecification(name = "zstd")
public interface ZstdLibrary {

    /**
     * Maximum compressed size for a given source size, in the worst-case single-pass scenario.
     *
     * @see <a href="https://facebook.github.io/zstd/zstd_manual.html">ZSTD_compressBound</a>
     */
    @Function("ZSTD_compressBound")
    long compressBound(long srcSize);

    /**
     * Compress {@code src} as a single zstd frame into {@code dst}.
     *
     * @return the compressed size written into {@code dst} (≤ {@code dstCap}), or an error code testable
     *         with {@link #isError(long)}.
     * @see <a href="https://facebook.github.io/zstd/zstd_manual.html">ZSTD_compress</a>
     */
    @Function("ZSTD_compress")
    long compress(MemorySegment dst, long dstCap, MemorySegment src, long srcSize, int level);

    /**
     * Heap-friendly variant of {@link #compress}: same C symbol bound with the {@code critical}
     * linker option so on-heap {@link MemorySegment} arguments avoid a JNI copy. On JDK 21 the critical
     * linker option is unavailable, so the binding is wrapped by {@link ZstdHeapFallback#compressHeap} which
     * stages the heap segments off-heap before the libzstd call.
     */
    @Function("ZSTD_compress")
    @Critical(fallbackAdapter = ZstdHeapFallback.class)
    long compressHeap(MemorySegment dst, long dstCap, MemorySegment src, long srcSize, int level);

    /**
     * Decompress one or more complete zstd frames in {@code src} into {@code dst}. {@code srcSize}
     * must be the exact size of the compressed input.
     *
     * @return the number of bytes written into {@code dst} (≤ {@code dstCap}), or an error code testable
     *         with {@link #isError(long)}.
     * @see <a href="https://facebook.github.io/zstd/zstd_manual.html">ZSTD_decompress</a>
     */
    @Function("ZSTD_decompress")
    long decompress(MemorySegment dst, long dstCap, MemorySegment src, long srcSize);

    /**
     * Heap-friendly variant of {@link #decompress}: same C symbol bound with the {@code critical}
     * linker option so on-heap {@link MemorySegment} arguments avoid a JNI copy. On JDK 21 the critical
     * linker option is unavailable, so the binding is wrapped by {@link ZstdHeapFallback#decompressHeap} which
     * stages the heap segments off-heap before the libzstd call.
     */
    @Function("ZSTD_decompress")
    @Critical(fallbackAdapter = ZstdHeapFallback.class)
    long decompressHeap(MemorySegment dst, long dstCap, MemorySegment src, long srcSize);

    /**
     * Tests whether a {@code size_t} return value from the one-shot or streaming APIs encodes an error.
     *
     * @see <a href="https://facebook.github.io/zstd/zstd_manual.html">ZSTD_isError</a>
     */
    @Function("ZSTD_isError")
    boolean isError(long code);

    /**
     * Returns a human-readable name for the error encoded in {@code code}, or {@code "No Error"} when
     * {@code code} is not an error.
     *
     * @see <a href="https://facebook.github.io/zstd/zstd_manual.html">ZSTD_getErrorName</a>
     */
    @Function("ZSTD_getErrorName")
    String getErrorName(long code);

    /**
     * Allocate a streaming decompression context ({@code ZSTD_DStream}). Returns {@code NULL} (i.e. a
     * zero address) on allocation failure. The returned segment must be released with
     * {@link #freeDStream(MemorySegment)}.
     *
     * @see <a href="https://facebook.github.io/zstd/zstd_manual.html">ZSTD_createDStream</a>
     */
    @Function("ZSTD_createDStream")
    MemorySegment createDStream();

    /**
     * Release a streaming decompression context previously obtained from {@link #createDStream()}.
     * Accepts a {@code NULL} address as a no-op.
     *
     * @see <a href="https://facebook.github.io/zstd/zstd_manual.html">ZSTD_freeDStream</a>
     */
    @Function("ZSTD_freeDStream")
    long freeDStream(MemorySegment dstream);

    /**
     * Recommended input buffer size for streaming decompression.
     *
     * @see <a href="https://facebook.github.io/zstd/zstd_manual.html">ZSTD_DStreamInSize</a>
     */
    @Function("ZSTD_DStreamInSize")
    long dStreamInSize();

    /**
     * Recommended output buffer size for streaming decompression. libzstd guarantees this is enough to
     * flush at least one complete decompressed block.
     *
     * @see <a href="https://facebook.github.io/zstd/zstd_manual.html">ZSTD_DStreamOutSize</a>
     */
    @Function("ZSTD_DStreamOutSize")
    long dStreamOutSize();

    /**
     * Drive one step of streaming decompression. Called repeatedly with the same context, advancing the
     * {@code pos} fields on the {@code ZSTD_outBuffer} / {@code ZSTD_inBuffer} structs.
     *
     * @return {@code 0} when a frame is completely decoded and fully flushed, an error code testable
     *         with {@link #isError(long)}, or any other positive value, which is a hint at how many more
     *         input bytes are expected to complete the current frame.
     * @see <a href="https://facebook.github.io/zstd/zstd_manual.html">ZSTD_decompressStream</a>
     */
    @Function("ZSTD_decompressStream")
    long decompressStream(MemorySegment dstream, MemorySegment output, MemorySegment input);
}
