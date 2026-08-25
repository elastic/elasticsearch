/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.nativeaccess;

import org.elasticsearch.foreign.CloseableByteBuffer;
import org.elasticsearch.nativeaccess.lib.ZstdLibrary;

import java.lang.foreign.Arena;
import java.lang.foreign.MemoryLayout;
import java.lang.foreign.MemoryLayout.PathElement;
import java.lang.foreign.MemorySegment;
import java.lang.invoke.VarHandle;
import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.concurrent.atomic.LongAdder;

import static java.lang.foreign.ValueLayout.ADDRESS;
import static java.lang.foreign.ValueLayout.JAVA_BYTE;
import static java.lang.foreign.ValueLayout.JAVA_LONG;
import static org.elasticsearch.foreign.adapter.MemorySegmentAdapter.varHandleWithoutOffset;

public final class Zstd {

    // ZSTD_inBuffer / ZSTD_outBuffer share the same C layout `{ void* ptr; size_t size; size_t pos; }`
    // (libzstd 1.5.7). `size_t` is bound as JAVA_LONG (8 bytes) — correct on all 64-bit targets we ship,
    // including Windows where size_t is `unsigned long long`.
    private static final MemoryLayout BUFFER_LAYOUT = MemoryLayout.structLayout(
        ADDRESS.withName("ptr"),
        JAVA_LONG.withName("size"),
        JAVA_LONG.withName("pos")
    );
    private static final VarHandle PTR_VH = varHandleWithoutOffset(BUFFER_LAYOUT, PathElement.groupElement("ptr"));
    private static final VarHandle SIZE_VH = varHandleWithoutOffset(BUFFER_LAYOUT, PathElement.groupElement("size"));
    private static final VarHandle POS_VH = varHandleWithoutOffset(BUFFER_LAYOUT, PathElement.groupElement("pos"));

    // The two ZSTD_inBuffer/ZSTD_outBuffer struct holders allocated in the DStream arena.
    private static final long DSTREAM_STRUCT_HOLDER_BYTES = 2L * BUFFER_LAYOUT.byteSize();

    // ZSTD_d_windowLogMax enum value (ZSTD_dParameter) — the streaming decoder refuses to allocate a
    // back-reference window larger than (1 << value). Stable API since libzstd 1.4.0.
    private static final int ZSTD_D_PARAM_WINDOW_LOG_MAX = 100;
    // Cap the decode window at 8 MiB (windowLog 23). This bounds the worst-case native allocation per
    // stream against maliciously or accidentally large-window frames. windowLog 23 accepts every frame
    // produced by standard zstd levels (1-19 top out at a 8 MiB window); it rejects `--ultra` (levels
    // 20-22) and `--long` frames, which libzstd would otherwise honour up to its 128 MiB default limit.
    // ClickHouse and DuckDB both default to that 128 MiB limit and expose a knob to raise it; we choose a
    // tighter default because external data-source files are largely producer-controlled. A frame above
    // the cap fails fast with libzstd's "Frame requires too much memory for decoding".
    private static final int DECOMPRESS_WINDOW_LOG_MAX = 23;

    /**
     * Visible-for-testing running total of native bytes currently reserved by live {@link DStream}s.
     * A stream's {@link DStream#accountedBytes()} footprint is added once the stream is fully
     * constructed and subtracted on {@link DStream#close()}. This is an accounting/visibility figure
     * (staging buffers + struct holders + a libzstd context estimate), not an exact RSS measurement.
     */
    static final LongAdder NATIVE_BYTES_IN_USE = new LongAdder();

    private final ZstdLibrary zstdLib;

    Zstd(ZstdLibrary zstdLib) {
        this.zstdLib = zstdLib;
    }

    /**
     * Compress the content of {@code src} into {@code dst} at compression level {@code level}, and return the number of compressed bytes.
     * {@link ByteBuffer#position()} and {@link ByteBuffer#limit()} of both {@link ByteBuffer}s are left unmodified.
     */
    public int compress(CloseableByteBuffer dst, CloseableByteBuffer src, int level) {
        Objects.requireNonNull(dst, "Null destination buffer");
        Objects.requireNonNull(src, "Null source buffer");
        long dstSize = dst.buffer().remaining();
        long srcSize = src.buffer().remaining();
        long ret = zstdLib.compress(MemorySegment.ofBuffer(dst.buffer()), dstSize, MemorySegment.ofBuffer(src.buffer()), srcSize, level);
        if (zstdLib.isError(ret)) {
            throw new IllegalArgumentException(zstdLib.getErrorName(ret));
        } else if (ret < 0 || ret > Integer.MAX_VALUE) {
            throw new IllegalStateException("Integer overflow? ret=" + ret);
        }
        return (int) ret;
    }

    /**
     * Decompress the content of {@code src} into {@code dst}, and return the number of decompressed bytes. {@link ByteBuffer#position()}
     * and {@link ByteBuffer#limit()} of both {@link ByteBuffer}s are left unmodified.
     */
    public int decompress(CloseableByteBuffer dst, CloseableByteBuffer src) {
        Objects.requireNonNull(dst, "Null destination buffer");
        Objects.requireNonNull(src, "Null source buffer");
        long dstSize = dst.buffer().remaining();
        long srcSize = src.buffer().remaining();
        long ret = zstdLib.decompress(MemorySegment.ofBuffer(dst.buffer()), dstSize, MemorySegment.ofBuffer(src.buffer()), srcSize);
        if (zstdLib.isError(ret)) {
            throw new IllegalArgumentException(zstdLib.getErrorName(ret));
        } else if (ret < 0 || ret > Integer.MAX_VALUE) {
            throw new IllegalStateException("Integer overflow? ret=" + ret);
        }
        return (int) ret;
    }

    /**
     * Decompress the content of {@code src} into {@code dst}, and return the number of decompressed bytes.
     * Both segments may be native or heap-backed. On JDK 22+ heap segments are passed through the
     * critical downcall without copying; on JDK 21 the fallback adapter stages them via a confined arena.
     */
    public int decompress(MemorySegment dst, MemorySegment src) {
        Objects.requireNonNull(dst, "Null dst segment");
        Objects.requireNonNull(src, "Null src segment");
        long ret = zstdLib.decompressHeap(dst, dst.byteSize(), src, src.byteSize());
        if (zstdLib.isError(ret)) {
            throw new IllegalArgumentException(zstdLib.getErrorName(ret));
        } else if (ret < 0 || ret > Integer.MAX_VALUE) {
            throw new IllegalStateException("Integer overflow? ret=" + ret);
        }
        return (int) ret;
    }

    /**
     * Decompress {@code srcSize} bytes starting at {@code srcOffset} of the direct {@link ByteBuffer} {@code src} into
     * {@code dstSize} bytes starting at {@code dstOffset} of the direct {@link ByteBuffer} {@code dst}, and return the
     * number of decompressed bytes. Both buffers must be direct. {@link ByteBuffer#position()} and {@link ByteBuffer#limit()}
     * of both buffers are left unmodified — callers manage their own cursors. Suits external codec APIs that pass plain
     * {@link ByteBuffer}s with explicit offsets/sizes (e.g. parquet-mr's {@code BytesInputDecompressor}).
     */
    public int decompress(ByteBuffer dst, int dstOffset, int dstSize, ByteBuffer src, int srcOffset, int srcSize) {
        Objects.requireNonNull(dst, "Null destination buffer");
        Objects.requireNonNull(src, "Null source buffer");
        if (dst.isDirect() == false) {
            throw new IllegalArgumentException("Destination buffer must be direct");
        }
        if (src.isDirect() == false) {
            throw new IllegalArgumentException("Source buffer must be direct");
        }
        checkRange("Destination", dstOffset, dstSize, dst.capacity());
        checkRange("Source", srcOffset, srcSize, src.capacity());
        // Use absolute addressing: MemorySegment.ofBuffer(buf) covers [position, limit), so
        // duplicate().clear() yields a non-mutating view over [0, capacity) on which the
        // explicit (offset, size) slice resolves to absolute byte positions in the buffer.
        var segmentDst = MemorySegment.ofBuffer(dst.duplicate().clear()).asSlice(dstOffset, dstSize);
        var segmentSrc = MemorySegment.ofBuffer(src.duplicate().clear()).asSlice(srcOffset, srcSize);
        long ret = zstdLib.decompress(segmentDst, (long) dstSize, segmentSrc, (long) srcSize);
        if (zstdLib.isError(ret)) {
            throw new IllegalArgumentException(zstdLib.getErrorName(ret));
        } else if (ret < 0 || ret > Integer.MAX_VALUE) {
            throw new IllegalStateException("Integer overflow? ret=" + ret);
        }
        return (int) ret;
    }

    /**
     * One-shot heap {@code byte[]} decompress for callers that already hold the full compressed
     * frame in a Java array (e.g. parquet-mr's {@code BytesInputDecompressor#decompress(BytesInput,int)}).
     * Equivalent to {@code com.github.luben.zstd.Zstd.decompress(byte[], byte[])} but routed through
     * Panama FFI's {@code critical(true)} downcall — the heap segments are passed through directly
     * without an off-heap staging copy. Returns the number of decompressed bytes actually written
     * into {@code dst[dstOffset .. dstOffset+dstSize)}.
     */
    public int decompress(byte[] dst, int dstOffset, int dstSize, byte[] src, int srcOffset, int srcSize) {
        Objects.requireNonNull(dst, "Null destination buffer");
        Objects.requireNonNull(src, "Null source buffer");
        checkRange("Destination", dstOffset, dstSize, dst.length);
        checkRange("Source", srcOffset, srcSize, src.length);
        long ret = zstdLib.decompressHeap(
            MemorySegment.ofArray(dst).asSlice(dstOffset, dstSize),
            (long) dstSize,
            MemorySegment.ofArray(src).asSlice(srcOffset, srcSize),
            (long) srcSize
        );
        if (zstdLib.isError(ret)) {
            throw new IllegalArgumentException(zstdLib.getErrorName(ret));
        } else if (ret < 0 || ret > Integer.MAX_VALUE) {
            throw new IllegalStateException("Integer overflow? ret=" + ret);
        }
        return (int) ret;
    }

    /**
     * One-shot heap {@code byte[]} compress at the given zstd compression {@code level}. The caller
     * must size {@code dst} to at least {@link #compressBound(int)} bytes for {@code srcSize};
     * Parquet's {@code BytesInputCompressor} sizes its output buffer based on libzstd's worst case.
     * Returns the actual compressed length written into {@code dst[dstOffset .. dstOffset+returned)}.
     */
    public int compress(byte[] dst, int dstOffset, int dstSize, byte[] src, int srcOffset, int srcSize, int level) {
        Objects.requireNonNull(dst, "Null destination buffer");
        Objects.requireNonNull(src, "Null source buffer");
        checkRange("Destination", dstOffset, dstSize, dst.length);
        checkRange("Source", srcOffset, srcSize, src.length);
        long ret = zstdLib.compressHeap(
            MemorySegment.ofArray(dst).asSlice(dstOffset, dstSize),
            (long) dstSize,
            MemorySegment.ofArray(src).asSlice(srcOffset, srcSize),
            (long) srcSize,
            level
        );
        if (zstdLib.isError(ret)) {
            throw new IllegalArgumentException(zstdLib.getErrorName(ret));
        } else if (ret < 0 || ret > Integer.MAX_VALUE) {
            throw new IllegalStateException("Integer overflow? ret=" + ret);
        }
        return (int) ret;
    }

    private static void checkRange(String label, int offset, int size, int capacity) {
        // The (offset > capacity - size) form avoids the (offset + size > capacity) overflow
        // that could otherwise wrap around when offset + size exceeds Integer.MAX_VALUE.
        if (offset < 0 || size < 0 || offset > capacity - size) {
            throw new IllegalArgumentException(
                label + " range [offset=" + offset + ", size=" + size + ") is out of bounds for buffer with capacity " + capacity
            );
        }
    }

    /**
     * Return the maximum number of compressed bytes given an input length.
     */
    public int compressBound(int srcLen) {
        long ret = zstdLib.compressBound((long) srcLen);
        if (zstdLib.isError(ret)) {
            throw new IllegalArgumentException(zstdLib.getErrorName(ret));
        } else if (ret < 0 || ret > Integer.MAX_VALUE) {
            throw new IllegalArgumentException(
                srcLen
                    + " bytes may require up to "
                    + Long.toUnsignedString(ret)
                    + " bytes, which overflows the maximum capacity of a ByteBuffer"
            );
        }
        return (int) ret;
    }

    /**
     * Recommended size for the input buffer feeding {@link DStream#decompress} ({@code ZSTD_DStreamInSize}).
     * The value is constant per libzstd build (≈ 128 KB on 1.5.7); callers typically cache it in a
     * {@code static final} field rather than calling per-stream.
     */
    public int dStreamInSize() {
        return checkSize(zstdLib.dStreamInSize(), "dStreamInSize");
    }

    /**
     * Recommended size for the output buffer of {@link DStream#decompress} ({@code ZSTD_DStreamOutSize}).
     * Used only by the {@code skip()} scratch buffer in the wrapper — the regular read path writes
     * straight into the caller's destination array.
     */
    public int dStreamOutSize() {
        return checkSize(zstdLib.dStreamOutSize(), "dStreamOutSize");
    }

    private int checkSize(long ret, String which) {
        if (zstdLib.isError(ret)) {
            throw new IllegalArgumentException(zstdLib.getErrorName(ret));
        } else if (ret <= 0 || ret > Integer.MAX_VALUE) {
            throw new IllegalStateException("Unexpected " + which + " value: " + Long.toUnsignedString(ret));
        }
        return (int) ret;
    }

    /**
     * Allocate a streaming decompression context wrapping {@code ZSTD_DStream}. The returned
     * {@link DStream} owns a native zstd context plus two small persistent struct holders;
     * call {@link DStream#close()} (or use try-with-resources) to release them. Single-threaded
     * by contract — do not share a single {@link DStream} across threads.
     */
    public DStream newDStream() {
        long inSize = zstdLib.dStreamInSize();
        long outSize = zstdLib.dStreamOutSize();
        if (inSize <= 0 || inSize > Integer.MAX_VALUE || outSize <= 0 || outSize > Integer.MAX_VALUE) {
            throw new IllegalStateException("libzstd reported unreasonable stream buffer sizes: in=" + inSize + ", out=" + outSize);
        }
        MemorySegment handle = zstdLib.createDStream();
        if (handle == null || handle.equals(MemorySegment.NULL)) {
            throw new IllegalStateException("ZSTD_createDStream returned NULL");
        }
        // Pin the maximum decode window before any input is fed. This bounds the native context the
        // decoder may allocate; a frame declaring a larger window is rejected on the first
        // decompressStream call rather than honoured. Roll back the handle if the parameter is refused
        // so a failed newDStream() never leaks a native context.
        long paramRc = zstdLib.dctxSetParameter(handle, ZSTD_D_PARAM_WINDOW_LOG_MAX, DECOMPRESS_WINDOW_LOG_MAX);
        if (zstdLib.isError(paramRc)) {
            String err = zstdLib.getErrorName(paramRc);
            long freeRc = zstdLib.freeDStream(handle);
            assert freeRc == 0 : "ZSTD_freeDStream returned " + freeRc;
            throw new IllegalStateException("Failed to set ZSTD_d_windowLogMax=" + DECOMPRESS_WINDOW_LOG_MAX + ": " + err);
        }
        return new DStream(handle, (int) inSize, (int) outSize);
    }

    /**
     * Streaming-mode zstd decompression resource. Backs {@code PanamaZstdInputStream} and any
     * other consumer that needs incremental {@code ZSTD_decompressStream} semantics: refill an
     * input chunk, call {@link #decompress}, read {@link #lastSrcPos()} / {@link #lastDstPos()} to
     * advance cursors, loop while the hint return is positive (more input wanted), and observe a
     * zero return when the current frame finished.
     *
     * <p>Bounds are validated Java-side; error returns from libzstd are translated to
     * {@link IllegalArgumentException} carrying the {@code ZSTD_getErrorName} string. All hot-path
     * state (the two {@code ZSTD_inBuffer}/{@code ZSTD_outBuffer} struct holders, the native context
     * handle) lives inside the binding — every {@link #decompress} call is allocation-free.
     *
     * <p>Not thread-safe. A single {@link DStream} is owned by one reader for its whole lifetime;
     * there is deliberately no locking. Calling {@link #decompress} concurrently with {@link #close()}
     * (from another thread) is a use-after-free: {@link #close()} releases the shared arena backing the
     * staging buffers while a racing {@code decompress} may still be reading them. Callers must
     * establish their own happens-before ordering between the last read and the close.
     */
    public final class DStream implements AutoCloseable {

        /**
         * Stateful binding for {@code ZSTD_decompressStream}. Owns the opaque {@code ZSTD_DStream*}
         * handle plus a single shared arena holding four persistent off-heap allocations: two 24-byte
         * struct holders ({@code ZSTD_inBuffer} / {@code ZSTD_outBuffer}) and two native staging buffers
         * sized to {@code ZSTD_DStreamInSize} / {@code ZSTD_DStreamOutSize}.
         *
         * <p><b>Why staging buffers and not heap segments.</b> Panama explicitly forbids embedding a
         * heap {@link MemorySegment} into an off-heap struct's {@code ADDRESS} field and then handing
         * that struct to a downcall ({@code JDK-8318645}): the in-struct heap pointer cannot be resolved
         * during the call. Marking the downcall critical only allows heap segments as <em>direct</em>
         * downcall arguments — but libzstd's signature requires us to pass an off-heap struct. So we
         * pay one memcpy per refill (input side) and per chunk produced (output side); both are
         * dominated by libzstd's own work and are the trade-off the Panama linker forces here.
         *
         * <p>The arena is closed on {@link #close()}, releasing all native memory back to the OS.
         *
         * <p><b>Threading.</b> The arena is {@link Arena#ofShared() shared}, not confined: the wrapper
         * is constructed eagerly when the codec opens the file, but the actual {@link #decompress}
         * calls run on whatever worker thread {@code StreamingParallelParsingCoordinator}'s segmentator
         * is scheduled on — almost never the constructing thread. A confined arena would raise
         * {@link java.lang.WrongThreadException} from {@code MemorySegment.copy} on the first read.
         * Shared arenas pay a small synchronization cost on {@link Arena#close()}, which is negligible
         * relative to libzstd's per-call work. The DStream itself is still single-reader by contract —
         * the segmentator owns it for the lifetime of one file.
         */
        private final Arena arena = Arena.ofShared();
        private final MemorySegment handle;
        private final MemorySegment inStruct;
        private final MemorySegment outStruct;
        private final MemorySegment inBuf;
        private final MemorySegment outBuf;
        private final int inBufSize;
        private final int outBufSize;
        // Off-heap memory this wrapper allocates itself: the two staging buffers plus the two struct
        // holders. Known exactly at construction and constant for the stream's lifetime.
        private final long fixedOverhead;
        // Live size of the libzstd context, sampled from ZSTD_sizeof_DStream. Small before the frame
        // header is parsed, then grows once the back-reference window is allocated on the first
        // decompress call — at which point {@link #settleAccounting} re-samples it exactly once.
        private long accountedContext;
        private boolean accountingSettled = false;
        private int lastSrcPosAbsolute = 0;
        private int lastDstPosAbsolute = 0;
        private boolean closed = false;

        DStream(MemorySegment handle, int inBufSize, int outBufSize) {
            this.handle = handle;
            this.inBufSize = inBufSize;
            this.outBufSize = outBufSize;
            long overhead;
            try {
                this.inStruct = arena.allocate(BUFFER_LAYOUT);
                this.outStruct = arena.allocate(BUFFER_LAYOUT);
                this.inBuf = arena.allocate(inBufSize);
                this.outBuf = arena.allocate(outBufSize);
                // Stamp the pointer fields once — they never change across calls, only size and pos
                // are mutated per-call (size depends on how many input bytes the caller has staged
                // and how much output room they want this round).
                PTR_VH.set(inStruct, inBuf);
                PTR_VH.set(outStruct, outBuf);
                overhead = (long) inBufSize + outBufSize + DSTREAM_STRUCT_HOLDER_BYTES;
            } catch (Throwable t) {
                // If any of the allocations throws (e.g. OOM mid-arena), drop the libzstd handle
                // we just got back from ZSTD_createDStream so we don't leak the native context, then
                // drop whatever the arena managed to allocate so far. We charge the accounting counter
                // only on the success path below, so a failed construction here leaves
                // NATIVE_BYTES_IN_USE untouched — nothing to roll back.
                freeNativeHandle(handle);
                arena.close();
                throw t;
            }
            this.fixedOverhead = overhead;
            // Sample the context now (pre-window) so the reservation is non-zero from the start; the
            // window buffer is allocated lazily on the first decompress and picked up by settleAccounting.
            this.accountedContext = zstdLib.sizeofDStream(handle);
            NATIVE_BYTES_IN_USE.add(fixedOverhead + accountedContext);
        }

        /**
         * Re-sample the libzstd context size once, after the first {@link #decompress} call has parsed
         * the frame header and allocated the back-reference window, and fold the delta into
         * {@link #NATIVE_BYTES_IN_USE}. Runs at most once per stream (guarded by
         * {@link #accountingSettled}); subsequent calls are no-ops because the window size is fixed for
         * the frame and libzstd reuses the same context across concatenated frames.
         */
        private void settleAccounting() {
            if (accountingSettled) {
                return;
            }
            accountingSettled = true;
            long current = zstdLib.sizeofDStream(handle);
            if (current != accountedContext) {
                NATIVE_BYTES_IN_USE.add(current - accountedContext);
                accountedContext = current;
            }
        }

        /**
         * Feed {@code src[srcPos..srcLen)} into the decoder and write decompressed bytes into
         * {@code dst[dstPos..dstLen)}. Returns the libzstd hint: {@code 0} means the current frame
         * finished decoding (the decoder is implicitly reset for the next concatenated frame, if any),
         * a positive return is libzstd's best guess for the amount of additional input it would like
         * to see next, and a zstd-side error is translated to {@link IllegalArgumentException}.
         *
         * <p>After return, the caller reads {@link #lastSrcPos()} / {@link #lastDstPos()} to learn
         * how far the input/output cursors advanced — these are absolute offsets within the supplied
         * arrays, not deltas.
         *
         * <p><b>Chunking contract.</b> Callers must pass at most {@link Zstd#dStreamInSize()} bytes
         * of input per call ({@code srcLen - srcPos <= dStreamInSize()}). Larger slices raise
         * {@link IllegalArgumentException} from the binding — the binding hard-fails rather than
         * silently consuming a prefix because partial consumption is easy to overlook at call sites.
         * {@code PanamaZstdInputStream} caches the recommended size in a {@code static final} and
         * refills upstream in chunks of that size. Output may be any size; the binding internally
         * caps each call at {@link Zstd#dStreamOutSize()} and the caller's outer loop drives
         * subsequent calls as needed.
         */
        public long decompress(byte[] dst, int dstPos, int dstLen, byte[] src, int srcPos, int srcLen) {
            if (closed) {
                throw new IllegalStateException("DStream is closed");
            }
            Objects.requireNonNull(dst, "Null destination buffer");
            Objects.requireNonNull(src, "Null source buffer");
            Objects.checkFromToIndex(dstPos, dstLen, dst.length);
            Objects.checkFromToIndex(srcPos, srcLen, src.length);
            int srcAvail = srcLen - srcPos;
            int dstAvail = dstLen - dstPos;
            if (srcAvail > inBufSize) {
                throw new IllegalArgumentException(
                    "Input slice [" + srcAvail + "B] exceeds the native staging buffer size [" + inBufSize + "B]"
                );
            }
            // Cap the output window to the native staging buffer size — the caller's outer read
            // loop will keep calling us if they wanted more than one buffer worth. Capping here
            // means we never overrun outBuf on the libzstd-side write.
            int outRoom = Math.min(dstAvail, outBufSize);

            // Copy caller's input slice into the native staging buffer at offset 0; libzstd reads
            // from inBuf[0..srcAvail) on this call. We always feed from offset 0 (rather than
            // tracking partial consumption inside the staging buffer) because the wrapper above
            // re-supplies the leftover bytes on the next call.
            if (srcAvail > 0) {
                MemorySegment.copy(src, srcPos, inBuf, JAVA_BYTE, 0L, srcAvail);
            }
            SIZE_VH.set(inStruct, (long) srcAvail);
            POS_VH.set(inStruct, 0L);
            SIZE_VH.set(outStruct, (long) outRoom);
            POS_VH.set(outStruct, 0L);

            long hint = zstdLib.decompressStream(handle, outStruct, inStruct);
            if (zstdLib.isError(hint)) {
                throw new IllegalArgumentException(zstdLib.getErrorName(hint));
            }

            int srcConsumed = (int) (long) POS_VH.get(inStruct);
            int dstProduced = (int) (long) POS_VH.get(outStruct);
            // libzstd guarantees pos ≤ size on return — the size fields we stamped above are the
            // upper bounds here, both already int-typed and bounded by the staging buffer sizes.
            assert srcConsumed >= 0 && srcConsumed <= srcAvail : "srcConsumed " + srcConsumed + " out of [0, " + srcAvail + "]";
            assert dstProduced >= 0 && dstProduced <= outRoom : "dstProduced " + dstProduced + " out of [0, " + outRoom + "]";
            if (dstProduced > 0) {
                MemorySegment.copy(outBuf, JAVA_BYTE, 0L, dst, dstPos, dstProduced);
            }
            // Translate native-staging positions back into absolute caller-array offsets — keeps
            // the SPI contract identical to zstd-jni's "positions are absolute in your byte[]".
            this.lastSrcPosAbsolute = srcPos + srcConsumed;
            this.lastDstPosAbsolute = dstPos + dstProduced;
            // The first successful call parses the frame header and allocates the window; fold the now-
            // real context size into the accounting counter exactly once.
            settleAccounting();
            return hint;
        }

        /**
         * Output position after the most recent {@link #decompress} — an absolute index into the
         * {@code dst} array that was passed to that call.
         */
        public int lastDstPos() {
            return lastDstPosAbsolute;
        }

        /**
         * Input position after the most recent {@link #decompress} — an absolute index into the
         * {@code src} array that was passed to that call.
         */
        public int lastSrcPos() {
            return lastSrcPosAbsolute;
        }

        /**
         * Native footprint of this stream in bytes: the two off-heap staging buffers, the two
         * {@code ZSTD_inBuffer}/{@code ZSTD_outBuffer} struct holders (24 bytes each), plus the real
         * libzstd {@code ZSTD_DStream} context size reported by {@code ZSTD_sizeof_DStream}. This is a
         * visibility/accounting figure meant for a circuit breaker to make the reservation observable.
         *
         * <p>Not constant: before the first {@link #decompress} call it reflects the pre-window context
         * (small); after the first call parses the frame header and allocates the back-reference window,
         * it reflects the steady-state size. Callers that charge a breaker at construction should
         * re-read this once after the first read to pick up the window (see {@code PanamaZstdInputStream}).
         */
        public long accountedBytes() {
            return fixedOverhead + accountedContext;
        }

        /** Idempotent — frees the native {@code ZSTD_DStream} and the cached struct holders. */
        @Override
        public void close() {
            if (closed == false) {
                closed = true;
                try {
                    freeNativeHandle(handle);
                } finally {
                    // Uncharge symmetrically with the native free: whether or not freeNativeHandle
                    // throws, the arena is released and the accounting reservation is dropped exactly
                    // once (guarded by the closed flag above).
                    arena.close();
                    NATIVE_BYTES_IN_USE.add(-(fixedOverhead + accountedContext));
                }
            }
        }

        private void freeNativeHandle(MemorySegment h) {
            long ret = zstdLib.freeDStream(h);
            assert ret == 0 : "ZSTD_freeDStream returned " + ret;
        }
    }
}
