/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.nativeaccess.jdk;

import org.elasticsearch.nativeaccess.CloseableByteBuffer;
import org.elasticsearch.nativeaccess.lib.LoaderHelper;
import org.elasticsearch.nativeaccess.lib.ZstdLibrary;

import java.lang.foreign.Arena;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.MemoryLayout;
import java.lang.foreign.MemoryLayout.PathElement;
import java.lang.foreign.MemorySegment;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.VarHandle;
import java.nio.ByteBuffer;

import static java.lang.foreign.ValueLayout.ADDRESS;
import static java.lang.foreign.ValueLayout.JAVA_BOOLEAN;
import static java.lang.foreign.ValueLayout.JAVA_BYTE;
import static java.lang.foreign.ValueLayout.JAVA_INT;
import static java.lang.foreign.ValueLayout.JAVA_LONG;
import static org.elasticsearch.nativeaccess.jdk.LinkerHelper.downcallHandle;

class JdkZstdLibrary implements ZstdLibrary {

    static {
        LoaderHelper.loadLibrary("zstd");
    }

    // The size_t parameters below are bound as JAVA_LONG because libzstd's one-shot entry points
    // take/return C `size_t` (8 bytes on every 64-bit target we ship). Binding them as JAVA_INT
    // (as they originally were, until #150130) leaves the upper 32 bits of the argument register
    // undefined, which libzstd reads as part of the size_t: most calls see zeroed high bits and
    // succeed, but under some JIT/register states (observed intermittently on JDK 26) the garbage
    // high bits make libzstd see a bogus size and fail the page compress/decompress.
    // See #150015, #150019 and #150077.
    private static final MethodHandle compressBound$mh = downcallHandle("ZSTD_compressBound", FunctionDescriptor.of(JAVA_LONG, JAVA_LONG));
    private static final MethodHandle compress$mh = downcallHandle(
        "ZSTD_compress",
        FunctionDescriptor.of(JAVA_LONG, ADDRESS, JAVA_LONG, ADDRESS, JAVA_LONG, JAVA_INT)
    );
    private static final MethodHandle isError$mh = downcallHandle("ZSTD_isError", FunctionDescriptor.of(JAVA_BOOLEAN, JAVA_LONG));
    private static final MethodHandle getErrorName$mh = downcallHandle("ZSTD_getErrorName", FunctionDescriptor.of(ADDRESS, JAVA_LONG));
    private static final MethodHandle decompress$mh = downcallHandle(
        "ZSTD_decompress",
        FunctionDescriptor.of(JAVA_LONG, ADDRESS, JAVA_LONG, ADDRESS, JAVA_LONG)
    );

    // Heap-array overloads bound with critical() so they accept heap MemorySegments directly —
    // the JDK-8318645 restriction does not apply because these are flat downcalls (no embedded
    // struct holding an ADDRESS field). Equivalent to zstd-jni's GetPrimitiveArrayCritical path
    // but without G1 region pinning: critical(true) tells the JVM the call won't safepoint, so
    // the heap segments stay addressable for the duration of the downcall without pinning.
    // Same C entry points as the off-heap handles above; only the linker option differs.
    private static final MethodHandle decompressHeap$mh = downcallHandle(
        "ZSTD_decompress",
        FunctionDescriptor.of(JAVA_LONG, ADDRESS, JAVA_LONG, ADDRESS, JAVA_LONG),
        LinkerHelperUtil.critical()
    );
    private static final MethodHandle compressHeap$mh = downcallHandle(
        "ZSTD_compress",
        FunctionDescriptor.of(JAVA_LONG, ADDRESS, JAVA_LONG, ADDRESS, JAVA_LONG, JAVA_INT),
        LinkerHelperUtil.critical()
    );

    // Linker.Option.critical(true) only exists from JDK 22 onward; on JDK 21 the heap handles above
    // are plain downcalls that reject heap MemorySegments ("Heap segment not allowed"). When heap
    // access is unavailable we route the heap byte[] overloads through an off-heap staging copy
    // instead of handing the heap segment straight in.
    private static final boolean HEAP_ACCESS_AVAILABLE = LinkerHelperUtil.heapAccessAvailable();

    // --- streaming API ---
    private static final MethodHandle createDStream$mh = downcallHandle("ZSTD_createDStream", FunctionDescriptor.of(ADDRESS));
    private static final MethodHandle freeDStream$mh = downcallHandle("ZSTD_freeDStream", FunctionDescriptor.of(JAVA_LONG, ADDRESS));
    private static final MethodHandle dStreamInSize$mh = downcallHandle("ZSTD_DStreamInSize", FunctionDescriptor.of(JAVA_LONG));
    private static final MethodHandle dStreamOutSize$mh = downcallHandle("ZSTD_DStreamOutSize", FunctionDescriptor.of(JAVA_LONG));
    // We cannot mark this critical: the struct holders are off-heap (confined arena), but the
    // ZSTD_inBuffer/outBuffer `ptr` fields must contain real native addresses. Panama explicitly
    // forbids storing a heap MemorySegment into an off-heap struct's ADDRESS field and then passing
    // that struct to a downcall (JDK-8318645). So our staging buffers are off-heap too — we pay one
    // memcpy per refill on input and one per produced chunk on output, dominated 100x over by the
    // libzstd decompression cost itself, while staying within a safe Panama pattern.
    private static final MethodHandle decompressStream$mh = downcallHandle(
        "ZSTD_decompressStream",
        FunctionDescriptor.of(JAVA_LONG, ADDRESS, ADDRESS, ADDRESS)
    );

    // ZSTD_inBuffer / ZSTD_outBuffer share the same C layout `{ void* ptr; size_t size; size_t pos; }`
    // (libzstd 1.5.7). `size_t` is bound as JAVA_LONG (8 bytes) — correct on all 64-bit targets we ship,
    // including Windows where size_t is `unsigned long long`.
    private static final MemoryLayout BUFFER_LAYOUT = MemoryLayout.structLayout(
        ADDRESS.withName("ptr"),
        JAVA_LONG.withName("size"),
        JAVA_LONG.withName("pos")
    );
    private static final VarHandle PTR_VH = BUFFER_LAYOUT.varHandle(PathElement.groupElement("ptr"));
    private static final VarHandle SIZE_VH = BUFFER_LAYOUT.varHandle(PathElement.groupElement("size"));
    private static final VarHandle POS_VH = BUFFER_LAYOUT.varHandle(PathElement.groupElement("pos"));
    // The FFM API changed the coordinate shape for struct-field VarHandles across supported JDKs.
    private static final boolean BUFFER_FIELD_HAS_OFFSET_COORDINATE = PTR_VH.coordinateTypes().size() == 2;

    @Override
    public long compressBound(int srcLen) {
        try {
            return (long) compressBound$mh.invokeExact((long) srcLen);
        } catch (Throwable t) {
            throw new AssertionError(t);
        }
    }

    private static void setBufferField(VarHandle handle, MemorySegment segment, MemorySegment value) {
        if (BUFFER_FIELD_HAS_OFFSET_COORDINATE) {
            handle.set(segment, 0L, value);
        } else {
            handle.set(segment, value);
        }
    }

    private static void setBufferField(VarHandle handle, MemorySegment segment, long value) {
        if (BUFFER_FIELD_HAS_OFFSET_COORDINATE) {
            handle.set(segment, 0L, value);
        } else {
            handle.set(segment, value);
        }
    }

    private static long getLongBufferField(VarHandle handle, MemorySegment segment) {
        if (BUFFER_FIELD_HAS_OFFSET_COORDINATE) {
            return (long) handle.get(segment, 0L);
        } else {
            return (long) handle.get(segment);
        }
    }

    @Override
    public long compress(CloseableByteBuffer dst, CloseableByteBuffer src, int compressionLevel) {
        assert dst instanceof JdkCloseableByteBuffer;
        assert src instanceof JdkCloseableByteBuffer;
        var nativeDst = (JdkCloseableByteBuffer) dst;
        var nativeSrc = (JdkCloseableByteBuffer) src;
        var dstSize = dst.buffer().remaining();
        var srcSize = src.buffer().remaining();
        var segmentDst = nativeDst.segment.asSlice(dst.buffer().position(), dstSize);
        var segmentSrc = nativeSrc.segment.asSlice(src.buffer().position(), srcSize);
        try {
            return (long) compress$mh.invokeExact(segmentDst, (long) dstSize, segmentSrc, (long) srcSize, compressionLevel);
        } catch (Throwable t) {
            throw new AssertionError(t);
        }
    }

    @Override
    public boolean isError(long code) {
        try {
            return (boolean) isError$mh.invokeExact(code);
        } catch (Throwable t) {
            throw new AssertionError(t);
        }
    }

    @Override
    public String getErrorName(long code) {
        try {
            MemorySegment str = (MemorySegment) getErrorName$mh.invokeExact(code);
            return MemorySegmentUtil.getString(str.reinterpret(Long.MAX_VALUE), 0);
        } catch (Throwable t) {
            throw new AssertionError(t);
        }
    }

    @Override
    public long decompress(CloseableByteBuffer dst, CloseableByteBuffer src) {
        assert dst instanceof JdkCloseableByteBuffer;
        assert src instanceof JdkCloseableByteBuffer;
        var nativeDst = (JdkCloseableByteBuffer) dst;
        var nativeSrc = (JdkCloseableByteBuffer) src;
        var dstSize = dst.buffer().remaining();
        var srcSize = src.buffer().remaining();
        var segmentDst = nativeDst.segment.asSlice(dst.buffer().position(), dstSize);
        var segmentSrc = nativeSrc.segment.asSlice(src.buffer().position(), srcSize);
        try {
            return (long) decompress$mh.invokeExact(segmentDst, (long) dstSize, segmentSrc, (long) srcSize);
        } catch (Throwable t) {
            throw new AssertionError(t);
        }
    }

    @Override
    public long decompress(CloseableByteBuffer dst, ByteBuffer src) {
        assert dst instanceof JdkCloseableByteBuffer;
        assert src.isDirect();
        var nativeDst = (JdkCloseableByteBuffer) dst;
        var dstSize = dst.buffer().remaining();
        var srcSize = src.remaining();
        var segmentDst = nativeDst.segment.asSlice(dst.buffer().position(), dstSize);
        var segmentSrc = MemorySegment.ofBuffer(src);
        try {
            return (long) decompress$mh.invokeExact(segmentDst, (long) dstSize, segmentSrc, (long) srcSize);
        } catch (Throwable t) {
            throw new AssertionError(t);
        }
    }

    @Override
    public long decompress(ByteBuffer dst, int dstOffset, int dstSize, ByteBuffer src, int srcOffset, int srcSize) {
        assert dst.isDirect();
        assert src.isDirect();
        // Use absolute addressing: MemorySegment.ofBuffer(buf) covers [position, limit), so
        // duplicate().clear() yields a non-mutating view over [0, capacity) on which the
        // explicit (offset, size) slice resolves to absolute byte positions in the buffer.
        var segmentDst = MemorySegment.ofBuffer(dst.duplicate().clear()).asSlice(dstOffset, dstSize);
        var segmentSrc = MemorySegment.ofBuffer(src.duplicate().clear()).asSlice(srcOffset, srcSize);
        try {
            return (long) decompress$mh.invokeExact(segmentDst, (long) dstSize, segmentSrc, (long) srcSize);
        } catch (Throwable t) {
            throw new AssertionError(t);
        }
    }

    @Override
    public long decompress(byte[] dst, int dstOffset, int dstSize, byte[] src, int srcOffset, int srcSize) {
        if (HEAP_ACCESS_AVAILABLE == false) {
            return runViaStaging(
                dst,
                dstOffset,
                dstSize,
                src,
                srcOffset,
                srcSize,
                (segmentDst, segmentSrc) -> (long) decompress$mh.invokeExact(segmentDst, (long) dstSize, segmentSrc, (long) srcSize)
            );
        }
        // Heap MemorySegments — bounds checked in the Zstd facade. The critical() linker option on
        // decompressHeap$mh tells Panama to pass these heap addresses through without copying,
        // matching the zero-extra-copy behavior the original Phase-1 plan wanted but couldn't have
        // for the streaming path (JDK-8318645). Flat downcall, no embedded struct, so it's safe.
        var segmentDst = MemorySegment.ofArray(dst).asSlice(dstOffset, dstSize);
        var segmentSrc = MemorySegment.ofArray(src).asSlice(srcOffset, srcSize);
        try {
            return (long) decompressHeap$mh.invokeExact(segmentDst, (long) dstSize, segmentSrc, (long) srcSize);
        } catch (Throwable t) {
            throw new AssertionError(t);
        }
    }

    @Override
    public long compress(byte[] dst, int dstOffset, int dstSize, byte[] src, int srcOffset, int srcSize, int level) {
        if (HEAP_ACCESS_AVAILABLE == false) {
            return runViaStaging(
                dst,
                dstOffset,
                dstSize,
                src,
                srcOffset,
                srcSize,
                (segmentDst, segmentSrc) -> (long) compress$mh.invokeExact(segmentDst, (long) dstSize, segmentSrc, (long) srcSize, level)
            );
        }
        var segmentDst = MemorySegment.ofArray(dst).asSlice(dstOffset, dstSize);
        var segmentSrc = MemorySegment.ofArray(src).asSlice(srcOffset, srcSize);
        try {
            return (long) compressHeap$mh.invokeExact(segmentDst, (long) dstSize, segmentSrc, (long) srcSize, level);
        } catch (Throwable t) {
            throw new AssertionError(t);
        }
    }

    /**
     * JDK 21 fallback for the heap {@code byte[]} {@code compress} / {@code decompress} overloads: stage the heap input
     * into a confined off-heap arena, run the supplied off-heap downcall (which accepts native segments without the
     * {@code critical} option), then copy the produced bytes back into the caller's array. The off-heap segments alias
     * the same C entry points as the heap handles, so the libzstd return value (length or error code) is identical.
     */
    private static long runViaStaging(
        byte[] dst,
        int dstOffset,
        int dstSize,
        byte[] src,
        int srcOffset,
        int srcSize,
        StagedDowncall downcall
    ) {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment segmentSrc = arena.allocate(srcSize);
            MemorySegment.copy(src, srcOffset, segmentSrc, JAVA_BYTE, 0L, srcSize);
            MemorySegment segmentDst = arena.allocate(dstSize);
            long ret;
            try {
                ret = downcall.invoke(segmentDst, segmentSrc);
            } catch (Throwable t) {
                throw new AssertionError(t);
            }
            if (ret >= 0 && ret <= dstSize) {
                MemorySegment.copy(segmentDst, JAVA_BYTE, 0L, dst, dstOffset, (int) ret);
            }
            return ret;
        }
    }

    /**
     * Functional interface for off-heap zstd downcalls invoked from {@link #runViaStaging}. Captures the size/level
     * arguments in the lambda so callers can adapt either the (dst, dstSize, src, srcSize) decompress signature or the
     * (dst, dstSize, src, srcSize, level) compress signature to a uniform two-segment shape.
     */
    @FunctionalInterface
    private interface StagedDowncall {
        long invoke(MemorySegment dst, MemorySegment src) throws Throwable;
    }

    @Override
    public long dStreamInSize() {
        try {
            return (long) dStreamInSize$mh.invokeExact();
        } catch (Throwable t) {
            throw new AssertionError(t);
        }
    }

    @Override
    public long dStreamOutSize() {
        try {
            return (long) dStreamOutSize$mh.invokeExact();
        } catch (Throwable t) {
            throw new AssertionError(t);
        }
    }

    @Override
    public ZstdLibrary.DStream createDStream() {
        long inSize = dStreamInSize();
        long outSize = dStreamOutSize();
        if (inSize <= 0 || inSize > Integer.MAX_VALUE || outSize <= 0 || outSize > Integer.MAX_VALUE) {
            throw new IllegalStateException("libzstd reported unreasonable stream buffer sizes: in=" + inSize + ", out=" + outSize);
        }
        MemorySegment handle;
        try {
            handle = (MemorySegment) createDStream$mh.invokeExact();
        } catch (Throwable t) {
            throw new AssertionError(t);
        }
        if (handle == null || handle.equals(MemorySegment.NULL)) {
            throw new IllegalStateException("ZSTD_createDStream returned NULL");
        }
        return new JdkDStream(handle, (int) inSize, (int) outSize);
    }

    /**
     * Stateful binding for {@code ZSTD_decompressStream}. Owns the opaque {@code ZSTD_DStream*}
     * handle plus a single confined arena holding four persistent off-heap allocations: two 24-byte
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
    private static final class JdkDStream implements ZstdLibrary.DStream {
        private final Arena arena = Arena.ofShared();
        private final MemorySegment handle;
        private final MemorySegment inStruct;
        private final MemorySegment outStruct;
        private final MemorySegment inBuf;
        private final MemorySegment outBuf;
        private final int inBufSize;
        private final int outBufSize;
        private boolean closed = false;

        JdkDStream(MemorySegment handle, int inBufSize, int outBufSize) {
            this.handle = handle;
            this.inBufSize = inBufSize;
            this.outBufSize = outBufSize;
            try {
                this.inStruct = arena.allocate(BUFFER_LAYOUT);
                this.outStruct = arena.allocate(BUFFER_LAYOUT);
                this.inBuf = arena.allocate(inBufSize);
                this.outBuf = arena.allocate(outBufSize);
                // Stamp the pointer fields once — they never change across calls, only size and pos
                // are mutated per-call (size depends on how many input bytes the caller has staged
                // and how much output room they want this round).
                setBufferField(PTR_VH, inStruct, inBuf);
                setBufferField(PTR_VH, outStruct, outBuf);
            } catch (Throwable t) {
                // If any of the allocations throws (e.g. OOM mid-arena), drop the libzstd handle
                // we just got back from ZSTD_createDStream so we don't leak the ~256 KB native
                // context, then drop whatever the arena managed to allocate so far.
                freeNativeHandle(handle);
                arena.close();
                throw t;
            }
        }

        @Override
        public long decompress(byte[] dst, int dstPos, int dstLen, byte[] src, int srcPos, int srcLen) {
            assert closed == false : "DStream already closed";
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
            setBufferField(SIZE_VH, inStruct, (long) srcAvail);
            setBufferField(POS_VH, inStruct, 0L);
            setBufferField(SIZE_VH, outStruct, (long) outRoom);
            setBufferField(POS_VH, outStruct, 0L);

            long hint;
            try {
                hint = (long) decompressStream$mh.invokeExact(handle, outStruct, inStruct);
            } catch (Throwable t) {
                throw new AssertionError(t);
            }

            int srcConsumed = (int) getLongBufferField(POS_VH, inStruct);
            int dstProduced = (int) getLongBufferField(POS_VH, outStruct);
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
            return hint;
        }

        // Cache the last absolute positions so the SPI exposes them via lastSrcPos / lastDstPos
        // without re-reading the struct (which holds buffer-local offsets, not caller offsets).
        private int lastSrcPosAbsolute = 0;
        private int lastDstPosAbsolute = 0;

        @Override
        public int lastDstPos() {
            return lastDstPosAbsolute;
        }

        @Override
        public int lastSrcPos() {
            return lastSrcPosAbsolute;
        }

        @Override
        public void close() {
            if (closed) {
                return;
            }
            closed = true;
            try {
                freeNativeHandle(handle);
            } finally {
                arena.close();
            }
        }

        private static void freeNativeHandle(MemorySegment handle) {
            try {
                long ret = (long) freeDStream$mh.invokeExact(handle);
                assert ret == 0 : "ZSTD_freeDStream returned " + ret;
            } catch (Throwable t) {
                throw new AssertionError(t);
            }
        }
    }
}
