/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.simdvec.internal;

import org.apache.lucene.store.FilterIndexInput;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.MemorySegmentAccessInput;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.core.CheckedFunction;
import org.elasticsearch.core.DirectAccessInput;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.lang.ref.Reference;
import java.nio.ByteBuffer;
import java.util.function.IntFunction;

import static org.elasticsearch.simdvec.internal.vectorization.JdkFeatures.SUPPORTS_HEAP_SEGMENTS;

/**
 * Utility for obtaining a {@link MemorySegment} view of data in an
 * {@link IndexInput} and passing it to a caller-supplied action. The
 * segment may come from a {@link MemorySegmentAccessInput} (mmap),
 * a direct {@link java.nio.ByteBuffer} view (e.g. blob-cache), or a
 * heap copy as a last resort.
 *
 * <p>All resource management (ref-counting, buffer release) is handled
 * internally — callers never see a closeable resource.
 */
public final class IndexInputUtils {

    private IndexInputUtils() {}

    /**
     * Returns {@code true} if {@code MemorySegment} slices can be obtained from the specified {@link IndexInput}.
     */
    public static boolean canUseSegmentSlices(IndexInput input) {
        return input instanceof MemorySegmentAccessInput || input instanceof DirectAccessInput;
    }

    /**
     * Obtains a memory segment for the next {@code length} bytes of the
     * index input, passes it to {@code action}, and returns the result.
     * The position of the index input is advanced by {@code length}.
     *
     * <p> This method first tries to obtain a slice via
     * {@link MemorySegmentAccessInput#segmentSliceOrNull}. If that
     * returns {@code null}, it tries a direct {@link java.nio.ByteBuffer}
     * view via {@link DirectAccessInput}. As a last resort it copies the
     * data onto the heap using a byte array obtained from
     * {@code scratchSupplier}.
     *
     * <p> The memory segment passed to {@code action} is valid only for
     * the duration of the call. Callers must not retain references to it.
     *
     * @param in              the index input positioned at the data to read
     * @param length          the number of bytes to read
     * @param scratchSupplier supplies a byte array of at least the requested
     *                        length, used only on the heap-copy fallback path
     * @param action          the function to apply to the memory segment
     * @return the result of applying {@code action}
     */
    public static <R> R withSlice(
        IndexInput in,
        long length,
        IntFunction<byte[]> scratchSupplier,
        CheckedFunction<MemorySegment, R, IOException> action
    ) throws IOException {
        checkInputType(in);
        if (in instanceof MemorySegmentAccessInput msai) {
            long offset = in.getFilePointer();
            MemorySegment slice = msai.segmentSliceOrNull(offset, length);
            if (slice != null) {
                in.skipBytes(length);
                return action.apply(slice);
            }
        }
        if (in instanceof DirectAccessInput dai) {
            long offset = in.getFilePointer();
            @SuppressWarnings("unchecked")
            R[] result = (R[]) new Object[1];
            boolean available = dai.withByteBufferSlice(offset, length, bb -> {
                assert bb.isDirect();
                in.skipBytes(length);
                result[0] = action.apply(MemorySegment.ofBuffer(bb));
            });
            if (available) {
                return result[0];
            }
        }
        return copyAndApply(in, Math.toIntExact(length), scratchSupplier, action);
    }

    /**
     * Resolves {@code count} file ranges to native memory addresses and passes the
     * address array to the action. Tries {@link MemorySegmentAccessInput} first
     * (contiguous segment, pointer arithmetic), then {@link DirectAccessInput}
     * ({@code withByteBufferSlices}). Returns {@code false} without invoking the
     * action if neither path is available - there is no heap fallback since native
     * addresses are required.
     *
     * <p><b>Memory safety:</b> The addresses in the {@code addrs} array are raw
     * native pointers extracted via {@link MemorySegment#address()}. The native
     * code that consumes them (e.g. a bulk-gather FFI downcall) will dereference
     * these pointers directly - there is no scope or bounds check at that point.
     * The backing memory must therefore remain valid for the entire duration of
     * the {@code action}.
     *
     * <p>With the current callers, the backing memory is independently kept alive:
     * on the MSAI path, the arena is owned by the {@code IndexInput} which the
     * caller holds as a field; on the DAI path, cache regions are ref-counted by
     * {@link DirectAccessInput#withByteBufferSlices} for the duration of the
     * callback. However, that safety relies on implementation details of
     * {@code MMapDirectory} and {@code SharedBlobCacheService}. The JIT is also
     * permitted to discard local references after their last use (JLS 12.6.1),
     * which could in theory allow the segment or buffer objects to be collected
     * while the native call is in flight. The {@link Reference#reachabilityFence}
     * calls below are therefore added as low-cost defensive insurance: they make
     * the lifetime contract explicit and protect against future changes to either
     * the callers or the backing-memory implementations.
     *
     * @param in            the index input
     * @param offsets       file byte offsets for each range (caller-owned, not modified)
     * @param length        byte length of each range (same for all)
     * @param count         number of ranges to resolve
     * @param addressesBufferSupplier  called with {@code count}; must return a writable
     *                      {@link MemorySegment} with room for at least {@code count}
     *                      pointer-width entries. The segment may be larger and may be
     *                      reused across calls; only entries {@code [0, count)} are
     *                      written by this method.
     * @param action        invoked with the same segment returned by the supplier; only
     *                      the first {@code count} address slots contain valid data, and
     *                      those addresses are valid only for the duration of the call.
     * @return {@code true} if addresses were resolved and the action was invoked
     */
    public static boolean withSliceAddresses(
        IndexInput in,
        long[] offsets,
        int length,
        int count,
        IntFunction<MemorySegment> addressesBufferSupplier,
        CheckedConsumer<MemorySegment, IOException> action
    ) throws IOException {
        assert validateInputs(in, offsets, length, count);
        MemorySegment addrs = addressesBufferSupplier.apply(count);
        assert addrs.byteSize() >= (long) count * ValueLayout.ADDRESS.byteSize()
            : "address buffer too small: " + addrs.byteSize() + " < " + ((long) count * ValueLayout.ADDRESS.byteSize());
        if (in instanceof MemorySegmentAccessInput msai) {
            return resolveFromMmap(msai, offsets, length, count, addrs, action);
        }
        if (in instanceof DirectAccessInput dai) {
            return resolveFromDirectAccess(dai, offsets, length, count, addrs, action);
        }
        return false;
    }

    private static boolean resolveFromMmap(
        MemorySegmentAccessInput msai,
        long[] offsets,
        int length,
        int count,
        MemorySegment addrs,
        CheckedConsumer<MemorySegment, IOException> action
    ) throws IOException {
        for (int i = 0; i < count; i++) {
            var segment = msai.segmentSliceOrNull(offsets[i], length);
            if (segment == null) {
                return false;
            }
            assert validateNativeSegment(segment, "mmap segment");
            addrs.setAtIndex(ValueLayout.ADDRESS, i, segment);
        }
        assert validateAddresses(addrs, count);
        try {
            action.accept(addrs);
        } finally {
            // We rely on the MSAI contract that segments returned by
            // segmentSliceOrNull remain valid until the input is closed:
            // keeping msai reachable across the native call keeps the
            // backing memory alive.
            Reference.reachabilityFence(msai);
        }
        return true;
    }

    private static boolean resolveFromDirectAccess(
        DirectAccessInput dai,
        long[] offsets,
        int length,
        int count,
        MemorySegment addrs,
        CheckedConsumer<MemorySegment, IOException> action
    ) throws IOException {
        return dai.withByteBufferSlices(offsets, length, count, bbs -> {
            assert validateByteBuffers(bbs, count, length);
            for (int i = 0; i < count; i++) {
                addrs.setAtIndex(ValueLayout.ADDRESS, i, MemorySegment.ofBuffer(bbs[i]));
            }
            assert validateAddresses(addrs, count);
            try {
                action.accept(addrs);
            } finally {
                Reference.reachabilityFence(bbs);
            }
        });
    }

    private static boolean validateInputs(IndexInput in, long[] offsets, int length, int count) {
        assert count > 0 : "count must be positive, got " + count;
        assert length > 0 : "length must be positive, got " + length;
        assert offsets.length >= count : "offsets array too small: " + offsets.length + " < " + count;
        long fileLength = in.length();
        for (int i = 0; i < count; i++) {
            assert offsets[i] >= 0 : "negative offset at index " + i + ": " + offsets[i];
            assert offsets[i] + length <= fileLength
                : "offset[" + i + "]=" + offsets[i] + " + length=" + length + " exceeds file length " + fileLength;
        }
        return true;
    }

    private static boolean validateNativeSegment(MemorySegment seg, String label) {
        assert seg.isNative() : label + " is not a native (off-heap) segment";
        assert seg.scope().isAlive() : label + " scope is closed";
        assert seg.address() > 0 : label + " has non-positive address: 0x" + Long.toHexString(seg.address());
        return true;
    }

    private static boolean validateByteBuffers(ByteBuffer[] bbs, int count, int length) {
        assert bbs.length >= count : "ByteBuffer array too small: " + bbs.length + " < " + count;
        for (int i = 0; i < count; i++) {
            assert bbs[i] != null : "null ByteBuffer at index " + i;
            assert bbs[i].isDirect() : "ByteBuffer at index " + i + " is not direct (heap-backed)";
            assert bbs[i].remaining() >= length : "ByteBuffer at index " + i + " too small: " + bbs[i].remaining() + " < " + length;
        }
        return true;
    }

    private static boolean validateAddresses(MemorySegment addrs, int count) {
        for (int i = 0; i < count; i++) {
            MemorySegment addr = addrs.getAtIndex(ValueLayout.ADDRESS, i);
            assert addr.equals(MemorySegment.NULL) == false : "null address at index " + i;
            assert addr.address() > 0 : "non-positive address at index " + i + ": 0x" + Long.toHexString(addr.address());
            if (i > 0) {
                MemorySegment prev = addrs.getAtIndex(ValueLayout.ADDRESS, i - 1);
                assert addr.address() != prev.address()
                    : "duplicate address at indices " + (i - 1) + " and " + i + ": 0x" + Long.toHexString(addr.address());
            }
        }
        return true;
    }

    /**
     * Checks that a {@link FilterIndexInput} wrapper also implements
     * {@link MemorySegmentAccessInput} or {@link DirectAccessInput},
     * so that zero-copy access is preserved through the wrapper chain.
     */
    public static void checkInputType(IndexInput in) {
        if (in instanceof FilterIndexInput && (in instanceof MemorySegmentAccessInput || in instanceof DirectAccessInput) == false) {
            throw new IllegalArgumentException(
                "IndexInput is a FilterIndexInput ("
                    + in.getClass().getName()
                    + ") that does not implement MemorySegmentAccessInput or DirectAccessInput. "
                    + "Ensure the wrapper implements DirectAccessInput or is unwrapped before constructing the scorer."
            );
        }
    }

    /**
     * Reads bytes from the index input and applies the action to a memory
     * segment containing the data. On Java 22+ a heap-backed segment is
     * used directly. On Java 21, where heap segments cannot be passed to
     * native downcalls, the data is copied into a confined arena.
     */
    private static <R> R copyAndApply(
        IndexInput in,
        int bytesToRead,
        IntFunction<byte[]> scratchSupplier,
        CheckedFunction<MemorySegment, R, IOException> action
    ) throws IOException {
        byte[] buf = scratchSupplier.apply(bytesToRead);
        in.readBytes(buf, 0, bytesToRead);
        if (SUPPORTS_HEAP_SEGMENTS) {
            return action.apply(MemorySegment.ofArray(buf).asSlice(0, bytesToRead));
        }
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment nativeSegment = arena.allocate(bytesToRead);
            MemorySegment.copy(buf, 0, nativeSegment, ValueLayout.JAVA_BYTE, 0, bytesToRead);
            return action.apply(nativeSegment);
        }
    }
}
