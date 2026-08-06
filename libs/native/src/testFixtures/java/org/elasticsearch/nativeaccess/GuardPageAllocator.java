/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.nativeaccess;

import org.elasticsearch.foreign.Platform;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

/**
 * A test utility that hands out memory segments backed by an inaccessible <i>guard page</i>: the data is
 * placed flush against the end of a readable region, and the page immediately after it is reserved and
 * marked inaccessible. A native read of even a single byte past the data therefore faults,
 * which turns a silent buffer over-read into a hard crash.
 *
 * <p>This makes the enforcement deterministic, rather than dependent on address-space layout.
 *
 * <p>Implements {@link Arena} so it can be used as a drop-in replacement for {@code Arena.ofConfined()} or
 * {@code Arena.ofShared()} in existing tests. All segments handed out are backed by a guard page, and all
 * of them are released when this arena is closed.
 *
 * <p>This allocator exists to catch "overflows" (over-reads or over-writes), so the data is right-aligned
 * against the guard page. The start address of the returned segment is therefore page-unaligned by design;
 * that is safe <i>only</i> if the kernels/native functions use unaligned loads.
 *
 * <p>The platform-specific reservation and protection primitives are OS-dependent; see
 * {@link #reserve} and {@link #denyAccess}. We currently have one implementation, {@link PosixGuardPageAllocator},
 * which uses {@code mmap}/{@code mprotect}/{@code munmap}.
 *
 * <p>Usage:
 * <pre>{@code
 * assumeTrue("guard pages unsupported on this platform", GuardPageAllocator.isSupported());
 * try (var arena = GuardPageAllocator.ofConfined()) {
 *     byte[] vectorData = ...;
 *     MemorySegment seg = arena.allocateAtPageEnd(vectorData);
 *     // seg holds vectorData; reading seg.byteSize() + 1 bytes from native code crashes the JVM
 *     nativeFunction(seg, vectorData.length, ...);
 * }
 * }</pre>
 */
public abstract class GuardPageAllocator implements Arena {

    private final Arena delegate;

    protected GuardPageAllocator(Arena delegate) {
        this.delegate = delegate;
    }

    /**
     * Whether guard pages can be created on the current platform.
     */
    public static boolean isSupported() {
        return PosixGuardPageAllocator.supportsCurrentPlatform();
    }

    /**
     * Creates a confined (single-threaded) guard page allocator. All access — including {@link #allocate},
     * {@link #allocateAtPageEnd}, and {@link #close} — must happen on the thread that created it.
     */
    public static GuardPageAllocator ofConfined() {
        return create(Arena.ofConfined());
    }

    /** Creates a shared (thread-safe) guard page allocator. May be accessed from any thread. */
    public static GuardPageAllocator ofShared() {
        return create(Arena.ofShared());
    }

    private static GuardPageAllocator create(Arena delegate) {
        // A Windows implementation over VirtualAlloc/VirtualProtect/VirtualFree would be offered here too,
        // each implementation answering for the platforms it supports.
        if (PosixGuardPageAllocator.supportsCurrentPlatform()) {
            return PosixGuardPageAllocator.create(delegate);
        }
        delegate.close();
        throw new UnsupportedOperationException("guard pages are not supported on [" + Platform.current() + "]");
    }

    /** Returns the OS page size, which is also the size of the guard page. */
    public abstract int pageSize();

    /**
     * Reserves {@code byteSize} bytes of readable and writable memory, zero filled, starting at a page
     * boundary. The reservation is released when this arena is closed.
     */
    protected abstract MemorySegment reserve(long byteSize);

    /**
     * Marks the given page inaccessible, so that a read or write of any byte within it faults.
     *
     * @param page a single page, page aligned, carved out of a region returned by {@link #reserve}
     */
    protected abstract void denyAccess(MemorySegment page);

    /** The arena that owns the reservations, and whose scope releases them. */
    protected final Arena delegate() {
        return delegate;
    }

    /**
     * Allocates a writable segment of {@code size} bytes, right-aligned so that its last byte is the last
     * accessible byte before the guard page.
     *
     * @param size number of usable bytes (must be positive)
     */
    public MemorySegment allocateAtPageEnd(long size) {
        if (size <= 0) {
            throw new IllegalArgumentException("size must be positive: " + size);
        }
        int pageSize = pageSize();
        long dataPages = (size + pageSize - 1) / pageSize;
        long dataBytes = dataPages * pageSize;

        // One page more than the data needs, marked inaccessible: reading the byte right after the data faults.
        MemorySegment region = reserve(dataBytes + pageSize);
        denyAccess(region.asSlice(dataBytes, pageSize));
        return region.asSlice(dataBytes - size, size);
    }

    /**
     * Allocates a segment holding a copy of {@code data}, right-aligned against the guard page. The
     * returned segment is exactly {@code data.length} bytes.
     */
    public MemorySegment allocateAtPageEnd(byte[] data) {
        MemorySegment segment = allocateAtPageEnd(data.length);
        MemorySegment.copy(data, 0, segment, ValueLayout.JAVA_BYTE, 0, data.length);
        return segment;
    }

    /**
     * Allocates a segment holding a copy of {@code source}, right-aligned against the guard page. The
     * returned segment is exactly {@code source.byteSize()} bytes.
     */
    public MemorySegment allocateAtPageEnd(MemorySegment source) {
        int size = Math.toIntExact(source.byteSize());
        MemorySegment segment = allocateAtPageEnd(size);
        MemorySegment.copy(source, 0, segment, 0, size);
        return segment;
    }

    /**
     * Allocates a writable segment of {@code byteSize} bytes, right-aligned against a guard page, so that
     * every segment obtained from this arena is over-read protected.
     *
     * <p>A zero-size request delegates to the internal arena: an empty segment cannot be over-read through.
     */
    @Override
    public MemorySegment allocate(long byteSize, long byteAlignment) {
        if (byteSize == 0) {
            return delegate.allocate(byteSize, byteAlignment);
        }
        if (byteSize < 0) {
            throw new IllegalArgumentException("size must be non-negative: " + byteSize);
        }
        // The data is right-aligned against the guard page, so its start address is (guardPageStart - size).
        // That start is correctly aligned only if size is itself a multiple of the requested alignment.
        assert byteSize % byteAlignment == 0
            : "size " + byteSize + " is not a multiple of alignment " + byteAlignment + "; the allocation would be misaligned";
        return allocateAtPageEnd(Math.toIntExact(byteSize));
    }

    @Override
    public MemorySegment.Scope scope() {
        return delegate.scope();
    }

    @Override
    public void close() {
        delegate.close();
    }
}
