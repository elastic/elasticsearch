/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.nativeaccess;

import org.elasticsearch.core.SuppressForbidden;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.UncheckedIOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.channels.FileChannel;

/**
 * A test utility that allocates native memory segments where the usable data
 * ends exactly at an OS page boundary. Any native read past the allocated size
 * will access an unmapped page and cause a SIGBUS/SIGSEGV, exposing buffer
 * over-reads in native code.
 *
 * <p>Implements {@link Arena} so it can be used as a drop-in replacement for
 * {@code Arena.ofConfined()} or {@code Arena.ofShared()} in existing tests. All
 * segments allocated through this arena are backed by memory-mapped files and
 * positioned so that the usable bytes end at a mapping boundary.
 *
 * <p>Implementation: for each allocation, creates a temporary file of exactly
 * {@code ceil(size/pageSize)} pages, maps it via
 * {@link FileChannel#map(FileChannel.MapMode, long, long, Arena)}, then returns
 * a slice positioned so the usable bytes end at the mapping boundary. Because
 * the mapping covers exactly the file size, the virtual page immediately after
 * is unmapped — accessing it delivers a fatal signal. The file descriptor is
 * closed immediately after mapping (POSIX guarantees the mapping remains valid),
 * so this allocator does not accumulate open file descriptors.
 *
 * <p>Files are opened via {@link RandomAccessFile} to obtain the real
 * {@code FileChannelImpl}, bypassing mock filesystem wrappers (e.g. Lucene's
 * {@code HandleTrackingFS}) that don't override the 4-argument
 * {@code FileChannel.map} method.
 *
 * <p>This mechanism is portable across Linux and macOS, on both aarch64 and
 * amd64 architectures.
 *
 * <p>Usage:
 * <pre>{@code
 * try (var arena = GuardPageAllocator.ofConfined(pageSize)) {
 *     byte[] vectorData = ...;
 *     MemorySegment seg = arena.allocateAtPageEnd(vectorData);
 *     // seg contains vectorData right-aligned; byte after end is unmapped
 *     nativeFunction(seg, vectorData.length, ...);
 * }
 * }</pre>
 */
public class GuardPageAllocator implements Arena {

    private final int pageSize;
    private final Arena delegate;
    private int fileCounter;

    private GuardPageAllocator(Arena delegate, int pageSize) {
        this.pageSize = pageSize;
        this.delegate = delegate;
    }

    /**
     * Creates a confined (single-threaded) guard-page allocator. All access — including
     * {@link #allocate}, {@link #allocateAtPageEnd}, and {@link #close} — must occur
     * from the thread that created it.
     *
     * @param pageSize the OS page size in bytes
     */
    public static GuardPageAllocator ofConfined(int pageSize) {
        return new GuardPageAllocator(Arena.ofConfined(), pageSize);
    }

    /**
     * Creates a shared (thread-safe) guard-page allocator. May be accessed from any thread.
     * Allocation methods are synchronized to protect internal mutable state.
     *
     * @param pageSize the OS page size in bytes
     */
    public static GuardPageAllocator ofShared(int pageSize) {
        return new SharedGuardPageAllocator(Arena.ofShared(), pageSize);
    }

    /** Returns the OS page size this allocator was configured with. */
    public int pageSize() {
        return pageSize;
    }

    @Override
    public MemorySegment.Scope scope() {
        return delegate.scope();
    }

    /**
     * Allocates a writable segment of {@code byteSize} bytes, right-aligned so that
     * the last usable byte is the final byte of a mapped page. The virtual
     * address immediately after the returned segment is on an unmapped page.
     *
     * <p>A zero-size request delegates to the internal arena (no guard page needed for empty segments).
     */
    @Override
    public MemorySegment allocate(long byteSize, long byteAlignment) {
        if (byteSize == 0) {
            return delegate.allocate(byteSize, byteAlignment);
        }
        if (byteSize < 0) {
            throw new IllegalArgumentException("size must be non-negative: " + byteSize);
        }
        // Data is right-aligned to the page end, so its start address is (pageAlignedEnd - size).
        // That start is only correctly aligned if size is itself a multiple of the requested alignment.
        assert byteSize % byteAlignment == 0
            : "size " + byteSize + " is not a multiple of alignment " + byteAlignment + "; guard page start would be misaligned";
        try {
            return mapAtPageEnd(Math.toIntExact(byteSize));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Allocates a writable segment of {@code size} bytes, right-aligned so that
     * the last usable byte is the final byte of a mapped page.
     *
     * @param size number of usable bytes (must be positive)
     * @return a writable {@link MemorySegment} of exactly {@code size} bytes
     */
    public MemorySegment allocateAtPageEnd(int size) throws IOException {
        if (size <= 0) {
            throw new IllegalArgumentException("size must be positive: " + size);
        }
        return mapAtPageEnd(size);
    }

    /**
     * Allocates a segment pre-filled with {@code data}, right-aligned to a page
     * boundary. The returned segment is exactly {@code data.length} bytes.
     *
     * @param data the bytes to copy into the segment
     * @return a {@link MemorySegment} containing {@code data}, positioned so that
     *         the byte after the last element is on an unmapped page
     */
    public MemorySegment allocateAtPageEnd(byte[] data) throws IOException {
        MemorySegment segment = allocateAtPageEnd(data.length);
        MemorySegment.copy(data, 0, segment, ValueLayout.JAVA_BYTE, 0, data.length);
        return segment;
    }

    /**
     * Allocates a segment pre-filled from the given source segment, right-aligned
     * to a page boundary.
     *
     * @param source the source segment to copy from
     * @return a {@link MemorySegment} containing the source data, positioned so that
     *         the byte after the last element is on an unmapped page
     */
    public MemorySegment allocateAtPageEnd(MemorySegment source) throws IOException {
        int size = Math.toIntExact(source.byteSize());
        MemorySegment segment = allocateAtPageEnd(size);
        MemorySegment.copy(source, 0, segment, 0, size);
        return segment;
    }

    @Override
    public void close() {
        delegate.close();
    }

    @SuppressForbidden(reason = "Uses RandomAccessFile to obtain a real FileChannelImpl, bypassing Lucene's HandleTrackingFS mock")
    MemorySegment mapAtPageEnd(int size) throws IOException {
        int pages = (size + pageSize - 1) / pageSize;
        long mappedSize = (long) pages * pageSize;

        // Use RandomAccessFile to get the real FileChannelImpl, bypassing Lucene's
        // HandleTrackingFS mock which doesn't override FileChannel.map(Mode,long,long,Arena)
        File file = File.createTempFile("guard_page_" + (fileCounter++), ".bin");
        file.deleteOnExit();
        MemorySegment mapped;
        try (RandomAccessFile raf = new RandomAccessFile(file, "rw")) {
            raf.setLength(mappedSize);
            FileChannel fc = raf.getChannel();
            mapped = fc.map(FileChannel.MapMode.READ_WRITE, 0, mappedSize, delegate);
            // fd is closed here; POSIX guarantees the mapping remains valid
        }

        // Zero the entire region
        mapped.fill((byte) 0);

        // Return a slice of exactly 'size' bytes at the end of the mapping
        return mapped.asSlice(mappedSize - size, size);
    }

    /**
     * Thread-safe variant that synchronizes allocation methods on an internal lock object.
     */
    private static final class SharedGuardPageAllocator extends GuardPageAllocator {

        private final Object lock = new Object();

        private SharedGuardPageAllocator(Arena delegate, int pageSize) {
            super(delegate, pageSize);
        }

        @Override
        public MemorySegment allocate(long byteSize, long byteAlignment) {
            synchronized (lock) {
                return super.allocate(byteSize, byteAlignment);
            }
        }

        @Override
        public MemorySegment allocateAtPageEnd(int size) throws IOException {
            synchronized (lock) {
                return super.allocateAtPageEnd(size);
            }
        }

        @Override
        public MemorySegment allocateAtPageEnd(byte[] data) throws IOException {
            synchronized (lock) {
                return super.allocateAtPageEnd(data);
            }
        }

        @Override
        public MemorySegment allocateAtPageEnd(MemorySegment source) throws IOException {
            synchronized (lock) {
                return super.allocateAtPageEnd(source);
            }
        }

        @Override
        public void close() {
            synchronized (lock) {
                super.close();
            }
        }
    }
}
