/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.nativeaccess;

import java.io.Closeable;
import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;

/**
 * A test utility that allocates native memory segments where the usable data
 * ends exactly at an OS page boundary. Any native read past the allocated size
 * will access an unmapped page and cause a SIGBUS/SIGSEGV, exposing buffer
 * over-reads in native code.
 *
 * <p>Implementation: creates temporary files whose size is a multiple of the OS
 * page size, maps them via {@link FileChannel#map}, then returns a slice
 * positioned so the usable bytes end at the mapping boundary. Because the
 * mapping covers exactly {@code N * pageSize} bytes, the virtual page
 * immediately after the mapping is not backed by any allocation — accessing it
 * delivers a fatal signal.
 *
 * <p>This mechanism is portable across Linux and macOS, on both aarch64 and
 * amd64 architectures.
 *
 * <p>Usage:
 * <pre>{@code
 * try (var allocator = new GuardPageAllocator(tempDir)) {
 *     byte[] vectorData = ...;
 *     MemorySegment seg = allocator.allocateAtPageEnd(vectorData);
 *     // seg contains vectorData right-aligned; byte after end is unmapped
 *     nativeFunction(seg, vectorData.length, ...);
 * }
 * }</pre>
 */
public class GuardPageAllocator implements Closeable {

    private final Path tempDir;
    private final int pageSize;
    private final List<MappedByteBuffer> mappedBuffers = new ArrayList<>();
    private final List<FileChannel> openChannels = new ArrayList<>();
    private int fileCounter;

    /**
     * Creates a new allocator that stores temporary backing files in {@code tempDir}.
     *
     * @param tempDir directory for temporary files (caller is responsible for cleanup)
     * @param pageSize the OS page size in bytes
     */
    public GuardPageAllocator(Path tempDir, int pageSize) {
        this.tempDir = tempDir;
        this.pageSize = pageSize;
    }

    /** Returns the OS page size this allocator was configured with. */
    public int pageSize() {
        return pageSize;
    }

    /**
     * Allocates a writable segment of {@code size} bytes, right-aligned so that
     * the last usable byte is the final byte of a mapped page. The virtual
     * address immediately after the returned segment is on an unmapped page.
     *
     * @param size number of usable bytes (must be positive and &le; pageSize * large-number)
     * @return a writable {@link MemorySegment} of exactly {@code size} bytes
     */
    public MemorySegment allocateAtPageEnd(int size) throws IOException {
        if (size <= 0) {
            throw new IllegalArgumentException("size must be positive: " + size);
        }
        int pages = (size + pageSize - 1) / pageSize;
        long mappedSize = (long) pages * pageSize;

        Path file = tempDir.resolve("guard_page_" + (fileCounter++) + ".bin");
        // Create a file of exactly mappedSize bytes
        try (FileChannel fc = FileChannel.open(file, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)) {
            fc.write(java.nio.ByteBuffer.allocate(1), mappedSize - 1);
            assert fc.size() == mappedSize : "expected file size " + mappedSize + " but got " + fc.size();
        }

        // Map the file READ_WRITE; keep the channel open to keep the mapping alive
        FileChannel fc = FileChannel.open(file, StandardOpenOption.READ, StandardOpenOption.WRITE);
        openChannels.add(fc);
        MappedByteBuffer mbb = fc.map(FileChannel.MapMode.READ_WRITE, 0, mappedSize);
        mappedBuffers.add(mbb);

        // Wrap the MappedByteBuffer as a MemorySegment (native, off-heap)
        MemorySegment mapped = MemorySegment.ofBuffer(mbb);
        // Zero the entire region
        mapped.fill((byte) 0);

        // Return a slice of exactly 'size' bytes at the end of the mapping
        return mapped.asSlice(mappedSize - size, size);
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
        // Close file channels; this does not unmap the buffers (they remain valid
        // until GC), but releasing them is good hygiene.
        for (FileChannel fc : openChannels) {
            try {
                fc.close();
            } catch (IOException e) {
                // best-effort
            }
        }
        openChannels.clear();
        mappedBuffers.clear();
        // Temp files are left for the test framework's temp dir cleanup
    }
}
