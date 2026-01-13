/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.nativeaccess.jdk;

import org.elasticsearch.nativeaccess.CloseableMappedByteBuffer;
import org.elasticsearch.nativeaccess.lib.NativeLibraryProvider;
import org.elasticsearch.nativeaccess.lib.PosixCLibrary;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.util.Objects;

class JdkCloseableMappedByteBuffer implements CloseableMappedByteBuffer {

    private static final PosixCLibrary LIB = NativeLibraryProvider.instance().getLibrary(PosixCLibrary.class);
    private static final int PAGE_SIZE = LIB.getPageSize();

    private final Arena arena;
    private final MemorySegment segment;
    private final MappedByteBuffer bufferView;

    static JdkCloseableMappedByteBuffer ofShared(FileChannel fileChannel, MapMode mode, long position, long size) throws IOException {
        var arena = Arena.ofShared();
        var seg = fileChannel.map(mode, position, size, arena);
        return new JdkCloseableMappedByteBuffer(seg, arena);
    }

    static JdkCloseableMappedByteBuffer ofAuto(FileChannel fileChannel, MapMode mode, long position, long size) throws IOException {
        var seg = fileChannel.map(mode, position, size, Arena.ofAuto());
        return new JdkCloseableMappedByteBuffer(seg, null);
    }

    private JdkCloseableMappedByteBuffer(MemorySegment seg, Arena arena) {
        this.arena = arena;
        this.segment = seg;
        this.bufferView = (MappedByteBuffer) seg.asByteBuffer();
    }

    @Override
    public MappedByteBuffer buffer() {
        return bufferView;
    }

    @Override
    public void close() {
        if (arena != null) {
            arena.close();
        }
    }

    @Override
    public CloseableMappedByteBuffer slice(long index, long length) {
        var slice = segment.asSlice(index, length);
        return new JdkCloseableMappedByteBuffer(slice, null); // closing a slice does not close the parent.
    }

    @Override
    public void prefetch(long offset, long length) {
        Objects.checkFromIndexSize(offset, length, segment.byteSize());
        // Align offset with the page size, this is required for madvise.
        // Compute the offset of the current position in the OS's page.
        final long offsetInPage = (segment.address() + offset) % PAGE_SIZE;
        offset -= offsetInPage;
        length += offsetInPage;
        if (offset < 0) {
            // start of the page is before the start of this segment, ignore the first page.
            offset += PAGE_SIZE;
            length -= PAGE_SIZE;
            if (length <= 0) {
                // This segment has no data beyond the first page.
                return;
            }
        }
        LIB.madvise(segment, offset, length, PosixCLibrary.POSIX_MADV_WILLNEED);
    }
}
