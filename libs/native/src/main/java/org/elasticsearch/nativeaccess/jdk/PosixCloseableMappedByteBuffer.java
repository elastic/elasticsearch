/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.nativeaccess.jdk;

import org.elasticsearch.nativeaccess.lib.NativeLibraryProvider;
import org.elasticsearch.nativeaccess.lib.PosixCLibrary;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.util.Objects;

public class PosixCloseableMappedByteBuffer extends JdkCloseableMappedByteBuffer {

    static final PosixCLibrary LIB = NativeLibraryProvider.instance().getLibrary(PosixCLibrary.class);
    static final int PAGE_SIZE = LIB.getPageSize();

    public static PosixCloseableMappedByteBuffer ofShared(FileChannel fileChannel, MapMode mode, long position, long size)
        throws IOException {
        var arena = Arena.ofShared();
        var seg = fileChannel.map(mode, position, size, arena);
        return new PosixCloseableMappedByteBuffer(seg, arena);
    }

    protected PosixCloseableMappedByteBuffer(MemorySegment seg, Arena arena) {
        super(seg, arena);
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
        int ret = LIB.madvise(segment, offset, length, PosixCLibrary.POSIX_MADV_WILLNEED);
        if (ret != 0) {
            int errno = LIB.errno();
            throw new RuntimeException("madvise failed with (error=" + errno + "): " + LIB.strerror(errno));
        }
    }
}
