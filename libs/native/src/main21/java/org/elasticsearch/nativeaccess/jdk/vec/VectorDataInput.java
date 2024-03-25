/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.nativeaccess.jdk.vec;

import java.io.Closeable;
import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.AccessController;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;

import static java.lang.foreign.ValueLayout.JAVA_BYTE;
import static java.lang.foreign.ValueLayout.JAVA_INT_UNALIGNED;
import static java.nio.ByteOrder.LITTLE_ENDIAN;

public class VectorDataInput implements Closeable {

    static final ValueLayout.OfInt LAYOUT_LE_INT = JAVA_INT_UNALIGNED.withOrder(LITTLE_ENDIAN);

    private final Arena arena;
    private final MemorySegment segment; // TODO: single segment, could be very large!

    VectorDataInput(Arena arena, MemorySegment segment) {
        this.arena = arena;
        this.segment = segment;
    }

    MemorySegment addressFor(int offset, int length) {
        return segment.asSlice(offset, length);
    }

    byte readByte(long offset) {
        return segment.get(JAVA_BYTE, offset);
    }

    float readFloat(long offset) {
        return Float.intBitsToFloat(segment.get(LAYOUT_LE_INT, offset));
    }

    static VectorDataInput createVectorDataInput(Path path/*,  int chunkSizePower*/) throws IOException {
        boolean success = false;
        final Arena arena = Arena.ofShared();
        try (var fc = privilegedOpen(path, StandardOpenOption.READ)) {
            long fileSize = fc.size();
            var segment = fc.map(FileChannel.MapMode.READ_ONLY, 0, fileSize, arena);
            success = true;
            return new VectorDataInput(arena, segment);
        } finally {
            if (success == false) {
                arena.close();
            }
        }
    }

    private MemorySegment[] map(Arena arena, String resourceDescription, FileChannel fc, int chunkSizePower, boolean preload, long length)
        throws IOException {
        if ((length >>> chunkSizePower) >= Integer.MAX_VALUE) throw new IllegalArgumentException(
            "File too big for chunk size: " + resourceDescription
        );

        final long chunkSize = 1L << chunkSizePower;

        // we always allocate one more segments, the last one may be a 0 byte one
        final int nrSegments = (int) (length >>> chunkSizePower) + 1;

        final MemorySegment[] segments = new MemorySegment[nrSegments];

        long startOffset = 0L;
        for (int segNr = 0; segNr < nrSegments; segNr++) {
            final long segSize = (length > (startOffset + chunkSize)) ? chunkSize : (length - startOffset);
            final MemorySegment segment;
            segment = fc.map(MapMode.READ_ONLY, startOffset, segSize, arena);
            if (preload) {
                segment.load();
            }
            segments[segNr] = segment;
            startOffset += segSize;
        }
        return segments;
    }

    public long getDefaultMaxChunkSize() {
        return (1L << 34);
    }

    @SuppressWarnings("removal")
    private static FileChannel privilegedOpen(Path path, OpenOption... options) throws IOException {
        try {
            return AccessController.doPrivileged((PrivilegedExceptionAction<FileChannel>) () -> FileChannel.open(path, options));
        } catch (PrivilegedActionException e) {
            throw (IOException) e.getCause();
        }
    }

    @Override
    public void close() {
        arena.close();
    }
}
