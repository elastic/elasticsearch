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
import java.lang.reflect.Method;
import java.nio.channels.FileChannel;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.AccessController;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;

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

    float readFloat(long offset) {
        return Float.intBitsToFloat(segment.get(LAYOUT_LE_INT, offset));
    }

    static VectorDataInput createVectorDataInput(Path path) throws IOException {
        // TODO: add existence / file checks

        // Work around for JDK-8259028: we need to unwrap our test-only file system layers
        path = unwrapAll(path);

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

    @SuppressWarnings("removal")
    private static FileChannel privilegedOpen(Path path, OpenOption... options) throws IOException {
        try {
            return AccessController.doPrivileged((PrivilegedExceptionAction<FileChannel>) () -> FileChannel.open(path, options));
        } catch (PrivilegedActionException e) {
            throw (IOException) e.getCause();
        }
    }

    @SuppressWarnings("unchecked") // TODO: this kinda sucks, maybe depend upon lucene :-(
    public static <T> T unwrapAll(T o) {
        try {
            Class<?> clazz = Class.forName("org.apache.lucene.util.Unwrappable");
            Method m = clazz.getMethod("unwrap");
            while (clazz.isAssignableFrom(o.getClass())) {
                o = (T) m.invoke(o);
            }
            return o;
        } catch (ReflectiveOperationException e) {
            throw new AssertionError(e);
        }
    }

    @Override
    public void close() {
        arena.close();
    }
}
