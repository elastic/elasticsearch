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

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.util.Objects;

public class JdkCloseableMappedByteBuffer implements CloseableMappedByteBuffer {

    private final Arena arena;
    protected final MemorySegment segment;
    private final MappedByteBuffer bufferView;

    public static JdkCloseableMappedByteBuffer ofShared(FileChannel fileChannel, MapMode mode, long position, long size)
        throws IOException {
        var arena = Arena.ofShared();
        var seg = fileChannel.map(mode, position, size, arena);
        return new JdkCloseableMappedByteBuffer(seg, arena);
    }

    protected JdkCloseableMappedByteBuffer(MemorySegment seg, Arena arena) {
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
        // no explicit action, override in subclass if needed.
    }
}
