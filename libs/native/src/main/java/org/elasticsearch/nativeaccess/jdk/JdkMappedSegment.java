/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.nativeaccess.jdk;

import org.elasticsearch.nativeaccess.MappedSegment;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.util.Objects;

public class JdkMappedSegment implements MappedSegment {

    private final Arena arena;
    protected final MemorySegment segment;

    public static JdkMappedSegment ofShared(FileChannel fileChannel, MapMode mode, long position, long size) throws IOException {
        var arena = Arena.ofShared();
        var seg = fileChannel.map(mode, position, size, arena);
        return new JdkMappedSegment(seg, arena);
    }

    protected JdkMappedSegment(MemorySegment seg, Arena arena) {
        this.arena = arena;
        this.segment = seg;
    }

    @Override
    public MemorySegment segment() {
        return segment;
    }

    @Override
    public void close() {
        if (arena != null) {
            arena.close();
        }
    }

    @Override
    public MappedSegment slice(long index, long length) {
        var slice = segment.asSlice(index, length);
        return new JdkMappedSegment(slice, null);
    }

    @Override
    public void prefetch(long offset, long length) {
        Objects.checkFromIndexSize(offset, length, segment.byteSize());
        // no explicit action, override in subclass if needed.
    }

    @Override
    public void madvise(long offset, long length, int advice) {
        Objects.checkFromIndexSize(offset, length, segment.byteSize());
        // no explicit action, override in subclass if needed.
    }
}
