/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.nativeaccess.jdk;

import org.elasticsearch.nativeaccess.CloseableByteBuffer;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;

class JdkCloseableByteBuffer implements CloseableByteBuffer {
    private final Arena arena;
    final MemorySegment segment;
    private final ByteBuffer bufferView;

    JdkCloseableByteBuffer(int len) {
        this.arena = Arena.ofConfined();
        this.segment = arena.allocate(len);
        this.bufferView = segment.asByteBuffer();
    }

    @Override
    public ByteBuffer buffer() {
        return bufferView;
    }

    @Override
    public void close() {
        arena.close();
    }
}
