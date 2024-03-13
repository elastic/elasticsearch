/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.nativeaccess.jdk;

import org.elasticsearch.nativeaccess.CloseableByteBuffer;

import java.lang.foreign.Arena;
import java.nio.ByteBuffer;

class JdkCloseableByteBuffer implements CloseableByteBuffer {
    private final Arena arena;
    private final ByteBuffer bufferView;

    JdkCloseableByteBuffer(int len) {
        this.arena = Arena.ofShared();
        this.bufferView = this.arena.allocate(len).asByteBuffer();
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
