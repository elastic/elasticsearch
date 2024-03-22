/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.nativeaccess.jna;

import com.sun.jna.Memory;

import org.elasticsearch.nativeaccess.CloseableByteBuffer;

import java.nio.ByteBuffer;

class JnaCloseableByteBuffer implements CloseableByteBuffer {
    private final Memory memory;
    private final ByteBuffer bufferView;

    JnaCloseableByteBuffer(int len) {
        this.memory = new Memory(len);
        this.bufferView = memory.getByteBuffer(0, len);
    }

    @Override
    public ByteBuffer buffer() {
        return bufferView;
    }

    @Override
    public void close() {
        memory.close();
    }
}
