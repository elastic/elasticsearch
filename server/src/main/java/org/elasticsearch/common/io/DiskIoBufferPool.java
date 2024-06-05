/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.io;

import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.threadpool.ThreadPool;

import java.nio.ByteBuffer;

public class DiskIoBufferPool {

    public static final int BUFFER_SIZE = StrictMath.toIntExact(
        ByteSizeValue.parseBytesSizeValue(System.getProperty("es.disk_io.direct.buffer.size", "64KB"), "es.disk_io.direct.buffer.size")
            .getBytes()
    );

    // placeholder to cache the fact that a thread does not work with cached direct IO buffers in #ioBufferPool
    private static final ByteBuffer EMPTY_BUFFER = ByteBuffer.allocate(0);

    // protected for testing
    protected static final ThreadLocal<ByteBuffer> ioBufferPool = ThreadLocal.withInitial(() -> {
        if (isWriteOrFlushThread()) {
            return ByteBuffer.allocateDirect(BUFFER_SIZE);
        } else {
            return EMPTY_BUFFER;
        }
    });

    public static final DiskIoBufferPool INSTANCE = new DiskIoBufferPool();

    protected DiskIoBufferPool() {}

    /**
     * @return thread-local cached direct byte buffer if we are on a thread that supports caching direct buffers or null otherwise
     */
    @Nullable
    public ByteBuffer maybeGetDirectIOBuffer() {
        ByteBuffer ioBuffer = ioBufferPool.get();
        if (ioBuffer == EMPTY_BUFFER) {
            return null;
        }
        return ioBuffer.clear();
    }

    private static final String[] WRITE_OR_FLUSH_THREAD_NAMES = new String[] {
        "[" + ThreadPool.Names.WRITE + "]",
        "[" + ThreadPool.Names.FLUSH + "]",
        "[" + ThreadPool.Names.SYSTEM_WRITE + "]",
        "[" + ThreadPool.Names.SYSTEM_CRITICAL_WRITE + "]" };

    private static boolean isWriteOrFlushThread() {
        String threadName = Thread.currentThread().getName();
        for (String s : WRITE_OR_FLUSH_THREAD_NAMES) {
            if (threadName.contains(s)) {
                return true;
            }
        }
        return false;
    }
}
