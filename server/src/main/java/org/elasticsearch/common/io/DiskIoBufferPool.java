/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.io;

import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.threadpool.ThreadPool;

import java.nio.ByteBuffer;
import java.util.Arrays;

public class DiskIoBufferPool {

    public static final int BUFFER_SIZE = StrictMath.toIntExact(ByteSizeValue.parseBytesSizeValue(
        System.getProperty("es.disk_io.direct.buffer.size", "64KB"), "es.disk_io.direct.buffer.size").getBytes());
    public static final int HEAP_BUFFER_SIZE = 8 * 1024;

    private static final ThreadLocal<ByteBuffer> ioBufferPool = ThreadLocal.withInitial(() -> {
        if (isWriteOrFlushThread()) {
            return ByteBuffer.allocateDirect(BUFFER_SIZE);
        } else {
            return ByteBuffer.allocate(HEAP_BUFFER_SIZE);
        }
    });

    public static ByteBuffer getIoBuffer() {
        ByteBuffer ioBuffer = ioBufferPool.get();
        ioBuffer.clear();
        return ioBuffer;
    }

    private static boolean isWriteOrFlushThread() {
        String threadName = Thread.currentThread().getName();
        for (String s : Arrays.asList(
            "[" + ThreadPool.Names.WRITE + "]",
            "[" + ThreadPool.Names.FLUSH + "]",
            "[" + ThreadPool.Names.SYSTEM_WRITE + "]",
            "[" + ThreadPool.Names.SYSTEM_CRITICAL_WRITE + "]")) {
            if (threadName.contains(s)) {
                return true;
            }
        }
        return false;
    }
}
