/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb.pipeline.numeric.stages;

import org.elasticsearch.nativeaccess.CloseableByteBuffer;
import org.elasticsearch.nativeaccess.NativeAccess;
import org.elasticsearch.nativeaccess.Zstd;

import java.lang.ref.Cleaner;
import java.nio.ByteOrder;

final class ZstdBuffers {

    private static final int MAX_COPY_BUFFER_SIZE = 4096;
    private static final Cleaner CLEANER = Cleaner.create();
    private static final ThreadLocal<ZstdBuffers> INSTANCE = ThreadLocal.withInitial(ZstdBuffers::new);

    private static final int INITIAL_BLOCK_SIZE = 512;

    final Zstd zstd;
    CloseableByteBuffer src;
    CloseableByteBuffer dest;
    byte[] copyBuffer;
    private int capacity;
    private Cleaner.Cleanable cleanable;

    static ZstdBuffers get(int blockSize) {
        final ZstdBuffers buffers = INSTANCE.get();
        buffers.ensureCapacity(blockSize);
        return buffers;
    }

    private ZstdBuffers() {
        this.zstd = NativeAccess.instance().getZstd();
        this.capacity = 0;
        allocate(INITIAL_BLOCK_SIZE);
    }

    private void ensureCapacity(int blockSize) {
        if (blockSize <= capacity) {
            return;
        }
        cleanable.clean();
        allocate(blockSize);
    }

    private void allocate(int blockSize) {
        final NativeAccess nativeAccess = NativeAccess.instance();
        final int uncompressedSize = blockSize * Long.BYTES;
        final int compressBound = zstd.compressBound(uncompressedSize);
        // NOTE: shared buffers so the Cleaner thread can close them
        this.src = nativeAccess.newSharedBuffer(compressBound);
        this.dest = nativeAccess.newSharedBuffer(compressBound);
        this.copyBuffer = new byte[Math.min(MAX_COPY_BUFFER_SIZE, compressBound)];
        this.src.buffer().order(ByteOrder.LITTLE_ENDIAN);
        this.dest.buffer().order(ByteOrder.LITTLE_ENDIAN);
        this.cleanable = CLEANER.register(this, new BufferCleaner(src, dest));
        this.capacity = blockSize;
    }

    private record BufferCleaner(CloseableByteBuffer src, CloseableByteBuffer dest) implements Runnable {
        @Override
        public void run() {
            src.close();
            dest.close();
        }
    }
}
