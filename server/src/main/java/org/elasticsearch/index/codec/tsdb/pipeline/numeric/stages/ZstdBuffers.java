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

    private static final int MAX_BLOCK_SIZE = 16384;
    private static final int MAX_COPY_BUFFER_SIZE = 4096;
    private static final Cleaner CLEANER = Cleaner.create();

    private static final ThreadLocal<ZstdBuffers> INSTANCE = ThreadLocal.withInitial(ZstdBuffers::new);

    final CloseableByteBuffer src;
    final CloseableByteBuffer dest;
    final byte[] copyBuffer;
    final Zstd zstd;

    static ZstdBuffers get() {
        return INSTANCE.get();
    }

    private ZstdBuffers() {
        final NativeAccess nativeAccess = NativeAccess.instance();
        this.zstd = nativeAccess.getZstd();
        final int maxUncompressedSize = MAX_BLOCK_SIZE * Long.BYTES;
        final int compressBound = zstd.compressBound(maxUncompressedSize);
        // NOTE: shared buffers so the Cleaner thread can close them
        this.src = nativeAccess.newSharedBuffer(compressBound);
        this.dest = nativeAccess.newSharedBuffer(compressBound);
        this.copyBuffer = new byte[Math.min(MAX_COPY_BUFFER_SIZE, compressBound)];
        this.src.buffer().order(ByteOrder.LITTLE_ENDIAN);
        this.dest.buffer().order(ByteOrder.LITTLE_ENDIAN);
        CLEANER.register(this, new BufferCleaner(src, dest));
    }

    private record BufferCleaner(CloseableByteBuffer src, CloseableByteBuffer dest) implements Runnable {
        @Override
        public void run() {
            src.close();
            dest.close();
        }
    }
}
