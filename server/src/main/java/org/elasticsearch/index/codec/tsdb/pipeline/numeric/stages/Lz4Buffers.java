/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb.pipeline.numeric.stages;

final class Lz4Buffers {

    private static final int INITIAL_BLOCK_SIZE = 512;
    private static final ThreadLocal<Lz4Buffers> INSTANCE = ThreadLocal.withInitial(Lz4Buffers::new);

    byte[] src;
    byte[] dest;

    static Lz4Buffers get(int blockSize) {
        final Lz4Buffers buffers = INSTANCE.get();
        buffers.ensureCapacity(blockSize);
        return buffers;
    }

    private Lz4Buffers() {
        final int initialSize = INITIAL_BLOCK_SIZE * Long.BYTES;
        this.src = new byte[initialSize];
        this.dest = new byte[initialSize + initialSize / 255 + 16];
    }

    private void ensureCapacity(int blockSize) {
        final int needed = blockSize * Long.BYTES;
        if (needed > src.length) {
            this.src = new byte[needed];
            this.dest = new byte[needed + needed / 255 + 16];
        }
    }
}
