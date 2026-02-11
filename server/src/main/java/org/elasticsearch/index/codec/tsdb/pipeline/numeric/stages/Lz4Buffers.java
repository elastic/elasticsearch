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

    private static final int MAX_BLOCK_SIZE = 16384;
    private static final ThreadLocal<Lz4Buffers> INSTANCE = ThreadLocal.withInitial(Lz4Buffers::new);

    final byte[] src;
    final byte[] dest;

    static Lz4Buffers get() {
        return INSTANCE.get();
    }

    private Lz4Buffers() {
        final int maxUncompressedSize = MAX_BLOCK_SIZE * Long.BYTES;
        final int maxCompressedSize = maxUncompressedSize + maxUncompressedSize / 255 + 16;
        this.src = new byte[maxUncompressedSize];
        this.dest = new byte[maxCompressedSize];
    }
}
