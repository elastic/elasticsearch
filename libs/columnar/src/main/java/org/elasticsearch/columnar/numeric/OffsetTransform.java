/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar.numeric;

import java.io.IOException;

/** Shifts a block into {@code [0, max - min]} by subtracting its minimum. Frozen id 1. */
public final class OffsetTransform implements BlockTransform {

    static final byte ID = 1;

    /** Shared stateless instance. */
    public static final OffsetTransform INSTANCE = new OffsetTransform();

    @Override
    public byte id() {
        return ID;
    }

    @Override
    public boolean tryEncode(long[] block, int valueCount, MetadataWriter params) throws IOException {
        long min = Long.MAX_VALUE;
        long max = Long.MIN_VALUE;
        for (int i = 0; i < valueCount; ++i) {
            min = Math.min(block[i], min);
            max = Math.max(block[i], max);
        }

        // The overflow guard must run first so Math.abs below is safe (Math.abs(Long.MIN_VALUE) overflows).
        if (max - min < 0) {
            // overflow: the shifted range would not fit in a signed long, so keep the block as-is
            min = 0;
        } else if (Math.abs(min) < Math.abs(max) / 4) {
            // removing the offset is unlikely to save bits per value, yet it makes decoding slower
            min = 0;
        }

        if (min == 0) {
            return false;
        }
        for (int i = 0; i < valueCount; ++i) {
            block[i] -= min;
        }
        params.writeZLong(min);
        return true;
    }

    @Override
    public void decode(long[] block, int valueCount, MetadataReader params) throws IOException {
        long min = params.readZLong();
        for (int i = 0; i < valueCount; ++i) {
            block[i] += min;
        }
    }
}
