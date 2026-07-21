/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar.numeric;

import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;

import java.io.IOException;

/** Shifts a block into {@code [0, max - min]} by subtracting its minimum. Frozen id 1. */
public final class OffsetTransform implements BlockTransform {

    static final byte ID = 1;

    @Override
    public byte id() {
        return ID;
    }

    @Override
    public boolean tryEncode(long[] block, DataOutput params) throws IOException {
        long min = Long.MAX_VALUE;
        long max = Long.MIN_VALUE;
        for (long l : block) {
            min = Math.min(l, min);
            max = Math.max(l, max);
        }

        if (max - min < 0) {
            // overflow
            min = 0;
        } else if (min > 0 && min < (max >>> 2)) {
            // removing the offset is unlikely to save bits per value, yet it makes decoding slower
            min = 0;
        }

        if (min == 0) {
            return false;
        }
        for (int i = 0; i < block.length; ++i) {
            block[i] -= min;
        }
        params.writeZLong(min);
        return true;
    }

    @Override
    public void decode(long[] block, DataInput params) throws IOException {
        long min = params.readZLong();
        for (int i = 0; i < block.length; ++i) {
            block[i] += min;
        }
    }
}
