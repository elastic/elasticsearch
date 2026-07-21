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

/** Delta-encodes strictly monotonic runs (first value kept as a delta from the second). Frozen id 0. */
public final class DeltaTransform implements BlockTransform {

    static final byte ID = 0;

    @Override
    public byte id() {
        return ID;
    }

    @Override
    public boolean tryEncode(long[] block, DataOutput params) throws IOException {
        int gts = 0;
        int lts = 0;
        for (int i = 1; i < block.length; ++i) {
            if (block[i] > block[i - 1]) {
                gts++;
            } else if (block[i] < block[i - 1]) {
                lts++;
            }
        }
        if ((gts == 0 && lts >= 2) == false && (lts == 0 && gts >= 2) == false) {
            return false;
        }
        for (int i = block.length - 1; i > 0; --i) {
            block[i] -= block[i - 1];
        }
        // Keep block[0] as a delta from block[1] to save bits.
        long first = block[0] - block[1];
        block[0] = block[1];
        params.writeZLong(first);
        return true;
    }

    @Override
    public void decode(long[] block, DataInput params) throws IOException {
        block[0] += params.readZLong();
        long sum = 0;
        for (int i = 0; i < block.length; ++i) {
            sum += block[i];
            block[i] = sum;
        }
    }
}
