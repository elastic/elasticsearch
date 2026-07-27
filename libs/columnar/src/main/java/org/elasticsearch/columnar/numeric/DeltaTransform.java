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

    /** Shared stateless instance. */
    public static final DeltaTransform INSTANCE = new DeltaTransform();

    @Override
    public byte id() {
        return ID;
    }

    @Override
    public boolean tryEncode(long[] block, DataOutput params) throws IOException {
        if (isMonotonic(block) == false) {
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

    /**
     * A block is monotonic (worth delta-encoding) when all its strict steps go the same direction and
     * there are at least two of them. Stops at the first direction conflict: once the block has shown
     * both an increase and a decrease it can never be monotonic.
     */
    private static boolean isMonotonic(long[] block) {
        boolean up = false;
        boolean down = false;
        int strictSteps = 0;
        for (int i = 1; i < block.length; ++i) {
            if (block[i] > block[i - 1]) {
                up = true;
                strictSteps++;
            } else if (block[i] < block[i - 1]) {
                down = true;
                strictSteps++;
            }
            if (up && down) {
                return false;
            }
        }
        return strictSteps >= 2;
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
