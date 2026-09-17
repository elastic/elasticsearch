/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar.substrate;

import org.apache.lucene.util.LongValues;

/**
 * How a block that repeats the one before it is written down: it writes no bytes, which leaves its extent
 * in the offsets table empty. A run of equal blocks costs the bytes of its first block and an empty extent
 * for each of the rest, and the table stays monotonic because an empty extent is an offset repeated rather
 * than one that goes backwards.
 *
 * <p>Every reader of a block offsets table resolves a repeat before it reads.
 */
public final class BlockRuns {

    private BlockRuns() {}

    /**
     * The block holding the bytes {@code blockIndex} decodes from: itself, or the block its run repeats.
     * A run is contiguous, so the block that holds the bytes is the one before the first block sharing
     * this offset.
     */
    public static long source(LongValues offsets, long blockIndex) {
        final long from = offsets.get(blockIndex);
        if (offsets.get(blockIndex + 1) != from) {
            return blockIndex;
        }
        long low = 0;
        long high = blockIndex;
        while (low < high) {
            final long mid = (low + high) >>> 1;
            if (offsets.get(mid) < from) {
                low = mid + 1;
            } else {
                high = mid;
            }
        }
        assert low > 0 : "a run of repeats with nothing to repeat at block " + blockIndex;
        return low - 1;
    }
}
