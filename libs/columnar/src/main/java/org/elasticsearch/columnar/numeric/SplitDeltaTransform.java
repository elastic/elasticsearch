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

/**
 * Segmented delta encoding for piecewise-monotonic sequences. Fires when the block has between
 * 1 and {@code kMax} direction flips and every resulting sub-run has at least two values. Blocks
 * with zero flips are left to {@link DeltaTransform}. Not thread-safe; each pipeline must own
 * its own instance. Frozen id {@code 0x03}.
 */
public final class SplitDeltaTransform implements BlockTransform {

    static final byte ID = 3;

    private static final int DEFAULT_K_MAX = 16;

    private final int kMax;
    // Size kMax+1 so decode can place valueCount in splits[k] as a sentinel, removing the
    // per-sub-run bound check from the inner prefix-sum loop.
    private final int[] splits;
    private final long[] firstDeltas;

    /** Creates a transform allowing at most {@code kMax} direction flips per block. */
    public SplitDeltaTransform(int kMax) {
        if (kMax < 1) {
            throw new IllegalArgumentException("kMax must be at least 1, got: " + kMax);
        }
        this.kMax = kMax;
        this.splits = new int[kMax + 1];
        this.firstDeltas = new long[kMax + 1];
    }

    /** Creates a transform with the default {@code kMax} of 16, matching the TSDB production default. */
    public SplitDeltaTransform() {
        this(DEFAULT_K_MAX);
    }

    /** {@inheritDoc} */
    @Override
    public byte id() {
        return ID;
    }

    /** {@inheritDoc} */
    @Override
    public boolean tryEncode(long[] block, int valueCount, MetadataWriter params) throws IOException {
        if (valueCount < 4) {
            return false;
        }
        final int k = countFlips(block, valueCount);
        if (k <= 0) {
            return false;
        }
        if (hasShortSubRun(k, valueCount)) {
            return false;
        }

        int lo = 0;
        for (int j = 0; j < k; j++) {
            deltaEncodeSubRun(block, lo, splits[j], j);
            lo = splits[j];
        }
        deltaEncodeSubRun(block, lo, valueCount, k);

        params.writeVInt(k);
        for (int j = 0; j < k; j++) {
            params.writeVInt(splits[j]);
        }
        for (int j = 0; j <= k; j++) {
            params.writeZLong(firstDeltas[j]);
        }
        return true;
    }

    /** {@inheritDoc} */
    @Override
    public void decode(long[] block, int valueCount, MetadataReader params) throws IOException {
        final int k = params.readVInt();
        for (int j = 0; j < k; j++) {
            splits[j] = params.readVInt();
        }
        splits[k] = valueCount;
        for (int j = 0; j <= k; j++) {
            firstDeltas[j] = params.readZLong();
        }

        int lo = 0;
        for (int j = 0; j <= k; j++) {
            final int hi = splits[j];
            long sum = firstDeltas[j];
            // 4-wide ILP unroll: the four partial prefix sums are computed as a balanced tree
            // (two serial adds instead of four), letting the CPU issue the four stores in parallel.
            final int unrollEnd = lo + ((hi - lo) & ~3);
            int i = lo;
            for (; i < unrollEnd; i += 4) {
                final long v0 = block[i];
                final long v1 = block[i + 1];
                final long v2 = block[i + 2];
                final long v3 = block[i + 3];
                final long s1 = v0 + v1;
                final long t23 = v2 + v3;
                final long s2 = s1 + v2;
                final long s3 = s1 + t23;
                block[i] = sum + v0;
                block[i + 1] = sum + s1;
                block[i + 2] = sum + s2;
                block[i + 3] = sum + s3;
                sum += s3;
            }
            for (; i < hi; i++) {
                sum += block[i];
                block[i] = sum;
            }
            lo = hi;
        }
    }

    /**
     * Direction changes are committed lazily so the canonical TSDB pattern [desc, UP, desc] resolves
     * to one split (the UP value joins the next sub-run) rather than two splits around a sub-run of
     * length 1. A trailing {@code pendingFlip} is also committed, otherwise a _tsid transition
     * landing on the last value would silently stay inside the last sub-run.
     */
    private int countFlips(final long[] block, final int valueCount) {
        int k = 0;
        int prev = 0;
        int pendingFlip = -1;
        int pendingDir = 0;
        for (int i = 1; i < valueCount; i++) {
            final long diff = block[i] - block[i - 1];
            final int cur = Long.signum(diff);
            if (cur == 0) {
                continue;
            }
            if (prev == 0) {
                prev = cur;
                continue;
            }
            if (pendingFlip < 0) {
                if (cur != prev) {
                    pendingFlip = i;
                    pendingDir = cur;
                }
                continue;
            }
            if (k == kMax) {
                return -1;
            }
            splits[k++] = pendingFlip;
            if (cur != prev) {
                prev = pendingDir;
            }
            pendingFlip = -1;
            pendingDir = 0;
        }
        if (pendingFlip > 0) {
            if (k == kMax) {
                return -1;
            }
            splits[k++] = pendingFlip;
        }
        return k;
    }

    // Only the trailing sub-run can degenerate to length one; internal ones are at least two apart
    // by construction in countFlips.
    private boolean hasShortSubRun(final int k, final int valueCount) {
        return valueCount - splits[k - 1] < 2;
    }

    private void deltaEncodeSubRun(final long[] block, int lo, int hi, int j) {
        for (int i = hi - 1; i > lo; i--) {
            block[i] -= block[i - 1];
        }
        firstDeltas[j] = block[lo] - block[lo + 1];
        block[lo] = block[lo + 1];
    }
}
