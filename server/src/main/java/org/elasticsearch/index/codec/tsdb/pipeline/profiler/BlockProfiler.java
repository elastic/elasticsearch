/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb.pipeline.profiler;

import org.apache.lucene.util.MathUtil;

public final class BlockProfiler {

    // NOTE: must match DeltaCodecStage.DEFAULT_MIN_DIRECTIONAL_CHANGES so the profiler's
    // monotonic classification agrees with the stage's internal guard. If the profiler reports
    // isMonotonicallyIncreasing=true but the stage requires more directional changes, the
    // delta transform silently becomes a no-op.
    private static final int DEFAULT_MIN_DIRECTIONAL_CHANGES = 2;

    public static final BlockProfiler INSTANCE = new BlockProfiler();

    private final int minDirectionalChanges;

    private static final BlockProfile EMPTY = new BlockProfile(0, 0, 0, 0, 0, 0, false, false, 0, 0, 0, 0, 0);

    public BlockProfiler() {
        this(DEFAULT_MIN_DIRECTIONAL_CHANGES);
    }

    public BlockProfiler(int minDirectionalChanges) {
        if (minDirectionalChanges < 1) {
            throw new IllegalArgumentException("minDirectionalChanges must be >= 1: " + minDirectionalChanges);
        }
        this.minDirectionalChanges = minDirectionalChanges;
    }

    private record MinMax(long min, long max) {}

    private record GcdPair(long rawGcd, long shiftedGcd) {}

    public BlockProfile profile(final long[] values, int valueCount) {
        if (valueCount == 0) {
            return EMPTY;
        }
        if (valueCount == 1) {
            return new BlockProfile(1, values[0], values[0], 0, values[0], 0, false, false, 1, 0, 0, 0, 0);
        }

        // NOTE: each pass is a single-reduction loop so C2 can auto-vectorize independently.
        final MinMax minMax = minMax(values, valueCount);
        final GcdPair gcdPair = gcdPair(values, valueCount);
        final long xorOr = xorOr(values, valueCount);
        final int gts = countGreater(values, valueCount);
        final int lts = countLess(values, valueCount);
        final int runCount = runCount(values, valueCount);
        final long ddOr = deltaDeltaOr(values, valueCount);

        final long range = minMax.max - minMax.min;
        return new BlockProfile(
            valueCount,
            minMax.min,
            minMax.max,
            range,
            gcdPair.rawGcd,
            gcdPair.shiftedGcd,
            gts >= minDirectionalChanges && lts == 0,
            lts >= minDirectionalChanges && gts == 0,
            runCount,
            Long.SIZE - Long.numberOfLeadingZeros(range),
            Long.SIZE - Long.numberOfLeadingZeros(xorOr),
            valueCount - runCount,
            Long.SIZE - Long.numberOfLeadingZeros(ddOr)
        );
    }

    private static MinMax minMax(final long[] values, int valueCount) {
        long min = values[0];
        long max = values[0];
        for (int i = 1; i < valueCount; i++) {
            min = Math.min(min, values[i]);
            max = Math.max(max, values[i]);
        }
        return new MinMax(min, max);
    }

    // NOTE: computes both raw GCD and shifted GCD (GCD of adjacent differences) in a
    // single pass. shiftedGcd correctly predicts what the gcd() stage will see after any
    // subtraction-based transform (offset, delta, deltaDelta) because GCD of differences
    // is shift-invariant. rawGcd = gcd(v[0], shiftedGcd) by the Euclidean identity.
    private static GcdPair gcdPair(final long[] values, int valueCount) {
        long shiftedGcd = 0;
        for (int i = 1; i < valueCount; i++) {
            final long diff = values[i] - values[i - 1];
            if (diff == Long.MIN_VALUE) {
                return new GcdPair(1, 1);
            }
            shiftedGcd = MathUtil.gcd(shiftedGcd, Math.abs(diff));
            if (shiftedGcd == 1) {
                return new GcdPair(1, 1);
            }
        }
        if (values[0] == Long.MIN_VALUE) {
            return new GcdPair(1, shiftedGcd);
        }
        final long rawGcd = shiftedGcd > 0 ? MathUtil.gcd(Math.abs(values[0]), shiftedGcd) : Math.abs(values[0]);
        return new GcdPair(rawGcd, shiftedGcd);
    }

    private static long xorOr(final long[] values, int valueCount) {
        long xorOr = 0;
        for (int i = 1; i < valueCount; i++) {
            xorOr |= values[i] ^ values[i - 1];
        }
        return xorOr;
    }

    private static int countGreater(final long[] values, int valueCount) {
        int count = 0;
        for (int i = 1; i < valueCount; i++) {
            count += (values[i] > values[i - 1]) ? 1 : 0;
        }
        return count;
    }

    private static int countLess(final long[] values, int valueCount) {
        int count = 0;
        for (int i = 1; i < valueCount; i++) {
            count += (values[i] < values[i - 1]) ? 1 : 0;
        }
        return count;
    }

    private static int runCount(final long[] values, int valueCount) {
        int count = 1;
        for (int i = 1; i < valueCount; i++) {
            count += (values[i] != values[i - 1]) ? 1 : 0;
        }
        return count;
    }

    // NOTE: uses direct formula values[i] - 2*values[i-1] + values[i-2] to avoid
    // loop-carried dependency on prevDelta, making it vectorizable.
    private static long deltaDeltaOr(final long[] values, int valueCount) {
        long ddOr = 0;
        for (int i = 2; i < valueCount; i++) {
            final long dd = values[i] - 2 * values[i - 1] + values[i - 2];
            ddOr |= (dd >> 63) ^ (dd << 1);
        }
        return ddOr;
    }
}
