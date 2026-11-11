/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb.pipeline.numeric;

import org.elasticsearch.index.codec.tsdb.pipeline.DecodingContext;

import java.io.IOException;

/**
 * Decodes the trailing {@code delta > offset > gcd} suffix of the production pipeline
 * in one pass over the block instead of three.
 *
 * <p>Each of {@link org.elasticsearch.index.codec.tsdb.pipeline.numeric.stages.DeltaCodecStage},
 * {@link org.elasticsearch.index.codec.tsdb.pipeline.numeric.stages.OffsetCodecStage}, and
 * {@link org.elasticsearch.index.codec.tsdb.pipeline.numeric.stages.GcdCodecStage} on its own
 * walks {@code values[]} once. They can be undone together though: every decoded value is
 * the previous decoded value plus a small step, where the step is the encoded value
 * scaled by {@code gcd} and shifted by {@code min}. When the delta stage did not apply
 * for the block we drop the running sum and treat each element independently. When
 * {@code offset} or {@code gcd} did not apply we substitute the neutral values
 * ({@code min = 0}, {@code gcd = 1}) and the same loop still produces the right answer.
 *
 * <p>Four inner loops cover the two binary choices that change the loop body shape.
 * {@code CUMULATIVE_SUM_*} carry a running sum (delta applied); {@code LINEAR_*} treat
 * each element independently, which lets the JIT autovectorize the body. {@code *_SHIFT}
 * replaces the multiply by {@code gcd} with a left shift when {@code gcd} is a power of
 * two; it also covers the {@code gcd == 1} case because {@code shift == 0} is identity.
 * {@code *_MUL} does the multiply otherwise.
 *
 * <p>Per-stage metadata is read in {@code gcd, offset, delta} order to match the
 * reverse-stage on-disk layout written by
 * {@link org.elasticsearch.index.codec.tsdb.pipeline.EncodingContext#writeStageMetadata},
 * and only for the stages whose bit is set in the block bitmap.
 *
 * <p>Stateless and thread-safe; {@link #INSTANCE} matches the singleton convention used
 * by the surrounding stage classes.
 */
final class DeltaOffsetGcd {

    /** Singleton instance. */
    static final DeltaOffsetGcd INSTANCE = new DeltaOffsetGcd();

    private DeltaOffsetGcd() {}

    /**
     * Decodes the fused {@code delta > offset > gcd} suffix into {@code values} in one pass.
     *
     * @param values       the array populated by the bit-pack payload stage
     * @param count        the number of values to transform
     * @param context      the per-block decoding context
     * @param basePosition the bitmap position of the {@code delta} stage ({@code offset} is at {@code +1}, {@code gcd} at {@code +2})
     * @throws IOException if a metadata read fails
     */
    void decode(final long[] values, int count, final DecodingContext context, int basePosition) throws IOException {
        final boolean deltaApplied = context.isStageApplied(basePosition);
        final boolean offsetApplied = context.isStageApplied(basePosition + 1);
        final boolean gcdApplied = context.isStageApplied(basePosition + 2);

        if (deltaApplied == false && offsetApplied == false && gcdApplied == false) {
            return;
        }

        final long gcd = gcdApplied ? context.metadata().readVLong() + 2 : 1L;
        final long min = offsetApplied ? context.metadata().readZLong() : 0L;
        final long first = deltaApplied ? context.metadata().readZLong() : 0L;
        final boolean pow2 = (gcd & (gcd - 1)) == 0;
        final int shift = pow2 ? Long.numberOfTrailingZeros(gcd) : 0;

        final int mode = (deltaApplied ? CUMULATIVE_SUM_MUL : LINEAR_MUL) | (pow2 ? 1 : 0);
        switch (mode) {
            case LINEAR_MUL -> decodeLinearMul(values, count, gcd, min);
            case LINEAR_SHIFT -> decodeLinearShift(values, count, shift, min);
            case CUMULATIVE_SUM_MUL -> decodeCumulativeSumMul(values, count, first, gcd, min);
            case CUMULATIVE_SUM_SHIFT -> decodeCumulativeSumShift(values, count, first, shift, min);
            default -> throw new AssertionError("unreachable loop dispatch mode: " + mode);
        }
    }

    private static final int LINEAR_MUL = 0;
    private static final int LINEAR_SHIFT = 1;
    private static final int CUMULATIVE_SUM_MUL = 2;
    private static final int CUMULATIVE_SUM_SHIFT = 3;

    private static void decodeLinearMul(final long[] values, int count, long gcd, long min) {
        for (int i = 0; i < count; i++) {
            values[i] = values[i] * gcd + min;
        }
    }

    private static void decodeLinearShift(final long[] values, int count, int shift, long min) {
        for (int i = 0; i < count; i++) {
            values[i] = (values[i] << shift) + min;
        }
    }

    private static void decodeCumulativeSumMul(final long[] values, int count, long first, long gcd, long min) {
        long sum = first;
        for (int i = 0; i < count; i++) {
            sum += values[i] * gcd + min;
            values[i] = sum;
        }
    }

    private static void decodeCumulativeSumShift(final long[] values, int count, long first, int shift, long min) {
        long sum = first;
        for (int i = 0; i < count; i++) {
            sum += (values[i] << shift) + min;
            values[i] = sum;
        }
    }
}
