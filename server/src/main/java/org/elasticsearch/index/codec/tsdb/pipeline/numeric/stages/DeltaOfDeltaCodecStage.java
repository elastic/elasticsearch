/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb.pipeline.numeric.stages;

import org.elasticsearch.index.codec.tsdb.pipeline.DecodingContext;
import org.elasticsearch.index.codec.tsdb.pipeline.EncodingContext;
import org.elasticsearch.index.codec.tsdb.pipeline.StageId;
import org.elasticsearch.index.codec.tsdb.pipeline.numeric.NumericCodecStage;

import java.io.IOException;

/**
 * Second order difference transform stage. Targets sequences where the rate of change
 * itself is roughly constant (linearly growing or shrinking intervals).
 *
 * <h2>Effectiveness</h2>
 * <p>Applied when the sequence is strictly monotonic (at least 2 increases with 0 decreases,
 * or at least 2 decreases with 0 increases), the same gate as {@link DeltaCodecStage}. The
 * gate naturally rejects {@code valueCount < 3}, the minimum size at which a second order
 * difference is defined.
 *
 * <p>Sharing the gate with {@link DeltaCodecStage} is intentional: when this stage's gate
 * fails on non monotonic input, {@code DeltaCodecStage}'s gate would fail on the same input,
 * so there is no scenario where this stage skips but a single delta fallback could still
 * apply. The pipeline degrades to {@code offset > bitPack} on raw values either way, the
 * same outcome a {@code delta > offset > bitPack} pipeline would produce on the same input.
 *
 * <h2>Example</h2>
 * <p>Monotonic ascending {@code [1000, 1100, 1210, 1330, 1460]} (intervals 100, 110, 120, 130)
 * produces second order differences {@code [10, 10, 10, 10, 10]} in the values array. First value
 * 1000 and first interval 100 are stored as metadata; the downstream offset stage then subtracts
 * the constant 10, leaving zeros for bitPack.
 *
 * <h2>Metadata layout</h2>
 * <p>Written to the stage metadata section (see {@link org.elasticsearch.index.codec.tsdb.pipeline.BlockFormat}):
 * <pre>
 *   +---------------------+
 *   | ZLong(first)        |  1 to 10 bytes, zigzag encoded first value
 *   +---------------------+
 *   | ZLong(firstDelta)   |  1 to 10 bytes, zigzag encoded first interval
 *   +---------------------+
 * </pre>
 * <p>Storing the first value {@code T_0} and the first interval {@code d_0} in metadata, plus
 * overwriting positions 0 and 1 with the first second order difference (see encode), keeps the
 * values array uniform on constant rate input. Without these moves {@code T_0} and {@code T_1}
 * would remain in the values array, spanning a wide range relative to the small second order
 * residuals, and {@link OffsetCodecStage} would skip via its {@code |min| < |max|/4} heuristic,
 * forcing bitPack to encode the full original range.
 *
 * <h2>Decode performance</h2>
 * <p>Decoding is two independent forward prefix sums over the values array, each with the
 * same shape as {@link DeltaCodecStage#decode}. The two passes are independent (no
 * interleaved accumulators) so each can be vectorized by the JIT as a standard prefix
 * sum scan.
 */
public final class DeltaOfDeltaCodecStage implements NumericCodecStage {

    /** Singleton instance. */
    public static final DeltaOfDeltaCodecStage INSTANCE = new DeltaOfDeltaCodecStage();

    private DeltaOfDeltaCodecStage() {}

    @Override
    public byte id() {
        return StageId.DELTA_OF_DELTA_STAGE.id;
    }

    @Override
    public void encode(final long[] values, final int valueCount, final EncodingContext context) {
        assert valueCount >= 1 : "valueCount must be at least 1";
        if (isMonotonic(values, valueCount) == false) {
            return;
        }
        assert valueCount >= 3 : "isMonotonic must reject valueCount < 3 (two strict transitions require at least three values)";

        final long first = values[0];
        final long firstDelta = values[1] - values[0];

        for (int i = valueCount - 1; i >= 2; i--) {
            final long previous = values[i - 1];
            values[i] = (values[i] - previous) - (previous - values[i - 2]);
        }

        values[0] = values[2];
        values[1] = values[2];

        context.metadata().writeZLong(first - values[2]);
        context.metadata().writeZLong(firstDelta);
    }

    @Override
    public void decode(final long[] values, final int valueCount, final DecodingContext context) throws IOException {
        assert valueCount >= 3 : "valueCount must be at least 3 for delta-of-delta decode";
        final long first = context.metadata().readZLong();
        final long firstDelta = context.metadata().readZLong();

        values[1] = firstDelta;
        long delta = firstDelta;
        for (int i = 2; i < valueCount; i++) {
            delta += values[i];
            values[i] = delta;
        }

        long sum = first;
        for (int i = 0; i < valueCount; i++) {
            sum += values[i];
            values[i] = sum;
        }
    }

    public static void encodeStatic(final DeltaOfDeltaCodecStage stage, final long[] values, int valueCount, final EncodingContext context)
        throws IOException {
        stage.encode(values, valueCount, context);
    }

    public static void decodeStatic(final DeltaOfDeltaCodecStage stage, final long[] values, int valueCount, final DecodingContext context)
        throws IOException {
        stage.decode(values, valueCount, context);
    }

    private static boolean isMonotonic(final long[] values, final int valueCount) {
        int increases = 0;
        int decreases = 0;
        for (int i = 1; i < valueCount; i++) {
            if (values[i] > values[i - 1]) {
                if (decreases > 0) {
                    return false;
                }
                increases++;
            } else if (values[i] < values[i - 1]) {
                if (increases > 0) {
                    return false;
                }
                decreases++;
            }
        }
        return increases >= 2 || decreases >= 2;
    }
}
