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
 * Delta encoding transform stage for monotonic sequences.
 *
 * <h2>Effectiveness</h2>
 * <p>Applied only when the sequence is monotonic (at least 2 increases with 0 decreases,
 * or at least 2 decreases with 0 increases). Non-monotonic or constant sequences are
 * skipped because deltas would not reduce the dynamic range.
 *
 * <h2>Example</h2>
 * <p>Monotonic ascending {@code [100, 200, 350, 500]} produces deltas
 * {@code [100, 150, 150]} with {@code values[0]} set to the first delta
 * ({@code 100}) and metadata storing {@code first = 100 - 100 = 0}.
 *
 * <h2>Metadata layout</h2>
 * <p>Written to the stage metadata section (see {@link org.elasticsearch.index.codec.tsdb.pipeline.BlockFormat}):
 * <pre>
 *   +---------------------+
 *   | ZLong(first)        |  1-10 bytes, zigzag-encoded (original first value - first delta)
 *   +---------------------+
 * </pre>
 * <p>Zigzag encoding ensures small absolute values (both positive and negative)
 * use few bytes. For constant-interval sequences the metadata value is zero.
 */
public final class DeltaCodecStage implements NumericCodecStage {

    /** Singleton instance. */
    public static final DeltaCodecStage INSTANCE = new DeltaCodecStage();

    private DeltaCodecStage() {}

    @Override
    public byte id() {
        return StageId.DELTA_STAGE.id;
    }

    @Override
    public void encode(final long[] values, final int valueCount, final EncodingContext context) {
        assert valueCount >= 1 : "valueCount must be at least 1";
        if (isMonotonic(values, valueCount) == false) {
            return;
        }

        for (int i = valueCount - 1; i > 0; i--) {
            values[i] -= values[i - 1];
        }
        final long first = values[0] - values[1];
        values[0] = values[1];

        context.metadata().writeZLong(first);
    }

    @Override
    public void decode(final long[] values, final int valueCount, final DecodingContext context) throws IOException {
        assert valueCount >= 1 : "valueCount must be at least 1";
        long sum = context.metadata().readZLong();
        for (int i = 0; i < valueCount; i++) {
            sum += values[i];
            values[i] = sum;
        }
    }

    public static void encodeStatic(final DeltaCodecStage stage, final long[] values, int valueCount, final EncodingContext context)
        throws IOException {
        stage.encode(values, valueCount, context);
    }

    public static void decodeStatic(final DeltaCodecStage stage, final long[] values, int valueCount, final DecodingContext context)
        throws IOException {
        stage.decode(values, valueCount, context);
    }

    private static boolean isMonotonic(final long[] values, final int valueCount) {
        int increases = 0;
        int decreases = 0;
        for (int i = 1; i < valueCount; i++) {
            increases += (values[i] > values[i - 1]) ? 1 : 0;
            decreases += (values[i] < values[i - 1]) ? 1 : 0;
        }
        return (increases >= 2 && decreases == 0) || (decreases >= 2 && increases == 0);
    }
}
