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
 * Offset removal transform stage.
 *
 * <h2>Effectiveness</h2>
 * <p>Applied when the minimum value is significant relative to the range. Skipped when:
 * <ul>
 *   <li>The range overflows ({@code max - min < 0} as unsigned)</li>
 *   <li>The minimum is already zero</li>
 *   <li>The absolute minimum is small relative to the absolute maximum
 *       ({@code |min| < |max| >>> 2}, compared unsigned)</li>
 * </ul>
 *
 * <h2>Example</h2>
 * <p>Values {@code [1000, 1050, 1100, 1150]} with min={@code 1000}
 * produces {@code [0, 50, 100, 150]}.
 *
 * <h2>Metadata layout</h2>
 * <p>Written to the stage metadata section (see {@link org.elasticsearch.index.codec.tsdb.pipeline.BlockFormat}):
 * <pre>
 *   +---------------------+
 *   | ZLong(min)          |  1-10 bytes, zigzag-encoded minimum value
 *   +---------------------+
 * </pre>
 * <p>Zigzag encoding is used because the minimum can be negative (e.g., after a
 * preceding delta stage produces negative deltas for a descending sequence).
 */
public final class OffsetCodecStage implements NumericCodecStage {

    /** Singleton instance. */
    public static final OffsetCodecStage INSTANCE = new OffsetCodecStage();

    private OffsetCodecStage() {}

    @Override
    public byte id() {
        return StageId.OFFSET_STAGE.id;
    }

    @Override
    public void encode(final long[] values, final int valueCount, final EncodingContext context) {
        assert valueCount >= 1 : "valueCount must be at least 1";

        long min = values[0];
        long max = values[0];
        for (int i = 1; i < valueCount; i++) {
            min = Math.min(min, values[i]);
            max = Math.max(max, values[i]);
        }

        if (max - min < 0) {
            return;
        }
        if (min == 0) {
            return;
        }
        // NOTE: skip when the minimum is small relative to the maximum. Uses unsigned
        // comparison so the heuristic works for both positive and negative ranges.
        final long absMin = min < 0 ? -min : min;
        final long absMax = max < 0 ? -max : max;
        if (Long.compareUnsigned(absMin, absMax >>> 2) < 0) {
            return;
        }

        for (int i = 0; i < valueCount; i++) {
            values[i] -= min;
        }

        context.metadata().writeZLong(min);
    }

    @Override
    public void decode(final long[] values, final int valueCount, final DecodingContext context) throws IOException {
        assert valueCount >= 1 : "valueCount must be at least 1";
        final long min = context.metadata().readZLong();
        for (int i = 0; i < valueCount; i++) {
            values[i] += min;
        }
    }

    public static void encodeStatic(final OffsetCodecStage stage, final long[] values, int valueCount, final EncodingContext context)
        throws IOException {
        stage.encode(values, valueCount, context);
    }

    public static void decodeStatic(final OffsetCodecStage stage, final long[] values, int valueCount, final DecodingContext context)
        throws IOException {
        stage.decode(values, valueCount, context);
    }
}
