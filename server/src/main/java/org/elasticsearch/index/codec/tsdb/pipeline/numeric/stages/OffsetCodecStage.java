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

public final class OffsetCodecStage implements NumericCodecStage {

    private static final int DEFAULT_MIN_OFFSET_RATIO_PERCENT = 25;
    public static final OffsetCodecStage INSTANCE = new OffsetCodecStage();

    private final int minOffsetRatioPercent;

    public OffsetCodecStage() {
        this(DEFAULT_MIN_OFFSET_RATIO_PERCENT);
    }

    public OffsetCodecStage(int minOffsetRatioPercent) {
        if (minOffsetRatioPercent < 1 || minOffsetRatioPercent > 99) {
            throw new IllegalArgumentException("minOffsetRatioPercent must be in [1, 99]: " + minOffsetRatioPercent);
        }
        this.minOffsetRatioPercent = minOffsetRatioPercent;
    }

    @Override
    public byte id() {
        return StageId.OFFSET.id;
    }

    @Override
    public String name() {
        return "offset";
    }

    @Override
    public int encode(final long[] values, int valueCount, final EncodingContext context) throws IOException {
        if (valueCount == 0) {
            return valueCount;
        }

        long min = values[0];
        long max = values[0];
        for (int i = 1; i < valueCount; i++) {
            final long v = values[i];
            if (v < min) min = v;
            if (v > max) max = v;
        }

        if (max - min < 0) {
            return valueCount;
        }
        if (min == 0) {
            return valueCount;
        }
        if (min > 0 && min < computeThreshold(max)) {
            return valueCount;
        }

        for (int i = 0; i < valueCount; i++) {
            values[i] -= min;
        }

        context.metadata().writeZLong(min);
        return valueCount;
    }

    @Override
    public int decode(long[] values, int valueCount, DecodingContext context) throws IOException {
        long min = context.metadata().readZLong();
        for (int i = 0; i < valueCount; i++) {
            values[i] += min;
        }
        return valueCount;
    }

    private long computeThreshold(long max) {
        if (minOffsetRatioPercent == DEFAULT_MIN_OFFSET_RATIO_PERCENT) {
            return max >>> 2;
        }
        if (max > Long.MAX_VALUE / minOffsetRatioPercent) {
            return (max / 100) * minOffsetRatioPercent;
        }
        return (max * minOffsetRatioPercent) / 100;
    }
}
