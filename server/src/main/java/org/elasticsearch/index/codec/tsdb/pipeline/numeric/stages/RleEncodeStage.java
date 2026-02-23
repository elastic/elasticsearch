/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb.pipeline.numeric.stages;

import org.elasticsearch.index.codec.tsdb.pipeline.EncodingContext;
import org.elasticsearch.index.codec.tsdb.pipeline.MetadataWriter;
import org.elasticsearch.index.codec.tsdb.pipeline.StageId;
import org.elasticsearch.index.codec.tsdb.pipeline.numeric.TransformEncoder;

import java.io.IOException;

// NOTE: RLE transform is broken — it reduces valueCount but the downstream BitPack stage
// always packs blockSize values (zero-filling the tail), so the payload size never decreases.
// RLE metadata (runCount + valueCount + run lengths) is pure overhead. Additionally, the skip
// threshold (0.90 run ratio) is far too generous, causing RLE to fire on blocks with minimal
// repetition. Kept for backward compatibility with existing encoded segments.
@Deprecated
public final class RleEncodeStage implements TransformEncoder {

    private static final double DEFAULT_MAX_RUN_RATIO_THRESHOLD = 0.90;

    private final double maxRunRatioThreshold;
    private final long[] scratchValues;

    public RleEncodeStage(int blockSize) {
        this(blockSize, DEFAULT_MAX_RUN_RATIO_THRESHOLD);
    }

    public RleEncodeStage(int blockSize, double maxRunRatioThreshold) {
        if (maxRunRatioThreshold <= 0.0 || maxRunRatioThreshold > 1.0) {
            throw new IllegalArgumentException("maxRunRatioThreshold must be in (0.0, 1.0]: " + maxRunRatioThreshold);
        }
        this.maxRunRatioThreshold = maxRunRatioThreshold;
        this.scratchValues = new long[blockSize];
    }

    @Override
    public byte id() {
        return StageId.RLE.id;
    }

    @Override
    public int maxMetadataBytes(int blockSize) {
        int maxRuns = (int) (blockSize * maxRunRatioThreshold);
        return 10 + maxRuns * 5;
    }

    // NOTE: Metadata layout: [runCount: VInt] [valueCount: VInt]
    // then [runCount × runLength: VInt] written in reverse order.
    // Run lengths are reversed so the decoder can expand right-to-left in-place
    // without an extra buffer — each run is expanded from the end of the values
    // array backward, never overwriting unread data.
    // Values array is compacted to unique run values (deduplicated, in order).
    @Override
    public int encode(final long[] values, int valueCount, final EncodingContext context) throws IOException {
        if (valueCount == 0) {
            return valueCount;
        }

        final long[] scratch = scratchValues;
        int runCount = 1;
        int currentRunLength = 1;
        for (int i = 1; i < valueCount; i++) {
            if (values[i] == values[i - 1]) {
                currentRunLength++;
            } else {
                scratch[runCount - 1] = currentRunLength;
                runCount++;
                currentRunLength = 1;
            }
        }
        scratch[runCount - 1] = currentRunLength;

        final double runRatio = (double) runCount / valueCount;
        if (runRatio > maxRunRatioThreshold) {
            return valueCount;
        }

        int writePos = 0;
        values[writePos++] = values[0];
        for (int i = 1; i < valueCount; i++) {
            if (values[i] != values[i - 1]) {
                values[writePos++] = values[i];
            }
        }

        final MetadataWriter meta = context.metadata();
        meta.writeVInt(runCount);
        meta.writeVInt(valueCount);
        for (int i = runCount - 1; i >= 0; i--) {
            meta.writeVInt((int) scratch[i]);
        }

        return runCount;
    }

    public static int encodeStatic(final RleEncodeStage stage, final long[] values, int valueCount, final EncodingContext context)
        throws IOException {
        return stage.encode(values, valueCount, context);
    }

    @Override
    public boolean equals(Object o) {
        return this == o || (o instanceof RleEncodeStage that && Double.compare(maxRunRatioThreshold, that.maxRunRatioThreshold) == 0);
    }

    @Override
    public int hashCode() {
        return Double.hashCode(maxRunRatioThreshold);
    }

    @Override
    public String toString() {
        return "RleEncodeStage{maxRunRatioThreshold=" + maxRunRatioThreshold + "}";
    }
}
