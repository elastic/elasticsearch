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
import org.elasticsearch.index.codec.tsdb.pipeline.MetadataWriter;
import org.elasticsearch.index.codec.tsdb.pipeline.StageId;
import org.elasticsearch.index.codec.tsdb.pipeline.numeric.TransformDecoder;
import org.elasticsearch.index.codec.tsdb.pipeline.numeric.TransformEncoder;

import java.io.IOException;

public final class DeltaCodecStage implements TransformEncoder, TransformDecoder {

    private static final int DEFAULT_MIN_DIRECTIONAL_CHANGES = 2;
    public static final DeltaCodecStage INSTANCE = new DeltaCodecStage();

    private final int minDirectionalChanges;

    public DeltaCodecStage() {
        this(DEFAULT_MIN_DIRECTIONAL_CHANGES);
    }

    public DeltaCodecStage(int minDirectionalChanges) {
        assert minDirectionalChanges >= 1 : "minDirectionalChanges must be >= 1: " + minDirectionalChanges;
        this.minDirectionalChanges = minDirectionalChanges;
    }

    @Override
    public byte id() {
        return StageId.DELTA.id;
    }

    @Override
    public int encode(final long[] values, int valueCount, final EncodingContext context) throws IOException {
        if (valueCount < 2) {
            return valueCount;
        }

        int gts = 0;
        int lts = 0;
        for (int i = 1; i < valueCount; i++) {
            if (values[i] > values[i - 1]) {
                gts++;
            } else if (values[i] < values[i - 1]) {
                lts++;
            }
        }

        final boolean doDeltaCompression = (gts == 0 && lts >= minDirectionalChanges) || (lts == 0 && gts >= minDirectionalChanges);
        if (doDeltaCompression == false) {
            return valueCount;
        }

        for (int i = valueCount - 1; i > 0; i--) {
            values[i] -= values[i - 1];
        }

        final long first = values[0] - values[1];
        values[0] = values[1];

        // NOTE: Metadata layout: [first: ZLong].
        // The original first value minus the first delta, stored as zigzag-encoded
        // variable-length long so negative values are compact.
        final MetadataWriter meta = context.metadata();
        meta.writeZLong(first);
        return valueCount;
    }

    public static int encodeStatic(final DeltaCodecStage stage, final long[] values, int valueCount, final EncodingContext context)
        throws IOException {
        return stage.encode(values, valueCount, context);
    }

    public static int decodeStatic(final DeltaCodecStage stage, final long[] values, int valueCount, final DecodingContext context)
        throws IOException {
        return stage.decode(values, valueCount, context);
    }

    @Override
    public int decode(final long[] values, int valueCount, final DecodingContext context) throws IOException {
        // NOTE: Metadata layout: [first: ZLong].
        long first = context.metadata().readZLong();
        values[0] += first;
        for (int i = 1; i < valueCount; i++) {
            values[i] += values[i - 1];
        }
        return valueCount;
    }

    @Override
    public boolean equals(Object o) {
        return this == o || (o instanceof DeltaCodecStage that && minDirectionalChanges == that.minDirectionalChanges);
    }

    @Override
    public int hashCode() {
        return Integer.hashCode(minDirectionalChanges);
    }

    @Override
    public String toString() {
        return "DeltaCodecStage{minDirectionalChanges=" + minDirectionalChanges + "}";
    }
}
