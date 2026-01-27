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
import org.elasticsearch.index.codec.tsdb.pipeline.numeric.NumericCodecStage;

import java.io.IOException;

public final class DeltaCodecStage implements NumericCodecStage {

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
    public String name() {
        return "delta";
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

        final MetadataWriter meta = context.metadata();
        meta.writeZLong(first);
        return valueCount;
    }

    @Override
    public int decode(long[] values, int valueCount, DecodingContext context) throws IOException {
        long first = context.metadata().readZLong();
        values[0] += first;
        for (int i = 1; i < valueCount; i++) {
            values[i] += values[i - 1];
        }
        return valueCount;
    }
}
