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

public final class ZigzagCodecStage implements NumericCodecStage {

    public static final ZigzagCodecStage INSTANCE = new ZigzagCodecStage();

    private ZigzagCodecStage() {}

    @Override
    public byte id() {
        return StageId.ZIGZAG.id;
    }

    @Override
    public String name() {
        return "zigzag";
    }

    @Override
    public int encode(long[] values, int valueCount, EncodingContext context) {
        for (int i = 0; i < valueCount; i++) {
            long v = values[i];
            values[i] = (v << 1) ^ (v >> 63);
        }

        final MetadataWriter meta = context.metadata();
        meta.empty();
        return valueCount;
    }

    @Override
    public int decode(long[] values, int valueCount, DecodingContext context) {
        for (int i = 0; i < valueCount; i++) {
            long v = values[i];
            values[i] = (v >>> 1) ^ -(v & 1);
        }
        return valueCount;
    }
}
