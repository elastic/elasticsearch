/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb.pipeline.numeric.stages;

import org.apache.lucene.util.MathUtil;
import org.elasticsearch.index.codec.tsdb.pipeline.DecodingContext;
import org.elasticsearch.index.codec.tsdb.pipeline.EncodingContext;
import org.elasticsearch.index.codec.tsdb.pipeline.MetadataWriter;
import org.elasticsearch.index.codec.tsdb.pipeline.StageId;
import org.elasticsearch.index.codec.tsdb.pipeline.numeric.NumericCodecStage;

import java.io.IOException;

public final class GcdCodecStage implements NumericCodecStage {

    public static final GcdCodecStage INSTANCE = new GcdCodecStage();

    GcdCodecStage() {}

    @Override
    public byte id() {
        return StageId.GCD.id;
    }

    @Override
    public String name() {
        return "gcd";
    }

    @Override
    public int encode(final long[] values, int valueCount, final EncodingContext context) throws IOException {
        long gcd = 0;
        for (int i = 0; i < valueCount; i++) {
            gcd = MathUtil.gcd(gcd, values[i]);
            if (gcd == 1) {
                return valueCount;
            }
        }

        if (Long.compareUnsigned(gcd, 1) <= 0) {
            return valueCount;
        }

        for (int i = 0; i < valueCount; i++) {
            values[i] /= gcd;
        }

        final MetadataWriter meta = context.metadata();
        meta.writeVLong(gcd - 2);
        return valueCount;
    }

    @Override
    public int decode(long[] values, int valueCount, DecodingContext context) throws IOException {
        long gcd = context.metadata().readVLong() + 2;
        for (int i = 0; i < valueCount; i++) {
            values[i] *= gcd;
        }
        return valueCount;
    }
}
