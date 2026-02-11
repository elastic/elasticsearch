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
import org.elasticsearch.index.codec.tsdb.pipeline.StageId;
import org.elasticsearch.index.codec.tsdb.pipeline.numeric.TransformDecoder;
import org.elasticsearch.index.codec.tsdb.pipeline.numeric.TransformEncoder;

import java.io.IOException;

public enum GcdCodecStage implements TransformEncoder, TransformDecoder {
    INSTANCE;

    @Override
    public byte id() {
        return StageId.GCD.id;
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

        // NOTE: Metadata layout: [gcd-2: VLong].
        // GCD is always >= 2 when the stage applies (0 and 1 are skipped), so
        // subtracting 2 makes the common case (small GCDs) fit in fewer bytes.
        context.metadata().writeVLong(gcd - 2);
        return valueCount;
    }

    public static int encodeStatic(final GcdCodecStage stage, final long[] values, int valueCount, final EncodingContext context)
        throws IOException {
        return stage.encode(values, valueCount, context);
    }

    public static int decodeStatic(final GcdCodecStage stage, final long[] values, int valueCount, final DecodingContext context)
        throws IOException {
        return stage.decode(values, valueCount, context);
    }

    @Override
    public int decode(final long[] values, int valueCount, final DecodingContext context) throws IOException {
        // NOTE: Metadata layout: [gcd-2: VLong].
        long gcd = context.metadata().readVLong() + 2;
        for (int i = 0; i < valueCount; i++) {
            values[i] *= gcd;
        }
        return valueCount;
    }

    @Override
    public String toString() {
        return "GcdCodecStage";
    }
}
