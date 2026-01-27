/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb.pipeline.numeric.stages;

import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;
import org.elasticsearch.index.codec.tsdb.pipeline.DecodingContext;
import org.elasticsearch.index.codec.tsdb.pipeline.EncodingContext;
import org.elasticsearch.index.codec.tsdb.pipeline.numeric.PayloadCodecStage;

import java.io.IOException;

public final class TestPayloadCodecStage implements PayloadCodecStage {

    // Test-only ID for pipeline descriptors - see StageId for reserved ID documentation
    public static final byte TEST_STAGE_ID = (byte) 0x00;

    public static final TestPayloadCodecStage INSTANCE = new TestPayloadCodecStage();

    private TestPayloadCodecStage() {}

    @Override
    public byte id() {
        throw new UnsupportedOperationException("Test-only stage cannot be persisted");
    }

    @Override
    public String name() {
        return "test";
    }

    @Override
    public void encode(long[] values, int valueCount, DataOutput out, EncodingContext context) throws IOException {
        out.writeVInt(valueCount);
        for (int i = 0; i < valueCount; i++) {
            out.writeLong(values[i]);
        }
    }

    @Override
    public int decode(long[] values, DataInput in, DecodingContext context) throws IOException {
        int valueCount = in.readVInt();
        for (int i = 0; i < valueCount; i++) {
            values[i] = in.readLong();
        }
        return valueCount;
    }
}
