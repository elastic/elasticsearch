/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb.pipeline;

import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;
import org.elasticsearch.index.codec.tsdb.pipeline.numeric.PayloadDecoder;
import org.elasticsearch.index.codec.tsdb.pipeline.numeric.PayloadEncoder;

import java.io.IOException;

/**
 * Test-only payload stage that writes values as raw longs prefixed with a VInt count.
 *
 * <p>This stage is used by framework-level tests (e.g., {@link BlockFormatTests}) that need
 * a concrete payload stage without depending on production stage implementations (PR 2+).
 * ID {@code 0x00} is reserved for test-only stages and must never be persisted.
 */
public final class TestPayloadCodecStage implements PayloadEncoder, PayloadDecoder {

    /** Singleton instance. */
    public static final TestPayloadCodecStage INSTANCE = new TestPayloadCodecStage();

    private TestPayloadCodecStage() {}

    @Override
    public byte id() {
        throw new UnsupportedOperationException("Test-only stage cannot be persisted");
    }

    @Override
    public void encode(final long[] values, final int valueCount, final DataOutput out, final EncodingContext context) throws IOException {
        out.writeVInt(valueCount);
        for (int i = 0; i < valueCount; i++) {
            out.writeLong(values[i]);
        }
    }

    @Override
    public int decode(final long[] values, final DataInput in, final DecodingContext context) throws IOException {
        final int valueCount = in.readVInt();
        for (int i = 0; i < valueCount; i++) {
            values[i] = in.readLong();
        }
        return valueCount;
    }
}
