/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb.pipeline.numeric.stages;

import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.store.ByteArrayDataOutput;
import org.elasticsearch.index.codec.tsdb.pipeline.BlockFormat;
import org.elasticsearch.index.codec.tsdb.pipeline.DecodingContext;
import org.elasticsearch.index.codec.tsdb.pipeline.EncodingContext;
import org.elasticsearch.index.codec.tsdb.pipeline.PipelineDescriptor;
import org.elasticsearch.index.codec.tsdb.pipeline.numeric.NumericCodecStage;

import java.io.IOException;
import java.util.Arrays;

public abstract class NumericCodecStageTestCase extends CodecStageTestCase {

    protected static final byte PAYLOAD_STAGE = TestPayloadCodecStage.TEST_STAGE_ID;

    protected void assertRoundTrip(long[] original, int blockSize, byte stageId, NumericCodecStage stage) throws IOException {
        assertRoundTrip(original, blockSize, stageId, stage, 64);
    }

    protected void assertRoundTrip(long[] original, int blockSize, byte stageId, NumericCodecStage stage, int extraBuffer)
        throws IOException {
        final long[] values = original.clone();
        final PipelineDescriptor pipeline = new PipelineDescriptor(new byte[] { stageId, PAYLOAD_STAGE }, blockSize);
        final EncodingContext encodingContext = new EncodingContext(blockSize, pipeline.pipelineLength());
        encodingContext.setValueCount(values.length);
        encodingContext.setCurrentPosition(0);

        stage.encode(values, values.length, encodingContext);

        final boolean stageApplied = Arrays.equals(original, values) == false;
        if (stageApplied == false) {
            return;
        }

        encodingContext.setCurrentPosition(1);
        encodingContext.applyStage(1);

        final byte[] buffer = new byte[original.length * Long.BYTES + extraBuffer];
        final ByteArrayDataOutput out = new ByteArrayDataOutput(buffer);
        BlockFormat.writeBlock(out, values, TestPayloadCodecStage.INSTANCE, encodingContext);

        final DecodingContext decodingContext = new DecodingContext(blockSize, pipeline.stageIds());
        final ByteArrayDataInput in = new ByteArrayDataInput(buffer, 0, out.getPosition());
        decodingContext.setDataInput(in);
        BlockFormat.readBlock(in, values, TestPayloadCodecStage.INSTANCE, decodingContext, 1);

        stage.decode(values, values.length, decodingContext);

        assertArrayEquals(original, values);
    }

    protected EncodingContext createEncodingContext(int blockSize, byte stageId) {
        final PipelineDescriptor pipeline = new PipelineDescriptor(new byte[] { stageId, PAYLOAD_STAGE }, blockSize);
        final EncodingContext context = new EncodingContext(blockSize, pipeline.pipelineLength());
        context.setCurrentPosition(0);
        return context;
    }

    protected void assertStageSkipped(long[] original, int blockSize, byte stageId, NumericCodecStage stage) throws IOException {
        final long[] values = original.clone();
        final EncodingContext encodingContext = createEncodingContext(blockSize, stageId);
        stage.encode(values, values.length, encodingContext);
        assertArrayEquals(original, values);
        assertFalse(encodingContext.isStageApplied(0));
    }
}
