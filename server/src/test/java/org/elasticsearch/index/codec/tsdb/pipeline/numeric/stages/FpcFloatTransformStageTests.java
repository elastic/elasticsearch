/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb.pipeline.numeric.stages;

import org.apache.lucene.util.NumericUtils;
import org.elasticsearch.index.codec.tsdb.pipeline.EncodingContext;
import org.elasticsearch.index.codec.tsdb.pipeline.PipelineConfig;
import org.elasticsearch.index.codec.tsdb.pipeline.PipelineDescriptor;
import org.elasticsearch.index.codec.tsdb.pipeline.StageId;
import org.elasticsearch.index.codec.tsdb.pipeline.numeric.NumericEncoder;
import org.elasticsearch.index.codec.tsdb.pipeline.numeric.TransformDecoder;

import java.io.IOException;
import java.util.Arrays;

public class FpcFloatTransformStageTests extends NumericCodecStageTestCase {

    private static final TransformDecoder DECODER = new FpcFloatTransformDecodeStage(128 << 7);
    private static final int EXTRA_BUFFER_PER_VALUE = 2;

    private static long floatToSortableLong(float f) {
        return NumericUtils.floatToSortableInt(f) & 0xFFFFFFFFL;
    }

    public void testFloatRoundTripConstantValues() throws IOException {
        final int blockSize = randomBlockSize();
        final long[] values = new long[blockSize];
        final long constant = floatToSortableLong(42.5f);
        Arrays.fill(values, constant);
        assertRoundTrip(
            values,
            blockSize,
            StageId.FPC_FLOAT_STAGE.id,
            new FpcFloatTransformEncodeStage(blockSize),
            DECODER,
            blockSize * EXTRA_BUFFER_PER_VALUE
        );
    }

    public void testFloatRoundTripLinearSequence() throws IOException {
        final int blockSize = randomBlockSize();
        final long[] values = new long[blockSize];
        for (int i = 0; i < blockSize; i++) {
            values[i] = floatToSortableLong(1.0f + i * 0.5f);
        }
        assertRoundTrip(
            values,
            blockSize,
            StageId.FPC_FLOAT_STAGE.id,
            new FpcFloatTransformEncodeStage(blockSize),
            DECODER,
            blockSize * EXTRA_BUFFER_PER_VALUE
        );
    }

    public void testFloatRoundTripSensorLikeData() throws IOException {
        final int blockSize = randomBlockSize();
        final long[] values = new long[blockSize];
        for (int i = 0; i < blockSize; i++) {
            float v = 22.5f + Math.round(randomFloat() * 10.0f - 5.0f) / 10.0f;
            values[i] = floatToSortableLong(v);
        }
        assertRoundTrip(
            values,
            blockSize,
            StageId.FPC_FLOAT_STAGE.id,
            new FpcFloatTransformEncodeStage(blockSize),
            DECODER,
            blockSize * EXTRA_BUFFER_PER_VALUE
        );
    }

    public void testFloatRoundTripRandomValues() throws IOException {
        final int blockSize = randomBlockSize();
        final long[] values = new long[blockSize];
        for (int i = 0; i < blockSize; i++) {
            values[i] = floatToSortableLong(randomFloat() * 2e6f - 1e6f);
        }
        assertRoundTrip(
            values,
            blockSize,
            StageId.FPC_FLOAT_STAGE.id,
            new FpcFloatTransformEncodeStage(blockSize),
            DECODER,
            blockSize * EXTRA_BUFFER_PER_VALUE
        );
    }

    public void testFloatRoundTripMixedPositiveNegative() throws IOException {
        final int blockSize = randomBlockSize();
        final long[] values = new long[blockSize];
        for (int i = 0; i < blockSize; i++) {
            float v = Math.round((randomFloat() * 200.0f - 100.0f) * 100.0f) / 100.0f;
            values[i] = floatToSortableLong(v);
        }
        assertRoundTrip(
            values,
            blockSize,
            StageId.FPC_FLOAT_STAGE.id,
            new FpcFloatTransformEncodeStage(blockSize),
            DECODER,
            blockSize * EXTRA_BUFFER_PER_VALUE
        );
    }

    public void testFloatRepeatedFuzz() throws IOException {
        for (int iter = 0; iter < 50; iter++) {
            final int blockSize = randomBlockSize();
            final long[] values = new long[blockSize];
            for (int i = 0; i < blockSize; i++) {
                values[i] = floatToSortableLong(randomFloat() * 2e6f - 1e6f);
            }
            assertRoundTrip(
                values,
                blockSize,
                StageId.FPC_FLOAT_STAGE.id,
                new FpcFloatTransformEncodeStage(blockSize),
                DECODER,
                blockSize * EXTRA_BUFFER_PER_VALUE
            );
        }
    }

    public void testFloatFullPipelineRoundTrip() throws IOException {
        final int blockSize = randomBlockSize();
        final long[] values = new long[blockSize];
        for (int i = 0; i < blockSize; i++) {
            values[i] = floatToSortableLong(Math.round(randomFloat() * 10000.0f) / 100.0f);
        }
        final NumericEncoder encoder = NumericEncoder.fromConfig(PipelineConfig.forFloats(blockSize).fpcStage().offset().gcd().bitPack());
        assertFullPipelineRoundTrip(values, encoder);
    }

    public void testFloatFullPipelineRoundTripLinear() throws IOException {
        final int blockSize = randomBlockSize();
        final long[] values = new long[blockSize];
        for (int i = 0; i < blockSize; i++) {
            values[i] = floatToSortableLong(1.0f + i * 0.25f);
        }
        final NumericEncoder encoder = NumericEncoder.fromConfig(PipelineConfig.forFloats(blockSize).fpcStage().offset().gcd().bitPack());
        assertFullPipelineRoundTrip(values, encoder);
    }

    public void testFloatRoundTripSpecialValues() throws IOException {
        final int blockSize = randomBlockSize();
        final long[] values = new long[blockSize];
        for (int i = 0; i < blockSize; i++) {
            values[i] = floatToSortableLong(randomFloat() * 100.0f);
        }
        values[0] = floatToSortableLong(Float.MAX_VALUE);
        values[1] = floatToSortableLong(Float.MIN_VALUE);
        values[2] = floatToSortableLong(-0.0f);
        values[3] = floatToSortableLong(Float.MIN_NORMAL);
        assertRoundTrip(
            values,
            blockSize,
            StageId.FPC_FLOAT_STAGE.id,
            new FpcFloatTransformEncodeStage(blockSize),
            DECODER,
            blockSize * EXTRA_BUFFER_PER_VALUE
        );
    }

    public void testFloatRoundTripIntegerLikeValues() throws IOException {
        final int blockSize = randomBlockSize();
        final long[] values = new long[blockSize];
        for (int i = 0; i < blockSize; i++) {
            values[i] = floatToSortableLong((float) randomIntBetween(0, 10000));
        }
        assertRoundTrip(
            values,
            blockSize,
            StageId.FPC_FLOAT_STAGE.id,
            new FpcFloatTransformEncodeStage(blockSize),
            DECODER,
            blockSize * EXTRA_BUFFER_PER_VALUE
        );
    }

    // NOTE: Build values with a truly constant stride in IEEE-754 bit-space.
    // Using floatToSortableInt(20.5f + i*0.25f) doesn't work because FP rounding
    // can make the bit-level stride vary. Instead, construct raw IEEE-754 bits
    // directly: base + i * stride.
    public void testDfcmStridesPrediction() throws IOException {
        final int blockSize = 128;
        final int rawBase = Float.floatToRawIntBits(100.0f);
        final int rawStride = 0x10;
        final long[] values = new long[blockSize];
        for (int i = 0; i < blockSize; i++) {
            values[i] = floatToSortableLong(Float.intBitsToFloat(rawBase + i * rawStride));
        }
        final long[] original = values.clone();
        final FpcFloatTransformEncodeStage encoder = new FpcFloatTransformEncodeStage(blockSize);
        final PipelineDescriptor pipeline = new PipelineDescriptor(
            new byte[] { StageId.FPC_FLOAT_STAGE.id, TestPayloadCodecStage.TEST_STAGE_ID },
            blockSize
        );
        final EncodingContext context = new EncodingContext(blockSize, pipeline.pipelineLength());
        context.setValueCount(values.length);
        context.setCurrentPosition(0);
        encoder.encode(values, values.length, context);

        // NOTE: after encoding, values[0] should be 0 (first value stored in metadata).
        // From i=3 onward, DFCM should predict the constant stride correctly,
        // producing near-zero residuals (< 8 bits).
        assertEquals(0L, values[0]);
        for (int i = 3; i < blockSize; i++) {
            assertTrue(
                "residual at i=" + i + " should be small (< 8 bits), got " + Long.toBinaryString(values[i]),
                Long.numberOfLeadingZeros(values[i]) >= 56
            );
        }

        // Verify round-trip
        assertRoundTrip(
            original,
            blockSize,
            StageId.FPC_FLOAT_STAGE.id,
            new FpcFloatTransformEncodeStage(blockSize),
            DECODER,
            blockSize * EXTRA_BUFFER_PER_VALUE
        );
    }
}
