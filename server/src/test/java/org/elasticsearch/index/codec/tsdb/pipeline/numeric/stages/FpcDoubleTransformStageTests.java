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
import org.apache.lucene.util.NumericUtils;
import org.elasticsearch.index.codec.tsdb.pipeline.EncodingContext;
import org.elasticsearch.index.codec.tsdb.pipeline.PipelineConfig;
import org.elasticsearch.index.codec.tsdb.pipeline.PipelineDescriptor;
import org.elasticsearch.index.codec.tsdb.pipeline.StageId;
import org.elasticsearch.index.codec.tsdb.pipeline.numeric.NumericBlockDecoder;
import org.elasticsearch.index.codec.tsdb.pipeline.numeric.NumericBlockEncoder;
import org.elasticsearch.index.codec.tsdb.pipeline.numeric.NumericDecoder;
import org.elasticsearch.index.codec.tsdb.pipeline.numeric.NumericEncoder;
import org.elasticsearch.index.codec.tsdb.pipeline.numeric.TransformDecoder;

import java.io.IOException;
import java.util.Arrays;

public class FpcDoubleTransformStageTests extends NumericCodecStageTestCase {

    private static final TransformDecoder DECODER = new FpcDoubleTransformDecodeStage(128 << 7);
    private static final int EXTRA_BUFFER_PER_VALUE = 2;

    public void testRoundTripConstantValues() throws IOException {
        final int blockSize = randomBlockSize();
        final long[] values = new long[blockSize];
        final long constant = NumericUtils.doubleToSortableLong(42.5);
        Arrays.fill(values, constant);
        assertRoundTrip(
            values,
            blockSize,
            StageId.FPC_DOUBLE_STAGE.id,
            new FpcDoubleTransformEncodeStage(blockSize),
            DECODER,
            blockSize * EXTRA_BUFFER_PER_VALUE
        );
    }

    public void testRoundTripLinearSequence() throws IOException {
        final int blockSize = randomBlockSize();
        final long[] values = new long[blockSize];
        for (int i = 0; i < blockSize; i++) {
            values[i] = NumericUtils.doubleToSortableLong(1.0 + i * 0.5);
        }
        assertRoundTrip(
            values,
            blockSize,
            StageId.FPC_DOUBLE_STAGE.id,
            new FpcDoubleTransformEncodeStage(blockSize),
            DECODER,
            blockSize * EXTRA_BUFFER_PER_VALUE
        );
    }

    public void testRoundTripSensorLikeData() throws IOException {
        final int blockSize = randomBlockSize();
        final long[] values = new long[blockSize];
        double base = 22.5;
        for (int i = 0; i < blockSize; i++) {
            double v = base + Math.round(randomDoubleBetween(-0.5, 0.5, true) * 10.0) / 10.0;
            values[i] = NumericUtils.doubleToSortableLong(v);
        }
        assertRoundTrip(
            values,
            blockSize,
            StageId.FPC_DOUBLE_STAGE.id,
            new FpcDoubleTransformEncodeStage(blockSize),
            DECODER,
            blockSize * EXTRA_BUFFER_PER_VALUE
        );
    }

    public void testRoundTripRandomDoubles() throws IOException {
        final int blockSize = randomBlockSize();
        final long[] values = new long[blockSize];
        for (int i = 0; i < blockSize; i++) {
            values[i] = NumericUtils.doubleToSortableLong(randomDoubleBetween(-1e6, 1e6, true));
        }
        assertRoundTrip(
            values,
            blockSize,
            StageId.FPC_DOUBLE_STAGE.id,
            new FpcDoubleTransformEncodeStage(blockSize),
            DECODER,
            blockSize * EXTRA_BUFFER_PER_VALUE
        );
    }

    public void testRoundTripSingleValue() throws IOException {
        final int blockSize = randomBlockSize();
        final long[] values = new long[] { NumericUtils.doubleToSortableLong(Math.PI) };
        assertRoundTrip(
            values,
            blockSize,
            StageId.FPC_DOUBLE_STAGE.id,
            new FpcDoubleTransformEncodeStage(blockSize),
            DECODER,
            blockSize * EXTRA_BUFFER_PER_VALUE
        );
    }

    public void testRoundTripAllZeros() throws IOException {
        final int blockSize = randomBlockSize();
        final long[] values = new long[blockSize];
        Arrays.fill(values, NumericUtils.doubleToSortableLong(0.0));
        assertRoundTrip(
            values,
            blockSize,
            StageId.FPC_DOUBLE_STAGE.id,
            new FpcDoubleTransformEncodeStage(blockSize),
            DECODER,
            blockSize * EXTRA_BUFFER_PER_VALUE
        );
    }

    public void testRoundTripCustomTableSize() throws IOException {
        final int blockSize = randomBlockSize();
        final long[] values = new long[blockSize];
        for (int i = 0; i < blockSize; i++) {
            values[i] = NumericUtils.doubleToSortableLong(10.0 + i * 0.01);
        }
        assertRoundTrip(
            values,
            blockSize,
            StageId.FPC_DOUBLE_STAGE.id,
            new FpcDoubleTransformEncodeStage(blockSize, 512),
            DECODER,
            blockSize * EXTRA_BUFFER_PER_VALUE
        );
    }

    public void testRepeatedFuzz() throws IOException {
        for (int iter = 0; iter < 50; iter++) {
            final int blockSize = randomBlockSize();
            final long[] values = new long[blockSize];
            for (int i = 0; i < blockSize; i++) {
                values[i] = NumericUtils.doubleToSortableLong(randomDoubleBetween(-1e12, 1e12, true));
            }
            assertRoundTrip(
                values,
                blockSize,
                StageId.FPC_DOUBLE_STAGE.id,
                new FpcDoubleTransformEncodeStage(blockSize),
                DECODER,
                blockSize * EXTRA_BUFFER_PER_VALUE
            );
        }
    }

    public void testFullPipelineRoundTrip() throws IOException {
        final int blockSize = randomBlockSize();
        final long[] values = new long[blockSize];
        for (int i = 0; i < blockSize; i++) {
            values[i] = NumericUtils.doubleToSortableLong(Math.round(randomDoubleBetween(0.01, 99.99, true) * 100.0) / 100.0);
        }
        final NumericEncoder encoder = NumericEncoder.fromConfig(PipelineConfig.forDoubles(blockSize).fpcStage().offset().gcd().bitPack());
        assertFullPipelineRoundTrip(values, encoder);
    }

    public void testFullPipelineRoundTripLinear() throws IOException {
        final int blockSize = randomBlockSize();
        final long[] values = new long[blockSize];
        for (int i = 0; i < blockSize; i++) {
            values[i] = NumericUtils.doubleToSortableLong(1.0 + i * 0.25);
        }
        final NumericEncoder encoder = NumericEncoder.fromConfig(PipelineConfig.forDoubles(blockSize).fpcStage().offset().gcd().bitPack());
        assertFullPipelineRoundTrip(values, encoder);
    }

    public void testRoundTripWithQuantization() throws IOException {
        final int blockSize = randomBlockSize();
        final double maxError = 0.01;
        final long[] values = new long[blockSize];
        for (int i = 0; i < blockSize; i++) {
            values[i] = NumericUtils.doubleToSortableLong(22.5 + randomDoubleBetween(-0.5, 0.5, true));
        }
        final FpcDoubleTransformEncodeStage encoder = new FpcDoubleTransformEncodeStage(
            blockSize,
            FpcDoubleTransformEncodeStage.DEFAULT_TABLE_SIZE,
            maxError
        );
        final long[] encoded = values.clone();
        final PipelineDescriptor pipeline = new PipelineDescriptor(
            new byte[] { StageId.FPC_DOUBLE_STAGE.id, TestPayloadCodecStage.TEST_STAGE_ID },
            blockSize
        );
        final EncodingContext context = new EncodingContext(blockSize, pipeline.pipelineLength());
        context.setValueCount(encoded.length);
        context.setCurrentPosition(0);
        encoder.encode(encoded, encoded.length, context);
        for (int i = 0; i < values.length; i++) {
            final double original = NumericUtils.sortableLongToDouble(values[i]);
            final double quantized = NumericUtils.sortableLongToDouble(encoded[i]);
            assertTrue(
                "quantized value should be within maxError: original=" + original + " quantized=" + quantized,
                Math.abs(original - quantized) <= maxError || encoded[i] != values[i]
            );
        }
    }

    public void testFullPipelineRoundTripWithQuantization() throws IOException {
        final int blockSize = randomBlockSize();
        final double maxError = 0.01;
        final long[] values = new long[blockSize];
        for (int i = 0; i < blockSize; i++) {
            values[i] = NumericUtils.doubleToSortableLong(Math.round(randomDoubleBetween(0.01, 99.99, true) * 100.0) / 100.0);
        }
        final NumericEncoder encoder = NumericEncoder.fromConfig(
            PipelineConfig.forDoubles(blockSize).fpcStage(maxError).offset().gcd().bitPack()
        );
        final long[] original = values.clone();
        final byte[] buffer = new byte[blockSize * Long.BYTES * 4 + 4096];
        final ByteArrayDataOutput out = new ByteArrayDataOutput(buffer);

        final NumericBlockEncoder blockEncoder = encoder.newBlockEncoder();
        blockEncoder.encode(values, values.length, out);

        final NumericDecoder decoder = NumericDecoder.fromDescriptor(encoder.descriptor());
        final NumericBlockDecoder blockDecoder = decoder.newBlockDecoder();
        final long[] decoded = new long[blockSize];
        final ByteArrayDataInput in = new ByteArrayDataInput(buffer, 0, out.getPosition());
        blockDecoder.decode(decoded, in);

        for (int i = 0; i < original.length; i++) {
            final double orig = NumericUtils.sortableLongToDouble(original[i]);
            final double dec = NumericUtils.sortableLongToDouble(decoded[i]);
            assertTrue(
                "decoded value should be within maxError: original=" + orig + " decoded=" + dec,
                Math.abs(orig - dec) <= maxError + 1e-9
            );
        }
    }

    public void testDoubleRoundTripIntegerLikeValues() throws IOException {
        final int blockSize = randomBlockSize();
        final long[] values = new long[blockSize];
        for (int i = 0; i < blockSize; i++) {
            values[i] = NumericUtils.doubleToSortableLong((double) randomIntBetween(0, 10000));
        }
        assertRoundTrip(
            values,
            blockSize,
            StageId.FPC_DOUBLE_STAGE.id,
            new FpcDoubleTransformEncodeStage(blockSize),
            DECODER,
            blockSize * EXTRA_BUFFER_PER_VALUE
        );
    }

    public void testDoubleRoundTripNegativeValues() throws IOException {
        final int blockSize = randomBlockSize();
        final long[] values = new long[blockSize];
        for (int i = 0; i < blockSize; i++) {
            values[i] = NumericUtils.doubleToSortableLong(-Math.abs(randomDoubleBetween(0.01, 1e6, true)));
        }
        assertRoundTrip(
            values,
            blockSize,
            StageId.FPC_DOUBLE_STAGE.id,
            new FpcDoubleTransformEncodeStage(blockSize),
            DECODER,
            blockSize * EXTRA_BUFFER_PER_VALUE
        );
    }

    public void testDoubleRoundTripSpecialValues() throws IOException {
        final int blockSize = randomBlockSize();
        final long[] values = new long[blockSize];
        for (int i = 0; i < blockSize; i++) {
            values[i] = NumericUtils.doubleToSortableLong(randomDoubleBetween(0.01, 99.99, true));
        }
        values[0] = NumericUtils.doubleToSortableLong(Double.MAX_VALUE);
        values[1] = NumericUtils.doubleToSortableLong(Double.MIN_VALUE);
        values[2] = NumericUtils.doubleToSortableLong(-0.0);
        values[3] = NumericUtils.doubleToSortableLong(Double.MIN_NORMAL);
        assertRoundTrip(
            values,
            blockSize,
            StageId.FPC_DOUBLE_STAGE.id,
            new FpcDoubleTransformEncodeStage(blockSize),
            DECODER,
            blockSize * EXTRA_BUFFER_PER_VALUE
        );
    }

    public void testDoubleRoundTripSubnormals() throws IOException {
        final int blockSize = randomBlockSize();
        final long[] values = new long[blockSize];
        for (int i = 0; i < blockSize; i++) {
            values[i] = NumericUtils.doubleToSortableLong(Double.MIN_VALUE * (i + 1));
        }
        assertRoundTrip(
            values,
            blockSize,
            StageId.FPC_DOUBLE_STAGE.id,
            new FpcDoubleTransformEncodeStage(blockSize),
            DECODER,
            blockSize * EXTRA_BUFFER_PER_VALUE
        );
    }

    // NOTE: Build values with a truly constant stride in IEEE-754 bit-space.
    // Using doubleToSortableLong(20.5 + i*0.1) doesn't work because FP rounding
    // makes the bit-level stride vary (e.g. 5*0.1 = 0.5 exact, but 4*0.1 is not).
    // Instead, construct raw IEEE-754 bits directly: base + i * stride.
    public void testDfcmStridesPrediction() throws IOException {
        final int blockSize = 128;
        final long rawBase = Double.doubleToRawLongBits(100.0);
        final long rawStride = 0x100;
        final long[] values = new long[blockSize];
        for (int i = 0; i < blockSize; i++) {
            values[i] = NumericUtils.doubleToSortableLong(Double.longBitsToDouble(rawBase + (long) i * rawStride));
        }
        final long[] original = values.clone();
        final FpcDoubleTransformEncodeStage encoder = new FpcDoubleTransformEncodeStage(blockSize);
        final PipelineDescriptor pipeline = new PipelineDescriptor(
            new byte[] { StageId.FPC_DOUBLE_STAGE.id, TestPayloadCodecStage.TEST_STAGE_ID },
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
            StageId.FPC_DOUBLE_STAGE.id,
            new FpcDoubleTransformEncodeStage(blockSize),
            DECODER,
            blockSize * EXTRA_BUFFER_PER_VALUE
        );
    }
}
