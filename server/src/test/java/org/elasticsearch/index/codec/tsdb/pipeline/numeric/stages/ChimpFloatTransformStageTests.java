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
import org.elasticsearch.index.codec.tsdb.pipeline.PipelineConfig;
import org.elasticsearch.index.codec.tsdb.pipeline.StageId;
import org.elasticsearch.index.codec.tsdb.pipeline.numeric.NumericEncoder;
import org.elasticsearch.index.codec.tsdb.pipeline.numeric.TransformDecoder;

import java.io.IOException;
import java.util.Arrays;

public class ChimpFloatTransformStageTests extends NumericCodecStageTestCase {

    private static final TransformDecoder DECODER = new ChimpFloatTransformDecodeStage();
    private static final int EXTRA_BUFFER_PER_VALUE = 4;

    private static long floatToSortableLong(float f) {
        return NumericUtils.floatToSortableInt(f) & 0xFFFFFFFFL;
    }

    public void testRoundTripConstantValues() throws IOException {
        final int blockSize = randomBlockSize();
        final long[] values = new long[blockSize];
        final long constant = floatToSortableLong(42.5f);
        Arrays.fill(values, constant);
        assertRoundTrip(
            values,
            blockSize,
            StageId.CHIMP_FLOAT_STAGE.id,
            new ChimpFloatTransformEncodeStage(blockSize),
            DECODER,
            blockSize * EXTRA_BUFFER_PER_VALUE
        );
    }

    public void testRoundTripLinearSequence() throws IOException {
        final int blockSize = randomBlockSize();
        final long[] values = new long[blockSize];
        for (int i = 0; i < blockSize; i++) {
            values[i] = floatToSortableLong(1.0f + i * 0.5f);
        }
        assertRoundTrip(
            values,
            blockSize,
            StageId.CHIMP_FLOAT_STAGE.id,
            new ChimpFloatTransformEncodeStage(blockSize),
            DECODER,
            blockSize * EXTRA_BUFFER_PER_VALUE
        );
    }

    public void testRoundTripSensorLikeData() throws IOException {
        final int blockSize = randomBlockSize();
        final long[] values = new long[blockSize];
        float base = 22.5f;
        for (int i = 0; i < blockSize; i++) {
            float v = base + Math.round(randomDoubleBetween(-0.5, 0.5, true) * 10.0f) / 10.0f;
            values[i] = floatToSortableLong(v);
        }
        assertRoundTrip(
            values,
            blockSize,
            StageId.CHIMP_FLOAT_STAGE.id,
            new ChimpFloatTransformEncodeStage(blockSize),
            DECODER,
            blockSize * EXTRA_BUFFER_PER_VALUE
        );
    }

    public void testRoundTripRandomFloats() throws IOException {
        final int blockSize = randomBlockSize();
        final long[] values = new long[blockSize];
        for (int i = 0; i < blockSize; i++) {
            values[i] = floatToSortableLong((float) randomDoubleBetween(-1e6, 1e6, true));
        }
        assertRoundTrip(
            values,
            blockSize,
            StageId.CHIMP_FLOAT_STAGE.id,
            new ChimpFloatTransformEncodeStage(blockSize),
            DECODER,
            blockSize * EXTRA_BUFFER_PER_VALUE
        );
    }

    public void testRoundTripSingleValue() throws IOException {
        final int blockSize = randomBlockSize();
        final long[] values = new long[] { floatToSortableLong((float) Math.PI) };
        assertRoundTrip(
            values,
            blockSize,
            StageId.CHIMP_FLOAT_STAGE.id,
            new ChimpFloatTransformEncodeStage(blockSize),
            DECODER,
            blockSize * EXTRA_BUFFER_PER_VALUE
        );
    }

    public void testRoundTripAllZeros() throws IOException {
        final int blockSize = randomBlockSize();
        final long[] values = new long[blockSize];
        Arrays.fill(values, floatToSortableLong(0.0f));
        assertRoundTrip(
            values,
            blockSize,
            StageId.CHIMP_FLOAT_STAGE.id,
            new ChimpFloatTransformEncodeStage(blockSize),
            DECODER,
            blockSize * EXTRA_BUFFER_PER_VALUE
        );
    }

    public void testRoundTripSpecialValues() throws IOException {
        final int blockSize = randomBlockSize();
        final long[] values = new long[blockSize];
        for (int i = 0; i < blockSize; i++) {
            values[i] = floatToSortableLong((float) randomDoubleBetween(0.01, 99.99, true));
        }
        values[0] = floatToSortableLong(Float.MAX_VALUE);
        values[1] = floatToSortableLong(Float.MIN_VALUE);
        values[2] = floatToSortableLong(-0.0f);
        values[3] = floatToSortableLong(Float.MIN_NORMAL);
        assertRoundTrip(
            values,
            blockSize,
            StageId.CHIMP_FLOAT_STAGE.id,
            new ChimpFloatTransformEncodeStage(blockSize),
            DECODER,
            blockSize * EXTRA_BUFFER_PER_VALUE
        );
    }

    public void testRoundTripNaNAndInfinity() throws IOException {
        final int blockSize = randomBlockSize();
        final long[] values = new long[blockSize];
        for (int i = 0; i < blockSize; i++) {
            values[i] = floatToSortableLong((float) randomDoubleBetween(1.0, 100.0, true));
        }
        values[0] = floatToSortableLong(Float.NaN);
        values[1] = floatToSortableLong(Float.POSITIVE_INFINITY);
        values[2] = floatToSortableLong(Float.NEGATIVE_INFINITY);
        assertRoundTrip(
            values,
            blockSize,
            StageId.CHIMP_FLOAT_STAGE.id,
            new ChimpFloatTransformEncodeStage(blockSize),
            DECODER,
            blockSize * EXTRA_BUFFER_PER_VALUE
        );
    }

    public void testRoundTripNegativeValues() throws IOException {
        final int blockSize = randomBlockSize();
        final long[] values = new long[blockSize];
        for (int i = 0; i < blockSize; i++) {
            values[i] = floatToSortableLong((float) -Math.abs(randomDoubleBetween(0.01, 1e6, true)));
        }
        assertRoundTrip(
            values,
            blockSize,
            StageId.CHIMP_FLOAT_STAGE.id,
            new ChimpFloatTransformEncodeStage(blockSize),
            DECODER,
            blockSize * EXTRA_BUFFER_PER_VALUE
        );
    }

    public void testRoundTripSubnormals() throws IOException {
        final int blockSize = randomBlockSize();
        final long[] values = new long[blockSize];
        for (int i = 0; i < blockSize; i++) {
            values[i] = floatToSortableLong(Float.MIN_VALUE * (i + 1));
        }
        assertRoundTrip(
            values,
            blockSize,
            StageId.CHIMP_FLOAT_STAGE.id,
            new ChimpFloatTransformEncodeStage(blockSize),
            DECODER,
            blockSize * EXTRA_BUFFER_PER_VALUE
        );
    }

    public void testRepeatedFuzz() throws IOException {
        for (int iter = 0; iter < 50; iter++) {
            final int blockSize = randomBlockSize();
            final long[] values = new long[blockSize];
            for (int i = 0; i < blockSize; i++) {
                values[i] = floatToSortableLong((float) randomDoubleBetween(-1e12, 1e12, true));
            }
            assertRoundTrip(
                values,
                blockSize,
                StageId.CHIMP_FLOAT_STAGE.id,
                new ChimpFloatTransformEncodeStage(blockSize),
                DECODER,
                blockSize * EXTRA_BUFFER_PER_VALUE
            );
        }
    }

    public void testFullPipelineRoundTrip() throws IOException {
        final int blockSize = randomBlockSize();
        final long[] values = new long[blockSize];
        for (int i = 0; i < blockSize; i++) {
            values[i] = floatToSortableLong((float) (Math.round(randomDoubleBetween(0.01, 99.99, true) * 100.0) / 100.0));
        }
        final NumericEncoder encoder = NumericEncoder.fromConfig(
            PipelineConfig.forFloats(blockSize).chimpFloatStage().offset().gcd().bitPack()
        );
        assertFullPipelineRoundTrip(values, encoder);
    }

    public void testFullPipelineRoundTripLinear() throws IOException {
        final int blockSize = randomBlockSize();
        final long[] values = new long[blockSize];
        for (int i = 0; i < blockSize; i++) {
            values[i] = floatToSortableLong(1.0f + i * 0.25f);
        }
        final NumericEncoder encoder = NumericEncoder.fromConfig(
            PipelineConfig.forFloats(blockSize).chimpFloatStage().offset().gcd().bitPack()
        );
        assertFullPipelineRoundTrip(values, encoder);
    }

    public void testFullPipelineRoundTripBitPackOnly() throws IOException {
        final int blockSize = randomBlockSize();
        final long[] values = new long[blockSize];
        for (int i = 0; i < blockSize; i++) {
            values[i] = floatToSortableLong((float) randomDoubleBetween(-1e6, 1e6, true));
        }
        final NumericEncoder encoder = NumericEncoder.fromConfig(PipelineConfig.forFloats(blockSize).chimpFloatStage().bitPack());
        assertFullPipelineRoundTrip(values, encoder);
    }

    public void testRoundTripCustomGroupSize() throws IOException {
        final int blockSize = randomBlockSize();
        final long[] values = new long[blockSize];
        for (int i = 0; i < blockSize; i++) {
            values[i] = floatToSortableLong(10.0f + i * 0.01f);
        }
        assertRoundTrip(
            values,
            blockSize,
            StageId.CHIMP_FLOAT_STAGE.id,
            new ChimpFloatTransformEncodeStage(blockSize, 32),
            DECODER,
            blockSize * EXTRA_BUFFER_PER_VALUE
        );
    }

    public void testFullPipelineRoundTripCustomGroupSize() throws IOException {
        final int blockSize = randomBlockSize();
        final long[] values = new long[blockSize];
        for (int i = 0; i < blockSize; i++) {
            values[i] = floatToSortableLong((float) (Math.round(randomDoubleBetween(0.01, 99.99, true) * 100.0) / 100.0));
        }
        final NumericEncoder encoder = NumericEncoder.fromConfig(
            PipelineConfig.forFloats(blockSize).chimpFloatStage(32).offset().gcd().bitPack()
        );
        assertFullPipelineRoundTrip(values, encoder);
    }

    public void testFuzzVaryingGroupSize() throws IOException {
        final int[] groupSizes = { 4, 8, 16, 32, 64 };
        for (int iter = 0; iter < 20; iter++) {
            final int blockSize = randomBlockSize();
            final int groupSize = groupSizes[randomIntBetween(0, groupSizes.length - 1)];
            final long[] values = new long[blockSize];
            for (int i = 0; i < blockSize; i++) {
                values[i] = floatToSortableLong((float) randomDoubleBetween(-1e12, 1e12, true));
            }
            assertRoundTrip(
                values,
                blockSize,
                StageId.CHIMP_FLOAT_STAGE.id,
                new ChimpFloatTransformEncodeStage(blockSize, groupSize),
                DECODER,
                blockSize * EXTRA_BUFFER_PER_VALUE
            );
        }
    }
}
