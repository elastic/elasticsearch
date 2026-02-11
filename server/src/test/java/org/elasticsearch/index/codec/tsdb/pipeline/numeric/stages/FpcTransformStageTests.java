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

public class FpcTransformStageTests extends NumericCodecStageTestCase {

    private static final TransformDecoder DECODER = FpcTransformDecodeStage.INSTANCE;
    private static final int EXTRA_BUFFER_PER_VALUE = 9;

    public void testRoundTripConstantValues() throws IOException {
        final int blockSize = randomBlockSize();
        final long[] values = new long[blockSize];
        final long constant = NumericUtils.doubleToSortableLong(42.5);
        Arrays.fill(values, constant);
        assertRoundTrip(
            values,
            blockSize,
            StageId.FPC_STAGE.id,
            new FpcTransformEncodeStage(blockSize),
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
            StageId.FPC_STAGE.id,
            new FpcTransformEncodeStage(blockSize),
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
            StageId.FPC_STAGE.id,
            new FpcTransformEncodeStage(blockSize),
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
            StageId.FPC_STAGE.id,
            new FpcTransformEncodeStage(blockSize),
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
            StageId.FPC_STAGE.id,
            new FpcTransformEncodeStage(blockSize),
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
            StageId.FPC_STAGE.id,
            new FpcTransformEncodeStage(blockSize),
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
            StageId.FPC_STAGE.id,
            new FpcTransformEncodeStage(blockSize, 512),
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
                StageId.FPC_STAGE.id,
                new FpcTransformEncodeStage(blockSize),
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

    public void testFloatRoundTripConstantValues() throws IOException {
        final int blockSize = randomBlockSize();
        final long[] values = new long[blockSize];
        final long constant = NumericUtils.floatToSortableInt(42.5f);
        Arrays.fill(values, constant);
        assertRoundTrip(
            values,
            blockSize,
            StageId.FPC_STAGE.id,
            new FpcTransformEncodeStage(blockSize),
            DECODER,
            blockSize * EXTRA_BUFFER_PER_VALUE
        );
    }

    public void testFloatRoundTripLinearSequence() throws IOException {
        final int blockSize = randomBlockSize();
        final long[] values = new long[blockSize];
        for (int i = 0; i < blockSize; i++) {
            values[i] = NumericUtils.floatToSortableInt(1.0f + i * 0.5f);
        }
        assertRoundTrip(
            values,
            blockSize,
            StageId.FPC_STAGE.id,
            new FpcTransformEncodeStage(blockSize),
            DECODER,
            blockSize * EXTRA_BUFFER_PER_VALUE
        );
    }

    public void testFloatRoundTripSensorLikeData() throws IOException {
        final int blockSize = randomBlockSize();
        final long[] values = new long[blockSize];
        for (int i = 0; i < blockSize; i++) {
            float v = 22.5f + Math.round(randomFloat() * 10.0f - 5.0f) / 10.0f;
            values[i] = NumericUtils.floatToSortableInt(v);
        }
        assertRoundTrip(
            values,
            blockSize,
            StageId.FPC_STAGE.id,
            new FpcTransformEncodeStage(blockSize),
            DECODER,
            blockSize * EXTRA_BUFFER_PER_VALUE
        );
    }

    public void testFloatRoundTripRandomValues() throws IOException {
        final int blockSize = randomBlockSize();
        final long[] values = new long[blockSize];
        for (int i = 0; i < blockSize; i++) {
            values[i] = NumericUtils.floatToSortableInt(randomFloat() * 2e6f - 1e6f);
        }
        assertRoundTrip(
            values,
            blockSize,
            StageId.FPC_STAGE.id,
            new FpcTransformEncodeStage(blockSize),
            DECODER,
            blockSize * EXTRA_BUFFER_PER_VALUE
        );
    }

    public void testFloatRoundTripMixedPositiveNegative() throws IOException {
        final int blockSize = randomBlockSize();
        final long[] values = new long[blockSize];
        for (int i = 0; i < blockSize; i++) {
            float v = Math.round((randomFloat() * 200.0f - 100.0f) * 100.0f) / 100.0f;
            values[i] = NumericUtils.floatToSortableInt(v);
        }
        assertRoundTrip(
            values,
            blockSize,
            StageId.FPC_STAGE.id,
            new FpcTransformEncodeStage(blockSize),
            DECODER,
            blockSize * EXTRA_BUFFER_PER_VALUE
        );
    }

    public void testFloatRepeatedFuzz() throws IOException {
        for (int iter = 0; iter < 50; iter++) {
            final int blockSize = randomBlockSize();
            final long[] values = new long[blockSize];
            for (int i = 0; i < blockSize; i++) {
                values[i] = NumericUtils.floatToSortableInt(randomFloat() * 2e6f - 1e6f);
            }
            assertRoundTrip(
                values,
                blockSize,
                StageId.FPC_STAGE.id,
                new FpcTransformEncodeStage(blockSize),
                DECODER,
                blockSize * EXTRA_BUFFER_PER_VALUE
            );
        }
    }

    public void testFloatFullPipelineRoundTrip() throws IOException {
        final int blockSize = randomBlockSize();
        final long[] values = new long[blockSize];
        for (int i = 0; i < blockSize; i++) {
            values[i] = NumericUtils.floatToSortableInt(Math.round(randomFloat() * 10000.0f) / 100.0f);
        }
        final NumericEncoder encoder = NumericEncoder.fromConfig(PipelineConfig.forFloats(blockSize).fpcStage().offset().gcd().bitPack());
        assertFullPipelineRoundTrip(values, encoder);
    }

    public void testFloatFullPipelineRoundTripLinear() throws IOException {
        final int blockSize = randomBlockSize();
        final long[] values = new long[blockSize];
        for (int i = 0; i < blockSize; i++) {
            values[i] = NumericUtils.floatToSortableInt(1.0f + i * 0.25f);
        }
        final NumericEncoder encoder = NumericEncoder.fromConfig(PipelineConfig.forFloats(blockSize).fpcStage().offset().gcd().bitPack());
        assertFullPipelineRoundTrip(values, encoder);
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
            StageId.FPC_STAGE.id,
            new FpcTransformEncodeStage(blockSize),
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
            StageId.FPC_STAGE.id,
            new FpcTransformEncodeStage(blockSize),
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
            StageId.FPC_STAGE.id,
            new FpcTransformEncodeStage(blockSize),
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
            StageId.FPC_STAGE.id,
            new FpcTransformEncodeStage(blockSize),
            DECODER,
            blockSize * EXTRA_BUFFER_PER_VALUE
        );
    }

    public void testFloatRoundTripSpecialValues() throws IOException {
        final int blockSize = randomBlockSize();
        final long[] values = new long[blockSize];
        for (int i = 0; i < blockSize; i++) {
            values[i] = NumericUtils.floatToSortableInt(randomFloat() * 100.0f);
        }
        values[0] = NumericUtils.floatToSortableInt(Float.MAX_VALUE);
        values[1] = NumericUtils.floatToSortableInt(Float.MIN_VALUE);
        values[2] = NumericUtils.floatToSortableInt(-0.0f);
        values[3] = NumericUtils.floatToSortableInt(Float.MIN_NORMAL);
        assertRoundTrip(
            values,
            blockSize,
            StageId.FPC_STAGE.id,
            new FpcTransformEncodeStage(blockSize),
            DECODER,
            blockSize * EXTRA_BUFFER_PER_VALUE
        );
    }

    public void testFloatRoundTripIntegerLikeValues() throws IOException {
        final int blockSize = randomBlockSize();
        final long[] values = new long[blockSize];
        for (int i = 0; i < blockSize; i++) {
            values[i] = NumericUtils.floatToSortableInt((float) randomIntBetween(0, 10000));
        }
        assertRoundTrip(
            values,
            blockSize,
            StageId.FPC_STAGE.id,
            new FpcTransformEncodeStage(blockSize),
            DECODER,
            blockSize * EXTRA_BUFFER_PER_VALUE
        );
    }

}
