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
import org.elasticsearch.index.codec.tsdb.pipeline.BlockFormat;
import org.elasticsearch.index.codec.tsdb.pipeline.DecodingContext;
import org.elasticsearch.index.codec.tsdb.pipeline.EncodingContext;
import org.elasticsearch.index.codec.tsdb.pipeline.PipelineDescriptor;
import org.elasticsearch.index.codec.tsdb.pipeline.StageId;

import java.io.IOException;

public class QuantizeDoubleCodecStageTests extends NumericCodecStageTestCase {

    private static final double MAX_ERROR = 0.01;
    private static final QuantizeDoubleCodecStage STAGE = new QuantizeDoubleCodecStage(MAX_ERROR);

    public void testRoundTripWithTolerance() throws IOException {
        final int blockSize = randomBlockSize();
        final double[] origDoubles = new double[blockSize];
        for (int i = 0; i < blockSize; i++) {
            origDoubles[i] = randomDoubleBetween(-1000.0, 1000.0, true);
        }
        final long[] original = new long[blockSize];
        for (int i = 0; i < blockSize; i++) {
            original[i] = NumericUtils.doubleToSortableLong(origDoubles[i]);
        }

        long[] decoded = encodeDecode(original.clone(), blockSize, STAGE);

        for (int i = 0; i < blockSize; i++) {
            double origVal = origDoubles[i];
            double decVal = NumericUtils.sortableLongToDouble(decoded[i]);
            assertTrue(
                "Value at " + i + ": original=" + origVal + " decoded=" + decVal + " diff=" + Math.abs(origVal - decVal),
                Math.abs(origVal - decVal) <= MAX_ERROR
            );
        }
    }

    public void testSpecialValuesPassThrough() throws IOException {
        final int blockSize = 4;
        final double[] specials = { Double.NaN, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY, -0.0 };
        final long[] values = new long[blockSize];
        for (int i = 0; i < blockSize; i++) {
            values[i] = NumericUtils.doubleToSortableLong(specials[i]);
        }

        long[] decoded = encodeDecode(values, blockSize, STAGE);

        for (int i = 0; i < blockSize; i++) {
            double origVal = specials[i];
            double decVal = NumericUtils.sortableLongToDouble(decoded[i]);
            if (Double.isNaN(origVal)) {
                assertTrue("Expected NaN at " + i, Double.isNaN(decVal));
            } else if (Double.isInfinite(origVal)) {
                assertEquals("Infinite mismatch at " + i, origVal, decVal, 0.0);
            } else {
                // -0.0: quantize(0.0) = 0.0, and doubleToSortableLong(-0.0) != doubleToSortableLong(0.0)
                // but Math.round(-0.0 / step) * step == 0.0, so the sortable-long changes.
                // Just check the double value is within tolerance.
                assertTrue(Math.abs(origVal - decVal) <= MAX_ERROR);
            }
        }
    }

    public void testDeterministicBehavior() throws IOException {
        final int blockSize = randomBlockSize();
        final long[] values = new long[blockSize];
        for (int i = 0; i < blockSize; i++) {
            values[i] = NumericUtils.doubleToSortableLong(randomDoubleBetween(-100.0, 100.0, true));
        }

        long[] result1 = encodeDecode(values.clone(), blockSize, STAGE);
        long[] result2 = encodeDecode(values.clone(), blockSize, STAGE);

        assertArrayEquals(result1, result2);
    }

    public void testMaxErrorBoundary() throws IOException {
        // Values that sit at step/2 boundary --worst case for quantization error.
        // Due to floating-point arithmetic in round(v/step)*step, the actual error
        // can exceed maxError by a tiny amount (ULP-level), so we allow a small epsilon.
        final double step = 2.0 * MAX_ERROR;
        final double epsilon = 1e-12;
        final int blockSize = 4;
        final double[] doubles = { step * 0.5, step * 1.5, step * 2.5, -step * 0.5 };
        final long[] values = new long[blockSize];
        for (int i = 0; i < blockSize; i++) {
            values[i] = NumericUtils.doubleToSortableLong(doubles[i]);
        }

        long[] decoded = encodeDecode(values.clone(), blockSize, STAGE);

        for (int i = 0; i < blockSize; i++) {
            double diff = Math.abs(doubles[i] - NumericUtils.sortableLongToDouble(decoded[i]));
            assertTrue("Boundary value at " + i + " exceeded maxError: diff=" + diff, diff <= MAX_ERROR + epsilon);
        }
    }

    public void testInvalidMaxErrorThrows() {
        expectThrows(IllegalArgumentException.class, () -> new QuantizeDoubleCodecStage(0));
        expectThrows(IllegalArgumentException.class, () -> new QuantizeDoubleCodecStage(-1.0));
        expectThrows(IllegalArgumentException.class, () -> new QuantizeDoubleCodecStage(Double.NaN));
        expectThrows(IllegalArgumentException.class, () -> new QuantizeDoubleCodecStage(Double.POSITIVE_INFINITY));
    }

    private long[] encodeDecode(final long[] values, int blockSize, final QuantizeDoubleCodecStage stage) throws IOException {
        final byte stageId = StageId.QUANTIZE_DOUBLE.id;
        final byte payloadId = TestPayloadCodecStage.TEST_STAGE_ID;
        final PipelineDescriptor pipeline = new PipelineDescriptor(new byte[] { stageId, payloadId }, blockSize);
        final EncodingContext encodingContext = new EncodingContext(blockSize, pipeline.pipelineLength());
        encodingContext.setValueCount(values.length);
        encodingContext.setCurrentPosition(0);

        stage.encode(values, values.length, encodingContext);

        encodingContext.setCurrentPosition(1);
        encodingContext.applyStage(1);

        final byte[] buffer = new byte[values.length * Long.BYTES + 256];
        final ByteArrayDataOutput out = new ByteArrayDataOutput(buffer);
        BlockFormat.writeBlock(out, values, TestPayloadCodecStage.INSTANCE, encodingContext);

        final DecodingContext decodingContext = new DecodingContext(blockSize, pipeline.pipelineLength());
        final ByteArrayDataInput in = new ByteArrayDataInput(buffer, 0, out.getPosition());
        decodingContext.setDataInput(in);
        BlockFormat.readBlock(in, values, TestPayloadCodecStage.INSTANCE, decodingContext, 1);

        stage.decode(values, values.length, decodingContext);

        return values;
    }
}
