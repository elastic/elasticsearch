/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.test.ComputeTestCase;
import org.elasticsearch.compute.test.TestWarningsSource;

import java.util.List;

import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.greaterThan;

public class RateIntGroupingAggregatorFunctionTests extends ComputeTestCase {

    private DriverContext driverContext() {
        BlockFactory blockFactory = blockFactory();
        return new DriverContext(blockFactory.bigArrays(), blockFactory, null);
    }

    /**
     * Verifies that RATE() on a {@code counter_integer} field produces a positive, correct result
     * when the counter wraps past {@code Integer.MAX_VALUE}.
     *
     * <p>A byte counter growing at 100 MB/s crosses the signed 32-bit boundary every ~21 seconds.
     * The values stored in the index are signed Java {@code int}s, so after the wrap the stored
     * values are negative. Without unsigned interpretation the rate aggregator mis-classifies the
     * wrap as a counter-reset-to-zero, producing a negative (or wildly wrong) rate.</p>
     *
     * <p>The correct interpretation is Counter32 / unsigned-32-bit: the wrap at 2^32 is a
     * continuation of the monotonically-increasing counter, not a reset.</p>
     */
    public void testWrappingIntCounterRate() {
        // Counter incrementing by 100_000_000 bytes per second.
        // Logical unsigned-32-bit values: 2_100_000_000 2_200_000_000 2_300_000_000 ...
        // Stored as signed int (truncated): 2_100_000_000 -2_094_967_296 -1_994_967_296 ...
        // (2_200_000_000 > Integer.MAX_VALUE=2_147_483_647, so it wraps to a negative signed int)
        final long increment = 100_000_000L;  // bytes/s
        final int numSamples = 5;
        int[] counterValues = new int[numSamples];
        long[] timestampsMs = new long[numSamples];

        long logicalValue = 2_100_000_000L;
        for (int i = 0; i < numSamples; i++) {
            counterValues[i] = (int) logicalValue;  // truncate to signed 32-bit
            timestampsMs[i] = (i + 1) * 1000L;     // 1000 ms, 2000 ms, 3000 ms, 4000 ms, 5000 ms
            logicalValue += increment;
        }

        // Sanity check: after the first increment the stored value should be negative (the wrap happened)
        assertTrue("test setup: counterValues[1] must be negative to exercise the wrapping path", counterValues[1] < 0);

        DriverContext driverContext = driverContext();
        BlockFactory blockFactory = driverContext.blockFactory();

        // The aggregator expects data in descending timestamp order (newest first), as produced by TSDS.
        var valuesBuilder = blockFactory.newIntBlockBuilder(numSamples);
        var timestampsBuilder = blockFactory.newLongBlockBuilder(numSamples);
        for (int i = numSamples - 1; i >= 0; i--) {
            valuesBuilder.appendInt(counterValues[i]);
            timestampsBuilder.appendLong(timestampsMs[i]);
        }

        // Page layout: [groupId, values, timestamps, temporality, sliceIndex, futureMaxTimestamps]
        // Channels for the aggregator: values=1, timestamps=2, temporality=3, sliceIndex=4, futureMaxTs=5
        Page page = new Page(
            blockFactory.newConstantIntBlockWith(0, numSamples),
            valuesBuilder.build(),
            timestampsBuilder.build(),
            blockFactory.newConstantNullBlock(numSamples),   // null → CUMULATIVE
            blockFactory.newConstantIntBlockWith(0, numSamples),
            blockFactory.newConstantLongBlockWith(Long.MAX_VALUE, numSamples)
        );

        var aggregator = new RateIntGroupingAggregatorFunction.FunctionSupplier(true, false, TestWarningsSource.INSTANCE)
            .groupingAggregator(driverContext, List.of(1, 2, 3, 4, 5));
        try {
            var addInput = aggregator.prepareProcessRawInputPage(null, page);
            try (var groupIds = blockFactory.newConstantIntBlockWith(0, numSamples).asVector()) {
                addInput.add(0, groupIds);
            }
            addInput.close();
            page.releaseBlocks();

            // Evaluate: group 0 selected; use the non-time-series context (computeRateWithoutExtrapolate path)
            try (var selected = blockFactory.newConstantIntBlockWith(0, 1).asVector()) {
                var ctx = new GroupingAggregatorEvaluationContext(driverContext);
                var prepared = aggregator.prepareEvaluateFinal(selected, ctx);
                Block[] result = new Block[1];
                prepared.evaluate(result, 0, selected);
                try (DoubleBlock rates = (DoubleBlock) result[0]) {
                    assertFalse("rate should not be null for a counter that wraps", rates.isNull(0));
                    double rate = rates.getDouble(0);
                    // A rate on a counter can never be negative.
                    // Without the unsigned fix, the rate is approximately -448_741_824 (negative).
                    assertThat("rate must be positive for a monotonically-increasing wrapping counter", rate, greaterThan(0.0));
                    // The counter increases by exactly `increment` bytes per second across all 4 seconds.
                    assertThat("rate should equal the per-second counter increment", rate, closeTo(increment, 1.0));
                }
            }
        } finally {
            aggregator.close();
        }
    }
}
