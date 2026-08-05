/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.columnar.storage;

import org.elasticsearch.columnar.ColumNARDocValuesFormat;
import org.elasticsearch.columnar.numeric.NumericPipeline;
import org.elasticsearch.columnar.numeric.NumericPipelineSelector;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;

import java.io.IOException;
import java.util.Locale;
import java.util.Map;

/**
 * Asserts ColumNAR storage parity with ES95 at block sizes 128 and 512. For pinned
 * byte-count assertions see {@link ColumnarNumericFootprintTests}.
 */
public class ColumnarBlockSizeSweepTests extends ColumnarNumericStorageTestBase {

    private static final Logger logger = LogManager.getLogger(ColumnarBlockSizeSweepTests.class);
    private static final int[] ES95_BLOCK_SIZES = { 128, 512 };
    private static final long SMALL_WORKLOAD_THRESHOLD_BYTES = 16_384L;
    private static final long SMALL_WORKLOAD_ABS_DELTA_BYTES = 512L;

    private record Ceiling(double bs128, double bs512) {}

    private static final Map<String, Ceiling> MAX_COLUMNAR_TO_ES95_RATIO = Map.of(
        "COUNTER_STEADY",
        new Ceiling(1.15, 1.30),
        "MONOTONIC_TIMESTAMPS",
        new Ceiling(1.10, 1.10),
        "TSDB_SPLIT",
        new Ceiling(1.05, 1.10),
        "GAUGE",
        new Ceiling(1.05, 1.05),
        "SENSOR_DOUBLES",
        new Ceiling(1.05, 1.05),
        "DOUBLE_GAUGE",
        new Ceiling(1.05, 1.05),
        "DOUBLE_COUNTER",
        new Ceiling(1.05, 1.05),
        "RANDOM_FULL",
        new Ceiling(1.05, 1.05)
    );

    public void testSweepMonotonicTimestamps() throws IOException {
        runSweep("MONOTONIC_TIMESTAMPS", (f, t) -> NumericPipeline::monotonicLongPipeline);
    }

    public void testSweepTsdbSplit() throws IOException {
        runSweep("TSDB_SPLIT", (f, t) -> NumericPipeline::monotonicLongPipeline);
    }

    public void testSweepCounterSteady() throws IOException {
        runSweep("COUNTER_STEADY", (f, t) -> NumericPipeline::monotonicLongPipeline);
    }

    public void testSweepGauge() throws IOException {
        runSweep("GAUGE", (f, t) -> NumericPipeline::defaultPipeline);
    }

    public void testSweepSensorDoubles() throws IOException {
        runSweep("SENSOR_DOUBLES", (f, t) -> NumericPipeline::doubleGaugePipeline);
    }

    public void testSweepDoubleGauge() throws IOException {
        runSweep("DOUBLE_GAUGE", (f, t) -> NumericPipeline::doubleGaugePipeline);
    }

    public void testSweepDoubleCounter() throws IOException {
        runSweep("DOUBLE_COUNTER", (f, t) -> NumericPipeline::doubleCounterPipeline);
    }

    public void testSweepRandomFull() throws IOException {
        runSweep("RANDOM_FULL", (f, t) -> NumericPipeline::defaultPipeline);
    }

    private void runSweep(String workload, NumericPipelineSelector selector) throws IOException {
        final long[] values = generate(workload, DOC_COUNT);
        runParity(workload, selector, values);
    }

    private void runParity(String workload, NumericPipelineSelector selector, long[] values) throws IOException {
        for (int blockSize : ES95_BLOCK_SIZES) {
            final long columnar = measureConsumer(new ColumNARDocValuesFormat(selector, blockSize), values, true);
            final long es95 = measureConsumer(es95Format(workload, blockSize == 512), values, false);
            final double ratio = (double) columnar / es95;
            logger.info(
                "parity workload={} blockSize={} columnar={} es95={} ratio={}x",
                workload,
                blockSize,
                columnar,
                es95,
                String.format(Locale.ROOT, "%.2f", ratio)
            );
            final Ceiling ceilings = MAX_COLUMNAR_TO_ES95_RATIO.get(workload);
            assertNotNull("no parity ceiling defined for workload: " + workload, ceilings);
            if (es95 < SMALL_WORKLOAD_THRESHOLD_BYTES) {
                final long absDelta = columnar - es95;
                assertTrue(
                    workload + " bs=" + blockSize + ": abs delta " + absDelta + " B > " + SMALL_WORKLOAD_ABS_DELTA_BYTES + " B",
                    absDelta <= SMALL_WORKLOAD_ABS_DELTA_BYTES
                );
            } else {
                final double ceiling = blockSize == 128 ? ceilings.bs128() : ceilings.bs512();
                assertTrue(
                    workload + " bs=" + blockSize + ": ratio " + String.format(Locale.ROOT, "%.2f", ratio) + "x > ceiling " + ceiling + "x",
                    ratio <= ceiling
                );
            }
        }
    }

}
