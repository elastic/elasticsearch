/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.columnar.storage;

import org.apache.lucene.codecs.lucene90.Lucene90DocValuesFormat;
import org.elasticsearch.columnar.ColumNARDocValuesFormat;
import org.elasticsearch.columnar.numeric.NumericPipeline;
import org.elasticsearch.columnar.numeric.NumericPipelineSelector;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;

import java.io.IOException;
import java.util.Locale;

/**
 * Pinned byte-count assertions at bs=128; workload data is fixed-seed (see {@code generate()} in
 * the base class). For block-size scaling curves see {@link ColumnarBlockSizeSweepTests}.
 */
public class ColumnarNumericFootprintTests extends ColumnarNumericStorageTestBase {

    private static final Logger logger = LogManager.getLogger(ColumnarNumericFootprintTests.class);
    private static final int BLOCK_SIZE = ColumNARDocValuesFormat.MIN_BLOCK_SIZE;

    public void testFootprintMonotonicTimestamps() throws IOException {
        runFootprintTest("MONOTONIC_TIMESTAMPS", (f, t) -> NumericPipeline::monotonicLongPipeline, 66361);
    }

    public void testFootprintTsdbSplit() throws IOException {
        runFootprintTest("TSDB_SPLIT", (f, t) -> NumericPipeline::monotonicLongPipeline, 4803);
    }

    public void testFootprintCounterSteady() throws IOException {
        runFootprintTest("COUNTER_STEADY", (f, t) -> NumericPipeline::monotonicLongPipeline, 3987);
    }

    public void testFootprintGauge() throws IOException {
        runFootprintTest("GAUGE", (f, t) -> NumericPipeline::defaultPipeline, 53066);
    }

    public void testFootprintSensorDoubles() throws IOException {
        runFootprintTest("SENSOR_DOUBLES", (f, t) -> NumericPipeline::doubleGaugePipeline, 57872);
    }

    public void testFootprintDoubleGauge() throws IOException {
        runFootprintTest("DOUBLE_GAUGE", (f, t) -> NumericPipeline::doubleGaugePipeline, 124095);
    }

    public void testFootprintDoubleCounter() throws IOException {
        runFootprintTest("DOUBLE_COUNTER", (f, t) -> NumericPipeline::doubleCounterPipeline, 48956);
    }

    public void testFootprintRandomFull() throws IOException {
        runFootprintTest("RANDOM_FULL", (f, t) -> NumericPipeline::defaultPipeline, 401838);
    }

    private void runFootprintTest(String workload, NumericPipelineSelector selector, long expectedBytes) throws IOException {
        final long[] values = generate(workload, DOC_COUNT);
        final long columnar = measureConsumer(new ColumNARDocValuesFormat(selector, BLOCK_SIZE), values, true);
        final long lucene = measureConsumer(new Lucene90DocValuesFormat(), values, false);
        final long es95Small = measureConsumer(es95Format(workload, false), values, false);
        final long es95Large = measureConsumer(es95Format(workload, true), values, false);
        logger.info(
            "workload={} columnar={} es95_128={} es95_512={} lucene={} vs_es95_128={}x vs_es95_512={}x vs_lucene={}x",
            workload,
            columnar,
            es95Small,
            es95Large,
            lucene,
            String.format(Locale.ROOT, "%.2f", (double) columnar / es95Small),
            String.format(Locale.ROOT, "%.2f", (double) columnar / es95Large),
            String.format(Locale.ROOT, "%.2f", (double) columnar / lucene)
        );
        assertEquals(workload + ": unexpected ColumNAR byte count", expectedBytes, columnar);
    }
}
