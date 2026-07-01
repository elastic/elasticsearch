/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.telemetry.InstrumentType;
import org.elasticsearch.telemetry.Measurement;
import org.elasticsearch.telemetry.RecordingMeterRegistry;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalSourceMetrics;

import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

/**
 * Wiring tests for {@link AsyncExternalSourceOperator}'s node-telemetry bridge: they assert that the production
 * getOutput()/close() call sites actually move the parse / splits / time-to-first-row instruments on a real
 * registry-backed {@link ExternalSourceMetrics}, tagged with the storage scheme. Uses a real
 * {@link AsyncExternalSourceBuffer} fed with a real {@link Page} (no mocks).
 */
public class AsyncExternalSourceOperatorTelemetryTests extends ESTestCase {

    private static final BlockFactory BLOCK_FACTORY = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE)
        .breaker(new NoopCircuitBreaker("none"))
        .build();

    private static Page createTestPage(int numColumns, int numRows) {
        IntBlock[] blocks = new IntBlock[numColumns];
        for (int c = 0; c < numColumns; c++) {
            IntBlock.Builder builder = BLOCK_FACTORY.newIntBlockBuilder(numRows);
            for (int r = 0; r < numRows; r++) {
                builder.appendInt(r);
            }
            blocks[c] = builder.build();
        }
        return new Page(blocks);
    }

    /**
     * Drives one page through getOutput() then closes the operator, and asserts the four instruments the punch
     * list names — time_to_first_row, parse.rows.total, parse.duration and discovery.splits_scanned — are all
     * recorded carrying the canonicalised scheme ("s3a" folds to "s3").
     */
    public void testGetOutputAndCloseRecordParseSplitsAndTtfrWithScheme() {
        RecordingMeterRegistry registry = new RecordingMeterRegistry();
        ExternalSourceMetrics metrics = new ExternalSourceMetrics(registry);

        AsyncExternalSourceBuffer buffer = new AsyncExternalSourceBuffer(1024 * 1024);
        buffer.setSplitsTotal(3);
        buffer.addPage(createTestPage(1, 5));

        AsyncExternalSourceOperator operator = new AsyncExternalSourceOperator(buffer, metrics, "s3a");

        Page page = operator.getOutput();
        assertNotNull("the buffered page must be emitted", page);
        page.releaseBlocks();

        // Buffer drained + marked finished => the next poll is a clean EOF.
        buffer.finish(true);
        assertNull(operator.getOutput());

        operator.close();

        // time_to_first_row recorded exactly once (on the first page), tagged with the canonical scheme.
        Measurement ttfr = single(registry, InstrumentType.LONG_HISTOGRAM, ExternalSourceMetrics.QUERY_TIME_TO_FIRST_ROW);
        assertThat(ttfr.attributes().get(ExternalSourceMetrics.SCHEME_ATTRIBUTE), equalTo("s3"));

        // parse.rows.total carries the 5 emitted rows plus the scheme.
        Measurement rows = single(registry, InstrumentType.LONG_COUNTER, ExternalSourceMetrics.PARSE_ROWS_TOTAL);
        assertThat(rows.getLong(), equalTo(5L));
        assertThat(rows.attributes().get(ExternalSourceMetrics.SCHEME_ATTRIBUTE), equalTo("s3"));

        // parse.duration recorded with the scheme (no format-reader status wired => a zero-duration observation).
        Measurement parseDuration = single(registry, InstrumentType.LONG_HISTOGRAM, ExternalSourceMetrics.PARSE_DURATION);
        assertThat(parseDuration.attributes().get(ExternalSourceMetrics.SCHEME_ATTRIBUTE), equalTo("s3"));

        // discovery.splits_scanned carries the split total plus the scheme.
        Measurement splits = single(registry, InstrumentType.LONG_HISTOGRAM, ExternalSourceMetrics.DISCOVERY_SPLITS_SCANNED);
        assertThat(splits.getLong(), equalTo(3L));
        assertThat(splits.attributes().get(ExternalSourceMetrics.SCHEME_ATTRIBUTE), equalTo("s3"));
    }

    /**
     * A failed/empty scan (no rows emitted, no splits) must NOT seed the parse/splits histograms with zeros: the
     * close-time recorder short-circuits, mirroring the read-stall {@code millis <= 0} guard.
     */
    public void testEmptyScanSkipsParseAndSplitsObservations() {
        RecordingMeterRegistry registry = new RecordingMeterRegistry();
        ExternalSourceMetrics metrics = new ExternalSourceMetrics(registry);

        AsyncExternalSourceBuffer buffer = new AsyncExternalSourceBuffer(1024 * 1024);
        buffer.finish(true);

        AsyncExternalSourceOperator operator = new AsyncExternalSourceOperator(buffer, metrics, "s3");
        assertNull(operator.getOutput());
        operator.close();

        assertThat(measurements(registry, InstrumentType.LONG_COUNTER, ExternalSourceMetrics.PARSE_ROWS_TOTAL), hasSize(0));
        assertThat(measurements(registry, InstrumentType.LONG_HISTOGRAM, ExternalSourceMetrics.PARSE_DURATION), hasSize(0));
        assertThat(measurements(registry, InstrumentType.LONG_HISTOGRAM, ExternalSourceMetrics.DISCOVERY_SPLITS_SCANNED), hasSize(0));
        assertThat(measurements(registry, InstrumentType.LONG_HISTOGRAM, ExternalSourceMetrics.QUERY_TIME_TO_FIRST_ROW), hasSize(0));
    }

    private static List<Measurement> measurements(RecordingMeterRegistry registry, InstrumentType type, String name) {
        return registry.getRecorder().getMeasurements(type, name);
    }

    private static Measurement single(RecordingMeterRegistry registry, InstrumentType type, String name) {
        List<Measurement> found = measurements(registry, type, name);
        assertThat("expected exactly one measurement for [" + name + "]", found, hasSize(1));
        return found.get(0);
    }
}
