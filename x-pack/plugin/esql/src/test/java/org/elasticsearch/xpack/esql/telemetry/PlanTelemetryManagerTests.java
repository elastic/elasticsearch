/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.telemetry;

import org.elasticsearch.core.TimeValue;
import org.elasticsearch.telemetry.InstrumentType;
import org.elasticsearch.telemetry.Measurement;
import org.elasticsearch.telemetry.RecordingMeterRegistry;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.querydsl.QueryDslTimestampBoundsExtractor.TimestampBounds;
import org.elasticsearch.xpack.esql.expression.function.EsqlFunctionRegistry;
import org.elasticsearch.xpack.esql.plan.QuerySettings;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.core.esql.QueryMetricsListener.Outcome.CANCELLED;
import static org.elasticsearch.xpack.core.esql.QueryMetricsListener.Outcome.FAILED;
import static org.elasticsearch.xpack.core.esql.QueryMetricsListener.Outcome.SUCCESS;
import static org.elasticsearch.xpack.esql.telemetry.PlanTelemetryManager.OUTCOME;
import static org.elasticsearch.xpack.esql.telemetry.PlanTelemetryManager.QUERY_DURATION_HISTOGRAM;
import static org.elasticsearch.xpack.esql.telemetry.PlanTelemetryManager.QUERY_TOTAL;
import static org.elasticsearch.xpack.esql.telemetry.PlanTelemetryManager.SOURCE_COMMAND;
import static org.elasticsearch.xpack.esql.telemetry.PlanTelemetryManager.TIME_RANGE_FILTER_FROM;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

public class PlanTelemetryManagerTests extends ESTestCase {

    private static final Instant START = Instant.parse("2026-01-02T12:00:00Z");

    public void testRecordQuery() {
        RecordingMeterRegistry registry = new RecordingMeterRegistry();
        PlanTelemetryManager manager = new PlanTelemetryManager(registry);

        manager.recordQuery(telemetry(QuerySourceCommand.TS), lastHours(1), START.toEpochMilli(), SUCCESS, millis(42));
        manager.recordQuery(telemetry(QuerySourceCommand.OTHER), null, START.toEpochMilli(), SUCCESS, millis(3));
        manager.recordQuery(telemetry(QuerySourceCommand.FROM), lastHours(48), START.toEpochMilli(), CANCELLED, millis(7));
        manager.recordQuery(telemetry(QuerySourceCommand.UNKNOWN), null, START.toEpochMilli(), FAILED, millis(1));

        // Only successful queries have their duration recorded...
        List<Measurement> durations = registry.getRecorder().getMeasurements(InstrumentType.LONG_HISTOGRAM, QUERY_DURATION_HISTOGRAM);
        assertThat(durations, hasSize(2));
        assertMeasurement(durations.get(0), 42, Map.of(SOURCE_COMMAND, "TS", TIME_RANGE_FILTER_FROM, "1_hour"));
        assertMeasurement(durations.get(1), 3, Map.of(SOURCE_COMMAND, "OTHER"));

        // ...but every query is counted, with its outcome.
        List<Measurement> counts = registry.getRecorder().getMeasurements(InstrumentType.LONG_COUNTER, QUERY_TOTAL);
        assertThat(counts, hasSize(4));
        assertMeasurement(counts.get(0), 1, Map.of(SOURCE_COMMAND, "TS", TIME_RANGE_FILTER_FROM, "1_hour", OUTCOME, "success"));
        assertMeasurement(counts.get(1), 1, Map.of(SOURCE_COMMAND, "OTHER", OUTCOME, "success"));
        assertMeasurement(counts.get(2), 1, Map.of(SOURCE_COMMAND, "FROM", TIME_RANGE_FILTER_FROM, "3_days", OUTCOME, "cancelled"));
        assertMeasurement(counts.get(3), 1, Map.of(SOURCE_COMMAND, "UNKNOWN", OUTCOME, "failed"));
    }

    /**
     * The attribute is the age of the range's lower bound when the query started, like Query DSL's, not the length of the range.
     */
    public void testTimeRangeFilterFrom() {
        // A short range a week ago reads old data.
        Instant weekAgo = START.minus(Duration.ofDays(7));
        assertTimeRangeFilterFrom(new TimestampBounds(weekAgo, weekAgo.plusSeconds(60)), "7_days");
        // Ages at a threshold land in the bucket named after it.
        Instant fifteenMinutesAgo = START.minus(Duration.ofMinutes(15));
        assertTimeRangeFilterFrom(new TimestampBounds(fifteenMinutesAgo, START), "15_minutes");
        assertTimeRangeFilterFrom(new TimestampBounds(fifteenMinutesAgo.minusMillis(1), START), "1_hour");
        // A range in the future, and one far outside the epoch millisecond range, land in the first and last bucket.
        assertTimeRangeFilterFrom(new TimestampBounds(START.plus(Duration.ofDays(1)), START.plus(Duration.ofDays(2))), "15_minutes");
        assertTimeRangeFilterFrom(new TimestampBounds(Instant.MIN, START), "older_than_14_days");
    }

    private static void assertTimeRangeFilterFrom(TimestampBounds range, String expected) {
        RecordingMeterRegistry registry = new RecordingMeterRegistry();
        new PlanTelemetryManager(registry).recordQuery(telemetry(QuerySourceCommand.FROM), range, START.toEpochMilli(), SUCCESS, millis(1));
        Measurement count = registry.getRecorder().getMeasurements(InstrumentType.LONG_COUNTER, QUERY_TOTAL).getFirst();
        assertThat(range.toString(), count.attributes().get(TIME_RANGE_FILTER_FROM), equalTo(expected));
    }

    private static void assertMeasurement(Measurement measurement, long value, Map<String, Object> attributes) {
        assertThat(measurement.getLong(), equalTo(value));
        assertThat(measurement.attributes(), equalTo(attributes));
    }

    private static TimestampBounds lastHours(int hours) {
        return new TimestampBounds(START.minus(Duration.ofHours(hours)), START);
    }

    private static TimeValue millis(long millis) {
        return TimeValue.timeValueMillis(millis);
    }

    private static PlanTelemetry telemetry(QuerySourceCommand sourceCommand) {
        PlanTelemetry telemetry = new PlanTelemetry(new EsqlFunctionRegistry(), QuerySettings.applicableIn(true, false));
        telemetry.sourceCommand(sourceCommand);
        return telemetry;
    }
}
