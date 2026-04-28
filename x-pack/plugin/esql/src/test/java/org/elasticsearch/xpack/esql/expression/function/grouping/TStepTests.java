/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.grouping;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.time.DateUtils;
import org.elasticsearch.xpack.esql.capabilities.ConfigurationAware;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.DocsV3Support;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;
import org.elasticsearch.xpack.esql.expression.function.scalar.AbstractConfigurationFunctionTestCase;
import org.elasticsearch.xpack.esql.session.Configuration;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

import static org.elasticsearch.xpack.esql.ConfigurationTestUtils.randomConfigurationBuilder;
import static org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier.TEST_SOURCE;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

public class TStepTests extends AbstractConfigurationFunctionTestCase {
    private static final DataType[] BOUND_TYPES = new DataType[] { DataType.DATETIME, DataType.DATE_NANOS, DataType.KEYWORD };

    public TStepTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        List<TestCaseSupplier> suppliers = new ArrayList<>();
        dateCasesWithStep(
            suppliers,
            "5 minute step uses query now",
            () -> Instant.parse("2024-01-01T12:09:00Z").toEpochMilli(),
            Duration.ofMinutes(5),
            Instant.parse("2024-01-01T12:12:00Z")
        );
        dateCasesWithStep(
            suppliers,
            "30 second step uses query now",
            () -> Instant.parse("2024-01-01T12:01:01Z").toEpochMilli(),
            Duration.ofSeconds(30),
            Instant.parse("2024-01-01T12:02:00Z")
        );
        dateCasesWithStep(
            suppliers,
            "1 hour step uses query now",
            () -> Instant.parse("2024-01-01T14:23:00Z").toEpochMilli(),
            Duration.ofHours(1),
            Instant.parse("2024-01-01T15:00:00Z")
        );
        dateCasesWithStep(
            suppliers,
            "1 day step uses query now",
            () -> Instant.parse("2024-01-02T09:30:00Z").toEpochMilli(),
            Duration.ofDays(1),
            Instant.parse("2024-01-03T00:00:00Z")
        );
        dateNanosCasesWithStep(
            suppliers,
            "date_nanos 30 second step uses query now",
            () -> DateUtils.toLong(Instant.parse("2024-01-01T12:01:01Z")),
            Duration.ofSeconds(30),
            Instant.parse("2024-01-01T12:02:00Z")
        );
        dateNanosCasesWithStep(
            suppliers,
            "date_nanos 5 minute step uses query now",
            () -> DateUtils.toLong(Instant.parse("2024-01-01T12:09:00Z")),
            Duration.ofMinutes(5),
            Instant.parse("2024-01-01T12:12:00Z")
        );
        dateCasesWithExplicitBounds(
            suppliers,
            "explicit bounds 1 hour step",
            () -> Instant.parse("2024-01-01T12:45:00Z").toEpochMilli(),
            Duration.ofHours(1),
            Instant.parse("2024-01-01T12:15:00Z"),
            Instant.parse("2024-01-01T13:55:00Z")
        );
        dateNanosCasesWithExplicitBounds(
            suppliers,
            "date_nanos explicit bounds 5 minute step",
            () -> DateUtils.toLong(Instant.parse("2024-01-01T12:09:00Z")),
            Duration.ofMinutes(5),
            Instant.parse("2024-01-01T12:04:00Z"),
            Instant.parse("2024-01-01T12:32:00Z")
        );
        return parameterSuppliersFromTypedData(suppliers);
    }

    private static void dateCasesWithStep(
        List<TestCaseSupplier> suppliers,
        String name,
        LongSupplier timestamp,
        Duration step,
        Instant now
    ) {
        suppliers.add(new TestCaseSupplier(name, List.of(DataType.TIME_DURATION, DataType.DATETIME), () -> {
            List<TestCaseSupplier.TypedData> args = new ArrayList<>();
            args.add(new TestCaseSupplier.TypedData(step, DataType.TIME_DURATION, "step").forceLiteral());
            args.add(new TestCaseSupplier.TypedData(timestamp.getAsLong(), DataType.DATETIME, "@timestamp"));
            return new TestCaseSupplier.TestCase(
                args,
                Matchers.startsWith("AddDatetimesEvaluator["),
                DataType.DATETIME,
                resultsMatcher(args, step.toMillis(), now)
            ).withConfiguration(
                TEST_SOURCE,
                randomConfigurationBuilder().query(TEST_SOURCE.text()).now(now).zoneId(ZoneOffset.ofHours(5)).build()
            );
        }));
    }

    private static void dateNanosCasesWithStep(
        List<TestCaseSupplier> suppliers,
        String name,
        LongSupplier timestampNanos,
        Duration step,
        Instant now
    ) {
        suppliers.add(new TestCaseSupplier(name, List.of(DataType.TIME_DURATION, DataType.DATE_NANOS), () -> {
            List<TestCaseSupplier.TypedData> args = new ArrayList<>();
            args.add(new TestCaseSupplier.TypedData(step, DataType.TIME_DURATION, "step").forceLiteral());
            args.add(new TestCaseSupplier.TypedData(timestampNanos.getAsLong(), DataType.DATE_NANOS, "@timestamp"));
            return new TestCaseSupplier.TestCase(
                args,
                Matchers.startsWith("AddDateNanosEvaluator["),
                DataType.DATE_NANOS,
                resultsMatcher(args, step.toMillis(), now)
            ).withConfiguration(
                TEST_SOURCE,
                randomConfigurationBuilder().query(TEST_SOURCE.text()).now(now).zoneId(ZoneOffset.ofHours(-7)).build()
            );
        }));
    }

    private static void dateCasesWithExplicitBounds(
        List<TestCaseSupplier> suppliers,
        String name,
        LongSupplier timestamp,
        Duration step,
        Instant start,
        Instant end
    ) {
        for (DataType fromType : BOUND_TYPES) {
            for (DataType toType : BOUND_TYPES) {
                suppliers.add(new TestCaseSupplier(name, List.of(DataType.TIME_DURATION, fromType, toType, DataType.DATETIME), () -> {
                    List<TestCaseSupplier.TypedData> args = new ArrayList<>();
                    args.add(new TestCaseSupplier.TypedData(step, DataType.TIME_DURATION, "step").forceLiteral());
                    args.add(dateBound("from", fromType, start));
                    args.add(dateBound("to", toType, end));
                    args.add(new TestCaseSupplier.TypedData(timestamp.getAsLong(), DataType.DATETIME, "@timestamp"));
                    return new TestCaseSupplier.TestCase(
                        args,
                        Matchers.startsWith("AddDatetimesEvaluator["),
                        DataType.DATETIME,
                        equalTo(alignedBucketEnd(timestamp.getAsLong(), start.toEpochMilli(), step.toMillis()))
                    ).withConfiguration(
                        TEST_SOURCE,
                        randomConfigurationBuilder().query(TEST_SOURCE.text()).now(end).zoneId(ZoneOffset.UTC).build()
                    );
                }));
            }
        }
    }

    private static void dateNanosCasesWithExplicitBounds(
        List<TestCaseSupplier> suppliers,
        String name,
        LongSupplier timestampNanos,
        Duration step,
        Instant start,
        Instant end
    ) {
        for (DataType fromType : BOUND_TYPES) {
            for (DataType toType : BOUND_TYPES) {
                suppliers.add(new TestCaseSupplier(name, List.of(DataType.TIME_DURATION, fromType, toType, DataType.DATE_NANOS), () -> {
                    List<TestCaseSupplier.TypedData> args = new ArrayList<>();
                    args.add(new TestCaseSupplier.TypedData(step, DataType.TIME_DURATION, "step").forceLiteral());
                    args.add(dateBound("from", fromType, start));
                    args.add(dateBound("to", toType, end));
                    args.add(new TestCaseSupplier.TypedData(timestampNanos.getAsLong(), DataType.DATE_NANOS, "@timestamp"));
                    long expectedMillis = alignedBucketEnd(
                        DateUtils.toMilliSeconds(timestampNanos.getAsLong()),
                        start.toEpochMilli(),
                        step.toMillis()
                    );
                    return new TestCaseSupplier.TestCase(
                        args,
                        Matchers.startsWith("AddDateNanosEvaluator["),
                        DataType.DATE_NANOS,
                        equalTo(DateUtils.toNanoSeconds(expectedMillis))
                    ).withConfiguration(
                        TEST_SOURCE,
                        randomConfigurationBuilder().query(TEST_SOURCE.text()).now(end).zoneId(ZoneOffset.UTC).build()
                    );
                }));
            }
        }
    }

    private static TestCaseSupplier.TypedData dateBound(String name, DataType type, Instant instant) {
        Object value;
        if (type == DataType.DATETIME) {
            value = instant.toEpochMilli();
        } else if (type == DataType.DATE_NANOS) {
            value = DateUtils.toLong(instant);
        } else {
            value = new BytesRef(instant.toString());
        }
        return new TestCaseSupplier.TypedData(value, type, name).forceLiteral();
    }

    private static Matcher<Object> resultsMatcher(List<TestCaseSupplier.TypedData> typedData, long stepMillis, Instant now) {
        if (typedData.get(1).type() == DataType.DATE_NANOS) {
            long tsNanos = ((Number) typedData.get(1).data()).longValue();
            long expectedMillis = alignedBucketEnd(DateUtils.toMilliSeconds(tsNanos), now.toEpochMilli(), stepMillis);
            return equalTo(DateUtils.toNanoSeconds(expectedMillis));
        }
        long tsMillis = ((Number) typedData.get(1).data()).longValue();
        return equalTo(alignedBucketEnd(tsMillis, now.toEpochMilli(), stepMillis));
    }

    @Override
    protected Expression buildWithConfiguration(Source source, List<Expression> args, Configuration configuration) {
        if (args.size() == 4) {
            return new TStep(source, args.get(0), args.get(1), args.get(2), args.get(3), configuration);
        }
        var anchor = configuration.now();
        return new TStep(source, args.get(0), args.get(1), configuration).withTimestampBounds(
            Literal.dateTime(source, anchor),
            Literal.dateTime(source, anchor)
        );
    }

    @Override
    protected boolean canSerialize() {
        return false;
    }

    public static List<DocsV3Support.Param> signatureTypes(List<DocsV3Support.Param> params) {
        assertThat(params.size(), anyOf(equalTo(2), equalTo(4)));
        int tsIndex = params.size() - 1;
        assertThat(params.get(tsIndex).dataType(), anyOf(equalTo(DataType.DATE_NANOS), equalTo(DataType.DATETIME)));
        return params.subList(0, tsIndex);
    }

    public void testBucketBoundariesAlignToRangeStart() {
        Duration step = Duration.ofMinutes(5);
        Instant start = Instant.parse("2024-01-01T12:04:00Z");
        Instant end = Instant.parse("2024-01-01T12:32:00Z");

        assertThat(
            evaluateWithBounds(step, start, end, millis("2024-01-01T12:04:00Z"), DataType.DATETIME, ZoneOffset.UTC),
            equalTo(millis("2024-01-01T12:04:00Z"))
        );
        assertThat(
            evaluateWithBounds(step, start, end, millis("2024-01-01T12:06:00Z"), DataType.DATETIME, ZoneOffset.UTC),
            equalTo(millis("2024-01-01T12:09:00Z"))
        );
        assertThat(
            evaluateWithBounds(step, start, end, millis("2024-01-01T12:08:00Z"), DataType.DATETIME, ZoneOffset.UTC),
            equalTo(millis("2024-01-01T12:09:00Z"))
        );
        assertThat(
            evaluateWithBounds(step, start, end, millis("2024-01-01T12:32:00Z"), DataType.DATETIME, ZoneOffset.UTC),
            equalTo(millis("2024-01-01T12:34:00Z"))
        );
    }

    public void testTStepDiffersFromTBucket() {
        Duration step = Duration.ofMinutes(5);
        Instant start = Instant.parse("2024-01-01T12:02:00Z");
        Instant end = Instant.parse("2024-01-01T12:02:00Z");
        long timestamp = millis("2024-01-01T12:06:00Z");

        long tstepBucket = evaluateWithBounds(step, start, end, timestamp, DataType.DATETIME, ZoneOffset.UTC);

        Bucket tbucket = new Bucket(
            Source.EMPTY,
            Literal.dateTime(Source.EMPTY, Instant.EPOCH),
            Literal.timeDuration(Source.EMPTY, step),
            null,
            null,
            ConfigurationAware.CONFIGURATION_MARKER,
            0L
        );
        long tbucketBucket = tbucket.getDateRounding(FoldContext.small(), null, null).round(timestamp);

        assertThat(tstepBucket, equalTo(millis("2024-01-01T12:07:00Z")));
        assertThat(tbucketBucket, equalTo(millis("2024-01-01T12:05:00Z")));
        assertThat(tstepBucket, not(equalTo(tbucketBucket)));
    }

    public void testTimezoneInvariance() {
        Duration step = Duration.ofMinutes(5);
        Instant start = Instant.parse("2024-01-01T12:02:00Z");
        Instant end = Instant.parse("2024-01-01T12:02:00Z");
        long timestamp = millis("2024-01-01T12:08:00Z");
        long expected = evaluateWithBounds(step, start, end, timestamp, DataType.DATETIME, ZoneOffset.UTC);

        for (ZoneId zone : List.of(
            ZoneOffset.UTC,
            ZoneOffset.ofHours(5),
            ZoneOffset.ofHours(-7),
            ZoneId.of("America/New_York"),
            ZoneId.of("Europe/Berlin")
        )) {
            assertThat(
                "zone " + zone + " should produce same bucket",
                evaluateWithBounds(step, start, end, timestamp, DataType.DATETIME, zone),
                equalTo(expected)
            );
        }
    }

    public void testSingleBucketWhenStepExceedsRange() {
        Duration step = Duration.ofHours(2);
        Instant start = Instant.parse("2024-01-01T12:00:00Z");
        Instant end = Instant.parse("2024-01-01T12:30:00Z");

        assertThat(
            evaluateWithBounds(step, start, end, millis("2024-01-01T12:00:00Z"), DataType.DATETIME, ZoneOffset.UTC),
            equalTo(millis("2024-01-01T12:00:00Z"))
        );
        long bucket = evaluateWithBounds(step, start, end, millis("2024-01-01T12:15:00Z"), DataType.DATETIME, ZoneOffset.UTC);
        assertThat(bucket, equalTo(millis("2024-01-01T14:00:00Z")));
        assertThat(
            evaluateWithBounds(step, start, end, millis("2024-01-01T12:15:00Z"), DataType.DATETIME, ZoneOffset.UTC),
            equalTo(bucket)
        );
        assertThat(
            evaluateWithBounds(step, start, end, millis("2024-01-01T12:29:00Z"), DataType.DATETIME, ZoneOffset.UTC),
            equalTo(bucket)
        );
    }

    public void testTimestampExactlyAtEnd() {
        Duration step = Duration.ofMinutes(5);
        Instant start = Instant.parse("2024-01-01T12:00:00Z");
        Instant end = Instant.parse("2024-01-01T12:10:00Z");

        assertThat(
            evaluateWithBounds(step, start, end, millis("2024-01-01T12:10:00Z"), DataType.DATETIME, ZoneOffset.UTC),
            equalTo(millis("2024-01-01T12:10:00Z"))
        );
    }

    public void testRightClosedBoundary() {
        // Grid anchored at 12:15 with 1h step: boundaries at 12:15, 13:15, 14:15, ...
        // Intervals are (left, right]: t exactly on a boundary belongs to that bucket.
        Duration step = Duration.ofHours(1);
        Instant start = Instant.parse("2024-01-01T12:15:00Z");
        Instant end = Instant.parse("2024-01-01T15:15:00Z");

        // t = 13:15 exactly: right boundary of (12:15, 13:15] -> label 13:15
        assertThat(
            evaluateWithBounds(step, start, end, millis("2024-01-01T13:15:00Z"), DataType.DATETIME, ZoneOffset.UTC),
            equalTo(millis("2024-01-01T13:15:00Z"))
        );
        // t = 14:15 exactly: right boundary of (13:15, 14:15] -> label 14:15
        assertThat(
            evaluateWithBounds(step, start, end, millis("2024-01-01T14:15:00Z"), DataType.DATETIME, ZoneOffset.UTC),
            equalTo(millis("2024-01-01T14:15:00Z"))
        );
    }

    public void testJustPastBoundaryGoesToNextBucket() {
        // t one millisecond past a grid boundary falls into the next bucket.
        Duration step = Duration.ofHours(1);
        Instant start = Instant.parse("2024-01-01T12:15:00Z");
        Instant end = Instant.parse("2024-01-01T15:15:00Z");

        // t = 13:15:00.001: just past 13:15 boundary -> in (13:15, 14:15] -> label 14:15
        assertThat(
            evaluateWithBounds(step, start, end, millis("2024-01-01T13:15:00.001Z"), DataType.DATETIME, ZoneOffset.UTC),
            equalTo(millis("2024-01-01T14:15:00Z"))
        );
    }

    public void testMidBucketValue() {
        // t in the middle of a bucket maps to the right boundary.
        Duration step = Duration.ofHours(1);
        Instant start = Instant.parse("2024-01-01T12:15:00Z");
        Instant end = Instant.parse("2024-01-01T15:15:00Z");

        // t = 14:00: in (13:15, 14:15] -> label 14:15
        assertThat(
            evaluateWithBounds(step, start, end, millis("2024-01-01T14:00:00Z"), DataType.DATETIME, ZoneOffset.UTC),
            equalTo(millis("2024-01-01T14:15:00Z"))
        );
    }

    public void testOneMillisStep() {
        Duration step = Duration.ofMillis(1);
        Instant start = Instant.parse("2024-01-01T12:00:00Z");
        Instant end = Instant.parse("2024-01-01T12:00:00Z");
        long ts = millis("2024-01-01T12:34:56.789Z");

        assertThat(evaluateWithBounds(step, start, end, ts, DataType.DATETIME, ZoneOffset.UTC), equalTo(ts));
    }

    private static long evaluateWithBounds(
        Duration step,
        Instant start,
        Instant end,
        long timestamp,
        DataType timestampType,
        ZoneId zoneId
    ) {
        Configuration configuration = randomConfigurationBuilder().query(TEST_SOURCE.text()).now(end).zoneId(zoneId).build();
        Literal timestampLiteral = timestampLiteral(timestamp, timestampType);
        TStep tStep = (TStep) new TStep(Source.EMPTY, Literal.timeDuration(Source.EMPTY, step), timestampLiteral, configuration)
            .withTimestampBounds(timestampLiteral(start, timestampType), timestampLiteral(end, timestampType));
        return ((Number) tStep.surrogate().fold(FoldContext.small())).longValue();
    }

    private static Literal timestampLiteral(Instant instant, DataType type) {
        return timestampLiteral(type == DataType.DATE_NANOS ? DateUtils.toLong(instant) : instant.toEpochMilli(), type);
    }

    private static Literal timestampLiteral(long value, DataType type) {
        return new Literal(Source.EMPTY, value, type);
    }

    private static long millis(String iso) {
        return Instant.parse(iso).toEpochMilli();
    }

    private static long alignedBucketEnd(long timestampMillis, long endMillis, long stepMillis) {
        long offset = DateUtils.floorRemainder(endMillis, stepMillis);
        return Math.floorDiv((timestampMillis - 1L) - offset, stepMillis) * stepMillis + offset + stepMillis;
    }
}
