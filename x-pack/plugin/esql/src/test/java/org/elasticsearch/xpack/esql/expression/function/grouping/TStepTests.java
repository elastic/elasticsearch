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
import org.elasticsearch.common.Rounding;
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
    private static final DataType[] TIME_RANGE_TYPES = new DataType[] { DataType.DATETIME, DataType.DATE_NANOS, DataType.KEYWORD };
    private static final DataType[] TIMESTAMP_TYPES = new DataType[] { DataType.DATETIME, DataType.DATE_NANOS };
    private static final DataType[] COUNT_STEP_TYPES = { DataType.INTEGER, DataType.LONG };

    public TStepTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        List<TestCaseSupplier> suppliers = new ArrayList<>();
        casesWithStep(
            suppliers,
            "5 minute step uses query now",
            DataType.DATETIME,
            () -> Instant.parse("2024-01-01T12:09:00Z").toEpochMilli(),
            Duration.ofMinutes(5),
            Instant.parse("2024-01-01T12:12:00Z")
        );
        casesWithStep(
            suppliers,
            "date_nanos 30 second step uses query now",
            DataType.DATE_NANOS,
            () -> DateUtils.toLong(Instant.parse("2024-01-01T12:01:01Z")),
            Duration.ofSeconds(30),
            Instant.parse("2024-01-01T12:02:00Z")
        );
        casesWithExplicitBounds(
            suppliers,
            "explicit bounds 1 hour step",
            DataType.DATETIME,
            () -> Instant.parse("2024-01-01T12:45:00Z").toEpochMilli(),
            Duration.ofHours(1),
            Instant.parse("2024-01-01T12:15:00Z"),
            Instant.parse("2024-01-01T13:55:00Z")
        );
        casesWithExplicitBounds(
            suppliers,
            "date_nanos explicit bounds 5 minute step",
            DataType.DATE_NANOS,
            () -> DateUtils.toLong(Instant.parse("2024-01-01T12:09:00Z")),
            Duration.ofMinutes(5),
            Instant.parse("2024-01-01T12:04:00Z"),
            Instant.parse("2024-01-01T12:32:00Z")
        );
        // count=2 over a 1h40m range -> derives exact 50 minute step from the requested range
        casesWithTargetBucketCount(
            suppliers,
            "bucket count 2 over 1h40m range gives 50 minute step",
            DataType.DATETIME,
            () -> Instant.parse("2024-01-01T12:45:00Z").toEpochMilli(),
            2,
            Instant.parse("2024-01-01T12:15:00Z"),
            Instant.parse("2024-01-01T13:55:00Z"),
            Instant.parse("2024-01-01T13:05:00Z")
        );
        casesWithTargetBucketCount(
            suppliers,
            "date_nanos bucket count 2 over 1h40m range gives 50 minute step",
            DataType.DATE_NANOS,
            () -> DateUtils.toLong(Instant.parse("2024-01-01T12:45:00Z")),
            2,
            Instant.parse("2024-01-01T12:15:00Z"),
            Instant.parse("2024-01-01T13:55:00Z"),
            Instant.parse("2024-01-01T13:05:00Z")
        );
        casesWithTargetBucketCount(
            suppliers,
            "bucket count rounds derived step up to whole seconds",
            DataType.DATETIME,
            () -> Instant.parse("2024-01-01T10:01:00Z").toEpochMilli(),
            10,
            Instant.parse("2024-01-01T10:00:00Z"),
            Instant.parse("2024-01-01T10:10:01Z"),
            Instant.parse("2024-01-01T10:01:01Z")
        );
        casesWithTargetBucketCount(
            suppliers,
            "date_nanos bucket count rounds derived step up to whole seconds",
            DataType.DATE_NANOS,
            () -> DateUtils.toLong(Instant.parse("2024-01-01T10:01:00Z")),
            10,
            Instant.parse("2024-01-01T10:00:00Z"),
            Instant.parse("2024-01-01T10:10:01Z"),
            Instant.parse("2024-01-01T10:01:01Z")
        );
        casesWithTargetBucketCount(
            suppliers,
            "misaligned bucket count 60 over 1h range keeps 1 minute step",
            DataType.DATETIME,
            () -> Instant.parse("2024-01-01T10:01:10Z").toEpochMilli(),
            60,
            Instant.parse("2024-01-01T10:00:30Z"),
            Instant.parse("2024-01-01T11:00:30Z"),
            Instant.parse("2024-01-01T10:01:30Z")
        );
        casesWithTargetBucketCount(
            suppliers,
            "date_nanos misaligned bucket count 60 over 1h range keeps 1 minute step",
            DataType.DATE_NANOS,
            () -> DateUtils.toLong(Instant.parse("2024-01-01T10:01:10Z")),
            60,
            Instant.parse("2024-01-01T10:00:30Z"),
            Instant.parse("2024-01-01T11:00:30Z"),
            Instant.parse("2024-01-01T10:01:30Z")
        );
        return parameterSuppliersFromTypedData(suppliers);
    }

    private static void casesWithStep(
        List<TestCaseSupplier> suppliers,
        String name,
        DataType timestampType,
        LongSupplier timestamp,
        Duration step,
        Instant now
    ) {
        suppliers.add(new TestCaseSupplier(name, List.of(DataType.TIME_DURATION, timestampType), () -> {
            List<TestCaseSupplier.TypedData> args = new ArrayList<>();
            args.add(new TestCaseSupplier.TypedData(step, DataType.TIME_DURATION, "step").forceLiteral());
            args.add(createTypedData("@timestamp", timestampType, timestamp.getAsLong()));
            return new TestCaseSupplier.TestCase(
                args,
                timestampType == DataType.DATE_NANOS
                    ? Matchers.startsWith("DateTruncDateNanosEvaluator[")
                    : Matchers.startsWith("DateTruncDatetimeEvaluator["),
                timestampType,
                matcher(args, step.toMillis(), now)
            ).withConfiguration(
                TEST_SOURCE,
                randomConfigurationBuilder().query(TEST_SOURCE.text())
                    .now(now)
                    .zoneId(timestampType == DataType.DATE_NANOS ? ZoneOffset.ofHours(-7) : ZoneOffset.ofHours(5))
                    .build()
            );
        }));
    }

    private static void casesWithExplicitBounds(
        List<TestCaseSupplier> suppliers,
        String name,
        DataType timestampType,
        LongSupplier timestamp,
        Duration step,
        Instant start,
        Instant end
    ) {
        for (DataType fromType : TIME_RANGE_TYPES) {
            for (DataType toType : TIME_RANGE_TYPES) {
                suppliers.add(new TestCaseSupplier(name, List.of(DataType.TIME_DURATION, fromType, toType, timestampType), () -> {
                    List<TestCaseSupplier.TypedData> args = new ArrayList<>();
                    args.add(new TestCaseSupplier.TypedData(step, DataType.TIME_DURATION, "step").forceLiteral());
                    args.add(createTypedData("from", fromType, start));
                    args.add(createTypedData("to", toType, end));
                    args.add(createTypedData("@timestamp", timestampType, timestamp.getAsLong()));
                    long expectedMillis = alignedBucketEnd(
                        timestampType == DataType.DATE_NANOS ? DateUtils.toMilliSeconds(timestamp.getAsLong()) : timestamp.getAsLong(),
                        start.toEpochMilli(),
                        step.toMillis()
                    );
                    return new TestCaseSupplier.TestCase(
                        args,
                        timestampType == DataType.DATE_NANOS
                            ? Matchers.startsWith("DateTruncDateNanosEvaluator[")
                            : Matchers.startsWith("DateTruncDatetimeEvaluator["),
                        timestampType,
                        equalTo(encodedTimestamp(expectedMillis, timestampType))
                    ).withConfiguration(
                        TEST_SOURCE,
                        randomConfigurationBuilder().query(TEST_SOURCE.text()).now(end).zoneId(ZoneOffset.UTC).build()
                    );
                }));
            }
        }
    }

    private static void casesWithTargetBucketCount(
        List<TestCaseSupplier> suppliers,
        String name,
        DataType timestampType,
        LongSupplier timestamp,
        int count,
        Instant start,
        Instant end,
        Instant expectedBucket
    ) {
        for (DataType stepType : COUNT_STEP_TYPES) {
            for (DataType fromType : TIME_RANGE_TYPES) {
                for (DataType toType : TIME_RANGE_TYPES) {
                    suppliers.add(new TestCaseSupplier(name, List.of(stepType, fromType, toType, timestampType), () -> {
                        List<TestCaseSupplier.TypedData> args = new ArrayList<>();
                        Object stepValue = stepType == DataType.LONG ? (long) count : count;
                        args.add(new TestCaseSupplier.TypedData(stepValue, stepType, "step").forceLiteral());
                        args.add(createTypedData("from", fromType, start));
                        args.add(createTypedData("to", toType, end));
                        args.add(createTypedData("@timestamp", timestampType, timestamp.getAsLong()));
                        return new TestCaseSupplier.TestCase(
                            args,
                            timestampType == DataType.DATE_NANOS
                                ? Matchers.startsWith("DateTruncDateNanosEvaluator[")
                                : Matchers.startsWith("DateTruncDatetimeEvaluator["),
                            timestampType,
                            equalTo(encodedTimestamp(expectedBucket.toEpochMilli(), timestampType))
                        ).withConfiguration(
                            TEST_SOURCE,
                            randomConfigurationBuilder().query(TEST_SOURCE.text()).now(end).zoneId(ZoneOffset.UTC).build()
                        );
                    }));
                }
            }
        }
    }

    private static TestCaseSupplier.TypedData createTypedData(String name, DataType type, Instant instant) {
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

    private static TestCaseSupplier.TypedData createTypedData(String name, DataType type, long value) {
        return new TestCaseSupplier.TypedData(value, type, name);
    }

    private static Matcher<Object> matcher(List<TestCaseSupplier.TypedData> typedData, long stepMillis, Instant now) {
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

    public void testAnchorsGridAtRangeStart() {
        for (DataType timestampType : TIMESTAMP_TYPES) {
            Duration step = Duration.ofMinutes(5);
            Instant start = Instant.parse("2024-01-01T12:04:00Z");
            Instant end = Instant.parse("2024-01-01T12:32:00Z");

            assertThat(
                evaluateWithBounds(step, start, end, Instant.parse("2024-01-01T12:04:00Z"), timestampType, ZoneOffset.UTC),
                equalTo(encodedTimestamp(Instant.parse("2024-01-01T12:04:00Z"), timestampType))
            );
            assertThat(
                evaluateWithBounds(step, start, end, Instant.parse("2024-01-01T12:06:00Z"), timestampType, ZoneOffset.UTC),
                equalTo(encodedTimestamp(Instant.parse("2024-01-01T12:09:00Z"), timestampType))
            );
            assertThat(
                evaluateWithBounds(step, start, end, Instant.parse("2024-01-01T12:08:00Z"), timestampType, ZoneOffset.UTC),
                equalTo(encodedTimestamp(Instant.parse("2024-01-01T12:09:00Z"), timestampType))
            );
            assertThat(
                evaluateWithBounds(step, start, end, Instant.parse("2024-01-01T12:32:00Z"), timestampType, ZoneOffset.UTC),
                equalTo(encodedTimestamp(Instant.parse("2024-01-01T12:34:00Z"), timestampType))
            );
        }
    }

    public void testUsesRangeStartAnchoringUnlikeTBucket() {
        Duration step = Duration.ofMinutes(5);
        Instant start = Instant.parse("2024-01-01T12:02:00Z");
        Instant end = Instant.parse("2024-01-01T12:02:00Z");
        Instant timestamp = Instant.parse("2024-01-01T12:06:00Z");

        long tstepBucket = evaluateWithBounds(step, start, end, timestamp, DataType.DATETIME, ZoneOffset.UTC);

        Bucket tbucket = new Bucket(
            Source.EMPTY,
            Literal.dateTime(Source.EMPTY, Instant.EPOCH),
            Literal.timeDuration(Source.EMPTY, step),
            null,
            null,
            ConfigurationAware.CONFIGURATION_MARKER,
            0L,
            Rounding.RoundingConvention.DOWN
        );
        long tbucketBucket = tbucket.getDateRounding(FoldContext.small(), null, null).round(timestamp.toEpochMilli());

        assertThat(tstepBucket, equalTo(millis("2024-01-01T12:07:00Z")));
        assertThat(tbucketBucket, equalTo(millis("2024-01-01T12:05:00Z")));
        assertThat(tstepBucket, not(equalTo(tbucketBucket)));
    }

    public void testUsesUtcGridRegardlessOfQueryZone() {
        for (DataType timestampType : TIMESTAMP_TYPES) {
            Duration step = Duration.ofMinutes(5);
            Instant start = Instant.parse("2024-01-01T12:02:00Z");
            Instant end = Instant.parse("2024-01-01T12:02:00Z");
            Instant timestamp = Instant.parse("2024-01-01T12:08:00Z");
            long expected = evaluateWithBounds(step, start, end, timestamp, timestampType, ZoneOffset.UTC);

            for (ZoneId zone : List.of(
                ZoneOffset.UTC,
                ZoneOffset.ofHours(5),
                ZoneOffset.ofHours(-7),
                ZoneId.of("America/New_York"),
                ZoneId.of("Europe/Berlin")
            )) {
                assertThat(
                    "zone " + zone + " should produce same bucket",
                    evaluateWithBounds(step, start, end, timestamp, timestampType, zone),
                    equalTo(expected)
                );
            }
        }
    }

    public void testReturnsSingleRightLabeledBucketWhenStepExceedsRange() {
        for (DataType timestampType : TIMESTAMP_TYPES) {
            Duration step = Duration.ofHours(2);
            Instant start = Instant.parse("2024-01-01T12:00:00Z");
            Instant end = Instant.parse("2024-01-01T12:30:00Z");

            assertThat(
                evaluateWithBounds(step, start, end, Instant.parse("2024-01-01T12:00:00Z"), timestampType, ZoneOffset.UTC),
                equalTo(encodedTimestamp(Instant.parse("2024-01-01T12:00:00Z"), timestampType))
            );
            long bucket = evaluateWithBounds(step, start, end, Instant.parse("2024-01-01T12:15:00Z"), timestampType, ZoneOffset.UTC);
            assertThat(bucket, equalTo(encodedTimestamp(Instant.parse("2024-01-01T14:00:00Z"), timestampType)));
            assertThat(
                evaluateWithBounds(step, start, end, Instant.parse("2024-01-01T12:15:00Z"), timestampType, ZoneOffset.UTC),
                equalTo(bucket)
            );
            assertThat(
                evaluateWithBounds(step, start, end, Instant.parse("2024-01-01T12:29:00Z"), timestampType, ZoneOffset.UTC),
                equalTo(bucket)
            );
        }
    }

    public void testUsesLeftOpenRightClosedIntervals() {
        // Grid anchored at 12:15 with 1h step: boundaries at 12:15, 13:15, 14:15, ...
        // Intervals are (left, right]: t exactly on a boundary belongs to that bucket.
        for (DataType timestampType : TIMESTAMP_TYPES) {
            Duration step = Duration.ofHours(1);
            Instant start = Instant.parse("2024-01-01T12:15:00Z");
            Instant end = Instant.parse("2024-01-01T15:15:00Z");

            assertThat(
                evaluateWithBounds(step, start, end, Instant.parse("2024-01-01T13:15:00Z"), timestampType, ZoneOffset.UTC),
                equalTo(encodedTimestamp(Instant.parse("2024-01-01T13:15:00Z"), timestampType))
            );
            assertThat(
                evaluateWithBounds(step, start, end, Instant.parse("2024-01-01T14:15:00Z"), timestampType, ZoneOffset.UTC),
                equalTo(encodedTimestamp(Instant.parse("2024-01-01T14:15:00Z"), timestampType))
            );
            assertThat(
                evaluateWithBounds(step, start, end, Instant.parse("2024-01-01T13:15:00.001Z"), timestampType, ZoneOffset.UTC),
                equalTo(encodedTimestamp(Instant.parse("2024-01-01T14:15:00Z"), timestampType))
            );
        }
    }

    public void testOneMillisecondStepPreservesTimestamp() {
        for (DataType timestampType : TIMESTAMP_TYPES) {
            Duration step = Duration.ofMillis(1);
            Instant start = Instant.parse("2024-01-01T12:00:00Z");
            Instant end = Instant.parse("2024-01-01T12:00:00Z");
            Instant timestamp = Instant.parse("2024-01-01T12:34:56.789Z");

            assertThat(
                evaluateWithBounds(step, start, end, timestamp, timestampType, ZoneOffset.UTC),
                equalTo(encodedTimestamp(timestamp, timestampType))
            );
        }
    }

    public void testBucketCountPreservesSubSecondDerivedStep() {
        for (DataType timestampType : TIMESTAMP_TYPES) {
            Instant start = Instant.parse("2024-01-01T12:00:00.100Z");
            Instant end = Instant.parse("2024-01-01T12:00:01.000Z");

            assertThat(
                evaluateWithBucketCount(3, start, end, Instant.parse("2024-01-01T12:00:00.450Z"), timestampType, ZoneOffset.UTC),
                equalTo(encodedTimestamp(Instant.parse("2024-01-01T12:00:00.700Z"), timestampType))
            );
        }
    }

    public void testBucketCountDoesNotRoundExactOneSecondStep() {
        for (DataType timestampType : TIMESTAMP_TYPES) {
            Instant start = Instant.parse("2024-01-01T12:00:00.000Z");
            Instant end = Instant.parse("2024-01-01T12:00:03.000Z");

            assertThat(
                evaluateWithBucketCount(3, start, end, Instant.parse("2024-01-01T12:00:01.500Z"), timestampType, ZoneOffset.UTC),
                equalTo(encodedTimestamp(Instant.parse("2024-01-01T12:00:02.000Z"), timestampType))
            );
        }
    }

    public void testBucketCountFallsBackToOneMillisecondWhenRangeIsZero() {
        for (DataType timestampType : TIMESTAMP_TYPES) {
            Instant start = Instant.parse("2024-01-01T12:00:00.000Z");

            assertThat(
                evaluateWithBucketCount(10, start, start, start, timestampType, ZoneOffset.UTC),
                equalTo(encodedTimestamp(start, timestampType))
            );
            assertThat(
                evaluateWithBucketCount(10, start, start, Instant.parse("2024-01-01T12:00:00.007Z"), timestampType, ZoneOffset.UTC),
                equalTo(encodedTimestamp(Instant.parse("2024-01-01T12:00:00.007Z"), timestampType))
            );
        }
    }

    private static long evaluateWithBounds(
        Duration step,
        Instant start,
        Instant end,
        Instant timestamp,
        DataType timestampType,
        ZoneId zoneId
    ) {
        Configuration configuration = randomConfigurationBuilder().query(TEST_SOURCE.text()).now(end).zoneId(zoneId).build();
        Literal timestampLiteral = timestampLiteral(timestamp, timestampType);
        TStep tStep = (TStep) new TStep(Source.EMPTY, Literal.timeDuration(Source.EMPTY, step), timestampLiteral, configuration)
            .withTimestampBounds(timestampLiteral(start, timestampType), timestampLiteral(end, timestampType));
        return ((Number) tStep.surrogate().fold(FoldContext.small())).longValue();
    }

    private static long evaluateWithBucketCount(
        int count,
        Instant start,
        Instant end,
        Instant timestamp,
        DataType timestampType,
        ZoneId zoneId
    ) {
        Configuration configuration = randomConfigurationBuilder().query(TEST_SOURCE.text()).now(end).zoneId(zoneId).build();
        Literal timestampLiteral = timestampLiteral(timestamp, timestampType);
        TStep tStep = new TStep(
            Source.EMPTY,
            new Literal(Source.EMPTY, count, DataType.INTEGER),
            timestampLiteral(start, timestampType),
            timestampLiteral(end, timestampType),
            timestampLiteral,
            configuration
        );
        return ((Number) tStep.surrogate().fold(FoldContext.small())).longValue();
    }

    private static Literal timestampLiteral(Instant instant, DataType type) {
        return timestampLiteral(type == DataType.DATE_NANOS ? DateUtils.toLong(instant) : instant.toEpochMilli(), type);
    }

    private static long encodedTimestamp(Instant instant, DataType type) {
        return encodedTimestamp(instant.toEpochMilli(), type);
    }

    private static long encodedTimestamp(long epochMillis, DataType type) {
        return type == DataType.DATE_NANOS ? DateUtils.toNanoSeconds(epochMillis) : epochMillis;
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
