/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.date;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.common.time.DateUtils;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.DocsV3Support;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;
import org.elasticsearch.xpack.esql.expression.function.scalar.AbstractConfigurationFunctionTestCase;
import org.elasticsearch.xpack.esql.session.Configuration;
import org.hamcrest.Matchers;

import java.time.Duration;
import java.time.Instant;
import java.time.Period;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.temporal.TemporalAmount;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import static java.time.Instant.ofEpochSecond;
import static java.time.Instant.parse;
import static org.elasticsearch.xpack.esql.ConfigurationTestUtils.randomConfigurationBuilder;
import static org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier.TEST_SOURCE;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter.DEFAULT_DATE_TIME_FORMATTER;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter.dateTimeToLong;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

public class TRangeTests extends AbstractConfigurationFunctionTestCase {

    public TRangeTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    /**
     * @param value Either a Duration (for TIME_DURATION), a Period (for DATE_PERIOD), or an Instant
     */
    record TestParameter(DataType dataType, Object value) {}

    record SingleParameterCase(
        DataType argumentDataType,
        Object argumentValue,
        DataType timestampDataType,
        Instant timestampValue,
        ZoneId timezone,
        Instant now,
        Instant expectedStartTime,
        boolean expectedResult
    ) {}

    record TwoParameterCase(
        DataType argument1DataType,
        Object argument1Value,
        DataType argument2DataType,
        Object argument2Value,
        DataType timestampDataType,
        Instant timestampValue,
        ZoneId timezone,
        boolean expectedResult
    ) {}

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        List<TestCaseSupplier> suppliers = new ArrayList<>();

        singleParameterTRangeSuppliers(suppliers, singleParameterTestCases());
        twoParameterTRangeSuppliers(suppliers, twoParameterAbsoluteTimeTestCases());

        return parameterSuppliersFromTypedData(suppliers);
    }

    private static List<SingleParameterCase> singleParameterTestCases() {
        Instant fixedNow = parse("2024-01-05T15:00:00Z");
        List<TestParameter> testParameters = List.of(
            new TestParameter(DataType.TIME_DURATION, Duration.ofHours(1)),
            new TestParameter(DataType.DATE_PERIOD, Period.ofDays(1))
        );

        List<SingleParameterCase> testCases = new ArrayList<>();
        for (DataType timestampDataType : List.of(DataType.DATETIME, DataType.DATE_NANOS)) {
            // UTC
            for (TestParameter testParameter : testParameters) {
                Instant expectedStartTime = fixedNow.minus((TemporalAmount) testParameter.value);

                Instant timestampInsideRange = timestampInRange(expectedStartTime, fixedNow);
                testCases.add(
                    new SingleParameterCase(
                        testParameter.dataType,
                        testParameter.value,
                        timestampDataType,
                        timestampInsideRange,
                        ZoneOffset.UTC,
                        fixedNow,
                        expectedStartTime,
                        true
                    )
                );

                Instant timestampOutsideRange = fixedNow.plus(Duration.ofMinutes(10));
                testCases.add(
                    new SingleParameterCase(
                        testParameter.dataType,
                        testParameter.value,
                        timestampDataType,
                        timestampOutsideRange,
                        ZoneOffset.UTC,
                        fixedNow,
                        expectedStartTime,
                        false
                    )
                );
            }
            testCases.add(
                new SingleParameterCase(
                    DataType.DATE_PERIOD,
                    Period.ofDays(1),
                    timestampDataType,
                    parse("2020-02-03T10:11:12.123456780Z"),
                    ZoneOffset.UTC,
                    parse("2020-02-03T10:11:12.123456789Z"),
                    parse("2020-02-02T10:11:12.123456789Z"),
                    true
                )
            );

            // Timezones
            testCases.add(
                new SingleParameterCase(
                    DataType.DATE_PERIOD,
                    Period.ofDays(2),
                    timestampDataType,
                    parse("2025-03-07T03:00:00.123456-05:00"),
                    ZoneId.of("America/New_York"),
                    parse("2025-03-09T03:00:00.123456-04:00"),
                    parse("2025-03-07T03:00:00.123456-05:00"),
                    false
                )
            );
            testCases.add(
                new SingleParameterCase(
                    DataType.DATE_PERIOD,
                    Period.ofDays(2),
                    timestampDataType,
                    parse("2025-03-07T03:00:59.123456-05:00"),
                    ZoneId.of("America/New_York"),
                    parse("2025-03-09T03:00:00.123456-04:00"),
                    parse("2025-03-07T03:00:00.123456-05:00"),
                    true
                )
            );
            testCases.add(
                new SingleParameterCase(
                    DataType.DATE_PERIOD,
                    Period.ofDays(2),
                    timestampDataType,
                    parse("2025-03-09T03:00:00.123456-04:00"),
                    ZoneId.of("America/New_York"),
                    parse("2025-03-09T03:00:00.123456-04:00"),
                    parse("2025-03-07T03:00:00.123456-05:00"),
                    true
                )
            );
            testCases.add(
                new SingleParameterCase(
                    DataType.DATE_PERIOD,
                    Period.ofDays(2),
                    timestampDataType,
                    parse("2025-10-31T01:00:00.123456-04:00"),
                    ZoneId.of("America/New_York"),
                    parse("2025-11-02T01:00:00.123456-05:00"),
                    parse("2025-10-31T01:00:00.123456-04:00"),
                    false
                )
            );
            testCases.add(
                new SingleParameterCase(
                    DataType.DATE_PERIOD,
                    Period.ofDays(2),
                    timestampDataType,
                    parse("2025-10-31T05:00:01.123456-04:00"),
                    ZoneId.of("America/New_York"),
                    parse("2025-11-02T05:00:00.123456-05:00"),
                    parse("2025-10-31T05:00:00.123456-04:00"),
                    true
                )
            );
        }

        // Nanos edge case
        testCases.add(
            new SingleParameterCase(
                DataType.DATE_PERIOD,
                Period.ofDays(1),
                DataType.DATETIME,
                parse("2020-02-03T10:11:12.123456799Z"),
                ZoneOffset.UTC,
                parse("2020-02-03T10:11:12.123456789Z"),
                parse("2020-02-02T10:11:12.123456789Z"),
                true
            )
        );

        return testCases;
    }

    private static void singleParameterTRangeSuppliers(List<TestCaseSupplier> suppliers, List<SingleParameterCase> testCases) {
        for (SingleParameterCase testCase : testCases) {
            long value1 = toAbsoluteTime(
                testCase.argumentValue,
                testCase.argumentDataType,
                testCase.now,
                testCase.timestampDataType == DataType.DATE_NANOS,
                testCase.timezone
            );
            long value2 = toAbsoluteTime(
                testCase.now,
                testCase.timestampDataType,
                testCase.now,
                testCase.timestampDataType == DataType.DATE_NANOS,
                testCase.timezone
            );
            long timestampValue = toAbsoluteTime(
                testCase.timestampValue,
                testCase.timestampDataType,
                testCase.now,
                testCase.timestampDataType == DataType.DATE_NANOS,
                testCase.timezone
            );

            long expectedStartTime = toAbsoluteTime(
                testCase.expectedStartTime,
                testCase.timestampDataType,
                testCase.now,
                testCase.timestampDataType == DataType.DATE_NANOS,
                testCase.timezone
            );

            suppliers.add(new TestCaseSupplier(List.of(testCase.argumentDataType, testCase.timestampDataType), () -> {
                assertThat(value1, equalTo(expectedStartTime));

                return new TestCaseSupplier.TestCase(
                    List.of(
                        new TestCaseSupplier.TypedData(testCase.argumentValue, testCase.argumentDataType, "start_time_or_interval")
                            .forceLiteral(),
                        new TestCaseSupplier.TypedData(timestampValue, testCase.timestampDataType, "@timestamp")
                    ),
                    Matchers.equalTo(
                        "BooleanLogicExpressionEvaluator[bl=source, "
                            + "leftEval=GreaterThanLongsEvaluator[lhs=Attribute[channel=0], rhs=LiteralsEvaluator[lit="
                            + value1
                            + "]], rightEval=LessThanOrEqualLongsEvaluator[lhs=Attribute[channel=0], rhs=LiteralsEvaluator[lit="
                            + value2
                            + "]]]"
                    ),
                    DataType.BOOLEAN,
                    equalTo(testCase.expectedResult)
                ).withConfiguration(
                    TEST_SOURCE,
                    randomConfigurationBuilder().query(TestCaseSupplier.TEST_SOURCE.text())
                        .now(testCase.now())
                        .zoneId(testCase.timezone)
                        .build()
                );
            }));
        }
    }

    private static List<TwoParameterCase> twoParameterAbsoluteTimeTestCases() {
        List<TestParameter[]> testParameters = List.of(
            new TestParameter[] {
                new TestParameter(DataType.LONG, parse("2024-01-01T00:00:00.123456789Z")),
                new TestParameter(DataType.LONG, parse("2024-01-01T12:00:00.123456789Z")) },
            new TestParameter[] {
                new TestParameter(DataType.DATETIME, parse("2024-01-01T00:00:00.123456789Z")),
                new TestParameter(DataType.DATETIME, parse("2024-01-01T12:00:00.123456789Z")), },
            new TestParameter[] {
                new TestParameter(DataType.DATE_NANOS, parse("2024-01-01T00:00:00.123456789Z")),
                new TestParameter(DataType.DATE_NANOS, parse("2024-01-01T12:00:00.123456789Z")), }
        );

        List<TwoParameterCase> testCases = new ArrayList<>();
        for (DataType timestampDataType : List.of(DataType.DATETIME, DataType.DATE_NANOS)) {
            for (TestParameter[] testParameter : testParameters) {
                Instant timestampInsideRange = timestampInRange((Instant) testParameter[0].value, (Instant) testParameter[1].value);
                testCases.add(
                    new TwoParameterCase(
                        testParameter[0].dataType,
                        testParameter[0].value,
                        testParameter[1].dataType,
                        testParameter[1].value,
                        timestampDataType,
                        timestampInsideRange,
                        null,
                        true
                    )
                );

                Instant timestampOutsideRange = ((Instant) testParameter[0].value).minus(Duration.ofMinutes(10));
                testCases.add(
                    new TwoParameterCase(
                        testParameter[0].dataType,
                        testParameter[0].value,
                        testParameter[1].dataType,
                        testParameter[1].value,
                        timestampDataType,
                        timestampOutsideRange,
                        null,
                        false
                    )
                );
            }

            // String parameters cases
            testCases.addAll(
                List.of(
                    new TwoParameterCase(
                        DataType.KEYWORD,
                        "2020-02-02T01:02:03.123456789Z",
                        DataType.KEYWORD,
                        "2020-02-02T01:02:04.123456789Z",
                        timestampDataType,
                        parse("2020-02-02T01:02:03.223456789Z"),
                        ZoneId.of("+11:45"),
                        true
                    ),
                    new TwoParameterCase(
                        DataType.KEYWORD,
                        "2020-02-02T01:02:03.123456789+03:00",
                        DataType.KEYWORD,
                        "2020-02-02T01:02:04.123456789+03:00",
                        timestampDataType,
                        parse("2020-02-02T01:02:03.223456789+03:00"),
                        ZoneId.of("-11:45"),
                        true
                    ),
                    new TwoParameterCase(
                        DataType.KEYWORD,
                        "2020-02-02T01:02:03.123456789",
                        DataType.KEYWORD,
                        "2020-02-02T01:02:04.123456789",
                        timestampDataType,
                        parse("2020-02-02T01:02:03.223456789+01:00"),
                        ZoneId.of("Europe/Paris"),
                        true
                    ),
                    new TwoParameterCase(
                        DataType.KEYWORD,
                        "2020-02-02T01:02:03.123456789",
                        DataType.KEYWORD,
                        "2020-02-02T01:02:04.123456789",
                        timestampDataType,
                        parse("2020-02-02T01:02:03.223456789Z"),
                        ZoneId.of("Europe/Paris"),
                        false
                    )
                )
            );
        }
        return testCases;
    }

    private static void twoParameterTRangeSuppliers(List<TestCaseSupplier> suppliers, List<TwoParameterCase> testCases) {
        for (TwoParameterCase testCase : testCases) {
            boolean isTimestampNanos = testCase.timestampDataType == DataType.DATE_NANOS;
            boolean isStringParam = testCase.argument1DataType == DataType.KEYWORD;
            long longValue1 = toAbsoluteTime(
                testCase.argument1Value,
                testCase.argument1DataType,
                null,
                isTimestampNanos,
                testCase.timezone
            );
            long longValue2 = toAbsoluteTime(
                testCase.argument2Value,
                testCase.argument2DataType,
                null,
                isTimestampNanos,
                testCase.timezone
            );
            long timestampValue = toAbsoluteTime(
                testCase.timestampValue,
                testCase.timestampDataType,
                null,
                isTimestampNanos,
                testCase.timezone
            );
            Object value1 = isStringParam ? testCase.argument1Value : longValue1;
            Object value2 = isStringParam ? testCase.argument2Value : longValue2;

            // Values are converted to the timestamp's type
            long expectedValue1 = makeExpectedValue(longValue1, testCase.argument1DataType == DataType.DATE_NANOS, isTimestampNanos);
            long expectedValue2 = makeExpectedValue(longValue2, testCase.argument2DataType == DataType.DATE_NANOS, isTimestampNanos);
            suppliers.add(
                new TestCaseSupplier(List.of(testCase.argument1DataType, testCase.argument2DataType, testCase.timestampDataType), () -> {
                    var tc = new TestCaseSupplier.TestCase(
                        List.of(
                            new TestCaseSupplier.TypedData(value1, testCase.argument1DataType, "start_time").forceLiteral(),
                            new TestCaseSupplier.TypedData(value2, testCase.argument2DataType, "end_time").forceLiteral(),
                            new TestCaseSupplier.TypedData(timestampValue, testCase.timestampDataType, "@timestamp")
                        ),
                        Matchers.equalTo(
                            "BooleanLogicExpressionEvaluator[bl=source, "
                                + "leftEval=GreaterThanLongsEvaluator[lhs=Attribute[channel=0], rhs=LiteralsEvaluator[lit="
                                + expectedValue1
                                + "]], rightEval=LessThanOrEqualLongsEvaluator[lhs=Attribute[channel=0], rhs=LiteralsEvaluator[lit="
                                + expectedValue2
                                + "]]]"
                        ),
                        DataType.BOOLEAN,
                        equalTo(testCase.expectedResult)
                    );
                    return testCase.timezone == null ? tc : tc.withConfiguration(TEST_SOURCE, configurationForTimezone(testCase.timezone));
                })
            );
        }
    }

    private static Instant timestampInRange(Instant min, Instant max) {
        if (min.equals(max)) {
            return min;
        }
        if (min.getEpochSecond() == max.getEpochSecond()) {
            return ofEpochSecond(min.getEpochSecond(), (min.getNano() + max.getNano()) / 2);
        }
        return ofEpochSecond((min.getEpochSecond() + max.getEpochSecond()) / 2);
    }

    private static long toAbsoluteTime(Object argument, DataType dataType, Instant now, boolean nanos, ZoneId timezone) {
        switch (dataType) {
            case TIME_DURATION, DATE_PERIOD -> {
                var zonedNow = now.atZone(timezone);
                return nanos
                    ? DateUtils.toLong(zonedNow.minus((TemporalAmount) argument).toInstant())
                    : DateUtils.toLongMillis(zonedNow.minus((TemporalAmount) argument).toInstant());
            }
            case KEYWORD -> {
                return dateTimeToLong((String) argument, DEFAULT_DATE_TIME_FORMATTER.withZone(timezone));
            }
            case DATETIME, LONG -> {
                return DateUtils.toLongMillis((Instant) argument);
            }
            case DATE_NANOS -> {
                return DateUtils.toLong((Instant) argument);
            }
            default -> throw new IllegalArgumentException("Unexpected data type: " + dataType);
        }
    }

    private static long makeExpectedValue(long value, boolean isNanos, boolean toNanos) {
        if (isNanos == toNanos) {
            return value;
        }
        return toNanos ? DateUtils.toNanoSeconds(value) : DateUtils.toMilliSeconds(value);
    }

    @Override
    protected Expression buildWithConfiguration(Source source, List<Expression> args, Configuration configuration) {
        if (args.size() == 2) {
            return new TRange(source, args.get(0), null, args.get(1), configuration);
        } else if (args.size() == 3) {
            return new TRange(source, args.get(0), args.get(1), args.get(2), configuration);
        } else {
            throw new IllegalArgumentException("Unexpected number of arguments: " + args.size());
        }
    }

    public static List<DocsV3Support.Param> signatureTypes(List<DocsV3Support.Param> params) {
        assertThat(params.getLast().dataType(), anyOf(equalTo(DataType.DATE_NANOS), equalTo(DataType.DATETIME)));

        if (params.size() == 2) {
            return List.of(params.get(0));
        }

        assertThat(params, hasSize(3));
        return List.of(params.get(0), params.get(1));
    }

    @Override
    protected boolean canSerialize() {
        return false;
    }
}
