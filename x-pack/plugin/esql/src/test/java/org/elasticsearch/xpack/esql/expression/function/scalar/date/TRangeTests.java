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
import org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter;
import org.hamcrest.Matchers;
import org.mockito.Mockito;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.Period;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

public class TRangeTests extends AbstractConfigurationFunctionTestCase {

    private static final ZonedDateTime fixedNow = ZonedDateTime.parse("2024-01-05T15:00:00Z");

    public TRangeTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    record TestParameter(DataType dataType, Object value) {}

    record SingleParameterCase(
        DataType argumentDataType,
        Object argumentValue,
        DataType timestampDataType,
        long timestampValue,
        long expectedStartTime,
        long expectedEndTime,
        boolean expectedResult
    ) {}

    record TwoParameterCase(
        DataType argument1DataType,
        Object argument1Value,
        DataType argument2DataType,
        Object argument2Value,
        DataType timestampDataType,
        long timestampValue,
        long expectedStartTime,
        long expectedEndTime,
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
        List<TestParameter> testParameters = List.of(
            new TestParameter(DataType.TIME_DURATION, Duration.ofHours(1)),
            new TestParameter(DataType.DATE_PERIOD, Period.ofDays(1))
        );

        List<SingleParameterCase> testCases = new ArrayList<>();
        for (DataType timestampDataType : List.of(DataType.DATETIME, DataType.DATE_NANOS)) {
            boolean nanos = timestampDataType == DataType.DATE_NANOS;

            for (TestParameter testParameter : testParameters) {
                long expectedStartTime = getExpectedAbsoluteTime(testParameter.value, testParameter.dataType, nanos);
                long expectedEndTime = getNow(fixedNow.toInstant().toEpochMilli(), nanos);

                long timestampInsideRange = timestampInRange(expectedStartTime, expectedEndTime);
                testCases.add(
                    new SingleParameterCase(
                        testParameter.dataType,
                        testParameter.value,
                        timestampDataType,
                        timestampInsideRange,
                        expectedStartTime,
                        expectedEndTime,
                        true
                    )
                );

                long timestampOutsideRange = expectedStartTime - Duration.ofMinutes(10).toMillis();
                testCases.add(
                    new SingleParameterCase(
                        testParameter.dataType,
                        testParameter.value,
                        timestampDataType,
                        timestampOutsideRange,
                        expectedStartTime,
                        expectedEndTime,
                        false
                    )
                );
            }
        }
        return testCases;
    }

    private static void singleParameterTRangeSuppliers(List<TestCaseSupplier> suppliers, List<SingleParameterCase> testCases) {
        for (SingleParameterCase testCase : testCases) {
            suppliers.add(
                new TestCaseSupplier(
                    List.of(testCase.argumentDataType, testCase.timestampDataType),
                    () -> new TestCaseSupplier.TestCase(
                        List.of(
                            new TestCaseSupplier.TypedData(testCase.argumentValue, testCase.argumentDataType, "start_time_or_interval")
                                .forceLiteral(),
                            new TestCaseSupplier.TypedData(testCase.timestampValue, testCase.timestampDataType, "@timestamp")
                        ),
                        Matchers.equalTo(
                            "BooleanLogicExpressionEvaluator[bl=source, "
                                + "leftEval=GreaterThanLongsEvaluator[lhs=Attribute[channel=0], rhs=LiteralsEvaluator[lit="
                                + testCase.expectedStartTime
                                + "]], rightEval=LessThanOrEqualLongsEvaluator[lhs=Attribute[channel=0], rhs=LiteralsEvaluator[lit="
                                + testCase.expectedEndTime
                                + "]]]"
                        ),
                        DataType.BOOLEAN,
                        equalTo(testCase.expectedResult)
                    )
                )
            );
        }
    }

    private static List<TwoParameterCase> twoParameterAbsoluteTimeTestCases() {
        List<TestParameter[]> testParameters = List.of(
            new TestParameter[] {
                new TestParameter(DataType.KEYWORD, "2024-01-01T00:00:00"),
                new TestParameter(DataType.KEYWORD, "2024-01-01T12:00:00") },
            new TestParameter[] {
                new TestParameter(DataType.LONG, ZonedDateTime.parse("2024-01-01T00:00:00Z").toInstant().toEpochMilli()),
                new TestParameter(DataType.LONG, ZonedDateTime.parse("2024-01-01T12:00:00Z").toInstant().toEpochMilli()) },
            new TestParameter[] {
                new TestParameter(DataType.DATETIME, Instant.parse("2024-01-01T00:00:00Z")),
                new TestParameter(DataType.DATETIME, Instant.parse("2024-01-01T12:00:00Z")), },
            new TestParameter[] {
                new TestParameter(DataType.DATE_NANOS, Instant.parse("2024-01-01T00:00:00Z")),
                new TestParameter(DataType.DATE_NANOS, Instant.parse("2024-01-01T12:00:00Z")), }
        );

        List<TwoParameterCase> testCases = new ArrayList<>();
        for (DataType timestampDataType : List.of(DataType.DATETIME, DataType.DATE_NANOS)) {
            boolean nanos = timestampDataType == DataType.DATE_NANOS;

            for (TestParameter[] testParameter : testParameters) {
                long expectedStartTime = getExpectedAbsoluteTime(testParameter[0].value, testParameter[0].dataType, nanos);
                long expectedEndTime = getExpectedAbsoluteTime(testParameter[1].value, testParameter[1].dataType, nanos);

                long timestampInsideRange = timestampInRange(expectedStartTime, expectedEndTime);
                testCases.add(
                    new TwoParameterCase(
                        testParameter[0].dataType,
                        testParameter[0].value,
                        testParameter[1].dataType,
                        testParameter[1].value,
                        timestampDataType,
                        timestampInsideRange,
                        expectedStartTime,
                        expectedEndTime,
                        true
                    )
                );

                long timestampOutsideRange = expectedStartTime - Duration.ofMinutes(10).toMillis();
                testCases.add(
                    new TwoParameterCase(
                        testParameter[0].dataType,
                        testParameter[0].value,
                        testParameter[1].dataType,
                        testParameter[1].value,
                        timestampDataType,
                        timestampOutsideRange,
                        expectedStartTime,
                        expectedEndTime,
                        false
                    )
                );
            }
        }
        return testCases;
    }

    private static void twoParameterTRangeSuppliers(List<TestCaseSupplier> suppliers, List<TwoParameterCase> testCases) {
        for (TwoParameterCase testCase : testCases) {
            suppliers.add(
                new TestCaseSupplier(
                    List.of(testCase.argument1DataType, testCase.argument2DataType, testCase.timestampDataType),
                    () -> new TestCaseSupplier.TestCase(
                        List.of(
                            new TestCaseSupplier.TypedData(testCase.argument1Value, testCase.argument1DataType, "start_time_or_interval")
                                .forceLiteral(),
                            new TestCaseSupplier.TypedData(testCase.argument2Value, testCase.argument2DataType, "start_time_or_interval")
                                .forceLiteral(),
                            new TestCaseSupplier.TypedData(testCase.timestampValue, testCase.timestampDataType, "@timestamp")
                        ),
                        Matchers.equalTo(
                            "BooleanLogicExpressionEvaluator[bl=source, "
                                + "leftEval=GreaterThanLongsEvaluator[lhs=Attribute[channel=0], rhs=LiteralsEvaluator[lit="
                                + testCase.expectedStartTime
                                + "]], rightEval=LessThanOrEqualLongsEvaluator[lhs=Attribute[channel=0], rhs=LiteralsEvaluator[lit="
                                + testCase.expectedEndTime
                                + "]]]"
                        ),
                        DataType.BOOLEAN,
                        equalTo(testCase.expectedResult)
                    )
                )
            );
        }
    }

    private static long timestampInRange(long min, long max) {
        return (min + max) / 2;
    }

    private static long getExpectedAbsoluteTime(Object argument, DataType dataType, boolean nanos) {
        final Instant now = fixedNow.toInstant();
        switch (dataType) {
            case TIME_DURATION -> {
                return nanos
                    ? DateUtils.toNanoSeconds(now.minus((Duration) argument).toEpochMilli())
                    : now.minus((Duration) argument).toEpochMilli();
            }
            case DATE_PERIOD -> {
                return nanos
                    ? DateUtils.toNanoSeconds(now.minus((Period) argument).toEpochMilli())
                    : now.minus((Period) argument).toEpochMilli();
            }
            case KEYWORD -> {
                long expectedStartTime = EsqlDataTypeConverter.DEFAULT_DATE_TIME_FORMATTER.parseMillis((String) argument);
                return nanos ? DateUtils.toNanoSeconds(expectedStartTime) : expectedStartTime;
            }
            case LONG -> {
                long expectedStartTime = (Long) argument;
                return nanos ? DateUtils.toNanoSeconds(expectedStartTime) : expectedStartTime;
            }
            case DATETIME, DATE_NANOS -> {
                return nanos ? DateUtils.toLong((Instant) argument) : DateUtils.toLongMillis((Instant) argument);
            }
            default -> throw new IllegalArgumentException("Unexpected data type: " + dataType);
        }
    }

    private static long getNow(long now, boolean nanos) {
        return nanos == false ? now : DateUtils.toNanoSeconds(now);
    }

    @Override
    protected Expression buildWithConfiguration(Source source, List<Expression> args, Configuration configuration) {
        Clock fixedClock = Clock.fixed(fixedNow.toInstant(), fixedNow.getZone().normalized());
        ZonedDateTime fixedNow = ZonedDateTime.now(Clock.tick(fixedClock, Duration.ofNanos(1)));

        Configuration spyConfig = Mockito.spy(configuration);
        Mockito.doReturn(fixedNow).when(spyConfig).now();

        if (args.size() == 2) {
            return new TRange(source, args.get(0), null, args.get(1), spyConfig);
        } else if (args.size() == 3) {
            return new TRange(source, args.get(0), args.get(1), args.get(2), spyConfig);
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
