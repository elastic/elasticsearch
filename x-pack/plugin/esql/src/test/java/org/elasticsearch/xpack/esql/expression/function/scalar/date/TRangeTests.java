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
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.DocsV3Support;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;
import org.elasticsearch.xpack.esql.expression.function.scalar.AbstractConfigurationFunctionTestCase;
import org.elasticsearch.xpack.esql.session.Configuration;
import org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter;
import org.hamcrest.Matchers;

import java.time.Clock;
import java.time.Duration;
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

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        List<TestCaseSupplier> suppliers = new ArrayList<>();

        singleParameterDuration(suppliers, Duration.ofHours(1));
        singleParameterPeriod(suppliers, Period.ofDays(1));

        twoParameterStringRange(suppliers, "2024-01-01T00:00:00", "2024-01-01T12:00:00");

        long now = fixedNow.toInstant().toEpochMilli();
        long startEpochMillis = now - Duration.ofHours(2).toMillis();
        long endEpochMillis = now + Duration.ofHours(1).toMillis();
        twoParameterEpochRange(suppliers, startEpochMillis, endEpochMillis);

        return parameterSuppliersFromTypedData(suppliers);
    }

    private static void singleParameterDuration(List<TestCaseSupplier> suppliers, Duration duration) {
        for (DataType timestampDataType : List.of(DataType.DATETIME, DataType.DATE_NANOS)) {
            boolean nanos = timestampDataType == DataType.DATE_NANOS;
            long expectedEndTime = nanos
                ? DateUtils.toNanoSeconds(fixedNow.toInstant().toEpochMilli())
                : fixedNow.toInstant().toEpochMilli();
            long expectedStartTime = nanos
                ? expectedEndTime - DateUtils.toNanoSeconds(duration.toMillis())
                : expectedEndTime - duration.toMillis();

            long timestampInsideRange = timestampInRange(expectedStartTime, expectedEndTime);
            suppliers.add(
                new TestCaseSupplier(
                    List.of(timestampDataType, DataType.TIME_DURATION),
                    () -> new TestCaseSupplier.TestCase(
                        List.of(
                            new TestCaseSupplier.TypedData(timestampInsideRange, timestampDataType, "@timestamp"),
                            new TestCaseSupplier.TypedData(duration, DataType.TIME_DURATION, "start_time_or_interval").forceLiteral()
                        ),
                        Matchers.equalTo(
                            "BooleanLogicExpressionEvaluator[bl=source, "
                                + "leftEval=GreaterThanOrEqualLongsEvaluator[lhs=Attribute[channel=0], rhs=LiteralsEvaluator[lit="
                                + expectedStartTime
                                + "]], rightEval=LessThanOrEqualLongsEvaluator[lhs=Attribute[channel=0], rhs=LiteralsEvaluator[lit="
                                + expectedEndTime
                                + "]]]"
                        ),
                        DataType.BOOLEAN,
                        equalTo(true)
                    )
                )
            );

            long timestampOutsideRange = expectedStartTime - 100_000;
            suppliers.add(
                new TestCaseSupplier(
                    List.of(timestampDataType, DataType.TIME_DURATION),
                    () -> new TestCaseSupplier.TestCase(
                        List.of(
                            new TestCaseSupplier.TypedData(timestampOutsideRange, timestampDataType, "@timestamp"),
                            new TestCaseSupplier.TypedData(duration, DataType.TIME_DURATION, "start_time_or_interval").forceLiteral()
                        ),
                        Matchers.equalTo(
                            "BooleanLogicExpressionEvaluator[bl=source, "
                                + "leftEval=GreaterThanOrEqualLongsEvaluator[lhs=Attribute[channel=0], rhs=LiteralsEvaluator[lit="
                                + expectedStartTime
                                + "]], rightEval=LessThanOrEqualLongsEvaluator[lhs=Attribute[channel=0], rhs=LiteralsEvaluator[lit="
                                + expectedEndTime
                                + "]]]"
                        ),
                        DataType.BOOLEAN,
                        equalTo(false)
                    )
                )
            );
        }
    }

    private static void singleParameterPeriod(List<TestCaseSupplier> suppliers, Period period) {
        for (DataType timestampDataType : List.of(DataType.DATETIME, DataType.DATE_NANOS)) {
            boolean nanos = timestampDataType == DataType.DATE_NANOS;

            long expectedEndTime = nanos
                ? DateUtils.toNanoSeconds(fixedNow.toInstant().toEpochMilli())
                : fixedNow.toInstant().toEpochMilli();
            long expectedStartTime = nanos
                ? DateUtils.toNanoSeconds(fixedNow.toInstant().minus(period).toEpochMilli())
                : fixedNow.toInstant().minus(period).toEpochMilli();

            long timestampInsideRange = timestampInRange(expectedStartTime, expectedEndTime);
            suppliers.add(
                new TestCaseSupplier(
                    List.of(timestampDataType, DataType.DATE_PERIOD),
                    () -> new TestCaseSupplier.TestCase(
                        List.of(
                            new TestCaseSupplier.TypedData(timestampInsideRange, timestampDataType, "@timestamp"),
                            new TestCaseSupplier.TypedData(period, DataType.DATE_PERIOD, "start_time_or_interval").forceLiteral()
                        ),
                        Matchers.equalTo(
                            "BooleanLogicExpressionEvaluator[bl=source, "
                                + "leftEval=GreaterThanOrEqualLongsEvaluator[lhs=Attribute[channel=0], rhs=LiteralsEvaluator[lit="
                                + expectedStartTime
                                + "]], rightEval=LessThanOrEqualLongsEvaluator[lhs=Attribute[channel=0], rhs=LiteralsEvaluator[lit="
                                + expectedEndTime
                                + "]]]"
                        ),
                        DataType.BOOLEAN,
                        equalTo(true)
                    )
                )
            );

            long timestampOutsideRange = expectedStartTime - Duration.ofMinutes(10).toMillis();
            suppliers.add(
                new TestCaseSupplier(
                    List.of(timestampDataType, DataType.DATE_PERIOD),
                    () -> new TestCaseSupplier.TestCase(
                        List.of(
                            new TestCaseSupplier.TypedData(timestampOutsideRange, timestampDataType, "@timestamp"),
                            new TestCaseSupplier.TypedData(period, DataType.DATE_PERIOD, "start_time_or_interval").forceLiteral()
                        ),
                        Matchers.equalTo(
                            "BooleanLogicExpressionEvaluator[bl=source, "
                                + "leftEval=GreaterThanOrEqualLongsEvaluator[lhs=Attribute[channel=0], rhs=LiteralsEvaluator[lit="
                                + expectedStartTime
                                + "]], rightEval=LessThanOrEqualLongsEvaluator[lhs=Attribute[channel=0], rhs=LiteralsEvaluator[lit="
                                + expectedEndTime
                                + "]]]"
                        ),
                        DataType.BOOLEAN,
                        equalTo(false)
                    )
                )
            );
        }
    }

    private static void twoParameterStringRange(List<TestCaseSupplier> suppliers, String startStr, String endStr) {
        for (DataType timestampDataType : List.of(DataType.DATETIME, DataType.DATE_NANOS)) {
            boolean nanos = timestampDataType == DataType.DATE_NANOS;

            long expectedStartTime = nanos ? DateUtils.toNanoSeconds(parseDateTime(startStr)) : parseDateTime(startStr);
            long expectedEndTime = nanos ? DateUtils.toNanoSeconds(parseDateTime(endStr)) : parseDateTime(endStr);

            long timestampInsideRange = timestampInRange(expectedStartTime, expectedEndTime);
            suppliers.add(
                new TestCaseSupplier(
                    List.of(timestampDataType, DataType.KEYWORD, DataType.KEYWORD),
                    () -> new TestCaseSupplier.TestCase(
                        List.of(
                            new TestCaseSupplier.TypedData(timestampInsideRange, timestampDataType, "@timestamp"),
                            new TestCaseSupplier.TypedData(startStr, DataType.KEYWORD, "start_time_or_interval").forceLiteral(),
                            new TestCaseSupplier.TypedData(endStr, DataType.KEYWORD, "end_time").forceLiteral()
                        ),
                        Matchers.equalTo(
                            "BooleanLogicExpressionEvaluator[bl=source, "
                                + "leftEval=GreaterThanOrEqualLongsEvaluator[lhs=Attribute[channel=0], rhs=LiteralsEvaluator[lit="
                                + expectedStartTime
                                + "]], rightEval=LessThanOrEqualLongsEvaluator[lhs=Attribute[channel=0], rhs=LiteralsEvaluator[lit="
                                + expectedEndTime
                                + "]]]"
                        ),
                        DataType.BOOLEAN,
                        equalTo(true)
                    )
                )
            );

            long timestampOutsideRange = expectedStartTime - Duration.ofMinutes(10).toMillis();
            suppliers.add(
                new TestCaseSupplier(
                    List.of(timestampDataType, DataType.KEYWORD, DataType.KEYWORD),
                    () -> new TestCaseSupplier.TestCase(
                        List.of(
                            new TestCaseSupplier.TypedData(timestampOutsideRange, timestampDataType, "@timestamp"),
                            new TestCaseSupplier.TypedData(startStr, DataType.KEYWORD, "start_time_or_interval").forceLiteral(),
                            new TestCaseSupplier.TypedData(endStr, DataType.KEYWORD, "end_time").forceLiteral()
                        ),
                        Matchers.equalTo(
                            "BooleanLogicExpressionEvaluator[bl=source, "
                                + "leftEval=GreaterThanOrEqualLongsEvaluator[lhs=Attribute[channel=0], rhs=LiteralsEvaluator[lit="
                                + expectedStartTime
                                + "]], rightEval=LessThanOrEqualLongsEvaluator[lhs=Attribute[channel=0], rhs=LiteralsEvaluator[lit="
                                + expectedEndTime
                                + "]]]"
                        ),
                        DataType.BOOLEAN,
                        equalTo(false)
                    )
                )
            );
        }
    }

    private static void twoParameterEpochRange(List<TestCaseSupplier> suppliers, long startEpoch, long endEpoch) {
        for (DataType paramsDataType : List.of(DataType.LONG, DataType.INTEGER)) {
            for (DataType timestampDataType : List.of(DataType.DATETIME, DataType.DATE_NANOS)) {
                boolean nanos = timestampDataType == DataType.DATE_NANOS;

                long expectedStartTime = nanos ? DateUtils.toNanoSeconds(startEpoch) : startEpoch;
                long expectedEndTime = nanos ? DateUtils.toNanoSeconds(endEpoch) : endEpoch;

                long timestampInsideRange = timestampInRange(expectedStartTime, expectedEndTime);
                suppliers.add(
                    new TestCaseSupplier(
                        List.of(timestampDataType, paramsDataType, paramsDataType),
                        () -> new TestCaseSupplier.TestCase(
                            List.of(
                                new TestCaseSupplier.TypedData(timestampInsideRange, timestampDataType, "@timestamp"),
                                new TestCaseSupplier.TypedData(startEpoch, paramsDataType, "start_time_or_interval").forceLiteral(),
                                new TestCaseSupplier.TypedData(endEpoch, paramsDataType, "end_time").forceLiteral()
                            ),
                            Matchers.equalTo(
                                "BooleanLogicExpressionEvaluator[bl=source, "
                                    + "leftEval=GreaterThanOrEqualLongsEvaluator[lhs=Attribute[channel=0], rhs=LiteralsEvaluator[lit="
                                    + expectedStartTime
                                    + "]], rightEval=LessThanOrEqualLongsEvaluator[lhs=Attribute[channel=0], rhs=LiteralsEvaluator[lit="
                                    + expectedEndTime
                                    + "]]]"
                            ),
                            DataType.BOOLEAN,
                            equalTo(true)
                        )
                    )
                );

                long timestampOutsideRange = expectedStartTime - Duration.ofMinutes(10).toMillis();
                suppliers.add(
                    new TestCaseSupplier(
                        List.of(timestampDataType, paramsDataType, paramsDataType),
                        () -> new TestCaseSupplier.TestCase(
                            List.of(
                                new TestCaseSupplier.TypedData(timestampOutsideRange, timestampDataType, "@timestamp"),
                                new TestCaseSupplier.TypedData(startEpoch, paramsDataType, "start_time_or_interval").forceLiteral(),
                                new TestCaseSupplier.TypedData(endEpoch, paramsDataType, "end_time").forceLiteral()
                            ),
                            Matchers.equalTo(
                                "BooleanLogicExpressionEvaluator[bl=source, "
                                    + "leftEval=GreaterThanOrEqualLongsEvaluator[lhs=Attribute[channel=0], rhs=LiteralsEvaluator[lit="
                                    + expectedStartTime
                                    + "]], rightEval=LessThanOrEqualLongsEvaluator[lhs=Attribute[channel=0], rhs=LiteralsEvaluator[lit="
                                    + expectedEndTime
                                    + "]]]"
                            ),
                            DataType.BOOLEAN,
                            equalTo(false)
                        )
                    )
                );
            }
        }
    }

    private static long parseDateTime(String dateTime) {
        return EsqlDataTypeConverter.DEFAULT_DATE_TIME_FORMATTER.parseMillis(dateTime);
    }

    private static long timestampInRange(long min, long max) {
        return (min + max) / 2;
    }

    @Override
    protected Expression buildWithConfiguration(Source source, List<Expression> args, Configuration configuration) {
        Configuration fixedClockConfig = EsqlTestUtils.configuration(Clock.fixed(fixedNow.toInstant(), fixedNow.getZone()));
        if (args.size() == 2) {
            return new TRange(source, args.get(0), args.get(1), null, fixedClockConfig);
        } else if (args.size() == 3) {
            return new TRange(source, args.get(0), args.get(1), args.get(2), fixedClockConfig);
        } else {
            throw new IllegalArgumentException("Unexpected number of arguments: " + args.size());
        }
    }

    public static List<DocsV3Support.Param> signatureTypes(List<DocsV3Support.Param> params) {
        if (params.size() == 2) {
            assertThat(params.get(0).dataType(), anyOf(equalTo(DataType.DATE_NANOS), equalTo(DataType.DATETIME)));
            return List.of(params.get(1));
        }

        assertThat(params, hasSize(3));
        assertThat(params.get(0).dataType(), anyOf(equalTo(DataType.DATE_NANOS), equalTo(DataType.DATETIME)));
        return List.of(params.get(1), params.get(2));
    }

    @Override
    public void testSerializationWithConfiguration() {
        super.testSerializationWithConfiguration();
    }

    @Override
    protected boolean canSerialize() {
        return false;
    }
}
