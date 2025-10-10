/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.date;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.time.DateUtils;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.AbstractScalarFunctionTestCase;
import org.elasticsearch.xpack.esql.expression.function.DocsV3Support;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;
import org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter;
import org.hamcrest.Matchers;

import java.time.Duration;
import java.time.Instant;
import java.time.Period;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

public class TRangeTests extends AbstractScalarFunctionTestCase {
    public TRangeTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        List<TestCaseSupplier> suppliers = new ArrayList<>();

        singleParameterDuration(suppliers, Duration.ofHours(1));
        singleParameterPeriod(suppliers, Period.ofDays(1));

        long now = Instant.now().toEpochMilli();
        twoParameterStringRange(suppliers, "2024-01-01T00:00:00", "2024-01-01T12:00:00");
        twoParameterEpochRange(suppliers, now - 7200000, now - 3600000); // 2 hours ago to 1 hour ago

        return parameterSuppliersFromTypedData(suppliers);
    }

    private static void singleParameterDuration(List<TestCaseSupplier> suppliers, Duration duration) {
        long endTime = Instant.now().toEpochMilli();
        long startTime = endTime - duration.toMillis();

        for (DataType timestampDataType : List.of(DataType.DATETIME, DataType.DATE_NANOS)) {
            long timestampInsideRange = timestampDataType != DataType.DATE_NANOS
                ? randomLong(startTime, endTime, duration)
                : DateUtils.toNanoSeconds(randomLong(startTime, endTime, duration));

            long timestampOutsideRange = timestampDataType != DataType.DATE_NANOS
                ? startTime - 100_000
                : DateUtils.toNanoSeconds(startTime - 100_000);

            suppliers.add(
                new TestCaseSupplier(
                    List.of(timestampDataType, DataType.TIME_DURATION),
                    () -> new TestCaseSupplier.TestCase(
                        List.of(
                            new TestCaseSupplier.TypedData(timestampInsideRange, timestampDataType, "@timestamp"),
                            new TestCaseSupplier.TypedData(duration, DataType.TIME_DURATION, "start_time_or_interval").forceLiteral()
                        ),
                        Matchers.startsWith("TRangeEvaluator[timestamp=Attribute[channel=0], startTimestamp="),
                        DataType.BOOLEAN,
                        equalTo(true)
                    )
                )
            );

            suppliers.add(
                new TestCaseSupplier(
                    List.of(timestampDataType, DataType.TIME_DURATION),
                    () -> new TestCaseSupplier.TestCase(
                        List.of(
                            new TestCaseSupplier.TypedData(timestampOutsideRange, timestampDataType, "@timestamp"),
                            new TestCaseSupplier.TypedData(duration, DataType.TIME_DURATION, "start_time_or_interval").forceLiteral()
                        ),
                        Matchers.startsWith("TRangeEvaluator[timestamp=Attribute[channel=0], startTimestamp="),
                        DataType.BOOLEAN,
                        equalTo(false)
                    )
                )
            );
        }
    }

    private static void singleParameterPeriod(List<TestCaseSupplier> suppliers, Period period) {
        Instant now = Instant.now();
        long endTime = now.toEpochMilli();
        long startTime = now.minus(period).toEpochMilli();

        for (DataType timestampDataType : List.of(DataType.DATETIME, DataType.DATE_NANOS)) {
            long timestampInsideRange = timestampDataType != DataType.DATE_NANOS
                ? randomLong(startTime, endTime, period)
                : DateUtils.toNanoSeconds(randomLong(startTime, endTime, period));

            long timestampOutsideRange = timestampDataType != DataType.DATE_NANOS
                ? startTime - Duration.ofMinutes(10).toMillis()
                : DateUtils.toNanoSeconds(startTime - Duration.ofMinutes(10).toMillis());

            suppliers.add(
                new TestCaseSupplier(
                    List.of(timestampDataType, DataType.DATE_PERIOD),
                    () -> new TestCaseSupplier.TestCase(
                        List.of(
                            new TestCaseSupplier.TypedData(timestampInsideRange, timestampDataType, "@timestamp"),
                            new TestCaseSupplier.TypedData(period, DataType.DATE_PERIOD, "start_time_or_interval").forceLiteral()
                        ),
                        Matchers.startsWith("TRangeEvaluator[timestamp=Attribute[channel=0], startTimestamp="),
                        DataType.BOOLEAN,
                        equalTo(true)
                    )
                )
            );

            suppliers.add(
                new TestCaseSupplier(
                    List.of(timestampDataType, DataType.DATE_PERIOD),
                    () -> new TestCaseSupplier.TestCase(
                        List.of(
                            new TestCaseSupplier.TypedData(timestampOutsideRange, timestampDataType, "@timestamp"),
                            new TestCaseSupplier.TypedData(period, DataType.DATE_PERIOD, "start_time_or_interval").forceLiteral()
                        ),
                        Matchers.startsWith("TRangeEvaluator[timestamp=Attribute[channel=0], startTimestamp="),
                        DataType.BOOLEAN,
                        equalTo(false)
                    )
                )
            );
        }
    }

    private static void twoParameterStringRange(List<TestCaseSupplier> suppliers, String startStr, String endStr) {
        long startTime = parseDateTime(startStr);
        long endTime = parseDateTime(endStr);

        for (DataType timestampDataType : List.of(DataType.DATETIME, DataType.DATE_NANOS)) {
            long timestampInsideRange = timestampDataType != DataType.DATE_NANOS
                ? randomLong(startTime, endTime, List.of(startTime, endTime))
                : DateUtils.toNanoSeconds(randomLong(startTime, endTime, List.of(startTime, endTime)));

            long timestampOutsideRange = timestampDataType != DataType.DATE_NANOS
                ? startTime - Duration.ofMinutes(10).toMillis()
                : DateUtils.toNanoSeconds(startTime - Duration.ofMinutes(10).toMillis());

            suppliers.add(
                new TestCaseSupplier(
                    List.of(timestampDataType, DataType.KEYWORD, DataType.KEYWORD),
                    () -> new TestCaseSupplier.TestCase(
                        List.of(
                            new TestCaseSupplier.TypedData(timestampInsideRange, timestampDataType, "@timestamp"),
                            new TestCaseSupplier.TypedData(startStr, DataType.KEYWORD, "start_time_or_interval").forceLiteral(),
                            new TestCaseSupplier.TypedData(endStr, DataType.KEYWORD, "end_time").forceLiteral()
                        ),
                        Matchers.startsWith("TRangeEvaluator[timestamp=Attribute[channel=0], startTimestamp="),
                        DataType.BOOLEAN,
                        equalTo(true)
                    )
                )
            );

            suppliers.add(
                new TestCaseSupplier(
                    List.of(timestampDataType, DataType.KEYWORD, DataType.KEYWORD),
                    () -> new TestCaseSupplier.TestCase(
                        List.of(
                            new TestCaseSupplier.TypedData(timestampOutsideRange, timestampDataType, "@timestamp"),
                            new TestCaseSupplier.TypedData(startStr, DataType.KEYWORD, "start_time_or_interval").forceLiteral(),
                            new TestCaseSupplier.TypedData(endStr, DataType.KEYWORD, "end_time").forceLiteral()
                        ),
                        Matchers.startsWith("TRangeEvaluator[timestamp=Attribute[channel=0], startTimestamp="),
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

                long timestampInsideRange = timestampDataType != DataType.DATE_NANOS
                    ? randomLong(startEpoch, endEpoch, List.of(startEpoch, endEpoch))
                    : DateUtils.toNanoSeconds(randomLong(startEpoch, endEpoch, List.of(startEpoch, endEpoch)));

                long timestampOutsideRange = timestampDataType != DataType.DATE_NANOS
                    ? startEpoch - Duration.ofMinutes(10).toMillis()
                    : DateUtils.toNanoSeconds(startEpoch - Duration.ofMinutes(10).toMillis());

                suppliers.add(
                    new TestCaseSupplier(
                        List.of(timestampDataType, paramsDataType, paramsDataType),
                        () -> new TestCaseSupplier.TestCase(
                            List.of(
                                new TestCaseSupplier.TypedData(timestampInsideRange, timestampDataType, "@timestamp"),
                                new TestCaseSupplier.TypedData(startEpoch, paramsDataType, "start_time_or_interval").forceLiteral(),
                                new TestCaseSupplier.TypedData(endEpoch, paramsDataType, "end_time").forceLiteral()
                            ),
                            Matchers.startsWith("TRangeEvaluator[timestamp=Attribute[channel=0], startTimestamp="),
                            DataType.BOOLEAN,
                            equalTo(true)
                        )
                    )
                );

                suppliers.add(
                    new TestCaseSupplier(
                        List.of(timestampDataType, paramsDataType, paramsDataType),
                        () -> new TestCaseSupplier.TestCase(
                            List.of(
                                new TestCaseSupplier.TypedData(timestampOutsideRange, timestampDataType, "@timestamp"),
                                new TestCaseSupplier.TypedData(startEpoch, paramsDataType, "start_time_or_interval").forceLiteral(),
                                new TestCaseSupplier.TypedData(endEpoch, paramsDataType, "end_time").forceLiteral()
                            ),
                            Matchers.startsWith("TRangeEvaluator[timestamp=Attribute[channel=0], startTimestamp="),
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

    private static long randomLong(long startInclusive, long endExclusive, Object obj) {
        return Randomness.get().nextLong(startInclusive, endExclusive);
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        if (args.size() == 2) {
            return new TRange(source, args.get(0), args.get(1), null);
        } else if (args.size() == 3) {
            return new TRange(source, args.get(0), args.get(1), args.get(2));
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
}
