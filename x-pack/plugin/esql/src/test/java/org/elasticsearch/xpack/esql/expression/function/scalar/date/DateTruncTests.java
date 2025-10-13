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
import org.elasticsearch.xpack.esql.expression.function.AbstractScalarFunctionTestCase;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;
import org.hamcrest.Matchers;

import java.time.Duration;
import java.time.Instant;
import java.time.Period;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.equalTo;

/**
 * Parameterized testing for {@link DateTrunc}.  See also {@link DateTruncRoundingTests} for non-parametrized tests.
 */
public class DateTruncTests extends AbstractScalarFunctionTestCase {

    public DateTruncTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        long ts = toMillis("2023-02-17T10:25:33.38Z");
        List<TestCaseSupplier> suppliers = new ArrayList<>();
        suppliers.addAll(ofDatePeriod(Period.ofDays(1), ts, "2023-02-17T00:00:00.00Z"));
        suppliers.addAll(ofDatePeriod(Period.ofMonths(1), ts, "2023-02-01T00:00:00.00Z"));
        suppliers.addAll(ofDatePeriod(Period.ofYears(1), ts, "2023-01-01T00:00:00.00Z"));
        suppliers.addAll(ofDatePeriod(Period.ofDays(10), ts, "2023-02-12T00:00:00.00Z"));
        // 7 days period should return weekly rounding
        suppliers.addAll(ofDatePeriod(Period.ofDays(7), ts, "2023-02-13T00:00:00.00Z"));
        // 3 months period should return quarterly
        suppliers.addAll(ofDatePeriod(Period.ofMonths(3), ts, "2023-01-01T00:00:00.00Z"));
        suppliers.addAll(ofDuration(Duration.ofHours(1), ts, "2023-02-17T10:00:00.00Z"));
        suppliers.addAll(ofDuration(Duration.ofMinutes(1), ts, "2023-02-17T10:25:00.00Z"));
        suppliers.addAll(ofDuration(Duration.ofSeconds(1), ts, "2023-02-17T10:25:33.00Z"));
        suppliers.addAll(ofDuration(Duration.ofHours(3), ts, "2023-02-17T09:00:00.00Z"));
        suppliers.addAll(ofDuration(Duration.ofMinutes(15), ts, "2023-02-17T10:15:00.00Z"));
        suppliers.addAll(ofDuration(Duration.ofSeconds(30), ts, "2023-02-17T10:25:30.00Z"));
        suppliers.add(randomSecond());

        // arbitrary period of months and years
        suppliers.addAll(ofDatePeriod(Period.ofMonths(7), ts, "2022-11-01T00:00:00.00Z"));
        suppliers.addAll(ofDatePeriod(Period.ofYears(5), ts, "2021-01-01T00:00:00.00Z"));

        return parameterSuppliersFromTypedDataWithDefaultChecksNoErrors(true, suppliers);
    }

    private static List<TestCaseSupplier> ofDatePeriod(Period period, long value, String expectedDate) {
        return List.of(
            new TestCaseSupplier(
                List.of(DataType.DATE_PERIOD, DataType.DATETIME),
                () -> new TestCaseSupplier.TestCase(
                    List.of(
                        new TestCaseSupplier.TypedData(period, DataType.DATE_PERIOD, "interval").forceLiteral(),
                        new TestCaseSupplier.TypedData(value, DataType.DATETIME, "date")
                    ),
                    Matchers.startsWith("DateTruncDatetimeEvaluator[fieldVal=Attribute[channel=0], rounding=Rounding["),
                    DataType.DATETIME,
                    equalTo(toMillis(expectedDate))
                )
            ),
            new TestCaseSupplier(
                List.of(DataType.DATE_PERIOD, DataType.DATE_NANOS),
                () -> new TestCaseSupplier.TestCase(
                    List.of(
                        new TestCaseSupplier.TypedData(period, DataType.DATE_PERIOD, "interval").forceLiteral(),
                        new TestCaseSupplier.TypedData(DateUtils.toNanoSeconds(value), DataType.DATE_NANOS, "date")
                    ),
                    Matchers.startsWith("DateTruncDateNanosEvaluator[fieldVal=Attribute[channel=0], rounding=Rounding["),
                    DataType.DATE_NANOS,
                    equalTo(toNanos(expectedDate))
                )
            )
        );
    }

    private static List<TestCaseSupplier> ofDuration(Duration duration, long value, String expectedDate) {
        return List.of(
            new TestCaseSupplier(
                List.of(DataType.TIME_DURATION, DataType.DATETIME),
                () -> new TestCaseSupplier.TestCase(
                    List.of(
                        new TestCaseSupplier.TypedData(duration, DataType.TIME_DURATION, "interval").forceLiteral(),
                        new TestCaseSupplier.TypedData(value, DataType.DATETIME, "date")
                    ),
                    Matchers.startsWith("DateTruncDatetimeEvaluator[fieldVal=Attribute[channel=0], rounding=Rounding["),
                    DataType.DATETIME,
                    equalTo(toMillis(expectedDate))
                )
            ),
            new TestCaseSupplier(
                List.of(DataType.TIME_DURATION, DataType.DATE_NANOS),
                () -> new TestCaseSupplier.TestCase(
                    List.of(
                        new TestCaseSupplier.TypedData(duration, DataType.TIME_DURATION, "interval").forceLiteral(),
                        new TestCaseSupplier.TypedData(DateUtils.toNanoSeconds(value), DataType.DATE_NANOS, "date")
                    ),
                    Matchers.startsWith("DateTruncDateNanosEvaluator[fieldVal=Attribute[channel=0], rounding=Rounding["),
                    DataType.DATE_NANOS,
                    equalTo(toNanos(expectedDate))
                )
            )
        );
    }

    private static TestCaseSupplier randomSecond() {
        return new TestCaseSupplier("random second", List.of(DataType.TIME_DURATION, DataType.DATETIME), () -> {
            String dateFragment = randomIntBetween(2000, 2050)
                + "-"
                + pad(randomIntBetween(1, 12))
                + "-"
                + pad(randomIntBetween(1, 28))
                + "T"
                + pad(randomIntBetween(0, 23))
                + ":"
                + pad(randomIntBetween(0, 59))
                + ":"
                + pad(randomIntBetween(0, 59));
            return new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(Duration.ofSeconds(1), DataType.TIME_DURATION, "interval").forceLiteral(),
                    new TestCaseSupplier.TypedData(toMillis(dateFragment + ".38Z"), DataType.DATETIME, "date")
                ),
                Matchers.startsWith("DateTruncDatetimeEvaluator[fieldVal=Attribute[channel=0], rounding=Rounding["),
                DataType.DATETIME,
                equalTo(toMillis(dateFragment + ".00Z"))
            );
        });
    }

    private static String pad(int i) {
        return i > 9 ? "" + i : "0" + i;
    }

    private static long toMillis(String timestamp) {
        return Instant.parse(timestamp).toEpochMilli();
    }

    private static long toNanos(String timestamp) {
        return DateUtils.toLong(Instant.parse(timestamp));
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new DateTrunc(source, args.get(0), args.get(1));
    }
}
