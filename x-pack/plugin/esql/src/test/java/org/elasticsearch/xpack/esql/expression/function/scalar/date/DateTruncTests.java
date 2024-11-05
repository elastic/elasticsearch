/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.date;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.AbstractScalarFunctionTestCase;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;
import org.hamcrest.Matchers;

import java.time.Duration;
import java.time.Instant;
import java.time.Period;
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
        List<TestCaseSupplier> suppliers = List.of(
            ofDatePeriod(Period.ofDays(1), ts, "2023-02-17T00:00:00.00Z"),
            ofDatePeriod(Period.ofMonths(1), ts, "2023-02-01T00:00:00.00Z"),
            ofDatePeriod(Period.ofYears(1), ts, "2023-01-01T00:00:00.00Z"),
            ofDatePeriod(Period.ofDays(10), ts, "2023-02-12T00:00:00.00Z"),
            // 7 days period should return weekly rounding
            ofDatePeriod(Period.ofDays(7), ts, "2023-02-13T00:00:00.00Z"),
            // 3 months period should return quarterly
            ofDatePeriod(Period.ofMonths(3), ts, "2023-01-01T00:00:00.00Z"),
            ofDuration(Duration.ofHours(1), ts, "2023-02-17T10:00:00.00Z"),
            ofDuration(Duration.ofMinutes(1), ts, "2023-02-17T10:25:00.00Z"),
            ofDuration(Duration.ofSeconds(1), ts, "2023-02-17T10:25:33.00Z"),
            ofDuration(Duration.ofHours(3), ts, "2023-02-17T09:00:00.00Z"),
            ofDuration(Duration.ofMinutes(15), ts, "2023-02-17T10:15:00.00Z"),
            ofDuration(Duration.ofSeconds(30), ts, "2023-02-17T10:25:30.00Z"),
            randomSecond()
        );
        return parameterSuppliersFromTypedDataWithDefaultChecks(true, suppliers, (v, p) -> switch (p) {
            case 0 -> "dateperiod or timeduration";
            case 1 -> "datetime";
            default -> null;
        });
    }

    private static TestCaseSupplier ofDatePeriod(Period period, long value, String expectedDate) {
        return new TestCaseSupplier(
            List.of(DataType.DATE_PERIOD, DataType.DATETIME),
            () -> new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(period, DataType.DATE_PERIOD, "interval").forceLiteral(),
                    new TestCaseSupplier.TypedData(value, DataType.DATETIME, "date")
                ),
                Matchers.startsWith("DateTruncEvaluator[fieldVal=Attribute[channel=0], rounding=Rounding["),
                DataType.DATETIME,
                equalTo(toMillis(expectedDate))
            )
        );
    }

    private static TestCaseSupplier ofDuration(Duration duration, long value, String expectedDate) {
        return new TestCaseSupplier(
            List.of(DataType.TIME_DURATION, DataType.DATETIME),
            () -> new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(duration, DataType.TIME_DURATION, "interval").forceLiteral(),
                    new TestCaseSupplier.TypedData(value, DataType.DATETIME, "date")
                ),
                Matchers.startsWith("DateTruncEvaluator[fieldVal=Attribute[channel=0], rounding=Rounding["),
                DataType.DATETIME,
                equalTo(toMillis(expectedDate))
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
                    new TestCaseSupplier.TypedData(Duration.ofSeconds(1), DataType.TIME_DURATION, "interval"),
                    new TestCaseSupplier.TypedData(toMillis(dateFragment + ".38Z"), DataType.DATETIME, "date")
                ),
                "DateTruncEvaluator[date=Attribute[channel=1], interval=Attribute[channel=0]]",
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

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new DateTrunc(source, args.get(0), args.get(1));
    }
}
