/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.date;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.common.Rounding;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.AbstractScalarFunctionTestCase;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;

import java.time.Duration;
import java.time.Instant;
import java.time.Period;
import java.util.List;
import java.util.function.Supplier;

import static org.elasticsearch.xpack.esql.expression.function.scalar.date.DateTrunc.createRounding;
import static org.elasticsearch.xpack.esql.expression.function.scalar.date.DateTrunc.process;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

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

    public void testCreateRoundingDuration() {
        Rounding.Prepared rounding;

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> createRounding(Duration.ofHours(0)));
        assertThat(e.getMessage(), containsString("Zero or negative time interval is not supported"));

        e = expectThrows(IllegalArgumentException.class, () -> createRounding(Duration.ofHours(-10)));
        assertThat(e.getMessage(), containsString("Zero or negative time interval is not supported"));

        rounding = createRounding(Duration.ofHours(1));
        assertEquals(1, rounding.roundingSize(Rounding.DateTimeUnit.HOUR_OF_DAY), 0d);

        rounding = createRounding(Duration.ofHours(10));
        assertEquals(10, rounding.roundingSize(Rounding.DateTimeUnit.HOUR_OF_DAY), 0d);

        rounding = createRounding(Duration.ofMinutes(1));
        assertEquals(1, rounding.roundingSize(Rounding.DateTimeUnit.MINUTES_OF_HOUR), 0d);

        rounding = createRounding(Duration.ofMinutes(100));
        assertEquals(100, rounding.roundingSize(Rounding.DateTimeUnit.MINUTES_OF_HOUR), 0d);

        rounding = createRounding(Duration.ofSeconds(1));
        assertEquals(1, rounding.roundingSize(Rounding.DateTimeUnit.SECOND_OF_MINUTE), 0d);

        rounding = createRounding(Duration.ofSeconds(120));
        assertEquals(120, rounding.roundingSize(Rounding.DateTimeUnit.SECOND_OF_MINUTE), 0d);

        rounding = createRounding(Duration.ofSeconds(60).plusMinutes(5).plusHours(1));
        assertEquals(1 + 5 + 60, rounding.roundingSize(Rounding.DateTimeUnit.MINUTES_OF_HOUR), 0d);
    }

    public void testCreateRoundingPeriod() {
        Rounding.Prepared rounding;

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> createRounding(Period.ofMonths(0)));
        assertThat(e.getMessage(), containsString("Zero or negative time interval is not supported"));

        e = expectThrows(IllegalArgumentException.class, () -> createRounding(Period.ofYears(-10)));
        assertThat(e.getMessage(), containsString("Zero or negative time interval is not supported"));

        e = expectThrows(IllegalArgumentException.class, () -> createRounding(Period.of(0, 1, 1)));
        assertThat(e.getMessage(), containsString("Time interval with multiple periods is not supported"));

        rounding = createRounding(Period.ofDays(1));
        assertEquals(1, rounding.roundingSize(Rounding.DateTimeUnit.DAY_OF_MONTH), 0d);

        rounding = createRounding(Period.ofDays(4));
        assertEquals(4, rounding.roundingSize(Rounding.DateTimeUnit.DAY_OF_MONTH), 0d);

        rounding = createRounding(Period.ofDays(7));
        assertEquals(1, rounding.roundingSize(Rounding.DateTimeUnit.WEEK_OF_WEEKYEAR), 0d);

        rounding = createRounding(Period.ofMonths(1));
        assertEquals(1, rounding.roundingSize(Rounding.DateTimeUnit.MONTH_OF_YEAR), 0d);

        rounding = createRounding(Period.ofMonths(3));
        assertEquals(1, rounding.roundingSize(Rounding.DateTimeUnit.QUARTER_OF_YEAR), 0d);

        rounding = createRounding(Period.ofYears(1));
        assertEquals(1, rounding.roundingSize(Rounding.DateTimeUnit.YEAR_OF_CENTURY), 0d);

        e = expectThrows(IllegalArgumentException.class, () -> createRounding(Period.ofYears(3)));
        assertThat(e.getMessage(), containsString("Time interval is not supported"));
    }

    public void testCreateRoundingNullInterval() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> createRounding(null));
        assertThat(e.getMessage(), containsString("Time interval is not supported"));
    }

    public void testDateTruncFunction() {
        long ts = toMillis("2023-02-17T10:25:33.38Z");

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> process(ts, createRounding(Period.ofDays(-1))));
        assertThat(e.getMessage(), containsString("Zero or negative time interval is not supported"));

        e = expectThrows(IllegalArgumentException.class, () -> process(ts, createRounding(Duration.ofHours(-1))));
        assertThat(e.getMessage(), containsString("Zero or negative time interval is not supported"));
    }

    private static TestCaseSupplier ofDatePeriod(Period period, long value, String expectedDate) {
        return new TestCaseSupplier(
            List.of(DataType.DATE_PERIOD, DataType.DATETIME),
            () -> new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(period, DataType.DATE_PERIOD, "interval"),
                    new TestCaseSupplier.TypedData(value, DataType.DATETIME, "date")
                ),
                "DateTruncEvaluator[date=Attribute[channel=1], interval=Attribute[channel=0]]",
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
                    new TestCaseSupplier.TypedData(duration, DataType.TIME_DURATION, "interval"),
                    new TestCaseSupplier.TypedData(value, DataType.DATETIME, "date")
                ),
                "DateTruncEvaluator[date=Attribute[channel=1], interval=Attribute[channel=0]]",
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
