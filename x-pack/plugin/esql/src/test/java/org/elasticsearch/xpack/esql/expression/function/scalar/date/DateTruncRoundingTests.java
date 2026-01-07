/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.date;

import org.elasticsearch.common.Rounding;
import org.elasticsearch.test.ESTestCase;

import java.time.Duration;
import java.time.Instant;
import java.time.Period;
import java.time.ZoneOffset;

import static org.elasticsearch.xpack.esql.expression.function.scalar.date.DateTrunc.processDatetime;
import static org.hamcrest.Matchers.containsString;

/**
 * This class supplements {@link DateTruncTests}.  The tests in this class are not run via the parametrized runner,
 * and exercise specific helper functions within the class.
 */
public class DateTruncRoundingTests extends ESTestCase {

    public void testCreateRoundingDuration() {
        Rounding.Prepared rounding;

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> createUtcRounding(Duration.ofHours(0)));
        assertThat(e.getMessage(), containsString("Zero or negative time interval is not supported"));

        e = expectThrows(IllegalArgumentException.class, () -> createUtcRounding(Duration.ofHours(-10)));
        assertThat(e.getMessage(), containsString("Zero or negative time interval is not supported"));

        rounding = createUtcRounding(Duration.ofHours(1));
        assertEquals(1, rounding.roundingSize(Rounding.DateTimeUnit.HOUR_OF_DAY), 0d);

        rounding = createUtcRounding(Duration.ofHours(10));
        assertEquals(10, rounding.roundingSize(Rounding.DateTimeUnit.HOUR_OF_DAY), 0d);

        rounding = createUtcRounding(Duration.ofMinutes(1));
        assertEquals(1, rounding.roundingSize(Rounding.DateTimeUnit.MINUTE_OF_HOUR), 0d);

        rounding = createUtcRounding(Duration.ofMinutes(100));
        assertEquals(100, rounding.roundingSize(Rounding.DateTimeUnit.MINUTE_OF_HOUR), 0d);

        rounding = createUtcRounding(Duration.ofSeconds(1));
        assertEquals(1, rounding.roundingSize(Rounding.DateTimeUnit.SECOND_OF_MINUTE), 0d);

        rounding = createUtcRounding(Duration.ofSeconds(120));
        assertEquals(120, rounding.roundingSize(Rounding.DateTimeUnit.SECOND_OF_MINUTE), 0d);

        rounding = createUtcRounding(Duration.ofSeconds(60).plusMinutes(5).plusHours(1));
        assertEquals(1 + 5 + 60, rounding.roundingSize(Rounding.DateTimeUnit.MINUTE_OF_HOUR), 0d);
    }

    public void testCreateRoundingPeriod() {
        Rounding.Prepared rounding;

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> createUtcRounding(Period.ofMonths(0)));
        assertThat(e.getMessage(), containsString("Zero or negative time interval is not supported"));

        e = expectThrows(IllegalArgumentException.class, () -> createUtcRounding(Period.ofYears(-10)));
        assertThat(e.getMessage(), containsString("Zero or negative time interval is not supported"));

        e = expectThrows(IllegalArgumentException.class, () -> createUtcRounding(Period.of(0, 1, 1)));
        assertThat(e.getMessage(), containsString("Time interval with multiple periods is not supported"));

        rounding = createUtcRounding(Period.ofDays(1));
        assertEquals(1, rounding.roundingSize(Rounding.DateTimeUnit.DAY_OF_MONTH), 0d);

        rounding = createUtcRounding(Period.ofDays(4));
        assertEquals(4, rounding.roundingSize(Rounding.DateTimeUnit.DAY_OF_MONTH), 0d);

        rounding = createUtcRounding(Period.ofDays(7));
        assertEquals(1, rounding.roundingSize(Rounding.DateTimeUnit.WEEK_OF_WEEKYEAR), 0d);

        rounding = createUtcRounding(Period.ofMonths(1));
        assertEquals(1, rounding.roundingSize(Rounding.DateTimeUnit.MONTH_OF_YEAR), 0d);

        rounding = createUtcRounding(Period.ofMonths(3));
        assertEquals(1, rounding.roundingSize(Rounding.DateTimeUnit.QUARTER_OF_YEAR), 0d);

        rounding = createUtcRounding(Period.ofMonths(5));
        assertEquals(1, rounding.roundingSize(Rounding.DateTimeUnit.MONTH_OF_YEAR), 0d);

        rounding = createUtcRounding(Period.ofYears(1));
        assertEquals(1, rounding.roundingSize(Rounding.DateTimeUnit.YEAR_OF_CENTURY), 0d);

        rounding = createUtcRounding(Period.ofYears(3));
        assertEquals(1, rounding.roundingSize(Rounding.DateTimeUnit.YEAR_OF_CENTURY), 0d);
    }

    public void testCreateRoundingNullInterval() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> createUtcRounding((Period) null));
        assertThat(e.getMessage(), containsString("Time interval is not supported"));
    }

    public void testDateTruncFunction() {
        long ts = toMillis("2023-02-17T10:25:33.38Z");

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> processDatetime(ts, createUtcRounding(Period.ofDays(-1)))
        );
        assertThat(e.getMessage(), containsString("Zero or negative time interval is not supported"));

        e = expectThrows(IllegalArgumentException.class, () -> processDatetime(ts, createUtcRounding(Duration.ofHours(-1))));
        assertThat(e.getMessage(), containsString("Zero or negative time interval is not supported"));
    }

    private static long toMillis(String timestamp) {
        return Instant.parse(timestamp).toEpochMilli();
    }

    private static Rounding.Prepared createUtcRounding(Duration duration) {
        return DateTrunc.createRounding(duration, ZoneOffset.UTC, null, null);
    }

    private static Rounding.Prepared createUtcRounding(Period period) {
        return DateTrunc.createRounding(period, ZoneOffset.UTC, null, null);
    }

}
