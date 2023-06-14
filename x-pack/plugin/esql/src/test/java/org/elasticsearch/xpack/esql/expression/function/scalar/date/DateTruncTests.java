/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.date;

import org.elasticsearch.common.Rounding;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.SerializationTestUtils;
import org.elasticsearch.xpack.esql.type.EsqlDataTypes;
import org.elasticsearch.xpack.ql.expression.FieldAttribute;
import org.elasticsearch.xpack.ql.expression.Literal;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataTypes;
import org.elasticsearch.xpack.ql.type.DateEsField;
import org.elasticsearch.xpack.ql.type.EsField;

import java.time.Duration;
import java.time.Instant;
import java.time.Period;
import java.util.Collections;
import java.util.Map;

import static org.elasticsearch.xpack.esql.expression.function.scalar.date.DateTrunc.createRounding;
import static org.elasticsearch.xpack.esql.expression.function.scalar.date.DateTrunc.process;
import static org.hamcrest.Matchers.containsString;

public class DateTruncTests extends ESTestCase {

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
        assertThat(e.getMessage(), containsString("Time interval is not supported"));

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

        assertEquals(toMillis("2023-02-17T00:00:00.00Z"), process(ts, createRounding(Period.ofDays(1))));
        assertEquals(toMillis("2023-02-01T00:00:00.00Z"), process(ts, createRounding(Period.ofMonths(1))));
        assertEquals(toMillis("2023-01-01T00:00:00.00Z"), process(ts, createRounding(Period.ofYears(1))));

        assertEquals(toMillis("2023-02-12T00:00:00.00Z"), process(ts, createRounding(Period.ofDays(10))));
        // 7 days period should return weekly rounding
        assertEquals(toMillis("2023-02-13T00:00:00.00Z"), process(ts, createRounding(Period.ofDays(7))));
        // 3 months period should return quarterly
        assertEquals(toMillis("2023-01-01T00:00:00.00Z"), process(ts, createRounding(Period.ofMonths(3))));

        assertEquals(toMillis("2023-02-17T10:00:00.00Z"), process(ts, createRounding(Duration.ofHours(1))));
        assertEquals(toMillis("2023-02-17T10:25:00.00Z"), process(ts, createRounding(Duration.ofMinutes(1))));
        assertEquals(toMillis("2023-02-17T10:25:33.00Z"), process(ts, createRounding(Duration.ofSeconds(1))));

        assertEquals(toMillis("2023-02-17T09:00:00.00Z"), process(ts, createRounding(Duration.ofHours(3))));
        assertEquals(toMillis("2023-02-17T10:15:00.00Z"), process(ts, createRounding(Duration.ofMinutes(15))));
        assertEquals(toMillis("2023-02-17T10:25:30.00Z"), process(ts, createRounding(Duration.ofSeconds(30))));
        assertEquals(toMillis("2023-02-17T10:25:30.00Z"), process(ts, createRounding(Duration.ofSeconds(30))));

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> process(ts, createRounding(Period.ofDays(-1))));
        assertThat(e.getMessage(), containsString("Zero or negative time interval is not supported"));

        e = expectThrows(IllegalArgumentException.class, () -> process(ts, createRounding(Duration.ofHours(-1))));
        assertThat(e.getMessage(), containsString("Zero or negative time interval is not supported"));
    }

    private static long toMillis(String timestamp) {
        return Instant.parse(timestamp).toEpochMilli();
    }

    public void testSerialization() {
        var dateTrunc = new DateTrunc(Source.EMPTY, randomDateField(), randomDateIntervalLiteral());
        SerializationTestUtils.assertSerialization(dateTrunc);
    }

    private static FieldAttribute randomDateField() {
        String fieldName = randomAlphaOfLength(randomIntBetween(1, 25));
        String dateName = randomAlphaOfLength(randomIntBetween(1, 25));
        boolean hasDocValues = randomBoolean();
        if (randomBoolean()) {
            return new FieldAttribute(Source.EMPTY, fieldName, new EsField(dateName, DataTypes.DATETIME, Map.of(), hasDocValues));
        } else {
            return new FieldAttribute(Source.EMPTY, fieldName, DateEsField.dateEsField(dateName, Collections.emptyMap(), hasDocValues));
        }
    }

    private static Literal randomDateIntervalLiteral() {
        Duration duration = switch (randomInt(5)) {
            case 0 -> Duration.ofNanos(randomIntBetween(1, 100000));
            case 1 -> Duration.ofMillis(randomIntBetween(1, 1000));
            case 2 -> Duration.ofSeconds(randomIntBetween(1, 1000));
            case 3 -> Duration.ofMinutes(randomIntBetween(1, 1000));
            case 4 -> Duration.ofHours(randomIntBetween(1, 100));
            case 5 -> Duration.ofDays(randomIntBetween(1, 60));
            default -> throw new AssertionError();
        };
        return new Literal(Source.EMPTY, duration, EsqlDataTypes.TIME_DURATION);
    }
}
