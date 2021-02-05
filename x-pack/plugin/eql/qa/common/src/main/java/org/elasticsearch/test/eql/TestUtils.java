/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.test.eql;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.util.Locale;

import static java.time.format.DateTimeFormatter.ISO_LOCAL_DATE;
import static java.time.temporal.ChronoField.HOUR_OF_DAY;
import static java.time.temporal.ChronoField.MINUTE_OF_HOUR;
import static java.time.temporal.ChronoField.NANO_OF_SECOND;
import static java.time.temporal.ChronoField.SECOND_OF_MINUTE;

public final class TestUtils {

    public static final ZoneId UTC = ZoneId.of("Z");

    public static final DateTimeFormatter ISO_DATE_WITH_NANOS = new DateTimeFormatterBuilder()
        .parseCaseInsensitive()
        .append(ISO_LOCAL_DATE)
        .appendLiteral('T')
        .appendValue(HOUR_OF_DAY, 2)
        .appendLiteral(':')
        .appendValue(MINUTE_OF_HOUR, 2)
        .appendLiteral(':')
        .appendValue(SECOND_OF_MINUTE, 2)
        .appendFraction(NANO_OF_SECOND, 3, 9, true)
        .appendOffsetId()
        .toFormatter(Locale.ROOT);

    public static ZonedDateTime asDateNanos(Long nanos) {
        ZonedDateTime zdt = ZonedDateTime.ofInstant(Instant.ofEpochMilli(nanos / 1_000_000_000), UTC);
        return zdt.withNano((int) (nanos % 1_000_000_000));
    }

    public static String asString(ZonedDateTime zdt) {
        return ISO_DATE_WITH_NANOS.format(zdt);
    }
}
