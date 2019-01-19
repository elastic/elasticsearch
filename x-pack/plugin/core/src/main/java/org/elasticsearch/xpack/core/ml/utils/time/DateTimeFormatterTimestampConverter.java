/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.utils.time;

import org.elasticsearch.cli.SuppressForbidden;

import java.time.DateTimeException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.time.temporal.TemporalAccessor;

/**
 * <p> This class implements {@link TimestampConverter} using the {@link DateTimeFormatter}
 * of the Java 8 time API for parsing timestamps and other classes of that API for converting
 * timestamps to epoch times.
 *
 * <p> Objects of this class are <b>immutable</b> and <b>thread-safe</b>
 *
 */
public class DateTimeFormatterTimestampConverter implements TimestampConverter {
    private final DateTimeFormatter formatter;
    private final boolean hasTimeZone;
    private final ZoneId defaultZoneId;

    private DateTimeFormatterTimestampConverter(DateTimeFormatter dateTimeFormatter, boolean hasTimeZone, ZoneId defaultTimezone) {
        formatter = dateTimeFormatter;
        this.hasTimeZone = hasTimeZone;
        defaultZoneId = defaultTimezone;
    }

    /**
     * Creates a formatter according to the given pattern
     * @param pattern the pattern to be used by the formatter, not null.
     * See {@link DateTimeFormatter} for the syntax of the accepted patterns
     * @param defaultTimezone the timezone to be used for dates without timezone information.
     * @return a {@code TimestampConverter}
     * @throws IllegalArgumentException if the pattern is invalid or cannot produce a full timestamp
     * (e.g. contains a date but not a time)
     */
    public static TimestampConverter ofPattern(String pattern, ZoneId defaultTimezone) {
        DateTimeFormatter formatter = new DateTimeFormatterBuilder()
                .parseLenient()
                .appendPattern(pattern)
                .parseDefaulting(ChronoField.YEAR_OF_ERA, LocalDate.now(defaultTimezone).getYear())
                .toFormatter();

        String formattedTime = formatter.format(ZonedDateTime.ofInstant(Instant.ofEpochSecond(0), ZoneOffset.UTC));
        try {
            TemporalAccessor parsed = formatter.parse(formattedTime);
            boolean hasTimeZone = parsed.isSupported(ChronoField.INSTANT_SECONDS);
            if (hasTimeZone) {
                Instant.from(parsed);
            }
            else {
                LocalDateTime.from(parsed);
            }
            return new DateTimeFormatterTimestampConverter(formatter, hasTimeZone, defaultTimezone);
        }
        catch (DateTimeException e) {
            throw new IllegalArgumentException("Timestamp cannot be derived from pattern: " + pattern, e);
        }
    }

    @Override
    public long toEpochSeconds(String timestamp) {
        return toInstant(timestamp).getEpochSecond();
    }

    @Override
    public long toEpochMillis(String timestamp) {
        return toInstant(timestamp).toEpochMilli();
    }

    private Instant toInstant(String timestamp) {
        TemporalAccessor parsed = formatter.parse(timestamp);
        if (hasTimeZone) {
            return Instant.from(parsed);
        }
        return toInstantUnsafelyIgnoringAmbiguity(parsed);
    }

    @SuppressForbidden(reason = "TODO https://github.com/elastic/x-pack-elasticsearch/issues/3810")
    private Instant toInstantUnsafelyIgnoringAmbiguity(TemporalAccessor parsed) {
        return LocalDateTime.from(parsed).atZone(defaultZoneId).toInstant();
    }
}
