/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.qautil;

import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.common.time.DateFormatters;

import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetTime;
import java.time.Period;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.TemporalAccessor;
import java.util.Locale;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import static java.time.format.DateTimeFormatter.ISO_LOCAL_DATE;
import static java.time.format.DateTimeFormatter.ISO_LOCAL_TIME;
import static java.time.temporal.ChronoField.HOUR_OF_DAY;
import static java.time.temporal.ChronoField.MINUTE_OF_HOUR;
import static java.time.temporal.ChronoField.NANO_OF_SECOND;
import static java.time.temporal.ChronoField.SECOND_OF_MINUTE;

public final class DateUtils
{
    public static final ZoneId UTC = ZoneId.of("Z");

    public static final String EMPTY = "";

    public static final DateTimeFormatter ISO_DATE_WITH_NANOS = new DateTimeFormatterBuilder().parseCaseInsensitive()
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
    public static final DateTimeFormatter ISO_TIME_WITH_NANOS = new DateTimeFormatterBuilder().parseCaseInsensitive()
        .appendValue(HOUR_OF_DAY, 2)
        .appendLiteral(':')
        .appendValue(MINUTE_OF_HOUR, 2)
        .appendLiteral(':')
        .appendValue(SECOND_OF_MINUTE, 2)
        .appendFraction(NANO_OF_SECOND, 3, 9, true)
        .appendOffsetId()
        .toFormatter(Locale.ROOT);

    public static final int SECONDS_PER_MINUTE = 60;
    public static final int SECONDS_PER_HOUR = SECONDS_PER_MINUTE * 60;
    public static final int SECONDS_PER_DAY = SECONDS_PER_HOUR * 24;

    private static final DateFormatter UTC_DATE_TIME_FORMATTER = DateFormatter.forPattern("strict_date_optional_time").withZone(UTC);

    private static final DateTimeFormatter ISO_LOCAL_TIME_OPTIONAL_TZ = new DateTimeFormatterBuilder().append(ISO_LOCAL_TIME)
        .optionalStart()
        .appendZoneOrOffsetId()
        .toFormatter(Locale.ROOT)
        .withZone(UTC);
    private static final DateTimeFormatter ISO_LOCAL_DATE_OPTIONAL_TIME_FORMATTER_T_LITERAL = new DateTimeFormatterBuilder().append(
        ISO_LOCAL_DATE
    ).optionalStart().appendLiteral('T').append(ISO_LOCAL_TIME_OPTIONAL_TZ).optionalEnd().toFormatter(Locale.ROOT).withZone(UTC);
    private static final DateTimeFormatter ISO_LOCAL_DATE_OPTIONAL_TIME_FORMATTER_WHITESPACE = new DateTimeFormatterBuilder().append(
        ISO_LOCAL_DATE
    ).optionalStart().appendLiteral(' ').append(ISO_LOCAL_TIME_OPTIONAL_TZ).optionalEnd().toFormatter(Locale.ROOT).withZone(UTC);
    // In Java 8 LocalDate.EPOCH is not available, introduced with later Java versions
    public static final LocalDate EPOCH = LocalDate.of(1970, 1, 1);
    public static final long DAY_IN_MILLIS = 60 * 60 * 24 * 1000L;

    private static final DateTimeFormatter DATE_OPTIONAL_TIME_FORMATTER_T_LITERAL = new DateTimeFormatterBuilder().append(ISO_LOCAL_DATE)
        .optionalStart()
        .appendLiteral('T')
        .append(ISO_LOCAL_TIME)
        .optionalStart()
        .appendZoneOrOffsetId()
        .optionalEnd()
        .toFormatter(Locale.ROOT)
        .withZone(UTC);
    private static final DateTimeFormatter DATE_OPTIONAL_TIME_FORMATTER_WHITESPACE = new DateTimeFormatterBuilder().append(ISO_LOCAL_DATE)
        .optionalStart()
        .appendLiteral(' ')
        .append(ISO_LOCAL_TIME)
        .optionalStart()
        .appendZoneOrOffsetId()
        .optionalEnd()
        .toFormatter(Locale.ROOT)
        .withZone(UTC);

    /**
     * Creates a datetime from the millis since epoch (thus the time-zone is UTC).
     */
    public static ZonedDateTime asDateTime(long millis) {
        return ZonedDateTime.ofInstant(Instant.ofEpochMilli(millis), UTC);
    }

    /**
     * Parses the given string into a DateTime using UTC as a default timezone.
     */
    public static ZonedDateTime asDateTime(String dateFormat) {
        int separatorIdx = dateFormat.indexOf('-'); // Find the first `-` date separator
        if (separatorIdx == 0) { // first char = `-` denotes a negative year
            separatorIdx = dateFormat.indexOf('-', 1); // Find the first `-` date separator past the negative year
        }
        // Find the second `-` date separator and move 3 places past the dayOfYear to find the time separator
        // e.g. 2020-06-01T10:20:30....
        // ^
        // +3 = ^
        separatorIdx = dateFormat.indexOf('-', separatorIdx + 1) + 3;

        // Avoid index out of bounds - it will lead to DateTimeParseException anyways
        if (separatorIdx >= dateFormat.length() || dateFormat.charAt(separatorIdx) == 'T') {
            return DateFormatters.from(DATE_OPTIONAL_TIME_FORMATTER_T_LITERAL.parse(dateFormat)).withZoneSameInstant(UTC);
        } else {
            return DateFormatters.from(DATE_OPTIONAL_TIME_FORMATTER_WHITESPACE.parse(dateFormat)).withZoneSameInstant(UTC);
        }
    }

    /**
     * Parses the given string into a ZonedDateTime using the provided timezone.
     */
    public static ZonedDateTime asDateTimeWithNanos(String dateFormat, ZoneId zoneId) {
        return DateFormatters.from(ISO_DATE_WITH_NANOS.parse(dateFormat)).withZoneSameInstant(zoneId);
    }

    public static String toString(Object value) {
        if (value == null)
        {
            return "null";
        }

        if (value instanceof ZonedDateTime) {
            return ((ZonedDateTime) value).format(ISO_DATE_WITH_NANOS);
        }
        if (value instanceof OffsetTime) {
            return ((OffsetTime) value).format(ISO_TIME_WITH_NANOS);
        }
        if (value instanceof Timestamp ts) {
            return ts.toInstant().toString();
        }

        // handle intervals
        // YEAR/MONTH/YEAR TO MONTH -> YEAR TO MONTH
        if (value instanceof Period p) {
            // +yyy-mm - 7 chars
            StringBuilder sb = new StringBuilder(7);
            if (p.isNegative()) {
                sb.append("-");
                p = p.negated();
            } else {
                sb.append("+");
            }
            sb.append(p.getYears());
            sb.append("-");
            sb.append(p.getMonths());
            return sb.toString();
        }

        // DAY/HOUR/MINUTE/SECOND (and variations) -> DAY_TO_SECOND
        if (value instanceof Duration d) {
            // +ddd hh:mm:ss.mmmmmmmmm - 23 chars
            StringBuilder sb = new StringBuilder(23);
            if (d.isNegative()) {
                sb.append("-");
                d = d.negated();
            } else {
                sb.append("+");
            }

            long durationInSec = d.getSeconds();

            sb.append(durationInSec / SECONDS_PER_DAY);
            sb.append(" ");
            durationInSec = durationInSec % SECONDS_PER_DAY;
            sb.append(indent(durationInSec / SECONDS_PER_HOUR));
            sb.append(":");
            durationInSec = durationInSec % SECONDS_PER_HOUR;
            sb.append(indent(durationInSec / SECONDS_PER_MINUTE));
            sb.append(":");
            durationInSec = durationInSec % SECONDS_PER_MINUTE;
            sb.append(indent(durationInSec));
            long millis = TimeUnit.NANOSECONDS.toMillis(d.getNano());
            if (millis > 0) {
                sb.append(".");
                while (millis % 10 == 0) {
                    millis /= 10;
                }
                sb.append(millis);
            }
            return sb.toString();
        }

        return Objects.toString(value);
    }

    private static String indent(long timeUnit) {
        return timeUnit < 10 ? "0" + timeUnit : Long.toString(timeUnit);
    }

    /**
     * Creates an date for SQL DATE type from the millis since epoch.
     */
    public static ZonedDateTime asDateOnly(long millis) {
        return ZonedDateTime.ofInstant(Instant.ofEpochMilli(millis), UTC).toLocalDate().atStartOfDay(UTC);
    }

    /**
     * Creates an date for SQL TIME type from the millis since epoch.
     */
    public static OffsetTime asTimeOnly(long millis) {
        return OffsetTime.ofInstant(Instant.ofEpochMilli(millis % DAY_IN_MILLIS), UTC);
    }

    /**
     * Creates an date for SQL TIME type from the millis since epoch.
     */
    public static OffsetTime asTimeOnly(long millis, ZoneId zoneId) {
        return OffsetTime.ofInstant(Instant.ofEpochMilli(millis % DAY_IN_MILLIS), zoneId);
    }

    public static OffsetTime asTimeAtZone(OffsetTime time, ZoneId zonedId) {
        return time.atDate(EPOCH).atZoneSameInstant(zonedId).toOffsetDateTime().toOffsetTime();
    }

    /**
     * Creates a datetime from the millis since epoch (thus the time-zone is UTC).
     */
    public static ZonedDateTime asDateTimeWithMillis(long millis) {
        return ZonedDateTime.ofInstant(Instant.ofEpochMilli(millis), UTC);
    }

    /**
     * Creates a datetime from the millis since epoch then translates the date into the given timezone.
     */
    public static ZonedDateTime asDateTimeWithMillis(long millis, ZoneId id) {
        return ZonedDateTime.ofInstant(Instant.ofEpochMilli(millis), id);
    }

    /**
     * Parses the given string into a Date (SQL DATE type) using UTC as a default timezone.
     */
    public static ZonedDateTime asDateOnly(String dateFormat) {
        int separatorIdx = timeSeparatorIdx(dateFormat);
        // Avoid index out of bounds - it will lead to DateTimeParseException anyways
        if (separatorIdx >= dateFormat.length() || dateFormat.charAt(separatorIdx) == 'T') {
            return LocalDate.parse(dateFormat, ISO_LOCAL_DATE_OPTIONAL_TIME_FORMATTER_T_LITERAL).atStartOfDay(UTC);
        } else {
            return LocalDate.parse(dateFormat, ISO_LOCAL_DATE_OPTIONAL_TIME_FORMATTER_WHITESPACE).atStartOfDay(UTC);
        }
    }

    public static ZonedDateTime asDateOnly(ZonedDateTime zdt) {
        return zdt.toLocalDate().atStartOfDay(zdt.getZone());
    }

    public static OffsetTime asTimeOnly(String timeFormat) {
        return DateFormatters.from(ISO_LOCAL_TIME_OPTIONAL_TZ.parse(timeFormat)).toOffsetDateTime().toOffsetTime();
    }

    /**
     * Parses the given string into a DateTime using UTC as a default timezone.
     */
    public static ZonedDateTime asDateTimeWithNanos(String dateFormat) {
        return DateFormatters.from(UTC_DATE_TIME_FORMATTER.parse(dateFormat)).withZoneSameInstant(UTC);
    }

    public static ZonedDateTime dateTimeOfEscapedLiteral(String dateFormat) {
        int separatorIdx = timeSeparatorIdx(dateFormat);
        // Avoid index out of bounds - it will lead to DateTimeParseException anyways
        if (separatorIdx >= dateFormat.length() || dateFormat.charAt(separatorIdx) == 'T') {
            return ZonedDateTime.parse(dateFormat, ISO_LOCAL_DATE_OPTIONAL_TIME_FORMATTER_T_LITERAL);
        } else {
            return ZonedDateTime.parse(dateFormat, ISO_LOCAL_DATE_OPTIONAL_TIME_FORMATTER_WHITESPACE);
        }
    }

    public static String toDateString(ZonedDateTime date) {
        return date.format(ISO_LOCAL_DATE);
    }

    public static String toTimeString(OffsetTime time) {
        return toString(time);
    }

    public static long minDayInterval(long l) {
        if (l < DAY_IN_MILLIS) {
            return DAY_IN_MILLIS;
        }
        return l - (l % DAY_IN_MILLIS);
    }

    public static ZonedDateTime atTimeZone(LocalDate ld, ZoneId zoneId) {
        return ld.atStartOfDay(zoneId);
    }

    public static ZonedDateTime atTimeZone(LocalDateTime ldt, ZoneId zoneId) {
        return ZonedDateTime.ofInstant(ldt, zoneId.getRules().getValidOffsets(ldt).get(0), zoneId);
    }

    public static OffsetTime atTimeZone(OffsetTime ot, ZoneId zoneId) {
        LocalDateTime ldt = ot.atDate(LocalDate.EPOCH).toLocalDateTime();
        return ot.withOffsetSameInstant(zoneId.getRules().getValidOffsets(ldt).get(0));
    }

    public static OffsetTime atTimeZone(LocalTime lt, ZoneId zoneId) {
        LocalDateTime ldt = lt.atDate(LocalDate.EPOCH);
        return OffsetTime.of(lt, zoneId.getRules().getValidOffsets(ldt).get(0));
    }

    public static ZonedDateTime atTimeZone(ZonedDateTime zdt, ZoneId zoneId) {
        return zdt.withZoneSameInstant(zoneId);
    }

    public static TemporalAccessor atTimeZone(TemporalAccessor ta, ZoneId zoneId) {
        if (ta instanceof LocalDateTime localDateTime) {
            return atTimeZone(localDateTime, zoneId);
        } else if (ta instanceof ZonedDateTime zonedDateTime) {
            return atTimeZone(zonedDateTime, zoneId);
        } else if (ta instanceof OffsetTime offsetTime) {
            return atTimeZone(offsetTime, zoneId);
        } else if (ta instanceof LocalTime localTime) {
            return atTimeZone(localTime, zoneId);
        } else if (ta instanceof LocalDate localDate) {
            return atTimeZone(localDate, zoneId);
        } else {
            return ta;
        }
    }

    private static int timeSeparatorIdx(String timestampStr) {
        int separatorIdx = timestampStr.indexOf('-'); // Find the first `-` date separator
        if (separatorIdx == 0) { // first char = `-` denotes a negative year
            separatorIdx = timestampStr.indexOf('-', 1); // Find the first `-` date separator past the negative year
        }
        // Find the second `-` date separator and move 3 places past the dayOfYear to find the time separator
        // e.g. 2020-06-01T10:20:30....
        // ^
        // +3 = ^
        return timestampStr.indexOf('-', separatorIdx + 1) + 3;
    }
}
