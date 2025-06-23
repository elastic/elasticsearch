/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql.util;

import org.elasticsearch.common.time.DateFormatters;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.Foldables;
import org.elasticsearch.xpack.ql.type.DataTypes;
import org.elasticsearch.xpack.sql.parser.ParsingException;
import org.elasticsearch.xpack.sql.proto.StringUtils;
import org.elasticsearch.xpack.sql.type.SqlDataTypeConverter;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.TemporalAccessor;
import java.util.Locale;

import static java.time.format.DateTimeFormatter.ISO_LOCAL_DATE;
import static java.time.format.DateTimeFormatter.ISO_LOCAL_TIME;
import static org.elasticsearch.xpack.ql.util.DateUtils.UTC_DATE_TIME_FORMATTER;

public final class DateUtils {

    public static final ZoneId UTC = ZoneId.of("Z");
    // In Java 8 LocalDate.EPOCH is not available, introduced with later Java versions
    public static final LocalDate EPOCH = LocalDate.of(1970, 1, 1);
    public static final long DAY_IN_MILLIS = 60 * 60 * 24 * 1000L;

    private static final DateTimeFormatter ISO_LOCAL_TIME_OPTIONAL_TZ = new DateTimeFormatterBuilder().append(ISO_LOCAL_TIME)
        .optionalStart()
        .appendZoneOrOffsetId()
        .toFormatter(Locale.ROOT)
        .withZone(UTC);
    private static final DateTimeFormatter ISO_LOCAL_DATE_OPTIONAL_TIME_FORMATTER_WHITESPACE = new DateTimeFormatterBuilder().append(
        ISO_LOCAL_DATE
    ).optionalStart().appendLiteral(' ').append(ISO_LOCAL_TIME_OPTIONAL_TZ).optionalEnd().toFormatter(Locale.ROOT).withZone(UTC);
    private static final DateTimeFormatter ISO_LOCAL_DATE_OPTIONAL_TIME_FORMATTER_T_LITERAL = new DateTimeFormatterBuilder().append(
        ISO_LOCAL_DATE
    ).optionalStart().appendLiteral('T').append(ISO_LOCAL_TIME_OPTIONAL_TZ).optionalEnd().toFormatter(Locale.ROOT).withZone(UTC);

    private static final int DEFAULT_PRECISION_FOR_CURRENT_FUNCTIONS = 3;

    private DateUtils() {}

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
        return time.atDate(DateUtils.EPOCH).atZoneSameInstant(zonedId).toOffsetDateTime().toOffsetTime();
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

    public static String toString(ZonedDateTime dateTime) {
        return StringUtils.toString(dateTime);
    }

    public static String toDateString(ZonedDateTime date) {
        return date.format(ISO_LOCAL_DATE);
    }

    public static String toTimeString(OffsetTime time) {
        return StringUtils.toString(time);
    }

    public static long minDayInterval(long l) {
        if (l < DAY_IN_MILLIS) {
            return DAY_IN_MILLIS;
        }
        return l - (l % DAY_IN_MILLIS);
    }

    public static int getNanoPrecision(Expression precisionExpression, int nano) {
        int precision = DEFAULT_PRECISION_FOR_CURRENT_FUNCTIONS;

        if (precisionExpression != null) {
            try {
                precision = (Integer) SqlDataTypeConverter.convert(Foldables.valueOf(precisionExpression), DataTypes.INTEGER);
            } catch (Exception e) {
                throw new ParsingException(precisionExpression.source(), "invalid precision; " + e.getMessage());
            }
        }

        if (precision < 0 || precision > 9) {
            throw new ParsingException(
                precisionExpression.source(),
                "precision needs to be between [0-9], received [{}]",
                precisionExpression.sourceText()
            );
        }

        // remove the remainder
        nano = nano - nano % (int) Math.pow(10, (9 - precision));
        return nano;
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
