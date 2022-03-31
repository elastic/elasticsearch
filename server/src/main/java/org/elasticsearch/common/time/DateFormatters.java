/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.time;

import org.elasticsearch.common.Strings;
import org.elasticsearch.core.SuppressForbidden;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.Year;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.ResolverStyle;
import java.time.format.SignStyle;
import java.time.temporal.ChronoField;
import java.time.temporal.IsoFields;
import java.time.temporal.TemporalAccessor;
import java.time.temporal.TemporalAdjusters;
import java.time.temporal.TemporalQueries;
import java.time.temporal.TemporalQuery;
import java.time.temporal.WeekFields;
import java.util.Locale;

import static java.time.temporal.ChronoField.DAY_OF_MONTH;
import static java.time.temporal.ChronoField.DAY_OF_WEEK;
import static java.time.temporal.ChronoField.DAY_OF_YEAR;
import static java.time.temporal.ChronoField.HOUR_OF_DAY;
import static java.time.temporal.ChronoField.MINUTE_OF_HOUR;
import static java.time.temporal.ChronoField.MONTH_OF_YEAR;
import static java.time.temporal.ChronoField.NANO_OF_SECOND;
import static java.time.temporal.ChronoField.SECOND_OF_MINUTE;

public class DateFormatters {

    public static final WeekFields WEEK_FIELDS_ROOT = WeekFields.of(Locale.ROOT);

    private static final DateTimeFormatter TIME_ZONE_FORMATTER_NO_COLON = new DateTimeFormatterBuilder().appendOffset("+HHmm", "Z")
        .toFormatter(Locale.ROOT)
        .withResolverStyle(ResolverStyle.STRICT);

    private static final DateTimeFormatter STRICT_YEAR_MONTH_DAY_PRINTER = new DateTimeFormatterBuilder().appendValue(
        ChronoField.YEAR,
        4,
        9,
        SignStyle.EXCEEDS_PAD
    )
        .optionalStart()
        .appendLiteral("-")
        .appendValue(MONTH_OF_YEAR, 2, 2, SignStyle.NOT_NEGATIVE)
        .optionalStart()
        .appendLiteral('-')
        .appendValue(DAY_OF_MONTH, 2, 2, SignStyle.NOT_NEGATIVE)
        .optionalEnd()
        .optionalEnd()
        .toFormatter(Locale.ROOT)
        .withResolverStyle(ResolverStyle.STRICT);

    private static final DateTimeFormatter STRICT_YEAR_MONTH_DAY_FORMATTER = new DateTimeFormatterBuilder().appendValue(
        ChronoField.YEAR,
        4,
        4,
        SignStyle.EXCEEDS_PAD
    )
        .optionalStart()
        .appendLiteral("-")
        .appendValue(MONTH_OF_YEAR, 2, 2, SignStyle.NOT_NEGATIVE)
        .optionalStart()
        .appendLiteral('-')
        .appendValue(DAY_OF_MONTH, 2, 2, SignStyle.NOT_NEGATIVE)
        .optionalEnd()
        .optionalEnd()
        .toFormatter(Locale.ROOT)
        .withResolverStyle(ResolverStyle.STRICT);

    private static final DateTimeFormatter STRICT_HOUR_MINUTE_SECOND_FORMATTER = new DateTimeFormatterBuilder().appendValue(
        HOUR_OF_DAY,
        2,
        2,
        SignStyle.NOT_NEGATIVE
    )
        .appendLiteral(':')
        .appendValue(MINUTE_OF_HOUR, 2, 2, SignStyle.NOT_NEGATIVE)
        .appendLiteral(':')
        .appendValue(SECOND_OF_MINUTE, 2, 2, SignStyle.NOT_NEGATIVE)
        .toFormatter(Locale.ROOT)
        .withResolverStyle(ResolverStyle.STRICT);

    private static final DateTimeFormatter STRICT_DATE_OPTIONAL_TIME_PRINTER = new DateTimeFormatterBuilder().append(
        STRICT_YEAR_MONTH_DAY_PRINTER
    )
        .appendLiteral('T')
        .optionalStart()
        .appendValue(HOUR_OF_DAY, 2, 2, SignStyle.NOT_NEGATIVE)
        .optionalStart()
        .appendLiteral(':')
        .appendValue(MINUTE_OF_HOUR, 2, 2, SignStyle.NOT_NEGATIVE)
        .optionalStart()
        .appendLiteral(':')
        .appendValue(SECOND_OF_MINUTE, 2, 2, SignStyle.NOT_NEGATIVE)
        .optionalStart()
        .appendFraction(NANO_OF_SECOND, 3, 3, true)
        .optionalEnd()
        .optionalEnd()
        .optionalStart()
        .appendOffset("+HH:MM", "Z")
        .optionalEnd()
        .optionalEnd()
        .optionalEnd()
        .toFormatter(Locale.ROOT)
        .withResolverStyle(ResolverStyle.STRICT);

    private static final DateTimeFormatter STRICT_DATE_OPTIONAL_TIME_FORMATTER = new DateTimeFormatterBuilder().append(
        STRICT_YEAR_MONTH_DAY_FORMATTER
    )
        .optionalStart()
        .appendLiteral('T')
        .optionalStart()
        .appendValue(HOUR_OF_DAY, 2, 2, SignStyle.NOT_NEGATIVE)
        .optionalStart()
        .appendLiteral(':')
        .appendValue(MINUTE_OF_HOUR, 2, 2, SignStyle.NOT_NEGATIVE)
        .optionalStart()
        .appendLiteral(':')
        .appendValue(SECOND_OF_MINUTE, 2, 2, SignStyle.NOT_NEGATIVE)
        .optionalStart()
        .appendFraction(NANO_OF_SECOND, 1, 9, true)
        .optionalEnd()
        .optionalStart()
        .appendLiteral(',')
        .appendFraction(NANO_OF_SECOND, 1, 9, false)
        .optionalEnd()
        .optionalEnd()
        .optionalEnd()
        .optionalStart()
        .appendZoneOrOffsetId()
        .optionalEnd()
        .optionalStart()
        .append(TIME_ZONE_FORMATTER_NO_COLON)
        .optionalEnd()
        .optionalEnd()
        .optionalEnd()
        .toFormatter(Locale.ROOT)
        .withResolverStyle(ResolverStyle.STRICT);

    /**
     * Returns a generic ISO datetime parser where the date is mandatory and the time is optional.
     */
    private static final DateFormatter STRICT_DATE_OPTIONAL_TIME = new JavaDateFormatter(
        "strict_date_optional_time",
        STRICT_DATE_OPTIONAL_TIME_PRINTER,
        STRICT_DATE_OPTIONAL_TIME_FORMATTER
    );

    private static final DateTimeFormatter STRICT_DATE_OPTIONAL_TIME_FORMATTER_WITH_NANOS = new DateTimeFormatterBuilder().append(
        STRICT_YEAR_MONTH_DAY_FORMATTER
    )
        .optionalStart()
        .appendLiteral('T')
        .append(STRICT_HOUR_MINUTE_SECOND_FORMATTER)
        .optionalStart()
        .appendFraction(NANO_OF_SECOND, 1, 9, true)
        .optionalEnd()
        .optionalStart()
        .appendLiteral(',')
        .appendFraction(NANO_OF_SECOND, 1, 9, false)
        .optionalEnd()
        .optionalStart()
        .appendZoneOrOffsetId()
        .optionalEnd()
        .optionalStart()
        .append(TIME_ZONE_FORMATTER_NO_COLON)
        .optionalEnd()
        .optionalEnd()
        .toFormatter(Locale.ROOT)
        .withResolverStyle(ResolverStyle.STRICT);

    private static final DateTimeFormatter STRICT_DATE_OPTIONAL_TIME_PRINTER_NANOS = new DateTimeFormatterBuilder().append(
        STRICT_YEAR_MONTH_DAY_PRINTER
    )
        .appendLiteral('T')
        .optionalStart()
        .appendValue(HOUR_OF_DAY, 2, 2, SignStyle.NOT_NEGATIVE)
        .optionalStart()
        .appendLiteral(':')
        .appendValue(MINUTE_OF_HOUR, 2, 2, SignStyle.NOT_NEGATIVE)
        .optionalStart()
        .appendLiteral(':')
        .appendValue(SECOND_OF_MINUTE, 2, 2, SignStyle.NOT_NEGATIVE)
        .optionalStart()
        .appendFraction(NANO_OF_SECOND, 3, 9, true)
        .optionalEnd()
        .optionalEnd()
        .optionalStart()
        .appendOffset("+HH:MM", "Z")
        .optionalEnd()
        .optionalEnd()
        .optionalEnd()
        .toFormatter(Locale.ROOT)
        .withResolverStyle(ResolverStyle.STRICT);

    /**
     * Returns a generic ISO datetime parser where the date is mandatory and the time is optional with nanosecond resolution.
     */
    private static final DateFormatter STRICT_DATE_OPTIONAL_TIME_NANOS = new JavaDateFormatter(
        "strict_date_optional_time_nanos",
        STRICT_DATE_OPTIONAL_TIME_PRINTER_NANOS,
        STRICT_DATE_OPTIONAL_TIME_FORMATTER_WITH_NANOS
    );

    /**
     * Returns a ISO 8601 compatible date time formatter and parser.
     * This is not fully compatible to the existing spec, which would require far more edge cases, but merely compatible with the
     * existing legacy joda time ISO date formatter
     */
    private static final DateFormatter ISO_8601 = new JavaDateFormatter(
        "iso8601",
        STRICT_DATE_OPTIONAL_TIME_PRINTER,
        new DateTimeFormatterBuilder().append(STRICT_YEAR_MONTH_DAY_FORMATTER)
            .optionalStart()
            .appendLiteral('T')
            .optionalStart()
            .appendValue(HOUR_OF_DAY, 2, 2, SignStyle.NOT_NEGATIVE)
            .optionalStart()
            .appendLiteral(':')
            .appendValue(MINUTE_OF_HOUR, 2, 2, SignStyle.NOT_NEGATIVE)
            .optionalStart()
            .appendLiteral(':')
            .appendValue(SECOND_OF_MINUTE, 2, 2, SignStyle.NOT_NEGATIVE)
            .optionalStart()
            .appendFraction(NANO_OF_SECOND, 1, 9, true)
            .optionalEnd()
            .optionalStart()
            .appendLiteral(",")
            .appendFraction(NANO_OF_SECOND, 1, 9, false)
            .optionalEnd()
            .optionalEnd()
            .optionalEnd()
            .optionalEnd()
            .optionalStart()
            .appendZoneOrOffsetId()
            .optionalEnd()
            .optionalStart()
            .append(TIME_ZONE_FORMATTER_NO_COLON)
            .optionalEnd()
            .optionalEnd()
            .toFormatter(Locale.ROOT)
            .withResolverStyle(ResolverStyle.STRICT)
    );

    /////////////////////////////////////////
    //
    // BEGIN basic time formatters
    //
    // these formatters to not have any splitting characters between hours, minutes, seconds, milliseconds
    // this means they have to be strict with the exception of the last element
    //
    /////////////////////////////////////////

    private static final DateTimeFormatter BASIC_TIME_NO_MILLIS_BASE = new DateTimeFormatterBuilder().appendValue(
        HOUR_OF_DAY,
        2,
        2,
        SignStyle.NOT_NEGATIVE
    )
        .appendValue(MINUTE_OF_HOUR, 2, 2, SignStyle.NOT_NEGATIVE)
        .appendValue(SECOND_OF_MINUTE, 2, 2, SignStyle.NOT_NEGATIVE)
        .toFormatter(Locale.ROOT)
        .withResolverStyle(ResolverStyle.STRICT);

    /*
     * Returns a basic formatter for a two digit hour of day, two digit minute
     * of hour, two digit second of minute, and time zone offset (HHmmssZ).
     */
    private static final DateFormatter BASIC_TIME_NO_MILLIS = new JavaDateFormatter(
        "basic_time_no_millis",
        new DateTimeFormatterBuilder().append(BASIC_TIME_NO_MILLIS_BASE)
            .appendOffset("+HH:MM", "Z")
            .toFormatter(Locale.ROOT)
            .withResolverStyle(ResolverStyle.STRICT),
        new DateTimeFormatterBuilder().append(BASIC_TIME_NO_MILLIS_BASE)
            .appendZoneOrOffsetId()
            .toFormatter(Locale.ROOT)
            .withResolverStyle(ResolverStyle.STRICT),
        new DateTimeFormatterBuilder().append(BASIC_TIME_NO_MILLIS_BASE)
            .append(TIME_ZONE_FORMATTER_NO_COLON)
            .toFormatter(Locale.ROOT)
            .withResolverStyle(ResolverStyle.STRICT)
    );

    private static final DateTimeFormatter BASIC_TIME_FORMATTER = new DateTimeFormatterBuilder().appendValue(
        HOUR_OF_DAY,
        2,
        2,
        SignStyle.NOT_NEGATIVE
    )
        .appendValue(MINUTE_OF_HOUR, 2, 2, SignStyle.NOT_NEGATIVE)
        .appendValue(SECOND_OF_MINUTE, 2, 2, SignStyle.NOT_NEGATIVE)
        .appendFraction(NANO_OF_SECOND, 1, 9, true)
        .toFormatter(Locale.ROOT)
        .withResolverStyle(ResolverStyle.STRICT);

    private static final DateTimeFormatter BASIC_TIME_PRINTER = new DateTimeFormatterBuilder().appendValue(
        HOUR_OF_DAY,
        2,
        2,
        SignStyle.NOT_NEGATIVE
    )
        .appendValue(MINUTE_OF_HOUR, 2, 2, SignStyle.NOT_NEGATIVE)
        .appendValue(SECOND_OF_MINUTE, 2, 2, SignStyle.NOT_NEGATIVE)
        .appendFraction(NANO_OF_SECOND, 3, 3, true)
        .toFormatter(Locale.ROOT)
        .withResolverStyle(ResolverStyle.STRICT);

    /*
     * Returns a basic formatter for a two digit hour of day, two digit minute
     * of hour, two digit second of minute, three digit millis, and time zone
     * offset (HHmmss.SSSZ).
     */
    private static final DateFormatter BASIC_TIME = new JavaDateFormatter(
        "basic_time",
        new DateTimeFormatterBuilder().append(BASIC_TIME_PRINTER)
            .appendOffset("+HH:MM", "Z")
            .toFormatter(Locale.ROOT)
            .withResolverStyle(ResolverStyle.STRICT),
        new DateTimeFormatterBuilder().append(BASIC_TIME_FORMATTER)
            .appendZoneOrOffsetId()
            .toFormatter(Locale.ROOT)
            .withResolverStyle(ResolverStyle.STRICT),
        new DateTimeFormatterBuilder().append(BASIC_TIME_FORMATTER)
            .append(TIME_ZONE_FORMATTER_NO_COLON)
            .toFormatter(Locale.ROOT)
            .withResolverStyle(ResolverStyle.STRICT)
    );

    private static final DateTimeFormatter BASIC_T_TIME_PRINTER = new DateTimeFormatterBuilder().appendLiteral("T")
        .append(BASIC_TIME_PRINTER)
        .toFormatter(Locale.ROOT)
        .withResolverStyle(ResolverStyle.STRICT);

    private static final DateTimeFormatter BASIC_T_TIME_FORMATTER = new DateTimeFormatterBuilder().appendLiteral("T")
        .append(BASIC_TIME_FORMATTER)
        .toFormatter(Locale.ROOT)
        .withResolverStyle(ResolverStyle.STRICT);

    /*
     * Returns a basic formatter for a two digit hour of day, two digit minute
     * of hour, two digit second of minute, three digit millis, and time zone
     * offset prefixed by 'T' ('T'HHmmss.SSSZ).
     */
    private static final DateFormatter BASIC_T_TIME = new JavaDateFormatter(
        "basic_t_time",
        new DateTimeFormatterBuilder().append(BASIC_T_TIME_PRINTER)
            .appendOffset("+HH:MM", "Z")
            .toFormatter(Locale.ROOT)
            .withResolverStyle(ResolverStyle.STRICT),
        new DateTimeFormatterBuilder().append(BASIC_T_TIME_FORMATTER)
            .appendZoneOrOffsetId()
            .toFormatter(Locale.ROOT)
            .withResolverStyle(ResolverStyle.STRICT),
        new DateTimeFormatterBuilder().append(BASIC_T_TIME_FORMATTER).append(TIME_ZONE_FORMATTER_NO_COLON).toFormatter(Locale.ROOT)
    );

    /*
     * Returns a basic formatter for a two digit hour of day, two digit minute
     * of hour, two digit second of minute, and time zone offset prefixed by 'T'
     * ('T'HHmmssZ).
     */
    private static final DateFormatter BASIC_T_TIME_NO_MILLIS = new JavaDateFormatter(
        "basic_t_time_no_millis",
        new DateTimeFormatterBuilder().appendLiteral("T")
            .append(BASIC_TIME_NO_MILLIS_BASE)
            .appendOffset("+HH:MM", "Z")
            .toFormatter(Locale.ROOT)
            .withResolverStyle(ResolverStyle.STRICT),
        new DateTimeFormatterBuilder().appendLiteral("T")
            .append(BASIC_TIME_NO_MILLIS_BASE)
            .appendZoneOrOffsetId()
            .toFormatter(Locale.ROOT)
            .withResolverStyle(ResolverStyle.STRICT),
        new DateTimeFormatterBuilder().appendLiteral("T")
            .append(BASIC_TIME_NO_MILLIS_BASE)
            .append(TIME_ZONE_FORMATTER_NO_COLON)
            .toFormatter(Locale.ROOT)
            .withResolverStyle(ResolverStyle.STRICT)
    );

    private static final DateTimeFormatter BASIC_YEAR_MONTH_DAY_PRINTER = new DateTimeFormatterBuilder().appendValue(
        ChronoField.YEAR,
        4,
        10,
        SignStyle.NORMAL
    )
        .appendValue(MONTH_OF_YEAR, 2, 2, SignStyle.NOT_NEGATIVE)
        .appendValue(DAY_OF_MONTH, 2, 2, SignStyle.NOT_NEGATIVE)
        .toFormatter(Locale.ROOT)
        .withResolverStyle(ResolverStyle.STRICT);

    private static final DateTimeFormatter BASIC_YEAR_MONTH_DAY_FORMATTER = new DateTimeFormatterBuilder().appendValue(
        ChronoField.YEAR,
        4,
        4,
        SignStyle.NORMAL
    )
        .appendValue(MONTH_OF_YEAR, 2, 2, SignStyle.NOT_NEGATIVE)
        .appendValue(DAY_OF_MONTH, 2, 2, SignStyle.NOT_NEGATIVE)
        .toFormatter(Locale.ROOT)
        .withResolverStyle(ResolverStyle.STRICT);

    private static final DateTimeFormatter BASIC_DATE_TIME_FORMATTER = new DateTimeFormatterBuilder().append(BASIC_YEAR_MONTH_DAY_FORMATTER)
        .append(BASIC_T_TIME_FORMATTER)
        .toFormatter(Locale.ROOT)
        .withResolverStyle(ResolverStyle.STRICT);

    private static final DateTimeFormatter BASIC_DATE_TIME_PRINTER = new DateTimeFormatterBuilder().append(BASIC_YEAR_MONTH_DAY_PRINTER)
        .append(BASIC_T_TIME_PRINTER)
        .toFormatter(Locale.ROOT)
        .withResolverStyle(ResolverStyle.STRICT);

    /*
     * Returns a basic formatter that combines a basic date and time, separated
     * by a 'T' (uuuuMMdd'T'HHmmss.SSSZ).
     */
    private static final DateFormatter BASIC_DATE_TIME = new JavaDateFormatter(
        "basic_date_time",
        new DateTimeFormatterBuilder().append(BASIC_DATE_TIME_PRINTER)
            .appendOffset("+HH:MM", "Z")
            .toFormatter(Locale.ROOT)
            .withResolverStyle(ResolverStyle.STRICT),
        new DateTimeFormatterBuilder().append(BASIC_DATE_TIME_FORMATTER)
            .appendZoneOrOffsetId()
            .toFormatter(Locale.ROOT)
            .withResolverStyle(ResolverStyle.STRICT),
        new DateTimeFormatterBuilder().append(BASIC_DATE_TIME_FORMATTER)
            .append(TIME_ZONE_FORMATTER_NO_COLON)
            .toFormatter(Locale.ROOT)
            .withResolverStyle(ResolverStyle.STRICT)
    );

    private static final DateTimeFormatter BASIC_DATE_T = new DateTimeFormatterBuilder().append(BASIC_YEAR_MONTH_DAY_FORMATTER)
        .appendLiteral("T")
        .toFormatter(Locale.ROOT)
        .withResolverStyle(ResolverStyle.STRICT);

    private static final DateTimeFormatter BASIC_DATE_T_PRINTER = new DateTimeFormatterBuilder().append(BASIC_YEAR_MONTH_DAY_PRINTER)
        .appendLiteral("T")
        .toFormatter(Locale.ROOT)
        .withResolverStyle(ResolverStyle.STRICT);

    /*
     * Returns a basic formatter that combines a basic date and time without millis,
     * separated by a 'T' (uuuuMMdd'T'HHmmssZ).
     */
    private static final DateFormatter BASIC_DATE_TIME_NO_MILLIS = new JavaDateFormatter(
        "basic_date_time_no_millis",
        new DateTimeFormatterBuilder().append(BASIC_DATE_T_PRINTER)
            .append(BASIC_TIME_NO_MILLIS_BASE)
            .appendOffset("+HH:MM", "Z")
            .toFormatter(Locale.ROOT)
            .withResolverStyle(ResolverStyle.STRICT),
        new DateTimeFormatterBuilder().append(BASIC_DATE_T)
            .append(BASIC_TIME_NO_MILLIS_BASE)
            .appendZoneOrOffsetId()
            .toFormatter(Locale.ROOT)
            .withResolverStyle(ResolverStyle.STRICT),
        new DateTimeFormatterBuilder().append(BASIC_DATE_T)
            .append(BASIC_TIME_NO_MILLIS_BASE)
            .append(TIME_ZONE_FORMATTER_NO_COLON)
            .toFormatter(Locale.ROOT)
            .withResolverStyle(ResolverStyle.STRICT)
    );

    /*
     * Returns a formatter for a full ordinal date, using a four
     * digit year and three digit dayOfYear (uuuuDDD).
     */
    private static final DateFormatter BASIC_ORDINAL_DATE = new JavaDateFormatter(
        "basic_ordinal_date",
        DateTimeFormatter.ofPattern("uuuuDDD", Locale.ROOT)
    );

    /*
     * Returns a formatter for a full ordinal date and time, using a four
     * digit year and three digit dayOfYear (uuuuDDD'T'HHmmss.SSSZ).
     */
    private static final DateFormatter BASIC_ORDINAL_DATE_TIME = new JavaDateFormatter(
        "basic_ordinal_date_time",
        new DateTimeFormatterBuilder().appendPattern("yyyyDDD")
            .append(BASIC_T_TIME_PRINTER)
            .appendOffset("+HH:MM", "Z")
            .toFormatter(Locale.ROOT)
            .withResolverStyle(ResolverStyle.STRICT),
        new DateTimeFormatterBuilder().appendPattern("yyyyDDD")
            .append(BASIC_T_TIME_FORMATTER)
            .appendZoneOrOffsetId()
            .toFormatter(Locale.ROOT)
            .withResolverStyle(ResolverStyle.STRICT),
        new DateTimeFormatterBuilder().appendPattern("yyyyDDD")
            .append(BASIC_T_TIME_FORMATTER)
            .append(TIME_ZONE_FORMATTER_NO_COLON)
            .toFormatter(Locale.ROOT)
            .withResolverStyle(ResolverStyle.STRICT)

    );

    /*
     * Returns a formatter for a full ordinal date and time without millis,
     * using a four digit year and three digit dayOfYear (uuuuDDD'T'HHmmssZ).
     */
    private static final DateFormatter BASIC_ORDINAL_DATE_TIME_NO_MILLIS = new JavaDateFormatter(
        "basic_ordinal_date_time_no_millis",
        new DateTimeFormatterBuilder().appendPattern("uuuuDDD")
            .appendLiteral("T")
            .append(BASIC_TIME_NO_MILLIS_BASE)
            .appendOffset("+HH:MM", "Z")
            .toFormatter(Locale.ROOT)
            .withResolverStyle(ResolverStyle.STRICT),
        new DateTimeFormatterBuilder().appendPattern("uuuuDDD")
            .appendLiteral("T")
            .append(BASIC_TIME_NO_MILLIS_BASE)
            .appendZoneOrOffsetId()
            .toFormatter(Locale.ROOT)
            .withResolverStyle(ResolverStyle.STRICT),
        new DateTimeFormatterBuilder().appendPattern("uuuuDDD")
            .appendLiteral("T")
            .append(BASIC_TIME_NO_MILLIS_BASE)
            .append(TIME_ZONE_FORMATTER_NO_COLON)
            .toFormatter(Locale.ROOT)
            .withResolverStyle(ResolverStyle.STRICT)
    );

    private static final DateTimeFormatter BASIC_WEEK_DATE_FORMATTER = new DateTimeFormatterBuilder().appendValue(IsoFields.WEEK_BASED_YEAR)
        .appendLiteral("W")
        .appendValue(IsoFields.WEEK_OF_WEEK_BASED_YEAR, 1, 2, SignStyle.NEVER)
        .appendValue(ChronoField.DAY_OF_WEEK)
        .toFormatter(Locale.ROOT)
        .withResolverStyle(ResolverStyle.STRICT);

    /////////////////////////////////////////
    //
    // END basic time formatters
    //
    /////////////////////////////////////////

    /////////////////////////////////////////
    //
    // start strict formatters
    //
    /////////////////////////////////////////
    private static final DateTimeFormatter STRICT_BASIC_WEEK_DATE_FORMATTER = new DateTimeFormatterBuilder().parseStrict()
        .appendValue(IsoFields.WEEK_BASED_YEAR, 4)
        .appendLiteral("W")
        .appendValue(IsoFields.WEEK_OF_WEEK_BASED_YEAR, 1, 2, SignStyle.NEVER)
        .appendValue(ChronoField.DAY_OF_WEEK)
        .toFormatter(Locale.ROOT)
        .withResolverStyle(ResolverStyle.STRICT);

    private static final DateTimeFormatter STRICT_BASIC_WEEK_DATE_PRINTER = new DateTimeFormatterBuilder().parseStrict()
        .appendValue(IsoFields.WEEK_BASED_YEAR, 4, 10, SignStyle.NORMAL)
        .appendLiteral("W")
        .appendValue(IsoFields.WEEK_OF_WEEK_BASED_YEAR, 2, 2, SignStyle.NEVER)
        .appendValue(ChronoField.DAY_OF_WEEK)
        .toFormatter(Locale.ROOT)
        .withResolverStyle(ResolverStyle.STRICT);

    /*
     * Returns a basic formatter for a full date as four digit weekyear, two
     * digit week of weekyear, and one digit day of week (xxxx'W'wwe).
     */
    private static final DateFormatter STRICT_BASIC_WEEK_DATE = new JavaDateFormatter(
        "strict_basic_week_date",
        STRICT_BASIC_WEEK_DATE_PRINTER,
        STRICT_BASIC_WEEK_DATE_FORMATTER
    );

    /*
     * Returns a basic formatter that combines a basic weekyear date and time
     * without millis, separated by a 'T' (xxxx'W'wwe'T'HHmmssX).
     */
    private static final DateFormatter STRICT_BASIC_WEEK_DATE_TIME_NO_MILLIS = new JavaDateFormatter(
        "strict_basic_week_date_time_no_millis",
        new DateTimeFormatterBuilder().append(STRICT_BASIC_WEEK_DATE_PRINTER)
            .appendLiteral("T")
            .appendValue(HOUR_OF_DAY, 2, 2, SignStyle.NOT_NEGATIVE)
            .appendValue(MINUTE_OF_HOUR, 2, 2, SignStyle.NOT_NEGATIVE)
            .appendValue(SECOND_OF_MINUTE, 2, 2, SignStyle.NOT_NEGATIVE)
            .appendOffset("+HH:MM", "Z")
            .toFormatter(Locale.ROOT)
            .withResolverStyle(ResolverStyle.STRICT),
        new DateTimeFormatterBuilder().append(STRICT_BASIC_WEEK_DATE_PRINTER)
            .appendLiteral("T")
            .appendValue(HOUR_OF_DAY, 2, 2, SignStyle.NOT_NEGATIVE)
            .appendValue(MINUTE_OF_HOUR, 2, 2, SignStyle.NOT_NEGATIVE)
            .appendValue(SECOND_OF_MINUTE, 2, 2, SignStyle.NOT_NEGATIVE)
            .appendZoneOrOffsetId()
            .toFormatter(Locale.ROOT)
            .withResolverStyle(ResolverStyle.STRICT),
        new DateTimeFormatterBuilder().append(STRICT_BASIC_WEEK_DATE_PRINTER)
            .appendLiteral("T")
            .appendValue(HOUR_OF_DAY, 2, 2, SignStyle.NOT_NEGATIVE)
            .appendValue(MINUTE_OF_HOUR, 2, 2, SignStyle.NOT_NEGATIVE)
            .appendValue(SECOND_OF_MINUTE, 2, 2, SignStyle.NOT_NEGATIVE)
            .append(TIME_ZONE_FORMATTER_NO_COLON)
            .toFormatter(Locale.ROOT)
            .withResolverStyle(ResolverStyle.STRICT)
    );

    /*
     * Returns a basic formatter that combines a basic weekyear date and time,
     * separated by a 'T' (xxxx'W'wwe'T'HHmmss.SSSX).
     */
    private static final DateFormatter STRICT_BASIC_WEEK_DATE_TIME = new JavaDateFormatter(
        "strict_basic_week_date_time",
        new DateTimeFormatterBuilder().append(STRICT_BASIC_WEEK_DATE_PRINTER)
            .append(DateTimeFormatter.ofPattern("'T'HHmmss.SSSX", Locale.ROOT))
            .toFormatter(Locale.ROOT)
            .withResolverStyle(ResolverStyle.STRICT),
        new DateTimeFormatterBuilder().append(STRICT_BASIC_WEEK_DATE_FORMATTER)
            .appendLiteral("T")
            .appendValue(HOUR_OF_DAY, 2, 2, SignStyle.NOT_NEGATIVE)
            .appendValue(MINUTE_OF_HOUR, 2, 2, SignStyle.NOT_NEGATIVE)
            .appendValue(SECOND_OF_MINUTE, 2, 2, SignStyle.NOT_NEGATIVE)
            .appendFraction(NANO_OF_SECOND, 1, 9, true)
            .appendZoneOrOffsetId()
            .toFormatter(Locale.ROOT)
            .withResolverStyle(ResolverStyle.STRICT),
        new DateTimeFormatterBuilder().append(STRICT_BASIC_WEEK_DATE_FORMATTER)
            .appendLiteral("T")
            .appendValue(HOUR_OF_DAY, 2, 2, SignStyle.NOT_NEGATIVE)
            .appendValue(MINUTE_OF_HOUR, 2, 2, SignStyle.NOT_NEGATIVE)
            .appendValue(SECOND_OF_MINUTE, 2, 2, SignStyle.NOT_NEGATIVE)
            .appendFraction(NANO_OF_SECOND, 1, 9, true)
            .append(TIME_ZONE_FORMATTER_NO_COLON)
            .toFormatter(Locale.ROOT)
            .withResolverStyle(ResolverStyle.STRICT)
    );

    /*
     * An ISO date formatter that formats or parses a date without an offset, such as '2011-12-03'.
     */
    private static final DateFormatter STRICT_DATE = new JavaDateFormatter(
        "strict_date",
        DateTimeFormatter.ISO_LOCAL_DATE.withResolverStyle(ResolverStyle.LENIENT).withLocale(Locale.ROOT)
    );

    /*
     * A date formatter that formats or parses a date plus an hour without an offset, such as '2011-12-03T01'.
     */
    private static final DateFormatter STRICT_DATE_HOUR = new JavaDateFormatter(
        "strict_date_hour",
        DateTimeFormatter.ofPattern("uuuu-MM-dd'T'HH", Locale.ROOT)
    );

    /*
     * A date formatter that formats or parses a date plus an hour/minute without an offset, such as '2011-12-03T01:10'.
     */
    private static final DateFormatter STRICT_DATE_HOUR_MINUTE = new JavaDateFormatter(
        "strict_date_hour_minute",
        DateTimeFormatter.ofPattern("uuuu-MM-dd'T'HH:mm", Locale.ROOT)
    );

    /*
     * A strict date formatter that formats or parses a date without an offset, such as '2011-12-03'.
     */
    private static final DateFormatter STRICT_YEAR_MONTH_DAY = new JavaDateFormatter(
        "strict_year_month_day",
        STRICT_YEAR_MONTH_DAY_FORMATTER
    );

    /*
     * A strict formatter that formats or parses a year and a month, such as '2011-12'.
     */
    private static final DateFormatter STRICT_YEAR_MONTH = new JavaDateFormatter(
        "strict_year_month",
        new DateTimeFormatterBuilder().appendValue(ChronoField.YEAR, 4, 4, SignStyle.EXCEEDS_PAD)
            .appendLiteral("-")
            .appendValue(MONTH_OF_YEAR, 2, 2, SignStyle.NOT_NEGATIVE)
            .toFormatter(Locale.ROOT)
            .withResolverStyle(ResolverStyle.STRICT)
    );

    /*
     * A strict formatter that formats or parses a year, such as '2011'.
     */
    private static final DateFormatter STRICT_YEAR = new JavaDateFormatter(
        "strict_year",
        new DateTimeFormatterBuilder().appendValue(ChronoField.YEAR, 4, 4, SignStyle.EXCEEDS_PAD)
            .toFormatter(Locale.ROOT)
            .withResolverStyle(ResolverStyle.STRICT)
    );

    /*
     * A strict formatter that formats or parses a hour, minute and second, such as '09:43:25'.
     */
    private static final DateFormatter STRICT_HOUR_MINUTE_SECOND = new JavaDateFormatter(
        "strict_hour_minute_second",
        STRICT_HOUR_MINUTE_SECOND_FORMATTER
    );

    private static final DateTimeFormatter STRICT_DATE_PRINTER = new DateTimeFormatterBuilder().append(STRICT_YEAR_MONTH_DAY_FORMATTER)
        .appendLiteral('T')
        .append(STRICT_HOUR_MINUTE_SECOND_FORMATTER)
        .appendFraction(NANO_OF_SECOND, 3, 9, true)
        .appendOffset("+HH:MM", "Z")
        .toFormatter(Locale.ROOT)
        .withResolverStyle(ResolverStyle.STRICT);

    private static final DateTimeFormatter STRICT_DATE_FORMATTER = new DateTimeFormatterBuilder().append(STRICT_YEAR_MONTH_DAY_FORMATTER)
        .appendLiteral('T')
        .append(STRICT_HOUR_MINUTE_SECOND_FORMATTER)
        .optionalStart()
        .appendFraction(NANO_OF_SECOND, 1, 9, true)
        .optionalEnd()
        .toFormatter(Locale.ROOT)
        .withResolverStyle(ResolverStyle.STRICT);

    /*
     * Returns a formatter that combines a full date and time, separated by a 'T'
     * (uuuu-MM-dd'T'HH:mm:ss.SSSZZ).
     */
    private static final DateFormatter STRICT_DATE_TIME = new JavaDateFormatter(
        "strict_date_time",
        STRICT_DATE_PRINTER,
        new DateTimeFormatterBuilder().append(STRICT_DATE_FORMATTER)
            .appendZoneOrOffsetId()
            .toFormatter(Locale.ROOT)
            .withResolverStyle(ResolverStyle.STRICT),
        new DateTimeFormatterBuilder().append(STRICT_DATE_FORMATTER)
            .append(TIME_ZONE_FORMATTER_NO_COLON)
            .toFormatter(Locale.ROOT)
            .withResolverStyle(ResolverStyle.STRICT)
    );

    private static final DateTimeFormatter STRICT_ORDINAL_DATE_TIME_NO_MILLIS_BASE = new DateTimeFormatterBuilder().appendValue(
        ChronoField.YEAR,
        4,
        4,
        SignStyle.EXCEEDS_PAD
    )
        .appendLiteral('-')
        .appendValue(DAY_OF_YEAR, 3, 3, SignStyle.NOT_NEGATIVE)
        .appendLiteral('T')
        .append(STRICT_HOUR_MINUTE_SECOND_FORMATTER)
        .toFormatter(Locale.ROOT)
        .withResolverStyle(ResolverStyle.STRICT);

    /*
     * Returns a formatter for a full ordinal date and time without millis,
     * using a four digit year and three digit dayOfYear (uuuu-DDD'T'HH:mm:ssZZ).
     */
    private static final DateFormatter STRICT_ORDINAL_DATE_TIME_NO_MILLIS = new JavaDateFormatter(
        "strict_ordinal_date_time_no_millis",
        new DateTimeFormatterBuilder().append(STRICT_ORDINAL_DATE_TIME_NO_MILLIS_BASE)
            .appendOffset("+HH:MM", "Z")
            .toFormatter(Locale.ROOT)
            .withResolverStyle(ResolverStyle.STRICT),
        new DateTimeFormatterBuilder().append(STRICT_ORDINAL_DATE_TIME_NO_MILLIS_BASE)
            .appendZoneOrOffsetId()
            .toFormatter(Locale.ROOT)
            .withResolverStyle(ResolverStyle.STRICT),
        new DateTimeFormatterBuilder().append(STRICT_ORDINAL_DATE_TIME_NO_MILLIS_BASE)
            .append(TIME_ZONE_FORMATTER_NO_COLON)
            .toFormatter(Locale.ROOT)
            .withResolverStyle(ResolverStyle.STRICT)
    );

    private static final DateTimeFormatter STRICT_DATE_TIME_NO_MILLIS_FORMATTER = new DateTimeFormatterBuilder().append(
        STRICT_YEAR_MONTH_DAY_FORMATTER
    ).appendLiteral('T').append(STRICT_HOUR_MINUTE_SECOND_FORMATTER).toFormatter(Locale.ROOT).withResolverStyle(ResolverStyle.STRICT);

    /*
     * Returns a formatter that combines a full date and time without millis,
     * separated by a 'T' (uuuu-MM-dd'T'HH:mm:ssZZ).
     */
    private static final DateFormatter STRICT_DATE_TIME_NO_MILLIS = new JavaDateFormatter(
        "strict_date_time_no_millis",
        new DateTimeFormatterBuilder().append(STRICT_DATE_TIME_NO_MILLIS_FORMATTER)
            .appendOffset("+HH:MM", "Z")
            .toFormatter(Locale.ROOT)
            .withResolverStyle(ResolverStyle.STRICT),
        new DateTimeFormatterBuilder().append(STRICT_DATE_TIME_NO_MILLIS_FORMATTER)
            .appendZoneOrOffsetId()
            .toFormatter(Locale.ROOT)
            .withResolverStyle(ResolverStyle.STRICT),
        new DateTimeFormatterBuilder().append(STRICT_DATE_TIME_NO_MILLIS_FORMATTER)
            .append(TIME_ZONE_FORMATTER_NO_COLON)
            .toFormatter(Locale.ROOT)
            .withResolverStyle(ResolverStyle.STRICT)
    );

    // NOTE: this is not a strict formatter to retain the joda time based behaviour, even though it's named like this
    private static final DateTimeFormatter STRICT_HOUR_MINUTE_SECOND_MILLIS_FORMATTER = new DateTimeFormatterBuilder().append(
        STRICT_HOUR_MINUTE_SECOND_FORMATTER
    ).appendFraction(NANO_OF_SECOND, 1, 9, true).toFormatter(Locale.ROOT).withResolverStyle(ResolverStyle.STRICT);

    private static final DateTimeFormatter STRICT_HOUR_MINUTE_SECOND_MILLIS_PRINTER = new DateTimeFormatterBuilder().append(
        STRICT_HOUR_MINUTE_SECOND_FORMATTER
    ).appendFraction(NANO_OF_SECOND, 3, 3, true).toFormatter(Locale.ROOT).withResolverStyle(ResolverStyle.STRICT);

    /*
     * Returns a formatter for a two digit hour of day, two digit minute of
     * hour, two digit second of minute, and three digit fraction of
     * second (HH:mm:ss.SSS).
     *
     * NOTE: this is not a strict formatter to retain the joda time based behaviour,
     *       even though it's named like this
     */
    private static final DateFormatter STRICT_HOUR_MINUTE_SECOND_MILLIS = new JavaDateFormatter(
        "strict_hour_minute_second_millis",
        STRICT_HOUR_MINUTE_SECOND_MILLIS_PRINTER,
        STRICT_HOUR_MINUTE_SECOND_MILLIS_FORMATTER
    );

    private static final DateFormatter STRICT_HOUR_MINUTE_SECOND_FRACTION = new JavaDateFormatter(
        "strict_hour_minute_second_fraction",
        STRICT_HOUR_MINUTE_SECOND_MILLIS_PRINTER,
        STRICT_HOUR_MINUTE_SECOND_MILLIS_FORMATTER
    );

    /*
     * Returns a formatter that combines a full date, two digit hour of day,
     * two digit minute of hour, two digit second of minute, and three digit
     * fraction of second (uuuu-MM-dd'T'HH:mm:ss.SSS).
     */
    private static final DateFormatter STRICT_DATE_HOUR_MINUTE_SECOND_FRACTION = new JavaDateFormatter(
        "strict_date_hour_minute_second_fraction",
        new DateTimeFormatterBuilder().append(STRICT_YEAR_MONTH_DAY_FORMATTER)
            .appendLiteral("T")
            .append(STRICT_HOUR_MINUTE_SECOND_MILLIS_PRINTER)
            .toFormatter(Locale.ROOT)
            .withResolverStyle(ResolverStyle.STRICT),
        new DateTimeFormatterBuilder().append(STRICT_YEAR_MONTH_DAY_FORMATTER)
            .appendLiteral("T")
            .append(STRICT_HOUR_MINUTE_SECOND_FORMATTER)
            // this one here is lenient as well to retain joda time based bwc compatibility
            .appendFraction(NANO_OF_SECOND, 1, 9, true)
            .toFormatter(Locale.ROOT)
            .withResolverStyle(ResolverStyle.STRICT)
    );

    private static final DateFormatter STRICT_DATE_HOUR_MINUTE_SECOND_MILLIS = new JavaDateFormatter(
        "strict_date_hour_minute_second_millis",
        new DateTimeFormatterBuilder().append(STRICT_YEAR_MONTH_DAY_FORMATTER)
            .appendLiteral("T")
            .append(STRICT_HOUR_MINUTE_SECOND_MILLIS_PRINTER)
            .toFormatter(Locale.ROOT)
            .withResolverStyle(ResolverStyle.STRICT),
        new DateTimeFormatterBuilder().append(STRICT_YEAR_MONTH_DAY_FORMATTER)
            .appendLiteral("T")
            .append(STRICT_HOUR_MINUTE_SECOND_FORMATTER)
            // this one here is lenient as well to retain joda time based bwc compatibility
            .appendFraction(NANO_OF_SECOND, 1, 9, true)
            .toFormatter(Locale.ROOT)
            .withResolverStyle(ResolverStyle.STRICT)
    );

    /*
     * Returns a formatter for a two digit hour of day. (HH)
     */
    private static final DateFormatter STRICT_HOUR = new JavaDateFormatter("strict_hour", DateTimeFormatter.ofPattern("HH", Locale.ROOT));

    /*
     * Returns a formatter for a two digit hour of day and two digit minute of
     * hour. (HH:mm)
     */
    private static final DateFormatter STRICT_HOUR_MINUTE = new JavaDateFormatter(
        "strict_hour_minute",
        DateTimeFormatter.ofPattern("HH:mm", Locale.ROOT)
    );

    private static final DateTimeFormatter STRICT_ORDINAL_DATE_TIME_PRINTER = new DateTimeFormatterBuilder().appendValue(
        ChronoField.YEAR,
        4,
        4,
        SignStyle.EXCEEDS_PAD
    )
        .appendLiteral('-')
        .appendValue(DAY_OF_YEAR, 3, 3, SignStyle.NOT_NEGATIVE)
        .appendLiteral('T')
        .appendPattern("HH:mm")
        .optionalStart()
        .appendLiteral(':')
        .appendValue(SECOND_OF_MINUTE, 2, 2, SignStyle.NOT_NEGATIVE)
        .appendFraction(NANO_OF_SECOND, 3, 9, true)
        .optionalEnd()
        .toFormatter(Locale.ROOT)
        .withResolverStyle(ResolverStyle.STRICT);

    private static final DateTimeFormatter STRICT_ORDINAL_DATE_TIME_FORMATTER_BASE = new DateTimeFormatterBuilder().appendValue(
        ChronoField.YEAR,
        4,
        4,
        SignStyle.EXCEEDS_PAD
    )
        .appendLiteral('-')
        .appendValue(DAY_OF_YEAR, 3, 3, SignStyle.NOT_NEGATIVE)
        .appendLiteral('T')
        .appendPattern("HH:mm")
        .optionalStart()
        .appendLiteral(':')
        .appendValue(SECOND_OF_MINUTE, 2, 2, SignStyle.NOT_NEGATIVE)
        .appendFraction(NANO_OF_SECOND, 1, 9, true)
        .optionalEnd()
        .toFormatter(Locale.ROOT)
        .withResolverStyle(ResolverStyle.STRICT);

    /*
     * Returns a formatter for a full ordinal date and time, using a four
     * digit year and three digit dayOfYear (uuuu-DDD'T'HH:mm:ss.SSSZZ).
     */
    private static final DateFormatter STRICT_ORDINAL_DATE_TIME = new JavaDateFormatter(
        "strict_ordinal_date_time",
        new DateTimeFormatterBuilder().append(STRICT_ORDINAL_DATE_TIME_PRINTER)
            .appendOffset("+HH:MM", "Z")
            .toFormatter(Locale.ROOT)
            .withResolverStyle(ResolverStyle.STRICT),
        new DateTimeFormatterBuilder().append(STRICT_ORDINAL_DATE_TIME_FORMATTER_BASE)
            .appendZoneOrOffsetId()
            .toFormatter(Locale.ROOT)
            .withResolverStyle(ResolverStyle.STRICT),
        new DateTimeFormatterBuilder().append(STRICT_ORDINAL_DATE_TIME_FORMATTER_BASE)
            .append(TIME_ZONE_FORMATTER_NO_COLON)
            .toFormatter(Locale.ROOT)
            .withResolverStyle(ResolverStyle.STRICT)
    );

    // Note: milliseconds parsing is not strict, others are
    private static final DateTimeFormatter STRICT_TIME_FORMATTER_BASE = new DateTimeFormatterBuilder().appendValue(
        HOUR_OF_DAY,
        2,
        2,
        SignStyle.NOT_NEGATIVE
    )
        .appendLiteral(':')
        .appendValue(MINUTE_OF_HOUR, 2, 2, SignStyle.NOT_NEGATIVE)
        .appendLiteral(':')
        .appendValue(SECOND_OF_MINUTE, 2, 2, SignStyle.NOT_NEGATIVE)
        .appendFraction(NANO_OF_SECOND, 1, 9, true)
        .toFormatter(Locale.ROOT)
        .withResolverStyle(ResolverStyle.STRICT);

    private static final DateTimeFormatter STRICT_TIME_PRINTER = new DateTimeFormatterBuilder().appendValue(
        HOUR_OF_DAY,
        2,
        2,
        SignStyle.NOT_NEGATIVE
    )
        .appendLiteral(':')
        .appendValue(MINUTE_OF_HOUR, 2, 2, SignStyle.NOT_NEGATIVE)
        .appendLiteral(':')
        .appendValue(SECOND_OF_MINUTE, 2, 2, SignStyle.NOT_NEGATIVE)
        .appendFraction(NANO_OF_SECOND, 3, 3, true)
        .toFormatter(Locale.ROOT)
        .withResolverStyle(ResolverStyle.STRICT);

    /*
     * Returns a formatter for a two digit hour of day, two digit minute of
     * hour, two digit second of minute, three digit fraction of second, and
     * time zone offset (HH:mm:ss.SSSZZ).
     */
    private static final DateFormatter STRICT_TIME = new JavaDateFormatter(
        "strict_time",
        new DateTimeFormatterBuilder().append(STRICT_TIME_PRINTER)
            .appendOffset("+HH:MM", "Z")
            .toFormatter(Locale.ROOT)
            .withResolverStyle(ResolverStyle.STRICT),
        new DateTimeFormatterBuilder().append(STRICT_TIME_FORMATTER_BASE)
            .appendZoneOrOffsetId()
            .toFormatter(Locale.ROOT)
            .withResolverStyle(ResolverStyle.STRICT),
        new DateTimeFormatterBuilder().append(STRICT_TIME_FORMATTER_BASE)
            .append(TIME_ZONE_FORMATTER_NO_COLON)
            .toFormatter(Locale.ROOT)
            .withResolverStyle(ResolverStyle.STRICT)
    );

    /*
     * Returns a formatter for a two digit hour of day, two digit minute of
     * hour, two digit second of minute, three digit fraction of second, and
     * time zone offset prefixed by 'T' ('T'HH:mm:ss.SSSZZ).
     */
    private static final DateFormatter STRICT_T_TIME = new JavaDateFormatter(
        "strict_t_time",
        new DateTimeFormatterBuilder().appendLiteral('T')
            .append(STRICT_TIME_PRINTER)
            .appendOffset("+HH:MM", "Z")
            .toFormatter(Locale.ROOT)
            .withResolverStyle(ResolverStyle.STRICT),
        new DateTimeFormatterBuilder().appendLiteral('T')
            .append(STRICT_TIME_FORMATTER_BASE)
            .appendZoneOrOffsetId()
            .toFormatter(Locale.ROOT)
            .withResolverStyle(ResolverStyle.STRICT),
        new DateTimeFormatterBuilder().appendLiteral('T')
            .append(STRICT_TIME_FORMATTER_BASE)
            .append(TIME_ZONE_FORMATTER_NO_COLON)
            .toFormatter(Locale.ROOT)
            .withResolverStyle(ResolverStyle.STRICT)
    );

    private static final DateTimeFormatter STRICT_TIME_NO_MILLIS_BASE = new DateTimeFormatterBuilder().appendValue(
        HOUR_OF_DAY,
        2,
        2,
        SignStyle.NOT_NEGATIVE
    )
        .appendLiteral(':')
        .appendValue(MINUTE_OF_HOUR, 2, 2, SignStyle.NOT_NEGATIVE)
        .appendLiteral(':')
        .appendValue(SECOND_OF_MINUTE, 2, 2, SignStyle.NOT_NEGATIVE)
        .toFormatter(Locale.ROOT)
        .withResolverStyle(ResolverStyle.STRICT);

    /*
     * Returns a formatter for a two digit hour of day, two digit minute of
     * hour, two digit second of minute, and time zone offset (HH:mm:ssZZ).
     */
    private static final DateFormatter STRICT_TIME_NO_MILLIS = new JavaDateFormatter(
        "strict_time_no_millis",
        new DateTimeFormatterBuilder().append(STRICT_TIME_NO_MILLIS_BASE)
            .appendOffset("+HH:MM", "Z")
            .toFormatter(Locale.ROOT)
            .withResolverStyle(ResolverStyle.STRICT),
        new DateTimeFormatterBuilder().append(STRICT_TIME_NO_MILLIS_BASE)
            .appendZoneOrOffsetId()
            .toFormatter(Locale.ROOT)
            .withResolverStyle(ResolverStyle.STRICT),
        new DateTimeFormatterBuilder().append(STRICT_TIME_NO_MILLIS_BASE)
            .append(TIME_ZONE_FORMATTER_NO_COLON)
            .toFormatter(Locale.ROOT)
            .withResolverStyle(ResolverStyle.STRICT)
    );

    /*
     * Returns a formatter for a two digit hour of day, two digit minute of
     * hour, two digit second of minute, and time zone offset prefixed
     * by 'T' ('T'HH:mm:ssZZ).
     */
    private static final DateFormatter STRICT_T_TIME_NO_MILLIS = new JavaDateFormatter(
        "strict_t_time_no_millis",
        new DateTimeFormatterBuilder().appendLiteral("T")
            .append(STRICT_TIME_NO_MILLIS_BASE)
            .appendOffset("+HH:MM", "Z")
            .toFormatter(Locale.ROOT)
            .withResolverStyle(ResolverStyle.STRICT),
        new DateTimeFormatterBuilder().appendLiteral("T")
            .append(STRICT_TIME_NO_MILLIS_BASE)
            .appendZoneOrOffsetId()
            .toFormatter(Locale.ROOT)
            .withResolverStyle(ResolverStyle.STRICT),
        new DateTimeFormatterBuilder().appendLiteral("T")
            .append(STRICT_TIME_NO_MILLIS_BASE)
            .append(TIME_ZONE_FORMATTER_NO_COLON)
            .toFormatter(Locale.ROOT)
            .withResolverStyle(ResolverStyle.STRICT)
    );

    private static final DateTimeFormatter ISO_WEEK_DATE = new DateTimeFormatterBuilder().parseCaseInsensitive()
        .appendValue(IsoFields.WEEK_BASED_YEAR, 4, 10, SignStyle.EXCEEDS_PAD)
        .appendLiteral("-W")
        .appendValue(IsoFields.WEEK_OF_WEEK_BASED_YEAR, 2)
        .appendLiteral('-')
        .appendValue(DAY_OF_WEEK, 1)
        .toFormatter(Locale.ROOT)
        .withResolverStyle(ResolverStyle.STRICT);

    private static final DateTimeFormatter ISO_WEEK_DATE_T = new DateTimeFormatterBuilder().append(ISO_WEEK_DATE)
        .appendLiteral('T')
        .toFormatter(Locale.ROOT)
        .withResolverStyle(ResolverStyle.STRICT);

    /*
     * Returns a formatter for a full date as four digit weekyear, two digit
     * week of weekyear, and one digit day of week (xxxx-'W'ww-e).
     */
    private static final DateFormatter STRICT_WEEK_DATE = new JavaDateFormatter("strict_week_date", ISO_WEEK_DATE);

    /*
     * Returns a formatter that combines a full weekyear date and time without millis,
     * separated by a 'T' (xxxx-'W'ww-e'T'HH:mm:ssZZ).
     */
    private static final DateFormatter STRICT_WEEK_DATE_TIME_NO_MILLIS = new JavaDateFormatter(
        "strict_week_date_time_no_millis",
        new DateTimeFormatterBuilder().append(ISO_WEEK_DATE_T)
            .append(STRICT_TIME_NO_MILLIS_BASE)
            .appendOffset("+HH:MM", "Z")
            .toFormatter(Locale.ROOT)
            .withResolverStyle(ResolverStyle.STRICT),
        new DateTimeFormatterBuilder().append(ISO_WEEK_DATE_T)
            .append(STRICT_TIME_NO_MILLIS_BASE)
            .appendZoneOrOffsetId()
            .toFormatter(Locale.ROOT)
            .withResolverStyle(ResolverStyle.STRICT),
        new DateTimeFormatterBuilder().append(ISO_WEEK_DATE_T)
            .append(STRICT_TIME_NO_MILLIS_BASE)
            .append(TIME_ZONE_FORMATTER_NO_COLON)
            .toFormatter(Locale.ROOT)
            .withResolverStyle(ResolverStyle.STRICT)
    );

    /*
     * Returns a formatter that combines a full weekyear date and time,
     * separated by a 'T' (xxxx-'W'ww-e'T'HH:mm:ss.SSSZZ).
     */
    private static final DateFormatter STRICT_WEEK_DATE_TIME = new JavaDateFormatter(
        "strict_week_date_time",
        new DateTimeFormatterBuilder().append(ISO_WEEK_DATE_T)
            .append(STRICT_TIME_PRINTER)
            .appendOffset("+HH:MM", "Z")
            .toFormatter(Locale.ROOT)
            .withResolverStyle(ResolverStyle.STRICT),
        new DateTimeFormatterBuilder().append(ISO_WEEK_DATE_T)
            .append(STRICT_TIME_FORMATTER_BASE)
            .appendZoneOrOffsetId()
            .toFormatter(Locale.ROOT)
            .withResolverStyle(ResolverStyle.STRICT),
        new DateTimeFormatterBuilder().append(ISO_WEEK_DATE_T)
            .append(STRICT_TIME_FORMATTER_BASE)
            .append(TIME_ZONE_FORMATTER_NO_COLON)
            .toFormatter(Locale.ROOT)
            .withResolverStyle(ResolverStyle.STRICT)
    );

    /*
     * Returns a formatter for a four digit weekyear
     */
    private static final DateFormatter STRICT_WEEKYEAR = new JavaDateFormatter(
        "strict_weekyear",
        new DateTimeFormatterBuilder().appendValue(WEEK_FIELDS_ROOT.weekBasedYear(), 4, 10, SignStyle.EXCEEDS_PAD)
            .toFormatter(Locale.ROOT)
            .withResolverStyle(ResolverStyle.STRICT)
    );

    private static final DateTimeFormatter STRICT_WEEKYEAR_WEEK_FORMATTER = new DateTimeFormatterBuilder().appendValue(
        WEEK_FIELDS_ROOT.weekBasedYear(),
        4,
        10,
        SignStyle.EXCEEDS_PAD
    )
        .appendLiteral("-W")
        .appendValue(WEEK_FIELDS_ROOT.weekOfWeekBasedYear(), 2, 2, SignStyle.NOT_NEGATIVE)
        .toFormatter(Locale.ROOT)
        .withResolverStyle(ResolverStyle.STRICT);

    /*
     * Returns a formatter for a four digit weekyear and two digit week of
     * weekyear. (xxxx-'W'ww)
     */
    private static final DateFormatter STRICT_WEEKYEAR_WEEK = new JavaDateFormatter("strict_weekyear_week", STRICT_WEEKYEAR_WEEK_FORMATTER);

    /*
     * Returns a formatter for a four digit weekyear, two digit week of
     * weekyear, and one digit day of week. (xxxx-'W'ww-e)
     */
    private static final DateFormatter STRICT_WEEKYEAR_WEEK_DAY = new JavaDateFormatter(
        "strict_weekyear_week_day",
        new DateTimeFormatterBuilder().append(STRICT_WEEKYEAR_WEEK_FORMATTER)
            .appendLiteral("-")
            .appendValue(WEEK_FIELDS_ROOT.dayOfWeek())
            .toFormatter(Locale.ROOT)
            .withResolverStyle(ResolverStyle.STRICT)
    );

    /*
     * Returns a formatter that combines a full date, two digit hour of day,
     * two digit minute of hour, and two digit second of
     * minute. (uuuu-MM-dd'T'HH:mm:ss)
     */
    private static final DateFormatter STRICT_DATE_HOUR_MINUTE_SECOND = new JavaDateFormatter(
        "strict_date_hour_minute_second",
        DateTimeFormatter.ofPattern("uuuu-MM-dd'T'HH:mm:ss", Locale.ROOT)
    );

    /*
     * A basic formatter for a full date as four digit year, two digit
     * month of year, and two digit day of month (uuuuMMdd).
     */
    private static final DateFormatter BASIC_DATE = new JavaDateFormatter(
        "basic_date",
        new DateTimeFormatterBuilder().appendValue(ChronoField.YEAR, 4, 10, SignStyle.NORMAL)
            .appendValue(MONTH_OF_YEAR, 2, 2, SignStyle.NOT_NEGATIVE)
            .appendValue(DAY_OF_MONTH, 2, 2, SignStyle.NOT_NEGATIVE)
            .toFormatter(Locale.ROOT)
            .withResolverStyle(ResolverStyle.STRICT)
            .withZone(ZoneOffset.UTC),
        new DateTimeFormatterBuilder().appendValue(ChronoField.YEAR, 1, 4, SignStyle.NORMAL)
            .appendValue(MONTH_OF_YEAR, 1, 2, SignStyle.NOT_NEGATIVE)
            .appendValue(DAY_OF_MONTH, 1, 2, SignStyle.NOT_NEGATIVE)
            .toFormatter(Locale.ROOT)
            .withResolverStyle(ResolverStyle.STRICT)
            .withZone(ZoneOffset.UTC)
    );

    private static final DateTimeFormatter STRICT_ORDINAL_DATE_FORMATTER = new DateTimeFormatterBuilder().parseCaseInsensitive()
        .appendValue(ChronoField.YEAR, 4, 4, SignStyle.EXCEEDS_PAD)
        .appendLiteral('-')
        .appendValue(DAY_OF_YEAR, 3)
        .optionalStart()
        .toFormatter(Locale.ROOT)
        .withResolverStyle(ResolverStyle.STRICT);

    /*
     * Returns a formatter for a full ordinal date, using a four
     * digit year and three digit dayOfYear (uuuu-DDD).
     */
    private static final DateFormatter STRICT_ORDINAL_DATE = new JavaDateFormatter("strict_ordinal_date", STRICT_ORDINAL_DATE_FORMATTER);

    /////////////////////////////////////////
    //
    // end strict formatters
    //
    /////////////////////////////////////////

    /////////////////////////////////////////
    //
    // start lenient formatters
    //
    /////////////////////////////////////////

    private static final DateTimeFormatter DATE_FORMATTER = new DateTimeFormatterBuilder().appendValue(
        ChronoField.YEAR,
        1,
        9,
        SignStyle.NORMAL
    )
        .optionalStart()
        .appendLiteral('-')
        .appendValue(MONTH_OF_YEAR, 1, 2, SignStyle.NOT_NEGATIVE)
        .optionalStart()
        .appendLiteral('-')
        .appendValue(DAY_OF_MONTH, 1, 2, SignStyle.NOT_NEGATIVE)
        .optionalEnd()
        .optionalEnd()
        .toFormatter(Locale.ROOT)
        .withResolverStyle(ResolverStyle.STRICT);

    private static final DateTimeFormatter HOUR_MINUTE_FORMATTER = new DateTimeFormatterBuilder().appendValue(
        HOUR_OF_DAY,
        1,
        2,
        SignStyle.NOT_NEGATIVE
    )
        .appendLiteral(':')
        .appendValue(MINUTE_OF_HOUR, 1, 2, SignStyle.NOT_NEGATIVE)
        .toFormatter(Locale.ROOT)
        .withResolverStyle(ResolverStyle.STRICT);

    /*
     * a date formatter with optional time, being very lenient, format is
     * uuuu-MM-dd'T'HH:mm:ss.SSSZ
     */
    private static final DateFormatter DATE_OPTIONAL_TIME = new JavaDateFormatter(
        "date_optional_time",
        STRICT_DATE_OPTIONAL_TIME_PRINTER,
        new DateTimeFormatterBuilder().append(DATE_FORMATTER)
            .optionalStart()
            .appendLiteral('T')
            .optionalStart()
            .appendValue(HOUR_OF_DAY, 1, 2, SignStyle.NOT_NEGATIVE)
            .optionalStart()
            .appendLiteral(':')
            .appendValue(MINUTE_OF_HOUR, 1, 2, SignStyle.NOT_NEGATIVE)
            .optionalStart()
            .appendLiteral(':')
            .appendValue(SECOND_OF_MINUTE, 1, 2, SignStyle.NOT_NEGATIVE)
            .optionalEnd()
            .optionalStart()
            .appendFraction(NANO_OF_SECOND, 1, 9, true)
            .optionalEnd()
            .optionalStart()
            .appendLiteral(',')
            .appendFraction(NANO_OF_SECOND, 1, 9, false)
            .optionalEnd()
            .optionalStart()
            .appendZoneOrOffsetId()
            .optionalEnd()
            .optionalStart()
            .appendOffset("+HHmm", "Z")
            .optionalEnd()
            .optionalEnd()
            .optionalEnd()
            .optionalEnd()
            .toFormatter(Locale.ROOT)
            .withResolverStyle(ResolverStyle.STRICT)
    );

    private static final DateTimeFormatter HOUR_MINUTE_SECOND_FORMATTER = new DateTimeFormatterBuilder().append(HOUR_MINUTE_FORMATTER)
        .appendLiteral(":")
        .appendValue(SECOND_OF_MINUTE, 1, 2, SignStyle.NOT_NEGATIVE)
        .toFormatter(Locale.ROOT)
        .withResolverStyle(ResolverStyle.STRICT);

    private static final DateTimeFormatter HOUR_MINUTE_SECOND_MILLIS_FORMATTER = new DateTimeFormatterBuilder().appendValue(
        HOUR_OF_DAY,
        1,
        2,
        SignStyle.NOT_NEGATIVE
    )
        .appendLiteral(':')
        .appendValue(MINUTE_OF_HOUR, 1, 2, SignStyle.NOT_NEGATIVE)
        .appendLiteral(':')
        .appendValue(SECOND_OF_MINUTE, 1, 2, SignStyle.NOT_NEGATIVE)
        .appendFraction(NANO_OF_SECOND, 1, 3, true)
        .toFormatter(Locale.ROOT)
        .withResolverStyle(ResolverStyle.STRICT);

    private static final DateTimeFormatter HOUR_MINUTE_SECOND_FRACTION_FORMATTER = new DateTimeFormatterBuilder().appendValue(
        HOUR_OF_DAY,
        1,
        2,
        SignStyle.NOT_NEGATIVE
    )
        .appendLiteral(':')
        .appendValue(MINUTE_OF_HOUR, 1, 2, SignStyle.NOT_NEGATIVE)
        .appendLiteral(':')
        .appendValue(SECOND_OF_MINUTE, 1, 2, SignStyle.NOT_NEGATIVE)
        .appendFraction(NANO_OF_SECOND, 1, 9, true)
        .toFormatter(Locale.ROOT)
        .withResolverStyle(ResolverStyle.STRICT);

    private static final DateTimeFormatter ORDINAL_DATE_FORMATTER = new DateTimeFormatterBuilder().appendValue(
        ChronoField.YEAR,
        4,
        10,
        SignStyle.EXCEEDS_PAD
    )
        .appendLiteral('-')
        .appendValue(DAY_OF_YEAR, 1, 3, SignStyle.NOT_NEGATIVE)
        .toFormatter(Locale.ROOT)
        .withResolverStyle(ResolverStyle.STRICT);

    private static final DateTimeFormatter ORDINAL_DATE_PRINTER = new DateTimeFormatterBuilder().appendValue(
        ChronoField.YEAR,
        4,
        10,
        SignStyle.EXCEEDS_PAD
    )
        .appendLiteral('-')
        .appendValue(DAY_OF_YEAR, 3, 3, SignStyle.NOT_NEGATIVE)
        .toFormatter(Locale.ROOT)
        .withResolverStyle(ResolverStyle.STRICT);

    /*
     * Returns a formatter for a full ordinal date, using a four
     * digit year and three digit dayOfYear (uuuu-DDD).
     */
    private static final DateFormatter ORDINAL_DATE = new JavaDateFormatter("ordinal_date", ORDINAL_DATE_PRINTER, ORDINAL_DATE_FORMATTER);

    private static final DateTimeFormatter TIME_NO_MILLIS_FORMATTER = new DateTimeFormatterBuilder().appendValue(
        HOUR_OF_DAY,
        1,
        2,
        SignStyle.NOT_NEGATIVE
    )
        .appendLiteral(':')
        .appendValue(MINUTE_OF_HOUR, 1, 2, SignStyle.NOT_NEGATIVE)
        .appendLiteral(':')
        .appendValue(SECOND_OF_MINUTE, 1, 2, SignStyle.NOT_NEGATIVE)
        .toFormatter(Locale.ROOT)
        .withResolverStyle(ResolverStyle.STRICT);

    private static final DateTimeFormatter T_TIME_NO_MILLIS_FORMATTER = new DateTimeFormatterBuilder().appendLiteral("T")
        .append(TIME_NO_MILLIS_FORMATTER)
        .toFormatter(Locale.ROOT)
        .withResolverStyle(ResolverStyle.STRICT);

    private static final DateTimeFormatter TIME_PREFIX = new DateTimeFormatterBuilder().append(TIME_NO_MILLIS_FORMATTER)
        .appendFraction(NANO_OF_SECOND, 1, 9, true)
        .toFormatter(Locale.ROOT)
        .withResolverStyle(ResolverStyle.STRICT);

    private static final DateTimeFormatter WEEK_DATE_FORMATTER = new DateTimeFormatterBuilder().appendValue(
        IsoFields.WEEK_BASED_YEAR,
        4,
        10,
        SignStyle.EXCEEDS_PAD
    )
        .appendLiteral("-W")
        .appendValue(IsoFields.WEEK_OF_WEEK_BASED_YEAR, 1, 2, SignStyle.NOT_NEGATIVE)
        .appendLiteral('-')
        .appendValue(DAY_OF_WEEK, 1)
        .toFormatter(Locale.ROOT)
        .withResolverStyle(ResolverStyle.STRICT);

    /*
     * Returns a formatter for a four digit weekyear. (YYYY)
     */
    private static final DateFormatter WEEKYEAR = new JavaDateFormatter(
        "weekyear",
        new DateTimeFormatterBuilder().appendValue(WEEK_FIELDS_ROOT.weekBasedYear())
            .toFormatter(Locale.ROOT)
            .withResolverStyle(ResolverStyle.STRICT)
    );
    /*
     * Returns a formatter for a four digit year. (uuuu)
     */
    private static final DateFormatter YEAR = new JavaDateFormatter(
        "year",
        new DateTimeFormatterBuilder().appendValue(ChronoField.YEAR).toFormatter(Locale.ROOT).withResolverStyle(ResolverStyle.STRICT)
    );

    /*
     * Returns a formatter that combines a full date and two digit hour of
     * day. (uuuu-MM-dd'T'HH)
     */
    private static final DateFormatter DATE_HOUR = new JavaDateFormatter(
        "date_hour",
        DateTimeFormatter.ofPattern("uuuu-MM-dd'T'HH", Locale.ROOT),
        new DateTimeFormatterBuilder().append(DATE_FORMATTER)
            .appendLiteral("T")
            .appendValue(HOUR_OF_DAY, 1, 2, SignStyle.NOT_NEGATIVE)
            .toFormatter(Locale.ROOT)
            .withResolverStyle(ResolverStyle.STRICT)
    );

    /*
     * Returns a formatter that combines a full date, two digit hour of day,
     * two digit minute of hour, two digit second of minute, and three digit
     * fraction of second (uuuu-MM-dd'T'HH:mm:ss.SSS).
     */
    private static final DateFormatter DATE_HOUR_MINUTE_SECOND_MILLIS = new JavaDateFormatter(
        "date_hour_minute_second_millis",
        new DateTimeFormatterBuilder().append(STRICT_YEAR_MONTH_DAY_FORMATTER)
            .appendLiteral("T")
            .append(STRICT_HOUR_MINUTE_SECOND_MILLIS_PRINTER)
            .toFormatter(Locale.ROOT)
            .withResolverStyle(ResolverStyle.STRICT),
        new DateTimeFormatterBuilder().append(DATE_FORMATTER)
            .appendLiteral("T")
            .append(HOUR_MINUTE_SECOND_MILLIS_FORMATTER)
            .toFormatter(Locale.ROOT)
            .withResolverStyle(ResolverStyle.STRICT)
    );

    private static final DateFormatter DATE_HOUR_MINUTE_SECOND_FRACTION = new JavaDateFormatter(
        "date_hour_minute_second_fraction",
        new DateTimeFormatterBuilder().append(STRICT_YEAR_MONTH_DAY_FORMATTER)
            .appendLiteral("T")
            .append(STRICT_HOUR_MINUTE_SECOND_MILLIS_PRINTER)
            .toFormatter(Locale.ROOT)
            .withResolverStyle(ResolverStyle.STRICT),
        new DateTimeFormatterBuilder().append(DATE_FORMATTER)
            .appendLiteral("T")
            .append(HOUR_MINUTE_SECOND_FRACTION_FORMATTER)
            .toFormatter(Locale.ROOT)
            .withResolverStyle(ResolverStyle.STRICT)
    );

    /*
     * Returns a formatter that combines a full date, two digit hour of day,
     * and two digit minute of hour. (uuuu-MM-dd'T'HH:mm)
     */
    private static final DateFormatter DATE_HOUR_MINUTE = new JavaDateFormatter(
        "date_hour_minute",
        DateTimeFormatter.ofPattern("uuuu-MM-dd'T'HH:mm", Locale.ROOT),
        new DateTimeFormatterBuilder().append(DATE_FORMATTER)
            .appendLiteral("T")
            .append(HOUR_MINUTE_FORMATTER)
            .toFormatter(Locale.ROOT)
            .withResolverStyle(ResolverStyle.STRICT)
    );

    /*
     * Returns a formatter that combines a full date, two digit hour of day,
     * two digit minute of hour, and two digit second of
     * minute. (uuuu-MM-dd'T'HH:mm:ss)
     */
    private static final DateFormatter DATE_HOUR_MINUTE_SECOND = new JavaDateFormatter(
        "date_hour_minute_second",
        DateTimeFormatter.ofPattern("uuuu-MM-dd'T'HH:mm:ss", Locale.ROOT),
        new DateTimeFormatterBuilder().append(DATE_FORMATTER)
            .appendLiteral("T")
            .append(HOUR_MINUTE_SECOND_FORMATTER)
            .toFormatter(Locale.ROOT)
            .withResolverStyle(ResolverStyle.STRICT)
    );

    private static final DateTimeFormatter DATE_TIME_FORMATTER = new DateTimeFormatterBuilder().append(DATE_FORMATTER)
        .appendLiteral('T')
        .append(HOUR_MINUTE_FORMATTER)
        .optionalStart()
        .appendLiteral(':')
        .appendValue(SECOND_OF_MINUTE, 1, 2, SignStyle.NOT_NEGATIVE)
        .appendFraction(NANO_OF_SECOND, 1, 9, true)
        .optionalEnd()
        .toFormatter(Locale.ROOT)
        .withResolverStyle(ResolverStyle.STRICT);

    /*
     * Returns a formatter that combines a full date and time, separated by a 'T'
     * (uuuu-MM-dd'T'HH:mm:ss.SSSZZ).
     */
    private static final DateFormatter DATE_TIME = new JavaDateFormatter(
        "date_time",
        STRICT_DATE_OPTIONAL_TIME_PRINTER,
        new DateTimeFormatterBuilder().append(DATE_TIME_FORMATTER)
            .appendOffset("+HH:MM", "Z")
            .toFormatter(Locale.ROOT)
            .withResolverStyle(ResolverStyle.STRICT),
        new DateTimeFormatterBuilder().append(DATE_TIME_FORMATTER)
            .append(TIME_ZONE_FORMATTER_NO_COLON)
            .toFormatter(Locale.ROOT)
            .withResolverStyle(ResolverStyle.STRICT)
    );

    /*
     * Returns a basic formatter for a full date as four digit weekyear, two
     * digit week of weekyear, and one digit day of week (YYYY'W'wwe).
     */
    private static final DateFormatter BASIC_WEEK_DATE = new JavaDateFormatter(
        "basic_week_date",
        STRICT_BASIC_WEEK_DATE_PRINTER,
        BASIC_WEEK_DATE_FORMATTER
    );

    /*
     * Returns a formatter for a full date as four digit year, two digit month
     * of year, and two digit day of month (uuuu-MM-dd).
     */
    private static final DateFormatter DATE = new JavaDateFormatter(
        "date",
        DateTimeFormatter.ISO_LOCAL_DATE.withResolverStyle(ResolverStyle.STRICT),
        DATE_FORMATTER
    );

    // only the formatter, nothing optional here
    private static final DateTimeFormatter DATE_TIME_NO_MILLIS_PRINTER = new DateTimeFormatterBuilder().append(
        DateTimeFormatter.ISO_LOCAL_DATE.withResolverStyle(ResolverStyle.LENIENT)
    )
        .appendLiteral('T')
        .appendPattern("HH:mm")
        .appendLiteral(':')
        .appendValue(SECOND_OF_MINUTE, 2, 2, SignStyle.NOT_NEGATIVE)
        .appendZoneId()
        .toFormatter(Locale.ROOT)
        .withResolverStyle(ResolverStyle.STRICT);

    private static final DateTimeFormatter DATE_TIME_PREFIX = new DateTimeFormatterBuilder().append(DATE_FORMATTER)
        .appendLiteral('T')
        .append(HOUR_MINUTE_FORMATTER)
        .optionalStart()
        .appendLiteral(':')
        .appendValue(SECOND_OF_MINUTE, 1, 2, SignStyle.NOT_NEGATIVE)
        .optionalEnd()
        .toFormatter(Locale.ROOT)
        .withResolverStyle(ResolverStyle.STRICT);

    /*
     * Returns a formatter that combines a full date and time without millis, but with a timezone that can be optional
     * separated by a 'T' (uuuu-MM-dd'T'HH:mm:ssZ).
     */
    private static final DateFormatter DATE_TIME_NO_MILLIS = new JavaDateFormatter(
        "date_time_no_millis",
        DATE_TIME_NO_MILLIS_PRINTER,
        new DateTimeFormatterBuilder().append(DATE_TIME_PREFIX)
            .appendOffset("+HH:MM", "Z")
            .toFormatter(Locale.ROOT)
            .withResolverStyle(ResolverStyle.STRICT),
        new DateTimeFormatterBuilder().append(DATE_TIME_PREFIX)
            .append(TIME_ZONE_FORMATTER_NO_COLON)
            .toFormatter(Locale.ROOT)
            .withResolverStyle(ResolverStyle.STRICT),
        new DateTimeFormatterBuilder().append(DATE_TIME_PREFIX)
            .optionalStart()
            .appendZoneOrOffsetId()
            .optionalEnd()
            .toFormatter(Locale.ROOT)
            .withResolverStyle(ResolverStyle.STRICT),
        new DateTimeFormatterBuilder().append(DATE_TIME_PREFIX)
            .optionalStart()
            .append(TIME_ZONE_FORMATTER_NO_COLON)
            .optionalEnd()
            .toFormatter(Locale.ROOT)
            .withResolverStyle(ResolverStyle.STRICT)
    );

    /*
     * Returns a formatter for a two digit hour of day, two digit minute of
     * hour, two digit second of minute, and three digit fraction of
     * second (HH:mm:ss.SSS).
     */
    private static final DateFormatter HOUR_MINUTE_SECOND_MILLIS = new JavaDateFormatter(
        "hour_minute_second_millis",
        STRICT_HOUR_MINUTE_SECOND_MILLIS_PRINTER,
        HOUR_MINUTE_SECOND_MILLIS_FORMATTER
    );

    private static final DateFormatter HOUR_MINUTE_SECOND_FRACTION = new JavaDateFormatter(
        "hour_minute_second_fraction",
        STRICT_HOUR_MINUTE_SECOND_MILLIS_PRINTER,
        HOUR_MINUTE_SECOND_FRACTION_FORMATTER
    );

    /*
     * Returns a formatter for a two digit hour of day and two digit minute of
     * hour. (HH:mm)
     */
    private static final DateFormatter HOUR_MINUTE = new JavaDateFormatter(
        "hour_minute",
        DateTimeFormatter.ofPattern("HH:mm", Locale.ROOT),
        HOUR_MINUTE_FORMATTER
    );

    /*
     * A strict formatter that formats or parses a hour, minute and second, such as '09:43:25'.
     */
    private static final DateFormatter HOUR_MINUTE_SECOND = new JavaDateFormatter(
        "hour_minute_second",
        STRICT_HOUR_MINUTE_SECOND_FORMATTER,
        new DateTimeFormatterBuilder().append(HOUR_MINUTE_FORMATTER)
            .appendLiteral(":")
            .appendValue(SECOND_OF_MINUTE, 1, 2, SignStyle.NOT_NEGATIVE)
            .toFormatter(Locale.ROOT)
            .withResolverStyle(ResolverStyle.STRICT)
    );

    /*
     * Returns a formatter for a two digit hour of day. (HH)
     */
    private static final DateFormatter HOUR = new JavaDateFormatter(
        "hour",
        DateTimeFormatter.ofPattern("HH", Locale.ROOT),
        new DateTimeFormatterBuilder().appendValue(HOUR_OF_DAY, 1, 2, SignStyle.NOT_NEGATIVE)
            .toFormatter(Locale.ROOT)
            .withResolverStyle(ResolverStyle.STRICT)
    );

    private static final DateTimeFormatter ORDINAL_DATE_TIME_FORMATTER_BASE = new DateTimeFormatterBuilder().append(ORDINAL_DATE_FORMATTER)
        .appendLiteral('T')
        .append(HOUR_MINUTE_FORMATTER)
        .optionalStart()
        .appendLiteral(':')
        .appendValue(SECOND_OF_MINUTE, 1, 2, SignStyle.NOT_NEGATIVE)
        .appendFraction(NANO_OF_SECOND, 1, 9, true)
        .optionalEnd()
        .toFormatter(Locale.ROOT)
        .withResolverStyle(ResolverStyle.STRICT);

    /*
     * Returns a formatter for a full ordinal date and time, using a four
     * digit year and three digit dayOfYear (uuuu-DDD'T'HH:mm:ss.SSSZZ).
     */
    private static final DateFormatter ORDINAL_DATE_TIME = new JavaDateFormatter(
        "ordinal_date_time",
        new DateTimeFormatterBuilder().append(STRICT_ORDINAL_DATE_TIME_PRINTER)
            .appendOffset("+HH:MM", "Z")
            .toFormatter(Locale.ROOT)
            .withResolverStyle(ResolverStyle.STRICT),
        new DateTimeFormatterBuilder().append(ORDINAL_DATE_TIME_FORMATTER_BASE)
            .appendZoneOrOffsetId()
            .toFormatter(Locale.ROOT)
            .withResolverStyle(ResolverStyle.STRICT),
        new DateTimeFormatterBuilder().append(ORDINAL_DATE_TIME_FORMATTER_BASE)
            .append(TIME_ZONE_FORMATTER_NO_COLON)
            .toFormatter(Locale.ROOT)
            .withResolverStyle(ResolverStyle.STRICT)
    );

    private static final DateTimeFormatter ORDINAL_DATE_TIME_NO_MILLIS_BASE = new DateTimeFormatterBuilder().append(ORDINAL_DATE_FORMATTER)
        .appendLiteral('T')
        .append(HOUR_MINUTE_SECOND_FORMATTER)
        .toFormatter(Locale.ROOT)
        .withResolverStyle(ResolverStyle.STRICT);

    /*
     * Returns a formatter for a full ordinal date and time without millis,
     * using a four digit year and three digit dayOfYear (uuuu-DDD'T'HH:mm:ssZZ).
     */
    private static final DateFormatter ORDINAL_DATE_TIME_NO_MILLIS = new JavaDateFormatter(
        "ordinal_date_time_no_millis",
        new DateTimeFormatterBuilder().append(STRICT_ORDINAL_DATE_TIME_NO_MILLIS_BASE)
            .appendOffset("+HH:MM", "Z")
            .toFormatter(Locale.ROOT)
            .withResolverStyle(ResolverStyle.STRICT),
        new DateTimeFormatterBuilder().append(ORDINAL_DATE_TIME_NO_MILLIS_BASE)
            .appendZoneOrOffsetId()
            .toFormatter(Locale.ROOT)
            .withResolverStyle(ResolverStyle.STRICT),
        new DateTimeFormatterBuilder().append(ORDINAL_DATE_TIME_NO_MILLIS_BASE)
            .append(TIME_ZONE_FORMATTER_NO_COLON)
            .toFormatter(Locale.ROOT)
            .withResolverStyle(ResolverStyle.STRICT)
    );

    /*
     * Returns a formatter that combines a full weekyear date and time,
     * separated by a 'T' (xxxx-'W'ww-e'T'HH:mm:ss.SSSZZ).
     */
    private static final DateFormatter WEEK_DATE_TIME = new JavaDateFormatter(
        "week_date_time",
        new DateTimeFormatterBuilder().append(ISO_WEEK_DATE_T)
            .append(STRICT_TIME_PRINTER)
            .appendOffset("+HH:MM", "Z")
            .toFormatter(Locale.ROOT)
            .withResolverStyle(ResolverStyle.STRICT),
        new DateTimeFormatterBuilder().append(WEEK_DATE_FORMATTER)
            .appendLiteral("T")
            .append(TIME_PREFIX)
            .appendZoneOrOffsetId()
            .toFormatter(Locale.ROOT)
            .withResolverStyle(ResolverStyle.STRICT),
        new DateTimeFormatterBuilder().append(WEEK_DATE_FORMATTER)
            .appendLiteral("T")
            .append(TIME_PREFIX)
            .append(TIME_ZONE_FORMATTER_NO_COLON)
            .toFormatter(Locale.ROOT)
            .withResolverStyle(ResolverStyle.STRICT)
    );

    /*
     * Returns a formatter that combines a full weekyear date and time,
     * separated by a 'T' (xxxx-'W'ww-e'T'HH:mm:ssZZ).
     */
    private static final DateFormatter WEEK_DATE_TIME_NO_MILLIS = new JavaDateFormatter(
        "week_date_time_no_millis",
        new DateTimeFormatterBuilder().append(ISO_WEEK_DATE_T)
            .append(STRICT_TIME_NO_MILLIS_BASE)
            .appendOffset("+HH:MM", "Z")
            .toFormatter(Locale.ROOT)
            .withResolverStyle(ResolverStyle.STRICT),
        new DateTimeFormatterBuilder().append(WEEK_DATE_FORMATTER)
            .append(T_TIME_NO_MILLIS_FORMATTER)
            .appendZoneOrOffsetId()
            .toFormatter(Locale.ROOT)
            .withResolverStyle(ResolverStyle.STRICT),
        new DateTimeFormatterBuilder().append(WEEK_DATE_FORMATTER)
            .append(T_TIME_NO_MILLIS_FORMATTER)
            .append(TIME_ZONE_FORMATTER_NO_COLON)
            .toFormatter(Locale.ROOT)
            .withResolverStyle(ResolverStyle.STRICT)
    );

    /*
     * Returns a basic formatter that combines a basic weekyear date and time,
     * separated by a 'T' (xxxx'W'wwe'T'HHmmss.SSSX).
     */
    private static final DateFormatter BASIC_WEEK_DATE_TIME = new JavaDateFormatter(
        "basic_week_date_time",
        new DateTimeFormatterBuilder().append(STRICT_BASIC_WEEK_DATE_PRINTER)
            .append(DateTimeFormatter.ofPattern("'T'HHmmss.SSSX", Locale.ROOT))
            .toFormatter(Locale.ROOT)
            .withResolverStyle(ResolverStyle.STRICT),
        new DateTimeFormatterBuilder().append(BASIC_WEEK_DATE_FORMATTER)
            .append(BASIC_T_TIME_FORMATTER)
            .appendZoneOrOffsetId()
            .toFormatter(Locale.ROOT)
            .withResolverStyle(ResolverStyle.STRICT),
        new DateTimeFormatterBuilder().append(BASIC_WEEK_DATE_FORMATTER)
            .append(BASIC_T_TIME_FORMATTER)
            .append(TIME_ZONE_FORMATTER_NO_COLON)
            .toFormatter(Locale.ROOT)
            .withResolverStyle(ResolverStyle.STRICT)
    );

    /*
     * Returns a basic formatter that combines a basic weekyear date and time,
     * separated by a 'T' (xxxx'W'wwe'T'HHmmssX).
     */
    private static final DateFormatter BASIC_WEEK_DATE_TIME_NO_MILLIS = new JavaDateFormatter(
        "basic_week_date_time_no_millis",
        new DateTimeFormatterBuilder().append(STRICT_BASIC_WEEK_DATE_PRINTER)
            .append(DateTimeFormatter.ofPattern("'T'HHmmssX", Locale.ROOT))
            .toFormatter(Locale.ROOT)
            .withResolverStyle(ResolverStyle.STRICT),
        new DateTimeFormatterBuilder().append(BASIC_WEEK_DATE_FORMATTER)
            .appendLiteral("T")
            .append(BASIC_TIME_NO_MILLIS_BASE)
            .appendZoneOrOffsetId()
            .toFormatter(Locale.ROOT)
            .withResolverStyle(ResolverStyle.STRICT),
        new DateTimeFormatterBuilder().append(BASIC_WEEK_DATE_FORMATTER)
            .appendLiteral("T")
            .append(BASIC_TIME_NO_MILLIS_BASE)
            .append(TIME_ZONE_FORMATTER_NO_COLON)
            .toFormatter(Locale.ROOT)
            .withResolverStyle(ResolverStyle.STRICT)
    );

    /*
     * Returns a formatter for a two digit hour of day, two digit minute of
     * hour, two digit second of minute, three digit fraction of second, and
     * time zone offset (HH:mm:ss.SSSZZ).
     */
    private static final DateFormatter TIME = new JavaDateFormatter(
        "time",
        new DateTimeFormatterBuilder().append(STRICT_TIME_PRINTER)
            .appendOffset("+HH:MM", "Z")
            .toFormatter(Locale.ROOT)
            .withResolverStyle(ResolverStyle.STRICT),
        new DateTimeFormatterBuilder().append(TIME_PREFIX)
            .appendZoneOrOffsetId()
            .toFormatter(Locale.ROOT)
            .withResolverStyle(ResolverStyle.STRICT),
        new DateTimeFormatterBuilder().append(TIME_PREFIX)
            .append(TIME_ZONE_FORMATTER_NO_COLON)
            .toFormatter(Locale.ROOT)
            .withResolverStyle(ResolverStyle.STRICT)
    );

    /*
     * Returns a formatter for a two digit hour of day, two digit minute of
     * hour, two digit second of minute, andtime zone offset (HH:mm:ssZZ).
     */
    private static final DateFormatter TIME_NO_MILLIS = new JavaDateFormatter(
        "time_no_millis",
        new DateTimeFormatterBuilder().append(STRICT_TIME_NO_MILLIS_BASE)
            .appendOffset("+HH:MM", "Z")
            .toFormatter(Locale.ROOT)
            .withResolverStyle(ResolverStyle.STRICT),
        new DateTimeFormatterBuilder().append(TIME_NO_MILLIS_FORMATTER)
            .appendZoneOrOffsetId()
            .toFormatter(Locale.ROOT)
            .withResolverStyle(ResolverStyle.STRICT),
        new DateTimeFormatterBuilder().append(TIME_NO_MILLIS_FORMATTER)
            .append(TIME_ZONE_FORMATTER_NO_COLON)
            .toFormatter(Locale.ROOT)
            .withResolverStyle(ResolverStyle.STRICT)
    );

    /*
     * Returns a formatter for a two digit hour of day, two digit minute of
     * hour, two digit second of minute, three digit fraction of second, and
     * time zone offset prefixed by 'T' ('T'HH:mm:ss.SSSZZ).
     */
    private static final DateFormatter T_TIME = new JavaDateFormatter(
        "t_time",
        new DateTimeFormatterBuilder().appendLiteral('T')
            .append(STRICT_TIME_PRINTER)
            .appendOffset("+HH:MM", "Z")
            .toFormatter(Locale.ROOT)
            .withResolverStyle(ResolverStyle.STRICT),
        new DateTimeFormatterBuilder().appendLiteral("T")
            .append(TIME_PREFIX)
            .appendZoneOrOffsetId()
            .toFormatter(Locale.ROOT)
            .withResolverStyle(ResolverStyle.STRICT),
        new DateTimeFormatterBuilder().appendLiteral("T")
            .append(TIME_PREFIX)
            .append(TIME_ZONE_FORMATTER_NO_COLON)
            .toFormatter(Locale.ROOT)
            .withResolverStyle(ResolverStyle.STRICT)
    );

    /*
     * Returns a formatter for a two digit hour of day, two digit minute of
     * hour, two digit second of minute, and time zone offset prefixed
     * by 'T' ('T'HH:mm:ssZZ).
     */
    private static final DateFormatter T_TIME_NO_MILLIS = new JavaDateFormatter(
        "t_time_no_millis",
        new DateTimeFormatterBuilder().appendLiteral("T")
            .append(STRICT_TIME_NO_MILLIS_BASE)
            .appendOffset("+HH:MM", "Z")
            .toFormatter(Locale.ROOT)
            .withResolverStyle(ResolverStyle.STRICT),
        new DateTimeFormatterBuilder().append(T_TIME_NO_MILLIS_FORMATTER)
            .appendZoneOrOffsetId()
            .toFormatter(Locale.ROOT)
            .withResolverStyle(ResolverStyle.STRICT),
        new DateTimeFormatterBuilder().append(T_TIME_NO_MILLIS_FORMATTER)
            .append(TIME_ZONE_FORMATTER_NO_COLON)
            .toFormatter(Locale.ROOT)
            .withResolverStyle(ResolverStyle.STRICT)
    );

    /*
     * A strict formatter that formats or parses a year and a month, such as '2011-12'.
     */
    private static final DateFormatter YEAR_MONTH = new JavaDateFormatter(
        "year_month",
        new DateTimeFormatterBuilder().appendValue(ChronoField.YEAR, 4, 10, SignStyle.EXCEEDS_PAD)
            .appendLiteral("-")
            .appendValue(MONTH_OF_YEAR, 2, 2, SignStyle.NOT_NEGATIVE)
            .toFormatter(Locale.ROOT)
            .withResolverStyle(ResolverStyle.STRICT),
        new DateTimeFormatterBuilder().appendValue(ChronoField.YEAR)
            .appendLiteral("-")
            .appendValue(MONTH_OF_YEAR)
            .toFormatter(Locale.ROOT)
            .withResolverStyle(ResolverStyle.STRICT)
    );

    /*
     * A strict date formatter that formats or parses a date without an offset, such as '2011-12-03'.
     */
    private static final DateFormatter YEAR_MONTH_DAY = new JavaDateFormatter(
        "year_month_day",
        STRICT_YEAR_MONTH_DAY_FORMATTER,
        new DateTimeFormatterBuilder().appendValue(ChronoField.YEAR)
            .appendLiteral("-")
            .appendValue(MONTH_OF_YEAR)
            .appendLiteral("-")
            .appendValue(DAY_OF_MONTH)
            .toFormatter(Locale.ROOT)
            .withResolverStyle(ResolverStyle.STRICT)
    );

    /*
     * Returns a formatter for a full date as four digit weekyear, two digit
     * week of weekyear, and one digit day of week (xxxx-'W'ww-e).
     */
    private static final DateFormatter WEEK_DATE = new JavaDateFormatter("week_date", ISO_WEEK_DATE, WEEK_DATE_FORMATTER);

    /*
     * Returns a formatter for a four digit weekyear and two digit week of
     * weekyear. (xxxx-'W'ww)
     */
    private static final DateFormatter WEEKYEAR_WEEK = new JavaDateFormatter(
        "weekyear_week",
        STRICT_WEEKYEAR_WEEK_FORMATTER,
        new DateTimeFormatterBuilder().appendValue(WEEK_FIELDS_ROOT.weekBasedYear())
            .appendLiteral("-W")
            .appendValue(WEEK_FIELDS_ROOT.weekOfWeekBasedYear())
            .toFormatter(Locale.ROOT)
            .withResolverStyle(ResolverStyle.STRICT)
    );

    /*
     * Returns a formatter for a four digit weekyear, two digit week of
     * weekyear, and one digit day of week. (xxxx-'W'ww-e)
     */
    private static final DateFormatter WEEKYEAR_WEEK_DAY = new JavaDateFormatter(
        "weekyear_week_day",
        new DateTimeFormatterBuilder().append(STRICT_WEEKYEAR_WEEK_FORMATTER)
            .appendLiteral("-")
            .appendValue(WEEK_FIELDS_ROOT.dayOfWeek())
            .toFormatter(Locale.ROOT)
            .withResolverStyle(ResolverStyle.STRICT),
        new DateTimeFormatterBuilder().appendValue(WEEK_FIELDS_ROOT.weekBasedYear())
            .appendLiteral("-W")
            .appendValue(WEEK_FIELDS_ROOT.weekOfWeekBasedYear())
            .appendLiteral("-")
            .appendValue(WEEK_FIELDS_ROOT.dayOfWeek())
            .toFormatter(Locale.ROOT)
            .withResolverStyle(ResolverStyle.STRICT)
    );

    /////////////////////////////////////////
    //
    // end lenient formatters
    //
    /////////////////////////////////////////

    static DateFormatter forPattern(String input) {
        if (Strings.hasLength(input)) {
            input = input.trim();
        }
        if (input == null || input.length() == 0) {
            throw new IllegalArgumentException("No date pattern provided");
        }

        if (FormatNames.ISO8601.matches(input)) {
            return ISO_8601;
        } else if (FormatNames.BASIC_DATE.matches(input)) {
            return BASIC_DATE;
        } else if (FormatNames.BASIC_DATE_TIME.matches(input)) {
            return BASIC_DATE_TIME;
        } else if (FormatNames.BASIC_DATE_TIME_NO_MILLIS.matches(input)) {
            return BASIC_DATE_TIME_NO_MILLIS;
        } else if (FormatNames.BASIC_ORDINAL_DATE.matches(input)) {
            return BASIC_ORDINAL_DATE;
        } else if (FormatNames.BASIC_ORDINAL_DATE_TIME.matches(input)) {
            return BASIC_ORDINAL_DATE_TIME;
        } else if (FormatNames.BASIC_ORDINAL_DATE_TIME_NO_MILLIS.matches(input)) {
            return BASIC_ORDINAL_DATE_TIME_NO_MILLIS;
        } else if (FormatNames.BASIC_TIME.matches(input)) {
            return BASIC_TIME;
        } else if (FormatNames.BASIC_TIME_NO_MILLIS.matches(input)) {
            return BASIC_TIME_NO_MILLIS;
        } else if (FormatNames.BASIC_T_TIME.matches(input)) {
            return BASIC_T_TIME;
        } else if (FormatNames.BASIC_T_TIME_NO_MILLIS.matches(input)) {
            return BASIC_T_TIME_NO_MILLIS;
        } else if (FormatNames.BASIC_WEEK_DATE.matches(input)) {
            return BASIC_WEEK_DATE;
        } else if (FormatNames.BASIC_WEEK_DATE_TIME.matches(input)) {
            return BASIC_WEEK_DATE_TIME;
        } else if (FormatNames.BASIC_WEEK_DATE_TIME_NO_MILLIS.matches(input)) {
            return BASIC_WEEK_DATE_TIME_NO_MILLIS;
        } else if (FormatNames.DATE.matches(input)) {
            return DATE;
        } else if (FormatNames.DATE_HOUR.matches(input)) {
            return DATE_HOUR;
        } else if (FormatNames.DATE_HOUR_MINUTE.matches(input)) {
            return DATE_HOUR_MINUTE;
        } else if (FormatNames.DATE_HOUR_MINUTE_SECOND.matches(input)) {
            return DATE_HOUR_MINUTE_SECOND;
        } else if (FormatNames.DATE_HOUR_MINUTE_SECOND_FRACTION.matches(input)) {
            return DATE_HOUR_MINUTE_SECOND_FRACTION;
        } else if (FormatNames.DATE_HOUR_MINUTE_SECOND_MILLIS.matches(input)) {
            return DATE_HOUR_MINUTE_SECOND_MILLIS;
        } else if (FormatNames.DATE_OPTIONAL_TIME.matches(input)) {
            return DATE_OPTIONAL_TIME;
        } else if (FormatNames.DATE_TIME.matches(input)) {
            return DATE_TIME;
        } else if (FormatNames.DATE_TIME_NO_MILLIS.matches(input)) {
            return DATE_TIME_NO_MILLIS;
        } else if (FormatNames.HOUR.matches(input)) {
            return HOUR;
        } else if (FormatNames.HOUR_MINUTE.matches(input)) {
            return HOUR_MINUTE;
        } else if (FormatNames.HOUR_MINUTE_SECOND.matches(input)) {
            return HOUR_MINUTE_SECOND;
        } else if (FormatNames.HOUR_MINUTE_SECOND_FRACTION.matches(input)) {
            return HOUR_MINUTE_SECOND_FRACTION;
        } else if (FormatNames.HOUR_MINUTE_SECOND_MILLIS.matches(input)) {
            return HOUR_MINUTE_SECOND_MILLIS;
        } else if (FormatNames.ORDINAL_DATE.matches(input)) {
            return ORDINAL_DATE;
        } else if (FormatNames.ORDINAL_DATE_TIME.matches(input)) {
            return ORDINAL_DATE_TIME;
        } else if (FormatNames.ORDINAL_DATE_TIME_NO_MILLIS.matches(input)) {
            return ORDINAL_DATE_TIME_NO_MILLIS;
        } else if (FormatNames.TIME.matches(input)) {
            return TIME;
        } else if (FormatNames.TIME_NO_MILLIS.matches(input)) {
            return TIME_NO_MILLIS;
        } else if (FormatNames.T_TIME.matches(input)) {
            return T_TIME;
        } else if (FormatNames.T_TIME_NO_MILLIS.matches(input)) {
            return T_TIME_NO_MILLIS;
        } else if (FormatNames.WEEK_DATE.matches(input)) {
            return WEEK_DATE;
        } else if (FormatNames.WEEK_DATE_TIME.matches(input)) {
            return WEEK_DATE_TIME;
        } else if (FormatNames.WEEK_DATE_TIME_NO_MILLIS.matches(input)) {
            return WEEK_DATE_TIME_NO_MILLIS;
        } else if (FormatNames.WEEKYEAR.matches(input)) {
            return WEEKYEAR;
        } else if (FormatNames.WEEK_YEAR_WEEK.matches(input)) {
            return WEEKYEAR_WEEK;
        } else if (FormatNames.WEEKYEAR_WEEK_DAY.matches(input)) {
            return WEEKYEAR_WEEK_DAY;
        } else if (FormatNames.YEAR.matches(input)) {
            return YEAR;
        } else if (FormatNames.YEAR_MONTH.matches(input)) {
            return YEAR_MONTH;
        } else if (FormatNames.YEAR_MONTH_DAY.matches(input)) {
            return YEAR_MONTH_DAY;
        } else if (FormatNames.EPOCH_SECOND.matches(input)) {
            return EpochTime.SECONDS_FORMATTER;
        } else if (FormatNames.EPOCH_MILLIS.matches(input)) {
            return EpochTime.MILLIS_FORMATTER;
            // strict date formats here, must be at least 4 digits for year and two for months and two for day
        } else if (FormatNames.STRICT_BASIC_WEEK_DATE.matches(input)) {
            return STRICT_BASIC_WEEK_DATE;
        } else if (FormatNames.STRICT_BASIC_WEEK_DATE_TIME.matches(input)) {
            return STRICT_BASIC_WEEK_DATE_TIME;
        } else if (FormatNames.STRICT_BASIC_WEEK_DATE_TIME_NO_MILLIS.matches(input)) {
            return STRICT_BASIC_WEEK_DATE_TIME_NO_MILLIS;
        } else if (FormatNames.STRICT_DATE.matches(input)) {
            return STRICT_DATE;
        } else if (FormatNames.STRICT_DATE_HOUR.matches(input)) {
            return STRICT_DATE_HOUR;
        } else if (FormatNames.STRICT_DATE_HOUR_MINUTE.matches(input)) {
            return STRICT_DATE_HOUR_MINUTE;
        } else if (FormatNames.STRICT_DATE_HOUR_MINUTE_SECOND.matches(input)) {
            return STRICT_DATE_HOUR_MINUTE_SECOND;
        } else if (FormatNames.STRICT_DATE_HOUR_MINUTE_SECOND_FRACTION.matches(input)) {
            return STRICT_DATE_HOUR_MINUTE_SECOND_FRACTION;
        } else if (FormatNames.STRICT_DATE_HOUR_MINUTE_SECOND_MILLIS.matches(input)) {
            return STRICT_DATE_HOUR_MINUTE_SECOND_MILLIS;
        } else if (FormatNames.STRICT_DATE_OPTIONAL_TIME.matches(input)) {
            return STRICT_DATE_OPTIONAL_TIME;
        } else if (FormatNames.STRICT_DATE_OPTIONAL_TIME_NANOS.matches(input)) {
            return STRICT_DATE_OPTIONAL_TIME_NANOS;
        } else if (FormatNames.STRICT_DATE_TIME.matches(input)) {
            return STRICT_DATE_TIME;
        } else if (FormatNames.STRICT_DATE_TIME_NO_MILLIS.matches(input)) {
            return STRICT_DATE_TIME_NO_MILLIS;
        } else if (FormatNames.STRICT_HOUR.matches(input)) {
            return STRICT_HOUR;
        } else if (FormatNames.STRICT_HOUR_MINUTE.matches(input)) {
            return STRICT_HOUR_MINUTE;
        } else if (FormatNames.STRICT_HOUR_MINUTE_SECOND.matches(input)) {
            return STRICT_HOUR_MINUTE_SECOND;
        } else if (FormatNames.STRICT_HOUR_MINUTE_SECOND_FRACTION.matches(input)) {
            return STRICT_HOUR_MINUTE_SECOND_FRACTION;
        } else if (FormatNames.STRICT_HOUR_MINUTE_SECOND_MILLIS.matches(input)) {
            return STRICT_HOUR_MINUTE_SECOND_MILLIS;
        } else if (FormatNames.STRICT_ORDINAL_DATE.matches(input)) {
            return STRICT_ORDINAL_DATE;
        } else if (FormatNames.STRICT_ORDINAL_DATE_TIME.matches(input)) {
            return STRICT_ORDINAL_DATE_TIME;
        } else if (FormatNames.STRICT_ORDINAL_DATE_TIME_NO_MILLIS.matches(input)) {
            return STRICT_ORDINAL_DATE_TIME_NO_MILLIS;
        } else if (FormatNames.STRICT_TIME.matches(input)) {
            return STRICT_TIME;
        } else if (FormatNames.STRICT_TIME_NO_MILLIS.matches(input)) {
            return STRICT_TIME_NO_MILLIS;
        } else if (FormatNames.STRICT_T_TIME.matches(input)) {
            return STRICT_T_TIME;
        } else if (FormatNames.STRICT_T_TIME_NO_MILLIS.matches(input)) {
            return STRICT_T_TIME_NO_MILLIS;
        } else if (FormatNames.STRICT_WEEK_DATE.matches(input)) {
            return STRICT_WEEK_DATE;
        } else if (FormatNames.STRICT_WEEK_DATE_TIME.matches(input)) {
            return STRICT_WEEK_DATE_TIME;
        } else if (FormatNames.STRICT_WEEK_DATE_TIME_NO_MILLIS.matches(input)) {
            return STRICT_WEEK_DATE_TIME_NO_MILLIS;
        } else if (FormatNames.STRICT_WEEKYEAR.matches(input)) {
            return STRICT_WEEKYEAR;
        } else if (FormatNames.STRICT_WEEKYEAR_WEEK.matches(input)) {
            return STRICT_WEEKYEAR_WEEK;
        } else if (FormatNames.STRICT_WEEKYEAR_WEEK_DAY.matches(input)) {
            return STRICT_WEEKYEAR_WEEK_DAY;
        } else if (FormatNames.STRICT_YEAR.matches(input)) {
            return STRICT_YEAR;
        } else if (FormatNames.STRICT_YEAR_MONTH.matches(input)) {
            return STRICT_YEAR_MONTH;
        } else if (FormatNames.STRICT_YEAR_MONTH_DAY.matches(input)) {
            return STRICT_YEAR_MONTH_DAY;
        } else {
            try {
                return new JavaDateFormatter(
                    input,
                    new DateTimeFormatterBuilder().appendPattern(input).toFormatter(Locale.ROOT).withResolverStyle(ResolverStyle.STRICT)
                );
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException("Invalid format: [" + input + "]: " + e.getMessage(), e);
            }
        }
    }

    private static final LocalDate LOCALDATE_EPOCH = LocalDate.of(1970, 1, 1);

    /**
     * Convert a temporal accessor to a zoned date time object - as performant as possible.
     * The .from() methods from the JDK are throwing exceptions when for example ZonedDateTime.from(accessor)
     * or Instant.from(accessor). This results in a huge performance penalty and should be prevented
     * This method prevents exceptions by querying the accessor for certain capabilities
     * and then act on it accordingly
     *
     * This action assumes that we can reliably fall back to some defaults if not all parts of a
     * zoned date time are set
     *
     * - If a zoned date time is passed, it is returned
     * - If no timezone is found, ZoneOffset.UTC is used
     * - If we find a time and a date, converting to a ZonedDateTime is straight forward,
     *   no defaults will be applied
     * - If an accessor only containing of seconds and nanos is found (like epoch_millis/second)
     *   an Instant is created out of that, that becomes a ZonedDateTime with a time zone
     * - If no time is given, the start of the day is used
     * - If no month of the year is found, the first day of the year is used
     * - If an iso based weekyear is found, but not week is specified, the first monday
     *   of the new year is chosen (retaining BWC with joda time)
     * - If an iso based weekyear is found and an iso based weekyear week, the start
     *   of the day is used
     *
     * @param accessor The accessor returned from a parser
     *
     * @return The converted zoned date time
     */
    public static ZonedDateTime from(TemporalAccessor accessor) {
        return from(accessor, Locale.ROOT, ZoneOffset.UTC);
    }

    public static ZonedDateTime from(TemporalAccessor accessor, Locale locale) {
        return from(accessor, locale, ZoneOffset.UTC);
    }

    public static ZonedDateTime from(TemporalAccessor accessor, Locale locale, ZoneId defaultZone) {
        if (accessor instanceof ZonedDateTime) {
            return (ZonedDateTime) accessor;
        }

        ZoneId zoneId = accessor.query(TemporalQueries.zone());
        if (zoneId == null) {
            zoneId = defaultZone;
        }

        LocalDate localDate = accessor.query(LOCAL_DATE_QUERY);
        LocalTime localTime = accessor.query(TemporalQueries.localTime());
        boolean isLocalDateSet = localDate != null;
        boolean isLocalTimeSet = localTime != null;

        // the first two cases are the most common, so this allows us to exit early when parsing dates
        if (isLocalDateSet && isLocalTimeSet) {
            return of(localDate, localTime, zoneId);
        } else if (accessor.isSupported(ChronoField.INSTANT_SECONDS) && accessor.isSupported(NANO_OF_SECOND)) {
            return Instant.from(accessor).atZone(zoneId);
        } else if (isLocalDateSet) {
            return localDate.atStartOfDay(zoneId);
        } else if (isLocalTimeSet) {
            return of(getLocalDate(accessor, locale), localTime, zoneId);
        } else if (accessor.isSupported(ChronoField.YEAR) || accessor.isSupported(ChronoField.YEAR_OF_ERA)) {
            if (accessor.isSupported(MONTH_OF_YEAR)) {
                return getFirstOfMonth(accessor).atStartOfDay(zoneId);
            } else {
                int year = getYear(accessor);
                return Year.of(year).atDay(1).atStartOfDay(zoneId);
            }
        } else if (accessor.isSupported(MONTH_OF_YEAR)) {
            // missing year, falling back to the epoch and then filling
            return getLocalDate(accessor, locale).atStartOfDay(zoneId);
        } else if (accessor.isSupported(WeekFields.of(locale).weekBasedYear())) {
            return localDateFromWeekBasedDate(accessor, locale).atStartOfDay(zoneId);
        }

        // we should not reach this piece of code, everything being parsed we should be able to
        // convert to a zoned date time! If not, we have to extend the above methods
        throw new IllegalArgumentException("temporal accessor [" + accessor + "] cannot be converted to zoned date time");
    }

    private static LocalDate localDateFromWeekBasedDate(TemporalAccessor accessor, Locale locale) {
        WeekFields weekFields = WeekFields.of(locale);
        if (accessor.isSupported(weekFields.weekOfWeekBasedYear())) {
            return LocalDate.ofEpochDay(0)
                .with(weekFields.weekBasedYear(), accessor.get(weekFields.weekBasedYear()))
                .with(weekFields.weekOfWeekBasedYear(), accessor.get(weekFields.weekOfWeekBasedYear()))
                .with(TemporalAdjusters.previousOrSame(weekFields.getFirstDayOfWeek()));
        } else {
            return LocalDate.ofEpochDay(0)
                .with(weekFields.weekBasedYear(), accessor.get(weekFields.weekBasedYear()))
                .with(TemporalAdjusters.previousOrSame(weekFields.getFirstDayOfWeek()));

        }
    }

    /**
     * extending the java.time.temporal.TemporalQueries.LOCAL_DATE implementation to also create local dates
     * when YearOfEra was used instead of Year.
     * This is to make it compatible with Joda behaviour
     */
    static final TemporalQuery<LocalDate> LOCAL_DATE_QUERY = new TemporalQuery<LocalDate>() {
        @Override
        public LocalDate queryFrom(TemporalAccessor temporal) {
            if (temporal.isSupported(ChronoField.EPOCH_DAY)) {
                return LocalDate.ofEpochDay(temporal.getLong(ChronoField.EPOCH_DAY));
            } else if (temporal.isSupported(ChronoField.YEAR_OF_ERA) || temporal.isSupported(ChronoField.YEAR)) {
                int year = getYear(temporal);
                if (temporal.isSupported(ChronoField.MONTH_OF_YEAR) && temporal.isSupported(ChronoField.DAY_OF_MONTH)) {
                    return LocalDate.of(year, temporal.get(ChronoField.MONTH_OF_YEAR), temporal.get(ChronoField.DAY_OF_MONTH));
                } else if (temporal.isSupported(DAY_OF_YEAR)) {
                    return LocalDate.ofYearDay(year, temporal.get(DAY_OF_YEAR));
                }
            }
            return null;
        }

        @Override
        public String toString() {
            return "LocalDate";
        }
    };

    private static LocalDate getLocalDate(TemporalAccessor accessor, Locale locale) {
        if (accessor.isSupported(WeekFields.of(locale).weekBasedYear())) {
            return localDateFromWeekBasedDate(accessor, locale);
        } else if (accessor.isSupported(MONTH_OF_YEAR)) {
            int year = getYear(accessor);
            if (accessor.isSupported(DAY_OF_MONTH)) {
                return LocalDate.of(year, accessor.get(MONTH_OF_YEAR), accessor.get(DAY_OF_MONTH));
            } else {
                return LocalDate.of(year, accessor.get(MONTH_OF_YEAR), 1);
            }
        }

        return LOCALDATE_EPOCH;
    }

    private static int getYear(TemporalAccessor accessor) {
        if (accessor.isSupported(ChronoField.YEAR)) {
            return accessor.get(ChronoField.YEAR);
        }
        if (accessor.isSupported(ChronoField.YEAR_OF_ERA)) {
            return accessor.get(ChronoField.YEAR_OF_ERA);
        }
        return 1970;
    }

    @SuppressForbidden(reason = "ZonedDateTime.of is fine here")
    private static ZonedDateTime of(LocalDate localDate, LocalTime localTime, ZoneId zoneId) {
        return ZonedDateTime.of(localDate, localTime, zoneId);
    }

    @SuppressForbidden(reason = "LocalDate.of is fine here")
    private static LocalDate getFirstOfMonth(TemporalAccessor accessor) {
        return LocalDate.of(getYear(accessor), accessor.get(MONTH_OF_YEAR), 1);
    }
}
