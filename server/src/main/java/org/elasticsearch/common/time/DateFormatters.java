/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.common.time;

import org.elasticsearch.common.Strings;

import java.time.DateTimeException;
import java.time.DayOfWeek;
import java.time.Instant;
import java.time.LocalDate;
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
import java.time.temporal.WeekFields;
import java.util.Locale;

import static java.time.temporal.ChronoField.DAY_OF_MONTH;
import static java.time.temporal.ChronoField.DAY_OF_WEEK;
import static java.time.temporal.ChronoField.DAY_OF_YEAR;
import static java.time.temporal.ChronoField.HOUR_OF_DAY;
import static java.time.temporal.ChronoField.MILLI_OF_SECOND;
import static java.time.temporal.ChronoField.MINUTE_OF_HOUR;
import static java.time.temporal.ChronoField.MONTH_OF_YEAR;
import static java.time.temporal.ChronoField.NANO_OF_SECOND;
import static java.time.temporal.ChronoField.SECOND_OF_MINUTE;

public class DateFormatters {

    private static final DateTimeFormatter TIME_ZONE_FORMATTER_NO_COLON = new DateTimeFormatterBuilder()
        .appendOffset("+HHmm", "Z")
        .toFormatter(Locale.ROOT);

    private static final DateTimeFormatter STRICT_YEAR_MONTH_DAY_FORMATTER = new DateTimeFormatterBuilder()
        .appendValue(ChronoField.YEAR, 4, 10, SignStyle.EXCEEDS_PAD)
        .appendLiteral("-")
        .appendValue(MONTH_OF_YEAR, 2, 2, SignStyle.NOT_NEGATIVE)
        .appendLiteral('-')
        .appendValue(DAY_OF_MONTH, 2, 2, SignStyle.NOT_NEGATIVE)
        .toFormatter(Locale.ROOT);

    private static final DateTimeFormatter STRICT_HOUR_MINUTE_SECOND_FORMATTER = new DateTimeFormatterBuilder()
        .appendValue(HOUR_OF_DAY, 2, 2, SignStyle.NOT_NEGATIVE)
        .appendLiteral(':')
        .appendValue(MINUTE_OF_HOUR, 2, 2, SignStyle.NOT_NEGATIVE)
        .appendLiteral(':')
        .appendValue(SECOND_OF_MINUTE, 2, 2, SignStyle.NOT_NEGATIVE)
        .toFormatter(Locale.ROOT);

    private static final DateTimeFormatter STRICT_DATE_OPTIONAL_TIME_FORMATTER_1 = new DateTimeFormatterBuilder()
        .append(STRICT_YEAR_MONTH_DAY_FORMATTER)
        .optionalStart()
        .appendLiteral('T')
        .append(STRICT_HOUR_MINUTE_SECOND_FORMATTER)
        .optionalStart()
        .appendFraction(MILLI_OF_SECOND, 3, 3, true)
        .optionalEnd()
        .optionalStart()
        .appendZoneOrOffsetId()
        .optionalEnd()
        .optionalEnd()
        .toFormatter(Locale.ROOT);

    private static final DateTimeFormatter STRICT_DATE_OPTIONAL_TIME_FORMATTER_2 = new DateTimeFormatterBuilder()
        .append(STRICT_YEAR_MONTH_DAY_FORMATTER)
        .optionalStart()
        .appendLiteral('T')
        .append(STRICT_HOUR_MINUTE_SECOND_FORMATTER)
        .optionalStart()
        .appendFraction(MILLI_OF_SECOND, 3, 3, true)
        .optionalEnd()
        .optionalStart()
        .appendOffset("+HHmm", "Z")
        .optionalEnd()
        .optionalEnd()
        .toFormatter(Locale.ROOT);

    /**
     * Returns a generic ISO datetime parser where the date is mandatory and the time is optional.
     */
    private static final DateFormatter STRICT_DATE_OPTIONAL_TIME =
        new JavaDateFormatter("strict_date_optional_time", STRICT_DATE_OPTIONAL_TIME_FORMATTER_1,
            STRICT_DATE_OPTIONAL_TIME_FORMATTER_1, STRICT_DATE_OPTIONAL_TIME_FORMATTER_2);

    private static final DateTimeFormatter STRICT_DATE_OPTIONAL_TIME_FORMATTER_WITH_NANOS_1 = new DateTimeFormatterBuilder()
        .append(STRICT_YEAR_MONTH_DAY_FORMATTER)
        .optionalStart()
        .appendLiteral('T')
        .append(STRICT_HOUR_MINUTE_SECOND_FORMATTER)
        .optionalStart()
        .appendFraction(NANO_OF_SECOND, 3, 9, true)
        .optionalEnd()
        .optionalStart()
        .appendZoneOrOffsetId()
        .optionalEnd()
        .optionalEnd()
        .toFormatter(Locale.ROOT);

    private static final DateTimeFormatter STRICT_DATE_OPTIONAL_TIME_FORMATTER_WITH_NANOS_2 = new DateTimeFormatterBuilder()
        .append(STRICT_YEAR_MONTH_DAY_FORMATTER)
        .optionalStart()
        .appendLiteral('T')
        .append(STRICT_HOUR_MINUTE_SECOND_FORMATTER)
        .optionalStart()
        .appendFraction(NANO_OF_SECOND, 3, 9, true)
        .optionalEnd()
        .optionalStart()
        .appendOffset("+HHmm", "Z")
        .optionalEnd()
        .optionalEnd()
        .toFormatter(Locale.ROOT);

    /**
     * Returns a generic ISO datetime parser where the date is mandatory and the time is optional with nanosecond resolution.
     */
    private static final DateFormatter STRICT_DATE_OPTIONAL_TIME_NANOS = new JavaDateFormatter("strict_date_optional_time_nanos",
        STRICT_DATE_OPTIONAL_TIME_FORMATTER_WITH_NANOS_1,
        STRICT_DATE_OPTIONAL_TIME_FORMATTER_WITH_NANOS_1, STRICT_DATE_OPTIONAL_TIME_FORMATTER_WITH_NANOS_2);

    /////////////////////////////////////////
    //
    // BEGIN basic time formatters
    //
    // these formatters to not have any splitting characters between hours, minutes, seconds, milliseconds
    // this means they have to be strict with the exception of the last element
    //
    /////////////////////////////////////////

    private static final DateTimeFormatter BASIC_TIME_NO_MILLIS_BASE = new DateTimeFormatterBuilder()
        .appendValue(HOUR_OF_DAY, 2, 2, SignStyle.NOT_NEGATIVE)
        .appendValue(MINUTE_OF_HOUR, 2, 2, SignStyle.NOT_NEGATIVE)
        .appendValue(SECOND_OF_MINUTE, 2, 2, SignStyle.NOT_NEGATIVE)
        .toFormatter(Locale.ROOT);

    /*
     * Returns a basic formatter for a two digit hour of day, two digit minute
     * of hour, two digit second of minute, and time zone offset (HHmmssZ).
     */
    private static final DateFormatter BASIC_TIME_NO_MILLIS = new JavaDateFormatter("basic_time_no_millis",
        new DateTimeFormatterBuilder().append(BASIC_TIME_NO_MILLIS_BASE).appendZoneOrOffsetId().toFormatter(Locale.ROOT),
        new DateTimeFormatterBuilder().append(BASIC_TIME_NO_MILLIS_BASE).appendZoneOrOffsetId().toFormatter(Locale.ROOT),
        new DateTimeFormatterBuilder().append(BASIC_TIME_NO_MILLIS_BASE).append(TIME_ZONE_FORMATTER_NO_COLON).toFormatter(Locale.ROOT)
    );

    private static final DateTimeFormatter BASIC_TIME_FORMATTER = new DateTimeFormatterBuilder()
        .appendValue(HOUR_OF_DAY, 2, 2, SignStyle.NOT_NEGATIVE)
        .appendValue(MINUTE_OF_HOUR, 2, 2, SignStyle.NOT_NEGATIVE)
        .appendValue(SECOND_OF_MINUTE, 2, 2, SignStyle.NOT_NEGATIVE)
        .appendFraction(MILLI_OF_SECOND, 1, 3, true)
        .toFormatter(Locale.ROOT);

    private static final DateTimeFormatter BASIC_TIME_PRINTER = new DateTimeFormatterBuilder()
        .appendValue(HOUR_OF_DAY, 2, 2, SignStyle.NOT_NEGATIVE)
        .appendValue(MINUTE_OF_HOUR, 2, 2, SignStyle.NOT_NEGATIVE)
        .appendValue(SECOND_OF_MINUTE, 2, 2, SignStyle.NOT_NEGATIVE)
        .appendFraction(MILLI_OF_SECOND, 3, 3, true)
        .toFormatter(Locale.ROOT);

    /*
     * Returns a basic formatter for a two digit hour of day, two digit minute
     * of hour, two digit second of minute, three digit millis, and time zone
     * offset (HHmmss.SSSZ).
     */
    private static final DateFormatter BASIC_TIME = new JavaDateFormatter("basic_time",
        new DateTimeFormatterBuilder().append(BASIC_TIME_PRINTER).appendZoneOrOffsetId().toFormatter(Locale.ROOT),
        new DateTimeFormatterBuilder().append(BASIC_TIME_FORMATTER).appendZoneOrOffsetId().toFormatter(Locale.ROOT),
        new DateTimeFormatterBuilder().append(BASIC_TIME_FORMATTER).append(TIME_ZONE_FORMATTER_NO_COLON).toFormatter(Locale.ROOT)
    );

    private static final DateTimeFormatter BASIC_T_TIME_PRINTER =
        new DateTimeFormatterBuilder().appendLiteral("T").append(BASIC_TIME_PRINTER).toFormatter(Locale.ROOT);

    private static final DateTimeFormatter BASIC_T_TIME_FORMATTER =
        new DateTimeFormatterBuilder().appendLiteral("T").append(BASIC_TIME_FORMATTER).toFormatter(Locale.ROOT);

    /*
     * Returns a basic formatter for a two digit hour of day, two digit minute
     * of hour, two digit second of minute, three digit millis, and time zone
     * offset prefixed by 'T' ('T'HHmmss.SSSZ).
     */
    private static final DateFormatter BASIC_T_TIME = new JavaDateFormatter("basic_t_time",
        new DateTimeFormatterBuilder().append(BASIC_T_TIME_PRINTER).appendZoneOrOffsetId().toFormatter(Locale.ROOT),
        new DateTimeFormatterBuilder().append(BASIC_T_TIME_FORMATTER).appendZoneOrOffsetId().toFormatter(Locale.ROOT),
        new DateTimeFormatterBuilder().append(BASIC_T_TIME_FORMATTER).append(TIME_ZONE_FORMATTER_NO_COLON).toFormatter(Locale.ROOT)
    );

    /*
     * Returns a basic formatter for a two digit hour of day, two digit minute
     * of hour, two digit second of minute, and time zone offset prefixed by 'T'
     * ('T'HHmmssZ).
     */
    private static final DateFormatter BASIC_T_TIME_NO_MILLIS = new JavaDateFormatter("basic_t_time_no_millis",
        new DateTimeFormatterBuilder().appendLiteral("T").append(BASIC_TIME_NO_MILLIS_BASE).appendZoneOrOffsetId().toFormatter(Locale.ROOT),
        new DateTimeFormatterBuilder().appendLiteral("T").append(BASIC_TIME_NO_MILLIS_BASE).appendZoneOrOffsetId().toFormatter(Locale.ROOT),
        new DateTimeFormatterBuilder().appendLiteral("T").append(BASIC_TIME_NO_MILLIS_BASE).append(TIME_ZONE_FORMATTER_NO_COLON)
            .toFormatter(Locale.ROOT)
    );

    private static final DateTimeFormatter BASIC_YEAR_MONTH_DAY_FORMATTER = new DateTimeFormatterBuilder()
        .appendValue(ChronoField.YEAR, 4, 4, SignStyle.NORMAL)
        .appendValue(MONTH_OF_YEAR, 2, 2, SignStyle.NOT_NEGATIVE)
        .appendValue(DAY_OF_MONTH, 2, 2, SignStyle.NOT_NEGATIVE)
        .toFormatter(Locale.ROOT);

    private static final DateTimeFormatter BASIC_DATE_TIME_FORMATTER = new DateTimeFormatterBuilder()
        .append(BASIC_YEAR_MONTH_DAY_FORMATTER)
        .append(BASIC_T_TIME_FORMATTER)
        .toFormatter(Locale.ROOT);

    private static final DateTimeFormatter BASIC_DATE_TIME_PRINTER = new DateTimeFormatterBuilder()
        .append(BASIC_YEAR_MONTH_DAY_FORMATTER)
        .append(BASIC_T_TIME_PRINTER)
        .toFormatter(Locale.ROOT);

    /*
     * Returns a basic formatter that combines a basic date and time, separated
     * by a 'T' (yyyyMMdd'T'HHmmss.SSSZ).
     */
    private static final DateFormatter BASIC_DATE_TIME = new JavaDateFormatter("basic_date_time",
        new DateTimeFormatterBuilder().append(BASIC_DATE_TIME_PRINTER).appendZoneOrOffsetId().toFormatter(Locale.ROOT),
        new DateTimeFormatterBuilder().append(BASIC_DATE_TIME_FORMATTER).appendZoneOrOffsetId().toFormatter(Locale.ROOT),
        new DateTimeFormatterBuilder().append(BASIC_DATE_TIME_FORMATTER).append(TIME_ZONE_FORMATTER_NO_COLON).toFormatter(Locale.ROOT)
    );

    private static final DateTimeFormatter BASIC_DATE_T =
        new DateTimeFormatterBuilder().append(BASIC_YEAR_MONTH_DAY_FORMATTER).appendLiteral("T").toFormatter(Locale.ROOT);

    /*
     * Returns a basic formatter that combines a basic date and time without millis,
     * separated by a 'T' (yyyyMMdd'T'HHmmssZ).
     */
    private static final DateFormatter BASIC_DATE_TIME_NO_MILLIS = new JavaDateFormatter("basic_t_time_no_millis",
        new DateTimeFormatterBuilder().append(BASIC_DATE_T).append(BASIC_TIME_NO_MILLIS_BASE)
            .appendZoneOrOffsetId().toFormatter(Locale.ROOT),
        new DateTimeFormatterBuilder().append(BASIC_DATE_T).append(BASIC_TIME_NO_MILLIS_BASE)
            .appendZoneOrOffsetId().toFormatter(Locale.ROOT),
        new DateTimeFormatterBuilder().append(BASIC_DATE_T).append(BASIC_TIME_NO_MILLIS_BASE)
            .append(TIME_ZONE_FORMATTER_NO_COLON).toFormatter(Locale.ROOT)
    );

    /*
     * Returns a formatter for a full ordinal date, using a four
     * digit year and three digit dayOfYear (yyyyDDD).
     */
    private static final DateFormatter BASIC_ORDINAL_DATE = new JavaDateFormatter("basic_ordinal_date",
        DateTimeFormatter.ofPattern("yyyyDDD", Locale.ROOT));

    /*
     * Returns a formatter for a full ordinal date and time, using a four
     * digit year and three digit dayOfYear (yyyyDDD'T'HHmmss.SSSZ).
     */
    private static final DateFormatter BASIC_ORDINAL_DATE_TIME = new JavaDateFormatter("basic_ordinal_date_time",
        new DateTimeFormatterBuilder().appendPattern("yyyyDDD").append(BASIC_T_TIME_PRINTER)
            .appendZoneOrOffsetId().toFormatter(Locale.ROOT),
        new DateTimeFormatterBuilder().appendPattern("yyyyDDD").append(BASIC_T_TIME_FORMATTER)
            .append(TIME_ZONE_FORMATTER_NO_COLON).toFormatter(Locale.ROOT)

    );

    /*
     * Returns a formatter for a full ordinal date and time without millis,
     * using a four digit year and three digit dayOfYear (yyyyDDD'T'HHmmssZ).
     */
    private static final DateFormatter BASIC_ORDINAL_DATE_TIME_NO_MILLIS = new JavaDateFormatter("basic_ordinal_date_time_no_millis",
        new DateTimeFormatterBuilder().appendPattern("yyyyDDD").appendLiteral("T").append(BASIC_TIME_NO_MILLIS_BASE)
            .appendZoneOrOffsetId().toFormatter(Locale.ROOT),
        new DateTimeFormatterBuilder().appendPattern("yyyyDDD").appendLiteral("T").append(BASIC_TIME_NO_MILLIS_BASE)
            .appendZoneOrOffsetId().toFormatter(Locale.ROOT),
        new DateTimeFormatterBuilder().appendPattern("yyyyDDD").appendLiteral("T").append(BASIC_TIME_NO_MILLIS_BASE)
            .append(TIME_ZONE_FORMATTER_NO_COLON).toFormatter(Locale.ROOT)
    );

    private static final DateTimeFormatter BASIC_WEEK_DATE_FORMATTER = new DateTimeFormatterBuilder()
        .appendValue(IsoFields.WEEK_BASED_YEAR)
        .appendLiteral("W")
        .appendValue(IsoFields.WEEK_OF_WEEK_BASED_YEAR, 1, 2, SignStyle.NEVER)
        .appendValue(ChronoField.DAY_OF_WEEK)
        .toFormatter(Locale.ROOT);

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
    private static final DateTimeFormatter STRICT_BASIC_WEEK_DATE_FORMATTER = new DateTimeFormatterBuilder()
        .parseStrict()
        .appendValue(IsoFields.WEEK_BASED_YEAR, 4)
        .appendLiteral("W")
        .appendValue(IsoFields.WEEK_OF_WEEK_BASED_YEAR, 1, 2, SignStyle.NEVER)
        .appendValue(ChronoField.DAY_OF_WEEK)
        .toFormatter(Locale.ROOT);

    private static final DateTimeFormatter STRICT_BASIC_WEEK_DATE_PRINTER = new DateTimeFormatterBuilder()
        .parseStrict()
        .appendValue(IsoFields.WEEK_BASED_YEAR, 4)
        .appendLiteral("W")
        .appendValue(IsoFields.WEEK_OF_WEEK_BASED_YEAR, 2, 2, SignStyle.NEVER)
        .appendValue(ChronoField.DAY_OF_WEEK)
        .toFormatter(Locale.ROOT);

    /*
     * Returns a basic formatter for a full date as four digit weekyear, two
     * digit week of weekyear, and one digit day of week (xxxx'W'wwe).
     */
    private static final DateFormatter STRICT_BASIC_WEEK_DATE =
        new JavaDateFormatter("strict_basic_week_date", STRICT_BASIC_WEEK_DATE_PRINTER, STRICT_BASIC_WEEK_DATE_FORMATTER);

    /*
     * Returns a basic formatter that combines a basic weekyear date and time
     * without millis, separated by a 'T' (xxxx'W'wwe'T'HHmmssX).
     */
    private static final DateFormatter STRICT_BASIC_WEEK_DATE_TIME_NO_MILLIS = new JavaDateFormatter("strict_basic_week_date_no_millis",
        new DateTimeFormatterBuilder()
            .append(STRICT_BASIC_WEEK_DATE_PRINTER).append(DateTimeFormatter.ofPattern("'T'HHmmssX", Locale.ROOT))
            .toFormatter(Locale.ROOT),
        new DateTimeFormatterBuilder()
            .append(STRICT_BASIC_WEEK_DATE_FORMATTER).append(DateTimeFormatter.ofPattern("'T'HHmmssX", Locale.ROOT))
            .toFormatter(Locale.ROOT)
    );

    /*
     * Returns a basic formatter that combines a basic weekyear date and time,
     * separated by a 'T' (xxxx'W'wwe'T'HHmmss.SSSX).
     */
    private static final DateFormatter STRICT_BASIC_WEEK_DATE_TIME = new JavaDateFormatter("strict_basic_week_date_time",
        new DateTimeFormatterBuilder()
        .append(STRICT_BASIC_WEEK_DATE_PRINTER)
        .append(DateTimeFormatter.ofPattern("'T'HHmmss.SSSX", Locale.ROOT))
        .toFormatter(Locale.ROOT),
        new DateTimeFormatterBuilder()
        .append(STRICT_BASIC_WEEK_DATE_FORMATTER)
        .append(DateTimeFormatter.ofPattern("'T'HHmmss.SSSX", Locale.ROOT))
        .toFormatter(Locale.ROOT)
    );

    /*
     * An ISO date formatter that formats or parses a date without an offset, such as '2011-12-03'.
     */
    private static final DateFormatter STRICT_DATE = new JavaDateFormatter("strict_date",
        DateTimeFormatter.ISO_LOCAL_DATE.withResolverStyle(ResolverStyle.LENIENT));

    /*
     * A date formatter that formats or parses a date plus an hour without an offset, such as '2011-12-03T01'.
     */
    private static final DateFormatter STRICT_DATE_HOUR = new JavaDateFormatter("strict_date_hour",
        DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH", Locale.ROOT));

    /*
     * A date formatter that formats or parses a date plus an hour/minute without an offset, such as '2011-12-03T01:10'.
     */
    private static final DateFormatter STRICT_DATE_HOUR_MINUTE = new JavaDateFormatter("strict_date_hour_minute",
        DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm", Locale.ROOT));

    /*
     * A strict date formatter that formats or parses a date without an offset, such as '2011-12-03'.
     */
    private static final DateFormatter STRICT_YEAR_MONTH_DAY =
        new JavaDateFormatter("strict_year_month_day", STRICT_YEAR_MONTH_DAY_FORMATTER);

    /*
     * A strict formatter that formats or parses a year and a month, such as '2011-12'.
     */
    private static final DateFormatter STRICT_YEAR_MONTH = new JavaDateFormatter("strict_year_month",
        new DateTimeFormatterBuilder()
        .appendValue(ChronoField.YEAR, 4, 10, SignStyle.EXCEEDS_PAD)
        .appendLiteral("-")
        .appendValue(MONTH_OF_YEAR, 2, 2, SignStyle.NOT_NEGATIVE)
        .toFormatter(Locale.ROOT));

    /*
     * A strict formatter that formats or parses a year, such as '2011'.
     */
    private static final DateFormatter STRICT_YEAR = new JavaDateFormatter("strict_year", new DateTimeFormatterBuilder()
        .appendValue(ChronoField.YEAR, 4, 10, SignStyle.EXCEEDS_PAD)
        .toFormatter(Locale.ROOT));

    /*
     * A strict formatter that formats or parses a hour, minute and second, such as '09:43:25'.
     */
    private static final DateFormatter STRICT_HOUR_MINUTE_SECOND =
        new JavaDateFormatter("strict_hour_minute_second", STRICT_HOUR_MINUTE_SECOND_FORMATTER);

    private static final DateTimeFormatter STRICT_DATE_FORMATTER = new DateTimeFormatterBuilder()
        .append(STRICT_YEAR_MONTH_DAY_FORMATTER)
        .appendLiteral('T')
        .append(STRICT_HOUR_MINUTE_SECOND_FORMATTER)
        .optionalStart()
        .appendFraction(MILLI_OF_SECOND, 3, 3, true)
        .optionalEnd()
        .toFormatter(Locale.ROOT);

    /*
     * Returns a formatter that combines a full date and time, separated by a 'T'
     * (yyyy-MM-dd'T'HH:mm:ss.SSSZZ).
     */
    private static final DateFormatter STRICT_DATE_TIME = new JavaDateFormatter("strict_date_time",
        new DateTimeFormatterBuilder().append(STRICT_DATE_FORMATTER).appendZoneOrOffsetId().toFormatter(Locale.ROOT),
        new DateTimeFormatterBuilder().append(STRICT_DATE_FORMATTER).appendZoneOrOffsetId().toFormatter(Locale.ROOT),
        new DateTimeFormatterBuilder().append(STRICT_DATE_FORMATTER).append(TIME_ZONE_FORMATTER_NO_COLON).toFormatter(Locale.ROOT)
    );

    private static final DateTimeFormatter STRICT_ORDINAL_DATE_TIME_NO_MILLIS_BASE = new DateTimeFormatterBuilder()
        .appendValue(ChronoField.YEAR, 4, 10, SignStyle.EXCEEDS_PAD)
        .appendLiteral('-')
        .appendValue(DAY_OF_YEAR, 3, 3, SignStyle.NOT_NEGATIVE)
        .appendLiteral('T')
        .append(STRICT_HOUR_MINUTE_SECOND_FORMATTER)
        .toFormatter(Locale.ROOT);

    /*
     * Returns a formatter for a full ordinal date and time without millis,
     * using a four digit year and three digit dayOfYear (yyyy-DDD'T'HH:mm:ssZZ).
     */
    private static final DateFormatter STRICT_ORDINAL_DATE_TIME_NO_MILLIS = new JavaDateFormatter("strict_ordinal_date_time_no_millis",
        new DateTimeFormatterBuilder().append(STRICT_ORDINAL_DATE_TIME_NO_MILLIS_BASE)
            .appendZoneOrOffsetId().toFormatter(Locale.ROOT),
        new DateTimeFormatterBuilder().append(STRICT_ORDINAL_DATE_TIME_NO_MILLIS_BASE)
            .append(TIME_ZONE_FORMATTER_NO_COLON).toFormatter(Locale.ROOT)
    );

    private static final DateTimeFormatter STRICT_DATE_TIME_NO_MILLIS_FORMATTER = new DateTimeFormatterBuilder()
        .append(STRICT_YEAR_MONTH_DAY_FORMATTER)
        .appendLiteral('T')
        .append(STRICT_HOUR_MINUTE_SECOND_FORMATTER)
        .toFormatter(Locale.ROOT);

    /*
     * Returns a formatter that combines a full date and time without millis,
     * separated by a 'T' (yyyy-MM-dd'T'HH:mm:ssZZ).
     */
    private static final DateFormatter STRICT_DATE_TIME_NO_MILLIS = new JavaDateFormatter("strict_date_time_no_millis",
        new DateTimeFormatterBuilder().append(STRICT_DATE_TIME_NO_MILLIS_FORMATTER)
            .appendZoneOrOffsetId().toFormatter(Locale.ROOT),
        new DateTimeFormatterBuilder().append(STRICT_DATE_TIME_NO_MILLIS_FORMATTER)
            .appendZoneOrOffsetId().toFormatter(Locale.ROOT),
        new DateTimeFormatterBuilder().append(STRICT_DATE_TIME_NO_MILLIS_FORMATTER)
            .append(TIME_ZONE_FORMATTER_NO_COLON).toFormatter(Locale.ROOT)
    );

    // NOTE: this is not a strict formatter to retain the joda time based behaviour, even though it's named like this
    private static final DateTimeFormatter STRICT_HOUR_MINUTE_SECOND_MILLIS_FORMATTER = new DateTimeFormatterBuilder()
        .append(STRICT_HOUR_MINUTE_SECOND_FORMATTER)
        .appendFraction(MILLI_OF_SECOND, 1, 3, true)
        .toFormatter(Locale.ROOT);

    private static final DateTimeFormatter STRICT_HOUR_MINUTE_SECOND_MILLIS_PRINTER = new DateTimeFormatterBuilder()
        .append(STRICT_HOUR_MINUTE_SECOND_FORMATTER)
        .appendFraction(MILLI_OF_SECOND, 3, 3, true)
        .toFormatter(Locale.ROOT);

    /*
     * Returns a formatter for a two digit hour of day, two digit minute of
     * hour, two digit second of minute, and three digit fraction of
     * second (HH:mm:ss.SSS).
     *
     * NOTE: this is not a strict formatter to retain the joda time based behaviour,
     *       even though it's named like this
     */
    private static final DateFormatter STRICT_HOUR_MINUTE_SECOND_MILLIS =
        new JavaDateFormatter("strict_hour_minute_second_millis",
            STRICT_HOUR_MINUTE_SECOND_MILLIS_PRINTER, STRICT_HOUR_MINUTE_SECOND_MILLIS_FORMATTER);

    private static final DateFormatter STRICT_HOUR_MINUTE_SECOND_FRACTION = STRICT_HOUR_MINUTE_SECOND_MILLIS;

    /*
     * Returns a formatter that combines a full date, two digit hour of day,
     * two digit minute of hour, two digit second of minute, and three digit
     * fraction of second (yyyy-MM-dd'T'HH:mm:ss.SSS).
     */
    private static final DateFormatter STRICT_DATE_HOUR_MINUTE_SECOND_FRACTION = new JavaDateFormatter(
        "strict_date_hour_minute_second_fraction",
        new DateTimeFormatterBuilder()
            .append(STRICT_YEAR_MONTH_DAY_FORMATTER)
            .appendLiteral("T")
            .append(STRICT_HOUR_MINUTE_SECOND_MILLIS_PRINTER)
            .toFormatter(Locale.ROOT),
        new DateTimeFormatterBuilder()
            .append(STRICT_YEAR_MONTH_DAY_FORMATTER)
            .appendLiteral("T")
            .append(STRICT_HOUR_MINUTE_SECOND_FORMATTER)
            //  this one here is lenient as well to retain joda time based bwc compatibility
            .appendFraction(MILLI_OF_SECOND, 1, 3, true)
            .toFormatter(Locale.ROOT)
    );

    private static final DateFormatter STRICT_DATE_HOUR_MINUTE_SECOND_MILLIS = STRICT_DATE_HOUR_MINUTE_SECOND_FRACTION;

    /*
     * Returns a formatter for a two digit hour of day. (HH)
     */
    private static final DateFormatter STRICT_HOUR =
        new JavaDateFormatter("strict_hour", DateTimeFormatter.ofPattern("HH", Locale.ROOT));

    /*
     * Returns a formatter for a two digit hour of day and two digit minute of
     * hour. (HH:mm)
     */
    private static final DateFormatter STRICT_HOUR_MINUTE =
        new JavaDateFormatter("strict_hour_minute", DateTimeFormatter.ofPattern("HH:mm", Locale.ROOT));

    private static final DateTimeFormatter STRICT_ORDINAL_DATE_TIME_FORMATTER_BASE = new DateTimeFormatterBuilder()
        .appendValue(ChronoField.YEAR, 4, 10, SignStyle.EXCEEDS_PAD)
        .appendLiteral('-')
        .appendValue(DAY_OF_YEAR, 3, 3, SignStyle.NOT_NEGATIVE)
        .appendLiteral('T')
        .appendPattern("HH:mm")
        .optionalStart()
        .appendLiteral(':')
        .appendValue(SECOND_OF_MINUTE, 2, 2, SignStyle.NOT_NEGATIVE)
        .appendFraction(MILLI_OF_SECOND, 3, 3, true)
        .optionalEnd()
        .toFormatter(Locale.ROOT);

    /*
     * Returns a formatter for a full ordinal date and time, using a four
     * digit year and three digit dayOfYear (yyyy-DDD'T'HH:mm:ss.SSSZZ).
     */
    private static final DateFormatter STRICT_ORDINAL_DATE_TIME = new JavaDateFormatter("strict_ordinal_date_time",
        new DateTimeFormatterBuilder().append(STRICT_ORDINAL_DATE_TIME_FORMATTER_BASE)
            .appendZoneOrOffsetId().toFormatter(Locale.ROOT),
        new DateTimeFormatterBuilder().append(STRICT_ORDINAL_DATE_TIME_FORMATTER_BASE)
            .append(TIME_ZONE_FORMATTER_NO_COLON).toFormatter(Locale.ROOT)
    );

    // Note: milliseconds parsing is not strict, others are
    private static final DateTimeFormatter STRICT_TIME_FORMATTER_BASE = new DateTimeFormatterBuilder()
        .appendValue(HOUR_OF_DAY, 2, 2, SignStyle.NOT_NEGATIVE)
        .appendLiteral(':')
        .appendValue(MINUTE_OF_HOUR, 2, 2, SignStyle.NOT_NEGATIVE)
        .appendLiteral(':')
        .appendValue(SECOND_OF_MINUTE, 2, 2, SignStyle.NOT_NEGATIVE)
        .appendFraction(MILLI_OF_SECOND, 1, 3, true)
        .toFormatter(Locale.ROOT);

    private static final DateTimeFormatter STRICT_TIME_PRINTER = new DateTimeFormatterBuilder()
        .appendValue(HOUR_OF_DAY, 2, 2, SignStyle.NOT_NEGATIVE)
        .appendLiteral(':')
        .appendValue(MINUTE_OF_HOUR, 2, 2, SignStyle.NOT_NEGATIVE)
        .appendLiteral(':')
        .appendValue(SECOND_OF_MINUTE, 2, 2, SignStyle.NOT_NEGATIVE)
        .appendFraction(MILLI_OF_SECOND, 3, 3, true)
        .toFormatter(Locale.ROOT);

    /*
     * Returns a formatter for a two digit hour of day, two digit minute of
     * hour, two digit second of minute, three digit fraction of second, and
     * time zone offset (HH:mm:ss.SSSZZ).
     */
    private static final DateFormatter STRICT_TIME = new JavaDateFormatter("strict_time",
        new DateTimeFormatterBuilder().append(STRICT_TIME_PRINTER).appendZoneOrOffsetId().toFormatter(Locale.ROOT),
        new DateTimeFormatterBuilder().append(STRICT_TIME_FORMATTER_BASE).appendZoneOrOffsetId().toFormatter(Locale.ROOT),
        new DateTimeFormatterBuilder().append(STRICT_TIME_FORMATTER_BASE).append(TIME_ZONE_FORMATTER_NO_COLON).toFormatter(Locale.ROOT)
    );

    /*
     * Returns a formatter for a two digit hour of day, two digit minute of
     * hour, two digit second of minute, three digit fraction of second, and
     * time zone offset prefixed by 'T' ('T'HH:mm:ss.SSSZZ).
     */
    private static final DateFormatter STRICT_T_TIME = new JavaDateFormatter("strict_t_time",
        new DateTimeFormatterBuilder().appendLiteral('T').append(STRICT_TIME_PRINTER).appendZoneOrOffsetId().toFormatter(Locale.ROOT),
        new DateTimeFormatterBuilder().appendLiteral('T').append(STRICT_TIME_FORMATTER_BASE)
            .appendZoneOrOffsetId().toFormatter(Locale.ROOT),
        new DateTimeFormatterBuilder().appendLiteral('T').append(STRICT_TIME_FORMATTER_BASE)
            .append(TIME_ZONE_FORMATTER_NO_COLON).toFormatter(Locale.ROOT)
    );

    private static final DateTimeFormatter STRICT_TIME_NO_MILLIS_BASE = new DateTimeFormatterBuilder()
        .appendValue(HOUR_OF_DAY, 2, 2, SignStyle.NOT_NEGATIVE)
        .appendLiteral(':')
        .appendValue(MINUTE_OF_HOUR, 2, 2, SignStyle.NOT_NEGATIVE)
        .appendLiteral(':')
        .appendValue(SECOND_OF_MINUTE, 2, 2, SignStyle.NOT_NEGATIVE)
        .toFormatter(Locale.ROOT);

    /*
     * Returns a formatter for a two digit hour of day, two digit minute of
     * hour, two digit second of minute, and time zone offset (HH:mm:ssZZ).
     */
    private static final DateFormatter STRICT_TIME_NO_MILLIS = new JavaDateFormatter("strict_time_no_millis",
        new DateTimeFormatterBuilder().append(STRICT_TIME_NO_MILLIS_BASE).appendZoneOrOffsetId().toFormatter(Locale.ROOT),
        new DateTimeFormatterBuilder().append(STRICT_TIME_NO_MILLIS_BASE).appendZoneOrOffsetId().toFormatter(Locale.ROOT),
        new DateTimeFormatterBuilder().append(STRICT_TIME_NO_MILLIS_BASE).append(TIME_ZONE_FORMATTER_NO_COLON).toFormatter(Locale.ROOT)
    );

    /*
     * Returns a formatter for a two digit hour of day, two digit minute of
     * hour, two digit second of minute, and time zone offset prefixed
     * by 'T' ('T'HH:mm:ssZZ).
     */
    private static final DateFormatter STRICT_T_TIME_NO_MILLIS = new JavaDateFormatter("strict_t_time_no_millis",
        new DateTimeFormatterBuilder().appendLiteral("T").append(STRICT_TIME_NO_MILLIS_BASE)
            .appendZoneOrOffsetId().toFormatter(Locale.ROOT),
        new DateTimeFormatterBuilder().appendLiteral("T").append(STRICT_TIME_NO_MILLIS_BASE)
            .appendZoneOrOffsetId().toFormatter(Locale.ROOT),
        new DateTimeFormatterBuilder().appendLiteral("T").append(STRICT_TIME_NO_MILLIS_BASE)
            .append(TIME_ZONE_FORMATTER_NO_COLON).toFormatter(Locale.ROOT)
    );

    private static final DateTimeFormatter ISO_WEEK_DATE = new DateTimeFormatterBuilder()
            .parseCaseInsensitive()
            .appendValue(IsoFields.WEEK_BASED_YEAR, 4, 10, SignStyle.EXCEEDS_PAD)
            .appendLiteral("-W")
            .appendValue(IsoFields.WEEK_OF_WEEK_BASED_YEAR, 2)
            .appendLiteral('-')
            .appendValue(DAY_OF_WEEK, 1)
            .toFormatter(Locale.ROOT);

    private static final DateTimeFormatter ISO_WEEK_DATE_T = new DateTimeFormatterBuilder()
            .append(ISO_WEEK_DATE)
            .appendLiteral('T')
            .toFormatter(Locale.ROOT);

    /*
     * Returns a formatter for a full date as four digit weekyear, two digit
     * week of weekyear, and one digit day of week (xxxx-'W'ww-e).
     */
    private static final DateFormatter STRICT_WEEK_DATE = new JavaDateFormatter("strict_week_date", ISO_WEEK_DATE);

    /*
     * Returns a formatter that combines a full weekyear date and time without millis,
     * separated by a 'T' (xxxx-'W'ww-e'T'HH:mm:ssZZ).
     */
    private static final DateFormatter STRICT_WEEK_DATE_TIME_NO_MILLIS = new JavaDateFormatter("strict_week_date_time_no_millis",
        new DateTimeFormatterBuilder().append(ISO_WEEK_DATE_T)
            .append(STRICT_TIME_NO_MILLIS_BASE).appendZoneOrOffsetId().toFormatter(Locale.ROOT),
        new DateTimeFormatterBuilder().append(ISO_WEEK_DATE_T)
            .append(STRICT_TIME_NO_MILLIS_BASE).appendZoneOrOffsetId().toFormatter(Locale.ROOT),
        new DateTimeFormatterBuilder().append(ISO_WEEK_DATE_T)
            .append(STRICT_TIME_NO_MILLIS_BASE).append(TIME_ZONE_FORMATTER_NO_COLON).toFormatter(Locale.ROOT)
    );

    /*
     * Returns a formatter that combines a full weekyear date and time,
     * separated by a 'T' (xxxx-'W'ww-e'T'HH:mm:ss.SSSZZ).
     */
    private static final DateFormatter STRICT_WEEK_DATE_TIME = new JavaDateFormatter("strict_week_date_time",
        new DateTimeFormatterBuilder().append(ISO_WEEK_DATE_T).append(STRICT_TIME_PRINTER).appendZoneOrOffsetId().toFormatter(Locale.ROOT),
        new DateTimeFormatterBuilder().append(ISO_WEEK_DATE_T).append(STRICT_TIME_FORMATTER_BASE)
            .appendZoneOrOffsetId().toFormatter(Locale.ROOT),
        new DateTimeFormatterBuilder().append(ISO_WEEK_DATE_T).append(STRICT_TIME_FORMATTER_BASE)
            .append(TIME_ZONE_FORMATTER_NO_COLON).toFormatter(Locale.ROOT)
    );

    /*
     * Returns a formatter for a four digit weekyear
     */
    private static final DateFormatter STRICT_WEEKYEAR = new JavaDateFormatter("strict_weekyear", new DateTimeFormatterBuilder()
        .appendValue(WeekFields.ISO.weekBasedYear(), 4, 10, SignStyle.EXCEEDS_PAD)
        .toFormatter(Locale.ROOT));

    private static final DateTimeFormatter STRICT_WEEKYEAR_WEEK_FORMATTER = new DateTimeFormatterBuilder()
        .appendValue(WeekFields.ISO.weekBasedYear(), 4, 10, SignStyle.EXCEEDS_PAD)
        .appendLiteral("-W")
        .appendValue(WeekFields.ISO.weekOfWeekBasedYear(), 2, 2, SignStyle.NOT_NEGATIVE)
        .toFormatter(Locale.ROOT);

    /*
     * Returns a formatter for a four digit weekyear and two digit week of
     * weekyear. (xxxx-'W'ww)
     */
    private static final DateFormatter STRICT_WEEKYEAR_WEEK =
        new JavaDateFormatter("strict_weekyear_week", STRICT_WEEKYEAR_WEEK_FORMATTER);

    /*
     * Returns a formatter for a four digit weekyear, two digit week of
     * weekyear, and one digit day of week. (xxxx-'W'ww-e)
     */
    private static final DateFormatter STRICT_WEEKYEAR_WEEK_DAY = new JavaDateFormatter("strict_weekyear_week_day",
        new DateTimeFormatterBuilder()
        .append(STRICT_WEEKYEAR_WEEK_FORMATTER)
        .appendLiteral("-")
        .appendValue(WeekFields.ISO.dayOfWeek())
        .toFormatter(Locale.ROOT));

    /*
     * Returns a formatter that combines a full date, two digit hour of day,
     * two digit minute of hour, and two digit second of
     * minute. (yyyy-MM-dd'T'HH:mm:ss)
     */
    private static final DateFormatter STRICT_DATE_HOUR_MINUTE_SECOND = new JavaDateFormatter("strict_date_hour_minute_second",
        DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss", Locale.ROOT));

    /*
     * A basic formatter for a full date as four digit year, two digit
     * month of year, and two digit day of month (yyyyMMdd).
     */
    private static final DateFormatter BASIC_DATE = new JavaDateFormatter("basic_date",
        new DateTimeFormatterBuilder()
            .appendValue(ChronoField.YEAR, 4, 4, SignStyle.NORMAL)
            .appendValue(MONTH_OF_YEAR, 2, 2, SignStyle.NOT_NEGATIVE)
            .appendValue(DAY_OF_MONTH, 2, 2, SignStyle.NOT_NEGATIVE)
            .toFormatter(Locale.ROOT).withZone(ZoneOffset.UTC),
        new DateTimeFormatterBuilder()
            .appendValue(ChronoField.YEAR, 1, 4, SignStyle.NORMAL)
            .appendValue(MONTH_OF_YEAR, 1, 2, SignStyle.NOT_NEGATIVE)
            .appendValue(DAY_OF_MONTH, 1, 2, SignStyle.NOT_NEGATIVE)
            .toFormatter(Locale.ROOT).withZone(ZoneOffset.UTC)
    );

    private static final DateTimeFormatter STRICT_ORDINAL_DATE_FORMATTER = new DateTimeFormatterBuilder()
        .parseCaseInsensitive()
        .appendValue(ChronoField.YEAR, 4, 10, SignStyle.EXCEEDS_PAD)
        .appendLiteral('-')
        .appendValue(DAY_OF_YEAR, 3)
        .optionalStart()
        .toFormatter(Locale.ROOT);

    /*
     * Returns a formatter for a full ordinal date, using a four
     * digit year and three digit dayOfYear (yyyy-DDD).
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

    private static final DateTimeFormatter DATE_FORMATTER = new DateTimeFormatterBuilder()
        .appendValue(ChronoField.YEAR, 1, 5, SignStyle.NORMAL)
        .optionalStart()
        .appendLiteral('-')
        .appendValue(MONTH_OF_YEAR, 1, 2, SignStyle.NOT_NEGATIVE)
        .optionalStart()
        .appendLiteral('-')
        .appendValue(DAY_OF_MONTH, 1, 2, SignStyle.NOT_NEGATIVE)
        .optionalEnd()
        .optionalEnd()
        .toFormatter(Locale.ROOT);

    private static final DateTimeFormatter HOUR_MINUTE_FORMATTER = new DateTimeFormatterBuilder()
        .appendValue(HOUR_OF_DAY, 1, 2, SignStyle.NOT_NEGATIVE)
        .appendLiteral(':')
        .appendValue(MINUTE_OF_HOUR, 1, 2, SignStyle.NOT_NEGATIVE)
        .toFormatter(Locale.ROOT);

    /*
     * a date formatter with optional time, being very lenient, format is
     * yyyy-MM-dd'T'HH:mm:ss.SSSZ
     */
    private static final DateFormatter DATE_OPTIONAL_TIME = new JavaDateFormatter("date_optional_time",
        STRICT_DATE_OPTIONAL_TIME_FORMATTER_1,
        new DateTimeFormatterBuilder()
            .append(DATE_FORMATTER)
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
            .appendFraction(MILLI_OF_SECOND, 1, 3, true)
            .optionalEnd()
            .optionalStart().appendZoneOrOffsetId().optionalEnd()
            .optionalEnd()
            .optionalEnd()
            .optionalEnd()
            .toFormatter(Locale.ROOT),
        new DateTimeFormatterBuilder()
            .append(DATE_FORMATTER)
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
            .appendFraction(MILLI_OF_SECOND, 1, 3, true)
            .optionalEnd()
            .optionalStart().appendOffset("+HHmm", "Z").optionalEnd()
            .optionalEnd()
            .optionalEnd()
            .optionalEnd()
            .toFormatter(Locale.ROOT));

    private static final DateTimeFormatter HOUR_MINUTE_SECOND_FORMATTER = new DateTimeFormatterBuilder()
        .append(HOUR_MINUTE_FORMATTER)
        .appendLiteral(":")
        .appendValue(SECOND_OF_MINUTE, 1, 2, SignStyle.NOT_NEGATIVE)
        .toFormatter(Locale.ROOT);

    private static final DateTimeFormatter HOUR_MINUTE_SECOND_MILLIS_FORMATTER = new DateTimeFormatterBuilder()
        .appendValue(HOUR_OF_DAY, 1, 2, SignStyle.NOT_NEGATIVE)
        .appendLiteral(':')
        .appendValue(MINUTE_OF_HOUR, 1, 2, SignStyle.NOT_NEGATIVE)
        .appendLiteral(':')
        .appendValue(SECOND_OF_MINUTE, 1, 2, SignStyle.NOT_NEGATIVE)
        .appendFraction(MILLI_OF_SECOND, 1, 3, true)
        .toFormatter(Locale.ROOT);

    private static final DateTimeFormatter ORDINAL_DATE_FORMATTER = new DateTimeFormatterBuilder()
        .appendValue(ChronoField.YEAR, 4, 10, SignStyle.EXCEEDS_PAD)
        .appendLiteral('-')
        .appendValue(DAY_OF_YEAR, 1, 3, SignStyle.NOT_NEGATIVE)
        .toFormatter(Locale.ROOT);

    private static final DateTimeFormatter ORDINAL_DATE_PRINTER = new DateTimeFormatterBuilder()
        .appendValue(ChronoField.YEAR, 4, 10, SignStyle.EXCEEDS_PAD)
        .appendLiteral('-')
        .appendValue(DAY_OF_YEAR, 3, 3, SignStyle.NOT_NEGATIVE)
        .toFormatter(Locale.ROOT);

    /*
     * Returns a formatter for a full ordinal date, using a four
     * digit year and three digit dayOfYear (yyyy-DDD).
     */
    private static final DateFormatter ORDINAL_DATE =
        new JavaDateFormatter("ordinal_date", ORDINAL_DATE_PRINTER, ORDINAL_DATE_FORMATTER);

    private static final DateTimeFormatter TIME_NO_MILLIS_FORMATTER = new DateTimeFormatterBuilder()
        .appendValue(HOUR_OF_DAY, 1, 2, SignStyle.NOT_NEGATIVE)
        .appendLiteral(':')
        .appendValue(MINUTE_OF_HOUR, 1, 2, SignStyle.NOT_NEGATIVE)
        .appendLiteral(':')
        .appendValue(SECOND_OF_MINUTE, 1, 2, SignStyle.NOT_NEGATIVE)
        .toFormatter(Locale.ROOT);

    private static final DateTimeFormatter T_TIME_NO_MILLIS_FORMATTER =
        new DateTimeFormatterBuilder().appendLiteral("T").append(TIME_NO_MILLIS_FORMATTER).toFormatter(Locale.ROOT);

    private static final DateTimeFormatter TIME_PREFIX = new DateTimeFormatterBuilder()
        .append(TIME_NO_MILLIS_FORMATTER)
        .appendFraction(MILLI_OF_SECOND, 1, 3, true)
        .toFormatter(Locale.ROOT);

    private static final DateTimeFormatter WEEK_DATE_FORMATTER = new DateTimeFormatterBuilder()
        .appendValue(IsoFields.WEEK_BASED_YEAR, 4, 10, SignStyle.EXCEEDS_PAD)
        .appendLiteral("-W")
        .appendValue(IsoFields.WEEK_OF_WEEK_BASED_YEAR, 1, 2, SignStyle.NOT_NEGATIVE)
        .appendLiteral('-')
        .appendValue(DAY_OF_WEEK, 1)
        .toFormatter(Locale.ROOT);

    /*
     * Returns a formatter for a four digit weekyear. (YYYY)
     */
    private static final DateFormatter WEEK_YEAR = new JavaDateFormatter("week_year",
        new DateTimeFormatterBuilder().appendValue(WeekFields.ISO.weekBasedYear()).toFormatter(Locale.ROOT));

    /*
     * Returns a formatter for a four digit weekyear. (uuuu)
     */
    private static final DateFormatter YEAR = new JavaDateFormatter("year",
        new DateTimeFormatterBuilder().appendValue(ChronoField.YEAR).toFormatter(Locale.ROOT));

    /*
     * Returns a formatter that combines a full date and two digit hour of
     * day. (yyyy-MM-dd'T'HH)
     */
    private static final DateFormatter DATE_HOUR = new JavaDateFormatter("date_hour",
        DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH", Locale.ROOT),
        new DateTimeFormatterBuilder()
            .append(DATE_FORMATTER)
            .appendLiteral("T")
            .appendValue(HOUR_OF_DAY, 1, 2, SignStyle.NOT_NEGATIVE)
            .toFormatter(Locale.ROOT));

    /*
     * Returns a formatter that combines a full date, two digit hour of day,
     * two digit minute of hour, two digit second of minute, and three digit
     * fraction of second (yyyy-MM-dd'T'HH:mm:ss.SSS). Parsing will parse up
     * to 3 fractional second digits.
     */
    private static final DateFormatter DATE_HOUR_MINUTE_SECOND_MILLIS =
        new JavaDateFormatter("date_hour_minute_second_millis",
            new DateTimeFormatterBuilder()
                .append(STRICT_YEAR_MONTH_DAY_FORMATTER)
                .appendLiteral("T")
                .append(STRICT_HOUR_MINUTE_SECOND_MILLIS_PRINTER)
                .toFormatter(Locale.ROOT),
            new DateTimeFormatterBuilder()
                .append(DATE_FORMATTER)
                .appendLiteral("T")
                .append(HOUR_MINUTE_SECOND_MILLIS_FORMATTER)
                .toFormatter(Locale.ROOT));

    private static final DateFormatter DATE_HOUR_MINUTE_SECOND_FRACTION = DATE_HOUR_MINUTE_SECOND_MILLIS;

    /*
     * Returns a formatter that combines a full date, two digit hour of day,
     * and two digit minute of hour. (yyyy-MM-dd'T'HH:mm)
     */
    private static final DateFormatter DATE_HOUR_MINUTE = new JavaDateFormatter("date_hour_minute",
        DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm", Locale.ROOT),
        new DateTimeFormatterBuilder()
            .append(DATE_FORMATTER)
            .appendLiteral("T")
            .append(HOUR_MINUTE_FORMATTER)
            .toFormatter(Locale.ROOT));

    /*
     * Returns a formatter that combines a full date, two digit hour of day,
     * two digit minute of hour, and two digit second of
     * minute. (yyyy-MM-dd'T'HH:mm:ss)
     */
    private static final DateFormatter DATE_HOUR_MINUTE_SECOND = new JavaDateFormatter("date_hour_minute_second",
        DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss", Locale.ROOT),
        new DateTimeFormatterBuilder()
            .append(DATE_FORMATTER)
            .appendLiteral("T")
            .append(HOUR_MINUTE_SECOND_FORMATTER)
            .toFormatter(Locale.ROOT));

    private static final DateTimeFormatter DATE_TIME_FORMATTER = new DateTimeFormatterBuilder()
        .append(DATE_FORMATTER)
        .appendLiteral('T')
        .append(HOUR_MINUTE_FORMATTER)
        .optionalStart()
        .appendLiteral(':')
        .appendValue(SECOND_OF_MINUTE, 1, 2, SignStyle.NOT_NEGATIVE)
        .appendFraction(MILLI_OF_SECOND, 1, 3, true)
        .optionalEnd()
        .toFormatter(Locale.ROOT);

    /*
     * Returns a formatter that combines a full date and time, separated by a 'T'
     * (yyyy-MM-dd'T'HH:mm:ss.SSSZZ).
     */
    private static final DateFormatter DATE_TIME = new JavaDateFormatter("date_time",
        STRICT_DATE_OPTIONAL_TIME_FORMATTER_1,
        new DateTimeFormatterBuilder().append(DATE_TIME_FORMATTER).appendZoneOrOffsetId().toFormatter(Locale.ROOT),
        new DateTimeFormatterBuilder().append(DATE_TIME_FORMATTER).append(TIME_ZONE_FORMATTER_NO_COLON).toFormatter(Locale.ROOT)
    );

    /*
     * Returns a basic formatter for a full date as four digit weekyear, two
     * digit week of weekyear, and one digit day of week (YYYY'W'wwe).
     */
    private static final DateFormatter BASIC_WEEK_DATE =
        new JavaDateFormatter("basic_week_date", STRICT_BASIC_WEEK_DATE_PRINTER, BASIC_WEEK_DATE_FORMATTER);

    /*
     * Returns a formatter for a full date as four digit year, two digit month
     * of year, and two digit day of month (yyyy-MM-dd).
     */
    private static final DateFormatter DATE = new JavaDateFormatter("date",
        DateTimeFormatter.ISO_LOCAL_DATE.withResolverStyle(ResolverStyle.LENIENT),
        DATE_FORMATTER);

    // only the formatter, nothing optional here
    private static final DateTimeFormatter DATE_TIME_NO_MILLIS_PRINTER = new DateTimeFormatterBuilder()
        .append(DateTimeFormatter.ISO_LOCAL_DATE.withResolverStyle(ResolverStyle.LENIENT))
        .appendLiteral('T')
        .appendPattern("HH:mm")
        .appendLiteral(':')
        .appendValue(SECOND_OF_MINUTE, 2, 2, SignStyle.NOT_NEGATIVE)
        .appendZoneId()
        .toFormatter(Locale.ROOT);

    private static final DateTimeFormatter DATE_TIME_PREFIX = new DateTimeFormatterBuilder()
        .append(DATE_FORMATTER)
        .appendLiteral('T')
        .append(HOUR_MINUTE_FORMATTER)
        .optionalStart()
        .appendLiteral(':')
        .appendValue(SECOND_OF_MINUTE, 1, 2, SignStyle.NOT_NEGATIVE)
        .optionalEnd()
        .toFormatter(Locale.ROOT);

    /*
     * Returns a formatter that combines a full date and time without millis, but with a timezone that can be optional
     * separated by a 'T' (yyyy-MM-dd'T'HH:mm:ssZ).
     */
    private static final DateFormatter DATE_TIME_NO_MILLIS = new JavaDateFormatter("date_time_no_millis",
        DATE_TIME_NO_MILLIS_PRINTER,
        new DateTimeFormatterBuilder().append(DATE_TIME_PREFIX).appendZoneOrOffsetId().toFormatter(Locale.ROOT),
        new DateTimeFormatterBuilder().append(DATE_TIME_PREFIX).append(TIME_ZONE_FORMATTER_NO_COLON).toFormatter(Locale.ROOT),
        new DateTimeFormatterBuilder().append(DATE_TIME_PREFIX)
            .optionalStart().appendZoneOrOffsetId().optionalEnd().toFormatter(Locale.ROOT),
        new DateTimeFormatterBuilder().append(DATE_TIME_PREFIX)
            .optionalStart().append(TIME_ZONE_FORMATTER_NO_COLON).optionalEnd().toFormatter(Locale.ROOT)
    );

    /*
     * Returns a formatter for a two digit hour of day, two digit minute of
     * hour, two digit second of minute, and three digit fraction of
     * second (HH:mm:ss.SSS).
     */
    private static final DateFormatter HOUR_MINUTE_SECOND_MILLIS = new JavaDateFormatter("hour_minute_second_millis",
        STRICT_HOUR_MINUTE_SECOND_MILLIS_PRINTER, HOUR_MINUTE_SECOND_MILLIS_FORMATTER);

    /*
     * Returns a formatter for a two digit hour of day and two digit minute of
     * hour. (HH:mm)
     */
    private static final DateFormatter HOUR_MINUTE =
        new JavaDateFormatter("hour_minute", DateTimeFormatter.ofPattern("HH:mm", Locale.ROOT), HOUR_MINUTE_FORMATTER);

    /*
     * A strict formatter that formats or parses a hour, minute and second, such as '09:43:25'.
     */
    private static final DateFormatter HOUR_MINUTE_SECOND = new JavaDateFormatter("hour_minute_second",
        STRICT_HOUR_MINUTE_SECOND_FORMATTER,
        new DateTimeFormatterBuilder()
            .append(HOUR_MINUTE_FORMATTER)
            .appendLiteral(":")
            .appendValue(SECOND_OF_MINUTE, 1, 2, SignStyle.NOT_NEGATIVE)
            .toFormatter(Locale.ROOT)
    );

    /*
     * Returns a formatter for a two digit hour of day. (HH)
     */
    private static final DateFormatter HOUR = new JavaDateFormatter("hour",
        DateTimeFormatter.ofPattern("HH", Locale.ROOT),
        new DateTimeFormatterBuilder().appendValue(HOUR_OF_DAY, 1, 2, SignStyle.NOT_NEGATIVE).toFormatter(Locale.ROOT)
    );

    private static final DateTimeFormatter ORDINAL_DATE_TIME_FORMATTER_BASE = new DateTimeFormatterBuilder()
        .append(ORDINAL_DATE_FORMATTER)
        .appendLiteral('T')
        .append(HOUR_MINUTE_FORMATTER)
        .optionalStart()
        .appendLiteral(':')
        .appendValue(SECOND_OF_MINUTE, 1, 2, SignStyle.NOT_NEGATIVE)
        .appendFraction(MILLI_OF_SECOND, 1, 3, true)
        .optionalEnd()
        .toFormatter(Locale.ROOT);

    /*
     * Returns a formatter for a full ordinal date and time, using a four
     * digit year and three digit dayOfYear (yyyy-DDD'T'HH:mm:ss.SSSZZ).
     */
    private static final DateFormatter ORDINAL_DATE_TIME = new JavaDateFormatter("ordinal_date_time",
        new DateTimeFormatterBuilder().append(STRICT_ORDINAL_DATE_TIME_FORMATTER_BASE)
            .appendZoneOrOffsetId().toFormatter(Locale.ROOT),
        new DateTimeFormatterBuilder().append(ORDINAL_DATE_TIME_FORMATTER_BASE)
            .appendZoneOrOffsetId().toFormatter(Locale.ROOT),
        new DateTimeFormatterBuilder().append(ORDINAL_DATE_TIME_FORMATTER_BASE)
            .append(TIME_ZONE_FORMATTER_NO_COLON).toFormatter(Locale.ROOT)
    );

    private static final DateTimeFormatter ORDINAL_DATE_TIME_NO_MILLIS_BASE = new DateTimeFormatterBuilder()
        .append(ORDINAL_DATE_FORMATTER)
        .appendLiteral('T')
        .append(HOUR_MINUTE_SECOND_FORMATTER)
        .toFormatter(Locale.ROOT);

    /*
     * Returns a formatter for a full ordinal date and time without millis,
     * using a four digit year and three digit dayOfYear (yyyy-DDD'T'HH:mm:ssZZ).
     */
    private static final DateFormatter ORDINAL_DATE_TIME_NO_MILLIS = new JavaDateFormatter("ordinal_date_time_no_millis",
        new DateTimeFormatterBuilder().append(STRICT_ORDINAL_DATE_TIME_NO_MILLIS_BASE)
            .appendZoneOrOffsetId().toFormatter(Locale.ROOT),
        new DateTimeFormatterBuilder().append(ORDINAL_DATE_TIME_NO_MILLIS_BASE)
            .appendZoneOrOffsetId().toFormatter(Locale.ROOT),
        new DateTimeFormatterBuilder().append(ORDINAL_DATE_TIME_NO_MILLIS_BASE)
            .append(TIME_ZONE_FORMATTER_NO_COLON).toFormatter(Locale.ROOT)
    );

    /*
     * Returns a formatter that combines a full weekyear date and time,
     * separated by a 'T' (xxxx-'W'ww-e'T'HH:mm:ss.SSSZZ).
     */
    private static final DateFormatter WEEK_DATE_TIME = new JavaDateFormatter("week_date_time",
        new DateTimeFormatterBuilder().append(ISO_WEEK_DATE_T).append(STRICT_TIME_PRINTER).appendZoneOrOffsetId().toFormatter(Locale.ROOT),
        new DateTimeFormatterBuilder().append(WEEK_DATE_FORMATTER).appendLiteral("T").append(TIME_PREFIX)
            .appendZoneOrOffsetId().toFormatter(Locale.ROOT),
        new DateTimeFormatterBuilder().append(WEEK_DATE_FORMATTER).appendLiteral("T").append(TIME_PREFIX)
            .append(TIME_ZONE_FORMATTER_NO_COLON).toFormatter(Locale.ROOT)
    );

    /*
     * Returns a formatter that combines a full weekyear date and time,
     * separated by a 'T' (xxxx-'W'ww-e'T'HH:mm:ssZZ).
     */
    private static final DateFormatter WEEK_DATE_TIME_NO_MILLIS = new JavaDateFormatter("week_date_time_no_millis",
        new DateTimeFormatterBuilder().append(ISO_WEEK_DATE_T)
            .append(STRICT_TIME_NO_MILLIS_BASE).appendZoneOrOffsetId().toFormatter(Locale.ROOT),
        new DateTimeFormatterBuilder().append(WEEK_DATE_FORMATTER).append(T_TIME_NO_MILLIS_FORMATTER)
            .appendZoneOrOffsetId().toFormatter(Locale.ROOT),
        new DateTimeFormatterBuilder().append(WEEK_DATE_FORMATTER).append(T_TIME_NO_MILLIS_FORMATTER)
            .append(TIME_ZONE_FORMATTER_NO_COLON).toFormatter(Locale.ROOT)
    );

    /*
     * Returns a basic formatter that combines a basic weekyear date and time,
     * separated by a 'T' (xxxx'W'wwe'T'HHmmss.SSSX).
     */
    private static final DateFormatter BASIC_WEEK_DATE_TIME = new JavaDateFormatter("basic_week_date_time",
        new DateTimeFormatterBuilder()
            .append(STRICT_BASIC_WEEK_DATE_PRINTER)
            .append(DateTimeFormatter.ofPattern("'T'HHmmss.SSSX", Locale.ROOT))
            .toFormatter(Locale.ROOT),
        new DateTimeFormatterBuilder().append(BASIC_WEEK_DATE_FORMATTER).append(BASIC_T_TIME_FORMATTER)
            .appendZoneOrOffsetId().toFormatter(Locale.ROOT),
        new DateTimeFormatterBuilder().append(BASIC_WEEK_DATE_FORMATTER).append(BASIC_T_TIME_FORMATTER)
            .append(TIME_ZONE_FORMATTER_NO_COLON).toFormatter(Locale.ROOT)
    );

    /*
     * Returns a basic formatter that combines a basic weekyear date and time,
     * separated by a 'T' (xxxx'W'wwe'T'HHmmssX).
     */
    private static final DateFormatter BASIC_WEEK_DATE_TIME_NO_MILLIS = new JavaDateFormatter("basic_week_date_time_no_millis",
        new DateTimeFormatterBuilder()
            .append(STRICT_BASIC_WEEK_DATE_PRINTER).append(DateTimeFormatter.ofPattern("'T'HHmmssX", Locale.ROOT))
            .toFormatter(Locale.ROOT),
        new DateTimeFormatterBuilder().append(BASIC_WEEK_DATE_FORMATTER).appendLiteral("T").append(BASIC_TIME_NO_MILLIS_BASE)
            .appendZoneOrOffsetId().toFormatter(Locale.ROOT),
        new DateTimeFormatterBuilder().append(BASIC_WEEK_DATE_FORMATTER).appendLiteral("T").append(BASIC_TIME_NO_MILLIS_BASE)
            .append(TIME_ZONE_FORMATTER_NO_COLON).toFormatter(Locale.ROOT)
    );

    /*
     * Returns a formatter for a two digit hour of day, two digit minute of
     * hour, two digit second of minute, three digit fraction of second, and
     * time zone offset (HH:mm:ss.SSSZZ).
     */
    private static final DateFormatter TIME = new JavaDateFormatter("time",
        new DateTimeFormatterBuilder().append(STRICT_TIME_PRINTER).appendZoneOrOffsetId().toFormatter(Locale.ROOT),
        new DateTimeFormatterBuilder().append(TIME_PREFIX).appendZoneOrOffsetId().toFormatter(Locale.ROOT),
        new DateTimeFormatterBuilder().append(TIME_PREFIX).append(TIME_ZONE_FORMATTER_NO_COLON).toFormatter(Locale.ROOT)
    );

    /*
     * Returns a formatter for a two digit hour of day, two digit minute of
     * hour, two digit second of minute, andtime zone offset (HH:mm:ssZZ).
     */
    private static final DateFormatter TIME_NO_MILLIS = new JavaDateFormatter("time_no_millis",
        new DateTimeFormatterBuilder().append(STRICT_TIME_NO_MILLIS_BASE).appendZoneOrOffsetId().toFormatter(Locale.ROOT),
        new DateTimeFormatterBuilder().append(TIME_NO_MILLIS_FORMATTER).appendZoneOrOffsetId().toFormatter(Locale.ROOT),
        new DateTimeFormatterBuilder().append(TIME_NO_MILLIS_FORMATTER).append(TIME_ZONE_FORMATTER_NO_COLON).toFormatter(Locale.ROOT)
    );

    /*
     * Returns a formatter for a two digit hour of day, two digit minute of
     * hour, two digit second of minute, three digit fraction of second, and
     * time zone offset prefixed by 'T' ('T'HH:mm:ss.SSSZZ).
     */
    private static final DateFormatter T_TIME = new JavaDateFormatter("t_time",
        new DateTimeFormatterBuilder().appendLiteral('T').append(STRICT_TIME_PRINTER).appendZoneOrOffsetId().toFormatter(Locale.ROOT),
        new DateTimeFormatterBuilder().appendLiteral("T").append(TIME_PREFIX)
            .appendZoneOrOffsetId().toFormatter(Locale.ROOT),
        new DateTimeFormatterBuilder().appendLiteral("T").append(TIME_PREFIX)
            .append(TIME_ZONE_FORMATTER_NO_COLON).toFormatter(Locale.ROOT)
    );

    /*
     * Returns a formatter for a two digit hour of day, two digit minute of
     * hour, two digit second of minute, and time zone offset prefixed
     * by 'T' ('T'HH:mm:ssZZ).
     */
    private static final DateFormatter T_TIME_NO_MILLIS = new JavaDateFormatter("t_time_no_millis",
        new DateTimeFormatterBuilder().appendLiteral("T").append(STRICT_TIME_NO_MILLIS_BASE)
            .appendZoneOrOffsetId().toFormatter(Locale.ROOT),
        new DateTimeFormatterBuilder().append(T_TIME_NO_MILLIS_FORMATTER).appendZoneOrOffsetId().toFormatter(Locale.ROOT),
        new DateTimeFormatterBuilder().append(T_TIME_NO_MILLIS_FORMATTER).append(TIME_ZONE_FORMATTER_NO_COLON).toFormatter(Locale.ROOT)
    );

    /*
     * A strict formatter that formats or parses a year and a month, such as '2011-12'.
     */
    private static final DateFormatter YEAR_MONTH = new JavaDateFormatter("year_month",
        new DateTimeFormatterBuilder()
            .appendValue(ChronoField.YEAR, 4, 10, SignStyle.EXCEEDS_PAD)
            .appendLiteral("-")
            .appendValue(MONTH_OF_YEAR, 2, 2, SignStyle.NOT_NEGATIVE)
            .toFormatter(Locale.ROOT),
        new DateTimeFormatterBuilder().appendValue(ChronoField.YEAR).appendLiteral("-").appendValue(MONTH_OF_YEAR).toFormatter(Locale.ROOT)
    );

    /*
     * A strict date formatter that formats or parses a date without an offset, such as '2011-12-03'.
     */
    private static final DateFormatter YEAR_MONTH_DAY = new JavaDateFormatter("year_month_day",
        STRICT_YEAR_MONTH_DAY_FORMATTER,
        new DateTimeFormatterBuilder()
            .appendValue(ChronoField.YEAR)
            .appendLiteral("-")
            .appendValue(MONTH_OF_YEAR)
            .appendLiteral("-")
            .appendValue(DAY_OF_MONTH)
            .toFormatter(Locale.ROOT)
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
    private static final DateFormatter WEEKYEAR_WEEK = new JavaDateFormatter("weekyear_week", STRICT_WEEKYEAR_WEEK_FORMATTER,
        new DateTimeFormatterBuilder()
            .appendValue(WeekFields.ISO.weekBasedYear())
            .appendLiteral("-W")
            .appendValue(WeekFields.ISO.weekOfWeekBasedYear())
            .toFormatter(Locale.ROOT)
    );

    /*
     * Returns a formatter for a four digit weekyear, two digit week of
     * weekyear, and one digit day of week. (xxxx-'W'ww-e)
     */
    private static final DateFormatter WEEKYEAR_WEEK_DAY = new JavaDateFormatter("weekyear_week_day",
        new DateTimeFormatterBuilder()
            .append(STRICT_WEEKYEAR_WEEK_FORMATTER)
            .appendLiteral("-")
            .appendValue(WeekFields.ISO.dayOfWeek())
            .toFormatter(Locale.ROOT),
        new DateTimeFormatterBuilder()
            .appendValue(WeekFields.ISO.weekBasedYear())
            .appendLiteral("-W")
            .appendValue(WeekFields.ISO.weekOfWeekBasedYear())
            .appendLiteral("-")
            .appendValue(WeekFields.ISO.dayOfWeek())
            .toFormatter(Locale.ROOT)
    );

    /////////////////////////////////////////
    //
    // end lenient formatters
    //
    /////////////////////////////////////////

    public static DateFormatter forPattern(String input) {
        return forPattern(input, Locale.ROOT);
    }

    private static DateFormatter forPattern(String input, Locale locale) {
        if (Strings.hasLength(input)) {
            input = input.trim();
        }
        if (input == null || input.length() == 0) {
            throw new IllegalArgumentException("No date pattern provided");
        }

        if ("basicDate".equals(input) || "basic_date".equals(input)) {
            return BASIC_DATE;
        } else if ("basicDateTime".equals(input) || "basic_date_time".equals(input)) {
            return BASIC_DATE_TIME;
        } else if ("basicDateTimeNoMillis".equals(input) || "basic_date_time_no_millis".equals(input)) {
            return BASIC_DATE_TIME_NO_MILLIS;
        } else if ("basicOrdinalDate".equals(input) || "basic_ordinal_date".equals(input)) {
            return BASIC_ORDINAL_DATE;
        } else if ("basicOrdinalDateTime".equals(input) || "basic_ordinal_date_time".equals(input)) {
            return BASIC_ORDINAL_DATE_TIME;
        } else if ("basicOrdinalDateTimeNoMillis".equals(input) || "basic_ordinal_date_time_no_millis".equals(input)) {
            return BASIC_ORDINAL_DATE_TIME_NO_MILLIS;
        } else if ("basicTime".equals(input) || "basic_time".equals(input)) {
            return BASIC_TIME;
        } else if ("basicTimeNoMillis".equals(input) || "basic_time_no_millis".equals(input)) {
            return BASIC_TIME_NO_MILLIS;
        } else if ("basicTTime".equals(input) || "basic_t_time".equals(input)) {
            return BASIC_T_TIME;
        } else if ("basicTTimeNoMillis".equals(input) || "basic_t_time_no_millis".equals(input)) {
            return BASIC_T_TIME_NO_MILLIS;
        } else if ("basicWeekDate".equals(input) || "basic_week_date".equals(input)) {
            return BASIC_WEEK_DATE;
        } else if ("basicWeekDateTime".equals(input) || "basic_week_date_time".equals(input)) {
            return BASIC_WEEK_DATE_TIME;
        } else if ("basicWeekDateTimeNoMillis".equals(input) || "basic_week_date_time_no_millis".equals(input)) {
            return BASIC_WEEK_DATE_TIME_NO_MILLIS;
        } else if ("date".equals(input)) {
            return DATE;
        } else if ("dateHour".equals(input) || "date_hour".equals(input)) {
            return DATE_HOUR;
        } else if ("dateHourMinute".equals(input) || "date_hour_minute".equals(input)) {
            return DATE_HOUR_MINUTE;
        } else if ("dateHourMinuteSecond".equals(input) || "date_hour_minute_second".equals(input)) {
            return DATE_HOUR_MINUTE_SECOND;
        } else if ("dateHourMinuteSecondFraction".equals(input) || "date_hour_minute_second_fraction".equals(input)) {
            return DATE_HOUR_MINUTE_SECOND_FRACTION;
        } else if ("dateHourMinuteSecondMillis".equals(input) || "date_hour_minute_second_millis".equals(input)) {
            return DATE_HOUR_MINUTE_SECOND_MILLIS;
        } else if ("dateOptionalTime".equals(input) || "date_optional_time".equals(input)) {
            return DATE_OPTIONAL_TIME;
        } else if ("dateTime".equals(input) || "date_time".equals(input)) {
            return DATE_TIME;
        } else if ("dateTimeNoMillis".equals(input) || "date_time_no_millis".equals(input)) {
            return DATE_TIME_NO_MILLIS;
        } else if ("hour".equals(input)) {
            return HOUR;
        } else if ("hourMinute".equals(input) || "hour_minute".equals(input)) {
            return HOUR_MINUTE;
        } else if ("hourMinuteSecond".equals(input) || "hour_minute_second".equals(input)) {
            return HOUR_MINUTE_SECOND;
        } else if ("hourMinuteSecondFraction".equals(input) || "hour_minute_second_fraction".equals(input)) {
            return HOUR_MINUTE_SECOND_MILLIS;
        } else if ("hourMinuteSecondMillis".equals(input) || "hour_minute_second_millis".equals(input)) {
            return HOUR_MINUTE_SECOND_MILLIS;
        } else if ("ordinalDate".equals(input) || "ordinal_date".equals(input)) {
            return ORDINAL_DATE;
        } else if ("ordinalDateTime".equals(input) || "ordinal_date_time".equals(input)) {
            return ORDINAL_DATE_TIME;
        } else if ("ordinalDateTimeNoMillis".equals(input) || "ordinal_date_time_no_millis".equals(input)) {
            return ORDINAL_DATE_TIME_NO_MILLIS;
        } else if ("time".equals(input)) {
            return TIME;
        } else if ("timeNoMillis".equals(input) || "time_no_millis".equals(input)) {
            return TIME_NO_MILLIS;
        } else if ("tTime".equals(input) || "t_time".equals(input)) {
            return T_TIME;
        } else if ("tTimeNoMillis".equals(input) || "t_time_no_millis".equals(input)) {
            return T_TIME_NO_MILLIS;
        } else if ("weekDate".equals(input) || "week_date".equals(input)) {
            return WEEK_DATE;
        } else if ("weekDateTime".equals(input) || "week_date_time".equals(input)) {
            return WEEK_DATE_TIME;
        } else if ("weekDateTimeNoMillis".equals(input) || "week_date_time_no_millis".equals(input)) {
            return WEEK_DATE_TIME_NO_MILLIS;
        } else if ("weekyear".equals(input) || "week_year".equals(input)) {
            return WEEK_YEAR;
        } else if ("weekyearWeek".equals(input) || "weekyear_week".equals(input)) {
            return WEEKYEAR_WEEK;
        } else if ("weekyearWeekDay".equals(input) || "weekyear_week_day".equals(input)) {
            return WEEKYEAR_WEEK_DAY;
        } else if ("year".equals(input)) {
            return YEAR;
        } else if ("yearMonth".equals(input) || "year_month".equals(input)) {
            return YEAR_MONTH;
        } else if ("yearMonthDay".equals(input) || "year_month_day".equals(input)) {
            return YEAR_MONTH_DAY;
        } else if ("epoch_second".equals(input)) {
            return EpochSecondsDateFormatter.INSTANCE;
        } else if ("epoch_millis".equals(input)) {
            return EpochMillisDateFormatter.INSTANCE;
        // strict date formats here, must be at least 4 digits for year and two for months and two for day
        } else if ("strictBasicWeekDate".equals(input) || "strict_basic_week_date".equals(input)) {
            return STRICT_BASIC_WEEK_DATE;
        } else if ("strictBasicWeekDateTime".equals(input) || "strict_basic_week_date_time".equals(input)) {
            return STRICT_BASIC_WEEK_DATE_TIME;
        } else if ("strictBasicWeekDateTimeNoMillis".equals(input) || "strict_basic_week_date_time_no_millis".equals(input)) {
            return STRICT_BASIC_WEEK_DATE_TIME_NO_MILLIS;
        } else if ("strictDate".equals(input) || "strict_date".equals(input)) {
            return STRICT_DATE;
        } else if ("strictDateHour".equals(input) || "strict_date_hour".equals(input)) {
            return STRICT_DATE_HOUR;
        } else if ("strictDateHourMinute".equals(input) || "strict_date_hour_minute".equals(input)) {
            return STRICT_DATE_HOUR_MINUTE;
        } else if ("strictDateHourMinuteSecond".equals(input) || "strict_date_hour_minute_second".equals(input)) {
            return STRICT_DATE_HOUR_MINUTE_SECOND;
        } else if ("strictDateHourMinuteSecondFraction".equals(input) || "strict_date_hour_minute_second_fraction".equals(input)) {
            return STRICT_DATE_HOUR_MINUTE_SECOND_FRACTION;
        } else if ("strictDateHourMinuteSecondMillis".equals(input) || "strict_date_hour_minute_second_millis".equals(input)) {
            return STRICT_DATE_HOUR_MINUTE_SECOND_MILLIS;
        } else if ("strictDateOptionalTime".equals(input) || "strict_date_optional_time".equals(input)) {
            return STRICT_DATE_OPTIONAL_TIME;
        } else if ("strictDateOptionalTimeNanos".equals(input) || "strict_date_optional_time_nanos".equals(input)) {
            return STRICT_DATE_OPTIONAL_TIME_NANOS;
        } else if ("strictDateTime".equals(input) || "strict_date_time".equals(input)) {
            return STRICT_DATE_TIME;
        } else if ("strictDateTimeNoMillis".equals(input) || "strict_date_time_no_millis".equals(input)) {
            return STRICT_DATE_TIME_NO_MILLIS;
        } else if ("strictHour".equals(input) || "strict_hour".equals(input)) {
            return STRICT_HOUR;
        } else if ("strictHourMinute".equals(input) || "strict_hour_minute".equals(input)) {
            return STRICT_HOUR_MINUTE;
        } else if ("strictHourMinuteSecond".equals(input) || "strict_hour_minute_second".equals(input)) {
            return STRICT_HOUR_MINUTE_SECOND;
        } else if ("strictHourMinuteSecondFraction".equals(input) || "strict_hour_minute_second_fraction".equals(input)) {
            return STRICT_HOUR_MINUTE_SECOND_FRACTION;
        } else if ("strictHourMinuteSecondMillis".equals(input) || "strict_hour_minute_second_millis".equals(input)) {
            return STRICT_HOUR_MINUTE_SECOND_MILLIS;
        } else if ("strictOrdinalDate".equals(input) || "strict_ordinal_date".equals(input)) {
            return STRICT_ORDINAL_DATE;
        } else if ("strictOrdinalDateTime".equals(input) || "strict_ordinal_date_time".equals(input)) {
            return STRICT_ORDINAL_DATE_TIME;
        } else if ("strictOrdinalDateTimeNoMillis".equals(input) || "strict_ordinal_date_time_no_millis".equals(input)) {
            return STRICT_ORDINAL_DATE_TIME_NO_MILLIS;
        } else if ("strictTime".equals(input) || "strict_time".equals(input)) {
            return STRICT_TIME;
        } else if ("strictTimeNoMillis".equals(input) || "strict_time_no_millis".equals(input)) {
            return STRICT_TIME_NO_MILLIS;
        } else if ("strictTTime".equals(input) || "strict_t_time".equals(input)) {
            return STRICT_T_TIME;
        } else if ("strictTTimeNoMillis".equals(input) || "strict_t_time_no_millis".equals(input)) {
            return STRICT_T_TIME_NO_MILLIS;
        } else if ("strictWeekDate".equals(input) || "strict_week_date".equals(input)) {
            return STRICT_WEEK_DATE;
        } else if ("strictWeekDateTime".equals(input) || "strict_week_date_time".equals(input)) {
            return STRICT_WEEK_DATE_TIME;
        } else if ("strictWeekDateTimeNoMillis".equals(input) || "strict_week_date_time_no_millis".equals(input)) {
            return STRICT_WEEK_DATE_TIME_NO_MILLIS;
        } else if ("strictWeekyear".equals(input) || "strict_weekyear".equals(input)) {
            return STRICT_WEEKYEAR;
        } else if ("strictWeekyearWeek".equals(input) || "strict_weekyear_week".equals(input)) {
            return STRICT_WEEKYEAR_WEEK;
        } else if ("strictWeekyearWeekDay".equals(input) || "strict_weekyear_week_day".equals(input)) {
            return STRICT_WEEKYEAR_WEEK_DAY;
        } else if ("strictYear".equals(input) || "strict_year".equals(input)) {
            return STRICT_YEAR;
        } else if ("strictYearMonth".equals(input) || "strict_year_month".equals(input)) {
            return STRICT_YEAR_MONTH;
        } else if ("strictYearMonthDay".equals(input) || "strict_year_month_day".equals(input)) {
            return STRICT_YEAR_MONTH_DAY;
        } else if (Strings.hasLength(input) && input.contains("||")) {
            String[] formats = Strings.delimitedListToStringArray(input, "||");
            if (formats.length == 1) {
                return forPattern(formats[0], locale);
            } else {
                try {
                    DateFormatter[] formatters = new DateFormatter[formats.length];
                    for (int i = 0; i < formats.length; i++) {
                        formatters[i] = forPattern(formats[i], locale);
                    }

                    return DateFormatter.merge(formatters);
                } catch (IllegalArgumentException e) {
                    throw new IllegalArgumentException("Invalid format: [" + input + "]: " + e.getMessage(), e);
                }
            }
        } else {
            try {
                return new JavaDateFormatter(input, new DateTimeFormatterBuilder().appendPattern(input).toFormatter(locale));
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException("Invalid format: [" + input + "]: " + e.getMessage(), e);
            }
        }
    }

    private static final ZonedDateTime EPOCH_ZONED_DATE_TIME = Instant.EPOCH.atZone(ZoneOffset.UTC);

    public static ZonedDateTime toZonedDateTime(TemporalAccessor accessor) {
        return toZonedDateTime(accessor, EPOCH_ZONED_DATE_TIME);
    }

    public static ZonedDateTime toZonedDateTime(TemporalAccessor accessor, ZonedDateTime defaults) {
        try {
            return ZonedDateTime.from(accessor);
        } catch (DateTimeException e ) {
        }

        ZonedDateTime result = defaults;

        // special case epoch seconds
        if (accessor.isSupported(ChronoField.INSTANT_SECONDS)) {
            result = result.with(ChronoField.INSTANT_SECONDS, accessor.getLong(ChronoField.INSTANT_SECONDS));
            if (accessor.isSupported(ChronoField.NANO_OF_SECOND)) {
                result = result.with(ChronoField.NANO_OF_SECOND, accessor.getLong(ChronoField.NANO_OF_SECOND));
            }
            return result;
        }

        // try to set current year
        if (accessor.isSupported(ChronoField.YEAR)) {
            result = result.with(ChronoField.YEAR, accessor.getLong(ChronoField.YEAR));
        } else if (accessor.isSupported(ChronoField.YEAR_OF_ERA)) {
            result = result.with(ChronoField.YEAR_OF_ERA, accessor.getLong(ChronoField.YEAR_OF_ERA));
        } else if (accessor.isSupported(WeekFields.ISO.weekBasedYear())) {
            if (accessor.isSupported(WeekFields.ISO.weekOfWeekBasedYear())) {
                return LocalDate.from(result)
                    .with(WeekFields.ISO.weekBasedYear(), accessor.getLong(WeekFields.ISO.weekBasedYear()))
                    .withDayOfMonth(1) // makes this compatible with joda
                    .with(WeekFields.ISO.weekOfWeekBasedYear(), accessor.getLong(WeekFields.ISO.weekOfWeekBasedYear()))
                    .atStartOfDay(ZoneOffset.UTC);
            } else {
                return LocalDate.from(result)
                    .with(WeekFields.ISO.weekBasedYear(), accessor.getLong(WeekFields.ISO.weekBasedYear()))
                    // this exists solely to be BWC compatible with joda
//                    .with(TemporalAdjusters.nextOrSame(DayOfWeek.MONDAY))
                    .with(TemporalAdjusters.firstInMonth(DayOfWeek.MONDAY))
                    .atStartOfDay(defaults.getZone());
//                return result.withHour(0).withMinute(0).withSecond(0)
//                    .with(WeekFields.ISO.weekBasedYear(), 0)
//                    .with(WeekFields.ISO.weekBasedYear(), accessor.getLong(WeekFields.ISO.weekBasedYear()));
//                return ((ZonedDateTime) tmp).with(WeekFields.ISO.weekOfWeekBasedYear(), 1);
            }
        } else if (accessor.isSupported(IsoFields.WEEK_BASED_YEAR)) {
            // special case weekbased year
            result = result.with(IsoFields.WEEK_BASED_YEAR, accessor.getLong(IsoFields.WEEK_BASED_YEAR));
            if (accessor.isSupported(IsoFields.WEEK_OF_WEEK_BASED_YEAR)) {
                result = result.with(IsoFields.WEEK_OF_WEEK_BASED_YEAR, accessor.getLong(IsoFields.WEEK_OF_WEEK_BASED_YEAR));
            }
            return result;
        }

        // month
        if (accessor.isSupported(ChronoField.MONTH_OF_YEAR)) {
            result = result.with(ChronoField.MONTH_OF_YEAR, accessor.getLong(ChronoField.MONTH_OF_YEAR));
        }

        // day of month
        if (accessor.isSupported(ChronoField.DAY_OF_MONTH)) {
            result = result.with(ChronoField.DAY_OF_MONTH, accessor.getLong(ChronoField.DAY_OF_MONTH));
        }

        // hour
        if (accessor.isSupported(ChronoField.HOUR_OF_DAY)) {
            result = result.with(ChronoField.HOUR_OF_DAY, accessor.getLong(ChronoField.HOUR_OF_DAY));
        }

        // minute
        if (accessor.isSupported(ChronoField.MINUTE_OF_HOUR)) {
            result = result.with(ChronoField.MINUTE_OF_HOUR, accessor.getLong(ChronoField.MINUTE_OF_HOUR));
        }

        // second
        if (accessor.isSupported(ChronoField.SECOND_OF_MINUTE)) {
            result = result.with(ChronoField.SECOND_OF_MINUTE, accessor.getLong(ChronoField.SECOND_OF_MINUTE));
        }

        if (accessor.isSupported(ChronoField.OFFSET_SECONDS)) {
            result = result.withZoneSameLocal(ZoneOffset.ofTotalSeconds(accessor.get(ChronoField.OFFSET_SECONDS)));
        }

        // millis
        if (accessor.isSupported(ChronoField.MILLI_OF_SECOND)) {
            result = result.with(ChronoField.MILLI_OF_SECOND, accessor.getLong(ChronoField.MILLI_OF_SECOND));
        }

        if (accessor.isSupported(ChronoField.NANO_OF_SECOND)) {
            result = result.with(ChronoField.NANO_OF_SECOND, accessor.getLong(ChronoField.NANO_OF_SECOND));
        }

        return result;
    }
}
