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
import org.elasticsearch.common.SuppressForbidden;

import java.time.DayOfWeek;
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
import java.time.temporal.WeekFields;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import static java.time.temporal.ChronoField.DAY_OF_MONTH;
import static java.time.temporal.ChronoField.DAY_OF_WEEK;
import static java.time.temporal.ChronoField.DAY_OF_YEAR;
import static java.time.temporal.ChronoField.HOUR_OF_DAY;
import static java.time.temporal.ChronoField.MINUTE_OF_HOUR;
import static java.time.temporal.ChronoField.MONTH_OF_YEAR;
import static java.time.temporal.ChronoField.NANO_OF_SECOND;
import static java.time.temporal.ChronoField.SECOND_OF_MINUTE;
import static org.elasticsearch.common.time.FormatterNames.BASICDATE;
import static org.elasticsearch.common.time.FormatterNames.BASICDATETIME;
import static org.elasticsearch.common.time.FormatterNames.BASICDATETIMENOMILLIS;
import static org.elasticsearch.common.time.FormatterNames.BASICORDINALDATE;
import static org.elasticsearch.common.time.FormatterNames.BASICORDINALDATETIME;
import static org.elasticsearch.common.time.FormatterNames.BASICORDINALDATETIMENOMILLIS;
import static org.elasticsearch.common.time.FormatterNames.BASICTIME;
import static org.elasticsearch.common.time.FormatterNames.BASICTIMENOMILLIS;
import static org.elasticsearch.common.time.FormatterNames.BASICTTIME;
import static org.elasticsearch.common.time.FormatterNames.BASICTTIMENOMILLIS;
import static org.elasticsearch.common.time.FormatterNames.BASICWEEKDATE;
import static org.elasticsearch.common.time.FormatterNames.BASICWEEKDATETIME;
import static org.elasticsearch.common.time.FormatterNames.BASICWEEKDATETIMENOMILLIS;
import static org.elasticsearch.common.time.FormatterNames.DATEHOUR;
import static org.elasticsearch.common.time.FormatterNames.DATEHOURMINUTE;
import static org.elasticsearch.common.time.FormatterNames.DATEHOURMINUTESECOND;
import static org.elasticsearch.common.time.FormatterNames.DATEHOURMINUTESECONDFRACTION;
import static org.elasticsearch.common.time.FormatterNames.DATEHOURMINUTESECONDMILLIS;
import static org.elasticsearch.common.time.FormatterNames.DATEOPTIONALTIME;
import static org.elasticsearch.common.time.FormatterNames.DATETIME;
import static org.elasticsearch.common.time.FormatterNames.DATETIMENOMILLIS;
import static org.elasticsearch.common.time.FormatterNames.EPOCH_MILLIS;
import static org.elasticsearch.common.time.FormatterNames.EPOCH_SECOND;
import static org.elasticsearch.common.time.FormatterNames.HOURMINUTE;
import static org.elasticsearch.common.time.FormatterNames.HOURMINUTESECOND;
import static org.elasticsearch.common.time.FormatterNames.HOURMINUTESECONDFRACTION;
import static org.elasticsearch.common.time.FormatterNames.HOURMINUTESECONDMILLIS;
import static org.elasticsearch.common.time.FormatterNames.ISO8601;
import static org.elasticsearch.common.time.FormatterNames.ORDINALDATE;
import static org.elasticsearch.common.time.FormatterNames.ORDINALDATETIME;
import static org.elasticsearch.common.time.FormatterNames.ORDINALDATETIMENOMILLIS;
import static org.elasticsearch.common.time.FormatterNames.STRICTBASICWEEKDATE;
import static org.elasticsearch.common.time.FormatterNames.STRICTBASICWEEKDATETIME;
import static org.elasticsearch.common.time.FormatterNames.STRICTBASICWEEKDATETIMENOMILLIS;
import static org.elasticsearch.common.time.FormatterNames.STRICTDATE;
import static org.elasticsearch.common.time.FormatterNames.STRICTDATEHOUR;
import static org.elasticsearch.common.time.FormatterNames.STRICTDATEHOURMINUTE;
import static org.elasticsearch.common.time.FormatterNames.STRICTDATEHOURMINUTESECOND;
import static org.elasticsearch.common.time.FormatterNames.STRICTDATEHOURMINUTESECONDFRACTION;
import static org.elasticsearch.common.time.FormatterNames.STRICTDATEHOURMINUTESECONDMILLIS;
import static org.elasticsearch.common.time.FormatterNames.STRICTDATEOPTIONALTIME;
import static org.elasticsearch.common.time.FormatterNames.STRICTDATEOPTIONALTIMENANOS;
import static org.elasticsearch.common.time.FormatterNames.STRICTDATETIME;
import static org.elasticsearch.common.time.FormatterNames.STRICTDATETIMENOMILLIS;
import static org.elasticsearch.common.time.FormatterNames.STRICTHOUR;
import static org.elasticsearch.common.time.FormatterNames.STRICTHOURMINUTE;
import static org.elasticsearch.common.time.FormatterNames.STRICTHOURMINUTESECOND;
import static org.elasticsearch.common.time.FormatterNames.STRICTHOURMINUTESECONDFRACTION;
import static org.elasticsearch.common.time.FormatterNames.STRICTHOURMINUTESECONDMILLIS;
import static org.elasticsearch.common.time.FormatterNames.STRICTORDINALDATE;
import static org.elasticsearch.common.time.FormatterNames.STRICTORDINALDATETIME;
import static org.elasticsearch.common.time.FormatterNames.STRICTORDINALDATETIMENOMILLIS;
import static org.elasticsearch.common.time.FormatterNames.STRICTTIME;
import static org.elasticsearch.common.time.FormatterNames.STRICTTIMENOMILLIS;
import static org.elasticsearch.common.time.FormatterNames.STRICTTTIME;
import static org.elasticsearch.common.time.FormatterNames.STRICTTTIMENOMILLIS;
import static org.elasticsearch.common.time.FormatterNames.STRICTWEEKDATE;
import static org.elasticsearch.common.time.FormatterNames.STRICTWEEKDATETIME;
import static org.elasticsearch.common.time.FormatterNames.STRICTWEEKDATETIMENOMILLIS;
import static org.elasticsearch.common.time.FormatterNames.STRICTWEEKYEAR;
import static org.elasticsearch.common.time.FormatterNames.STRICTWEEKYEARWEEK;
import static org.elasticsearch.common.time.FormatterNames.STRICTWEEKYEARWEEKDAY;
import static org.elasticsearch.common.time.FormatterNames.STRICTYEAR;
import static org.elasticsearch.common.time.FormatterNames.STRICTYEARMONTH;
import static org.elasticsearch.common.time.FormatterNames.STRICTYEARMONTHDAY;
import static org.elasticsearch.common.time.FormatterNames.TIMENOMILLIS;
import static org.elasticsearch.common.time.FormatterNames.TTIME;
import static org.elasticsearch.common.time.FormatterNames.TTIMENOMILLIS;
import static org.elasticsearch.common.time.FormatterNames.WEEKDATE;
import static org.elasticsearch.common.time.FormatterNames.WEEKDATETIME;
import static org.elasticsearch.common.time.FormatterNames.WEEKDATETIMENOMILLIS;
import static org.elasticsearch.common.time.FormatterNames.WEEKYEAR;
import static org.elasticsearch.common.time.FormatterNames.WEEKYEARWEEK;
import static org.elasticsearch.common.time.FormatterNames.WEEKYEARWEEKDAY;
import static org.elasticsearch.common.time.FormatterNames.YEARMONTH;
import static org.elasticsearch.common.time.FormatterNames.YEARMONTHDAY;

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

    private static final DateTimeFormatter STRICT_DATE_OPTIONAL_TIME_PRINTER = new DateTimeFormatterBuilder()
        .append(STRICT_YEAR_MONTH_DAY_FORMATTER)
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
        .toFormatter(Locale.ROOT);

    private static final DateTimeFormatter STRICT_DATE_OPTIONAL_TIME_FORMATTER = new DateTimeFormatterBuilder()
        .append(STRICT_YEAR_MONTH_DAY_FORMATTER)
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
        .appendFraction(NANO_OF_SECOND, 3, 9, true)
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
        .optionalEnd()
        .toFormatter(Locale.ROOT);

    /**
     * Returns a generic ISO datetime parser where the date is mandatory and the time is optional.
     */
    private static final DateFormatter STRICT_DATE_OPTIONAL_TIME =
        new JavaDateFormatter("strict_date_optional_time", STRICT_DATE_OPTIONAL_TIME_PRINTER, STRICT_DATE_OPTIONAL_TIME_FORMATTER);

    private static final DateTimeFormatter STRICT_DATE_OPTIONAL_TIME_FORMATTER_WITH_NANOS = new DateTimeFormatterBuilder()
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
        .optionalStart()
        .append(TIME_ZONE_FORMATTER_NO_COLON)
        .optionalEnd()
        .optionalEnd()
        .toFormatter(Locale.ROOT);

    private static final DateTimeFormatter STRICT_DATE_OPTIONAL_TIME_PRINTER_NANOS = new DateTimeFormatterBuilder()
        .append(STRICT_YEAR_MONTH_DAY_FORMATTER)
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
        .toFormatter(Locale.ROOT);

    /**
     * Returns a generic ISO datetime parser where the date is mandatory and the time is optional with nanosecond resolution.
     */
    private static final DateFormatter STRICT_DATE_OPTIONAL_TIME_NANOS = new JavaDateFormatter("strict_date_optional_time_nanos",
        STRICT_DATE_OPTIONAL_TIME_PRINTER_NANOS, STRICT_DATE_OPTIONAL_TIME_FORMATTER_WITH_NANOS);

    /**
     * Returns a ISO 8601 compatible date time formatter and parser.
     * This is not fully compatible to the existing spec, which would require far more edge cases, but merely compatible with the
     * existing joda time ISO data formater
     */
    private static final DateFormatter ISO_8601 = new JavaDateFormatter("iso8601", STRICT_DATE_OPTIONAL_TIME_PRINTER,
        new DateTimeFormatterBuilder()
            .append(STRICT_YEAR_MONTH_DAY_FORMATTER)
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
            .optionalStart()
            .appendZoneOrOffsetId()
            .optionalEnd()
            .optionalStart()
            .append(TIME_ZONE_FORMATTER_NO_COLON)
            .optionalEnd()
            .optionalEnd()
            .optionalEnd()
            .optionalEnd()
            .toFormatter(Locale.ROOT));

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
        new DateTimeFormatterBuilder().append(BASIC_TIME_NO_MILLIS_BASE).appendOffset("+HH:MM", "Z").toFormatter(Locale.ROOT),
        new DateTimeFormatterBuilder().append(BASIC_TIME_NO_MILLIS_BASE).appendZoneOrOffsetId().toFormatter(Locale.ROOT),
        new DateTimeFormatterBuilder().append(BASIC_TIME_NO_MILLIS_BASE).append(TIME_ZONE_FORMATTER_NO_COLON).toFormatter(Locale.ROOT)
    );

    private static final DateTimeFormatter BASIC_TIME_FORMATTER = new DateTimeFormatterBuilder()
        .appendValue(HOUR_OF_DAY, 2, 2, SignStyle.NOT_NEGATIVE)
        .appendValue(MINUTE_OF_HOUR, 2, 2, SignStyle.NOT_NEGATIVE)
        .appendValue(SECOND_OF_MINUTE, 2, 2, SignStyle.NOT_NEGATIVE)
        .appendFraction(NANO_OF_SECOND, 1, 9, true)
        .toFormatter(Locale.ROOT);

    private static final DateTimeFormatter BASIC_TIME_PRINTER = new DateTimeFormatterBuilder()
        .appendValue(HOUR_OF_DAY, 2, 2, SignStyle.NOT_NEGATIVE)
        .appendValue(MINUTE_OF_HOUR, 2, 2, SignStyle.NOT_NEGATIVE)
        .appendValue(SECOND_OF_MINUTE, 2, 2, SignStyle.NOT_NEGATIVE)
        .appendFraction(NANO_OF_SECOND, 3, 3, true)
        .toFormatter(Locale.ROOT);

    /*
     * Returns a basic formatter for a two digit hour of day, two digit minute
     * of hour, two digit second of minute, three digit millis, and time zone
     * offset (HHmmss.SSSZ).
     */
    private static final DateFormatter BASIC_TIME = new JavaDateFormatter("basic_time",
        new DateTimeFormatterBuilder().append(BASIC_TIME_PRINTER).appendOffset("+HH:MM", "Z").toFormatter(Locale.ROOT),
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
        new DateTimeFormatterBuilder().append(BASIC_T_TIME_PRINTER).appendOffset("+HH:MM", "Z").toFormatter(Locale.ROOT),
        new DateTimeFormatterBuilder().append(BASIC_T_TIME_FORMATTER).appendZoneOrOffsetId().toFormatter(Locale.ROOT),
        new DateTimeFormatterBuilder().append(BASIC_T_TIME_FORMATTER).append(TIME_ZONE_FORMATTER_NO_COLON).toFormatter(Locale.ROOT)
    );

    /*
     * Returns a basic formatter for a two digit hour of day, two digit minute
     * of hour, two digit second of minute, and time zone offset prefixed by 'T'
     * ('T'HHmmssZ).
     */
    private static final DateFormatter BASIC_T_TIME_NO_MILLIS = new JavaDateFormatter("basic_t_time_no_millis",
        new DateTimeFormatterBuilder().appendLiteral("T").append(BASIC_TIME_NO_MILLIS_BASE)
                                      .appendOffset("+HH:MM", "Z").toFormatter(Locale.ROOT),
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
        new DateTimeFormatterBuilder().append(BASIC_DATE_TIME_PRINTER).appendOffset("+HH:MM", "Z").toFormatter(Locale.ROOT),
        new DateTimeFormatterBuilder().append(BASIC_DATE_TIME_FORMATTER).appendZoneOrOffsetId().toFormatter(Locale.ROOT),
        new DateTimeFormatterBuilder().append(BASIC_DATE_TIME_FORMATTER).append(TIME_ZONE_FORMATTER_NO_COLON).toFormatter(Locale.ROOT)
    );

    private static final DateTimeFormatter BASIC_DATE_T =
        new DateTimeFormatterBuilder().append(BASIC_YEAR_MONTH_DAY_FORMATTER).appendLiteral("T").toFormatter(Locale.ROOT);

    /*
     * Returns a basic formatter that combines a basic date and time without millis,
     * separated by a 'T' (yyyyMMdd'T'HHmmssZ).
     */
    private static final DateFormatter BASIC_DATE_TIME_NO_MILLIS = new JavaDateFormatter("basic_date_time_no_millis",
        new DateTimeFormatterBuilder().append(BASIC_DATE_T).append(BASIC_TIME_NO_MILLIS_BASE)
            .appendOffset("+HH:MM", "Z").toFormatter(Locale.ROOT),
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
            .appendOffset("+HH:MM", "Z").toFormatter(Locale.ROOT),
        new DateTimeFormatterBuilder().appendPattern("yyyyDDD").append(BASIC_T_TIME_FORMATTER)
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
            .appendOffset("+HH:MM", "Z").toFormatter(Locale.ROOT),
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
    private static final DateFormatter STRICT_BASIC_WEEK_DATE_TIME_NO_MILLIS =
        new JavaDateFormatter("strict_basic_week_date_time_no_millis",
            new DateTimeFormatterBuilder()
                .append(STRICT_BASIC_WEEK_DATE_PRINTER)
                .appendLiteral("T")
                .appendValue(HOUR_OF_DAY, 2, 2, SignStyle.NOT_NEGATIVE)
                .appendValue(MINUTE_OF_HOUR, 2, 2, SignStyle.NOT_NEGATIVE)
                .appendValue(SECOND_OF_MINUTE, 2, 2, SignStyle.NOT_NEGATIVE)
                .appendOffset("+HH:MM", "Z")
                .toFormatter(Locale.ROOT),
            new DateTimeFormatterBuilder()
                .append(STRICT_BASIC_WEEK_DATE_PRINTER)
                .appendLiteral("T")
                .appendValue(HOUR_OF_DAY, 2, 2, SignStyle.NOT_NEGATIVE)
                .appendValue(MINUTE_OF_HOUR, 2, 2, SignStyle.NOT_NEGATIVE)
                .appendValue(SECOND_OF_MINUTE, 2, 2, SignStyle.NOT_NEGATIVE)
                .appendZoneOrOffsetId()
                .toFormatter(Locale.ROOT),
            new DateTimeFormatterBuilder()
                .append(STRICT_BASIC_WEEK_DATE_PRINTER)
                .appendLiteral("T")
                .appendValue(HOUR_OF_DAY, 2, 2, SignStyle.NOT_NEGATIVE)
                .appendValue(MINUTE_OF_HOUR, 2, 2, SignStyle.NOT_NEGATIVE)
                .appendValue(SECOND_OF_MINUTE, 2, 2, SignStyle.NOT_NEGATIVE)
                .append(TIME_ZONE_FORMATTER_NO_COLON)
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
            .appendLiteral("T")
            .appendValue(HOUR_OF_DAY, 2, 2, SignStyle.NOT_NEGATIVE)
            .appendValue(MINUTE_OF_HOUR, 2, 2, SignStyle.NOT_NEGATIVE)
            .appendValue(SECOND_OF_MINUTE, 2, 2, SignStyle.NOT_NEGATIVE)
            .appendFraction(NANO_OF_SECOND, 1, 9, true)
            .appendZoneOrOffsetId()
            .toFormatter(Locale.ROOT),
        new DateTimeFormatterBuilder()
            .append(STRICT_BASIC_WEEK_DATE_FORMATTER)
            .appendLiteral("T")
            .appendValue(HOUR_OF_DAY, 2, 2, SignStyle.NOT_NEGATIVE)
            .appendValue(MINUTE_OF_HOUR, 2, 2, SignStyle.NOT_NEGATIVE)
            .appendValue(SECOND_OF_MINUTE, 2, 2, SignStyle.NOT_NEGATIVE)
            .appendFraction(NANO_OF_SECOND, 1, 9, true)
            .append(TIME_ZONE_FORMATTER_NO_COLON)
            .toFormatter(Locale.ROOT)
    );

    /*
     * An ISO date formatter that formats or parses a date without an offset, such as '2011-12-03'.
     */
    private static final DateFormatter STRICT_DATE = new JavaDateFormatter("strict_date",
        DateTimeFormatter.ISO_LOCAL_DATE.withResolverStyle(ResolverStyle.LENIENT).withLocale(Locale.ROOT));

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

    private static final DateTimeFormatter STRICT_DATE_PRINTER = new DateTimeFormatterBuilder()
        .append(STRICT_YEAR_MONTH_DAY_FORMATTER)
        .appendLiteral('T')
        .append(STRICT_HOUR_MINUTE_SECOND_FORMATTER)
        .appendFraction(NANO_OF_SECOND, 3, 9, true)
        .appendOffset("+HH:MM", "Z")
        .toFormatter(Locale.ROOT);

    private static final DateTimeFormatter STRICT_DATE_FORMATTER = new DateTimeFormatterBuilder()
        .append(STRICT_YEAR_MONTH_DAY_FORMATTER)
        .appendLiteral('T')
        .append(STRICT_HOUR_MINUTE_SECOND_FORMATTER)
        .optionalStart()
        .appendFraction(NANO_OF_SECOND, 1, 9, true)
        .optionalEnd()
        .toFormatter(Locale.ROOT);

    /*
     * Returns a formatter that combines a full date and time, separated by a 'T'
     * (yyyy-MM-dd'T'HH:mm:ss.SSSZZ).
     */
    private static final DateFormatter STRICT_DATE_TIME = new JavaDateFormatter("strict_date_time", STRICT_DATE_PRINTER,
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
            .appendOffset("+HH:MM", "Z").toFormatter(Locale.ROOT),
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
            .appendOffset("+HH:MM", "Z").toFormatter(Locale.ROOT),
        new DateTimeFormatterBuilder().append(STRICT_DATE_TIME_NO_MILLIS_FORMATTER)
            .appendZoneOrOffsetId().toFormatter(Locale.ROOT),
        new DateTimeFormatterBuilder().append(STRICT_DATE_TIME_NO_MILLIS_FORMATTER)
            .append(TIME_ZONE_FORMATTER_NO_COLON).toFormatter(Locale.ROOT)
    );

    // NOTE: this is not a strict formatter to retain the joda time based behaviour, even though it's named like this
    private static final DateTimeFormatter STRICT_HOUR_MINUTE_SECOND_MILLIS_FORMATTER = new DateTimeFormatterBuilder()
        .append(STRICT_HOUR_MINUTE_SECOND_FORMATTER)
        .appendFraction(NANO_OF_SECOND, 1, 9, true)
        .toFormatter(Locale.ROOT);

    private static final DateTimeFormatter STRICT_HOUR_MINUTE_SECOND_MILLIS_PRINTER = new DateTimeFormatterBuilder()
        .append(STRICT_HOUR_MINUTE_SECOND_FORMATTER)
        .appendFraction(NANO_OF_SECOND, 3, 3, true)
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

    private static final DateFormatter STRICT_HOUR_MINUTE_SECOND_FRACTION =
        new JavaDateFormatter("strict_hour_minute_second_fraction",
            STRICT_HOUR_MINUTE_SECOND_MILLIS_PRINTER, STRICT_HOUR_MINUTE_SECOND_MILLIS_FORMATTER);

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
            // this one here is lenient as well to retain joda time based bwc compatibility
            .appendFraction(NANO_OF_SECOND, 1, 9, true)
            .toFormatter(Locale.ROOT)
    );

    private static final DateFormatter STRICT_DATE_HOUR_MINUTE_SECOND_MILLIS = new JavaDateFormatter(
        "strict_date_hour_minute_second_millis",
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
            .appendFraction(NANO_OF_SECOND, 1, 9, true)
            .toFormatter(Locale.ROOT)
    );

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

    private static final DateTimeFormatter STRICT_ORDINAL_DATE_TIME_PRINTER = new DateTimeFormatterBuilder()
        .appendValue(ChronoField.YEAR, 4, 10, SignStyle.EXCEEDS_PAD)
        .appendLiteral('-')
        .appendValue(DAY_OF_YEAR, 3, 3, SignStyle.NOT_NEGATIVE)
        .appendLiteral('T')
        .appendPattern("HH:mm")
        .optionalStart()
        .appendLiteral(':')
        .appendValue(SECOND_OF_MINUTE, 2, 2, SignStyle.NOT_NEGATIVE)
        .appendFraction(NANO_OF_SECOND, 3, 9, true)
        .optionalEnd()
        .toFormatter(Locale.ROOT);

    private static final DateTimeFormatter STRICT_ORDINAL_DATE_TIME_FORMATTER_BASE = new DateTimeFormatterBuilder()
        .appendValue(ChronoField.YEAR, 4, 10, SignStyle.EXCEEDS_PAD)
        .appendLiteral('-')
        .appendValue(DAY_OF_YEAR, 3, 3, SignStyle.NOT_NEGATIVE)
        .appendLiteral('T')
        .appendPattern("HH:mm")
        .optionalStart()
        .appendLiteral(':')
        .appendValue(SECOND_OF_MINUTE, 2, 2, SignStyle.NOT_NEGATIVE)
        .appendFraction(NANO_OF_SECOND, 1, 9, true)
        .optionalEnd()
        .toFormatter(Locale.ROOT);

    /*
     * Returns a formatter for a full ordinal date and time, using a four
     * digit year and three digit dayOfYear (yyyy-DDD'T'HH:mm:ss.SSSZZ).
     */
    private static final DateFormatter STRICT_ORDINAL_DATE_TIME = new JavaDateFormatter("strict_ordinal_date_time",
        new DateTimeFormatterBuilder().append(STRICT_ORDINAL_DATE_TIME_PRINTER)
            .appendOffset("+HH:MM", "Z").toFormatter(Locale.ROOT),
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
        .appendFraction(NANO_OF_SECOND, 1, 9, true)
        .toFormatter(Locale.ROOT);

    private static final DateTimeFormatter STRICT_TIME_PRINTER = new DateTimeFormatterBuilder()
        .appendValue(HOUR_OF_DAY, 2, 2, SignStyle.NOT_NEGATIVE)
        .appendLiteral(':')
        .appendValue(MINUTE_OF_HOUR, 2, 2, SignStyle.NOT_NEGATIVE)
        .appendLiteral(':')
        .appendValue(SECOND_OF_MINUTE, 2, 2, SignStyle.NOT_NEGATIVE)
        .appendFraction(NANO_OF_SECOND, 3, 3, true)
        .toFormatter(Locale.ROOT);

    /*
     * Returns a formatter for a two digit hour of day, two digit minute of
     * hour, two digit second of minute, three digit fraction of second, and
     * time zone offset (HH:mm:ss.SSSZZ).
     */
    private static final DateFormatter STRICT_TIME = new JavaDateFormatter("strict_time",
        new DateTimeFormatterBuilder().append(STRICT_TIME_PRINTER).appendOffset("+HH:MM", "Z").toFormatter(Locale.ROOT),
        new DateTimeFormatterBuilder().append(STRICT_TIME_FORMATTER_BASE).appendZoneOrOffsetId().toFormatter(Locale.ROOT),
        new DateTimeFormatterBuilder().append(STRICT_TIME_FORMATTER_BASE).append(TIME_ZONE_FORMATTER_NO_COLON).toFormatter(Locale.ROOT)
    );

    /*
     * Returns a formatter for a two digit hour of day, two digit minute of
     * hour, two digit second of minute, three digit fraction of second, and
     * time zone offset prefixed by 'T' ('T'HH:mm:ss.SSSZZ).
     */
    private static final DateFormatter STRICT_T_TIME = new JavaDateFormatter("strict_t_time",
        new DateTimeFormatterBuilder().appendLiteral('T').append(STRICT_TIME_PRINTER).appendOffset("+HH:MM", "Z").toFormatter(Locale.ROOT),
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
        new DateTimeFormatterBuilder().append(STRICT_TIME_NO_MILLIS_BASE).appendOffset("+HH:MM", "Z").toFormatter(Locale.ROOT),
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
            .appendOffset("+HH:MM", "Z").toFormatter(Locale.ROOT),
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
            .append(STRICT_TIME_NO_MILLIS_BASE).appendOffset("+HH:MM", "Z").toFormatter(Locale.ROOT),
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
        new DateTimeFormatterBuilder().append(ISO_WEEK_DATE_T)
                                      .append(STRICT_TIME_PRINTER).appendOffset("+HH:MM", "Z").toFormatter(Locale.ROOT),
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
        .appendLiteral('-')
        .appendValue(MONTH_OF_YEAR, 1, 2, SignStyle.NOT_NEGATIVE)
        .optionalStart()
        .appendLiteral('-')
        .appendValue(DAY_OF_MONTH, 1, 2, SignStyle.NOT_NEGATIVE)
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
        STRICT_DATE_OPTIONAL_TIME_PRINTER,
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
            .appendFraction(NANO_OF_SECOND, 1, 9, true)
            .optionalEnd()
            .optionalStart().appendZoneOrOffsetId().optionalEnd()
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
        .appendFraction(NANO_OF_SECOND, 1, 3, true)
        .toFormatter(Locale.ROOT);

    private static final DateTimeFormatter HOUR_MINUTE_SECOND_FRACTION_FORMATTER = new DateTimeFormatterBuilder()
        .appendValue(HOUR_OF_DAY, 1, 2, SignStyle.NOT_NEGATIVE)
        .appendLiteral(':')
        .appendValue(MINUTE_OF_HOUR, 1, 2, SignStyle.NOT_NEGATIVE)
        .appendLiteral(':')
        .appendValue(SECOND_OF_MINUTE, 1, 2, SignStyle.NOT_NEGATIVE)
        .appendFraction(NANO_OF_SECOND, 1, 9, true)
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
        .appendFraction(NANO_OF_SECOND, 1, 9, true)
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
     * fraction of second (yyyy-MM-dd'T'HH:mm:ss.SSS).
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

    private static final DateFormatter DATE_HOUR_MINUTE_SECOND_FRACTION =
        new JavaDateFormatter("date_hour_minute_second_fraction",
            new DateTimeFormatterBuilder()
                .append(STRICT_YEAR_MONTH_DAY_FORMATTER)
                .appendLiteral("T")
                .append(STRICT_HOUR_MINUTE_SECOND_MILLIS_PRINTER)
                .toFormatter(Locale.ROOT),
            new DateTimeFormatterBuilder()
                .append(DATE_FORMATTER)
                .appendLiteral("T")
                .append(HOUR_MINUTE_SECOND_FRACTION_FORMATTER)
                .toFormatter(Locale.ROOT));

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
        .appendFraction(NANO_OF_SECOND, 1, 9, true)
        .optionalEnd()
        .toFormatter(Locale.ROOT);

    /*
     * Returns a formatter that combines a full date and time, separated by a 'T'
     * (yyyy-MM-dd'T'HH:mm:ss.SSSZZ).
     */
    private static final DateFormatter DATE_TIME = new JavaDateFormatter("date_time",
        STRICT_DATE_OPTIONAL_TIME_PRINTER,
        new DateTimeFormatterBuilder().append(DATE_TIME_FORMATTER).appendOffset("+HH:MM", "Z").toFormatter(Locale.ROOT),
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
        new DateTimeFormatterBuilder().append(DATE_TIME_PREFIX).appendOffset("+HH:MM", "Z").toFormatter(Locale.ROOT),
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

    private static final DateFormatter HOUR_MINUTE_SECOND_FRACTION = new JavaDateFormatter("hour_minute_second_fraction",
        STRICT_HOUR_MINUTE_SECOND_MILLIS_PRINTER, HOUR_MINUTE_SECOND_FRACTION_FORMATTER);

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
        .appendFraction(NANO_OF_SECOND, 1, 9, true)
        .optionalEnd()
        .toFormatter(Locale.ROOT);

    /*
     * Returns a formatter for a full ordinal date and time, using a four
     * digit year and three digit dayOfYear (yyyy-DDD'T'HH:mm:ss.SSSZZ).
     */
    private static final DateFormatter ORDINAL_DATE_TIME = new JavaDateFormatter("ordinal_date_time",
        new DateTimeFormatterBuilder().append(STRICT_ORDINAL_DATE_TIME_PRINTER)
            .appendOffset("+HH:MM", "Z").toFormatter(Locale.ROOT),
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
            .appendOffset("+HH:MM", "Z").toFormatter(Locale.ROOT),
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
        new DateTimeFormatterBuilder().append(ISO_WEEK_DATE_T)
                                      .append(STRICT_TIME_PRINTER).appendOffset("+HH:MM", "Z").toFormatter(Locale.ROOT),
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
            .append(STRICT_TIME_NO_MILLIS_BASE).appendOffset("+HH:MM", "Z").toFormatter(Locale.ROOT),
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
        new DateTimeFormatterBuilder().append(STRICT_TIME_PRINTER).appendOffset("+HH:MM", "Z").toFormatter(Locale.ROOT),
        new DateTimeFormatterBuilder().append(TIME_PREFIX).appendZoneOrOffsetId().toFormatter(Locale.ROOT),
        new DateTimeFormatterBuilder().append(TIME_PREFIX).append(TIME_ZONE_FORMATTER_NO_COLON).toFormatter(Locale.ROOT)
    );

    /*
     * Returns a formatter for a two digit hour of day, two digit minute of
     * hour, two digit second of minute, andtime zone offset (HH:mm:ssZZ).
     */
    private static final DateFormatter TIME_NO_MILLIS = new JavaDateFormatter("time_no_millis",
        new DateTimeFormatterBuilder().append(STRICT_TIME_NO_MILLIS_BASE).appendOffset("+HH:MM", "Z").toFormatter(Locale.ROOT),
        new DateTimeFormatterBuilder().append(TIME_NO_MILLIS_FORMATTER).appendZoneOrOffsetId().toFormatter(Locale.ROOT),
        new DateTimeFormatterBuilder().append(TIME_NO_MILLIS_FORMATTER).append(TIME_ZONE_FORMATTER_NO_COLON).toFormatter(Locale.ROOT)
    );

    /*
     * Returns a formatter for a two digit hour of day, two digit minute of
     * hour, two digit second of minute, three digit fraction of second, and
     * time zone offset prefixed by 'T' ('T'HH:mm:ss.SSSZZ).
     */
    private static final DateFormatter T_TIME = new JavaDateFormatter("t_time",
        new DateTimeFormatterBuilder().appendLiteral('T').append(STRICT_TIME_PRINTER).appendOffset("+HH:MM", "Z").toFormatter(Locale.ROOT),
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
                                      .appendOffset("+HH:MM", "Z").toFormatter(Locale.ROOT),
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
        if (Strings.hasLength(input)) {
            input = input.trim();
        }
        if (input == null || input.length() == 0) {
            throw new IllegalArgumentException("No date pattern provided");
        }

        if (ISO8601.matches(input)) {
            return ISO_8601;
        } else if (BASICDATE.matches(input) ) {
            return BASIC_DATE;
        } else if (BASICDATETIME.matches(input) ) {
            return BASIC_DATE_TIME;
        } else if (BASICDATETIMENOMILLIS.matches(input) ) {
            return BASIC_DATE_TIME_NO_MILLIS;
        } else if (BASICORDINALDATE.matches(input) ) {
            return BASIC_ORDINAL_DATE;
        } else if (BASICORDINALDATETIME.matches(input) ) {
            return BASIC_ORDINAL_DATE_TIME;
        } else if (BASICORDINALDATETIMENOMILLIS.matches(input) ) {
            return BASIC_ORDINAL_DATE_TIME_NO_MILLIS;
        } else if (BASICTIME.matches(input) ) {
            return BASIC_TIME;
        } else if (BASICTIMENOMILLIS.matches(input) ) {
            return BASIC_TIME_NO_MILLIS;
        } else if (BASICTTIME.matches(input) ) {
            return BASIC_T_TIME;
        } else if (BASICTTIMENOMILLIS.matches(input) ) {
            return BASIC_T_TIME_NO_MILLIS;
        } else if (BASICWEEKDATE.matches(input) ) {
            return BASIC_WEEK_DATE;
        } else if (BASICWEEKDATETIME.matches(input) ) {
            return BASIC_WEEK_DATE_TIME;
        } else if (BASICWEEKDATETIMENOMILLIS.matches(input) ) {
            return BASIC_WEEK_DATE_TIME_NO_MILLIS;
        } else if (FormatterNames.DATE.matches(input)) {
            return DATE;
        } else if (DATEHOUR.matches(input) ) {
            return DATE_HOUR;
        } else if (DATEHOURMINUTE.matches(input) ) {
            return DATE_HOUR_MINUTE;
        } else if (DATEHOURMINUTESECOND.matches(input) ) {
            return DATE_HOUR_MINUTE_SECOND;
        } else if (DATEHOURMINUTESECONDFRACTION.matches(input) ) {
            return DATE_HOUR_MINUTE_SECOND_FRACTION;
        } else if (DATEHOURMINUTESECONDMILLIS.matches(input) ) {
            return DATE_HOUR_MINUTE_SECOND_MILLIS;
        } else if (DATEOPTIONALTIME.matches(input) ) {
            return DATE_OPTIONAL_TIME;
        } else if (DATETIME.matches(input) ) {
            return DATE_TIME;
        } else if (DATETIMENOMILLIS.matches(input) ) {
            return DATE_TIME_NO_MILLIS;
        } else if (FormatterNames.HOUR.matches(input)) {
            return HOUR;
        } else if (HOURMINUTE.matches(input) ) {
            return HOUR_MINUTE;
        } else if (HOURMINUTESECOND.matches(input) ) {
            return HOUR_MINUTE_SECOND;
        } else if (HOURMINUTESECONDFRACTION.matches(input) ) {
            return HOUR_MINUTE_SECOND_FRACTION;
        } else if (HOURMINUTESECONDMILLIS.matches(input) ) {
            return HOUR_MINUTE_SECOND_MILLIS;
        } else if (ORDINALDATE.matches(input) ) {
            return ORDINAL_DATE;
        } else if (ORDINALDATETIME.matches(input) ) {
            return ORDINAL_DATE_TIME;
        } else if (ORDINALDATETIMENOMILLIS.matches(input) ) {
            return ORDINAL_DATE_TIME_NO_MILLIS;
        } else if (FormatterNames.TIME.matches(input)) {
            return TIME;
        } else if (TIMENOMILLIS.matches(input) ) {
            return TIME_NO_MILLIS;
        } else if (TTIME.matches(input) ) {
            return T_TIME;
        } else if (TTIMENOMILLIS.matches(input) ) {
            return T_TIME_NO_MILLIS;
        } else if (WEEKDATE.matches(input) ) {
            return WEEK_DATE;
        } else if (WEEKDATETIME.matches(input) ) {
            return WEEK_DATE_TIME;
        } else if (WEEKDATETIMENOMILLIS.matches(input) ) {
            return WEEK_DATE_TIME_NO_MILLIS;
        } else if (WEEKYEAR.matches(input) ) {
            return WEEK_YEAR;
        } else if (WEEKYEARWEEK.matches(input) ) {
            return WEEKYEAR_WEEK;
        } else if (WEEKYEARWEEKDAY.matches(input) ) {
            return WEEKYEAR_WEEK_DAY;
        } else if (FormatterNames.YEAR.matches(input)) {
            return YEAR;
        } else if (YEARMONTH.matches(input) ) {
            return YEAR_MONTH;
        } else if (YEARMONTHDAY.matches(input) ) {
            return YEAR_MONTH_DAY;
        } else if (EPOCH_SECOND.matches(input)) {
            return EpochTime.SECONDS_FORMATTER;
        } else if (EPOCH_MILLIS.matches(input)) {
            return EpochTime.MILLIS_FORMATTER;
            // strict date formats here, must be at least 4 digits for year and two for months and two for day
        } else if (STRICTBASICWEEKDATE.matches(input) ) {
            return STRICT_BASIC_WEEK_DATE;
        } else if (STRICTBASICWEEKDATETIME.matches(input) ) {
            return STRICT_BASIC_WEEK_DATE_TIME;
        } else if (STRICTBASICWEEKDATETIMENOMILLIS.matches(input) ) {
            return STRICT_BASIC_WEEK_DATE_TIME_NO_MILLIS;
        } else if (STRICTDATE.matches(input) ) {
            return STRICT_DATE;
        } else if (STRICTDATEHOUR.matches(input) ) {
            return STRICT_DATE_HOUR;
        } else if (STRICTDATEHOURMINUTE.matches(input) ) {
            return STRICT_DATE_HOUR_MINUTE;
        } else if (STRICTDATEHOURMINUTESECOND.matches(input) ) {
            return STRICT_DATE_HOUR_MINUTE_SECOND;
        } else if (STRICTDATEHOURMINUTESECONDFRACTION.matches(input) ) {
            return STRICT_DATE_HOUR_MINUTE_SECOND_FRACTION;
        } else if (STRICTDATEHOURMINUTESECONDMILLIS.matches(input) ) {
            return STRICT_DATE_HOUR_MINUTE_SECOND_MILLIS;
        } else if (STRICTDATEOPTIONALTIME.matches(input) ) {
            return STRICT_DATE_OPTIONAL_TIME;
        } else if (STRICTDATEOPTIONALTIMENANOS.matches(input) ) {
            return STRICT_DATE_OPTIONAL_TIME_NANOS;
        } else if (STRICTDATETIME.matches(input) ) {
            return STRICT_DATE_TIME;
        } else if (STRICTDATETIMENOMILLIS.matches(input) ) {
            return STRICT_DATE_TIME_NO_MILLIS;
        } else if (STRICTHOUR.matches(input) ) {
            return STRICT_HOUR;
        } else if (STRICTHOURMINUTE.matches(input) ) {
            return STRICT_HOUR_MINUTE;
        } else if (STRICTHOURMINUTESECOND.matches(input) ) {
            return STRICT_HOUR_MINUTE_SECOND;
        } else if (STRICTHOURMINUTESECONDFRACTION.matches(input) ) {
            return STRICT_HOUR_MINUTE_SECOND_FRACTION;
        } else if (STRICTHOURMINUTESECONDMILLIS.matches(input) ) {
            return STRICT_HOUR_MINUTE_SECOND_MILLIS;
        } else if (STRICTORDINALDATE.matches(input) ) {
            return STRICT_ORDINAL_DATE;
        } else if (STRICTORDINALDATETIME.matches(input) ) {
            return STRICT_ORDINAL_DATE_TIME;
        } else if (STRICTORDINALDATETIMENOMILLIS.matches(input) ) {
            return STRICT_ORDINAL_DATE_TIME_NO_MILLIS;
        } else if (STRICTTIME.matches(input) ) {
            return STRICT_TIME;
        } else if (STRICTTIMENOMILLIS.matches(input) ) {
            return STRICT_TIME_NO_MILLIS;
        } else if (STRICTTTIME.matches(input) ) {
            return STRICT_T_TIME;
        } else if (STRICTTTIMENOMILLIS.matches(input) ) {
            return STRICT_T_TIME_NO_MILLIS;
        } else if (STRICTWEEKDATE.matches(input) ) {
            return STRICT_WEEK_DATE;
        } else if (STRICTWEEKDATETIME.matches(input) ) {
            return STRICT_WEEK_DATE_TIME;
        } else if (STRICTWEEKDATETIMENOMILLIS.matches(input) ) {
            return STRICT_WEEK_DATE_TIME_NO_MILLIS;
        } else if (STRICTWEEKYEAR.matches(input) ) {
            return STRICT_WEEKYEAR;
        } else if (STRICTWEEKYEARWEEK.matches(input) ) {
            return STRICT_WEEKYEAR_WEEK;
        } else if (STRICTWEEKYEARWEEKDAY.matches(input) ) {
            return STRICT_WEEKYEAR_WEEK_DAY;
        } else if (STRICTYEAR.matches(input) ) {
            return STRICT_YEAR;
        } else if (STRICTYEARMONTH.matches(input) ) {
            return STRICT_YEAR_MONTH;
        } else if (STRICTYEARMONTHDAY.matches(input) ) {
            return STRICT_YEAR_MONTH_DAY;
        } else {
            try {
                return new JavaDateFormatter(input, new DateTimeFormatterBuilder().appendPattern(input).toFormatter(Locale.ROOT));
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException("Invalid format: [" + input + "]: " + e.getMessage(), e);
            }
        }
    }

    static JavaDateFormatter merge(String pattern, List<DateFormatter> formatters) {
        assert formatters.size() > 0;

        List<DateTimeFormatter> dateTimeFormatters = new ArrayList<>(formatters.size());
        DateTimeFormatterBuilder roundupBuilder = new DateTimeFormatterBuilder();
        DateTimeFormatter printer = null;
        for (DateFormatter formatter : formatters) {
            assert formatter instanceof JavaDateFormatter;
            JavaDateFormatter javaDateFormatter = (JavaDateFormatter) formatter;
            if (printer == null) {
                printer = javaDateFormatter.getPrinter();
            }
            dateTimeFormatters.addAll(javaDateFormatter.getParsers());
            roundupBuilder.appendOptional(javaDateFormatter.getRoundupParser());
        }
        DateTimeFormatter roundUpParser = roundupBuilder.toFormatter(Locale.ROOT);

        return new JavaDateFormatter(pattern, printer, builder -> builder.append(roundUpParser),
            dateTimeFormatters.toArray(new DateTimeFormatter[0]));
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
     *   of the new year is chosen (reataining BWC to joda time)
     * - If an iso based weekyear is found and an iso based weekyear week, the start
     *   of the day is used
     *
     * @param accessor The accessor returned from a parser
     *
     * @return The converted zoned date time
     */
    public static ZonedDateTime from(TemporalAccessor accessor) {
        if (accessor instanceof ZonedDateTime) {
            return (ZonedDateTime) accessor;
        }

        ZoneId zoneId = accessor.query(TemporalQueries.zone());
        if (zoneId == null) {
            zoneId = ZoneOffset.UTC;
        }

        LocalDate localDate = accessor.query(TemporalQueries.localDate());
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
            return of(getLocaldate(accessor), localTime, zoneId);
        } else if (accessor.isSupported(ChronoField.YEAR)) {
            if (accessor.isSupported(MONTH_OF_YEAR)) {
                return getFirstOfMonth(accessor).atStartOfDay(zoneId);
            } else {
                return Year.of(accessor.get(ChronoField.YEAR)).atDay(1).atStartOfDay(zoneId);
            }
        } else if (accessor.isSupported(MONTH_OF_YEAR)) {
            // missing year, falling back to the epoch and then filling
            return getLocaldate(accessor).atStartOfDay(zoneId);
        } else if (accessor.isSupported(WeekFields.ISO.weekBasedYear())) {
            if (accessor.isSupported(WeekFields.ISO.weekOfWeekBasedYear())) {
                return Year.of(accessor.get(WeekFields.ISO.weekBasedYear()))
                    .atDay(1)
                    .with(WeekFields.ISO.weekOfWeekBasedYear(), accessor.getLong(WeekFields.ISO.weekOfWeekBasedYear()))
                    .atStartOfDay(zoneId);
            } else {
                return Year.of(accessor.get(WeekFields.ISO.weekBasedYear()))
                    .atDay(1)
                    .with(TemporalAdjusters.firstInMonth(DayOfWeek.MONDAY))
                    .atStartOfDay(zoneId);
            }
        }

        // we should not reach this piece of code, everything being parsed we should be able to
        // convert to a zoned date time! If not, we have to extend the above methods
        throw new IllegalArgumentException("temporal accessor [" + accessor + "] cannot be converted to zoned date time");
    }

    private static LocalDate getLocaldate(TemporalAccessor accessor) {
        if (accessor.isSupported(MONTH_OF_YEAR)) {
            if (accessor.isSupported(DAY_OF_MONTH)) {
                return LocalDate.of(1970, accessor.get(MONTH_OF_YEAR), accessor.get(DAY_OF_MONTH));
            } else {
                return LocalDate.of(1970, accessor.get(MONTH_OF_YEAR), 1);
            }
        }

        return LOCALDATE_EPOCH;
    }

    @SuppressForbidden(reason = "ZonedDateTime.of is fine here")
    private static ZonedDateTime of(LocalDate localDate, LocalTime localTime, ZoneId zoneId) {
        return ZonedDateTime.of(localDate, localTime, zoneId);
    }

    @SuppressForbidden(reason = "LocalDate.of is fine here")
    private static LocalDate getFirstOfMonth(TemporalAccessor accessor) {
        return LocalDate.of(accessor.get(ChronoField.YEAR), accessor.get(MONTH_OF_YEAR), 1);
    }
}
