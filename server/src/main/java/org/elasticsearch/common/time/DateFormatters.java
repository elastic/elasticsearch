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
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.Locale;

import static java.time.temporal.ChronoField.DAY_OF_MONTH;
import static java.time.temporal.ChronoField.DAY_OF_WEEK;
import static java.time.temporal.ChronoField.DAY_OF_YEAR;
import static java.time.temporal.ChronoField.HOUR_OF_DAY;
import static java.time.temporal.ChronoField.MILLI_OF_SECOND;
import static java.time.temporal.ChronoField.MINUTE_OF_HOUR;
import static java.time.temporal.ChronoField.MONTH_OF_YEAR;
import static java.time.temporal.ChronoField.SECOND_OF_MINUTE;

public class DateFormatters {

    private static final DateTimeFormatter TIME_ZONE_FORMATTER_ZONE_ID = new DateTimeFormatterBuilder()
        .appendZoneId()
        .toFormatter(Locale.ROOT);

    private static final DateTimeFormatter TIME_ZONE_FORMATTER_WITHOUT_COLON = new DateTimeFormatterBuilder()
        .appendOffset("+HHmm", "Z")
        .toFormatter(Locale.ROOT);

    private static final DateTimeFormatter TIME_ZONE_FORMATTER_WITH_COLON = new DateTimeFormatterBuilder()
        .appendOffset("+HH:mm", "Z")
        .toFormatter(Locale.ROOT);

    private static final DateTimeFormatter TIME_ZONE_FORMATTER = new DateTimeFormatterBuilder()
        .optionalStart().appendZoneId().optionalEnd()
        .optionalStart().appendOffset("+HHmm", "Z").optionalEnd()
        .optionalStart().appendOffset("+HH:mm", "Z").optionalEnd()
        .toFormatter(Locale.ROOT);

    private static final DateTimeFormatter OPTIONAL_TIME_ZONE_FORMATTER = new DateTimeFormatterBuilder()
        .optionalStart()
        .append(TIME_ZONE_FORMATTER)
        .optionalEnd()
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
        .append(TIME_ZONE_FORMATTER_WITHOUT_COLON)
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
        .append(TIME_ZONE_FORMATTER_WITH_COLON)
        .optionalEnd()
        .optionalEnd()
        .toFormatter(Locale.ROOT);

    private static final DateTimeFormatter STRICT_DATE_OPTIONAL_TIME_FORMATTER_3 = new DateTimeFormatterBuilder()
        .append(STRICT_YEAR_MONTH_DAY_FORMATTER)
        .optionalStart()
        .appendLiteral('T')
        .append(STRICT_HOUR_MINUTE_SECOND_FORMATTER)
        .optionalStart()
        .appendFraction(MILLI_OF_SECOND, 3, 3, true)
        .optionalEnd()
        .optionalStart()
        .append(TIME_ZONE_FORMATTER_ZONE_ID)
        .optionalEnd()
        .optionalEnd()
        .toFormatter(Locale.ROOT);

    private static final CompoundDateTimeFormatter STRICT_DATE_OPTIONAL_TIME =
        new CompoundDateTimeFormatter(STRICT_DATE_OPTIONAL_TIME_FORMATTER_1, STRICT_DATE_OPTIONAL_TIME_FORMATTER_2,
            STRICT_DATE_OPTIONAL_TIME_FORMATTER_3);

    private static final DateTimeFormatter BASIC_TIME_NO_MILLIS_FORMATTER = new DateTimeFormatterBuilder()
        .appendValue(HOUR_OF_DAY, 2, 2, SignStyle.NOT_NEGATIVE)
        .appendValue(MINUTE_OF_HOUR, 2, 2, SignStyle.NOT_NEGATIVE)
        .appendValue(SECOND_OF_MINUTE, 2, 2, SignStyle.NOT_NEGATIVE)
        .append(OPTIONAL_TIME_ZONE_FORMATTER)
        .toFormatter(Locale.ROOT);

    private static final CompoundDateTimeFormatter BASIC_TIME_NO_MILLIS = new CompoundDateTimeFormatter(BASIC_TIME_NO_MILLIS_FORMATTER);

    private static final DateTimeFormatter BASIC_TIME_FORMATTER = new DateTimeFormatterBuilder()
        .appendValue(HOUR_OF_DAY, 2, 2, SignStyle.NOT_NEGATIVE)
        .appendValue(MINUTE_OF_HOUR, 2, 2, SignStyle.NOT_NEGATIVE)
        .appendValue(SECOND_OF_MINUTE, 2, 2, SignStyle.NOT_NEGATIVE)
        .appendFraction(MILLI_OF_SECOND, 1, 3, true)
        .append(OPTIONAL_TIME_ZONE_FORMATTER)
        .toFormatter(Locale.ROOT);

    private static final CompoundDateTimeFormatter BASIC_TIME = new CompoundDateTimeFormatter(BASIC_TIME_FORMATTER);

    private static final DateTimeFormatter BASIC_T_TIME_FORMATTER = new DateTimeFormatterBuilder()
        .appendLiteral("T")
        .append(BASIC_TIME_FORMATTER)
        .toFormatter(Locale.ROOT);

    private static final CompoundDateTimeFormatter BASIC_T_TIME = new CompoundDateTimeFormatter(BASIC_T_TIME_FORMATTER);

    private static final CompoundDateTimeFormatter BASIC_T_TIME_NO_MILLIS = new CompoundDateTimeFormatter(new DateTimeFormatterBuilder()
        .appendLiteral("T")
        .append(BASIC_TIME_NO_MILLIS_FORMATTER)
        .toFormatter(Locale.ROOT));

    private static final CompoundDateTimeFormatter BASIC_DATE_TIME = new CompoundDateTimeFormatter(new DateTimeFormatterBuilder()
        .appendValue(ChronoField.YEAR, 4, 4, SignStyle.NORMAL)
        .appendValue(MONTH_OF_YEAR, 2, 2, SignStyle.NOT_NEGATIVE)
        .appendValue(DAY_OF_MONTH, 2, 2, SignStyle.NOT_NEGATIVE)
        .append(BASIC_T_TIME_FORMATTER)
        .toFormatter(Locale.ROOT));

    private static final CompoundDateTimeFormatter BASIC_DATE_TIME_NO_MILLIS = new CompoundDateTimeFormatter(new DateTimeFormatterBuilder()
        .appendValue(ChronoField.YEAR, 4, 4, SignStyle.NORMAL)
        .appendValue(MONTH_OF_YEAR, 2, 2, SignStyle.NOT_NEGATIVE)
        .appendValue(DAY_OF_MONTH, 2, 2, SignStyle.NOT_NEGATIVE)
        .appendLiteral("T")
        .append(BASIC_TIME_NO_MILLIS_FORMATTER)
        .toFormatter(Locale.ROOT));

    private static final CompoundDateTimeFormatter BASIC_ORDINAL_DATE = new CompoundDateTimeFormatter(
        DateTimeFormatter.ofPattern("yyyyDDD", Locale.ROOT));

    private static final CompoundDateTimeFormatter BASIC_ORDINAL_DATE_TIME = new CompoundDateTimeFormatter(new DateTimeFormatterBuilder()
        .appendPattern("yyyyDDD")
        .append(BASIC_T_TIME_FORMATTER)
        .toFormatter(Locale.ROOT));

    private static final CompoundDateTimeFormatter BASIC_ORDINAL_DATE_TIME_NO_MILLIS = new CompoundDateTimeFormatter(
        new DateTimeFormatterBuilder()
        .appendPattern("yyyyDDD")
        .appendLiteral("T")
        .append(BASIC_TIME_NO_MILLIS_FORMATTER)
        .toFormatter(Locale.ROOT));

    private static final DateTimeFormatter BASIC_WEEK_DATE_FORMATTER = new DateTimeFormatterBuilder()
        .appendValue(IsoFields.WEEK_BASED_YEAR)
        .appendLiteral("W")
        .appendValue(IsoFields.WEEK_OF_WEEK_BASED_YEAR, 1, 2, SignStyle.NEVER)
        .appendValue(ChronoField.DAY_OF_WEEK)
        .toFormatter(Locale.ROOT);

    private static final CompoundDateTimeFormatter BASIC_WEEK_DATE = new CompoundDateTimeFormatter(BASIC_WEEK_DATE_FORMATTER);

    private static final CompoundDateTimeFormatter BASIC_WEEK_DATE_TIME_NO_MILLIS = new CompoundDateTimeFormatter(
        new DateTimeFormatterBuilder()
        .append(BASIC_WEEK_DATE_FORMATTER)
        .appendLiteral("T")
        .append(BASIC_TIME_NO_MILLIS_FORMATTER)
        .toFormatter(Locale.ROOT));

    private static final CompoundDateTimeFormatter BASIC_WEEK_DATE_TIME = new CompoundDateTimeFormatter(new DateTimeFormatterBuilder()
        .append(BASIC_WEEK_DATE_FORMATTER)
        .append(BASIC_T_TIME_FORMATTER)
        .toFormatter(Locale.ROOT));

    private static final DateTimeFormatter DATE_FORMATTER = new DateTimeFormatterBuilder()
        .appendValue(ChronoField.YEAR, 1, 4, SignStyle.NORMAL)
        .appendLiteral('-')
        .appendValue(MONTH_OF_YEAR, 1, 2, SignStyle.NOT_NEGATIVE)
        .appendLiteral('-')
        .appendValue(DAY_OF_MONTH, 1, 2, SignStyle.NOT_NEGATIVE)
        .toFormatter(Locale.ROOT);

    private static final CompoundDateTimeFormatter DATE = new CompoundDateTimeFormatter(DATE_FORMATTER);

    private static final CompoundDateTimeFormatter HOUR = new CompoundDateTimeFormatter(new DateTimeFormatterBuilder()
        .appendValue(HOUR_OF_DAY, 1, 2, SignStyle.NOT_NEGATIVE)
        .toFormatter(Locale.ROOT));

    private static final CompoundDateTimeFormatter DATE_HOUR = new CompoundDateTimeFormatter(new DateTimeFormatterBuilder()
        .append(DATE_FORMATTER)
        .appendLiteral("T")
        .appendValue(HOUR_OF_DAY, 1, 2, SignStyle.NOT_NEGATIVE)
        .toFormatter(Locale.ROOT));

    private static final DateTimeFormatter HOUR_MINUTE_FORMATTER = new DateTimeFormatterBuilder()
        .appendValue(HOUR_OF_DAY, 1, 2, SignStyle.NOT_NEGATIVE)
        .appendLiteral(':')
        .appendValue(MINUTE_OF_HOUR, 1, 2, SignStyle.NOT_NEGATIVE)
        .toFormatter(Locale.ROOT);

    private static final CompoundDateTimeFormatter HOUR_MINUTE = new CompoundDateTimeFormatter(HOUR_MINUTE_FORMATTER);

    private static final DateTimeFormatter DATE_TIME_PREFIX = new DateTimeFormatterBuilder()
        .append(DATE_FORMATTER)
        .appendLiteral('T')
        .append(HOUR_MINUTE_FORMATTER)
        .optionalStart()
        .appendLiteral(':')
        .appendValue(SECOND_OF_MINUTE, 1, 2, SignStyle.NOT_NEGATIVE)
        .optionalEnd()
        .toFormatter(Locale.ROOT);

    // only the formatter, nothing optional here
    private static final DateTimeFormatter DATE_TIME_NO_MILLIS_FORMATTER = new DateTimeFormatterBuilder()
        .append(DATE_FORMATTER)
        .appendLiteral('T')
        .append(HOUR_MINUTE_FORMATTER)
        .appendLiteral(':')
        .appendValue(SECOND_OF_MINUTE, 1, 2, SignStyle.NOT_NEGATIVE)
        .appendZoneId()
        .toFormatter(Locale.ROOT);

    private static final DateTimeFormatter DATE_TIME_NO_MILLIS_1 = new DateTimeFormatterBuilder()
        .append(DATE_TIME_PREFIX)
        .append(TIME_ZONE_FORMATTER_WITH_COLON)
        .toFormatter(Locale.ROOT);

    private static final DateTimeFormatter DATE_TIME_NO_MILLIS_2 = new DateTimeFormatterBuilder()
        .append(DATE_TIME_PREFIX)
        .append(TIME_ZONE_FORMATTER_WITHOUT_COLON)
        .toFormatter(Locale.ROOT);

    private static final DateTimeFormatter DATE_TIME_NO_MILLIS_3 = new DateTimeFormatterBuilder()
        .append(DATE_TIME_PREFIX)
        .append(TIME_ZONE_FORMATTER_ZONE_ID)
        .toFormatter(Locale.ROOT);

    private static final DateTimeFormatter DATE_TIME_NO_MILLIS_4 = new DateTimeFormatterBuilder()
        .append(DATE_TIME_PREFIX)
        .optionalStart()
        .append(TIME_ZONE_FORMATTER_WITH_COLON)
        .optionalEnd()
        .toFormatter(Locale.ROOT);

    private static final DateTimeFormatter DATE_TIME_NO_MILLIS_5 = new DateTimeFormatterBuilder()
        .append(DATE_TIME_PREFIX)
        .optionalStart()
        .append(TIME_ZONE_FORMATTER_WITHOUT_COLON)
        .optionalEnd()
        .toFormatter(Locale.ROOT);

    private static final DateTimeFormatter DATE_TIME_NO_MILLIS_6 = new DateTimeFormatterBuilder()
        .append(DATE_TIME_PREFIX)
        .optionalStart()
        .append(TIME_ZONE_FORMATTER_ZONE_ID)
        .optionalEnd()
        .toFormatter(Locale.ROOT);

    private static final CompoundDateTimeFormatter DATE_TIME_NO_MILLIS = new CompoundDateTimeFormatter(DATE_TIME_NO_MILLIS_FORMATTER,
        DATE_TIME_NO_MILLIS_1,  DATE_TIME_NO_MILLIS_2, DATE_TIME_NO_MILLIS_3, DATE_TIME_NO_MILLIS_4, DATE_TIME_NO_MILLIS_5,
        DATE_TIME_NO_MILLIS_6);

    private static final CompoundDateTimeFormatter DATE_TIME = new CompoundDateTimeFormatter(new DateTimeFormatterBuilder()
        .append(DATE_FORMATTER)
        .appendLiteral('T')
        .append(HOUR_MINUTE_FORMATTER)
        .optionalStart()
        .appendLiteral(':')
        .appendValue(SECOND_OF_MINUTE, 1, 2, SignStyle.NOT_NEGATIVE)
        .appendFraction(MILLI_OF_SECOND, 1, 3, true)
        .optionalEnd()
        .append(OPTIONAL_TIME_ZONE_FORMATTER)
        .toFormatter(Locale.ROOT));

    private static final CompoundDateTimeFormatter DATE_OPTIONAL_TIME = new CompoundDateTimeFormatter(STRICT_DATE_OPTIONAL_TIME.printer,
        new DateTimeFormatterBuilder()
        .append(DATE_FORMATTER)
        .parseLenient()
        .optionalStart()
        .appendLiteral('T')
        .append(HOUR_MINUTE_FORMATTER)
        .optionalStart()
        .appendLiteral(':')
        .appendValue(SECOND_OF_MINUTE, 1, 2, SignStyle.NOT_NEGATIVE)
        .appendFraction(MILLI_OF_SECOND, 1, 3, true)
        .optionalEnd()
        .append(OPTIONAL_TIME_ZONE_FORMATTER)
        .optionalEnd()
        .toFormatter(Locale.ROOT));

    private static final DateTimeFormatter HOUR_MINUTE_SECOND_FORMATTER = new DateTimeFormatterBuilder()
        .append(HOUR_MINUTE_FORMATTER)
        .appendLiteral(":")
        .appendValue(SECOND_OF_MINUTE, 1, 2, SignStyle.NOT_NEGATIVE)
        .toFormatter(Locale.ROOT);

    private static final CompoundDateTimeFormatter HOUR_MINUTE_SECOND = new CompoundDateTimeFormatter(new DateTimeFormatterBuilder()
        .append(HOUR_MINUTE_FORMATTER)
        .appendLiteral(":")
        .appendValue(SECOND_OF_MINUTE, 1, 2, SignStyle.NOT_NEGATIVE)
        .toFormatter(Locale.ROOT));

    private static final CompoundDateTimeFormatter DATE_HOUR_MINUTE_SECOND = new CompoundDateTimeFormatter(new DateTimeFormatterBuilder()
        .append(DATE_FORMATTER)
        .appendLiteral("T")
        .append(HOUR_MINUTE_SECOND_FORMATTER)
        .toFormatter(Locale.ROOT));

    private static final CompoundDateTimeFormatter DATE_HOUR_MINUTE = new CompoundDateTimeFormatter(new DateTimeFormatterBuilder()
        .append(DATE_FORMATTER)
        .appendLiteral("T")
        .append(HOUR_MINUTE_FORMATTER)
        .toFormatter(Locale.ROOT));

    private static final DateTimeFormatter HOUR_MINUTE_SECOND_MILLIS_FORMATTER = new DateTimeFormatterBuilder()
        .appendValue(HOUR_OF_DAY, 1, 2, SignStyle.NOT_NEGATIVE)
        .appendLiteral(':')
        .appendValue(MINUTE_OF_HOUR, 1, 2, SignStyle.NOT_NEGATIVE)
        .appendLiteral(':')
        .appendValue(SECOND_OF_MINUTE, 1, 2, SignStyle.NOT_NEGATIVE)
        .appendFraction(MILLI_OF_SECOND, 1, 3, true)
        .toFormatter(Locale.ROOT);

    private static final CompoundDateTimeFormatter HOUR_MINUTE_SECOND_MILLIS =
        new CompoundDateTimeFormatter(HOUR_MINUTE_SECOND_MILLIS_FORMATTER);

    private static final CompoundDateTimeFormatter DATE_HOUR_MINUTE_SECOND_MILLIS =
        new CompoundDateTimeFormatter(new DateTimeFormatterBuilder()
        .append(DATE_FORMATTER)
        .appendLiteral("T")
        .append(HOUR_MINUTE_SECOND_MILLIS_FORMATTER)
        .toFormatter(Locale.ROOT));

    private static final CompoundDateTimeFormatter DATE_HOUR_MINUTE_SECOND_FRACTION =
        new CompoundDateTimeFormatter(new DateTimeFormatterBuilder()
        .append(DATE_FORMATTER)
        .appendLiteral("T")
        .append(HOUR_MINUTE_SECOND_MILLIS_FORMATTER)
        .toFormatter(Locale.ROOT));

    private static final DateTimeFormatter ORDINAL_DATE_FORMATTER = new DateTimeFormatterBuilder()
        .appendValue(ChronoField.YEAR, 4, 10, SignStyle.EXCEEDS_PAD)
        .appendLiteral('-')
        .appendValue(DAY_OF_YEAR, 1, 3, SignStyle.NOT_NEGATIVE)
        .toFormatter(Locale.ROOT);

    private static final CompoundDateTimeFormatter ORDINAL_DATE = new CompoundDateTimeFormatter(ORDINAL_DATE_FORMATTER);

    private static final CompoundDateTimeFormatter ORDINAL_DATE_TIME_NO_MILLIS = new CompoundDateTimeFormatter(
        new DateTimeFormatterBuilder()
        .append(ORDINAL_DATE_FORMATTER)
        .appendLiteral('T')
        .append(HOUR_MINUTE_SECOND_FORMATTER)
        .append(OPTIONAL_TIME_ZONE_FORMATTER)
        .toFormatter(Locale.ROOT));

    private static final CompoundDateTimeFormatter ORDINAL_DATE_TIME = new CompoundDateTimeFormatter(new DateTimeFormatterBuilder()
        .append(ORDINAL_DATE_FORMATTER)
        .appendLiteral('T')
        .append(HOUR_MINUTE_FORMATTER)
        .optionalStart()
        .appendLiteral(':')
        .appendValue(SECOND_OF_MINUTE, 1, 2, SignStyle.NOT_NEGATIVE)
        .appendFraction(MILLI_OF_SECOND, 1, 3, true)
        .optionalEnd()
        .append(TIME_ZONE_FORMATTER)
        .toFormatter(Locale.ROOT));

    private static final DateTimeFormatter TIME_FORMATTER_1 = new DateTimeFormatterBuilder()
        .appendValue(HOUR_OF_DAY, 1, 2, SignStyle.NOT_NEGATIVE)
        .appendLiteral(':')
        .appendValue(MINUTE_OF_HOUR, 1, 2, SignStyle.NOT_NEGATIVE)
        .appendLiteral(':')
        .appendValue(SECOND_OF_MINUTE, 1, 2, SignStyle.NOT_NEGATIVE)
        .appendFraction(MILLI_OF_SECOND, 1, 3, true)
        .append(TIME_ZONE_FORMATTER_ZONE_ID)
        .toFormatter(Locale.ROOT);

    private static final DateTimeFormatter TIME_FORMATTER_2 = new DateTimeFormatterBuilder()
        .appendValue(HOUR_OF_DAY, 1, 2, SignStyle.NOT_NEGATIVE)
        .appendLiteral(':')
        .appendValue(MINUTE_OF_HOUR, 1, 2, SignStyle.NOT_NEGATIVE)
        .appendLiteral(':')
        .appendValue(SECOND_OF_MINUTE, 1, 2, SignStyle.NOT_NEGATIVE)
        .appendFraction(MILLI_OF_SECOND, 1, 3, true)
        .append(TIME_ZONE_FORMATTER_WITH_COLON)
        .toFormatter(Locale.ROOT);

    private static final DateTimeFormatter TIME_FORMATTER_3 = new DateTimeFormatterBuilder()
        .appendValue(HOUR_OF_DAY, 1, 2, SignStyle.NOT_NEGATIVE)
        .appendLiteral(':')
        .appendValue(MINUTE_OF_HOUR, 1, 2, SignStyle.NOT_NEGATIVE)
        .appendLiteral(':')
        .appendValue(SECOND_OF_MINUTE, 1, 2, SignStyle.NOT_NEGATIVE)
        .appendFraction(MILLI_OF_SECOND, 1, 3, true)
        .append(TIME_ZONE_FORMATTER_WITHOUT_COLON)
        .toFormatter(Locale.ROOT);

    private static final DateTimeFormatter TIME_PREFIX = new DateTimeFormatterBuilder()
        .appendValue(HOUR_OF_DAY, 1, 2, SignStyle.NOT_NEGATIVE)
        .appendLiteral(':')
        .appendValue(MINUTE_OF_HOUR, 1, 2, SignStyle.NOT_NEGATIVE)
        .appendLiteral(':')
        .appendValue(SECOND_OF_MINUTE, 1, 2, SignStyle.NOT_NEGATIVE)
        .appendFraction(MILLI_OF_SECOND, 1, 3, true)
        .toFormatter(Locale.ROOT);

    private static final DateTimeFormatter TIME_ZONE_ID = new DateTimeFormatterBuilder()
        .append(TIME_PREFIX)
        .append(TIME_ZONE_FORMATTER_ZONE_ID)
        .toFormatter(Locale.ROOT);

    private static final DateTimeFormatter TIME_ZONE_WITH_COLON = new DateTimeFormatterBuilder()
        .append(TIME_PREFIX)
        .append(TIME_ZONE_FORMATTER_WITH_COLON)
        .toFormatter(Locale.ROOT);

    private static final DateTimeFormatter TIME_ZONE_WITHOUT_COLON = new DateTimeFormatterBuilder()
        .append(TIME_PREFIX)
        .append(TIME_ZONE_FORMATTER_WITHOUT_COLON)
        .toFormatter(Locale.ROOT);

    private static final CompoundDateTimeFormatter T_TIME = new CompoundDateTimeFormatter(
        new DateTimeFormatterBuilder().appendLiteral("T").append(TIME_FORMATTER_1).toFormatter(Locale.ROOT),
        new DateTimeFormatterBuilder().appendLiteral("T").append(TIME_FORMATTER_2).toFormatter(Locale.ROOT),
        new DateTimeFormatterBuilder().appendLiteral("T").append(TIME_FORMATTER_3).toFormatter(Locale.ROOT)
    );

    private static final DateTimeFormatter TIME_NO_MILLIS_FORMATTER_1 = new DateTimeFormatterBuilder()
        .appendValue(HOUR_OF_DAY, 1, 2, SignStyle.NOT_NEGATIVE)
        .appendLiteral(':')
        .appendValue(MINUTE_OF_HOUR, 1, 2, SignStyle.NOT_NEGATIVE)
        .appendLiteral(':')
        .appendValue(SECOND_OF_MINUTE, 1, 2, SignStyle.NOT_NEGATIVE)
        .append(TIME_ZONE_FORMATTER_ZONE_ID)
        .toFormatter(Locale.ROOT);

    private static final DateTimeFormatter TIME_NO_MILLIS_FORMATTER_2 = new DateTimeFormatterBuilder()
        .appendValue(HOUR_OF_DAY, 1, 2, SignStyle.NOT_NEGATIVE)
        .appendLiteral(':')
        .appendValue(MINUTE_OF_HOUR, 1, 2, SignStyle.NOT_NEGATIVE)
        .appendLiteral(':')
        .appendValue(SECOND_OF_MINUTE, 1, 2, SignStyle.NOT_NEGATIVE)
        .append(TIME_ZONE_FORMATTER_WITH_COLON)
        .toFormatter(Locale.ROOT);

    private static final DateTimeFormatter TIME_NO_MILLIS_FORMATTER_3 = new DateTimeFormatterBuilder()
        .appendValue(HOUR_OF_DAY, 1, 2, SignStyle.NOT_NEGATIVE)
        .appendLiteral(':')
        .appendValue(MINUTE_OF_HOUR, 1, 2, SignStyle.NOT_NEGATIVE)
        .appendLiteral(':')
        .appendValue(SECOND_OF_MINUTE, 1, 2, SignStyle.NOT_NEGATIVE)
        .append(TIME_ZONE_FORMATTER_WITHOUT_COLON)
        .toFormatter(Locale.ROOT);

    private static final CompoundDateTimeFormatter TIME = new CompoundDateTimeFormatter(TIME_ZONE_ID, TIME_ZONE_WITH_COLON,
        TIME_ZONE_WITHOUT_COLON);

    private static final CompoundDateTimeFormatter TIME_NO_MILLIS =
        new CompoundDateTimeFormatter(TIME_NO_MILLIS_FORMATTER_1, TIME_NO_MILLIS_FORMATTER_2, TIME_NO_MILLIS_FORMATTER_3);

    private static final DateTimeFormatter T_TIME_NO_MILLIS_FORMATTER_1 = new DateTimeFormatterBuilder()
        .appendLiteral("T")
        .append(TIME_NO_MILLIS_FORMATTER_1)
        .toFormatter(Locale.ROOT);

    private static final DateTimeFormatter T_TIME_NO_MILLIS_FORMATTER_2 = new DateTimeFormatterBuilder()
        .appendLiteral("T")
        .append(TIME_NO_MILLIS_FORMATTER_2)
        .toFormatter(Locale.ROOT);

    private static final DateTimeFormatter T_TIME_NO_MILLIS_FORMATTER_3 = new DateTimeFormatterBuilder()
        .appendLiteral("T")
        .append(TIME_NO_MILLIS_FORMATTER_3)
        .toFormatter(Locale.ROOT);

    private static final CompoundDateTimeFormatter T_TIME_NO_MILLIS =
        new CompoundDateTimeFormatter(T_TIME_NO_MILLIS_FORMATTER_1, T_TIME_NO_MILLIS_FORMATTER_2, T_TIME_NO_MILLIS_FORMATTER_3);

    private static final DateTimeFormatter WEEK_DATE_FORMATTER = new DateTimeFormatterBuilder()
        .appendValue(IsoFields.WEEK_BASED_YEAR, 4, 10, SignStyle.EXCEEDS_PAD)
        .appendLiteral("-W")
        .appendValue(IsoFields.WEEK_OF_WEEK_BASED_YEAR, 1, 2, SignStyle.NOT_NEGATIVE)
        .appendLiteral('-')
        .appendValue(DAY_OF_WEEK, 1)
        .toFormatter(Locale.ROOT);

    private static final CompoundDateTimeFormatter WEEK_DATE = new CompoundDateTimeFormatter(WEEK_DATE_FORMATTER);

    private static final CompoundDateTimeFormatter WEEK_DATE_TIME_NO_MILLIS = new CompoundDateTimeFormatter(
        new DateTimeFormatterBuilder().append(WEEK_DATE_FORMATTER).append(T_TIME_NO_MILLIS_FORMATTER_1).toFormatter(Locale.ROOT),
        new DateTimeFormatterBuilder().append(WEEK_DATE_FORMATTER).append(T_TIME_NO_MILLIS_FORMATTER_2).toFormatter(Locale.ROOT),
        new DateTimeFormatterBuilder().append(WEEK_DATE_FORMATTER).append(T_TIME_NO_MILLIS_FORMATTER_3).toFormatter(Locale.ROOT)
        );

    private static final CompoundDateTimeFormatter WEEK_DATE_TIME = new CompoundDateTimeFormatter(
        new DateTimeFormatterBuilder().append(WEEK_DATE_FORMATTER).appendLiteral("T").append(TIME_FORMATTER_1).toFormatter(Locale.ROOT),
        new DateTimeFormatterBuilder().append(WEEK_DATE_FORMATTER).appendLiteral("T").append(TIME_FORMATTER_2).toFormatter(Locale.ROOT),
        new DateTimeFormatterBuilder().append(WEEK_DATE_FORMATTER).appendLiteral("T").append(TIME_FORMATTER_3).toFormatter(Locale.ROOT)
    );

    private static final CompoundDateTimeFormatter WEEK_YEAR = new CompoundDateTimeFormatter(new DateTimeFormatterBuilder()
        .appendValue(WeekFields.ISO.weekBasedYear())
        .toFormatter(Locale.ROOT));

    private static final CompoundDateTimeFormatter WEEKYEAR_WEEK = new CompoundDateTimeFormatter(new DateTimeFormatterBuilder()
        .appendValue(WeekFields.ISO.weekBasedYear())
        .appendLiteral("-W")
        .appendValue(WeekFields.ISO.weekOfWeekBasedYear())
        .toFormatter(Locale.ROOT));

    private static final CompoundDateTimeFormatter WEEKYEAR_WEEK_DAY = new CompoundDateTimeFormatter(new DateTimeFormatterBuilder()
        .appendValue(WeekFields.ISO.weekBasedYear())
        .appendLiteral("-W")
        .appendValue(WeekFields.ISO.weekOfWeekBasedYear())
        .appendLiteral("-")
        .appendValue(WeekFields.ISO.dayOfWeek())
        .toFormatter(Locale.ROOT));

    private static final CompoundDateTimeFormatter YEAR = new CompoundDateTimeFormatter(new DateTimeFormatterBuilder()
        .appendValue(ChronoField.YEAR)
        .toFormatter(Locale.ROOT));

    private static final CompoundDateTimeFormatter YEAR_MONTH = new CompoundDateTimeFormatter(new DateTimeFormatterBuilder()
        .appendValue(ChronoField.YEAR)
        .appendLiteral("-")
        .appendValue(MONTH_OF_YEAR)
        .toFormatter(Locale.ROOT));

    private static final CompoundDateTimeFormatter YEAR_MONTH_DAY = new CompoundDateTimeFormatter(new DateTimeFormatterBuilder()
        .appendValue(ChronoField.YEAR)
        .appendLiteral("-")
        .appendValue(MONTH_OF_YEAR)
        .appendLiteral("-")
        .appendValue(DAY_OF_MONTH)
        .toFormatter(Locale.ROOT));

    private static final CompoundDateTimeFormatter EPOCH_SECOND = new CompoundDateTimeFormatter(new DateTimeFormatterBuilder()
        .appendValue(ChronoField.INSTANT_SECONDS)
        .toFormatter(Locale.ROOT));

    private static final CompoundDateTimeFormatter EPOCH_MILLIS = new CompoundDateTimeFormatter(new DateTimeFormatterBuilder()
        .appendValue(ChronoField.INSTANT_SECONDS, 1, 19, SignStyle.NEVER)
        .appendValue(ChronoField.MILLI_OF_SECOND, 3)
        .toFormatter(Locale.ROOT));

    private static final DateTimeFormatter STRICT_BASIC_WEEK_DATE_FORMATTER = new DateTimeFormatterBuilder()
        .parseStrict()
        .appendValue(IsoFields.WEEK_BASED_YEAR, 4)
        .appendLiteral("W")
        .appendValue(IsoFields.WEEK_OF_WEEK_BASED_YEAR, 1, 2, SignStyle.NEVER)
        .appendValue(ChronoField.DAY_OF_WEEK)
        .toFormatter(Locale.ROOT);

    private static final CompoundDateTimeFormatter STRICT_BASIC_WEEK_DATE = new CompoundDateTimeFormatter(STRICT_BASIC_WEEK_DATE_FORMATTER);

    private static final CompoundDateTimeFormatter STRICT_BASIC_WEEK_DATE_TIME_NO_MILLIS = new CompoundDateTimeFormatter(
        new DateTimeFormatterBuilder()
        .append(STRICT_BASIC_WEEK_DATE_FORMATTER)
        .append(DateTimeFormatter.ofPattern("'T'HHmmssX", Locale.ROOT))
        .toFormatter(Locale.ROOT));

    private static final CompoundDateTimeFormatter STRICT_BASIC_WEEK_DATE_TIME = new CompoundDateTimeFormatter(
        new DateTimeFormatterBuilder()
        .append(STRICT_BASIC_WEEK_DATE_FORMATTER)
        .append(DateTimeFormatter.ofPattern("'T'HHmmss.SSSX", Locale.ROOT))
        .toFormatter(Locale.ROOT));

    private static final CompoundDateTimeFormatter STRICT_DATE = new CompoundDateTimeFormatter(
        DateTimeFormatter.ISO_LOCAL_DATE.withResolverStyle(ResolverStyle.LENIENT));

    private static final CompoundDateTimeFormatter STRICT_DATE_HOUR = new CompoundDateTimeFormatter(
        DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH", Locale.ROOT));

    private static final CompoundDateTimeFormatter STRICT_DATE_HOUR_MINUTE = new CompoundDateTimeFormatter(
        DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm", Locale.ROOT));

    private static final CompoundDateTimeFormatter STRICT_YEAR_MONTH_DAY = new CompoundDateTimeFormatter(STRICT_YEAR_MONTH_DAY_FORMATTER);

    private static final CompoundDateTimeFormatter STRICT_YEAR_MONTH = new CompoundDateTimeFormatter(new DateTimeFormatterBuilder()
        .appendValue(ChronoField.YEAR, 4, 10, SignStyle.EXCEEDS_PAD)
        .appendLiteral("-")
        .appendValue(MONTH_OF_YEAR, 2, 2, SignStyle.NOT_NEGATIVE)
        .toFormatter(Locale.ROOT));

    private static final CompoundDateTimeFormatter STRICT_YEAR = new CompoundDateTimeFormatter(new DateTimeFormatterBuilder()
        .appendValue(ChronoField.YEAR, 4, 10, SignStyle.EXCEEDS_PAD)
        .toFormatter(Locale.ROOT));

    private static final CompoundDateTimeFormatter STRICT_HOUR_MINUTE_SECOND =
        new CompoundDateTimeFormatter(STRICT_HOUR_MINUTE_SECOND_FORMATTER);

    private static final CompoundDateTimeFormatter STRICT_DATE_TIME = new CompoundDateTimeFormatter(new DateTimeFormatterBuilder()
        .append(STRICT_YEAR_MONTH_DAY_FORMATTER)
        .appendLiteral('T')
        .append(STRICT_HOUR_MINUTE_SECOND_FORMATTER)
        .optionalStart()
        .appendFraction(MILLI_OF_SECOND, 3, 3, true)
        .optionalEnd()
        .append(OPTIONAL_TIME_ZONE_FORMATTER)
        .toFormatter(Locale.ROOT));

    private static final CompoundDateTimeFormatter STRICT_ORDINAL_DATE_TIME_NO_MILLIS = new CompoundDateTimeFormatter(
        new DateTimeFormatterBuilder()
        .appendValue(ChronoField.YEAR, 4, 10, SignStyle.EXCEEDS_PAD)
        .appendLiteral('-')
        .appendValue(DAY_OF_YEAR, 3, 3, SignStyle.NOT_NEGATIVE)
        .appendLiteral('T')
        .append(STRICT_HOUR_MINUTE_SECOND_FORMATTER)
        .append(OPTIONAL_TIME_ZONE_FORMATTER)
        .toFormatter(Locale.ROOT));

    private static final CompoundDateTimeFormatter STRICT_DATE_TIME_NO_MILLIS = new CompoundDateTimeFormatter(new DateTimeFormatterBuilder()
        .append(STRICT_YEAR_MONTH_DAY_FORMATTER)
        .appendLiteral('T')
        .append(STRICT_HOUR_MINUTE_SECOND_FORMATTER)
        .append(OPTIONAL_TIME_ZONE_FORMATTER)
        .toFormatter(Locale.ROOT));

    private static final DateTimeFormatter STRICT_HOUR_MINUTE_SECOND_MILLIS_FORMATTER = new DateTimeFormatterBuilder()
        .append(STRICT_HOUR_MINUTE_SECOND_FORMATTER)
        .appendFraction(MILLI_OF_SECOND, 1, 3, true)
        .toFormatter(Locale.ROOT);

    private static final CompoundDateTimeFormatter STRICT_HOUR_MINUTE_SECOND_MILLIS =
        new CompoundDateTimeFormatter(STRICT_HOUR_MINUTE_SECOND_MILLIS_FORMATTER);

    private static final CompoundDateTimeFormatter STRICT_HOUR_MINUTE_SECOND_FRACTION = STRICT_HOUR_MINUTE_SECOND_MILLIS;

    private static final CompoundDateTimeFormatter STRICT_DATE_HOUR_MINUTE_SECOND_FRACTION = new CompoundDateTimeFormatter(
        new DateTimeFormatterBuilder()
        .append(STRICT_YEAR_MONTH_DAY_FORMATTER)
        .appendLiteral("T")
        .append(STRICT_HOUR_MINUTE_SECOND_MILLIS_FORMATTER)
        .toFormatter(Locale.ROOT));

    private static final CompoundDateTimeFormatter STRICT_DATE_HOUR_MINUTE_SECOND_MILLIS = STRICT_DATE_HOUR_MINUTE_SECOND_FRACTION;

    private static final CompoundDateTimeFormatter STRICT_HOUR =
        new CompoundDateTimeFormatter(DateTimeFormatter.ofPattern("HH", Locale.ROOT));

    private static final CompoundDateTimeFormatter STRICT_HOUR_MINUTE =
        new CompoundDateTimeFormatter(DateTimeFormatter.ofPattern("HH:mm", Locale.ROOT));

    private static final CompoundDateTimeFormatter STRICT_ORDINAL_DATE_TIME = new CompoundDateTimeFormatter(new DateTimeFormatterBuilder()
        .appendValue(ChronoField.YEAR, 4, 10, SignStyle.EXCEEDS_PAD)
        .appendLiteral('-')
        .appendValue(DAY_OF_YEAR, 3, 3, SignStyle.NOT_NEGATIVE)
        .appendLiteral('T')
        .appendPattern("HH:mm")
        .optionalStart()
        .appendLiteral(':')
        .appendValue(SECOND_OF_MINUTE, 2, 2, SignStyle.NOT_NEGATIVE)
        .appendFraction(MILLI_OF_SECOND, 1, 3, true)
        .optionalEnd()
        .append(OPTIONAL_TIME_ZONE_FORMATTER)
        .toFormatter(Locale.ROOT));

    private static final DateTimeFormatter STRICT_TIME_FORMATTER = new DateTimeFormatterBuilder()
        .appendValue(HOUR_OF_DAY, 2, 2, SignStyle.NOT_NEGATIVE)
        .appendLiteral(':')
        .appendValue(MINUTE_OF_HOUR, 2, 2, SignStyle.NOT_NEGATIVE)
        .appendLiteral(':')
        .appendValue(SECOND_OF_MINUTE, 2, 2, SignStyle.NOT_NEGATIVE)
        .appendFraction(MILLI_OF_SECOND, 1, 3, true)
        .append(TIME_ZONE_FORMATTER)
        .toFormatter(Locale.ROOT);

    private static final CompoundDateTimeFormatter STRICT_TIME = new CompoundDateTimeFormatter(STRICT_TIME_FORMATTER);

    private static final DateTimeFormatter STRICT_T_TIME_FORMATTER = new DateTimeFormatterBuilder()
        .appendLiteral("T")
        .append(STRICT_TIME_FORMATTER)
        .toFormatter(Locale.ROOT);

    private static final CompoundDateTimeFormatter STRICT_T_TIME = new CompoundDateTimeFormatter(STRICT_T_TIME_FORMATTER);

    private static final DateTimeFormatter STRICT_TIME_NO_MILLIS_FORMATTER = new DateTimeFormatterBuilder()
        .appendValue(HOUR_OF_DAY, 2, 2, SignStyle.NOT_NEGATIVE)
        .appendLiteral(':')
        .appendValue(MINUTE_OF_HOUR, 2, 2, SignStyle.NOT_NEGATIVE)
        .appendLiteral(':')
        .appendValue(SECOND_OF_MINUTE, 2, 2, SignStyle.NOT_NEGATIVE)
        .append(TIME_ZONE_FORMATTER)
        .toFormatter(Locale.ROOT);

    private static final CompoundDateTimeFormatter STRICT_TIME_NO_MILLIS = new CompoundDateTimeFormatter(STRICT_TIME_NO_MILLIS_FORMATTER);

    private static final DateTimeFormatter STRICT_T_TIME_NO_MILLIS_FORMATTER = new DateTimeFormatterBuilder()
        .appendLiteral("T")
        .append(STRICT_TIME_NO_MILLIS_FORMATTER)
        .toFormatter(Locale.ROOT);

    private static final CompoundDateTimeFormatter STRICT_T_TIME_NO_MILLIS =
        new CompoundDateTimeFormatter(STRICT_T_TIME_NO_MILLIS_FORMATTER);

    private static final CompoundDateTimeFormatter STRICT_WEEK_DATE = new CompoundDateTimeFormatter(DateTimeFormatter.ISO_WEEK_DATE);

    private static final CompoundDateTimeFormatter STRICT_WEEK_DATE_TIME_NO_MILLIS = new CompoundDateTimeFormatter(
        new DateTimeFormatterBuilder()
        .append(DateTimeFormatter.ISO_WEEK_DATE)
        .append(STRICT_T_TIME_NO_MILLIS_FORMATTER)
        .toFormatter(Locale.ROOT));

    private static final CompoundDateTimeFormatter STRICT_WEEK_DATE_TIME = new CompoundDateTimeFormatter(new DateTimeFormatterBuilder()
        .append(DateTimeFormatter.ISO_WEEK_DATE)
        .append(STRICT_T_TIME_FORMATTER)
        .toFormatter(Locale.ROOT));

    private static final CompoundDateTimeFormatter STRICT_WEEKYEAR = new CompoundDateTimeFormatter(new DateTimeFormatterBuilder()
        .appendValue(WeekFields.ISO.weekBasedYear(), 4, 10, SignStyle.EXCEEDS_PAD)
        .toFormatter(Locale.ROOT));

    private static final DateTimeFormatter STRICT_WEEKYEAR_WEEK_FORMATTER = new DateTimeFormatterBuilder()
        .appendValue(WeekFields.ISO.weekBasedYear(), 4, 10, SignStyle.EXCEEDS_PAD)
        .appendLiteral("-W")
        .appendValue(WeekFields.ISO.weekOfWeekBasedYear(), 2, 2, SignStyle.NOT_NEGATIVE)
        .toFormatter(Locale.ROOT);

    private static final CompoundDateTimeFormatter STRICT_WEEKYEAR_WEEK = new CompoundDateTimeFormatter(STRICT_WEEKYEAR_WEEK_FORMATTER);

    private static final CompoundDateTimeFormatter STRICT_WEEKYEAR_WEEK_DAY = new CompoundDateTimeFormatter(new DateTimeFormatterBuilder()
        .append(STRICT_WEEKYEAR_WEEK_FORMATTER)
        .appendLiteral("-")
        .appendValue(WeekFields.ISO.dayOfWeek())
        .toFormatter(Locale.ROOT));

    private static final CompoundDateTimeFormatter BASIC_ISO_DATE = new CompoundDateTimeFormatter(DateTimeFormatter.BASIC_ISO_DATE);
    private static final CompoundDateTimeFormatter ISO_ORDINAL_DATE = new CompoundDateTimeFormatter(DateTimeFormatter.ISO_ORDINAL_DATE);
    private static final CompoundDateTimeFormatter STRICT_DATE_HOUR_MINUTE_SECOND =
        new CompoundDateTimeFormatter(DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss", Locale.ROOT));

    public static CompoundDateTimeFormatter forPattern(String input) {
        return forPattern(input, Locale.ROOT);
    }

    public static CompoundDateTimeFormatter forPattern(String input, Locale locale) {
        if (Strings.hasLength(input)) {
            input = input.trim();
        }
        if (input == null || input.length() == 0) {
            throw new IllegalArgumentException("No date pattern provided");
        }

        if ("basicDate".equals(input) || "basic_date".equals(input)) {
            return BASIC_ISO_DATE;
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
            return EPOCH_SECOND;
        } else if ("epoch_millis".equals(input)) {
            return EPOCH_MILLIS;
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
            return ISO_ORDINAL_DATE;
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
                Collection<DateTimeFormatter> parsers = new LinkedHashSet<>(formats.length);
                for (String format : formats) {
                    CompoundDateTimeFormatter dateTimeFormatter = forPattern(format, locale);
                    try {
                        parsers.addAll(Arrays.asList(dateTimeFormatter.parsers));
                    } catch (IllegalArgumentException e) {
                        throw new IllegalArgumentException("Invalid format: [" + input + "]: " + e.getMessage(), e);
                    }
                }

                return new CompoundDateTimeFormatter(parsers.toArray(new DateTimeFormatter[0]));
            }
        } else {
            try {
                return new CompoundDateTimeFormatter(new DateTimeFormatterBuilder().appendPattern(input).toFormatter(locale));
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
