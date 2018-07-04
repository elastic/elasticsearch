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
import static java.time.temporal.ChronoField.SECOND_OF_MINUTE;

public class DateFormatters {

    private static final DateTimeFormatter TIME_ZONE_FORMATTER = new DateTimeFormatterBuilder()
        .optionalStart().appendZoneId().optionalEnd()
        .optionalStart().appendOffset("+HHmm", "Z").optionalEnd()
        .optionalStart().appendOffset("+HH:mm", "Z").optionalEnd()
        .toFormatter(Locale.ROOT);

    private static final DateTimeFormatter TIME_ZONE_FORMATTER_ZONE_ID = new DateTimeFormatterBuilder()
        .appendZoneId()
        .toFormatter(Locale.ROOT);

    private static final DateTimeFormatter TIME_ZONE_FORMATTER_WITHOUT_COLON = new DateTimeFormatterBuilder()
        .appendOffset("+HHmm", "Z")
        .toFormatter(Locale.ROOT);

    private static final DateTimeFormatter TIME_ZONE_FORMATTER_WITH_COLON = new DateTimeFormatterBuilder()
        .appendOffset("+HH:mm", "Z")
        .toFormatter(Locale.ROOT);

    public static final DateTimeFormatter OPTIONAL_TIME_ZONE_FORMATTER = new DateTimeFormatterBuilder()
        .optionalStart()
        .append(TIME_ZONE_FORMATTER)
        .optionalEnd()
        .toFormatter(Locale.ROOT);

    private static final DateTimeFormatter BASIC_TIME_NO_MILLIS = new DateTimeFormatterBuilder()
        .appendValue(HOUR_OF_DAY, 2, 2, SignStyle.NOT_NEGATIVE)
        .appendValue(MINUTE_OF_HOUR, 2, 2, SignStyle.NOT_NEGATIVE)
        .appendValue(SECOND_OF_MINUTE, 2, 2, SignStyle.NOT_NEGATIVE)
        .append(OPTIONAL_TIME_ZONE_FORMATTER)
        .toFormatter(Locale.ROOT);

    private static final DateTimeFormatter BASIC_TIME = new DateTimeFormatterBuilder()
        .appendValue(HOUR_OF_DAY, 2, 2, SignStyle.NOT_NEGATIVE)
        .appendValue(MINUTE_OF_HOUR, 2, 2, SignStyle.NOT_NEGATIVE)
        .appendValue(SECOND_OF_MINUTE, 2, 2, SignStyle.NOT_NEGATIVE)
        .appendFraction(MILLI_OF_SECOND, 1, 3, true)
        .append(OPTIONAL_TIME_ZONE_FORMATTER)
        .toFormatter(Locale.ROOT);

    private static final DateTimeFormatter BASIC_T_TIME = new DateTimeFormatterBuilder()
        .appendLiteral("T")
        .append(BASIC_TIME)
        .toFormatter(Locale.ROOT);

    private static final DateTimeFormatter BASIC_T_TIME_NO_MILLIS = new DateTimeFormatterBuilder()
        .appendLiteral("T")
        .append(BASIC_TIME_NO_MILLIS)
        .toFormatter(Locale.ROOT);

    private static final DateTimeFormatter BASIC_DATE_TIME = new DateTimeFormatterBuilder()
        .appendValue(ChronoField.YEAR, 4, 4, SignStyle.NORMAL)
        .appendValue(MONTH_OF_YEAR, 2, 2, SignStyle.NOT_NEGATIVE)
        .appendValue(DAY_OF_MONTH, 2, 2, SignStyle.NOT_NEGATIVE)
        .append(BASIC_T_TIME)
        .toFormatter(Locale.ROOT);

    private static final DateTimeFormatter BASIC_DATE_TIME_NO_MILLIS = new DateTimeFormatterBuilder()
        .appendValue(ChronoField.YEAR, 4, 4, SignStyle.NORMAL)
        .appendValue(MONTH_OF_YEAR, 2, 2, SignStyle.NOT_NEGATIVE)
        .appendValue(DAY_OF_MONTH, 2, 2, SignStyle.NOT_NEGATIVE)
        .append(BASIC_T_TIME_NO_MILLIS)
        .toFormatter(Locale.ROOT);

    private static final DateTimeFormatter BASIC_ORDINAL_DATE =
        DateTimeFormatter.ofPattern("yyyyDDD", Locale.ROOT);

    private static final DateTimeFormatter BASIC_ORDINAL_DATE_TIME = new DateTimeFormatterBuilder()
        .append(BASIC_ORDINAL_DATE)
        .append(BASIC_T_TIME)
        .toFormatter(Locale.ROOT);

    private static final DateTimeFormatter BASIC_ORDINAL_DATE_TIME_NO_MILLIS = new DateTimeFormatterBuilder()
        .append(BASIC_ORDINAL_DATE)
        .append(BASIC_T_TIME_NO_MILLIS)
        .toFormatter(Locale.ROOT);

    private static final DateTimeFormatter BASIC_WEEK_DATE = new DateTimeFormatterBuilder()
        .appendValue(IsoFields.WEEK_BASED_YEAR)
        .appendLiteral("W")
        .appendValue(IsoFields.WEEK_OF_WEEK_BASED_YEAR, 1, 2, SignStyle.NEVER)
        .appendValue(ChronoField.DAY_OF_WEEK)
        .toFormatter(Locale.ROOT);

    private static final DateTimeFormatter BASIC_WEEK_DATE_TIME_NO_MILLIS = new DateTimeFormatterBuilder()
        .append(BASIC_WEEK_DATE)
        .append(BASIC_T_TIME_NO_MILLIS)
        .toFormatter(Locale.ROOT);

    private static final DateTimeFormatter BASIC_WEEK_DATE_TIME = new DateTimeFormatterBuilder()
        .append(BASIC_WEEK_DATE)
        .append(BASIC_T_TIME)
        .toFormatter(Locale.ROOT);

    private static final DateTimeFormatter DATE = new DateTimeFormatterBuilder()
        .appendValue(ChronoField.YEAR, 1, 4, SignStyle.NORMAL)
        .appendLiteral('-')
        .appendValue(MONTH_OF_YEAR, 1, 2, SignStyle.NOT_NEGATIVE)
        .appendLiteral('-')
        .appendValue(DAY_OF_MONTH, 1, 2, SignStyle.NOT_NEGATIVE)
        .toFormatter(Locale.ROOT);

    private static final DateTimeFormatter HOUR = new DateTimeFormatterBuilder()
        .appendValue(HOUR_OF_DAY, 1, 2, SignStyle.NOT_NEGATIVE)
        .toFormatter(Locale.ROOT);

    private static final DateTimeFormatter DATE_HOUR = new DateTimeFormatterBuilder()
        .append(DATE)
        .appendLiteral("T")
        .append(HOUR)
        .toFormatter(Locale.ROOT);

    private static final DateTimeFormatter HOUR_MINUTE = new DateTimeFormatterBuilder()
        .appendValue(HOUR_OF_DAY, 1, 2, SignStyle.NOT_NEGATIVE)
        .appendLiteral(':')
        .appendValue(MINUTE_OF_HOUR, 1, 2, SignStyle.NOT_NEGATIVE)
        .toFormatter(Locale.ROOT);

    private static final DateTimeFormatter DATE_TIME_PREFIX = new DateTimeFormatterBuilder()
        .append(DATE)
        .appendLiteral('T')
        .append(HOUR_MINUTE)
        .optionalStart()
        .appendLiteral(':')
        .appendValue(SECOND_OF_MINUTE, 1, 2, SignStyle.NOT_NEGATIVE)
        .optionalEnd()
        .toFormatter(Locale.ROOT);

    // only the formatter, nothing optional here
    private static final DateTimeFormatter DATE_TIME_NO_MILLIS_FORMATTER = new DateTimeFormatterBuilder()
        .append(DATE)
        .appendLiteral('T')
        .append(HOUR_MINUTE)
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

    private static final DateTimeFormatter DATE_TIME = new DateTimeFormatterBuilder()
        .append(DATE)
        .appendLiteral('T')
        .append(HOUR_MINUTE)
        .optionalStart()
        .appendLiteral(':')
        .appendValue(SECOND_OF_MINUTE, 1, 2, SignStyle.NOT_NEGATIVE)
        .appendFraction(MILLI_OF_SECOND, 1, 3, true)
        .optionalEnd()
        .append(OPTIONAL_TIME_ZONE_FORMATTER)
        .toFormatter(Locale.ROOT);

    private static final DateTimeFormatter DATE_OPTIONAL_TIME = new DateTimeFormatterBuilder()
        .append(DATE)
        .parseLenient()
        .optionalStart()
        .appendLiteral('T')
        .append(HOUR_MINUTE)
        .optionalStart()
        .appendLiteral(':')
        .appendValue(SECOND_OF_MINUTE, 1, 2, SignStyle.NOT_NEGATIVE)
        .appendFraction(MILLI_OF_SECOND, 1, 3, true)
        .optionalEnd()
        .append(OPTIONAL_TIME_ZONE_FORMATTER)
        .optionalEnd()
        .toFormatter(Locale.ROOT);

    private static final DateTimeFormatter HOUR_MINUTE_SECOND = new DateTimeFormatterBuilder()
        .append(HOUR_MINUTE)
        .appendLiteral(":")
        .appendValue(SECOND_OF_MINUTE, 1, 2, SignStyle.NOT_NEGATIVE)
        .toFormatter(Locale.ROOT);

    private static final DateTimeFormatter DATE_HOUR_MINUTE_SECOND = new DateTimeFormatterBuilder()
        .append(DATE)
        .appendLiteral("T")
        .append(HOUR_MINUTE_SECOND)
        .toFormatter(Locale.ROOT);

    private static final DateTimeFormatter DATE_HOUR_MINUTE = new DateTimeFormatterBuilder()
        .append(DATE)
        .appendLiteral("T")
        .append(HOUR_MINUTE)
        .toFormatter(Locale.ROOT);

    private static final DateTimeFormatter HOUR_MINUTE_SECOND_MILLIS = new DateTimeFormatterBuilder()
        .appendValue(HOUR_OF_DAY, 1, 2, SignStyle.NOT_NEGATIVE)
        .appendLiteral(':')
        .appendValue(MINUTE_OF_HOUR, 1, 2, SignStyle.NOT_NEGATIVE)
        .appendLiteral(':')
        .appendValue(SECOND_OF_MINUTE, 1, 2, SignStyle.NOT_NEGATIVE)
        .appendFraction(MILLI_OF_SECOND, 1, 3, true)
        .toFormatter(Locale.ROOT);

    private static final DateTimeFormatter DATE_HOUR_MINUTE_SECOND_MILLIS = new DateTimeFormatterBuilder()
        .append(DATE)
        .appendLiteral("T")
        .append(HOUR_MINUTE_SECOND_MILLIS)
        .toFormatter(Locale.ROOT);

    private static final DateTimeFormatter DATE_HOUR_MINUTE_SECOND_FRACTION = new DateTimeFormatterBuilder()
        .append(DATE)
        .appendLiteral("T")
        .append(HOUR_MINUTE_SECOND_MILLIS)
        .toFormatter(Locale.ROOT);

    private static final DateTimeFormatter ORDINAL_DATE = new DateTimeFormatterBuilder()
        .appendValue(ChronoField.YEAR, 4, 10, SignStyle.EXCEEDS_PAD)
        .appendLiteral('-')
        .appendValue(DAY_OF_YEAR, 1, 3, SignStyle.NOT_NEGATIVE)
        .toFormatter(Locale.ROOT);

    private static final DateTimeFormatter ORDINAL_DATE_TIME_NO_MILLIS = new DateTimeFormatterBuilder()
        .append(ORDINAL_DATE)
        .appendLiteral('T')
        .append(HOUR_MINUTE_SECOND)
        .append(OPTIONAL_TIME_ZONE_FORMATTER)
        .toFormatter(Locale.ROOT);

    private static final DateTimeFormatter ORDINAL_DATE_TIME = new DateTimeFormatterBuilder()
        .append(ORDINAL_DATE)
        .appendLiteral('T')
        .append(HOUR_MINUTE)
        .optionalStart()
        .appendLiteral(':')
        .appendValue(SECOND_OF_MINUTE, 1, 2, SignStyle.NOT_NEGATIVE)
        .appendFraction(MILLI_OF_SECOND, 1, 3, true)
        .optionalEnd()
        .append(TIME_ZONE_FORMATTER)
        .toFormatter(Locale.ROOT);

    private static final DateTimeFormatter TIME = new DateTimeFormatterBuilder()
        .appendValue(HOUR_OF_DAY, 1, 2, SignStyle.NOT_NEGATIVE)
        .appendLiteral(':')
        .appendValue(MINUTE_OF_HOUR, 1, 2, SignStyle.NOT_NEGATIVE)
        .appendLiteral(':')
        .appendValue(SECOND_OF_MINUTE, 1, 2, SignStyle.NOT_NEGATIVE)
        .appendFraction(MILLI_OF_SECOND, 1, 3, true)
        .append(TIME_ZONE_FORMATTER)
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

    private static final DateTimeFormatter T_TIME = new DateTimeFormatterBuilder()
        .appendLiteral("T")
        .append(TIME)
        .toFormatter(Locale.ROOT);

    private static final DateTimeFormatter TIME_NO_MILLIS = new DateTimeFormatterBuilder()
        .appendValue(HOUR_OF_DAY, 1, 2, SignStyle.NOT_NEGATIVE)
        .appendLiteral(':')
        .appendValue(MINUTE_OF_HOUR, 1, 2, SignStyle.NOT_NEGATIVE)
        .appendLiteral(':')
        .appendValue(SECOND_OF_MINUTE, 1, 2, SignStyle.NOT_NEGATIVE)
        .append(TIME_ZONE_FORMATTER)
        .toFormatter(Locale.ROOT);

    private static final DateTimeFormatter T_TIME_NO_MILLIS = new DateTimeFormatterBuilder()
        .appendLiteral("T")
        .append(TIME_NO_MILLIS)
        .toFormatter(Locale.ROOT);

    private static final DateTimeFormatter WEEK_DATE = new DateTimeFormatterBuilder()
        .appendValue(IsoFields.WEEK_BASED_YEAR, 4, 10, SignStyle.EXCEEDS_PAD)
        .appendLiteral("-W")
        .appendValue(IsoFields.WEEK_OF_WEEK_BASED_YEAR, 1, 2, SignStyle.NOT_NEGATIVE)
        .appendLiteral('-')
        .appendValue(DAY_OF_WEEK, 1)
        .toFormatter(Locale.ROOT);

    private static final DateTimeFormatter WEEK_DATE_TIME_NO_MILLIS = new DateTimeFormatterBuilder()
        .append(WEEK_DATE)
        .append(T_TIME_NO_MILLIS)
        .toFormatter(Locale.ROOT);

    private static final DateTimeFormatter WEEK_DATE_TIME = new DateTimeFormatterBuilder()
        .append(WEEK_DATE)
        .append(T_TIME)
        .toFormatter(Locale.ROOT);

    private static final DateTimeFormatter WEEK_YEAR = new DateTimeFormatterBuilder()
        .appendValue(WeekFields.ISO.weekBasedYear())
        .toFormatter(Locale.ROOT);

    private static final DateTimeFormatter WEEKYEAR_WEEK = new DateTimeFormatterBuilder()
        .appendValue(WeekFields.ISO.weekBasedYear())
        .appendLiteral("-W")
        .appendValue(WeekFields.ISO.weekOfWeekBasedYear())
        .toFormatter(Locale.ROOT);

    private static final DateTimeFormatter WEEKYEAR_WEEK_DAY = new DateTimeFormatterBuilder()
        .appendValue(WeekFields.ISO.weekBasedYear())
        .appendLiteral("-W")
        .appendValue(WeekFields.ISO.weekOfWeekBasedYear())
        .appendLiteral("-")
        .appendValue(WeekFields.ISO.dayOfWeek())
        .toFormatter(Locale.ROOT);

    private static final DateTimeFormatter YEAR = new DateTimeFormatterBuilder()
        .appendValue(ChronoField.YEAR)
        .toFormatter(Locale.ROOT);

    private static final DateTimeFormatter YEAR_MONTH = new DateTimeFormatterBuilder()
        .appendValue(ChronoField.YEAR)
        .appendLiteral("-")
        .appendValue(MONTH_OF_YEAR)
        .toFormatter(Locale.ROOT);

    private static final DateTimeFormatter YEAR_MONTH_DAY = new DateTimeFormatterBuilder()
        .appendValue(ChronoField.YEAR)
        .appendLiteral("-")
        .appendValue(MONTH_OF_YEAR)
        .appendLiteral("-")
        .appendValue(DAY_OF_MONTH)
        .toFormatter(Locale.ROOT);

    private static final DateTimeFormatter EPOCH_SECOND = new DateTimeFormatterBuilder()
        .appendValue(ChronoField.INSTANT_SECONDS)
        .toFormatter(Locale.ROOT);

    private static final DateTimeFormatter EPOCH_MILLIS = new DateTimeFormatterBuilder()
        .appendValue(ChronoField.INSTANT_SECONDS, 1, 19, SignStyle.NEVER)
        .appendValue(ChronoField.MILLI_OF_SECOND, 3)
        .toFormatter(Locale.ROOT);

    private static final DateTimeFormatter STRICT_BASIC_WEEK_DATE = new DateTimeFormatterBuilder()
        .parseStrict()
        .appendValue(IsoFields.WEEK_BASED_YEAR, 4)
        .appendLiteral("W")
        .appendValue(IsoFields.WEEK_OF_WEEK_BASED_YEAR, 1, 2, SignStyle.NEVER)
        .appendValue(ChronoField.DAY_OF_WEEK)
        .toFormatter(Locale.ROOT);

    private static final DateTimeFormatter STRICT_BASIC_WEEK_DATE_TIME_NO_MILLIS = new DateTimeFormatterBuilder()
        .append(STRICT_BASIC_WEEK_DATE)
        .append(DateTimeFormatter.ofPattern("'T'HHmmssX", Locale.ROOT))
        .toFormatter(Locale.ROOT);

    private static final DateTimeFormatter STRICT_BASIC_WEEK_DATE_TIME = new DateTimeFormatterBuilder()
        .append(STRICT_BASIC_WEEK_DATE)
        .append(DateTimeFormatter.ofPattern("'T'HHmmss.SSSX", Locale.ROOT))
        .toFormatter(Locale.ROOT);

    private static final DateTimeFormatter STRICT_DATE = DateTimeFormatter.ISO_LOCAL_DATE.withResolverStyle(ResolverStyle.LENIENT);

    private static final DateTimeFormatter STRICT_DATE_HOUR = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH", Locale.ROOT);

    private static final DateTimeFormatter STRICT_DATE_HOUR_MINUTE = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm", Locale.ROOT);

    private static final DateTimeFormatter STRICT_YEAR_MONTH_DAY = new DateTimeFormatterBuilder()
        .appendValue(ChronoField.YEAR, 4, 10, SignStyle.EXCEEDS_PAD)
        .appendLiteral("-")
        .appendValue(MONTH_OF_YEAR, 2, 2, SignStyle.NOT_NEGATIVE)
        .appendLiteral('-')
        .appendValue(DAY_OF_MONTH, 2, 2, SignStyle.NOT_NEGATIVE)
        .toFormatter(Locale.ROOT);

    private static final DateTimeFormatter STRICT_YEAR_MONTH = new DateTimeFormatterBuilder()
        .appendValue(ChronoField.YEAR, 4, 10, SignStyle.EXCEEDS_PAD)
        .appendLiteral("-")
        .appendValue(MONTH_OF_YEAR, 2, 2, SignStyle.NOT_NEGATIVE)
        .toFormatter(Locale.ROOT);

    private static final DateTimeFormatter STRICT_YEAR = new DateTimeFormatterBuilder()
        .appendValue(ChronoField.YEAR, 4, 10, SignStyle.EXCEEDS_PAD)
        .toFormatter(Locale.ROOT);

    private static final DateTimeFormatter STRICT_HOUR_MINUTE_SECOND = new DateTimeFormatterBuilder()
        .appendValue(HOUR_OF_DAY, 2, 2, SignStyle.NOT_NEGATIVE)
        .appendLiteral(':')
        .appendValue(MINUTE_OF_HOUR, 2, 2, SignStyle.NOT_NEGATIVE)
        .appendLiteral(':')
        .appendValue(SECOND_OF_MINUTE, 2, 2, SignStyle.NOT_NEGATIVE)
        .toFormatter(Locale.ROOT);

    private static final DateTimeFormatter STRICT_DATE_TIME = new DateTimeFormatterBuilder()
        .append(STRICT_YEAR_MONTH_DAY)
        .appendLiteral('T')
        .append(STRICT_HOUR_MINUTE_SECOND)
        .optionalStart()
        .appendFraction(MILLI_OF_SECOND, 3, 3, true)
        .optionalEnd()
        .append(OPTIONAL_TIME_ZONE_FORMATTER)
        .toFormatter(Locale.ROOT);

    private static final DateTimeFormatter STRICT_DATE_OPTIONAL_TIME = new DateTimeFormatterBuilder()
        .append(STRICT_YEAR_MONTH_DAY)
        .optionalStart()
        .appendLiteral('T')
        .append(STRICT_HOUR_MINUTE_SECOND)
        .optionalStart()
        .appendFraction(MILLI_OF_SECOND, 3, 3, true)
        .optionalEnd()
        .append(OPTIONAL_TIME_ZONE_FORMATTER)
        .optionalEnd()
        .toFormatter(Locale.ROOT);

    private static final DateTimeFormatter STRICT_ORDINAL_DATE_TIME_NO_MILLIS = new DateTimeFormatterBuilder()
        .appendValue(ChronoField.YEAR, 4, 10, SignStyle.EXCEEDS_PAD)
        .appendLiteral('-')
        .appendValue(DAY_OF_YEAR, 3, 3, SignStyle.NOT_NEGATIVE)
        .appendLiteral('T')
        .append(STRICT_HOUR_MINUTE_SECOND)
        .append(OPTIONAL_TIME_ZONE_FORMATTER)
        .toFormatter(Locale.ROOT);

    private static final DateTimeFormatter STRICT_DATE_TIME_NO_MILLIS = new DateTimeFormatterBuilder()
        .append(STRICT_YEAR_MONTH_DAY)
        .appendLiteral('T')
        .append(STRICT_HOUR_MINUTE_SECOND)
        .append(OPTIONAL_TIME_ZONE_FORMATTER)
        .toFormatter(Locale.ROOT);

    private static final DateTimeFormatter STRICT_HOUR_MINUTE_SECOND_MILLIS = new DateTimeFormatterBuilder()
        .append(STRICT_HOUR_MINUTE_SECOND)
        .appendFraction(MILLI_OF_SECOND, 1, 3, true)
        .toFormatter(Locale.ROOT);

    private static final DateTimeFormatter STRICT_HOUR_MINUTE_SECOND_FRACTION = STRICT_HOUR_MINUTE_SECOND_MILLIS;

    private static final DateTimeFormatter STRICT_DATE_HOUR_MINUTE_SECOND_FRACTION = new DateTimeFormatterBuilder()
        .append(STRICT_YEAR_MONTH_DAY)
        .appendLiteral("T")
        .append(STRICT_HOUR_MINUTE_SECOND_FRACTION)
        .toFormatter(Locale.ROOT);

    private static final DateTimeFormatter STRICT_DATE_HOUR_MINUTE_SECOND_MILLIS = STRICT_DATE_HOUR_MINUTE_SECOND_FRACTION;

    private static final DateTimeFormatter STRICT_HOUR = DateTimeFormatter.ofPattern("HH", Locale.ROOT);

    private static final DateTimeFormatter STRICT_HOUR_MINUTE = DateTimeFormatter.ofPattern("HH:mm", Locale.ROOT);

    private static final DateTimeFormatter STRICT_ORDINAL_DATE_TIME = new DateTimeFormatterBuilder()
        .appendValue(ChronoField.YEAR, 4, 10, SignStyle.EXCEEDS_PAD)
        .appendLiteral('-')
        .appendValue(DAY_OF_YEAR, 3, 3, SignStyle.NOT_NEGATIVE)
        .appendLiteral('T')
        .append(STRICT_HOUR_MINUTE)
        .optionalStart()
        .appendLiteral(':')
        .appendValue(SECOND_OF_MINUTE, 2, 2, SignStyle.NOT_NEGATIVE)
        .appendFraction(MILLI_OF_SECOND, 1, 3, true)
        .optionalEnd()
        .append(OPTIONAL_TIME_ZONE_FORMATTER)
        .toFormatter(Locale.ROOT);

    private static final DateTimeFormatter STRICT_TIME = new DateTimeFormatterBuilder()
        .appendValue(HOUR_OF_DAY, 2, 2, SignStyle.NOT_NEGATIVE)
        .appendLiteral(':')
        .appendValue(MINUTE_OF_HOUR, 2, 2, SignStyle.NOT_NEGATIVE)
        .appendLiteral(':')
        .appendValue(SECOND_OF_MINUTE, 2, 2, SignStyle.NOT_NEGATIVE)
        .appendFraction(MILLI_OF_SECOND, 1, 3, true)
        .append(TIME_ZONE_FORMATTER)
        .toFormatter(Locale.ROOT);

    private static final DateTimeFormatter STRICT_T_TIME = new DateTimeFormatterBuilder()
        .appendLiteral("T")
        .append(STRICT_TIME)
        .toFormatter(Locale.ROOT);

    private static final DateTimeFormatter STRICT_TIME_NO_MILLIS = new DateTimeFormatterBuilder()
        .appendValue(HOUR_OF_DAY, 2, 2, SignStyle.NOT_NEGATIVE)
        .appendLiteral(':')
        .appendValue(MINUTE_OF_HOUR, 2, 2, SignStyle.NOT_NEGATIVE)
        .appendLiteral(':')
        .appendValue(SECOND_OF_MINUTE, 2, 2, SignStyle.NOT_NEGATIVE)
        .append(TIME_ZONE_FORMATTER)
        .toFormatter(Locale.ROOT);

    private static final DateTimeFormatter STRICT_T_TIME_NO_MILLIS = new DateTimeFormatterBuilder()
        .appendLiteral("T")
        .append(STRICT_TIME_NO_MILLIS)
        .toFormatter(Locale.ROOT);

    private static final DateTimeFormatter STRICT_WEEK_DATE = DateTimeFormatter.ISO_WEEK_DATE;

    private static final DateTimeFormatter STRICT_WEEK_DATE_TIME_NO_MILLIS = new DateTimeFormatterBuilder()
        .append(STRICT_WEEK_DATE)
        .append(STRICT_T_TIME_NO_MILLIS)
        .toFormatter(Locale.ROOT);

    private static final DateTimeFormatter STRICT_WEEK_DATE_TIME = new DateTimeFormatterBuilder()
        .append(STRICT_WEEK_DATE)
        .append(STRICT_T_TIME)
        .toFormatter(Locale.ROOT);

    private static final DateTimeFormatter STRICT_WEEKYEAR = new DateTimeFormatterBuilder()
        .appendValue(WeekFields.ISO.weekBasedYear(), 4, 10, SignStyle.EXCEEDS_PAD)
        .toFormatter(Locale.ROOT);

    private static final DateTimeFormatter STRICT_WEEKYEAR_WEEK = new DateTimeFormatterBuilder()
        .append(STRICT_WEEKYEAR)
        .appendLiteral("-W")
        .appendValue(WeekFields.ISO.weekOfWeekBasedYear(), 2, 2, SignStyle.NOT_NEGATIVE)
        .toFormatter(Locale.ROOT);

    private static final DateTimeFormatter STRICT_WEEKYEAR_WEEK_DAY = new DateTimeFormatterBuilder()
        .append(STRICT_WEEKYEAR_WEEK)
        .appendLiteral("-")
        .appendValue(WeekFields.ISO.dayOfWeek())
        .toFormatter(Locale.ROOT);

    public static DateFormatter forPattern(String input) {
        return forPattern(input, Locale.ROOT);
    }

    public static DateFormatter forPattern(String input, Locale locale) {
        if (Strings.hasLength(input)) {
            input = input.trim();
        }
        if (input == null || input.length() == 0) {
            throw new IllegalArgumentException("No date pattern provided");
        }

        // TODO prevent creation of all those new objects, make them static
        if ("basicDate".equals(input) || "basic_date".equals(input)) {
            return new DateFormatter(DateTimeFormatter.BASIC_ISO_DATE);
        } else if ("basicDateTime".equals(input) || "basic_date_time".equals(input)) {
            return new DateFormatter(BASIC_DATE_TIME);
        } else if ("basicDateTimeNoMillis".equals(input) || "basic_date_time_no_millis".equals(input)) {
            return new DateFormatter(BASIC_DATE_TIME_NO_MILLIS);
        } else if ("basicOrdinalDate".equals(input) || "basic_ordinal_date".equals(input)) {
            return new DateFormatter(BASIC_ORDINAL_DATE);
        } else if ("basicOrdinalDateTime".equals(input) || "basic_ordinal_date_time".equals(input)) {
            return new DateFormatter(BASIC_ORDINAL_DATE_TIME);
        } else if ("basicOrdinalDateTimeNoMillis".equals(input) || "basic_ordinal_date_time_no_millis".equals(input)) {
            return new DateFormatter(BASIC_ORDINAL_DATE_TIME_NO_MILLIS);
        } else if ("basicTime".equals(input) || "basic_time".equals(input)) {
            return new DateFormatter(BASIC_TIME);
        } else if ("basicTimeNoMillis".equals(input) || "basic_time_no_millis".equals(input)) {
            return new DateFormatter(BASIC_TIME_NO_MILLIS);
        } else if ("basicTTime".equals(input) || "basic_t_time".equals(input)) {
            return new DateFormatter(BASIC_T_TIME);
        } else if ("basicTTimeNoMillis".equals(input) || "basic_t_time_no_millis".equals(input)) {
            return new DateFormatter(BASIC_T_TIME_NO_MILLIS);
        } else if ("basicWeekDate".equals(input) || "basic_week_date".equals(input)) {
            return new DateFormatter(BASIC_WEEK_DATE);
        } else if ("basicWeekDateTime".equals(input) || "basic_week_date_time".equals(input)) {
            return new DateFormatter(BASIC_WEEK_DATE_TIME);
        } else if ("basicWeekDateTimeNoMillis".equals(input) || "basic_week_date_time_no_millis".equals(input)) {
            return new DateFormatter(BASIC_WEEK_DATE_TIME_NO_MILLIS);
        } else if ("date".equals(input)) {
            return new DateFormatter(DATE);
        } else if ("dateHour".equals(input) || "date_hour".equals(input)) {
            return new DateFormatter(DATE_HOUR);
        } else if ("dateHourMinute".equals(input) || "date_hour_minute".equals(input)) {
            return new DateFormatter(DATE_HOUR_MINUTE);
        } else if ("dateHourMinuteSecond".equals(input) || "date_hour_minute_second".equals(input)) {
            return new DateFormatter(DATE_HOUR_MINUTE_SECOND);
        } else if ("dateHourMinuteSecondFraction".equals(input) || "date_hour_minute_second_fraction".equals(input)) {
            return new DateFormatter(DATE_HOUR_MINUTE_SECOND_FRACTION);
        } else if ("dateHourMinuteSecondMillis".equals(input) || "date_hour_minute_second_millis".equals(input)) {
            return new DateFormatter(DATE_HOUR_MINUTE_SECOND_MILLIS);
        } else if ("dateOptionalTime".equals(input) || "date_optional_time".equals(input)) {
            return new DateFormatter(DATE_OPTIONAL_TIME);
        } else if ("dateTime".equals(input) || "date_time".equals(input)) {
            return new DateFormatter(DATE_TIME);
        } else if ("dateTimeNoMillis".equals(input) || "date_time_no_millis".equals(input)) {
            return new DateFormatter(STRICT_DATE_TIME_NO_MILLIS, DATE_TIME_NO_MILLIS_1, DATE_TIME_NO_MILLIS_2, DATE_TIME_NO_MILLIS_3,
                DATE_TIME_NO_MILLIS_4, DATE_TIME_NO_MILLIS_5, DATE_TIME_NO_MILLIS_6);
        } else if ("hour".equals(input)) {
            return new DateFormatter(HOUR);
        } else if ("hourMinute".equals(input) || "hour_minute".equals(input)) {
            return new DateFormatter(HOUR_MINUTE);
        } else if ("hourMinuteSecond".equals(input) || "hour_minute_second".equals(input)) {
            return new DateFormatter(HOUR_MINUTE_SECOND);
        } else if ("hourMinuteSecondFraction".equals(input) || "hour_minute_second_fraction".equals(input)) {
            return new DateFormatter(HOUR_MINUTE_SECOND_MILLIS);
        } else if ("hourMinuteSecondMillis".equals(input) || "hour_minute_second_millis".equals(input)) {
            return new DateFormatter(HOUR_MINUTE_SECOND_MILLIS);
        } else if ("ordinalDate".equals(input) || "ordinal_date".equals(input)) {
            return new DateFormatter(ORDINAL_DATE);
        } else if ("ordinalDateTime".equals(input) || "ordinal_date_time".equals(input)) {
            return new DateFormatter(ORDINAL_DATE_TIME);
        } else if ("ordinalDateTimeNoMillis".equals(input) || "ordinal_date_time_no_millis".equals(input)) {
            return new DateFormatter(ORDINAL_DATE_TIME_NO_MILLIS);
        } else if ("time".equals(input)) {
            return new DateFormatter(TIME_ZONE_ID, TIME_ZONE_WITH_COLON, TIME_ZONE_WITHOUT_COLON);
        } else if ("timeNoMillis".equals(input) || "time_no_millis".equals(input)) {
            return new DateFormatter(TIME_NO_MILLIS);
        } else if ("tTime".equals(input) || "t_time".equals(input)) {
            return new DateFormatter(T_TIME);
        } else if ("tTimeNoMillis".equals(input) || "t_time_no_millis".equals(input)) {
            return new DateFormatter(T_TIME_NO_MILLIS);
        } else if ("weekDate".equals(input) || "week_date".equals(input)) {
            return new DateFormatter(WEEK_DATE);
        } else if ("weekDateTime".equals(input) || "week_date_time".equals(input)) {
            return new DateFormatter(WEEK_DATE_TIME);
        } else if ("weekDateTimeNoMillis".equals(input) || "week_date_time_no_millis".equals(input)) {
            return new DateFormatter(WEEK_DATE_TIME_NO_MILLIS);
        } else if ("weekyear".equals(input) || "week_year".equals(input)) {
            return new DateFormatter(WEEK_YEAR);
        } else if ("weekyearWeek".equals(input) || "weekyear_week".equals(input)) {
            return new DateFormatter(WEEKYEAR_WEEK);
        } else if ("weekyearWeekDay".equals(input) || "weekyear_week_day".equals(input)) {
            return new DateFormatter(WEEKYEAR_WEEK_DAY);
        } else if ("year".equals(input)) {
            return new DateFormatter(YEAR);
        } else if ("yearMonth".equals(input) || "year_month".equals(input)) {
            return new DateFormatter(YEAR_MONTH);
        } else if ("yearMonthDay".equals(input) || "year_month_day".equals(input)) {
            return new DateFormatter(YEAR_MONTH_DAY);
        } else if ("epoch_second".equals(input)) {
            return new DateFormatter(EPOCH_SECOND);
        } else if ("epoch_millis".equals(input)) {
            return new DateFormatter(EPOCH_MILLIS);
        // strict date formats here, must be at least 4 digits for year and two for months and two for day
        } else if ("strictBasicWeekDate".equals(input) || "strict_basic_week_date".equals(input)) {
            return new DateFormatter(STRICT_BASIC_WEEK_DATE);
        } else if ("strictBasicWeekDateTime".equals(input) || "strict_basic_week_date_time".equals(input)) {
            return new DateFormatter(STRICT_BASIC_WEEK_DATE_TIME);
        } else if ("strictBasicWeekDateTimeNoMillis".equals(input) || "strict_basic_week_date_time_no_millis".equals(input)) {
            return new DateFormatter(STRICT_BASIC_WEEK_DATE_TIME_NO_MILLIS);
        } else if ("strictDate".equals(input) || "strict_date".equals(input)) {
            return new DateFormatter(STRICT_DATE);
        } else if ("strictDateHour".equals(input) || "strict_date_hour".equals(input)) {
            return new DateFormatter(STRICT_DATE_HOUR);
        } else if ("strictDateHourMinute".equals(input) || "strict_date_hour_minute".equals(input)) {
            return new DateFormatter(STRICT_DATE_HOUR_MINUTE);
        } else if ("strictDateHourMinuteSecond".equals(input) || "strict_date_hour_minute_second".equals(input)) {
            // TODO make this static
            return new DateFormatter(DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss", Locale.ROOT));
        } else if ("strictDateHourMinuteSecondFraction".equals(input) || "strict_date_hour_minute_second_fraction".equals(input)) {
            return new DateFormatter(STRICT_DATE_HOUR_MINUTE_SECOND_FRACTION);
        } else if ("strictDateHourMinuteSecondMillis".equals(input) || "strict_date_hour_minute_second_millis".equals(input)) {
            return new DateFormatter(STRICT_DATE_HOUR_MINUTE_SECOND_MILLIS);
        } else if ("strictDateOptionalTime".equals(input) || "strict_date_optional_time".equals(input)) {
            return new DateFormatter(STRICT_DATE_OPTIONAL_TIME);
        } else if ("strictDateTime".equals(input) || "strict_date_time".equals(input)) {
            return new DateFormatter(STRICT_DATE_TIME);
        } else if ("strictDateTimeNoMillis".equals(input) || "strict_date_time_no_millis".equals(input)) {
            return new DateFormatter(STRICT_DATE_TIME_NO_MILLIS);
        } else if ("strictHour".equals(input) || "strict_hour".equals(input)) {
            return new DateFormatter(STRICT_HOUR);
        } else if ("strictHourMinute".equals(input) || "strict_hour_minute".equals(input)) {
            return new DateFormatter(STRICT_HOUR_MINUTE);
        } else if ("strictHourMinuteSecond".equals(input) || "strict_hour_minute_second".equals(input)) {
            return new DateFormatter(STRICT_HOUR_MINUTE_SECOND);
        } else if ("strictHourMinuteSecondFraction".equals(input) || "strict_hour_minute_second_fraction".equals(input)) {
            return new DateFormatter(STRICT_HOUR_MINUTE_SECOND_FRACTION);
        } else if ("strictHourMinuteSecondMillis".equals(input) || "strict_hour_minute_second_millis".equals(input)) {
            return new DateFormatter(STRICT_HOUR_MINUTE_SECOND_MILLIS);
        } else if ("strictOrdinalDate".equals(input) || "strict_ordinal_date".equals(input)) {
            return new DateFormatter(DateTimeFormatter.ISO_ORDINAL_DATE);
        } else if ("strictOrdinalDateTime".equals(input) || "strict_ordinal_date_time".equals(input)) {
            return new DateFormatter(STRICT_ORDINAL_DATE_TIME);
        } else if ("strictOrdinalDateTimeNoMillis".equals(input) || "strict_ordinal_date_time_no_millis".equals(input)) {
            return new DateFormatter(STRICT_ORDINAL_DATE_TIME_NO_MILLIS);
        } else if ("strictTime".equals(input) || "strict_time".equals(input)) {
            return new DateFormatter(STRICT_TIME);
        } else if ("strictTimeNoMillis".equals(input) || "strict_time_no_millis".equals(input)) {
            return new DateFormatter(STRICT_TIME_NO_MILLIS);
        } else if ("strictTTime".equals(input) || "strict_t_time".equals(input)) {
            return new DateFormatter(STRICT_T_TIME);
        } else if ("strictTTimeNoMillis".equals(input) || "strict_t_time_no_millis".equals(input)) {
            return new DateFormatter(STRICT_T_TIME_NO_MILLIS);
        } else if ("strictWeekDate".equals(input) || "strict_week_date".equals(input)) {
            return new DateFormatter(STRICT_WEEK_DATE);
        } else if ("strictWeekDateTime".equals(input) || "strict_week_date_time".equals(input)) {
            return new DateFormatter(STRICT_WEEK_DATE_TIME);
        } else if ("strictWeekDateTimeNoMillis".equals(input) || "strict_week_date_time_no_millis".equals(input)) {
            return new DateFormatter(STRICT_WEEK_DATE_TIME_NO_MILLIS);
        } else if ("strictWeekyear".equals(input) || "strict_weekyear".equals(input)) {
            return new DateFormatter(STRICT_WEEKYEAR);
        } else if ("strictWeekyearWeek".equals(input) || "strict_weekyear_week".equals(input)) {
            return new DateFormatter(STRICT_WEEKYEAR_WEEK);
        } else if ("strictWeekyearWeekDay".equals(input) || "strict_weekyear_week_day".equals(input)) {
            return new DateFormatter(STRICT_WEEKYEAR_WEEK_DAY);
        } else if ("strictYear".equals(input) || "strict_year".equals(input)) {
            return new DateFormatter(STRICT_YEAR);
        } else if ("strictYearMonth".equals(input) || "strict_year_month".equals(input)) {
            return new DateFormatter(STRICT_YEAR_MONTH);
        } else if ("strictYearMonthDay".equals(input) || "strict_year_month_day".equals(input)) {
            return new DateFormatter(STRICT_YEAR_MONTH_DAY);
        } else if (Strings.hasLength(input) && input.contains("||")) {
            String[] formats = Strings.delimitedListToStringArray(input, "||");
            if (formats.length == 1) {
                return forPattern(formats[0], locale);
            } else {
                DateTimeFormatter[] formatters = new DateTimeFormatter[formats.length];
                for (int i = 0; i < formats.length; i++) {
                    try {
                        formatters[i] = forPattern(formats[i], locale).formatters()[0];
                    } catch (IllegalArgumentException e) {
                        throw new IllegalArgumentException("Invalid format: [" + input + "]: " + e.getMessage(), e);
                    }
                }

                return new DateFormatter(formatters);
            }
        } else {
            try {
                return new DateFormatter(new DateTimeFormatterBuilder().appendPattern(input).toFormatter(locale));
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException("Invalid format: [" + input + "]: " + e.getMessage(), e);
            }
        }
    }

    public static final ZonedDateTime EPOCH_ZONED_DATE_TIME = Instant.EPOCH.atZone(ZoneOffset.UTC);

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
            if (result.getZone().equals(ZoneOffset.UTC)) {
                result = result.withZoneSameLocal(ZoneOffset.ofTotalSeconds(accessor.get(ChronoField.OFFSET_SECONDS)));
            } else {
                result = result.withZoneSameInstant(ZoneOffset.ofTotalSeconds(accessor.get(ChronoField.OFFSET_SECONDS)));
            }
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
