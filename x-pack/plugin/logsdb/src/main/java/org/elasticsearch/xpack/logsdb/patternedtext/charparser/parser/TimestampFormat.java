/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logsdb.patternedtext.charparser.parser;

import org.elasticsearch.xpack.logsdb.patternedtext.charparser.common.TimestampComponentType;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Locale;

public final class TimestampFormat {

    // Default year for yearless timestamp formats (BSD/RFC-3164 syslog: "MMM dd HH:mm:ss"). Chosen as a leap
    // year so that Feb 29 is a valid calendar date; the true year is not present in the log line. This only
    // affects the computed value, never the %T template, and is deterministic (no clock read) to keep parsing pure.
    private static final int DEFAULT_YEAR_WHEN_ABSENT = 2000;

    private final String javaTimeFormat;
    private final DateTimeFormatter dateTimeFormatter;

    /**
     * Indicates the order of timestamp components in the format for the eventual calculation of the timestamp.
     * This array's length equals to the number of {@link TimestampComponentType} instances. The value in index i in this array represents
     * the index of the timestamp component in the input array that corresponds to the i-th component in this timestamp format.
     * Whenever the timestamp format does not include a specific component, the corresponding index in this array will be -1.
     * For example, if the format is "yyyy-MM-dd HH:mm:ss TZh:TZm", then the order array might look like this:
     * [0, 1, 2, 3, -1, 4, 5, -1, -1, -1, 6, 7, -1] and the input array for the timestamp 2025-12-31 15:30:45 +02:00 will be:
     * [2025, 12, 31, 15, 0, 30, 45, 0, 0, 0, 2, 0, 0] (see {@link TimestampComponentType} for the meaning of each index).
     */
    private final int[] timestampComponentsOrder;

    private final int numTimestampComponents;

    private final int yearIndex;
    private final int twoDigitYearIndex;
    private final int compactDateIndex;
    private final int compactTimeIndex;
    private final int compactDateYyyymmddIndex;
    private final int monthIndex;
    private final int dayIndex;
    private final int hourIndex;
    private final int amPmIndex;
    private final int minuteIndex;
    private final int secondIndex;
    private final int millisecondIndex;
    private final int microsecondIndex;
    private final int nanosecondIndex;
    private final int timezoneOffsetHoursIndex;
    private final int timezoneOffsetMinutesIndex;
    private final int timezoneOffsetHoursAndMinutesIndex;

    public TimestampFormat(String javaTimeFormat, int[] timestampComponentsOrder) {
        this.javaTimeFormat = javaTimeFormat;
        boolean amPm = false;

        this.timestampComponentsOrder = timestampComponentsOrder;
        int timestampComponentsCount = 0;
        this.yearIndex = timestampComponentsOrder[TimestampComponentType.YEAR_CODE];
        this.twoDigitYearIndex = timestampComponentsOrder[TimestampComponentType.TWO_DIGIT_YEAR_CODE];
        this.compactDateIndex = timestampComponentsOrder[TimestampComponentType.COMPACT_DATE_YYMMDD_CODE];
        this.compactTimeIndex = timestampComponentsOrder[TimestampComponentType.COMPACT_TIME_HHMMSS_CODE];
        this.compactDateYyyymmddIndex = timestampComponentsOrder[TimestampComponentType.COMPACT_DATE_YYYYMMDD_CODE];
        // A year is OPTIONAL: BSD/RFC-3164 syslog timestamps ("MMM dd HH:mm:ss", e.g. Linux/OpenSSH) carry no
        // year. When none is present, toTimestamp() substitutes DEFAULT_YEAR_WHEN_ABSENT. Month, day, hour,
        // minute and second stay required (enforced below), so a bare time can still never become a timestamp.
        // A compact date (yymmdd or yyyymmdd) carries year+month+day in one value, so it satisfies all three.
        if (yearIndex >= 0 || twoDigitYearIndex >= 0 || compactDateIndex >= 0 || compactDateYyyymmddIndex >= 0) {
            timestampComponentsCount++;
        }
        this.monthIndex = timestampComponentsOrder[TimestampComponentType.MONTH_CODE];
        if (monthIndex < 0 && compactDateIndex < 0 && compactDateYyyymmddIndex < 0) {
            throw new IllegalArgumentException("Timestamp format must include a month component");
        }
        timestampComponentsCount++;
        this.dayIndex = timestampComponentsOrder[TimestampComponentType.DAY_CODE];
        if (dayIndex < 0 && compactDateIndex < 0 && compactDateYyyymmddIndex < 0) {
            throw new IllegalArgumentException("Timestamp format must include a day component");
        }
        timestampComponentsCount++;
        this.hourIndex = timestampComponentsOrder[TimestampComponentType.HOUR_CODE];
        if (hourIndex < 0 && compactTimeIndex < 0) {
            throw new IllegalArgumentException("Timestamp format must include an hour component");
        }
        timestampComponentsCount++;
        this.amPmIndex = timestampComponentsOrder[TimestampComponentType.AM_PM_CODE];
        if (amPmIndex >= 0) {
            timestampComponentsCount++;
            amPm = true;
        }
        this.minuteIndex = timestampComponentsOrder[TimestampComponentType.MINUTE_CODE];
        if (minuteIndex < 0 && compactTimeIndex < 0) {
            throw new IllegalArgumentException("Timestamp format must include a minute component");
        }
        timestampComponentsCount++;
        this.secondIndex = timestampComponentsOrder[TimestampComponentType.SECOND_CODE];
        if (secondIndex < 0 && compactTimeIndex < 0) {
            throw new IllegalArgumentException("Timestamp format must include a second component");
        }
        timestampComponentsCount++;
        this.millisecondIndex = timestampComponentsOrder[TimestampComponentType.MILLISECOND_CODE];
        if (millisecondIndex >= 0) {
            timestampComponentsCount++;
        }
        this.microsecondIndex = timestampComponentsOrder[TimestampComponentType.MICROSECOND_CODE];
        if (microsecondIndex >= 0) {
            timestampComponentsCount++;
        }
        this.nanosecondIndex = timestampComponentsOrder[TimestampComponentType.NANOSECOND_CODE];
        if (nanosecondIndex >= 0) {
            timestampComponentsCount++;
        }
        this.timezoneOffsetHoursIndex = timestampComponentsOrder[TimestampComponentType.TIMEZONE_OFFSET_HOURS_CODE];
        if (timezoneOffsetHoursIndex >= 0) {
            timestampComponentsCount++;
        }
        this.timezoneOffsetMinutesIndex = timestampComponentsOrder[TimestampComponentType.TIMEZONE_OFFSET_MINUTES_CODE];
        if (timezoneOffsetMinutesIndex >= 0) {
            timestampComponentsCount++;
        }
        this.timezoneOffsetHoursAndMinutesIndex = timestampComponentsOrder[TimestampComponentType.TIMEZONE_OFFSET_HOURS_AND_MINUTES_CODE];
        if (timezoneOffsetHoursAndMinutesIndex >= 0) {
            timestampComponentsCount++;
        }
        this.numTimestampComponents = timestampComponentsCount;

        this.dateTimeFormatter = amPm
            ? DateTimeFormatter.ofPattern(javaTimeFormat, Locale.US)
            : DateTimeFormatter.ofPattern(javaTimeFormat, Locale.ROOT);
    }

    public String getJavaTimeFormat() {
        return javaTimeFormat;
    }

    public int getNumTimestampComponents() {
        return numTimestampComponents;
    }

    public int[] getTimestampComponentsOrder() {
        return timestampComponentsOrder;
    }

    public long toTimestamp(int[] parsedTimestampComponents) {
        int year, month, day, hour, minute, second, nanos, timezoneOffset;
        if (compactDateIndex >= 0) {
            // compact date yymmdd in a single value: rightmost two digits = day, then month, then 2-digit year (20yy)
            int compactDate = parsedTimestampComponents[compactDateIndex];
            day = compactDate % 100;
            compactDate /= 100;
            month = compactDate % 100;
            compactDate /= 100;
            year = 2000 + compactDate % 100;
        } else if (compactDateYyyymmddIndex >= 0) {
            // compact date yyyymmdd in a single value (e.g. HealthApp): day, then month, then the full 4-digit year
            int compactDate = parsedTimestampComponents[compactDateYyyymmddIndex];
            day = compactDate % 100;
            compactDate /= 100;
            month = compactDate % 100;
            compactDate /= 100;
            year = compactDate;
        } else {
            year = yearIndex >= 0 ? parsedTimestampComponents[yearIndex]
                : twoDigitYearIndex >= 0 ? 2000 + parsedTimestampComponents[twoDigitYearIndex]
                : DEFAULT_YEAR_WHEN_ABSENT;
            month = parsedTimestampComponents[monthIndex];
            day = parsedTimestampComponents[dayIndex];
        }
        if (compactTimeIndex >= 0) {
            // compact time HHMMSS in a single value: rightmost two digits = second, then minute, then hour
            int compactTime = parsedTimestampComponents[compactTimeIndex];
            second = compactTime % 100;
            compactTime /= 100;
            minute = compactTime % 100;
            compactTime /= 100;
            hour = compactTime % 100;
        } else {
            hour = parsedTimestampComponents[hourIndex];
            // Handle AM/PM if present
            if (amPmIndex >= 0) {
                int amPmCode = parsedTimestampComponents[amPmIndex];
                if (amPmCode == TimestampComponentType.AM_CODE && hour == 12) {
                    hour = 0; // 12 AM is midnight
                } else if (amPmCode == TimestampComponentType.PM_CODE && hour < 12) {
                    hour += 12; // Convert PM hour to 24-hour format
                }
            }
            minute = parsedTimestampComponents[minuteIndex];
            second = parsedTimestampComponents[secondIndex];
        }
        if (millisecondIndex >= 0) {
            nanos = parsedTimestampComponents[millisecondIndex] * 1_000_000;
        } else if (microsecondIndex >= 0) {
            nanos = parsedTimestampComponents[microsecondIndex] * 1000;
        } else if (nanosecondIndex >= 0) {
            nanos = parsedTimestampComponents[nanosecondIndex];
        } else {
            nanos = 0;
        }
        LocalDateTime localDateTime = LocalDateTime.of(year, month, day, hour, minute, second, nanos);
        // todo - properly compute timezone offset
        // timezoneOffset = parsedTimestampComponents[timestampComponentsOrder[TimestampComponentType.TIMEZONE_OFFSET_HOURS_CODE]];
        timezoneOffset = 0;
        // todo - probably better to use java.time.OffsetDateTime.of(int, int, int, int, int, int, int, java.time.ZoneOffset)
        // no need to cache the ZoneOffset, as it is already cached in ZoneOffset class
        // todo - is it possible to compute the timestamp without any allocation?
        return localDateTime.toInstant(java.time.ZoneOffset.ofTotalSeconds(timezoneOffset)).toEpochMilli();
    }

    public long parseTimestamp(String timestampString) {
        return parseTimestamp(dateTimeFormatter, timestampString);
    }

    public static long parseTimestamp(DateTimeFormatter dateTimeFormatter, String timestampString) {
        return dateTimeFormatter.parse(timestampString, LocalDateTime::from)
            // todo - handle timezone offset
            .toInstant(java.time.ZoneOffset.ofTotalSeconds(0))
            .toEpochMilli();
    }

    public String representAsString(long timestamp) {
        return representAsString(dateTimeFormatter, timestamp);
    }

    public static String representAsString(DateTimeFormatter dateTimeFormatter, long timestamp) {
        return Instant.ofEpochMilli(timestamp)
            // todo - handle timezone offset
            .atZone(java.time.ZoneOffset.ofTotalSeconds(0))
            .format(dateTimeFormatter);
    }
}
