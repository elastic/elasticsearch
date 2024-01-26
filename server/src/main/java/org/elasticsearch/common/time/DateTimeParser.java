/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.time;

import org.elasticsearch.core.Nullable;

import java.time.ZoneOffset;
import java.time.temporal.TemporalAccessor;

/**
 * Parses datetimes in ISO9601 format (and subsequences of)
 */
class DateTimeParser {

    record Result(TemporalAccessor result, int errorIndex) {
        Result(TemporalAccessor result) {
            this(result, -1);
        }

        static Result error(int errorIndex) {
            return new Result(null, errorIndex);
        }
    }

    static Result tryParse(CharSequence str, @Nullable ZoneOffset defaultOffset) {
        int len = str.length();

        /*
         * This parses datetimes in the format YYYY-MM-ddTHH:mm:ss.SSS[tz]
         * Years, months, days are mandatory, everything else is optional (in order)
         */

        // years, months, days are mandatory
        Integer years = parseInt(str, 0, 4);
        if (years == null) return Result.error(0);
        if (len == 4 || str.charAt(4) != '-') return Result.error(4);

        Integer months = parseInt(str, 5, 7);
        if (months == null) return Result.error(5);
        if (len == 7 || str.charAt(7) != '-') return Result.error(7);

        Integer days = parseInt(str, 8, 10);
        if (days == null) return Result.error(8);

        // all time fields are optional
        if (len == 10) return new Result(new DateTime(years, months, days, null, null, null, null, defaultOffset));

        if (str.charAt(10) != 'T') return Result.error(10);
        if (len == 11) return new Result(new DateTime(years, months, days, null, null, null, null, defaultOffset));

        Integer hours = parseInt(str, 11, 13);
        if (hours == null) return Result.error(11);
        if (len == 13) {
            return new Result(new DateTime(years, months, days, hours, 0, 0, 0, defaultOffset));
        }
        if (isTimezone(str, 13)) {
            ZoneOffset timezone = parseTimezone(str, 13);
            return timezone == null ? Result.error(13) : new Result(new DateTime(years, months, days, hours, 0, 0, 0, timezone));
        }

        if (str.charAt(13) != ':') return Result.error(13);

        Integer minutes = parseInt(str, 14, 16);
        if (minutes == null) return Result.error(14);
        if (len == 16) {
            return new Result(new DateTime(years, months, days, hours, minutes, 0, 0, defaultOffset));
        }
        if (isTimezone(str, 16)) {
            ZoneOffset timezone = parseTimezone(str, 16);
            return timezone == null ? Result.error(16) : new Result(new DateTime(years, months, days, hours, minutes, 0, 0, timezone));
        }

        if (str.charAt(16) != ':') return Result.error(16);

        Integer seconds = parseInt(str, 17, 19);
        if (seconds == null) return Result.error(17);
        if (len == 19) {
            return new Result(new DateTime(years, months, days, hours, minutes, seconds, 0, defaultOffset));
        }
        if (isTimezone(str, 19)) {
            ZoneOffset timezone = parseTimezone(str, 19);
            return timezone == null
                ? Result.error(19)
                : new Result(new DateTime(years, months, days, hours, minutes, seconds, 0, timezone));
        }

        char decSeparator = str.charAt(19);
        if (decSeparator != '.' && decSeparator != ',') return Result.error(19);

        // the last number could be millis or nanos, or any combination in the middle
        // so we keep parsing numbers until we get to not a number
        int nanos = 0;
        int pos;
        for (pos = 20; pos < len && pos < 29; pos++) {
            char c = str.charAt(pos);
            if (c < ZERO || c > NINE) break;
            nanos = (nanos << 1) + (nanos << 3);
            nanos -= c - ZERO;
        }
        nanos = -nanos;

        if (pos == 20) return Result.error(20);   // didn't find a number at all

        // multiply it by the remainder of the nano digits missed off the end
        for (int pow10 = 29 - pos; pow10 > 0; pow10--) {
            nanos *= 10;
        }

        if (len == pos) {
            return new Result(new DateTime(years, months, days, hours, minutes, seconds, nanos, defaultOffset));
        }
        if (isTimezone(str, pos)) {
            ZoneOffset timezone = parseTimezone(str, pos);
            return timezone == null
                ? Result.error(pos)
                : new Result(new DateTime(years, months, days, hours, minutes, seconds, nanos, timezone));
        }

        // still chars left at the end - string is not valid
        return Result.error(pos);
    }

    private static boolean isTimezone(CharSequence str, int pos) {
        char c = str.charAt(pos);
        return c == 'Z' || c == '+' || c == '-';
    }

    private static ZoneOffset parseTimezone(CharSequence str, int pos) {
        boolean positive;
        switch (str.charAt(pos++)) {
            case 'Z' -> {
                return ZoneOffset.UTC;
            }
            case '+' -> positive = true;
            case '-' -> positive = false;
            default -> {
                return null;
            }
        }

        int len = str.length();

        Integer hours = parseInt(str, pos, pos += 2);
        if (hours == null) return null;
        if (len == pos) return ofHoursMinutesSeconds(hours, 0, 0, positive);

        // zone offset may or may not have a : in the middle
        if (str.charAt(pos) == ':') pos++;

        Integer minutes = parseInt(str, pos, pos += 2);
        if (minutes == null) return null;
        if (len == pos) return ofHoursMinutesSeconds(hours, minutes, 0, positive);

        // if there are seconds, there should be a : before them
        if (str.charAt(pos++) != ':') return null;

        Integer seconds = parseInt(str, pos, pos += 2);
        if (seconds == null) return null;
        if (len == pos) return ofHoursMinutesSeconds(hours, minutes, seconds, positive);

        return null;
    }

    private static ZoneOffset ofHoursMinutesSeconds(int hours, int minutes, int seconds, boolean positive) {
        int totalSeconds = hours * 3600 + minutes * 60 + seconds;
        return ZoneOffset.ofTotalSeconds(positive ? totalSeconds : -totalSeconds);
    }

    private static final char ZERO = '0';
    private static final char NINE = '9';

    private static Integer parseInt(CharSequence str, int startInclusive, int endExclusive) {
        if (str.length() < endExclusive) return null;

        int result = 0;
        for (int i = startInclusive; i < endExclusive; i++) {
            char c = str.charAt(i);
            if (c < ZERO || c > NINE) return null;
            result = (result << 1) + (result << 3);
            result -= c - ZERO;
        }
        return -result;
    }
}
