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

class DateTimeParser {

    @Nullable
    public static TemporalAccessor tryParse(CharSequence str) {
        int len = str.length();

        Integer years = parseInt(str, 0, 4);
        if (years == null || len == 4 || str.charAt(4) != '-') return null;

        Integer months = parseInt(str, 5, 7);
        if (months == null || len == 7 || str.charAt(7) != '-') return null;

        Integer days = parseInt(str, 8, 10);
        if (days == null) return null;

        if (len == 10) return new DateTime(years, months, days, null, null, null, null, null);

        if (str.charAt(10) != 'T') return null;
        if (len == 11) return new DateTime(years, months, days, null, null, null, null, null);

        Integer hours = parseInt(str, 11, 13);
        if (hours == null) return null;
        if (len == 13) {
            return new DateTime(years, months, days, hours, null, null, null, null);
        }
        if (isTimezone(str, 13)) {
            ZoneOffset timezone = parseTimezone(str, 13);
            return timezone != null ? new DateTime(years, months, days, hours, null, null, null, timezone) : null;
        }

        if (str.charAt(13) != ':') return null;

        Integer minutes = parseInt(str, 14, 16);
        if (minutes == null) return null;
        if (len == 16) {
            return new DateTime(years, months, days, hours, minutes, null, null, null);
        }
        if (isTimezone(str, 16)) {
            ZoneOffset timezone = parseTimezone(str, 16);
            return timezone != null ? new DateTime(years, months, days, hours, minutes, null, null, timezone) : null;
        }

        if (str.charAt(16) != ':') return null;

        Integer seconds = parseInt(str, 17, 19);
        if (seconds == null) return null;
        if (len == 19) {
            return new DateTime(years, months, days, hours, minutes, seconds, null, null);
        }
        if (isTimezone(str, 19)) {
            ZoneOffset timezone = parseTimezone(str, 19);
            return timezone != null ? new DateTime(years, months, days, hours, minutes, seconds, null, timezone) : null;
        }

        char decSeparator = str.charAt(19);
        if (decSeparator != '.' && decSeparator != ',') return null;

        // the last number could be millis or nanos, or any combination in the middle
        // so we keep parsing numbers until we get to not a number
        int nanos = 0;
        int pos;
        for (pos = 20; pos < len; pos++) {
            char c = str.charAt(pos);
            if (c < ZERO || c > NINE) break;
            nanos = (nanos << 1) + (nanos << 3);
            nanos -= c - ZERO;
        }
        nanos = -nanos;

        if (pos == 20) return null;   // didn't find a number at all

        // multiply it by the remainder of the nano digits missed off the end
        for (int pow10 = 29 - pos; pow10 > 0; pow10--) {
            nanos *= 10;
        }

        if (len == pos) {
            return new DateTime(years, months, days, hours, minutes, seconds, nanos, null);
        }
        if (isTimezone(str, pos)) {
            ZoneOffset timezone = parseTimezone(str, pos);
            return timezone != null ? new DateTime(years, months, days, hours, minutes, seconds, nanos, timezone) : null;
        }

        // still chars left at the end - string is not valid
        return null;
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
        if (len == pos) return ZoneOffset.ofHours(positive ? hours : -hours);

        // zone offset may or may not have a : in the middle
        if (str.charAt(pos) == ':') pos++;

        Integer minutes = parseInt(str, pos, pos += 2);
        if (minutes == null) return null;
        if (len == pos) return ZoneOffset.ofHoursMinutes(positive ? hours : -hours, minutes);

        // if there are seconds, there should be a : before them
        if (str.charAt(pos++) != ':') return null;

        Integer seconds = parseInt(str, pos, pos += 2);
        if (seconds == null) return null;
        if (len == pos) return ZoneOffset.ofHoursMinutesSeconds(positive ? hours : -hours, minutes, seconds);

        return null;
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

    private static boolean checkPosition(CharSequence str, int pos, char c) {
        return str.length() > pos && str.charAt(pos) == c;
    }
}
