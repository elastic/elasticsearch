/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.core;

import java.util.Locale;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

public class TimeValue implements Comparable<TimeValue> {

    /** How many nano-seconds in one milli-second */
    public static final long NSEC_PER_MSEC = TimeUnit.NANOSECONDS.convert(1, TimeUnit.MILLISECONDS);

    public static final TimeValue MINUS_ONE = new TimeValue(-1, TimeUnit.MILLISECONDS);
    public static final TimeValue ZERO = new TimeValue(0, TimeUnit.MILLISECONDS);
    public static final TimeValue MAX_VALUE = new TimeValue(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
    public static final TimeValue THIRTY_SECONDS = new TimeValue(30, TimeUnit.SECONDS);
    public static final TimeValue ONE_MINUTE = new TimeValue(1, TimeUnit.MINUTES);

    private static final long C0 = 1L;
    private static final long C1 = C0 * 1000L;
    private static final long C2 = C1 * 1000L;
    private static final long C3 = C2 * 1000L;
    private static final long C4 = C3 * 60L;
    private static final long C5 = C4 * 60L;
    private static final long C6 = C5 * 24L;

    private final long duration;
    private final TimeUnit timeUnit;

    public TimeValue(long millis) {
        this(millis, TimeUnit.MILLISECONDS);
    }

    public TimeValue(long duration, TimeUnit timeUnit) {
        if (duration < -1) {
            throw new IllegalArgumentException("duration cannot be negative, was given [" + duration + "]");
        }
        this.duration = duration;
        this.timeUnit = timeUnit;
    }

    public static TimeValue timeValueNanos(long nanos) {
        return new TimeValue(nanos, TimeUnit.NANOSECONDS);
    }

    public static TimeValue timeValueMillis(long millis) {
        if (millis == 0) {
            return ZERO;
        }
        if (millis == -1) {
            return MINUS_ONE;
        }
        return new TimeValue(millis, TimeUnit.MILLISECONDS);
    }

    public static TimeValue timeValueSeconds(long seconds) {
        if (seconds == 30) {
            // common value, no need to allocate each time
            return THIRTY_SECONDS;
        }
        return new TimeValue(seconds, TimeUnit.SECONDS);
    }

    public static TimeValue timeValueMinutes(long minutes) {
        if (minutes == 1) {
            // common value, no need to allocate each time
            return ONE_MINUTE;
        }
        return new TimeValue(minutes, TimeUnit.MINUTES);
    }

    public static TimeValue timeValueHours(long hours) {
        return new TimeValue(hours, TimeUnit.HOURS);
    }

    public static TimeValue timeValueDays(long days) {
        // 106751.9 days is Long.MAX_VALUE nanoseconds, so we cannot store 106752 days
        if (days > 106751) {
            throw new IllegalArgumentException("time value cannot store values greater than 106751 days");
        }
        return new TimeValue(days, TimeUnit.DAYS);
    }

    /**
     * @return the {@link TimeValue} object that has the least duration.
     */
    public static TimeValue min(TimeValue time1, TimeValue time2) {
        return time1.compareTo(time2) < 0 ? time1 : time2;
    }

    /**
     * @return the unit used for the this time value, see {@link #duration()}
     */
    public TimeUnit timeUnit() {
        return timeUnit;
    }

    /**
     * @return the number of {@link #timeUnit()} units this value contains
     */
    public long duration() {
        return duration;
    }

    public long nanos() {
        return timeUnit.toNanos(duration);
    }

    public long getNanos() {
        return nanos();
    }

    public long micros() {
        return timeUnit.toMicros(duration);
    }

    public long getMicros() {
        return micros();
    }

    public long millis() {
        return timeUnit.toMillis(duration);
    }

    public long getMillis() {
        return millis();
    }

    public long seconds() {
        return timeUnit.toSeconds(duration);
    }

    public long getSeconds() {
        return seconds();
    }

    public long minutes() {
        return timeUnit.toMinutes(duration);
    }

    public long getMinutes() {
        return minutes();
    }

    public long hours() {
        return timeUnit.toHours(duration);
    }

    public long getHours() {
        return hours();
    }

    public long days() {
        return timeUnit.toDays(duration);
    }

    public long getDays() {
        return days();
    }

    public double microsFrac() {
        return ((double) nanos()) / C1;
    }

    public double getMicrosFrac() {
        return microsFrac();
    }

    public double millisFrac() {
        return ((double) nanos()) / C2;
    }

    public double getMillisFrac() {
        return millisFrac();
    }

    public double secondsFrac() {
        return ((double) nanos()) / C3;
    }

    public double getSecondsFrac() {
        return secondsFrac();
    }

    public double minutesFrac() {
        return ((double) nanos()) / C4;
    }

    public double getMinutesFrac() {
        return minutesFrac();
    }

    public double hoursFrac() {
        return ((double) nanos()) / C5;
    }

    public double getHoursFrac() {
        return hoursFrac();
    }

    public double daysFrac() {
        return ((double) nanos()) / C6;
    }

    public double getDaysFrac() {
        return daysFrac();
    }

    /**
     * Returns a {@link String} representation of the current {@link TimeValue}.
     *
     * Note that this method might produce fractional time values (ex 1.6m) which cannot be
     * parsed by method like {@link TimeValue#parse(String, String, String, String)}.
     *
     * Also note that the maximum string value that will be generated is
     * {@code 106751.9d} due to the way that values are internally converted
     * to nanoseconds (106751.9 days is Long.MAX_VALUE nanoseconds)
     */
    @Override
    public String toString() {
        return this.toHumanReadableString(1);
    }

    /**
     * Returns a {@link String} representation of the current {@link TimeValue}.
     *
     * Note that this method might produce fractional time values (ex 1.6m) which cannot be
     * parsed by method like {@link TimeValue#parse(String, String, String, String)}. The number of
     * fractional decimals (up to 10 maximum) are truncated to the number of fraction pieces
     * specified.
     *
     * Also note that the maximum string value that will be generated is
     * {@code 106751.9d} due to the way that values are internally converted
     * to nanoseconds (106751.9 days is Long.MAX_VALUE nanoseconds)
     *
     * @param fractionPieces the number of decimal places to include
     */
    public String toHumanReadableString(int fractionPieces) {
        if (duration < 0) {
            return Long.toString(duration);
        }
        long nanos = nanos();
        if (nanos == 0) {
            return "0s";
        }
        double value = nanos;
        String suffix = "nanos";
        if (nanos >= C6) {
            value = daysFrac();
            suffix = "d";
        } else if (nanos >= C5) {
            value = hoursFrac();
            suffix = "h";
        } else if (nanos >= C4) {
            value = minutesFrac();
            suffix = "m";
        } else if (nanos >= C3) {
            value = secondsFrac();
            suffix = "s";
        } else if (nanos >= C2) {
            value = millisFrac();
            suffix = "ms";
        } else if (nanos >= C1) {
            value = microsFrac();
            suffix = "micros";
        }
        // Limit fraction pieces to a min of 0 and maximum of 10
        return formatDecimal(value, Math.min(10, Math.max(0, fractionPieces))) + suffix;
    }

    private static String formatDecimal(double value, int fractionPieces) {
        String p = String.valueOf(value);
        int totalLength = p.length();
        int ix = p.indexOf('.') + 1;
        int ex = p.indexOf('E');
        // Location where the fractional values end
        int fractionEnd = ex == -1 ? Math.min(ix + fractionPieces, totalLength) : ex;

        // Determine the value of the fraction, so if it were .000 the
        // actual long value is 0, in which case, it can be elided.
        long fractionValue;
        try {
            fractionValue = Long.parseLong(p.substring(ix, fractionEnd));
        } catch (NumberFormatException e) {
            fractionValue = 0;
        }

        if (fractionValue == 0 || fractionPieces <= 0) {
            // Either the part of the fraction we were asked to report is
            // zero, or the user requested 0 fraction pieces, so return
            // only the integral value
            if (ex != -1) {
                return p.substring(0, ix - 1) + p.substring(ex);
            } else {
                return p.substring(0, ix - 1);
            }
        } else {
            // Build up an array of fraction characters, without going past
            // the end of the string. This keeps track of trailing '0' chars
            // that should be truncated from the end to avoid getting a
            // string like "1.3000d" (returning "1.3d" instead) when the
            // value is 1.30000009
            char[] fractions = new char[fractionPieces];
            int fracCount = 0;
            int truncateCount = 0;
            for (int i = 0; i < fractionPieces; i++) {
                int position = ix + i;
                if (position >= fractionEnd) {
                    // No more pieces, the fraction has ended
                    break;
                }
                char fraction = p.charAt(position);
                if (fraction == '0') {
                    truncateCount++;
                } else {
                    truncateCount = 0;
                }
                fractions[i] = fraction;
                fracCount++;
            }

            // Generate the fraction string from the char array, truncating any trailing zeros
            String fractionStr = new String(fractions, 0, fracCount - truncateCount);

            if (ex != -1) {
                return p.substring(0, ix) + fractionStr + p.substring(ex);
            } else {
                return p.substring(0, ix) + fractionStr;
            }
        }
    }

    public String getStringRep() {
        if (duration < 0) {
            return Long.toString(duration);
        }
        return switch (timeUnit) {
            case NANOSECONDS -> duration + "nanos";
            case MICROSECONDS -> duration + "micros";
            case MILLISECONDS -> duration + "ms";
            case SECONDS -> duration + "s";
            case MINUTES -> duration + "m";
            case HOURS -> duration + "h";
            case DAYS -> duration + "d";
        };
    }

    /**
     * @param sValue       Value to parse, which may not be {@code null}.
     * @param settingName  Name of the parameter or setting. On invalid input, this value is included in the exception message. Otherwise,
     *                     this parameter is unused.
     * @return The {@link TimeValue} which the input string represents.
     */
    public static TimeValue parseTimeValue(String sValue, String settingName) {
        Objects.requireNonNull(settingName);
        Objects.requireNonNull(sValue);
        return parseTimeValue(sValue, null, settingName);
    }

    /**
     * @param sValue       Value to parse, which may be {@code null}.
     * @param defaultValue Value to return if {@code sValue} is {@code null}.
     * @param settingName  Name of the parameter or setting. On invalid input, this value is included in the exception message. Otherwise,
     *                     this parameter is unused.
     * @return The {@link TimeValue} which the input string represents, or {@code defaultValue} if the input is {@code null}.
     */
    public static TimeValue parseTimeValue(@Nullable String sValue, TimeValue defaultValue, String settingName) {
        settingName = Objects.requireNonNull(settingName);
        if (sValue == null) {
            return defaultValue;
        }
        final String normalized = sValue.toLowerCase(Locale.ROOT).trim();
        if (normalized.endsWith("nanos")) {
            return TimeValue.timeValueNanos(parse(sValue, normalized, "nanos", settingName));
        } else if (normalized.endsWith("micros")) {
            return new TimeValue(parse(sValue, normalized, "micros", settingName), TimeUnit.MICROSECONDS);
        } else if (normalized.endsWith("ms")) {
            return TimeValue.timeValueMillis(parse(sValue, normalized, "ms", settingName));
        } else if (normalized.endsWith("s")) {
            return TimeValue.timeValueSeconds(parse(sValue, normalized, "s", settingName));
        } else if (sValue.endsWith("m")) {
            // parsing minutes should be case-sensitive as 'M' means "months", not "minutes"; this is the only special case.
            return TimeValue.timeValueMinutes(parse(sValue, normalized, "m", settingName));
        } else if (normalized.endsWith("h")) {
            return TimeValue.timeValueHours(parse(sValue, normalized, "h", settingName));
        } else if (normalized.endsWith("d")) {
            return new TimeValue(parse(sValue, normalized, "d", settingName), TimeUnit.DAYS);
        } else if (normalized.matches("-0*1")) {
            return TimeValue.MINUS_ONE;
        } else if (normalized.matches("0+")) {
            return TimeValue.ZERO;
        } else {
            // Missing units:
            throw new IllegalArgumentException(
                "failed to parse setting [" + settingName + "] with value [" + sValue + "] as a time value: unit is missing or unrecognized"
            );
        }
    }

    private static long parse(final String initialInput, final String normalized, final String suffix, String settingName) {
        final String s = normalized.substring(0, normalized.length() - suffix.length()).trim();
        try {
            final long value = Long.parseLong(s);
            if (value < -1) {
                // -1 is magic, but reject any other negative values
                throw new IllegalArgumentException(
                    "failed to parse setting ["
                        + settingName
                        + "] with value ["
                        + initialInput
                        + "] as a time value: negative durations are not supported"
                );
            }
            return value;
        } catch (final NumberFormatException e) {
            try {
                @SuppressWarnings("unused")
                final double ignored = Double.parseDouble(s);
                throw new IllegalArgumentException("failed to parse [" + initialInput + "], fractional time values are not supported", e);
            } catch (final NumberFormatException ignored) {
                throw new IllegalArgumentException("failed to parse [" + initialInput + "]", e);
            }
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        return this.compareTo(((TimeValue) o)) == 0;
    }

    @Override
    public int hashCode() {
        return Double.hashCode(((double) duration) * timeUnit.toNanos(1));
    }

    public static long nsecToMSec(long ns) {
        return ns / NSEC_PER_MSEC;
    }

    @Override
    public int compareTo(TimeValue timeValue) {
        double thisValue = ((double) duration) * timeUnit.toNanos(1);
        double otherValue = ((double) timeValue.duration) * timeValue.timeUnit.toNanos(1);
        return Double.compare(thisValue, otherValue);
    }
}
