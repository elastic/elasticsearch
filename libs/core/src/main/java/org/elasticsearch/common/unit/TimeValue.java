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

package org.elasticsearch.common.unit;

import java.util.Locale;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

public class TimeValue implements Comparable<TimeValue> {

    /** How many nano-seconds in one milli-second */
    public static final long NSEC_PER_MSEC = TimeUnit.NANOSECONDS.convert(1, TimeUnit.MILLISECONDS);

    public static final TimeValue MINUS_ONE = timeValueMillis(-1);
    public static final TimeValue ZERO = timeValueMillis(0);

    public static TimeValue timeValueNanos(long nanos) {
        return new TimeValue(nanos, TimeUnit.NANOSECONDS);
    }

    public static TimeValue timeValueMillis(long millis) {
        return new TimeValue(millis, TimeUnit.MILLISECONDS);
    }

    public static TimeValue timeValueSeconds(long seconds) {
        return new TimeValue(seconds, TimeUnit.SECONDS);
    }

    public static TimeValue timeValueMinutes(long minutes) {
        return new TimeValue(minutes, TimeUnit.MINUTES);
    }

    public static TimeValue timeValueHours(long hours) {
        return new TimeValue(hours, TimeUnit.HOURS);
    }

    private final long duration;

    /**
     * @return the number of {@link #timeUnit()} units this value contains
     */
    public long duration() {
        return duration;
    }

    private final TimeUnit timeUnit;

    /**
     * @return the unit used for the this time value, see {@link #duration()}
     */
    public TimeUnit timeUnit() {
        return timeUnit;
    }

    public TimeValue(long millis) {
        this(millis, TimeUnit.MILLISECONDS);
    }

    public TimeValue(long duration, TimeUnit timeUnit) {
        if (duration > Long.MAX_VALUE / timeUnit.toNanos(1)) {
            throw new IllegalArgumentException(
                "Values greater than " + Long.MAX_VALUE + " nanoseconds are not supported: " + duration + unitToSuffix(timeUnit));
        } else if (duration < Long.MIN_VALUE / timeUnit.toNanos(1)) {
            throw new IllegalArgumentException(
                "Values less than " + Long.MIN_VALUE + " nanoseconds are not supported: " + duration + unitToSuffix(timeUnit));
        }
        this.duration = duration;
        this.timeUnit = timeUnit;
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
     * Note that this method might produce fractional time values (ex 1.6m) which may lose
     * resolution when parsed by methods like
     * {@link TimeValue#parse(String, String, String, TimeUnit)}.
     */
    @Override
    public String toString() {
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
        return formatDecimal(value) + suffix;
    }

    private static String formatDecimal(double value) {
        String p = String.valueOf(value);
        int ix = p.indexOf('.') + 1;
        int ex = p.indexOf('E');
        char fraction = p.charAt(ix);
        if (fraction == '0') {
            if (ex != -1) {
                return p.substring(0, ix - 1) + p.substring(ex);
            } else {
                return p.substring(0, ix - 1);
            }
        } else {
            if (ex != -1) {
                return p.substring(0, ix) + fraction + p.substring(ex);
            } else {
                return p.substring(0, ix) + fraction;
            }
        }
    }

    public String getStringRep() {
        if (duration < 0) {
            return Long.toString(duration);
        }
        return duration + unitToSuffix(timeUnit);
    }

    private static String unitToSuffix(TimeUnit unit) {
        switch (unit) {
            case NANOSECONDS:
                return "nanos";
            case MICROSECONDS:
                return "micros";
            case MILLISECONDS:
                return "ms";
            case SECONDS:
                return "s";
            case MINUTES:
                return "m";
            case HOURS:
                return "h";
            case DAYS:
                return "d";
            default:
                throw new IllegalArgumentException("unknown time unit: " + unit.name());
        }
    }

    public static TimeValue parseTimeValue(String sValue, String settingName) {
        Objects.requireNonNull(settingName);
        Objects.requireNonNull(sValue);
        return parseTimeValue(sValue, null, settingName);
    }

    public static TimeValue parseTimeValue(String sValue, TimeValue defaultValue, String settingName) {
        settingName = Objects.requireNonNull(settingName);
        if (sValue == null) {
            return defaultValue;
        }
        final String normalized = sValue.toLowerCase(Locale.ROOT).trim();
        if (normalized.endsWith("nanos")) {
            return parse(sValue, normalized, "nanos", TimeUnit.NANOSECONDS);
        } else if (normalized.endsWith("micros")) {
            return parse(sValue, normalized, "micros", TimeUnit.MICROSECONDS);
        } else if (normalized.endsWith("ms")) {
            return parse(sValue, normalized, "ms", TimeUnit.MILLISECONDS);
        } else if (normalized.endsWith("s")) {
            return parse(sValue, normalized, "s", TimeUnit.SECONDS);
        } else if (sValue.endsWith("m")) {
            // parsing minutes should be case-sensitive as 'M' means "months", not "minutes"; this is the only special case.
            return parse(sValue, normalized, "m", TimeUnit.MINUTES);
        } else if (normalized.endsWith("h")) {
            return parse(sValue, normalized, "h", TimeUnit.HOURS);
        } else if (normalized.endsWith("d")) {
            return parse(sValue, normalized, "d", TimeUnit.DAYS);
        } else if (normalized.matches("-0*1")) {
            return TimeValue.MINUS_ONE;
        } else if (normalized.matches("0+")) {
            return TimeValue.ZERO;
        } else {
            // Missing units:
            throw new IllegalArgumentException("failed to parse setting [" + settingName + "] with value [" + sValue +
                    "] as a time value: unit is missing or unrecognized");
        }
    }

    private static TimeValue parse(final String initialInput, final String normalized, final String suffix, TimeUnit targetUnit) {
        final String s = normalized.substring(0, normalized.length() - suffix.length()).trim();
        if (s.contains(".")) {
            try {
                final double fractional = Double.parseDouble(s);
                return parseFractional(initialInput, fractional, targetUnit);
            } catch (final NumberFormatException e) {
                throw new IllegalArgumentException("failed to parse [" + initialInput + "]", e);
            }
        } else {
            try {
                return new TimeValue(Long.parseLong(s), targetUnit);
            } catch (final NumberFormatException e) {
                throw new IllegalArgumentException("failed to parse [" + initialInput + "]", e);
            }
        }
    }

    private static TimeValue parseFractional(final String initialInput, final double fractional, TimeUnit targetUnit) {
        if (TimeUnit.NANOSECONDS.equals(targetUnit)) {
            throw new IllegalArgumentException("failed to parse [" + initialInput + "], fractional nanosecond values are not supported");
        }
        long multiplier = TimeUnit.NANOSECONDS.convert(1, targetUnit);
        // check for overflow/underflow
        if (fractional > 0) {
            if (fractional != 0 && (Long.MAX_VALUE / (double) multiplier) < fractional) {
                throw new IllegalArgumentException(
                    "Values greater than " + Long.MAX_VALUE + " nanoseconds are not supported: " + initialInput);
            }
        } else {
            if (fractional != 0 && (Long.MIN_VALUE / (double) multiplier) > fractional) {
                throw new IllegalArgumentException(
                    "Values less than " + Long.MIN_VALUE + " nanoseconds are not supported: " + initialInput);
            }
        }
        long nanoValue = (long)(multiplier * fractional);
        // right-size it - Skip days resolution since a fractional value's final unit will always be hours or lower
        if (nanoValue / C5 != 0 && nanoValue % C5 == 0) {
            long duration = nanoValue / C5;
            return new TimeValue(duration, TimeUnit.HOURS);
        } else if (nanoValue / C4 != 0 && nanoValue % C4 == 0) {
            long duration = nanoValue / C4;
            return new TimeValue(duration, TimeUnit.MINUTES);
        } else if (nanoValue / C3 != 0 && nanoValue % C3 == 0) {
            long duration = nanoValue / C3;
            return new TimeValue(duration, TimeUnit.SECONDS);
        } else if (nanoValue / C2 != 0 && nanoValue % C2 == 0) {
            long duration = nanoValue / C2;
            return new TimeValue(duration, TimeUnit.MILLISECONDS);
        } else if (nanoValue / C1 != 0 && nanoValue % C1 == 0) {
            long duration = nanoValue / C1;
            return new TimeValue(duration, TimeUnit.MICROSECONDS);
        }
        return new TimeValue(nanoValue, TimeUnit.NANOSECONDS);
    }

    private static final long C0 = 1L;
    private static final long C1 = C0 * 1000L;
    private static final long C2 = C1 * 1000L;
    private static final long C3 = C2 * 1000L;
    private static final long C4 = C3 * 60L;
    private static final long C5 = C4 * 60L;
    private static final long C6 = C5 * 24L;

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
