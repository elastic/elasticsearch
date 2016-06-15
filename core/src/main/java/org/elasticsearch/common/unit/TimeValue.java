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

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.joda.time.Period;
import org.joda.time.PeriodType;
import org.joda.time.format.PeriodFormat;
import org.joda.time.format.PeriodFormatter;

import java.io.IOException;
import java.util.Locale;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

public class TimeValue implements Writeable {

    /** How many nano-seconds in one milli-second */
    public static final long NSEC_PER_MSEC = 1000000;

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
    private final TimeUnit timeUnit;

    public TimeValue(long millis) {
        this(millis, TimeUnit.MILLISECONDS);
    }

    public TimeValue(long duration, TimeUnit timeUnit) {
        this.duration = duration;
        this.timeUnit = timeUnit;
    }

    /**
     * Read from a stream.
     */
    public TimeValue(StreamInput in) throws IOException {
        duration = in.readZLong();
        timeUnit = TimeUnit.NANOSECONDS;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeZLong(nanos());
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

    private final PeriodFormatter defaultFormatter = PeriodFormat.getDefault()
            .withParseType(PeriodType.standard());

    public String format() {
        Period period = new Period(millis());
        return defaultFormatter.print(period);
    }

    public String format(PeriodType type) {
        Period period = new Period(millis());
        return PeriodFormat.getDefault().withParseType(type).print(period);
    }

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
        return Strings.format1Decimals(value, suffix);
    }

    public String getStringRep() {
        if (duration < 0) {
            return Long.toString(duration);
        }
        switch (timeUnit) {
            case NANOSECONDS:
                return Strings.format1Decimals(duration, "nanos");
            case MICROSECONDS:
                return Strings.format1Decimals(duration, "micros");
            case MILLISECONDS:
                return Strings.format1Decimals(duration, "ms");
            case SECONDS:
                return Strings.format1Decimals(duration, "s");
            case MINUTES:
                return Strings.format1Decimals(duration, "m");
            case HOURS:
                return Strings.format1Decimals(duration, "h");
            case DAYS:
                return Strings.format1Decimals(duration, "d");
            default:
                throw new IllegalArgumentException("unknown time unit: " + timeUnit.name());
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
        try {
            long millis;
            String lowerSValue = sValue.toLowerCase(Locale.ROOT).trim();
            if (lowerSValue.endsWith("ms")) {
                millis = parse(lowerSValue, 2, 1);
            } else if (lowerSValue.endsWith("s")) {
                millis = parse(lowerSValue, 1, 1000);
            } else if (lowerSValue.endsWith("m")) {
                millis = parse(lowerSValue, 1, 60 * 1000);
            } else if (lowerSValue.endsWith("h")) {
                millis = parse(lowerSValue, 1, 60 * 60 * 1000);
            } else if (lowerSValue.endsWith("d")) {
                millis = parse(lowerSValue, 1, 24 * 60 * 60 * 1000);
            } else if (lowerSValue.endsWith("w")) {
                millis = parse(lowerSValue, 1, 7 * 24 * 60 * 60 * 1000);
            } else if (lowerSValue.equals("-1")) {
                // Allow this special value to be unit-less:
                millis = -1;
            } else if (lowerSValue.equals("0")) {
                // Allow this special value to be unit-less:
                millis = 0;
            } else {
                // Missing units:
                throw new ElasticsearchParseException("Failed to parse setting [{}] with value [{}] as a time value: unit is missing or unrecognized", settingName, sValue);
            }
            return new TimeValue(millis, TimeUnit.MILLISECONDS);
        } catch (NumberFormatException e) {
            throw new ElasticsearchParseException("Failed to parse [{}]", e, sValue);
        }
    }

    private static long parse(String s, int suffixLength, long scale) {
        return (long) (Double.parseDouble(s.substring(0, s.length() - suffixLength)) * scale);
    }

    static final long C0 = 1L;
    static final long C1 = C0 * 1000L;
    static final long C2 = C1 * 1000L;
    static final long C3 = C2 * 1000L;
    static final long C4 = C3 * 60L;
    static final long C5 = C4 * 60L;
    static final long C6 = C5 * 24L;

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        TimeValue timeValue = (TimeValue) o;
        return timeUnit.toNanos(duration) == timeValue.timeUnit.toNanos(timeValue.duration);
    }

    @Override
    public int hashCode() {
        long normalized = timeUnit.toNanos(duration);
        return Long.hashCode(normalized);
    }

    public static long nsecToMSec(long ns) {
        return ns / NSEC_PER_MSEC;
    }
}
