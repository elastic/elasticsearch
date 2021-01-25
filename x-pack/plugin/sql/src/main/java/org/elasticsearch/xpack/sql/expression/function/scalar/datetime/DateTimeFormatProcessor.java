/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function.scalar.datetime;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.ql.expression.gen.processor.Processor;
import org.elasticsearch.xpack.sql.SqlIllegalArgumentException;

import java.io.IOException;
import java.time.DateTimeException;
import java.time.DayOfWeek;
import java.time.OffsetTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalAccessor;
import java.time.temporal.TemporalField;
import java.time.temporal.WeekFields;
import java.util.Locale;
import java.util.Objects;
import java.util.function.Function;

import static org.elasticsearch.xpack.sql.util.DateUtils.asTimeAtZone;

public class DateTimeFormatProcessor extends BinaryDateTimeProcessor {

    public static final String NAME = "dtformat";
    private static final String[][] JAVA_TIME_FORMAT_REPLACEMENTS = {
        {"tt", "a"},
        {"t", "a"},
        {"dddd", "eeee"},
        {"ddd", "eee"},
        {"K", "v"},
        {"g", "G"},
        {"f", "S"},
        {"F", "S"},
        {"z", "X"}
    };

    private static final String[][] JAVA_TIME_FORMAT_REPLACEMENTS_FOR_MYSQL = {
        {"%a", "EEE"},
        {"%b", "MMM"},
        {"%c", "MM"},
        {"%d", "dd"},
        {"%e", "d"},
        {"%f", "SSSSSS"},
        {"%H", "HH"},
        {"%h", "hh"},
        {"%I", "hh"},
        {"%i", "mm"},
        {"%j", "DDD"},
        {"%k", "H"},
        {"%l", "h"},
        {"%M", "MMMM"},
        {"%m", "MM"},
        {"%p", "a"},
        {"%r", "hh:mm:ss a"},
        {"%S", "ss"},
        {"%s", "ss"},
        {"%T", "HH:mm:ss"},
        {"%v", "ww"}, // mysql %v and %x seems comply with how DateTimeFormatter define week of year, but not with %U, %u, %V, and %X
        {"%W", "EEEE"},
        {"%w", "e"},
        {"%x", "Y"},
        {"%Y", "yyyy"},
        {"%y", "yy"},
        {"%%", "%"}
    };

    private final Formatter formatter;

    public enum Formatter {
        FORMAT {
            @Override
            protected Function<TemporalAccessor, String> formatterFor(String pattern) {
                if (pattern.isEmpty()) {
                    return null;
                }
                for (String[] replacement : JAVA_TIME_FORMAT_REPLACEMENTS) {
                    pattern = pattern.replace(replacement[0], replacement[1]);
                }
                final String javaPattern = pattern;
                return DateTimeFormatter.ofPattern(javaPattern, Locale.ROOT)::format;
            }
        },
        DATE_TIME_FORMAT {
            @Override
            protected Function<TemporalAccessor, String> formatterFor(String pattern) {
                return DateTimeFormatter.ofPattern(pattern, Locale.ROOT)::format;
            }
        },
        DATE_FORMAT {
            private String replacePatternForMySql(String pattern) {
                // escape any chars that are not starts with '%' to avoid chars being interpreted by DateTimeFormatter
                pattern = pattern.replaceAll(
                    "(%\\w)",
                    "{$0}");
                pattern = pattern.replaceAll(
                    "([^(\\{%\\w\\})])?(\\w+)([^(\\{%\\w\\})])?",
                    "$1'$2'$3"
                );
                pattern = pattern.replaceAll(
                    "\\{%'(\\w)'\\}",
                    "%$1"
                );

                // replace %x with x, for any "x" not used in mysql date format specifiers
                pattern = pattern.replaceAll(
                    "%([^abcDdefHhIijklMmprSsTUuVvWwXxYy])",
                    "'$1'"
                );
                pattern = pattern.replace("''", "");

                String pattern1 = pattern;
                for (String[] replacement : JAVA_TIME_FORMAT_REPLACEMENTS_FOR_MYSQL) {
                    pattern1 = pattern1.replace(replacement[0], replacement[1]);
                }
                return pattern1;
            }

            private String interpretXSpecifier(TemporalAccessor ta, String javaPattern) {
                if (javaPattern.contains("%X")) {
                    TemporalField weekBasedYearTempField = WeekFields.of(DayOfWeek.SUNDAY, 7).weekBasedYear();
                    int weekBasedYear = ta.get(weekBasedYearTempField);
                    String replacement = String.valueOf(weekBasedYear);
                    javaPattern = javaPattern.replace("%X", replacement);
                }
                return javaPattern;
            }

            private String interpretVSpecifier(TemporalAccessor ta, String javaPattern) {
                if (javaPattern.contains("%V")) {
                    TemporalField weekOfWeekBasedYearTempField = WeekFields.of(DayOfWeek.SUNDAY, 7).weekOfWeekBasedYear();
                    int weekOfWeekBasedYear = ta.get(weekOfWeekBasedYearTempField);
                    String replacement = String.valueOf(weekOfWeekBasedYear);
                    if (weekOfWeekBasedYear < 10) {
                        replacement = "0" + replacement;
                    }
                    javaPattern = javaPattern.replace("%V", replacement);
                }
                return javaPattern;
            }

            private String interpretUAnduSpecifier(TemporalAccessor ta, String javaPattern) {
                if (javaPattern.contains("%U")) {
                    TemporalField weekOfYearTempField = WeekFields.of(DayOfWeek.SUNDAY, 7).weekOfYear();
                    int weekOfYear = ta.get(weekOfYearTempField);
                    String replacement = String.valueOf(weekOfYear);
                    if (weekOfYear < 10) {
                        replacement = "0" + replacement;
                    }
                    javaPattern = javaPattern.replace("%U", replacement);
                }
                if (javaPattern.contains("%u")) {
                    TemporalField weekOfWeekBasedYearTempField = WeekFields.of(DayOfWeek.MONDAY, 4).weekOfWeekBasedYear();
                    int weekOfWeekBasedYear = ta.get(weekOfWeekBasedYearTempField);
                    String replacement = String.valueOf(weekOfWeekBasedYear);
                    if (weekOfWeekBasedYear < 10) {
                        replacement = "0" + replacement;
                    }
                    javaPattern = javaPattern.replace("%u", replacement);
                }
                return javaPattern;
            }

            private String interpretAndEscapeDayOfMonthWithOrdinalIndicator(TemporalAccessor ta, String javaPattern) {
                if (javaPattern.contains("%D")) {
                    String dayOfMonth = DateTimeFormatter.ofPattern("d", Locale.ROOT).format(ta);
                    if (dayOfMonth.endsWith("1")) {
                        dayOfMonth = "'" + dayOfMonth + "st'";
                    } else if (dayOfMonth.endsWith("2")) {
                        dayOfMonth = "'" + dayOfMonth + "nd'";
                    } else if (dayOfMonth.endsWith("3")) {
                        dayOfMonth = "'" + dayOfMonth + "rd'";
                    } else {
                        dayOfMonth = "'" + dayOfMonth + "th'";
                    }
                    javaPattern = javaPattern.replace("%D", dayOfMonth);
                }
                return javaPattern;
            }
            

            @Override
            protected Function<TemporalAccessor, String> formatterFor(String pattern) {
                return ta -> {
                    String javaPattern = replacePatternForMySql(pattern);
                    javaPattern = interpretAndEscapeDayOfMonthWithOrdinalIndicator(ta, javaPattern);
                    javaPattern = interpretUAnduSpecifier(ta, javaPattern);
                    javaPattern = interpretVSpecifier(ta, javaPattern);
                    javaPattern = interpretXSpecifier(ta, javaPattern);
                    return DateTimeFormatter.ofPattern(javaPattern).format(ta);
                };
            }
        },
        TO_CHAR {
            @Override 
            protected Function<TemporalAccessor, String> formatterFor(String pattern) {
                return ToCharFormatter.ofPattern(pattern);
            }
        };

        protected abstract Function<TemporalAccessor, String> formatterFor(String pattern);
        
        public Object format(Object timestamp, Object pattern, ZoneId zoneId) {
            if (timestamp == null || pattern == null) {
                return null;
            }
            String patternString;
            if (pattern instanceof String) {
                patternString = (String) pattern;
            } else {
                throw new SqlIllegalArgumentException("A string is required; received [{}]", pattern);
            }
            if (patternString.isEmpty()) {
                return null;
            }

            if (timestamp instanceof ZonedDateTime == false && timestamp instanceof OffsetTime == false) {
                throw new SqlIllegalArgumentException("A date/datetime/time is required; received [{}]", timestamp);
            }

            TemporalAccessor ta;
            if (timestamp instanceof ZonedDateTime) {
                ta = ((ZonedDateTime) timestamp).withZoneSameInstant(zoneId);
            } else {
                ta = asTimeAtZone((OffsetTime) timestamp, zoneId);
            }
            try {
                return formatterFor(patternString).apply(ta);
            } catch (IllegalArgumentException | DateTimeException e) {
                throw new SqlIllegalArgumentException(
                    "Invalid pattern [{}] is received for formatting date/time [{}]; {}",
                    pattern,
                    timestamp,
                    e.getMessage()
                );
            }
        }

    }

    public DateTimeFormatProcessor(Processor source1, Processor source2, ZoneId zoneId, Formatter formatter) {
        super(source1, source2, zoneId);
        this.formatter = formatter;
    }

    public DateTimeFormatProcessor(StreamInput in) throws IOException {
        super(in);
        this.formatter = in.readEnum(Formatter.class);
    }

    @Override
    protected void doWrite(StreamOutput out) throws IOException {
        out.writeEnum(formatter);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    protected Object doProcess(Object timestamp, Object pattern) {
        return this.formatter.format(timestamp, pattern, zoneId());
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), formatter);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        DateTimeFormatProcessor other = (DateTimeFormatProcessor) obj;
        return super.equals(other) && Objects.equals(formatter, other.formatter);
    }

    public Formatter formatter() {
        return formatter;
    }
}
