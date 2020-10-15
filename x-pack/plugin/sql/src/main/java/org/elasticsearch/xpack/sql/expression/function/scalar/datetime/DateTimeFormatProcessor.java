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
import java.time.OffsetTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalAccessor;
import java.util.Locale;
import java.util.Objects;

import static org.elasticsearch.xpack.sql.util.DateUtils.*;

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
    private static final DateTimeFormatMapping[] JAVA_TIME_TO_CHAR_MAPPINGS = {
        new DateTimeFormatMapping("MONTH", "MMMM", String::toUpperCase),
        new DateTimeFormatMapping("Month", "MMMM"),
        new DateTimeFormatMapping("month", "MMMM", String::toLowerCase),
        new DateTimeFormatMapping("MON", "MMM", String::toUpperCase),
        new DateTimeFormatMapping("Mon", "MMM"),
        new DateTimeFormatMapping("mon", "MMM", String::toLowerCase),
        new DateTimeFormatMapping("DAY", "EEEE", String::toUpperCase),
        new DateTimeFormatMapping("Day", "EEEE"),
        new DateTimeFormatMapping("day", "EEEE", String::toLowerCase),
        new DateTimeFormatMapping("DY", "E", String::toUpperCase),
        new DateTimeFormatMapping("Dy", "E"),
        new DateTimeFormatMapping("dy", "E", String::toLowerCase),
        new DateTimeFormatMapping("HH12", "hh"),
        new DateTimeFormatMapping("HH24", "HH"),
        new DateTimeFormatMapping("HH", "hh"),
        new DateTimeFormatMapping("MI", "mm"),
        new DateTimeFormatMapping("SSSS", "A", milliSecondOfDay -> String.valueOf(Integer.parseInt(milliSecondOfDay) / 1000)),
        new DateTimeFormatMapping("SS", "s"),
        new DateTimeFormatMapping("MS", "SSS"),
        new DateTimeFormatMapping("US", "n", nano -> String.format("%06d", (int) (Integer.parseInt(nano) / Math.pow(10.0, Math.max(nano.length() - 6, 0))))),
        new DateTimeFormatMapping("AM", "a", String::toUpperCase),
        new DateTimeFormatMapping("am", "a", String::toLowerCase),
        new DateTimeFormatMapping("PM", "a", String::toUpperCase),
        new DateTimeFormatMapping("pm", "a", String::toLowerCase),
        new DateTimeFormatMapping("A.M.", "a", x -> x.charAt(0) + "." + x.charAt(1) + "."),
        new DateTimeFormatMapping("a.m.", "a", x -> (x.charAt(0) + "." + x.charAt(1) + ".").toLowerCase()),
        new DateTimeFormatMapping("P.M.", "a", x -> x.charAt(0) + "." + x.charAt(1) + "."),
        new DateTimeFormatMapping("p.m.", "a", x -> (x.charAt(0) + "." + x.charAt(1) + ".").toLowerCase()),
        new DateTimeFormatMapping("BC", "G"),
        new DateTimeFormatMapping("bc", "G", String::toLowerCase),
        new DateTimeFormatMapping("AD", "G"),
        new DateTimeFormatMapping("ad", "G", String::toLowerCase),
        new DateTimeFormatMapping("B.C.", "G", x -> x.charAt(0) + "." + x.charAt(1) + "."),
        new DateTimeFormatMapping("b.c.", "G", x -> (x.charAt(0) + "." + x.charAt(1) + ".").toLowerCase()),
        new DateTimeFormatMapping("A.D.", "G", x -> x.charAt(0) + "." + x.charAt(1) + "."),
        new DateTimeFormatMapping("a.d.", "G", x -> (x.charAt(0) + "." + x.charAt(1) + ".").toLowerCase()),
        new DateTimeFormatMapping("DDD", "DDD"),
        new DateTimeFormatMapping("DD", "d"),
        new DateTimeFormatMapping("ID", "e"),
        new DateTimeFormatMapping("D", "e", dayOfWeek -> String.valueOf((Integer.parseInt(dayOfWeek) + 1) % 7)),
        new DateTimeFormatMapping("IW", "ww"),
        new DateTimeFormatMapping("CC", "YYYY", year -> String.format(year.charAt(0) == '-' ? "%03d" : "%02d", century(Integer.parseInt(year)))),
        new DateTimeFormatMapping("J", "g", modifiedJulianDay -> String.valueOf(Integer.parseInt(modifiedJulianDay) + 2400001)),
        new DateTimeFormatMapping("RM", "MM", month -> toRome(Integer.parseInt(month))),
        new DateTimeFormatMapping("rm", "MM", month -> toRome(Integer.parseInt(month)).toLowerCase()),
        new DateTimeFormatMapping("IYYY", "YYYY"),
        new DateTimeFormatMapping("IYY", "YYYY", year -> year.substring(1)),
        new DateTimeFormatMapping("IY", "YY"),
        new DateTimeFormatMapping("I", "YY", year -> year.substring(1)),
        new DateTimeFormatMapping("Y,YYY", "yyyy", year -> year.charAt(0) + "," + year.substring(1)),
        new DateTimeFormatMapping("YYYY", "yyyy"),
        new DateTimeFormatMapping("YYY", "yyyy", year -> year.substring(1)),
        new DateTimeFormatMapping("YY", "yy"),
        new DateTimeFormatMapping("Y", "yy", year -> year.substring(1)),
    };
    private final Formatter formatter;


    public enum Formatter {
        FORMAT,
        DATE_TIME_FORMAT,
        TO_CHAR;

        private String getJavaPattern(String pattern) {
            if (this == FORMAT) {
                for (String[] replacement : JAVA_TIME_FORMAT_REPLACEMENTS) {
                    pattern = pattern.replace(replacement[0], replacement[1]);
                }
            }
            return pattern;
        }

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
                if (this == TO_CHAR) {
                    return formatUsingMappings(ta, patternString);
                }

                return format(getJavaPattern(patternString), ta);
            } catch (IllegalArgumentException | DateTimeException e) {
                throw new SqlIllegalArgumentException(
                    "Invalid pattern [{}] is received for formatting date/time [{}]; {}",
                    pattern,
                    timestamp,
                    e.getMessage()
                );
            }
        }

        private String formatUsingMappings(TemporalAccessor temporalAccessor, String pattern) {
            for (DateTimeFormatMapping mapping : JAVA_TIME_TO_CHAR_MAPPINGS) {
                if (pattern.contains(mapping.getPattern())) {
                    var middle = mapping
                        .getAdditionalMapper()
                        .apply(format(mapping.getJavaPattern(), temporalAccessor));

                    var parts = pattern.split(mapping.getPattern(), 2);
                    return formatUsingMappings(temporalAccessor, parts[0])
                        + middle
                        + formatUsingMappings(temporalAccessor, parts[1]);
                }
            }

            return format(pattern, temporalAccessor);
        }

        private String format(String pattern, TemporalAccessor temporalAccessor) {
            return DateTimeFormatter.ofPattern(pattern, Locale.ROOT)
                .format(temporalAccessor);
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
