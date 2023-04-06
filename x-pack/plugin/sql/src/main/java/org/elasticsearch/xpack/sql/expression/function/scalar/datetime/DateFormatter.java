/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql.expression.function.scalar.datetime;

import org.elasticsearch.common.Strings;

import java.time.DayOfWeek;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalAccessor;
import java.time.temporal.WeekFields;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Formatting according to the MySQL <code>DATE_FORMAT</code> function specification:
 * <a href="https://dev.mysql.com/doc/refman/8.0/en/date-and-time-functions.html#function_date-format">MySQL DATE_FORMAT function</a>
 */
class DateFormatter {

    protected static final Map<String, DateFormatter> FORMATTER_MAP;

    static {
        List<DateFormatter> formatters = List.of(
            new Builder().pattern("%a").javaPattern("E").build(),
            new Builder().pattern("%b").javaPattern("MMM").build(),
            new Builder().pattern("%c").javaPattern("M").build(),
            new Builder().pattern("%D")
                .javaPattern("d")
                .additionalMapper(s -> s + ordinalSuffixForDayOfTheMonth(Integer.parseInt(s)))
                .build(),
            new Builder().pattern("%d").javaPattern("dd").build(),
            new Builder().pattern("%e").javaPattern("d").build(),
            new Builder().pattern("%f")
                .javaPattern("n")
                .additionalMapper(s -> String.format(Locale.ROOT, "%06d", Math.round(Integer.parseInt(s) / 1000.0)))
                .build(),
            new Builder().pattern("%H").javaPattern("HH").build(),
            new Builder().pattern("%h").javaPattern("hh").build(),
            new Builder().pattern("%I").javaPattern("hh").build(),
            new Builder().pattern("%i").javaPattern("mm").build(),
            new Builder().pattern("%j").javaPattern("DDD").build(),
            new Builder().pattern("%k").javaPattern("H").build(),
            new Builder().pattern("%l").javaPattern("h").build(),
            new Builder().pattern("%M").javaPattern("MMMM").build(),
            new Builder().pattern("%m").javaPattern("MM").build(),
            new Builder().pattern("%p").javaPattern("a").build(),
            new Builder().pattern("%r").javaPattern("hh:mm:ss a").build(),
            new Builder().pattern("%S").javaPattern("ss").build(),
            new Builder().pattern("%s").javaPattern("ss").build(),
            new Builder().pattern("%T").javaPattern("HH:mm:ss").build(),
            new Builder().pattern("%U")
                .javaFormat(t -> String.format(Locale.ROOT, "%02d", t.get(WeekFields.of(DayOfWeek.SUNDAY, 7).weekOfYear())))
                .build(),
            new Builder().pattern("%u").javaFormat(t -> String.format(Locale.ROOT, "%02d", t.get(WeekFields.ISO.weekOfYear()))).build(),
            new Builder().pattern("%V")
                .javaFormat(t -> String.format(Locale.ROOT, "%02d", t.get(WeekFields.of(DayOfWeek.SUNDAY, 7).weekOfWeekBasedYear())))
                .build(),
            new Builder().pattern("%v").javaPattern("ww").build(),
            new Builder().pattern("%W").javaPattern("EEEE").build(),
            new Builder().pattern("%w").javaPattern("e").additionalMapper(s -> Integer.parseInt(s) == 7 ? String.valueOf(0) : s).build(),
            new Builder().pattern("%X")
                .javaFormat(t -> String.format(Locale.ROOT, "%04d", t.get(WeekFields.of(DayOfWeek.SUNDAY, 7).weekBasedYear())))
                .build(),
            new Builder().pattern("%x").javaPattern("Y").build(),
            new Builder().pattern("%Y").javaPattern("yyyy").build(),
            new Builder().pattern("%y").javaPattern("yy").build()
        );

        Map<String, DateFormatter> formatterMap = new LinkedHashMap<>();
        for (DateFormatter dateFormatter : formatters) {
            formatterMap.put(dateFormatter.pattern, dateFormatter);
        }
        FORMATTER_MAP = formatterMap;
    }

    private static String ordinalSuffixForDayOfTheMonth(int i) {
        if (i == 1 || i == 21 || i == 31) {
            return "st";
        } else if (i == 2 || i == 22) {
            return "nd";
        } else if (i == 3 || i == 23) {
            return "rd";
        } else {
            return "th";
        }

    }

    private String pattern;
    private Function<TemporalAccessor, String> javaFormat;
    private Function<String, String> additionalMapper;

    static Function<TemporalAccessor, String> ofPattern(String dateFormatPattern) {
        if (Strings.isEmpty(dateFormatPattern)) {
            return timestamp -> "";
        }
        List<DateFormatter> dateFormatters = parsePattern(dateFormatPattern);
        return timestamp -> dateFormatters.stream().map(p -> p.format(timestamp)).collect(Collectors.joining());
    }

    private String format(TemporalAccessor timestamp) {
        String formatted = this.javaFormat.apply(timestamp);
        return additionalMapper == null ? formatted : this.additionalMapper.apply(formatted);
    }

    private static List<DateFormatter> parsePattern(String dateFormatPattern) {
        LinkedList<DateFormatter> formatters = new LinkedList<>();
        DateFormatter dateFormatter;

        while (dateFormatPattern.length() > 1) {
            String potentialPattern = dateFormatPattern.substring(0, 2);
            dateFormatter = FORMATTER_MAP.get(potentialPattern);

            if (dateFormatter != null) {
                dateFormatPattern = dateFormatPattern.substring(2);
            } else if (potentialPattern.startsWith("%")) {
                dateFormatter = literal(dateFormatPattern.substring(1, 2));
                dateFormatPattern = dateFormatPattern.substring(2);
            } else if (potentialPattern.endsWith("%")) {
                dateFormatter = literal(dateFormatPattern.substring(0, 1));
                dateFormatPattern = dateFormatPattern.substring(1);
            } else {
                dateFormatter = literal(dateFormatPattern.substring(0, 2));
                dateFormatPattern = dateFormatPattern.substring(2);
            }

            formatters.addLast(dateFormatter);

        }
        if (dateFormatPattern.length() == 1) {
            dateFormatter = literal(dateFormatPattern.substring(0, 1));
            formatters.addLast(dateFormatter);
        }

        return formatters;
    }

    private static DateFormatter literal(String literal) {
        DateFormatter dateFormatter = new DateFormatter();
        dateFormatter.javaFormat = timestamp -> literal;
        return dateFormatter;
    }

    private static class Builder {
        private String pattern;
        private Function<TemporalAccessor, String> javaFormat;
        private Function<String, String> additionalMapper;

        private Builder pattern(String pattern) {
            this.pattern = pattern;
            return this;
        }

        private Builder javaPattern(String javaPattern) {
            this.javaFormat = temporalAccessor -> DateTimeFormatter.ofPattern(javaPattern, Locale.ROOT).format(temporalAccessor);
            return this;
        }

        private Builder javaFormat(Function<TemporalAccessor, String> javaFormat) {
            this.javaFormat = javaFormat;
            return this;
        }

        private Builder additionalMapper(Function<String, String> additionalMapper) {
            this.additionalMapper = additionalMapper;
            return this;
        }

        private DateFormatter build() {
            DateFormatter dateFormatter = new DateFormatter();
            dateFormatter.pattern = this.pattern;
            dateFormatter.javaFormat = this.javaFormat;
            dateFormatter.additionalMapper = additionalMapper;
            return dateFormatter;
        }

    }
}
