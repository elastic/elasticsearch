/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql.expression.function.scalar.datetime;

import org.elasticsearch.common.Strings;

import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.TextStyle;
import java.time.temporal.ChronoField;
import java.time.temporal.IsoFields;
import java.time.temporal.JulianFields;
import java.time.temporal.TemporalAccessor;
import java.time.temporal.WeekFields;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.lang.Integer.parseInt;

/**
 * Formatting according to the PostgreSQL <code>to_char</code> function specification:
 * https://www.postgresql.org/docs/13/functions-formatting.html#FUNCTIONS-FORMATTING-DATETIME-TABLE
 */
class ToCharFormatter {

    protected static final Map<String, ToCharFormatter> FORMATTER_MAP;

    static {
        List<ToCharFormatter> formatters = List.of(
            of("HH").formatFn("hh").numeric(),
            of("HH12").formatFn("hh").numeric(),
            of("HH24").formatFn("HH").numeric(),
            of("MI").formatFn("mm").numeric(),
            of("SS").formatFn("s", x -> String.format(Locale.ROOT, "%02d", parseInt(x))).numeric(),
            of("MS").formatFn("n", nano -> firstDigitsOfNanos(nano, 3)).numericWithLeadingZeros(),
            of("US").formatFn("n", nano -> firstDigitsOfNanos(nano, 6)).numericWithLeadingZeros(),
            of("FF1").formatFn("n", nano -> firstDigitsOfNanos(nano, 1)).numericWithLeadingZeros(),
            of("FF2").formatFn("n", nano -> firstDigitsOfNanos(nano, 2)).numericWithLeadingZeros(),
            of("FF3").formatFn("n", nano -> firstDigitsOfNanos(nano, 3)).numericWithLeadingZeros(),
            of("FF4").formatFn("n", nano -> firstDigitsOfNanos(nano, 4)).numericWithLeadingZeros(),
            of("FF5").formatFn("n", nano -> firstDigitsOfNanos(nano, 5)).numericWithLeadingZeros(),
            of("FF6").formatFn("n", nano -> firstDigitsOfNanos(nano, 6)).numericWithLeadingZeros(),
            of("SSSSS").formatFn("A", milliSecondOfDay -> String.valueOf(parseInt(milliSecondOfDay) / 1000)).numeric(),
            of("SSSS").formatFn("A", milliSecondOfDay -> String.valueOf(parseInt(milliSecondOfDay) / 1000)).numeric(),
            of("AM").formatFn("a", x -> x.toUpperCase(Locale.ROOT)).text(),
            of("am").formatFn("a", x -> x.toLowerCase(Locale.ROOT)).text(),
            of("PM").formatFn("a", x -> x.toUpperCase(Locale.ROOT)).text(),
            of("pm").formatFn("a", x -> x.toLowerCase(Locale.ROOT)).text(),
            of("A.M.").formatFn("a", x -> x.charAt(0) + "." + x.charAt(1) + ".").text(),
            of("a.m.").formatFn("a", x -> (x.charAt(0) + "." + x.charAt(1) + ".").toLowerCase(Locale.ROOT)).text(),
            of("P.M.").formatFn("a", x -> x.charAt(0) + "." + x.charAt(1) + ".").text(),
            of("p.m.").formatFn("a", x -> (x.charAt(0) + "." + x.charAt(1) + ".").toLowerCase(Locale.ROOT)).text(),
            of("Y,YYY").formatFn("yyyy", year -> year.charAt(0) + "," + year.substring(1)).numericWithLeadingZeros(),
            of("YYYY").formatFn("yyyy").numeric(),
            of("YYY").formatFn("yyyy", year -> year.substring(1)).numeric(),
            of("YY").formatFn("yy").numeric(),
            of("Y").formatFn("yy", year -> year.substring(1)).numeric(),
            of("IYYY").formatFn(t -> lastNCharacter(absoluteWeekBasedYear(t), 4)).numeric(),
            of("IYY").formatFn(t -> lastNCharacter(absoluteWeekBasedYear(t), 3)).numeric(),
            of("IY").formatFn(t -> lastNCharacter(absoluteWeekBasedYear(t), 2)).numeric(),
            of("I").formatFn(t -> lastNCharacter(absoluteWeekBasedYear(t), 1)).numeric(),
            of("BC").formatFn("G").text(),
            of("bc").formatFn("G", x -> x.toLowerCase(Locale.ROOT)).text(),
            of("AD").formatFn("G").text(),
            of("ad").formatFn("G", x -> x.toLowerCase(Locale.ROOT)).text(),
            of("B.C.").formatFn("G", x -> x.charAt(0) + "." + x.charAt(1) + ".").text(),
            of("b.c.").formatFn("G", x -> (x.charAt(0) + "." + x.charAt(1) + ".").toLowerCase(Locale.ROOT)).text(),
            of("A.D.").formatFn("G", x -> x.charAt(0) + "." + x.charAt(1) + ".").text(),
            of("a.d.").formatFn("G", x -> (x.charAt(0) + "." + x.charAt(1) + ".").toLowerCase(Locale.ROOT)).text(),
            of("MONTH").formatFn("MMMM", x -> String.format(Locale.ROOT, "%-9s", x.toUpperCase(Locale.ROOT))).text(),
            of("Month").formatFn("MMMM", x -> String.format(Locale.ROOT, "%-9s", x)).text(),
            of("month").formatFn("MMMM", x -> String.format(Locale.ROOT, "%-9s", x.toLowerCase(Locale.ROOT))).text(),
            of("MON").formatFn("MMM", x -> x.toUpperCase(Locale.ROOT)).text(),
            of("Mon").formatFn("MMM").text(),
            of("mon").formatFn("MMM", x -> x.toLowerCase(Locale.ROOT)).text(),
            of("MM").formatFn("MM").numeric(),
            of("DAY").formatFn("EEEE", x -> String.format(Locale.ROOT, "%-9s", x.toUpperCase(Locale.ROOT))).text(),
            of("Day").formatFn("EEEE", x -> String.format(Locale.ROOT, "%-9s", x)).text(),
            of("day").formatFn("EEEE", x -> String.format(Locale.ROOT, "%-9s", x.toLowerCase(Locale.ROOT))).text(),
            of("DY").formatFn("E", x -> x.toUpperCase(Locale.ROOT)).text(),
            of("Dy").formatFn("E").text(),
            of("dy").formatFn("E", x -> x.toLowerCase(Locale.ROOT)).text(),
            of("DDD").formatFn("DDD").numeric(),
            of("IDDD").formatFn(t -> String.format(Locale.ROOT,
                "%03d",
                (t.get(WeekFields.ISO.weekOfWeekBasedYear()) - 1) * 7 + t.get(ChronoField.DAY_OF_WEEK))
            ).numeric(),
            of("DD").formatFn("d", x -> String.format(Locale.ROOT, "%02d", parseInt(x))).numeric(),
            of("ID").formatFn(t -> String.valueOf(t.get(ChronoField.DAY_OF_WEEK))).numeric(),
            of("D").formatFn(t -> String.valueOf(t.get(WeekFields.SUNDAY_START.dayOfWeek()))).numeric(),
            of("W").formatFn(t -> String.valueOf(t.get(ChronoField.ALIGNED_WEEK_OF_MONTH))).numeric(),
            of("WW").formatFn(t -> String.format(Locale.ROOT, "%02d", t.get(ChronoField.ALIGNED_WEEK_OF_YEAR))).numeric(),
            of("IW").formatFn(t -> String.format(Locale.ROOT, "%02d", t.get(WeekFields.ISO.weekOfWeekBasedYear()))).numeric(),
            of("CC").formatFn(t -> {
                int century = yearToCentury(t.get(ChronoField.YEAR));
                return String.format(Locale.ROOT, century < 0 ? "%03d" : "%02d", century);
            }).numeric(),
            of("J").formatFn(t -> String.valueOf(t.getLong(JulianFields.JULIAN_DAY))).numeric(),
            of("Q").formatFn("Q").numeric(),
            of("RM").formatFn("MM", month -> String.format(Locale.ROOT, "%-4s", monthToRoman(parseInt(month)))).text(),
            of("rm")
                .formatFn("MM", month -> String.format(Locale.ROOT, "%-4s", monthToRoman(parseInt(month)).toLowerCase(Locale.ROOT)))
                .text(),
            of("TZ").formatFn(ToCharFormatter::zoneAbbreviationOf).text(),
            of("tz").formatFn(t -> zoneAbbreviationOf(t).toLowerCase(Locale.ROOT)).text(),
            of("TZH").acceptsLowercase(false).formatFn("ZZ", s -> s.substring(0, 3)).text(),
            of("TZM").acceptsLowercase(false).formatFn("ZZ", s -> lastNCharacter(s, 2)).text(),
            of("OF").acceptsLowercase(false).formatFn("ZZZZZ", ToCharFormatter::formatOffset).offset()
        );

        Map<String, ToCharFormatter> formatterMap = new LinkedHashMap<>();
        for (ToCharFormatter formatter : formatters) {
            formatterMap.put(formatter.pattern, formatter);
        }
        // also index the lower case version of the patterns if accepted
        for (ToCharFormatter formatter : formatters) {
            if (formatter.acceptsLowercase) {
                formatterMap.putIfAbsent(formatter.pattern.toLowerCase(Locale.ROOT), formatter);
            }
        }
        FORMATTER_MAP = formatterMap;
    }

    private static final int MAX_TO_CHAR_FORMAT_STRING_LENGTH =
        FORMATTER_MAP.keySet().stream().mapToInt(String::length).max().orElse(Integer.MAX_VALUE);

    private static final String[] ROMAN_NUMBERS = {"I", "II", "III", "IV", "V", "VI", "VII", "VIII", "IX", "X", "XI", "XII"};

    private final String pattern;
    private final boolean acceptsLowercase;
    // Fill mode: suppress leading zeroes and padding blanks
    // https://www.postgresql.org/docs/13/functions-formatting.html#FUNCTIONS-FORMATTING-DATETIMEMOD-TABLE
    private final Function<String, String> fillModeFn;
    private final boolean hasOrdinalSuffix;
    private final Function<TemporalAccessor, String> formatter;

    private ToCharFormatter(
        String pattern,
        boolean acceptsLowercase,
        Function<String, String> fillModeFn,
        boolean hasOrdinalSuffix,
        Function<TemporalAccessor, String> formatter) {

        this.pattern = pattern;
        this.acceptsLowercase = acceptsLowercase;
        this.fillModeFn = fillModeFn;
        this.hasOrdinalSuffix = hasOrdinalSuffix;
        this.formatter = formatter;
    }

    private static Builder of(String pattern) {
        return new Builder(pattern);
    }

    private static String monthToRoman(int month) {
        return ROMAN_NUMBERS[month - 1];
    }

    private static int yearToCentury(int year) {
        int offset = -1;
        if (year > 0) {
            offset = year % 100 == 0 ? 0 : 1;
        }
        return year / 100 + offset;
    }

    private String format(TemporalAccessor temporalAccessor) {
        return formatter.apply(temporalAccessor);
    }

    private ToCharFormatter withModifier(Function<String, String> modifier) {
        return new ToCharFormatter(pattern, acceptsLowercase, fillModeFn, hasOrdinalSuffix, formatter.andThen(modifier));
    }

    private static List<ToCharFormatter> parsePattern(String toCharPattern) {
        LinkedList<ToCharFormatter> formatters = new LinkedList<>();

        while (toCharPattern.isEmpty() == false) {
            ToCharFormatter formatter = null;
            boolean fillModeModifierActive = false;

            // we try to match the following: ( fill-modifier? ( ( pattern ordinal-suffix-modifier? ) | literal-non-pattern ) ) *
            // and extract the individual patterns with the fill-modifiers and ordinal-suffix-modifiers or
            // the non-matched literals (removing the potential fill modifiers specified for them, FMFM turns into FM)

            // check for fill-modifier first
            if (toCharPattern.startsWith("FM") || toCharPattern.startsWith("fm")) {
                // try to apply the fill mode modifier to the next formatter
                fillModeModifierActive = true;
                toCharPattern = toCharPattern.substring(2);
            }

            // try to find a potential pattern next
            for (int length = Math.min(MAX_TO_CHAR_FORMAT_STRING_LENGTH, toCharPattern.length()); length >= 1; length--) {
                final String potentialPattern = toCharPattern.substring(0, length);
                formatter = FORMATTER_MAP.get(potentialPattern);
                // check if it is a known pattern string, if so apply it, with any modifier
                if (formatter != null) {
                    if (fillModeModifierActive && formatter.fillModeFn != null) {
                        formatter = formatter.withModifier(formatter.fillModeFn);
                    }
                    toCharPattern = toCharPattern.substring(length);
                    break;
                }
            }

            if (formatter == null) {
                // the fill mode modifier is dropped in case of literals
                formatter = literal(toCharPattern.substring(0, 1));
                toCharPattern = toCharPattern.substring(1);
            } else {
                // try to look for an ordinal suffix modifier in case we found a pattern
                if (toCharPattern.startsWith("TH") || toCharPattern.startsWith("th")) {
                    final String ordinalSuffixModifier = toCharPattern.substring(0, 2);
                    if (formatter.hasOrdinalSuffix) {
                        formatter = formatter.withModifier(s -> appendOrdinalSuffix(ordinalSuffixModifier, s));
                    }
                    toCharPattern = toCharPattern.substring(2);
                }
            }

            formatters.addLast(formatter);
        }
        return formatters;
    }

    public static Function<TemporalAccessor, String> ofPattern(String pattern) {
        if (Strings.isEmpty(pattern)) {
            return timestamp -> "";
        }
        final List<ToCharFormatter> toCharFormatters = parsePattern(pattern);
        return timestamp -> toCharFormatters.stream().map(p -> p.format(timestamp)).collect(Collectors.joining());
    }

    private static ToCharFormatter literal(String literal) {
        return new ToCharFormatter(literal, false, null, true, t -> literal);
    }

    private static String ordinalSuffix(int i) {
        if (i < 0) {
            i = -i;
        }
        int mod100 = i % 100;
        int mod10 = i % 10;
        if (mod10 == 1 && mod100 != 11) {
            return "st";
        } else if (mod10 == 2 && mod100 != 12) {
            return "nd";
        } else if (mod10 == 3 && mod100 != 13) {
            return "rd";
        } else {
            return "th";
        }
    }

    private static String appendOrdinalSuffix(String defaultSuffix, String s) {
        try {
            // the Y,YYY pattern might can cause problems with the parsing, but thankfully the last 3
            // characters is enough to calculate the suffix
            int i = parseInt(lastNCharacter(s, 3));
            final boolean upperCase = defaultSuffix.equals(defaultSuffix.toUpperCase(Locale.ROOT));
            return s + (upperCase ? ordinalSuffix(i).toUpperCase(Locale.ROOT) : ordinalSuffix(i));
        } catch (NumberFormatException ex) {
            return s + defaultSuffix;
        }
    }

    private static String formatOffset(String offset) {
        if (offset.equals("Z")) {
            return "+00";
        }
        if (offset.matches("^[+-][0-9][0-9]00$")) {
            offset = offset.substring(0, offset.length() - 2);
        } else if (offset.matches("^[+-][0-9]{3,4}$")) {
            offset = offset.substring(0, offset.length() - 2) + ":" + offset.substring(offset.length() - 2);
        } else if (offset.matches("^[+-][0-9][0-9]:00$")) {
            offset = offset.substring(0, offset.length() - 3);
        }
        return offset.substring(0, Math.min(offset.length(), 6));
    }

    private static String removeLeadingZerosFromOffset(String offset) {
        if (offset.matches("[+-]0{1,2}")) {
            return offset.substring(0, 2);
        } else {
            if (offset.startsWith("+0")) {
                return "+" + offset.substring(2);
            } else if (offset.startsWith("-0")) {
                return "-" + offset.substring(2);
            } else {
                return offset;
            }
        }
    }

    private static String absoluteWeekBasedYear(TemporalAccessor t) {
        int year = t.get(IsoFields.WEEK_BASED_YEAR);
        year = year > 0 ? year : -(year - 1);
        return String.format(Locale.ROOT, "%04d", year);
    }

    private static String firstDigitsOfNanos(String nano, int digits) {
        return String.format(Locale.ROOT, "%09d", parseInt(nano)).substring(0, digits);
    }

    private static String lastNCharacter(String s, int n) {
        return s.substring(Math.max(0, s.length() - n));
    }

    private static String zoneAbbreviationOf(TemporalAccessor temporalAccessor) {
        String zone = ZoneId.from(temporalAccessor).getDisplayName(TextStyle.SHORT, Locale.ROOT);
        return "Z".equals(zone) ? "UTC" : zone;
    }

    private static class Builder {

        private final String pattern;
        private boolean lowercaseAccepted = true;
        private Function<TemporalAccessor, String> formatFn;

        Builder(String pattern) {
            this.pattern = pattern;
        }

        public Builder formatFn(final String javaPattern) {
            return formatFn(javaPattern, null);
        }

        public Builder formatFn(final String javaPattern, final Function<String, String> additionalMapper) {
            this.formatFn = temporalAccessor -> {
                String formatted = DateTimeFormatter.ofPattern(javaPattern != null ? javaPattern : "'" + pattern + "'", Locale.ROOT)
                    .format(temporalAccessor);
                return additionalMapper == null ? formatted : additionalMapper.apply(formatted);
            };
            return this;
        }

        public Builder formatFn(Function<TemporalAccessor, String> formatFn) {
            this.formatFn = formatFn;
            return this;
        }

        public ToCharFormatter numeric() {
            return build(number -> String.valueOf(parseInt(number)), true);
        }

        public ToCharFormatter numericWithLeadingZeros() {
            return build(null, true);
        }

        public ToCharFormatter text() {
            return build(paddedText -> paddedText.replaceAll(" +$", ""), false);
        }

        public ToCharFormatter offset() {
            return build(ToCharFormatter::removeLeadingZerosFromOffset, false);
        }

        public Builder acceptsLowercase(boolean lowercaseAccepted) {
            this.lowercaseAccepted = lowercaseAccepted;
            return this;
        }

        private ToCharFormatter build(Function<String, String> fillModeFn, boolean hasOrdinalSuffix) {
            return new ToCharFormatter(pattern, lowercaseAccepted, fillModeFn, hasOrdinalSuffix, formatFn);
        }
    }
}
