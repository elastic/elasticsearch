/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.sql.expression.function.scalar.datetime;

import org.elasticsearch.common.Strings;

import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.TextStyle;
import java.time.temporal.ChronoField;
import java.time.temporal.IsoFields;
import java.time.temporal.TemporalAccessor;
import java.time.temporal.WeekFields;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.sql.util.DateUtils.century;
import static org.elasticsearch.xpack.sql.util.DateUtils.toRome;

class ToCharFormatter {

    private static final Map<String, ToCharFormatter> FORMATTER_MAP;

    static {
        List<ToCharFormatter> formatters = List.of(
            ToCharFormatter.of("HH").formatFn("hh"),
            ToCharFormatter.of("HH12").formatFn("hh"),
            ToCharFormatter.of("HH24").formatFn("HH"),
            ToCharFormatter.of("MI").formatFn("mm"),
            ToCharFormatter.of("SS").formatFn("s", x -> String.format(Locale.ROOT, "%02d", Integer.parseInt(x))),
            ToCharFormatter.of("MS").formatFn("n", nano -> firstDigitsOfNanos(nano, 3)).noFillMode(),
            ToCharFormatter.of("US").formatFn("n", nano -> firstDigitsOfNanos(nano, 6)).noFillMode(),
            ToCharFormatter.of("FF1").formatFn("n", nano -> firstDigitsOfNanos(nano, 1)).noFillMode(),
            ToCharFormatter.of("FF2").formatFn("n", nano -> firstDigitsOfNanos(nano, 2)).noFillMode(),
            ToCharFormatter.of("FF3").formatFn("n", nano -> firstDigitsOfNanos(nano, 3)).noFillMode(),
            ToCharFormatter.of("FF4").formatFn("n", nano -> firstDigitsOfNanos(nano, 4)).noFillMode(),
            ToCharFormatter.of("FF5").formatFn("n", nano -> firstDigitsOfNanos(nano, 5)).noFillMode(),
            ToCharFormatter.of("FF6").formatFn("n", nano -> firstDigitsOfNanos(nano, 6)).noFillMode(),
            ToCharFormatter.of("SSSSS").formatFn("A", milliSecondOfDay -> String.valueOf(Integer.parseInt(milliSecondOfDay) / 1000)),
            ToCharFormatter.of("SSSS").formatFn("A", milliSecondOfDay -> String.valueOf(Integer.parseInt(milliSecondOfDay) / 1000)),
            ToCharFormatter.of("AM").formatFn("a", x -> x.toUpperCase(Locale.ROOT)).text(),
            ToCharFormatter.of("am").formatFn("a", x -> x.toLowerCase(Locale.ROOT)).text(),
            ToCharFormatter.of("PM").formatFn("a", x -> x.toUpperCase(Locale.ROOT)).text(),
            ToCharFormatter.of("pm").formatFn("a", x -> x.toLowerCase(Locale.ROOT)).text(),
            ToCharFormatter.of("A.M.").formatFn("a", x -> x.charAt(0) + "." + x.charAt(1) + ".").text(),
            ToCharFormatter.of("a.m.").formatFn("a", x -> (x.charAt(0) + "." + x.charAt(1) + ".").toLowerCase(Locale.ROOT)).text(),
            ToCharFormatter.of("P.M.").formatFn("a", x -> x.charAt(0) + "." + x.charAt(1) + ".").text(),
            ToCharFormatter.of("p.m.").formatFn("a", x -> (x.charAt(0) + "." + x.charAt(1) + ".").toLowerCase(Locale.ROOT)).text(),
            ToCharFormatter.of("Y,YYY").formatFn("yyyy", year -> year.charAt(0) + "," + year.substring(1)).noFillMode(),
            ToCharFormatter.of("YYYY").formatFn("yyyy"),
            ToCharFormatter.of("YYY").formatFn("yyyy", year -> year.substring(1)),
            ToCharFormatter.of("YY").formatFn("yy"),
            ToCharFormatter.of("Y").formatFn("yy", year -> year.substring(1)),
            ToCharFormatter.of("IYYY").formatFn(t -> lastNCharacter(absoluteWeekBasedYear(t), 4)),
            ToCharFormatter.of("IYY").formatFn(t -> lastNCharacter(absoluteWeekBasedYear(t), 3)),
            ToCharFormatter.of("IY").formatFn(t -> lastNCharacter(absoluteWeekBasedYear(t), 2)),
            ToCharFormatter.of("I").formatFn(t -> lastNCharacter(absoluteWeekBasedYear(t), 1)),
            ToCharFormatter.of("BC").formatFn("G").text(),
            ToCharFormatter.of("bc").formatFn("G", x -> x.toLowerCase(Locale.ROOT)).text(),
            ToCharFormatter.of("AD").formatFn("G").text(),
            ToCharFormatter.of("ad").formatFn("G", x -> x.toLowerCase(Locale.ROOT)).text(),
            ToCharFormatter.of("B.C.").formatFn("G", x -> x.charAt(0) + "." + x.charAt(1) + ".").text(),
            ToCharFormatter.of("b.c.").formatFn("G", x -> (x.charAt(0) + "." + x.charAt(1) + ".").toLowerCase(Locale.ROOT)).text(),
            ToCharFormatter.of("A.D.").formatFn("G", x -> x.charAt(0) + "." + x.charAt(1) + ".").text(),
            ToCharFormatter.of("a.d.").formatFn("G", x -> (x.charAt(0) + "." + x.charAt(1) + ".").toLowerCase(Locale.ROOT)).text(),
            ToCharFormatter.of("MONTH").formatFn("MMMM", x -> String.format(Locale.ROOT, "%-9s", x.toUpperCase(Locale.ROOT))).text(),
            ToCharFormatter.of("Month").formatFn("MMMM", x -> String.format(Locale.ROOT, "%-9s", x)).text(),
            ToCharFormatter.of("month").formatFn("MMMM", x -> String.format(Locale.ROOT, "%-9s", x.toLowerCase(Locale.ROOT))).text(),
            ToCharFormatter.of("MON").formatFn("MMM", x -> x.toUpperCase(Locale.ROOT)).text(),
            ToCharFormatter.of("Mon").formatFn("MMM").text(),
            ToCharFormatter.of("mon").formatFn("MMM", x -> x.toLowerCase(Locale.ROOT)).text(),
            ToCharFormatter.of("MM").formatFn("MM"),
            ToCharFormatter.of("DAY").formatFn("EEEE", x -> String.format(Locale.ROOT, "%-9s", x.toUpperCase(Locale.ROOT))).text(),
            ToCharFormatter.of("Day").formatFn("EEEE", x -> String.format(Locale.ROOT, "%-9s", x)).text(),
            ToCharFormatter.of("day").formatFn("EEEE", x -> String.format(Locale.ROOT, "%-9s", x.toLowerCase(Locale.ROOT))).text(),
            ToCharFormatter.of("DY").formatFn("E", x -> x.toUpperCase(Locale.ROOT)).text(),
            ToCharFormatter.of("Dy").formatFn("E").text(),
            ToCharFormatter.of("dy").formatFn("E", x -> x.toLowerCase(Locale.ROOT)).text(),
            ToCharFormatter.of("DDD").formatFn("DDD"),
            ToCharFormatter.of("IDDD")
                .formatFn(t -> String.format(Locale.ROOT,
                    "%03d",
                    (t.get(WeekFields.ISO.weekOfWeekBasedYear()) - 1) * 7 + t.get(ChronoField.DAY_OF_WEEK))),
            ToCharFormatter.of("DD").formatFn("d", x -> String.format(Locale.ROOT, "%02d", Integer.parseInt(x))),
            ToCharFormatter.of("ID").formatFn("e"),
            ToCharFormatter.of("D").formatFn(t -> String.valueOf(t.get(WeekFields.SUNDAY_START.dayOfWeek()))),
            ToCharFormatter.of("W").formatFn(t -> String.valueOf(t.get(ChronoField.ALIGNED_WEEK_OF_MONTH))),
            ToCharFormatter.of("WW").formatFn(t -> String.format(Locale.ROOT, "%02d", t.get(ChronoField.ALIGNED_WEEK_OF_YEAR))),
            ToCharFormatter.of("IW").formatFn(t -> String.format(Locale.ROOT, "%02d", t.get(WeekFields.ISO.weekOfWeekBasedYear()))),
            ToCharFormatter.of("CC").formatFn(t -> {
                int century = century(t.get(ChronoField.YEAR));
                return String.format(Locale.ROOT, century < 0 ? "%03d" : "%02d", century);
            }),
            ToCharFormatter.of("J").formatFn("g", modifiedJulianDay -> String.valueOf(Integer.parseInt(modifiedJulianDay) + 2400001)),
            ToCharFormatter.of("Q").formatFn("Q"),
            ToCharFormatter.of("RM").formatFn("MM", month -> String.format(Locale.ROOT, "%-4s", toRome(Integer.parseInt(month)))).text(),
            ToCharFormatter.of("rm")
                .formatFn("MM", month -> String.format(Locale.ROOT, "%-4s", toRome(Integer.parseInt(month)).toLowerCase(Locale.ROOT)))
                .text(),
            ToCharFormatter.of("TZ").formatFn(ToCharFormatter::zoneAbbreviationOf).text(),
            ToCharFormatter.of("tz").formatFn(t -> zoneAbbreviationOf(t).toLowerCase(Locale.ROOT)).text(),
            ToCharFormatter.of("TZH").lowercaseAccepted(false).formatFn("ZZ", s -> s.substring(0, 3)).text(),
            ToCharFormatter.of("TZM").lowercaseAccepted(false).formatFn("ZZ", s -> lastNCharacter(s, 2)).text(),
            ToCharFormatter.of("OF")
                .lowercaseAccepted(false)
                .formatFn("ZZZZZ", ToCharFormatter::formatOffset)
                .text(ToCharFormatter::fillOffset)).stream().map(Builder::build).collect(Collectors.toList());

        Map<String, ToCharFormatter> formatterMap = new LinkedHashMap<>();
        for (ToCharFormatter formatter : formatters) {
            formatterMap.put(formatter.pattern, formatter);
        }
        // also index the lower case version of the patterns if accepted
        for (ToCharFormatter formatter : formatters) {
            if (formatter.lowercaseAccepted) {
                formatterMap.putIfAbsent(formatter.pattern.toLowerCase(Locale.ROOT), formatter);
            }
        }
        FORMATTER_MAP = formatterMap;
    }

    @SuppressWarnings("OptionalGetWithoutIsPresent") private static final int
        MAX_TO_CHAR_FORMAT_STRING_LENGTH =
        FORMATTER_MAP.keySet().stream().mapToInt(String::length).max().getAsInt();

    private final String pattern;
    private final boolean lowercaseAccepted;
    private final Function<String, String> fillMode;
    private final boolean hasOrdinalSuffix;
    private final Function<TemporalAccessor, String> formatFn;

    private ToCharFormatter(
        String pattern,
        boolean lowercaseAccepted,
        Function<String, String> fillMode,
        boolean hasOrdinalSuffix,
        Function<TemporalAccessor, String> formatFn) {

        this.pattern = pattern;
        this.lowercaseAccepted = lowercaseAccepted;
        this.fillMode = fillMode;
        this.hasOrdinalSuffix = hasOrdinalSuffix;
        this.formatFn = formatFn;
    }

    private static Builder of(String pattern) {
        return new Builder(pattern);
    }

    private String format(TemporalAccessor temporalAccessor) {
        return formatFn.apply(temporalAccessor);
    }

    private ToCharFormatter withModifier(Function<String, String> modifierFn) {
        return new ToCharFormatter(pattern, lowercaseAccepted, fillMode, hasOrdinalSuffix, formatFn.andThen(modifierFn));
    }

    private static List<ToCharFormatter> parsePattern(String toCharPattern) {
        LinkedList<ToCharFormatter> formatters = new LinkedList<>();

        boolean fillModeModifierActive = false;

        while (toCharPattern.isEmpty() == false) {
            boolean foundPattern = false;
            for (int length = Math.min(MAX_TO_CHAR_FORMAT_STRING_LENGTH, toCharPattern.length()); length >= 1; length--) {
                final String potentialPattern = toCharPattern.substring(0, length);
                if ("FM".equals(potentialPattern) || "fm".equals(potentialPattern)) {
                    // try to apply the fill mode modifier to the next formatter
                    fillModeModifierActive = true;
                    foundPattern = true;
                } else if ("TH".equals(potentialPattern) || "th".equals(potentialPattern)) {
                    // try to apply the ordinal suffix modifier to the last formatter
                    ToCharFormatter lastFormatter = formatters.removeLast();
                    if (lastFormatter == null) {
                        lastFormatter = literal(potentialPattern);
                    } else if (lastFormatter.hasOrdinalSuffix) {
                        // if there was no previous pattern, simply prints the ordinal suffix pattern
                        final boolean upperCase = "TH".equals(potentialPattern);
                        lastFormatter = lastFormatter.withModifier(s -> appendOrdinalSuffix(potentialPattern, upperCase, s));
                    }
                    formatters.addLast(lastFormatter);
                    foundPattern = true;
                } else {
                    ToCharFormatter formatter = FORMATTER_MAP.get(potentialPattern);
                    // check if it is a known pattern string, if so apply it, with any modifier
                    if (formatter != null) {
                        if (fillModeModifierActive && formatter.fillMode != null) {
                            formatter = formatter.withModifier(formatter.fillMode);
                        }
                        formatters.addLast(formatter);
                        fillModeModifierActive = false;
                        foundPattern = true;
                    }
                }
                if (foundPattern) {
                    toCharPattern = toCharPattern.substring(length);
                    break;
                }
            }
            if (foundPattern == false) {
                // the fill mode modifier does not apply in case of literals
                formatters.addLast(literal(toCharPattern.substring(0, 1)));
                toCharPattern = toCharPattern.substring(1);
                fillModeModifierActive = false;
            }
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

    private static String appendOrdinalSuffix(String defaultSuffix, boolean upperCase, String s) {
        try {
            // the Y,YYY pattern might can cause problems with the parsing, but thankfully the last 3
            // characters is enough to calculate the suffix
            int i = Integer.parseInt(lastNCharacter(s, 3));
            return s + (upperCase ? ordinalSuffix(i).toUpperCase(Locale.ROOT) : ordinalSuffix(i));
        } catch (NumberFormatException ex) {
            return s + defaultSuffix;
        }
    }

    private static String fillNumeric(String paddedNumber) {
        return String.valueOf(Integer.parseInt(paddedNumber));
    }

    private static String fillText(String paddedText) {
        return paddedText.replaceAll(" +$", "");
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

    private static String fillOffset(String offset) {
        //offset = offset.replace(":{0,1}00$", "");
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
        return String.format(Locale.ROOT, "%09d", Integer.parseInt(nano)).substring(0, digits);
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
        private Function<String, String> fillMode = ToCharFormatter::fillNumeric;
        private boolean hasOrdinalSuffix = true;
        private Function<TemporalAccessor, String> formatFn;

        Builder(String pattern) {
            this.pattern = pattern;
        }

        public Builder formatFn(final String javaPatterm) {
            return formatFn(javaPatterm, null);
        }

        public Builder formatFn(final String javaPattern, final Function<String, String> additionalMapper) {
            this.formatFn = temporalAccessor -> {
                String
                    formatted =
                    DateTimeFormatter.ofPattern(javaPattern != null ? javaPattern : "'" + pattern + "'", Locale.ROOT)
                        .format(temporalAccessor);
                return additionalMapper == null ? formatted : additionalMapper.apply(formatted);
            };
            return this;
        }

        public Builder formatFn(Function<TemporalAccessor, String> formatFn) {
            this.formatFn = formatFn;
            return this;
        }

        public Builder noFillMode() {
            this.fillMode = null;
            return this;
        }

        public Builder text() {
            return text(ToCharFormatter::fillText);
        }

        public Builder text(Function<String, String> fillMode) {
            this.hasOrdinalSuffix = false;
            this.fillMode = fillMode;
            return this;
        }

        public Builder lowercaseAccepted(boolean lowercaseAccepted) {
            this.lowercaseAccepted = lowercaseAccepted;
            return this;
        }

        public ToCharFormatter build() {
            return new ToCharFormatter(pattern, lowercaseAccepted, fillMode, hasOrdinalSuffix, formatFn);
        }
    }
}
