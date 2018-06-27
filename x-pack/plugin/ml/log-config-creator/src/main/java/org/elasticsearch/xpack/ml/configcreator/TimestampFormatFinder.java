/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.configcreator;

import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.grok.Grok;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public final class TimestampFormatFinder {

    private static final String PREFACE = "preface";
    private static final String EPILOGUE = "epilogue";

    private static final Pattern FRACTIONAL_SECOND_INTERPRETER = Pattern.compile("([:.,])(\\d{3,9})");
    private static final char DEFAULT_FRACTIONAL_SECOND_SEPARATOR = ',';

    /**
     * The timestamp patterns are complex and it can be slow to prove they do not
     * match anywhere in a long message.  Many of the timestamps have similar and
     * will never be found in a string if simpler sub-patterns do not exist in the
     * string.  These sub-patterns can be used to quickly rule out multiple complex
     * patterns.
     */
    private static final List<Pattern> QUICK_RULE_OUT_PATTERNS = Arrays.asList(
        Pattern.compile("\\b\\d{4}-\\d{2}-\\d{2} "),
        Pattern.compile("\\d \\d{2}:\\d{2}\\b"),
        Pattern.compile(" \\d{2}:\\d{2}:\\d{2} ")
    );

    /**
     * The first match in this list will be chosen, so it needs to be ordered
     * such that more generic patterns come after more specific patterns.
     */
    static final List<CandidateTimestampFormat> ORDERED_CANDIDATE_FORMATS = Arrays.asList(
        // The TOMCAT_DATESTAMP format has to come before ISO8601 because it's basically ISO8601 but
        // with a space before the timezone, and because the timezone is optional in ISO8601 it will
        // be recognised as that with the timezone missed off if ISO8601 is checked first
        new CandidateTimestampFormat("YYYY-MM-dd HH:mm:ss,SSS Z", "\\b\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2},\\d{3}",
            "\\b20\\d{2}-%{MONTHNUM}-%{MONTHDAY} %{HOUR}:?%{MINUTE}:(?:[0-5][0-9]|60)[:.,][0-9]{3,9} (?:Z|[+-]%{HOUR}%{MINUTE})\\b",
            "TOMCAT_DATESTAMP", Arrays.asList(0, 1)),
        // The Elasticsearch ISO8601 parser requires a literal T between the date and time, so
        // longhand formats are needed if there's a space instead
        new CandidateTimestampFormat("YYYY-MM-dd HH:mm:ss,SSSZ", "\\b\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2},\\d{3}",
            "\\b%{YEAR}-%{MONTHNUM}-%{MONTHDAY} %{HOUR}:?%{MINUTE}:(?:[0-5][0-9]|60)[:.,][0-9]{3,9}(?:Z|[+-]%{HOUR}%{MINUTE})\\b",
            "TIMESTAMP_ISO8601", Arrays.asList(0, 1)),
        new CandidateTimestampFormat("YYYY-MM-dd HH:mm:ss,SSSZZ", "\\b\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2},\\d{3}",
            "\\b%{YEAR}-%{MONTHNUM}-%{MONTHDAY} %{HOUR}:?%{MINUTE}:(?:[0-5][0-9]|60)[:.,][0-9]{3,9}[+-]%{HOUR}:%{MINUTE}\\b",
            "TIMESTAMP_ISO8601", Arrays.asList(0, 1)),
        new CandidateTimestampFormat("YYYY-MM-dd HH:mm:ss,SSS", "\\b\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2},\\d{3}",
            "\\b%{YEAR}-%{MONTHNUM}-%{MONTHDAY} %{HOUR}:?%{MINUTE}:(?:[0-5][0-9]|60)[:.,][0-9]{3,9}\\b", "TIMESTAMP_ISO8601",
            Arrays.asList(0, 1)),
        new CandidateTimestampFormat("YYYY-MM-dd HH:mm:ssZ", "\\b\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}",
            "\\b%{YEAR}-%{MONTHNUM}-%{MONTHDAY} %{HOUR}:?%{MINUTE}:(?:[0-5][0-9]|60)(?:Z|[+-]%{HOUR}%{MINUTE})\\b", "TIMESTAMP_ISO8601",
            Arrays.asList(0, 1)),
        new CandidateTimestampFormat("YYYY-MM-dd HH:mm:ssZZ", "\\b\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}",
            "\\b%{YEAR}-%{MONTHNUM}-%{MONTHDAY} %{HOUR}:?%{MINUTE}:(?:[0-5][0-9]|60)[+-]%{HOUR}:%{MINUTE}\\b", "TIMESTAMP_ISO8601",
            Arrays.asList(0, 1)),
        new CandidateTimestampFormat("YYYY-MM-dd HH:mm:ss", "\\b\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}",
            "\\b%{YEAR}-%{MONTHNUM}-%{MONTHDAY} %{HOUR}:?%{MINUTE}:(?:[0-5][0-9]|60)\\b", "TIMESTAMP_ISO8601",
            Arrays.asList(0, 1)),
        new CandidateTimestampFormat("ISO8601", "\\b\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}", "\\b%{TIMESTAMP_ISO8601}\\b",
            "TIMESTAMP_ISO8601"),
        new CandidateTimestampFormat("EEE MMM dd YYYY HH:mm:ss zzz",
            "\\b[A-Z]\\S{2,8} [A-Z]\\S{2,8} \\d{1,2} \\d{4} \\d{2}:\\d{2}:\\d{2} ",
            "\\b%{DAY} %{MONTH} %{MONTHDAY} %{YEAR} %{HOUR}:%{MINUTE}:(?:[0-5][0-9]|60) %{TZ}\\b", "DATESTAMP_RFC822", Arrays.asList(1, 2)),
        new CandidateTimestampFormat("EEE MMM dd YYYY HH:mm zzz", "\\b[A-Z]\\S{2,8} [A-Z]\\S{2,8} \\d{1,2} \\d{4} \\d{2}:\\d{2} ",
            "\\b%{DAY} %{MONTH} %{MONTHDAY} %{YEAR} %{HOUR}:%{MINUTE} %{TZ}\\b", "DATESTAMP_RFC822", Collections.singletonList(1)),
        new CandidateTimestampFormat("EEE, dd MMM YYYY HH:mm:ss ZZ",
            "\\b[A-Z]\\S{2,8}, \\d{1,2} [A-Z]\\S{2,8} \\d{4} \\d{2}:\\d{2}:\\d{2} ",
            "\\b%{DAY}, %{MONTHDAY} %{MONTH} %{YEAR} %{HOUR}:%{MINUTE}:(?:[0-5][0-9]|60) (?:Z|[+-]%{HOUR}:%{MINUTE})\\b",
            "DATESTAMP_RFC2822", Arrays.asList(1, 2)),
        new CandidateTimestampFormat("EEE, dd MMM YYYY HH:mm:ss Z",
            "\\b[A-Z]\\S{2,8}, \\d{1,2} [A-Z]\\S{2,8} \\d{4} \\d{2}:\\d{2}:\\d{2} ",
            "\\b%{DAY}, %{MONTHDAY} %{MONTH} %{YEAR} %{HOUR}:%{MINUTE}:(?:[0-5][0-9]|60) (?:Z|[+-]%{HOUR}%{MINUTE})\\b",
            "DATESTAMP_RFC2822", Arrays.asList(1, 2)),
        new CandidateTimestampFormat("EEE, dd MMM YYYY HH:mm ZZ", "\\b[A-Z]\\S{2,8}, \\d{1,2} [A-Z]\\S{2,8} \\d{4} \\d{2}:\\d{2} ",
            "\\b%{DAY}, %{MONTHDAY} %{MONTH} %{YEAR} %{HOUR}:%{MINUTE} (?:Z|[+-]%{HOUR}:%{MINUTE})\\b", "DATESTAMP_RFC2822",
            Collections.singletonList(1)),
        new CandidateTimestampFormat("EEE, dd MMM YYYY HH:mm Z", "\\b[A-Z]\\S{2,8}, \\d{1,2} [A-Z]\\S{2,8} \\d{4} \\d{2}:\\d{2} ",
            "\\b%{DAY}, %{MONTHDAY} %{MONTH} %{YEAR} %{HOUR}:%{MINUTE} (?:Z|[+-]%{HOUR}%{MINUTE})\\b", "DATESTAMP_RFC2822",
            Collections.singletonList(1)),
        new CandidateTimestampFormat("EEE MMM dd HH:mm:ss zzz YYYY",
            "\\b[A-Z]\\S{2,8} [A-Z]\\S{2,8} \\d{1,2} \\d{2}:\\d{2}:\\d{2} [A-Z]{3,4} \\d{4}\\b",
            "\\b%{DAY} %{MONTH} %{MONTHDAY} %{HOUR}:%{MINUTE}:(?:[0-5][0-9]|60) %{TZ} %{YEAR}\\b", "DATESTAMP_OTHER",
            Arrays.asList(1, 2)),
        new CandidateTimestampFormat("EEE MMM dd HH:mm zzz YYYY",
            "\\b[A-Z]\\S{2,8} [A-Z]\\S{2,8} \\d{1,2} \\d{2}:\\d{2} [A-Z]{3,4} \\d{4}\\b",
            "\\b%{DAY} %{MONTH} %{MONTHDAY} %{HOUR}:%{MINUTE} %{TZ} %{YEAR}\\b", "DATESTAMP_OTHER", Collections.singletonList(1)),
        new CandidateTimestampFormat("YYYYMMddHHmmss", "\\b\\d{14}\\b",
            "\\b20\\d{2}%{MONTHNUM2}(?:(?:0[1-9])|(?:[12][0-9])|(?:3[01]))(?:2[0123]|[01][0-9])%{MINUTE}(?:[0-5][0-9]|60)\\b",
            "DATESTAMP_EVENTLOG"),
        new CandidateTimestampFormat("EEE MMM dd HH:mm:ss YYYY",
            "\\b[A-Z]\\S{2,8} [A-Z]\\S{2,8} \\d{1,2} \\d{2}:\\d{2}:\\d{2} \\d{4}\\b",
            "\\b%{DAY} %{MONTH} %{MONTHDAY} %{HOUR}:%{MINUTE}:(?:[0-5][0-9]|60) %{YEAR}\\b", "HTTPDERROR_DATE", Arrays.asList(1, 2)),
        new CandidateTimestampFormat(Arrays.asList("MMM dd HH:mm:ss", "MMM  d HH:mm:ss"),
            "\\b[A-Z]\\S{2,8} {1,2}\\d{1,2} \\d{2}:\\d{2}:\\d{2}\\b", "%{MONTH} +%{MONTHDAY} %{HOUR}:%{MINUTE}:(?:[0-5][0-9]|60)\\b",
            "SYSLOGTIMESTAMP", Collections.singletonList(1)),
        new CandidateTimestampFormat("dd/MMM/YYYY:HH:mm:ss Z", "\\b\\d{2}/[A-Z]\\S{2}/\\d{4}:\\d{2}:\\d{2}:\\d{2} ",
            "\\b%{MONTHDAY}/%{MONTH}/%{YEAR}:%{HOUR}:%{MINUTE}:(?:[0-5][0-9]|60) [+-]?%{HOUR}%{MINUTE}\\b", "HTTPDATE"),
        new CandidateTimestampFormat("MMM dd, YYYY K:mm:ss a", "\\b[A-Z]\\S{2,8} \\d{1,2}, \\d{4} \\d{1,2}:\\d{2}:\\d{2} [AP]M\\b",
            "%{MONTH} %{MONTHDAY}, 20\\d{2} %{HOUR}:%{MINUTE}:(?:[0-5][0-9]|60) (?:AM|PM)\\b", "CATALINA_DATESTAMP"),
        new CandidateTimestampFormat(Arrays.asList("MMM dd YYYY HH:mm:ss", "MMM  d YYYY HH:mm:ss"),
            "\\b[A-Z]\\S{2,8} {1,2}\\d{1,2} \\d{4} \\d{2}:\\d{2}:\\d{2}\\b",
            "%{MONTH} +%{MONTHDAY} %{YEAR} %{HOUR}:%{MINUTE}:(?:[0-5][0-9]|60)\\b", "CISCOTIMESTAMP", Collections.singletonList(1)),
        new CandidateTimestampFormat("UNIX_MS", "\\b\\d{13}\\b", "\\b\\d{13}\\b", "POSINT"),
        new CandidateTimestampFormat("UNIX", "\\b\\d{10}\\.\\d{3,9}\\b", "\\b\\d{10}\\.(?:\\d{3}){1,3}\\b", "NUMBER"),
        new CandidateTimestampFormat("UNIX", "\\b\\d{10}\\b", "\\b\\d{10}\\b", "POSINT"),
        new CandidateTimestampFormat("TAI64N", "\\b[0-9A-Fa-f]{24}\\b", "\\b[0-9A-Fa-f]{24}\\b", "BASE16NUM")
    );

    private TimestampFormatFinder() {
    }

    public static TimestampMatch findFirstMatch(String text) {
        return findFirstMatch(text, 0);
    }

    public static TimestampMatch findFirstMatch(String text, int ignoreCandidates) {
        Boolean[] quickRuleoutMatches = new Boolean[QUICK_RULE_OUT_PATTERNS.size()];
        int index = ignoreCandidates;
        for (CandidateTimestampFormat candidate : ORDERED_CANDIDATE_FORMATS.subList(ignoreCandidates, ORDERED_CANDIDATE_FORMATS.size())) {
            boolean quicklyRuledOut = false;
            for (Integer quickRuleOutIndex : candidate.quickRuleOutIndices) {
                if (quickRuleoutMatches[quickRuleOutIndex] == null) {
                    quickRuleoutMatches[quickRuleOutIndex] = QUICK_RULE_OUT_PATTERNS.get(quickRuleOutIndex).matcher(text).find();
                }
                if (quickRuleoutMatches[quickRuleOutIndex] == false) {
                    quicklyRuledOut = true;
                    break;
                }
            }
            if (quicklyRuledOut == false) {
                Map<String, Object> captures = candidate.strictSearchGrok.captures(text);
                if (captures != null) {
                    String preface = captures.getOrDefault(PREFACE, "").toString();
                    String epilogue = captures.getOrDefault(EPILOGUE, "").toString();
                    return makeTimestampMatch(candidate, index, preface, text.substring(preface.length(),
                        text.length() - epilogue.length()), epilogue);
                }
            }
            ++index;
        }
        return null;
    }

    public static TimestampMatch findFirstFullMatch(String text) {
        return findFirstFullMatch(text, 0);
    }

    public static TimestampMatch findFirstFullMatch(String text, int ignoreCandidates) {
        int index = ignoreCandidates;
        for (CandidateTimestampFormat candidate : ORDERED_CANDIDATE_FORMATS.subList(ignoreCandidates, ORDERED_CANDIDATE_FORMATS.size())) {
            Map<String, Object> captures = candidate.strictFullMatchGrok.captures(text);
            if (captures != null) {
                return makeTimestampMatch(candidate, index, "", text, "");
            }
            ++index;
        }
        return null;
    }

    private static TimestampMatch makeTimestampMatch(CandidateTimestampFormat chosenTimestampFormat, int chosenIndex,
                                                     String preface, String matchedDate, String epilogue) {
        Tuple<Character, Boolean> fractionalSecondsInterpretation = interpretFractionalSeconds(matchedDate);
        List<String> dateFormats = chosenTimestampFormat.dateFormats;
        Pattern simplePattern = chosenTimestampFormat.simplePattern;
        char separator = fractionalSecondsInterpretation.v1();
        if (separator != DEFAULT_FRACTIONAL_SECOND_SEPARATOR) {
            dateFormats = dateFormats.stream().map(dateFormat -> dateFormat.replace(DEFAULT_FRACTIONAL_SECOND_SEPARATOR, separator))
                .collect(Collectors.toList());
            if (dateFormats.stream().noneMatch(dateFormat -> dateFormat.startsWith("UNIX"))) {
                simplePattern = Pattern.compile(simplePattern.pattern().replace(String.valueOf(DEFAULT_FRACTIONAL_SECOND_SEPARATOR),
                    String.format(Locale.ROOT, "%s%c", (separator == '.') ? "\\" : "", separator)));
            }
        }
        return new TimestampMatch(chosenIndex, preface, dateFormats, simplePattern, chosenTimestampFormat.standardGrokPatternName, epilogue,
            fractionalSecondsInterpretation.v2());
    }

    static Tuple<Character, Boolean> interpretFractionalSeconds(String date) {

        Matcher matcher = FRACTIONAL_SECOND_INTERPRETER.matcher(date);
        if (matcher.find()) {
            return new Tuple<>(matcher.group(1).charAt(0), matcher.group(2).length() > 3);
        }

        return new Tuple<>(DEFAULT_FRACTIONAL_SECOND_SEPARATOR, false);
    }

    public static final class TimestampMatch {

        final int candidateIndex;
        final String preface;
        final List<String> dateFormats;
        final Pattern simplePattern;
        final String grokPatternName;
        final String epilogue;
        final boolean hasFractionalComponentSmallerThanMillisecond;

        TimestampMatch(int candidateIndex, String preface, String dateFormat, String simpleRegex, String grokPatternName, String epilogue) {
            this(candidateIndex, preface, Collections.singletonList(dateFormat), simpleRegex, grokPatternName, epilogue);
        }

        TimestampMatch(int candidateIndex, String preface, String dateFormat, String simpleRegex, String grokPatternName, String epilogue,
                       boolean hasFractionalComponentSmallerThanMillisecond) {
            this(candidateIndex, preface, Collections.singletonList(dateFormat), simpleRegex, grokPatternName, epilogue,
                hasFractionalComponentSmallerThanMillisecond);
        }

        TimestampMatch(int candidateIndex, String preface, List<String> dateFormats, String simpleRegex, String grokPatternName,
                       String epilogue) {
            this(candidateIndex, preface, dateFormats, Pattern.compile(simpleRegex), grokPatternName, epilogue, false);
        }

        TimestampMatch(int candidateIndex, String preface, List<String> dateFormats, String simpleRegex, String grokPatternName,
                       String epilogue, boolean hasFractionalComponentSmallerThanMillisecond) {
            this(candidateIndex, preface, dateFormats, Pattern.compile(simpleRegex), grokPatternName, epilogue,
                hasFractionalComponentSmallerThanMillisecond);
        }

        TimestampMatch(int candidateIndex, String preface, List<String> dateFormats, Pattern simplePattern, String grokPatternName,
                       String epilogue, boolean hasFractionalComponentSmallerThanMillisecond) {
            this.candidateIndex = candidateIndex;
            this.preface = preface;
            this.dateFormats = dateFormats;
            this.simplePattern = simplePattern;
            this.grokPatternName = grokPatternName;
            this.epilogue = epilogue;
            this.hasFractionalComponentSmallerThanMillisecond = hasFractionalComponentSmallerThanMillisecond;
        }

        @Override
        public int hashCode() {
            return Objects.hash(candidateIndex, preface, dateFormats, simplePattern.pattern(), grokPatternName, epilogue,
                hasFractionalComponentSmallerThanMillisecond);
        }

        @Override
        public boolean equals(Object other) {
            if (other == null) {
                return false;
            }
            if (getClass() != other.getClass()) {
                return false;
            }

            TimestampMatch that = (TimestampMatch) other;
            return this.candidateIndex == that.candidateIndex &&
                Objects.equals(this.preface, that.preface) &&
                Objects.equals(this.dateFormats, that.dateFormats) &&
                Objects.equals(this.simplePattern.pattern(), that.simplePattern.pattern()) &&
                Objects.equals(this.grokPatternName, that.grokPatternName) &&
                Objects.equals(this.epilogue, that.epilogue) &&
                this.hasFractionalComponentSmallerThanMillisecond == that.hasFractionalComponentSmallerThanMillisecond;
        }

        @Override
        public String toString() {
            return "index = " + candidateIndex + (preface.isEmpty() ? "" : ", preface = '" + preface + "'") +
                ", date formats = " + dateFormats.stream().collect(Collectors.joining("', '", "[ '", "' ]")) +
                ", simple pattern = '" + simplePattern.pattern() + "', grok pattern = '" + grokPatternName +
                (epilogue.isEmpty() ? "" : "', epilogue = '" + epilogue) +
                "', has fractional component smaller than millisecond = " + hasFractionalComponentSmallerThanMillisecond;
        }
    }

    static final class CandidateTimestampFormat {

        final List<String> dateFormats;
        final Pattern simplePattern;
        final Grok strictSearchGrok;
        final Grok strictFullMatchGrok;
        final String standardGrokPatternName;
        final List<Integer> quickRuleOutIndices;

        CandidateTimestampFormat(String dateFormat, String simpleRegex, String strictGrokPattern, String standardGrokPatternName) {
            this(Collections.singletonList(dateFormat), simpleRegex, strictGrokPattern, standardGrokPatternName);
        }

        CandidateTimestampFormat(String dateFormat, String simpleRegex, String strictGrokPattern, String standardGrokPatternName,
                                 List<Integer> quickRuleOutIndices) {
            this(Collections.singletonList(dateFormat), simpleRegex, strictGrokPattern, standardGrokPatternName, quickRuleOutIndices);
        }

        CandidateTimestampFormat(List<String> dateFormats, String simpleRegex, String strictGrokPattern, String standardGrokPatternName) {
            this(dateFormats, simpleRegex, strictGrokPattern, standardGrokPatternName, Collections.emptyList());
        }

        CandidateTimestampFormat(List<String> dateFormats, String simpleRegex, String strictGrokPattern, String standardGrokPatternName,
                                 List<Integer> quickRuleOutIndices) {
            this.dateFormats = dateFormats;
            this.simplePattern = Pattern.compile(simpleRegex, Pattern.MULTILINE);
            // The (?m) here has the Ruby meaning, which is equivalent to (?s) in Java
            this.strictSearchGrok = new Grok(Grok.getBuiltinPatterns(), "(?m)%{DATA:" + PREFACE + "}" + strictGrokPattern +
                "%{GREEDYDATA:" + EPILOGUE + "}");
            this.strictFullMatchGrok = new Grok(Grok.getBuiltinPatterns(), strictGrokPattern);
            this.standardGrokPatternName = standardGrokPatternName;
            assert quickRuleOutIndices.stream()
                .noneMatch(quickRuleOutIndex -> quickRuleOutIndex < 0 || quickRuleOutIndex >= QUICK_RULE_OUT_PATTERNS.size());
            this.quickRuleOutIndices = quickRuleOutIndices;
        }
    }
}
