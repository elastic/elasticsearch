/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.configcreator;

import org.elasticsearch.grok.Grok;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Pattern;

public final class TimestampFormatFinder {

    private static String PREFACE = "preface";
    private static String EPILOGUE = "epilogue";

    /**
     * The first match in this list will be chosen, so it needs to be ordered
     * such that more generic patterns come after more specific patterns.
     */
    static final List<CandidateTimestampFormat> ORDERED_CANDIDATE_FORMATS = Arrays.asList(
        // The TOMCAT_DATESTAMP format has to come before ISO8601 because it's basically ISO8601 but
        // with a space before the timezone, and because the timezone is optional in ISO8601 it will
        // be recognised as that with the timezone missed off if ISO8601 is checked first
        new CandidateTimestampFormat("YYYY-MM-dd HH:mm:ss,SSS Z", "\\b\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2},\\d{3} ",
            "\\b20\\d{2}-%{MONTHNUM}-%{MONTHDAY} %{HOUR}:?%{MINUTE}:(?:[0-5][0-9]|60),[0-9]{3} (?:Z|[+-]%{HOUR}%{MINUTE})\\b",
            "TOMCAT_DATESTAMP"),
        // The Elasticsearch ISO8601 parser requires a literal T between the date and time, so
        // longhand formats are needed if there's a space instead
        new CandidateTimestampFormat("YYYY-MM-dd HH:mm:ss,SSSZ", "\\b\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2},\\d{3}",
            "\\b%{YEAR}-%{MONTHNUM}-%{MONTHDAY} %{HOUR}:?%{MINUTE}:(?:[0-5][0-9]|60),[0-9]{3}(?:Z|[+-]%{HOUR}%{MINUTE})\\b",
            "TIMESTAMP_ISO8601"),
        new CandidateTimestampFormat("YYYY-MM-dd HH:mm:ss,SSSZZ", "\\b\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2},\\d{3}",
            "\\b%{YEAR}-%{MONTHNUM}-%{MONTHDAY} %{HOUR}:?%{MINUTE}:(?:[0-5][0-9]|60),[0-9]{3}[+-]%{HOUR}:%{MINUTE}\\b",
            "TIMESTAMP_ISO8601"),
        new CandidateTimestampFormat("YYYY-MM-dd HH:mm:ss,SSS", "\\b\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2},\\d{3}",
            "\\b%{YEAR}-%{MONTHNUM}-%{MONTHDAY} %{HOUR}:?%{MINUTE}:(?:[0-5][0-9]|60),[0-9]{3}\\b", "TIMESTAMP_ISO8601"),
        new CandidateTimestampFormat("YYYY-MM-dd HH:mm:ssZ", "\\b\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}",
            "\\b%{YEAR}-%{MONTHNUM}-%{MONTHDAY} %{HOUR}:?%{MINUTE}:(?:[0-5][0-9]|60)(?:Z|[+-]%{HOUR}%{MINUTE})\\b", "TIMESTAMP_ISO8601"),
        new CandidateTimestampFormat("YYYY-MM-dd HH:mm:ssZZ", "\\b\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}",
            "\\b%{YEAR}-%{MONTHNUM}-%{MONTHDAY} %{HOUR}:?%{MINUTE}:(?:[0-5][0-9]|60)[+-]%{HOUR}:%{MINUTE}\\b", "TIMESTAMP_ISO8601"),
        new CandidateTimestampFormat("YYYY-MM-dd HH:mm:ss", "\\b\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}",
            "\\b%{YEAR}-%{MONTHNUM}-%{MONTHDAY} %{HOUR}:?%{MINUTE}:(?:[0-5][0-9]|60)\\b", "TIMESTAMP_ISO8601"),
        new CandidateTimestampFormat("ISO8601", "\\b\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}", "\\b%{TIMESTAMP_ISO8601}\\b",
            "TIMESTAMP_ISO8601"),
        new CandidateTimestampFormat("EEE MMM dd YYYY HH:mm:ss zzz", "\\b[A-Z]\\S{2,8} [A-Z]\\S{2,8} \\d{1,2} \\d{4} \\d{2}:\\d{2}:\\d{2} ",
            "\\b%{DAY} %{MONTH} %{MONTHDAY} %{YEAR} %{HOUR}:%{MINUTE}:(?:[0-5][0-9]|60) %{TZ}\\b", "DATESTAMP_RFC822"),
        new CandidateTimestampFormat("EEE MMM dd YYYY HH:mm zzz", "\\b[A-Z]\\S{2,8} [A-Z]\\S{2,8} \\d{1,2} \\d{4} \\d{2}:\\d{2} ",
            "\\b%{DAY} %{MONTH} %{MONTHDAY} %{YEAR} %{HOUR}:%{MINUTE} %{TZ}\\b", "DATESTAMP_RFC822"),
        new CandidateTimestampFormat("EEE, dd MMM YYYY HH:mm:ss ZZ",
            "\\b[A-Z]\\S{2,8}, \\d{1,2} [A-Z]\\S{2,8} \\d{4} \\d{2}:\\d{2}:\\d{2} ",
            "\\b%{DAY}, %{MONTHDAY} %{MONTH} %{YEAR} %{HOUR}:%{MINUTE}:(?:[0-5][0-9]|60) (?:Z|[+-]%{HOUR}:%{MINUTE})\\b",
            "DATESTAMP_RFC2822"),
        new CandidateTimestampFormat("EEE, dd MMM YYYY HH:mm:ss Z", "\\b[A-Z]\\S{2,8}, \\d{1,2} [A-Z]\\S{2,8} \\d{4} \\d{2}:\\d{2}:\\d{2} ",
            "\\b%{DAY}, %{MONTHDAY} %{MONTH} %{YEAR} %{HOUR}:%{MINUTE}:(?:[0-5][0-9]|60) (?:Z|[+-]%{HOUR}%{MINUTE})\\b",
            "DATESTAMP_RFC2822"),
        new CandidateTimestampFormat("EEE, dd MMM YYYY HH:mm ZZ", "\\b[A-Z]\\S{2,8}, \\d{1,2} [A-Z]\\S{2,8} \\d{4} \\d{2}:\\d{2} ",
            "\\b%{DAY}, %{MONTHDAY} %{MONTH} %{YEAR} %{HOUR}:%{MINUTE} (?:Z|[+-]%{HOUR}:%{MINUTE})\\b", "DATESTAMP_RFC2822"),
        new CandidateTimestampFormat("EEE, dd MMM YYYY HH:mm Z", "\\b[A-Z]\\S{2,8}, \\d{1,2} [A-Z]\\S{2,8} \\d{4} \\d{2}:\\d{2} ",
            "\\b%{DAY}, %{MONTHDAY} %{MONTH} %{YEAR} %{HOUR}:%{MINUTE} (?:Z|[+-]%{HOUR}%{MINUTE})\\b", "DATESTAMP_RFC2822"),
        new CandidateTimestampFormat("EEE MMM dd HH:mm:ss zzz YYYY",
            "\\b[A-Z]\\S{2,8} [A-Z]\\S{2,8} \\d{1,2} \\d{2}:\\d{2}:\\d{2} [A-Z]{3,4} \\d{4}\\b",
            "\\b%{DAY} %{MONTH} %{MONTHDAY} %{HOUR}:%{MINUTE}:(?:[0-5][0-9]|60) %{TZ} %{YEAR}\\b", "DATESTAMP_OTHER"),
        new CandidateTimestampFormat("EEE MMM dd HH:mm zzz YYYY",
            "\\b[A-Z]\\S{2,8} [A-Z]\\S{2,8} \\d{1,2} \\d{2}:\\d{2} [A-Z]{3,4} \\d{4}\\b",
            "\\b%{DAY} %{MONTH} %{MONTHDAY} %{HOUR}:%{MINUTE} %{TZ} %{YEAR}\\b", "DATESTAMP_OTHER"),
        new CandidateTimestampFormat("YYYYMMddHHmmss", "\\b\\d{14}\\b",
            "\\b20\\d{2}%{MONTHNUM2}(?:(?:0[1-9])|(?:[12][0-9])|(?:3[01]))(?:2[0123]|[01][0-9])%{MINUTE}(?:[0-5][0-9]|60)\\b",
            "DATESTAMP_EVENTLOG"),
        new CandidateTimestampFormat("EEE MMM dd HH:mm:ss YYYY", "\\b[A-Z]\\S{2,8} [A-Z]\\S{2,8} \\d{1,2} \\d{2}:\\d{2}:\\d{2} \\d{4}\\b",
            "\\b%{DAY} %{MONTH} %{MONTHDAY} %{HOUR}:%{MINUTE}:(?:[0-5][0-9]|60) %{YEAR}\\b", "HTTPDERROR_DATE"),
        new CandidateTimestampFormat("MMM dd HH:mm:ss", "\\b[A-Z]\\S{2,8} \\d{1,2} \\d{2}:\\d{2}:\\d{2}\\b",
            "%{MONTH} +%{MONTHDAY} %{HOUR}:%{MINUTE}:(?:[0-5][0-9]|60)\\b", "SYSLOGTIMESTAMP"),
        new CandidateTimestampFormat("dd/MMM/YYYY:HH:mm:ss Z", "\\b\\d{2}/[A-Z]\\S{2}/\\d{4}:\\d{2}:\\d{2}:\\d{2} ",
            "\\b%{MONTHDAY}/%{MONTH}/%{YEAR}:%{HOUR}:%{MINUTE}:(?:[0-5][0-9]|60) [+-]?%{HOUR}%{MINUTE}\\b", "HTTPDATE"),
        new CandidateTimestampFormat("MMM dd, YYYY K:mm:ss a", "\\b[A-Z]\\S{2,8} \\d{1,2}, \\d{4} \\d{1,2}:\\d{2}:\\d{2} [AP]M\\b",
            "%{MONTH} %{MONTHDAY}, 20\\d{2} %{HOUR}:%{MINUTE}:(?:[0-5][0-9]|60) (?:AM|PM)\\b", "CATALINA_DATESTAMP"),
        new CandidateTimestampFormat("MMM dd YYYY HH:mm:ss", "\\b[A-Z]\\S{2,8} \\d{1,2} \\d{4} \\d{2}:\\d{2}:\\d{2}\\b",
            "%{MONTH} +%{MONTHDAY} %{YEAR} %{HOUR}:%{MINUTE}:(?:[0-5][0-9]|60)\\b", "CISCOTIMESTAMP"),
        new CandidateTimestampFormat("UNIX_MS", "\\b\\d{13}\\b", "\\b\\d{13}\\b", "POSINT"),
        new CandidateTimestampFormat("UNIX", "\\b\\d{10}\\b", "\\b\\d{10}\\b", "POSINT"),
        new CandidateTimestampFormat("TAI64N", "\\b[0-9A-Fa-f]{24}\\b", "\\b[0-9A-Fa-f]{24}\\b", "BASE16NUM")
    );

    private TimestampFormatFinder() {
    }

    public static TimestampMatch findFirstMatch(String text) {
        return findFirstMatch(text, 0);
    }

    public static TimestampMatch findFirstMatch(String text, int ignoreCandidates) {
        int index = ignoreCandidates;
        for (CandidateTimestampFormat candidate : ORDERED_CANDIDATE_FORMATS.subList(ignoreCandidates, ORDERED_CANDIDATE_FORMATS.size())) {
            Map<String, Object> captures = candidate.strictSearchGrok.captures(text);
            if (captures != null) {
                return new TimestampMatch(index, captures.getOrDefault(PREFACE, "").toString(), candidate.dateFormat,
                    candidate.simplePattern, candidate.standardGrokPatternName, captures.getOrDefault(EPILOGUE, "").toString());
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
                return new TimestampMatch(index, "", candidate.dateFormat, candidate.simplePattern, candidate.standardGrokPatternName, "");
            }
            ++index;
        }
        return null;
    }

    public static final class TimestampMatch {

        final int candidateIndex;
        final String preface;
        final String dateFormat;
        final Pattern simplePattern;
        final String grokPatternName;
        final String epilogue;

        TimestampMatch(int candidateIndex, String preface, String dateFormat, String simpleRegex, String grokPatternName, String epilogue) {
            this(candidateIndex, preface, dateFormat, Pattern.compile(simpleRegex), grokPatternName, epilogue);
        }

        TimestampMatch(int candidateIndex, String preface, String dateFormat, Pattern simplePattern, String grokPatternName,
                       String epilogue) {
            this.candidateIndex = candidateIndex;
            this.preface = preface;
            this.dateFormat = dateFormat;
            this.simplePattern = simplePattern;
            this.grokPatternName = grokPatternName;
            this.epilogue = epilogue;
        }

        @Override
        public int hashCode() {
            return Objects.hash(candidateIndex, preface, dateFormat, simplePattern.pattern(), grokPatternName, epilogue);
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
                Objects.equals(this.dateFormat, that.dateFormat) &&
                Objects.equals(this.simplePattern.pattern(), that.simplePattern.pattern()) &&
                Objects.equals(this.grokPatternName, that.grokPatternName) &&
                Objects.equals(this.epilogue, that.epilogue);
        }

        @Override
        public String toString() {
            return "index = " + candidateIndex + ", preface = '" + preface + "', format = '" + dateFormat + "', simple pattern = '" +
                simplePattern.pattern() + "', grok pattern = '" + grokPatternName + "', epilogue = '" + epilogue + "'";
        }
    }

    static final class CandidateTimestampFormat {

        final String dateFormat;
        final Pattern simplePattern;
        final Grok strictSearchGrok;
        final Grok strictFullMatchGrok;
        final String standardGrokPatternName;

        CandidateTimestampFormat(String dateFormat, String simpleRegex, String strictGrokPattern, String standardGrokPatternName) {
            this.dateFormat = dateFormat;
            this.simplePattern = Pattern.compile(simpleRegex, Pattern.MULTILINE);
            this.strictSearchGrok = new Grok(Grok.getBuiltinPatterns(), "%{DATA:" + PREFACE + "}" + strictGrokPattern +
                "%{GREEDYDATA:" + EPILOGUE + "}");
            this.strictFullMatchGrok = new Grok(Grok.getBuiltinPatterns(), strictGrokPattern);
            this.standardGrokPatternName = standardGrokPatternName;
        }
    }
}
