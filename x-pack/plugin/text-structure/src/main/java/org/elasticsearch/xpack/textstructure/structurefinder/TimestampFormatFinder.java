/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.textstructure.structurefinder;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.grok.Grok;

import java.time.DateTimeException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.FormatStyle;
import java.time.format.ResolverStyle;
import java.time.temporal.ChronoField;
import java.time.temporal.TemporalAccessor;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Used to find the best timestamp format for one of the following situations:
 * 1. Matching an entire field value
 * 2. Matching a timestamp found somewhere within a message
 *
 * This class is <em>not</em> thread safe.  Each object of this class should only be used from within a single thread.
 */
public final class TimestampFormatFinder {

    private static final String PREFACE = "preface";
    private static final String EPILOGUE = "epilogue";

    private static final Logger logger = LogManager.getLogger(TimestampFormatFinder.class);
    private static final String PUNCTUATION_THAT_NEEDS_ESCAPING_IN_REGEX = "\\|()[]{}^$.*?";
    private static final String FRACTIONAL_SECOND_SEPARATORS = ":.,";
    private static final Pattern FRACTIONAL_SECOND_INTERPRETER = Pattern.compile(
        "([" + FRACTIONAL_SECOND_SEPARATORS + "])(\\d{3,9})($|[Z+-])"
    );
    private static final char INDETERMINATE_FIELD_PLACEHOLDER = '?';
    // The ? characters in this must match INDETERMINATE_FIELD_PLACEHOLDER
    // above, but they're literals in this regex to aid readability
    private static final Pattern INDETERMINATE_FORMAT_INTERPRETER = Pattern.compile("([^?]*)(\\?{1,2})(?:([^?]*)(\\?{1,2})([^?]*))?");

    /**
     * These are the date format letter groups that are supported in custom formats
     *
     * (Note: Fractional seconds is a special case as they have to follow seconds.)
     */
    private static final Map<String, Tuple<String, String>> VALID_LETTER_GROUPS;
    static {
        Map<String, Tuple<String, String>> validLetterGroups = new HashMap<>();
        validLetterGroups.put("yyyy", new Tuple<>("%{YEAR}", "\\d{4}"));
        validLetterGroups.put("yy", new Tuple<>("%{YEAR}", "\\d{2}"));
        validLetterGroups.put("M", new Tuple<>("%{MONTHNUM}", "\\d{1,2}"));
        validLetterGroups.put("MM", new Tuple<>("%{MONTHNUM2}", "\\d{2}"));
        // The simple regex here is based on the fact that the %{MONTH} Grok pattern only matches English and German month names
        validLetterGroups.put("MMM", new Tuple<>("%{MONTH}", "[A-Z]\\S{2}"));
        validLetterGroups.put("MMMM", new Tuple<>("%{MONTH}", "[A-Z]\\S{2,8}"));
        validLetterGroups.put("d", new Tuple<>("%{MONTHDAY}", "\\d{1,2}"));
        validLetterGroups.put("dd", new Tuple<>("%{MONTHDAY}", "\\d{2}"));
        // The simple regex here is based on the fact that the %{DAY} Grok pattern only matches English and German day names
        validLetterGroups.put("EEE", new Tuple<>("%{DAY}", "[A-Z]\\S{2}"));
        validLetterGroups.put("EEEE", new Tuple<>("%{DAY}", "[A-Z]\\S{2,8}"));
        validLetterGroups.put("H", new Tuple<>("%{HOUR}", "\\d{1,2}"));
        validLetterGroups.put("HH", new Tuple<>("%{HOUR}", "\\d{2}"));
        validLetterGroups.put("h", new Tuple<>("%{HOUR}", "\\d{1,2}"));
        validLetterGroups.put("mm", new Tuple<>("%{MINUTE}", "\\d{2}"));
        validLetterGroups.put("ss", new Tuple<>("%{SECOND}", "\\d{2}"));
        validLetterGroups.put("a", new Tuple<>("(?:AM|PM)", "[AP]M"));
        validLetterGroups.put("XX", new Tuple<>("%{ISO8601_TIMEZONE}", "(?:Z|[+-]\\d{4})"));
        validLetterGroups.put("XXX", new Tuple<>("%{ISO8601_TIMEZONE}", "(?:Z|[+-]\\d{2}:\\d{2})"));
        validLetterGroups.put("zzz", new Tuple<>("%{TZ}", "[A-Z]{3}"));
        VALID_LETTER_GROUPS = Collections.unmodifiableMap(validLetterGroups);
    }

    static final String CUSTOM_TIMESTAMP_GROK_NAME = "CUSTOM_TIMESTAMP";

    /**
     * Candidates for the special format strings (ISO8601, UNIX_MS, UNIX and TAI64N)
     */
    static final CandidateTimestampFormat ISO8601_CANDIDATE_FORMAT = new CandidateTimestampFormat(
        CandidateTimestampFormat::iso8601FormatFromExample,
        "\\b\\d{4}-\\d{2}-\\d{2}[T ]\\d{2}:\\d{2}",
        "\\b%{TIMESTAMP_ISO8601}\\b",
        "TIMESTAMP_ISO8601",
        "1111 11 11 11 11",
        0,
        19
    );
    static final CandidateTimestampFormat UNIX_MS_CANDIDATE_FORMAT = new CandidateTimestampFormat(
        example -> Collections.singletonList("UNIX_MS"),
        "\\b\\d{13}\\b",
        "\\b[12]\\d{12}\\b",
        "POSINT",
        "1111111111111",
        0,
        0
    );
    static final CandidateTimestampFormat UNIX_CANDIDATE_FORMAT = new CandidateTimestampFormat(
        example -> Collections.singletonList("UNIX"),
        "\\b\\d{10}\\b",
        "\\b[12]\\d{9}(?:\\.\\d{3,9})?\\b",
        "NUMBER",
        "1111111111",
        0,
        10
    );
    static final CandidateTimestampFormat TAI64N_CANDIDATE_FORMAT = new CandidateTimestampFormat(
        example -> Collections.singletonList("TAI64N"),
        "\\b[0-9A-Fa-f]{24}\\b",
        "\\b[0-9A-Fa-f]{24}\\b",
        "BASE16NUM"
    );

    /**
     * The first match in this list will be chosen, so it needs to be ordered
     * such that more generic patterns come after more specific patterns.
     */
    static final List<CandidateTimestampFormat> ORDERED_CANDIDATE_FORMATS = Arrays.asList(
        // The TOMCAT_DATESTAMP format has to come before ISO8601 because it's basically ISO8601 but
        // with a space before the timezone, and because the timezone is optional in ISO8601 it will
        // be recognised as that with the timezone missed off if ISO8601 is checked first
        new CandidateTimestampFormat(
            example -> CandidateTimestampFormat.iso8601LikeFormatFromExample(example, " ", " "),
            "\\b\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}[:.,]\\d{3}",
            "\\b20\\d{2}-%{MONTHNUM}-%{MONTHDAY} %{HOUR}:?%{MINUTE}:(?:[0-5][0-9]|60)[:.,][0-9]{3,9} (?:Z|[+-]%{HOUR}%{MINUTE})\\b",
            "TOMCAT_DATESTAMP",
            "1111 11 11 11 11 11 111",
            0,
            13
        ),
        ISO8601_CANDIDATE_FORMAT,
        new CandidateTimestampFormat(
            example -> Arrays.asList("EEE MMM dd yy HH:mm:ss zzz", "EEE MMM d yy HH:mm:ss zzz"),
            "\\b[A-Z]\\S{2} [A-Z]\\S{2} \\d{1,2} \\d{2} \\d{2}:\\d{2}:\\d{2}\\b",
            "\\b%{DAY} %{MONTH} %{MONTHDAY} %{YEAR} %{HOUR}:%{MINUTE}(?::(?:[0-5][0-9]|60)) %{TZ}\\b",
            "DATESTAMP_RFC822",
            Arrays.asList("        11 11 11 11 11", "        1 11 11 11 11"),
            0,
            5
        ),
        new CandidateTimestampFormat(
            example -> CandidateTimestampFormat.adjustTrailingTimezoneFromExample(example, "EEE, dd MMM yyyy HH:mm:ss XX"),
            "\\b[A-Z]\\S{2}, \\d{1,2} [A-Z]\\S{2} \\d{4} \\d{2}:\\d{2}:\\d{2}\\b",
            "\\b%{DAY}, %{MONTHDAY} %{MONTH} %{YEAR} %{HOUR}:%{MINUTE}(?::(?:[0-5][0-9]|60)) (?:Z|[+-]%{HOUR}:?%{MINUTE})\\b",
            "DATESTAMP_RFC2822",
            Arrays.asList("     11     1111 11 11 11", "     1     1111 11 11 11"),
            0,
            7
        ),
        new CandidateTimestampFormat(
            example -> Arrays.asList("EEE MMM dd HH:mm:ss zzz yyyy", "EEE MMM d HH:mm:ss zzz yyyy"),
            "\\b[A-Z]\\S{2,8} [A-Z]\\S{2,8} \\d{1,2} \\d{2}:\\d{2}:\\d{2}\\b",
            "\\b%{DAY} %{MONTH} %{MONTHDAY} %{HOUR}:%{MINUTE}(?::(?:[0-5][0-9]|60)) %{TZ} %{YEAR}\\b",
            "DATESTAMP_OTHER",
            Arrays.asList("        11 11 11 11", "        1 11 11 11"),
            12,
            10
        ),
        new CandidateTimestampFormat(
            example -> Collections.singletonList("yyyyMMddHHmmss"),
            "\\b\\d{14}\\b",
            "\\b20\\d{2}%{MONTHNUM2}(?:(?:0[1-9])|(?:[12][0-9])|(?:3[01]))(?:2[0123]|[01][0-9])%{MINUTE}(?:[0-5][0-9]|60)\\b",
            "DATESTAMP_EVENTLOG",
            "11111111111111",
            0,
            0
        ),
        new CandidateTimestampFormat(
            example -> Collections.singletonList("EEE MMM dd HH:mm:ss yyyy"),
            "\\b[A-Z]\\S{2} [A-Z]\\S{2} \\d{2} \\d{2}:\\d{2}:\\d{2} \\d{4}\\b",
            "\\b%{DAY} %{MONTH} %{MONTHDAY} %{HOUR}:%{MINUTE}:(?:[0-5][0-9]|60) %{YEAR}\\b",
            "HTTPDERROR_DATE",
            "        11 11 11 11 1111",
            0,
            0
        ),
        new CandidateTimestampFormat(
            example -> CandidateTimestampFormat.expandDayAndAdjustFractionalSecondsFromExample(example, "MMM dd HH:mm:ss"),
            "\\b[A-Z]\\S{2,8} {1,2}\\d{1,2} \\d{2}:\\d{2}:\\d{2}\\b",
            "%{MONTH} +%{MONTHDAY} %{HOUR}:%{MINUTE}:(?:[0-5][0-9]|60)(?:[:.,][0-9]{3,9})?\\b",
            "SYSLOGTIMESTAMP",
            Arrays.asList("    11 11 11 11", "    1 11 11 11"),
            6,
            10
        ),
        new CandidateTimestampFormat(
            example -> Collections.singletonList("dd/MMM/yyyy:HH:mm:ss XX"),
            "\\b\\d{2}/[A-Z]\\S{2}/\\d{4}:\\d{2}:\\d{2}:\\d{2} ",
            "\\b%{MONTHDAY}/%{MONTH}/%{YEAR}:%{HOUR}:%{MINUTE}:(?:[0-5][0-9]|60) [+-]?%{HOUR}%{MINUTE}\\b",
            "HTTPDATE",
            "11     1111 11 11 11",
            0,
            6
        ),
        new CandidateTimestampFormat(
            example -> Collections.singletonList("MMM dd, yyyy h:mm:ss a"),
            "\\b[A-Z]\\S{2} \\d{2}, \\d{4} \\d{1,2}:\\d{2}:\\d{2} [AP]M\\b",
            "%{MONTH} %{MONTHDAY}, 20\\d{2} %{HOUR}:%{MINUTE}:(?:[0-5][0-9]|60) (?:AM|PM)\\b",
            "CATALINA_DATESTAMP",
            Arrays.asList("    11  1111 1 11 11", "    11  1111 11 11 11"),
            0,
            3
        ),
        new CandidateTimestampFormat(
            example -> Arrays.asList("MMM dd yyyy HH:mm:ss", "MMM  d yyyy HH:mm:ss", "MMM d yyyy HH:mm:ss"),
            "\\b[A-Z]\\S{2} {1,2}\\d{1,2} \\d{4} \\d{2}:\\d{2}:\\d{2}\\b",
            "%{MONTH} +%{MONTHDAY} %{YEAR} %{HOUR}:%{MINUTE}:(?:[0-5][0-9]|60)\\b",
            "CISCOTIMESTAMP",
            Arrays.asList("    11 1111 11 11 11", "    1 1111 11 11 11"),
            1,
            0
        ),
        new CandidateTimestampFormat(
            CandidateTimestampFormat::indeterminateDayMonthFormatFromExample,
            "\\b\\d{1,2}[/.-]\\d{1,2}[/.-](?:\\d{2}){1,2}[- ]\\d{2}:\\d{2}:\\d{2}\\b",
            "\\b%{DATESTAMP}\\b",
            "DATESTAMP",
            // In DATESTAMP the month may be 1 or 2 digits, the year 2 or 4, but the day must be 2
            // Also note the Grok pattern search space is set to start one character before a quick rule-out
            // match because we don't want 11 11 11 matching into 1111 11 11 with this pattern
            Arrays.asList(
                "11 11 1111 11 11 11",
                "1 11 1111 11 11 11",
                "11 1 1111 11 11 11",
                "11 11 11 11 11 11",
                "1 11 11 11 11 11",
                "11 1 11 11 11 11"
            ),
            1,
            10
        ),
        new CandidateTimestampFormat(
            CandidateTimestampFormat::indeterminateDayMonthFormatFromExample,
            "\\b\\d{1,2}[/.-]\\d{1,2}[/.-](?:\\d{2}){1,2}\\b",
            "\\b%{DATE}\\b",
            "DATE",
            // In DATE the month may be 1 or 2 digits, the year 2 or 4, but the day must be 2
            // Also note the Grok pattern search space is set to start one character before a quick rule-out
            // match because we don't want 11 11 11 matching into 1111 11 11 with this pattern
            Arrays.asList("11 11 1111", "11 1 1111", "1 11 1111", "11 11 11", "11 1 11", "1 11 11"),
            1,
            0
        ),
        UNIX_MS_CANDIDATE_FORMAT,
        UNIX_CANDIDATE_FORMAT,
        TAI64N_CANDIDATE_FORMAT,
        // This one is an ISO8601 date with no time, but the TIMESTAMP_ISO8601 Grok pattern doesn't cover it
        new CandidateTimestampFormat(
            example -> Collections.singletonList("ISO8601"),
            "\\b\\d{4}-\\d{2}-\\d{2}\\b",
            "\\b%{YEAR}-%{MONTHNUM2}-%{MONTHDAY}\\b",
            CUSTOM_TIMESTAMP_GROK_NAME,
            "1111 11 11",
            0,
            0
        ),
        // The Kibana export format
        new CandidateTimestampFormat(
            example -> Collections.singletonList("MMM dd, yyyy @ HH:mm:ss.SSS"),
            "\\b[A-Z]\\S{2} \\d{2}, \\d{4} @ \\d{2}:\\d{2}:\\d{2}\\.\\d{3}\\b",
            "\\b%{MONTH} %{MONTHDAY}, %{YEAR} @ %{HOUR}:%{MINUTE}:%{SECOND}\\b",
            CUSTOM_TIMESTAMP_GROK_NAME,
            "    11  1111   11 11 11 111",
            0,
            0
        )
    );

    /**
     * It is expected that the explanation will be shared with other code.
     * Both this class and other classes will update it.
     */
    private final List<String> explanation;
    private final boolean requireFullMatch;
    private final boolean errorOnNoTimestamp;
    private final boolean errorOnMultiplePatterns;
    private final List<CandidateTimestampFormat> orderedCandidateFormats;
    private final TimeoutChecker timeoutChecker;
    private final List<TimestampMatch> matches;
    // These two are not volatile because the class is explicitly not for use from multiple threads.
    // But if it ever were to be made thread safe, making these volatile would be one required step.
    private List<TimestampFormat> matchedFormats;
    private List<String> cachedJavaTimestampFormats;

    /**
     * Construct without any specific timestamp format override.
     * @param explanation             List of reasons for making decisions.  May contain items when passed and new reasons
     *                                can be appended by the methods of this class.
     * @param requireFullMatch        Must samples added to this object represent a timestamp in their entirety?
     * @param errorOnNoTimestamp      Should an exception be thrown if a sample is added that does not contain a recognised timestamp?
     * @param errorOnMultiplePatterns Should an exception be thrown if samples are uploaded that require different Grok patterns?
     * @param timeoutChecker          Will abort the operation if its timeout is exceeded.
     */
    public TimestampFormatFinder(
        List<String> explanation,
        boolean requireFullMatch,
        boolean errorOnNoTimestamp,
        boolean errorOnMultiplePatterns,
        TimeoutChecker timeoutChecker
    ) {
        this(explanation, null, requireFullMatch, errorOnNoTimestamp, errorOnMultiplePatterns, timeoutChecker);
    }

    /**
     * Construct with a timestamp format override.
     * @param explanation             List of reasons for making decisions.  May contain items when passed and new reasons
     *                                can be appended by the methods of this class.
     * @param overrideFormat          A timestamp format that will take precedence when looking for timestamps.  If <code>null</code>
     *                                then the effect is to have no such override, i.e. equivalent to calling the other constructor.
     *                                Timestamps will also be matched that have slightly different formats, but match the same Grok
     *                                pattern as is implied by the override format.
     * @param requireFullMatch        Must samples added to this object represent a timestamp in their entirety?
     * @param errorOnNoTimestamp      Should an exception be thrown if a sample is added that does not contain a recognised timestamp?
     * @param errorOnMultiplePatterns Should an exception be thrown if samples are uploaded that require different Grok patterns?
     * @param timeoutChecker          Will abort the operation if its timeout is exceeded.
     */
    public TimestampFormatFinder(
        List<String> explanation,
        @Nullable String overrideFormat,
        boolean requireFullMatch,
        boolean errorOnNoTimestamp,
        boolean errorOnMultiplePatterns,
        TimeoutChecker timeoutChecker
    ) {
        this.explanation = Objects.requireNonNull(explanation);
        this.requireFullMatch = requireFullMatch;
        this.errorOnNoTimestamp = errorOnNoTimestamp;
        this.errorOnMultiplePatterns = errorOnMultiplePatterns;
        this.orderedCandidateFormats = (overrideFormat != null)
            ? Collections.singletonList(makeCandidateFromOverrideFormat(overrideFormat, timeoutChecker))
            : ORDERED_CANDIDATE_FORMATS;
        this.timeoutChecker = Objects.requireNonNull(timeoutChecker);
        this.matches = new ArrayList<>();
        this.matchedFormats = new ArrayList<>();
    }

    /**
     * Convert a user supplied Java timestamp format to a Grok pattern and simple regular expression.
     * @param overrideFormat A user supplied Java timestamp format.
     * @return A tuple where the first value is a Grok pattern and the second is a simple regex.
     */
    static Tuple<String, String> overrideFormatToGrokAndRegex(String overrideFormat) {

        if (overrideFormat.indexOf('\n') >= 0 || overrideFormat.indexOf('\r') >= 0) {
            throw new IllegalArgumentException("Multi-line timestamp formats [" + overrideFormat + "] not supported");
        }

        if (overrideFormat.indexOf(INDETERMINATE_FIELD_PLACEHOLDER) >= 0) {
            throw new IllegalArgumentException(
                "Timestamp format [" + overrideFormat + "] not supported because it contains [" + INDETERMINATE_FIELD_PLACEHOLDER + "]"
            );
        }

        StringBuilder grokPatternBuilder = new StringBuilder();
        StringBuilder regexBuilder = new StringBuilder();

        boolean notQuoted = true;
        char prevChar = '\0';
        String prevLetterGroup = null;
        int pos = 0;
        while (pos < overrideFormat.length()) {
            char curChar = overrideFormat.charAt(pos);

            if (curChar == '\'') {
                notQuoted = notQuoted == false;
            } else if (notQuoted && Character.isLetter(curChar)) {
                int startPos = pos;
                int endPos = startPos + 1;
                while (endPos < overrideFormat.length() && overrideFormat.charAt(endPos) == curChar) {
                    ++endPos;
                    ++pos;
                }
                String letterGroup = overrideFormat.substring(startPos, endPos);
                Tuple<String, String> grokPatternAndRegexForGroup = VALID_LETTER_GROUPS.get(letterGroup);
                if (grokPatternAndRegexForGroup == null) {
                    // Special case of fractional seconds
                    if (curChar != 'S'
                        || FRACTIONAL_SECOND_SEPARATORS.indexOf(prevChar) == -1
                        || "ss".equals(prevLetterGroup) == false
                        || endPos - startPos > 9) {
                        String msg = "Letter group [" + letterGroup + "] in [" + overrideFormat + "] is not supported";
                        if (curChar == 'S') {
                            msg += " because it is not preceded by [ss] and a separator from [" + FRACTIONAL_SECOND_SEPARATORS + "]";
                        }
                        throw new IllegalArgumentException(msg);
                    }
                    // No need to append to the Grok pattern as %{SECOND} already allows for an optional fraction,
                    // but we need to remove the separator that's included in %{SECOND} (and that might be escaped)
                    int numCharsToDelete = (PUNCTUATION_THAT_NEEDS_ESCAPING_IN_REGEX.indexOf(prevChar) >= 0) ? 2 : 1;
                    grokPatternBuilder.delete(grokPatternBuilder.length() - numCharsToDelete, grokPatternBuilder.length());
                    regexBuilder.append("\\d{").append(endPos - startPos).append('}');
                } else {
                    grokPatternBuilder.append(grokPatternAndRegexForGroup.v1());
                    if (regexBuilder.length() == 0) {
                        regexBuilder.append("\\b");
                    }
                    regexBuilder.append(grokPatternAndRegexForGroup.v2());
                }
                if (pos + 1 == overrideFormat.length()) {
                    regexBuilder.append("\\b");
                }
                prevLetterGroup = letterGroup;
            } else {
                if (PUNCTUATION_THAT_NEEDS_ESCAPING_IN_REGEX.indexOf(curChar) >= 0) {
                    grokPatternBuilder.append('\\');
                    regexBuilder.append('\\');
                }
                grokPatternBuilder.append(curChar);
                regexBuilder.append(curChar);
            }

            prevChar = curChar;
            ++pos;
        }

        if (prevLetterGroup == null) {
            throw new IllegalArgumentException("No time format letter groups in override format [" + overrideFormat + "]");
        }

        return new Tuple<>(grokPatternBuilder.toString(), regexBuilder.toString());
    }

    /**
     * Given a user supplied Java timestamp format, return an appropriate candidate timestamp object as required by this class.
     * The returned candidate might be a built-in one, or might be generated from the supplied format.
     * @param overrideFormat A user supplied Java timestamp format.
     * @param timeoutChecker Will abort the operation if its timeout is exceeded.
     * @return An appropriate candidate timestamp object.
     */
    static CandidateTimestampFormat makeCandidateFromOverrideFormat(String overrideFormat, TimeoutChecker timeoutChecker) {

        // First check for a special format string
        switch (overrideFormat.toUpperCase(Locale.ROOT)) {
            case "ISO8601":
                return ISO8601_CANDIDATE_FORMAT;
            case "UNIX_MS":
                return UNIX_MS_CANDIDATE_FORMAT;
            case "UNIX":
                return UNIX_CANDIDATE_FORMAT;
            case "TAI64N":
                return TAI64N_CANDIDATE_FORMAT;
        }

        // Next check for a built-in candidate that incorporates the override, and prefer this

        // If the override is not a valid format then one or other of these two calls will
        // throw, and that is how we'll report the invalid format to the user
        Tuple<String, String> grokPatternAndRegex = overrideFormatToGrokAndRegex(overrideFormat);
        DateTimeFormatter javaTimeFormatter = DateTimeFormatter.ofPattern(overrideFormat, Locale.ROOT);

        // This timestamp (2001-02-03T04:05:06,123456789+0545) is chosen such that the month, day and hour all have just 1 digit.
        // This means that it will distinguish between formats that do/don't output leading zeroes for month, day and hour.
        // Additionally it has the full 9 digits of fractional second precision, to avoid the possibility of truncating the fraction.
        String generatedTimestamp = javaTimeFormatter.withZone(ZoneOffset.ofHoursMinutesSeconds(5, 45, 0))
            .format(Instant.ofEpochMilli(981173106123L).plusNanos(456789L));
        for (CandidateTimestampFormat candidate : ORDERED_CANDIDATE_FORMATS) {

            TimestampMatch match = checkCandidate(candidate, generatedTimestamp, null, true, timeoutChecker);
            if (match != null) {
                return new CandidateTimestampFormat(example -> {

                    // Modify the built-in candidate so it prefers to return the user supplied format
                    // if at all possible, and only falls back to standard logic for other situations
                    try {
                        // TODO consider support for overriding the locale too
                        // But since Grok only supports English and German date words ingest
                        // via Grok will fall down at an earlier stage for other languages...
                        javaTimeFormatter.parse(example);
                        return Collections.singletonList(overrideFormat);
                    } catch (DateTimeException e) {
                        return candidate.javaTimestampFormatSupplier.apply(example);
                    }
                }, candidate.simplePattern.pattern(), candidate.strictGrokPattern, candidate.outputGrokPatternName);
            }
        }

        // None of the out-of-the-box formats were close, so use the built Grok pattern and simple regex for the override
        return new CandidateTimestampFormat(
            example -> Collections.singletonList(overrideFormat),
            grokPatternAndRegex.v2(),
            grokPatternAndRegex.v1(),
            CUSTOM_TIMESTAMP_GROK_NAME
        );
    }

    /**
     * Find the first timestamp format that matches part or all of the supplied text.
     * @param candidate        The timestamp candidate to consider.
     * @param text             The text that the returned timestamp format must exist within.
     * @param numberPosBitSet  If not <code>null</code>, each bit must be set to <code>true</code> if and only if the
     *                         corresponding position in {@code text} is a digit.
     * @param requireFullMatch Does the candidate have to match the entire text?
     * @param timeoutChecker   Will abort the operation if its timeout is exceeded.
     * @return The timestamp format, or <code>null</code> if none matches.
     */
    private static TimestampMatch checkCandidate(
        CandidateTimestampFormat candidate,
        String text,
        @Nullable BitSet numberPosBitSet,
        boolean requireFullMatch,
        TimeoutChecker timeoutChecker
    ) {
        if (requireFullMatch) {
            Map<String, Object> captures = timeoutChecker.grokCaptures(
                candidate.strictFullMatchGrok,
                text,
                "timestamp format determination"
            );
            if (captures != null) {
                return new TimestampMatch(candidate, "", text, "");
            }
        } else {
            // Since a search in a long string that has sections that nearly match will be very slow, it's
            // worth doing an initial sanity check to see if the relative positions of digits necessary to
            // get a match exist first
            Tuple<Integer, Integer> boundsForCandidate = findBoundsForCandidate(candidate, numberPosBitSet);
            if (boundsForCandidate.v1() >= 0) {
                assert boundsForCandidate.v2() > boundsForCandidate.v1();
                String matchIn = text.substring(boundsForCandidate.v1(), Math.min(boundsForCandidate.v2(), text.length()));
                Map<String, Object> captures = timeoutChecker.grokCaptures(
                    candidate.strictSearchGrok,
                    matchIn,
                    "timestamp format determination"
                );
                if (captures != null) {
                    StringBuilder prefaceBuilder = new StringBuilder();
                    if (boundsForCandidate.v1() > 0) {
                        prefaceBuilder.append(text.subSequence(0, boundsForCandidate.v1()));
                    }
                    prefaceBuilder.append(captures.getOrDefault(PREFACE, ""));
                    StringBuilder epilogueBuilder = new StringBuilder();
                    epilogueBuilder.append(captures.getOrDefault(EPILOGUE, ""));
                    if (boundsForCandidate.v2() < text.length()) {
                        epilogueBuilder.append(text.subSequence(boundsForCandidate.v2(), text.length()));
                    }
                    return new TimestampMatch(
                        candidate,
                        prefaceBuilder.toString(),
                        text.substring(prefaceBuilder.length(), text.length() - epilogueBuilder.length()),
                        epilogueBuilder.toString()
                    );
                }
            } else {
                timeoutChecker.check("timestamp format determination");
            }
        }

        return null;
    }

    /**
     * Add a sample value to be considered by the format finder.  If {@code requireFullMatch} was set to
     * <code>true</code> on construction then the entire sample will be tested to see if it is a timestamp,
     * otherwise a timestamp may be detected as just a portion of the sample.  An exception will be thrown
     * if {@code errorOnNoTimestamp} was set to <code>true</code> on construction, and no timestamp is
     * found.  An exception will also be thrown if {@code errorOnMultiplePatterns} was set to <code>true</code>
     * on construction and a new timestamp format is detected that cannot be merged with a previously detected
     * format.
     * @param text The sample in which to detect a timestamp.
     */
    public void addSample(String text) {

        BitSet numberPosBitSet = requireFullMatch ? null : stringToNumberPosBitSet(text);

        for (CandidateTimestampFormat candidate : orderedCandidateFormats) {

            TimestampMatch match = checkCandidate(candidate, text, numberPosBitSet, requireFullMatch, timeoutChecker);
            if (match != null) {
                TimestampFormat newFormat = match.timestampFormat;
                boolean mustAdd = true;
                for (int i = 0; i < matchedFormats.size(); ++i) {
                    TimestampFormat existingFormat = matchedFormats.get(i);
                    if (existingFormat.canMergeWith(newFormat)) {
                        matchedFormats.set(i, existingFormat.mergeWith(newFormat));
                        mustAdd = false;
                        // Sharing formats considerably reduces the memory usage during the analysis
                        // when there are many samples, so reconstruct the match with a shared format
                        match = new TimestampMatch(match, matchedFormats.get(i));
                        break;
                    }
                }
                if (mustAdd) {
                    if (errorOnMultiplePatterns && matchedFormats.isEmpty() == false) {
                        throw new IllegalArgumentException(
                            "Multiple timestamp formats found [" + matchedFormats.get(0) + "] and [" + newFormat + "]"
                        );
                    }
                    matchedFormats.add(newFormat);
                }

                matches.add(match);
                cachedJavaTimestampFormats = null;
                return;
            }
        }

        if (errorOnNoTimestamp) {
            throw new IllegalArgumentException("No timestamp found in [" + text + "]");
        }
    }

    /**
     * Where multiple timestamp formats have been found, select the "best" one, whose details
     * will then be returned by methods such as {@link #getGrokPatternName} and
     * {@link #getJavaTimestampFormats}.  If fewer than two timestamp formats have been found
     * then this method does nothing.
     */
    public void selectBestMatch() {

        if (matchedFormats.size() < 2) {
            // Nothing to do
            return;
        }

        double[] weights = calculateMatchWeights();
        timeoutChecker.check("timestamp format determination");
        int highestWeightFormatIndex = findHighestWeightIndex(weights);
        timeoutChecker.check("timestamp format determination");
        selectHighestWeightFormat(highestWeightFormatIndex);
    }

    /**
     * For each matched format, calculate a weight that can be used to decide which match is best.  The
     * weight for each matched format is the sum of the weights for all matches that have that format.
     * @return An array of weights.  There is one entry in the array for each entry in {@link #matchedFormats},
     *         in the same order as the entries in {@link #matchedFormats}.
     */
    private double[] calculateMatchWeights() {

        int remainingMatches = matches.size();
        double[] weights = new double[matchedFormats.size()];
        for (TimestampMatch match : matches) {

            for (int matchedFormatIndex = 0; matchedFormatIndex < matchedFormats.size(); ++matchedFormatIndex) {
                if (matchedFormats.get(matchedFormatIndex).canMergeWith(match.timestampFormat)) {
                    weights[matchedFormatIndex] += weightForMatch(match.preface);
                    break;
                }
                ++matchedFormatIndex;
            }

            // The highest possible weight is 1, so if the difference between the two highest weights
            // is less than the number of lines remaining then the leader cannot possibly be overtaken
            if (findDifferenceBetweenTwoHighestWeights(weights) > --remainingMatches) {
                break;
            }
        }

        return weights;
    }

    /**
     * Used to weight a timestamp match according to how far along the line it is found.
     * Timestamps at the very beginning of the line are given a weight of 1.  The weight
     * progressively decreases the more text there is preceding the timestamp match, but
     * is always greater than 0.
     * @return A weight in the range (0, 1].
     */
    private static double weightForMatch(String preface) {
        return Math.pow(1.0 + preface.length() / 15.0, -1.1);
    }

    /**
     * Given an array of weights, find the difference between the two highest values.
     * @param weights Array of weights.  Must have at least two elements.
     * @return The difference between the two highest values.
     */
    private static double findDifferenceBetweenTwoHighestWeights(double[] weights) {
        assert weights.length >= 2;

        double highestWeight = 0.0;
        double secondHighestWeight = 0.0;
        for (double weight : weights) {
            if (weight > highestWeight) {
                secondHighestWeight = highestWeight;
                highestWeight = weight;
            } else if (weight > secondHighestWeight) {
                secondHighestWeight = weight;
            }
        }
        return highestWeight - secondHighestWeight;
    }

    /**
     * Given an array of weights, find the index with the highest weight.
     * @param weights Array of weights.
     * @return The index of the element with the highest weight.
     */
    private static int findHighestWeightIndex(double[] weights) {

        double highestWeight = Double.NEGATIVE_INFINITY;
        int highestWeightFormatIndex = -1;
        for (int index = 0; index < weights.length; ++index) {
            double weight = weights[index];
            if (weight > highestWeight) {
                highestWeight = weight;
                highestWeightFormatIndex = index;
            }
        }

        return highestWeightFormatIndex;
    }

    /**
     * Ensure the highest weight matched format is at the beginning of the list of matched formats.
     * @param highestWeightFormatIndex The index of the matched format with the highest weight.
     */
    private void selectHighestWeightFormat(int highestWeightFormatIndex) {

        assert highestWeightFormatIndex >= 0;
        // If the selected format is already at the beginning of the list there's nothing to do
        if (highestWeightFormatIndex == 0) {
            return;
        }

        cachedJavaTimestampFormats = null;
        List<TimestampFormat> newMatchedFormats = new ArrayList<>(matchedFormats);
        // Swap the selected format with the one that's currently at the beginning of the list
        newMatchedFormats.set(0, matchedFormats.get(highestWeightFormatIndex));
        newMatchedFormats.set(highestWeightFormatIndex, matchedFormats.get(0));
        matchedFormats = newMatchedFormats;
    }

    /**
     * How many different timestamp formats have been matched in the supplied samples?
     * @return The number of different timestamp formats that have been matched in the supplied samples.
     */
    public int getNumMatchedFormats() {
        return matchedFormats.size();
    }

    /**
     * Get the Grok pattern name that corresponds to the selected timestamp format.
     * @return The Grok pattern name that corresponds to the selected timestamp format.
     */
    public String getGrokPatternName() {
        if (matchedFormats.isEmpty()) {
            // If errorOnNoTimestamp is set and we get here it means no samples have been added, which is likely a programmer mistake
            assert errorOnNoTimestamp == false;
            return null;
        }
        return matchedFormats.get(0).grokPatternName;
    }

    /**
     * Get the custom Grok pattern definitions derived from the override format, if any.
     * @return The custom Grok pattern definitions for the selected timestamp format.
     *         If there are none an empty map is returned.
     */
    public Map<String, String> getCustomGrokPatternDefinitions() {
        if (matchedFormats.isEmpty()) {
            // If errorOnNoTimestamp is set and we get here it means no samples have been added, which is likely a programmer mistake
            assert errorOnNoTimestamp == false;
            return Collections.emptyMap();
        }
        return matchedFormats.get(0).customGrokPatternDefinitions;
    }

    /**
     * Of all the samples added that correspond to the selected format, return
     * the portion of the sample that comes before the timestamp.
     * @return A list of prefaces from samples that match the selected timestamp format.
     */
    public List<String> getPrefaces() {
        if (matchedFormats.isEmpty()) {
            // If errorOnNoTimestamp is set and we get here it means no samples have been added, which is likely a programmer mistake
            assert errorOnNoTimestamp == false;
            return Collections.emptyList();
        }
        return matches.stream()
            .filter(match -> matchedFormats.size() < 2 || matchedFormats.get(0).canMergeWith(match.timestampFormat))
            .map(match -> match.preface)
            .collect(Collectors.toList());
    }

    /**
     * Get the simple regular expression that can be used to identify timestamps
     * of the selected format in almost any programming language.
     * @return A {@link Pattern} that will match timestamps of the selected format.
     */
    public Pattern getSimplePattern() {
        if (matchedFormats.isEmpty()) {
            // If errorOnNoTimestamp is set and we get here it means no samples have been added, which is likely a programmer mistake
            assert errorOnNoTimestamp == false;
            return null;
        }
        return matchedFormats.get(0).simplePattern;
    }

    /**
     * These are similar to Java timestamp formats but may contain indeterminate day/month
     * placeholders if the order of day and month is uncertain.
     * @return A list of Java timestamp formats possibly containing indeterminate day/month placeholders.
     */
    public List<String> getRawJavaTimestampFormats() {
        if (matchedFormats.isEmpty()) {
            // If errorOnNoTimestamp is set and we get here it means no samples have been added, which is likely a programmer mistake
            assert errorOnNoTimestamp == false;
            return Collections.emptyList();
        }
        return matchedFormats.get(0).rawJavaTimestampFormats;
    }

    /**
     * These are used by ingest pipeline and index mappings.
     * @return A list of Java timestamp formats to use for parsing documents.
     */
    public List<String> getJavaTimestampFormats() {
        if (cachedJavaTimestampFormats != null) {
            return cachedJavaTimestampFormats;
        }
        return determiniseJavaTimestampFormats(
            getRawJavaTimestampFormats(),
            // With multiple formats, only consider the matches that correspond to the first
            // in the list (which is what we're returning information about via the getters).
            // With just one format it's most efficient not to bother checking formats.
            (matchedFormats.size() > 1) ? matchedFormats.get(0) : null
        );
    }

    /**
     * This is needed to decide between "date" and "date_nanos" as the index mapping type.
     * @return Do the observed timestamps require nanosecond precision to store accurately?
     */
    public boolean needNanosecondPrecision() {
        if (matchedFormats.isEmpty()) {
            // If errorOnNoTimestamp is set and we get here it means no samples have been added, which is likely a programmer mistake
            assert errorOnNoTimestamp == false;
            return false;
        }
        return matches.stream()
            .filter(match -> matchedFormats.size() < 2 || matchedFormats.get(0).canMergeWith(match.timestampFormat))
            .anyMatch(match -> match.hasNanosecondPrecision);
    }

    /**
     * Given a list of timestamp formats that might contain indeterminate day/month parts,
     * return the corresponding pattern with the placeholders replaced with concrete
     * day/month formats.
     */
    private List<String> determiniseJavaTimestampFormats(
        List<String> rawJavaTimestampFormats,
        @Nullable TimestampFormat onlyConsiderFormat
    ) {

        // This method needs rework if the class is ever made thread safe

        if (rawJavaTimestampFormats.stream().anyMatch(format -> format.indexOf(INDETERMINATE_FIELD_PLACEHOLDER) >= 0)) {
            boolean isDayFirst = guessIsDayFirst(rawJavaTimestampFormats, onlyConsiderFormat, Locale.getDefault());
            cachedJavaTimestampFormats = rawJavaTimestampFormats.stream()
                .map(format -> determiniseJavaTimestampFormat(format, isDayFirst))
                .collect(Collectors.toList());
        } else {
            cachedJavaTimestampFormats = rawJavaTimestampFormats;
        }
        return cachedJavaTimestampFormats;
    }

    /**
     * If timestamp formats where the order of day and month could vary (as in a choice between dd/MM/yyyy
     * or MM/dd/yyyy for example), make a guess about whether the day comes first.
     * @return <code>true</code> if the day comes first and <code>false</code> if the month comes first.
     */
    private boolean guessIsDayFirst(
        List<String> rawJavaTimestampFormats,
        @Nullable TimestampFormat onlyConsiderFormat,
        Locale localeForFallback
    ) {

        Boolean isDayFirst = guessIsDayFirstFromFormats(rawJavaTimestampFormats);
        if (isDayFirst != null) {
            return isDayFirst;
        }
        isDayFirst = guessIsDayFirstFromMatches(onlyConsiderFormat);
        if (isDayFirst != null) {
            return isDayFirst;
        }
        return guessIsDayFirstFromLocale(localeForFallback);
    }

    /**
     * If timestamp formats where the order of day and month could vary (as in a choice between dd/MM/yyyy
     * or MM/dd/yyyy for example), make a guess about whether the day comes first based on quirks of the
     * built-in Grok patterns.
     * @return <code>true</code> if the day comes first, <code>false</code> if the month comes first, and
     *         <code>null</code> if there is insufficient evidence to decide.
     */
    Boolean guessIsDayFirstFromFormats(List<String> rawJavaTimestampFormats) {

        Boolean isDayFirst = null;

        for (String rawJavaTimestampFormat : rawJavaTimestampFormats) {
            Matcher matcher = INDETERMINATE_FORMAT_INTERPRETER.matcher(rawJavaTimestampFormat);
            if (matcher.matches()) {
                String firstNumber = matcher.group(2);
                assert firstNumber != null;
                String secondNumber = matcher.group(4);
                if (secondNumber == null) {
                    return null;
                }
                if (firstNumber.length() == 2 && secondNumber.length() == 1) {
                    if (Boolean.FALSE.equals(isDayFirst)) {
                        // Inconsistency
                        return null;
                    }
                    isDayFirst = Boolean.TRUE;
                }
                if (firstNumber.length() == 1 && secondNumber.length() == 2) {
                    if (Boolean.TRUE.equals(isDayFirst)) {
                        // Inconsistency
                        return null;
                    }
                    isDayFirst = Boolean.FALSE;
                }
            }
        }

        if (isDayFirst != null) {
            if (isDayFirst) {
                explanation.add(
                    "Guessing day precedes month in timestamps as all detected formats have a two digits in the first number "
                        + "and a single digit in the second number which is what the %{MONTHDAY} and %{MONTHNUM} Grok patterns permit"
                );
            } else {
                explanation.add(
                    "Guessing month precedes day in timestamps as all detected formats have a single digit in the first number "
                        + "and two digits in the second number which is what the %{MONTHNUM} and %{MONTHDAY} Grok patterns permit"
                );
            }
        }

        return isDayFirst;
    }

    /**
     * If timestamp formats where the order of day and month could vary (as in a choice between dd/MM/yyyy
     * or MM/dd/yyyy for example), make a guess about whether the day comes first based on observed values
     * of the first and second numbers.
     * @return <code>true</code> if the day comes first, <code>false</code> if the month comes first, and
     *         <code>null</code> if there is insufficient evidence to decide.
     */
    Boolean guessIsDayFirstFromMatches(@Nullable TimestampFormat onlyConsiderFormat) {

        BitSet firstIndeterminateNumbers = new BitSet();
        BitSet secondIndeterminateNumbers = new BitSet();

        for (TimestampMatch match : matches) {

            if (onlyConsiderFormat == null || onlyConsiderFormat.canMergeWith(match.timestampFormat)) {

                // Valid indeterminate day/month numbers will be in the range 1 to 31.
                // -1 is used to mean "not present", and we ignore that here.

                if (match.firstIndeterminateDateNumber > 0) {
                    assert match.firstIndeterminateDateNumber <= 31;
                    if (match.firstIndeterminateDateNumber > 12) {
                        explanation.add(
                            "Guessing day precedes month in timestamps as one sample had first number ["
                                + match.firstIndeterminateDateNumber
                                + "]"
                        );
                        return Boolean.TRUE;
                    }
                    firstIndeterminateNumbers.set(match.firstIndeterminateDateNumber);
                }
                if (match.secondIndeterminateDateNumber > 0) {
                    assert match.secondIndeterminateDateNumber <= 31;
                    if (match.secondIndeterminateDateNumber > 12) {
                        explanation.add(
                            "Guessing month precedes day in timestamps as one sample had second number ["
                                + match.secondIndeterminateDateNumber
                                + "]"
                        );
                        return Boolean.FALSE;
                    }
                    secondIndeterminateNumbers.set(match.secondIndeterminateDateNumber);
                }
            }
        }

        // If there are many more values of one number than the other then assume that's the day
        final int ratioForResult = 3;
        int firstCardinality = firstIndeterminateNumbers.cardinality();
        int secondCardinality = secondIndeterminateNumbers.cardinality();
        if (secondCardinality == 0) {
            // This happens in the following cases:
            // - No indeterminate numbers (in which case the answer is irrelevant)
            // - Only one indeterminate number (in which case we favour month over day)
            return Boolean.FALSE;
        }
        // firstCardinality can be 0, but then secondCardinality should have been 0 too
        assert firstCardinality > 0;
        if (firstCardinality >= ratioForResult * secondCardinality) {
            explanation.add(
                "Guessing day precedes month in timestamps as there were ["
                    + firstCardinality
                    + "] distinct values of the first number but only ["
                    + secondCardinality
                    + "] for the second"
            );
            return Boolean.TRUE;
        }
        if (secondCardinality >= ratioForResult * firstCardinality) {
            explanation.add(
                "Guessing month precedes day in timestamps as there "
                    + (firstCardinality == 1 ? "was" : "were")
                    + " only ["
                    + firstCardinality
                    + "] distinct "
                    + (firstCardinality == 1 ? "value" : "values")
                    + " of the first number but ["
                    + secondCardinality
                    + "] for the second"
            );
            return Boolean.FALSE;
        }

        return null;
    }

    /**
     * If timestamp formats where the order of day and month could vary (as in a choice between dd/MM/yyyy
     * or MM/dd/yyyy for example), make a guess about whether the day comes first based on the default order
     * for a given locale.
     * @return <code>true</code> if the day comes first and <code>false</code> if the month comes first.
     */
    boolean guessIsDayFirstFromLocale(Locale locale) {

        // Fall back to whether the day comes before the month in the default short date format for the server locale.
        // Can't use 1 as that occurs in 1970, so 3rd Feb is the earliest date that will reveal the server default.
        String feb3rd1970 = makeShortLocalizedDateTimeFormatterForLocale(locale).format(LocalDate.ofEpochDay(33));
        if (feb3rd1970.indexOf('3') < feb3rd1970.indexOf('2')) {
            explanation.add(
                "Guessing day precedes month in timestamps based on server locale [" + locale.getDisplayName(Locale.ROOT) + "]"
            );
            return true;
        } else {
            explanation.add(
                "Guessing month precedes day in timestamps based on server locale [" + locale.getDisplayName(Locale.ROOT) + "]"
            );
            return false;
        }
    }

    @SuppressForbidden(
        reason = "DateTimeFormatter.ofLocalizedDate() is forbidden because it uses the default locale, "
            + "but here we are explicitly setting the locale on the formatter in a subsequent call"
    )
    private static DateTimeFormatter makeShortLocalizedDateTimeFormatterForLocale(Locale locale) {
        return DateTimeFormatter.ofLocalizedDate(FormatStyle.SHORT).withLocale(locale).withZone(ZoneOffset.UTC);
    }

    /**
     * Given a raw timestamp format that might contain indeterminate day/month parts,
     * return the corresponding pattern with the placeholders replaced with concrete
     * day/month formats.
     */
    static String determiniseJavaTimestampFormat(String rawJavaTimestampFormat, boolean isDayFirst) {

        Matcher matcher = INDETERMINATE_FORMAT_INTERPRETER.matcher(rawJavaTimestampFormat);
        if (matcher.matches()) {
            StringBuilder builder = new StringBuilder();
            for (int groupNum = 1; groupNum <= matcher.groupCount(); ++groupNum) {
                switch (groupNum) {
                    case 2: {
                        char formatChar = isDayFirst ? 'd' : 'M';
                        for (int count = matcher.group(groupNum).length(); count > 0; --count) {
                            builder.append(formatChar);
                        }
                        break;
                    }
                    case 4: {
                        char formatChar = isDayFirst ? 'M' : 'd';
                        for (int count = matcher.group(groupNum).length(); count > 0; --count) {
                            builder.append(formatChar);
                        }
                        break;
                    }
                    default:
                        builder.append(matcher.group(groupNum));
                        break;
                }
            }
            return builder.toString();
        } else {
            return rawJavaTimestampFormat;
        }
    }

    /**
     * These are still used by Logstash.
     * @return A list of Joda timestamp formats that correspond to the detected Java timestamp formats.
     */
    public List<String> getJodaTimestampFormats() {
        List<String> javaTimestampFormats = getJavaTimestampFormats();
        return (javaTimestampFormats == null)
            ? null
            : javaTimestampFormats.stream()
                .map(format -> format.replace("yy", "YY").replace("XXX", "ZZ").replace("XX", "Z"))
                .collect(Collectors.toList());
    }

    /**
     * Does the parsing the timestamp produce different results depending on the timezone of the parser?
     * I.e., does the textual representation NOT define the timezone?
     */
    public boolean hasTimezoneDependentParsing() {
        if (matchedFormats.isEmpty()) {
            // If errorOnNoTimestamp is set and we get here it means no samples have been added, which is likely a programmer mistake
            assert errorOnNoTimestamp == false;
            return false;
        }
        return matches.stream()
            .filter(match -> matchedFormats.size() < 2 || matchedFormats.get(0).canMergeWith(match.timestampFormat))
            .anyMatch(match -> match.hasTimezoneDependentParsing);
    }

    /**
     * The @timestamp field will always have been parsed into epoch format,
     * so we just need to know if it has nanosecond resolution or not.
     */
    public Map<String, String> getEsDateMappingTypeWithoutFormat() {
        return Collections.singletonMap(TextStructureUtils.MAPPING_TYPE_SETTING, needNanosecondPrecision() ? "date_nanos" : "date");
    }

    /**
     * Sometimes Elasticsearch mappings for dates need to include the format.
     * This method returns appropriate mappings settings: at minimum "type" : "date",
     * and possibly also a "format" setting.
     */
    public Map<String, String> getEsDateMappingTypeWithFormat() {
        List<String> javaTimestampFormats = getJavaTimestampFormats();
        if (javaTimestampFormats.contains("TAI64N")) {
            // There's no format for TAI64N in the timestamp formats used in mappings
            return Collections.singletonMap(TextStructureUtils.MAPPING_TYPE_SETTING, "keyword");
        }
        Map<String, String> mapping = new LinkedHashMap<>();
        mapping.put(TextStructureUtils.MAPPING_TYPE_SETTING, needNanosecondPrecision() ? "date_nanos" : "date");
        String formats = javaTimestampFormats.stream().map(format -> {
            switch (format) {
                case "ISO8601":
                    return "iso8601";
                case "UNIX_MS":
                    return "epoch_millis";
                case "UNIX":
                    return "epoch_second";
                default:
                    return format;
            }
        }).collect(Collectors.joining("||"));
        if (formats.isEmpty() == false) {
            mapping.put(TextStructureUtils.MAPPING_FORMAT_SETTING, formats);
        }
        return mapping;
    }

    /**
     * Given a timestamp candidate and a bit set showing the positions of digits in a piece of text, find the range
     * of indices over which the candidate <em>might</em> possibly match.  Searching for Grok patterns that nearly
     * match but don't quite is very expensive, so this method allows only a substring of a long string to be
     * searched using the full Grok pattern.
     * @param candidate       The timestamp candidate to consider.
     * @param numberPosBitSet If not <code>null</code>, each bit must be set to <code>true</code> if and only if the
     *                        corresponding position in the original text is a digit.
     * @return A tuple of the form (start index, end index).  If the timestamp candidate cannot possibly match
     *         anywhere then (-1, -1) is returned.  The end index in the returned tuple may be beyond the end of the
     *         string (because the bit set is not necessarily the same length as the string so it cannot be capped
     *         by this method), so the caller must cap it before passing to {@link String#substring(int, int)}.
     */
    static Tuple<Integer, Integer> findBoundsForCandidate(CandidateTimestampFormat candidate, BitSet numberPosBitSet) {

        if (numberPosBitSet == null || candidate.quickRuleOutBitSets.isEmpty()) {
            return new Tuple<>(0, Integer.MAX_VALUE);
        }

        int minFirstMatchStart = -1;
        int maxLastMatchEnd = -1;
        for (BitSet quickRuleOutBitSet : candidate.quickRuleOutBitSets) {
            int currentMatch = findBitPattern(numberPosBitSet, 0, quickRuleOutBitSet);
            if (currentMatch >= 0) {
                if (minFirstMatchStart == -1 || currentMatch < minFirstMatchStart) {
                    minFirstMatchStart = currentMatch;
                }
                do {
                    int currentMatchEnd = currentMatch + quickRuleOutBitSet.length();
                    if (currentMatchEnd > maxLastMatchEnd) {
                        maxLastMatchEnd = currentMatchEnd;
                    }
                    currentMatch = findBitPattern(numberPosBitSet, currentMatch + 1, quickRuleOutBitSet);
                } while (currentMatch > 0);
            }
        }
        if (minFirstMatchStart == -1) {
            assert maxLastMatchEnd == -1;
            return new Tuple<>(-1, -1);
        }
        int lowerBound = Math.max(0, minFirstMatchStart - candidate.maxCharsBeforeQuickRuleOutMatch);
        int upperBound = (Integer.MAX_VALUE - candidate.maxCharsAfterQuickRuleOutMatch - maxLastMatchEnd < 0)
            ? Integer.MAX_VALUE
            : (maxLastMatchEnd + candidate.maxCharsAfterQuickRuleOutMatch);
        return new Tuple<>(lowerBound, upperBound);
    }

    /**
     * This is basically the "Shift-Add" algorithm for string matching from the paper "A New Approach to Text Searching".
     * In this case the "alphabet" has just two "characters": 0 and 1 (or <code>false</code> and <code>true</code> in
     * some places because of the {@link BitSet} interface).
     * @see <a href="http://akira.ruc.dk/~keld/teaching/algoritmedesign_f08/Artikler/09/Baeza92.pdf">A New Approach to Text Searching</a>
     * @param findIn     The binary string to search in; "text" in the terminology of the paper.
     * @param beginIndex The index to start searching {@code findIn}.
     * @param toFind     The binary string to find; "pattern" in the terminology of the paper.
     * @return The index (starting from 0) of the first match of {@code toFind} in {@code findIn}, or -1 if no match is found.
     */
    static int findBitPattern(BitSet findIn, int beginIndex, BitSet toFind) {

        assert beginIndex >= 0;

        // Note that this only compares up to the highest bit that is set, so trailing non digit characters will not participate
        // in the comparison. This is not currently a problem for this class, but is something to consider if this functionality
        // is ever reused elsewhere. The solution would be to use a wrapper class containing a BitSet and a separate int to store
        // the length to compare.
        int toFindLength = toFind.length();
        int findInLength = findIn.length();
        if (toFindLength == 0) {
            return beginIndex;
        }
        // 63 here is the largest bit position (starting from 0) in a long
        if (toFindLength > Math.min(63, findInLength)) {
            // Since we control the input we should avoid the situation
            // where the pattern to find has more bits than a single long
            assert toFindLength <= 63 : "Length to find was [" + toFindLength + "] - cannot be greater than 63";
            return -1;
        }
        // ~1L means all bits set except the least significant
        long state = ~1L;
        // This array has one entry per "character" in the "alphabet" (which for this method consists of just 0 and 1)
        // ~0L means all bits set
        long[] toFindMask = { ~0L, ~0L };
        for (int i = 0; i < toFindLength; ++i) {
            toFindMask[toFind.get(i) ? 1 : 0] &= ~(1L << i);
        }
        for (int i = beginIndex; i < findInLength; ++i) {
            state |= toFindMask[findIn.get(i) ? 1 : 0];
            state <<= 1;
            if ((state & (1L << toFindLength)) == 0L) {
                return i - toFindLength + 1;
            }
        }

        return -1;
    }

    /**
     * Converts a string into a {@link BitSet} with one bit per character of the string and bits
     * set to 1 if the corresponding character in the string is a digit and 0 if not.  (The first
     * character of the string corresponds to the least significant bit in the {@link BitSet}, so
     * if the {@link BitSet} is printed in natural order it will be reversed compared to the input,
     * and then the most significant bit will be printed first.  However, in terms of random access
     * to individual characters/bits, this "reversal" is by far the most intuitive representation.)
     * @param str The string to be mapped.
     * @return A {@link BitSet} suitable for use as input to {@link #findBitPattern}.
     */
    static BitSet stringToNumberPosBitSet(String str) {

        BitSet result = new BitSet();
        for (int index = 0; index < str.length(); ++index) {
            if (Character.isDigit(str.charAt(index))) {
                result.set(index);
            }
        }
        return result;
    }

    /**
     * Represents an overall format matched within the supplied samples.
     * Similar {@link TimestampFormat}s can be merged when they can be
     * recognised by the same Grok pattern, simple regular expression, and
     * punctuation in the preface, but have different Java timestamp formats.
     *
     * Objects are immutable.  Merges that result in changes return new
     * objects.
     */
    static final class TimestampFormat {

        /**
         * Java time formats that may contain indeterminate day/month patterns.
         */
        final List<String> rawJavaTimestampFormats;

        /**
         * A simple regex that will work in many languages to detect whether the timestamp format
         * exists in a particular line.
         */
        final Pattern simplePattern;

        /**
         * Name of a Grok pattern that will match the timestamp.
         */
        final String grokPatternName;

        /**
         * If {@link #grokPatternName} is not an out-of-the-box Grok pattern, then its definition.
         */
        final Map<String, String> customGrokPatternDefinitions;

        /**
         * The punctuation characters in the text preceeding the timestamp in the samples.
         */
        final String prefacePunctuation;

        TimestampFormat(
            List<String> rawJavaTimestampFormats,
            Pattern simplePattern,
            String grokPatternName,
            Map<String, String> customGrokPatternDefinitions,
            String prefacePunctuation
        ) {
            this.rawJavaTimestampFormats = Collections.unmodifiableList(rawJavaTimestampFormats);
            this.simplePattern = Objects.requireNonNull(simplePattern);
            this.grokPatternName = Objects.requireNonNull(grokPatternName);
            this.customGrokPatternDefinitions = Objects.requireNonNull(customGrokPatternDefinitions);
            this.prefacePunctuation = prefacePunctuation;
        }

        boolean canMergeWith(TimestampFormat other) {

            if (this == other) {
                return true;
            }

            return other != null
                && this.simplePattern.pattern().equals(other.simplePattern.pattern())
                && this.grokPatternName.equals(other.grokPatternName)
                && Objects.equals(this.customGrokPatternDefinitions, other.customGrokPatternDefinitions)
                && this.prefacePunctuation.equals(other.prefacePunctuation);
        }

        TimestampFormat mergeWith(TimestampFormat other) {

            if (canMergeWith(other)) {
                if (rawJavaTimestampFormats.equals(other.rawJavaTimestampFormats) == false) {
                    // Do the merge like this to preserve ordering
                    Set<String> mergedJavaTimestampFormats = new LinkedHashSet<>(rawJavaTimestampFormats);
                    if (mergedJavaTimestampFormats.addAll(other.rawJavaTimestampFormats)) {
                        return new TimestampFormat(
                            new ArrayList<>(mergedJavaTimestampFormats),
                            simplePattern,
                            grokPatternName,
                            customGrokPatternDefinitions,
                            prefacePunctuation
                        );
                    }
                }
                // The merged format is exactly the same as this format, so there's no need to create a new object
                return this;
            }

            throw new IllegalArgumentException("Cannot merge timestamp format [" + this + "] with [" + other + "]");
        }

        @Override
        public int hashCode() {
            return Objects.hash(
                rawJavaTimestampFormats,
                simplePattern.pattern(),
                grokPatternName,
                customGrokPatternDefinitions,
                prefacePunctuation
            );
        }

        @Override
        public boolean equals(Object other) {
            if (this == other) {
                return true;
            }
            if (other == null || getClass() != other.getClass()) {
                return false;
            }

            TimestampFormat that = (TimestampFormat) other;
            return Objects.equals(this.rawJavaTimestampFormats, that.rawJavaTimestampFormats)
                && Objects.equals(this.simplePattern.pattern(), that.simplePattern.pattern())
                && Objects.equals(this.grokPatternName, that.grokPatternName)
                && Objects.equals(this.customGrokPatternDefinitions, that.customGrokPatternDefinitions)
                && Objects.equals(this.prefacePunctuation, that.prefacePunctuation);
        }

        @Override
        public String toString() {
            return "Java timestamp formats = "
                + rawJavaTimestampFormats.stream().collect(Collectors.joining("', '", "[ '", "' ]"))
                + ", simple pattern = '"
                + simplePattern.pattern()
                + "', grok pattern = '"
                + grokPatternName
                + "'"
                + (customGrokPatternDefinitions.isEmpty() ? "" : ", custom grok pattern definitions = " + customGrokPatternDefinitions)
                + ", preface punctuation = '"
                + prefacePunctuation
                + "'";
        }
    }

    /**
     * Represents one match of a timestamp in one added sample.
     */
    static final class TimestampMatch {

        // This picks out punctuation that is likely to represent a field separator. It deliberately
        // leaves out punctuation that's most likely to vary between field values, such as dots.
        private static final Pattern NON_PUNCTUATION_PATTERN = Pattern.compile("[^\\\\/|~:;,<>()\\[\\]{}«»\t]+");

        // Used for deciding whether an ISO8601 timestamp contains a timezone.
        private static final Pattern ISO8601_TIMEZONE_PATTERN = Pattern.compile("(Z|[+-]\\d{2}:?\\d{2})$");

        /**
         * Text that came before the timestamp in the matched field/message.
         */
        final String preface;

        /**
         * Time format specifier(s) that will work with Logstash and Ingest pipeline date parsers.
         */
        final TimestampFormat timestampFormat;

        /**
         * These store the first and second numbers when the ordering of day and month is unclear,
         * for example in 05/05/2019.  Where the ordering is obvious they are set to -1.
         */
        final int firstIndeterminateDateNumber;
        final int secondIndeterminateDateNumber;

        final boolean hasTimezoneDependentParsing;
        final boolean hasNanosecondPrecision;

        /**
         * Text that came after the timestamp in the matched field/message.
         */
        final String epilogue;

        TimestampMatch(CandidateTimestampFormat chosenTimestampFormat, String preface, String matchedDate, String epilogue) {
            this.preface = Objects.requireNonNull(preface);
            this.timestampFormat = new TimestampFormat(
                chosenTimestampFormat.javaTimestampFormatSupplier.apply(matchedDate),
                chosenTimestampFormat.simplePattern,
                chosenTimestampFormat.outputGrokPatternName,
                chosenTimestampFormat.customGrokPatternDefinitions(),
                preface.isEmpty() ? preface : NON_PUNCTUATION_PATTERN.matcher(preface).replaceAll("")
            );
            int[] indeterminateDateNumbers = parseIndeterminateDateNumbers(matchedDate, timestampFormat.rawJavaTimestampFormats);
            this.firstIndeterminateDateNumber = indeterminateDateNumbers[0];
            this.secondIndeterminateDateNumber = indeterminateDateNumbers[1];
            this.hasTimezoneDependentParsing = requiresTimezoneDependentParsing(
                timestampFormat.rawJavaTimestampFormats.get(0),
                matchedDate
            );
            this.hasNanosecondPrecision = matchHasNanosecondPrecision(timestampFormat.rawJavaTimestampFormats.get(0), matchedDate);
            this.epilogue = Objects.requireNonNull(epilogue);
        }

        TimestampMatch(TimestampMatch toCopyExceptFormat, TimestampFormat timestampFormat) {
            this.preface = toCopyExceptFormat.preface;
            this.timestampFormat = Objects.requireNonNull(timestampFormat);
            this.firstIndeterminateDateNumber = toCopyExceptFormat.firstIndeterminateDateNumber;
            this.secondIndeterminateDateNumber = toCopyExceptFormat.secondIndeterminateDateNumber;
            this.hasTimezoneDependentParsing = toCopyExceptFormat.hasTimezoneDependentParsing;
            this.hasNanosecondPrecision = toCopyExceptFormat.hasNanosecondPrecision;
            this.epilogue = toCopyExceptFormat.epilogue;
        }

        static boolean requiresTimezoneDependentParsing(String format, String matchedDate) {
            switch (format) {
                case "ISO8601":
                    assert matchedDate.length() > 6;
                    return ISO8601_TIMEZONE_PATTERN.matcher(matchedDate).find(matchedDate.length() - 6) == false;
                case "UNIX_MS":
                case "UNIX":
                case "TAI64N":
                    return false;
                default:
                    boolean notQuoted = true;
                    for (int pos = 0; pos < format.length(); ++pos) {
                        char curChar = format.charAt(pos);
                        if (curChar == '\'') {
                            notQuoted = notQuoted == false;
                        } else if (notQuoted && (curChar == 'X' || curChar == 'z')) {
                            return false;
                        }
                    }
                    return true;
            }
        }

        static boolean matchHasNanosecondPrecision(String format, String matchedDate) {
            switch (format) {
                case "ISO8601":
                    Matcher matcher = FRACTIONAL_SECOND_INTERPRETER.matcher(matchedDate);
                    return matcher.find() && matcher.group(2).length() > 3;
                case "UNIX_MS":
                case "UNIX":
                    return false;
                case "TAI64N":
                    return true;
                default:
                    boolean notQuoted = true;
                    int consecutiveSs = 0;
                    for (int pos = 0; pos < format.length(); ++pos) {
                        char curChar = format.charAt(pos);
                        if (curChar == '\'') {
                            // Literal single quotes are escaped by using two consecutive single quotes.
                            // Technically this code does the wrong thing in this case, as it flips quoting
                            // from off to on or on to off and then back. However, since by definition there
                            // is nothing in between the consecutive single quotes in this case, the net
                            // effect is correct and good enough for what this method is doing.
                            notQuoted = notQuoted == false;
                            consecutiveSs = 0;
                        } else if (notQuoted) {
                            if (curChar == 'S') {
                                if (++consecutiveSs > 3) {
                                    return true;
                                }
                            } else {
                                consecutiveSs = 0;
                            }
                        }
                    }
                    return false;
            }
        }

        static int[] parseIndeterminateDateNumbers(String matchedDate, List<String> rawJavaTimestampFormats) {
            int[] indeterminateDateNumbers = { -1, -1 };

            for (String rawJavaTimestampFormat : rawJavaTimestampFormats) {

                if (rawJavaTimestampFormat.indexOf(INDETERMINATE_FIELD_PLACEHOLDER) >= 0) {

                    try {
                        // Parse leniently under the assumption the first sequence of hashes is day and the
                        // second is month - this may not be true but all we do is extract the numbers
                        String javaTimestampFormat = determiniseJavaTimestampFormat(rawJavaTimestampFormat, true);

                        // TODO consider support for overriding the locale too
                        // But it's not clear-cut as Grok only knows English and German date
                        // words and for indeterminate formats we're expecting numbers anyway
                        DateTimeFormatter javaTimeFormatter = DateTimeFormatter.ofPattern(javaTimestampFormat, Locale.ROOT)
                            .withResolverStyle(ResolverStyle.LENIENT);
                        TemporalAccessor accessor = javaTimeFormatter.parse(matchedDate);
                        indeterminateDateNumbers[0] = accessor.get(ChronoField.DAY_OF_MONTH);

                        // Now parse again leniently under the assumption the first sequence of hashes is month and the
                        // second is day - we have to do it twice and extract day as the lenient parser will wrap months > 12
                        javaTimestampFormat = determiniseJavaTimestampFormat(rawJavaTimestampFormat, false);

                        // TODO consider support for overriding the locale too
                        // But it's not clear-cut as Grok only knows English and German date
                        // words and for indeterminate formats we're expecting numbers anyway
                        javaTimeFormatter = DateTimeFormatter.ofPattern(javaTimestampFormat, Locale.ROOT)
                            .withResolverStyle(ResolverStyle.LENIENT);
                        accessor = javaTimeFormatter.parse(matchedDate);
                        indeterminateDateNumbers[1] = accessor.get(ChronoField.DAY_OF_MONTH);
                        if (indeterminateDateNumbers[0] > 0 && indeterminateDateNumbers[1] > 0) {
                            break;
                        }
                    } catch (DateTimeException e) {
                        // Move on to the next format
                    }
                }
            }

            return indeterminateDateNumbers;
        }

        @Override
        public int hashCode() {
            return Objects.hash(
                preface,
                timestampFormat,
                firstIndeterminateDateNumber,
                secondIndeterminateDateNumber,
                hasTimezoneDependentParsing,
                epilogue
            );
        }

        @Override
        public boolean equals(Object other) {
            if (this == other) {
                return true;
            }
            if (other == null || getClass() != other.getClass()) {
                return false;
            }

            TimestampMatch that = (TimestampMatch) other;
            return Objects.equals(this.preface, that.preface)
                && Objects.equals(this.timestampFormat, that.timestampFormat)
                && this.firstIndeterminateDateNumber == that.firstIndeterminateDateNumber
                && this.secondIndeterminateDateNumber == that.secondIndeterminateDateNumber
                && this.hasTimezoneDependentParsing == that.hasTimezoneDependentParsing
                && Objects.equals(this.epilogue, that.epilogue);
        }

        @Override
        public String toString() {
            return (preface.isEmpty() ? "" : "preface = '" + preface + "', ")
                + timestampFormat
                + ((firstIndeterminateDateNumber > 0 || secondIndeterminateDateNumber > 0)
                    ? ", indeterminate date numbers = (" + firstIndeterminateDateNumber + "," + secondIndeterminateDateNumber + ")"
                    : "")
                + ", has timezone-dependent parsing = "
                + hasTimezoneDependentParsing
                + (epilogue.isEmpty() ? "" : ", epilogue = '" + epilogue + "'");
        }
    }

    /**
     * Stores the details of a possible timestamp format to consider when looking for timestamps.
     */
    static final class CandidateTimestampFormat {

        // This means that in the case of a literal Z, XXX is preferred
        private static final Pattern TRAILING_OFFSET_WITHOUT_COLON_FINDER = Pattern.compile("[+-]\\d{4}$");

        final Function<String, List<String>> javaTimestampFormatSupplier;
        final Pattern simplePattern;
        final String strictGrokPattern;
        final Grok strictSearchGrok;
        final Grok strictFullMatchGrok;
        final String outputGrokPatternName;
        final List<BitSet> quickRuleOutBitSets;
        final int maxCharsBeforeQuickRuleOutMatch;
        final int maxCharsAfterQuickRuleOutMatch;

        CandidateTimestampFormat(
            Function<String, List<String>> javaTimestampFormatSupplier,
            String simpleRegex,
            String strictGrokPattern,
            String outputGrokPatternName
        ) {
            this(
                javaTimestampFormatSupplier,
                simpleRegex,
                strictGrokPattern,
                outputGrokPatternName,
                Collections.emptyList(),
                Integer.MAX_VALUE,
                Integer.MAX_VALUE
            );
        }

        CandidateTimestampFormat(
            Function<String, List<String>> javaTimestampFormatSupplier,
            String simpleRegex,
            String strictGrokPattern,
            String outputGrokPatternName,
            String quickRuleOutPattern,
            int maxCharsBeforeQuickRuleOutMatch,
            int maxCharsAfterQuickRuleOutMatch
        ) {
            this(
                javaTimestampFormatSupplier,
                simpleRegex,
                strictGrokPattern,
                outputGrokPatternName,
                Collections.singletonList(quickRuleOutPattern),
                maxCharsBeforeQuickRuleOutMatch,
                maxCharsAfterQuickRuleOutMatch
            );
        }

        CandidateTimestampFormat(
            Function<String, List<String>> javaTimestampFormatSupplier,
            String simpleRegex,
            String strictGrokPattern,
            String outputGrokPatternName,
            List<String> quickRuleOutPatterns,
            int maxCharsBeforeQuickRuleOutMatch,
            int maxCharsAfterQuickRuleOutMatch
        ) {
            this.javaTimestampFormatSupplier = Objects.requireNonNull(javaTimestampFormatSupplier);
            this.simplePattern = Pattern.compile(simpleRegex, Pattern.MULTILINE);
            this.strictGrokPattern = Objects.requireNonNull(strictGrokPattern);
            // The (?m) here has the Ruby meaning, which is equivalent to (?s) in Java
            this.strictSearchGrok = new Grok(
                Grok.getBuiltinPatterns(false),
                "(?m)%{DATA:" + PREFACE + "}" + strictGrokPattern + "%{GREEDYDATA:" + EPILOGUE + "}",
                TimeoutChecker.watchdog,
                logger::warn
            );
            this.strictFullMatchGrok = new Grok(
                Grok.getBuiltinPatterns(false),
                "^" + strictGrokPattern + "$",
                TimeoutChecker.watchdog,
                logger::warn
            );
            this.outputGrokPatternName = Objects.requireNonNull(outputGrokPatternName);
            this.quickRuleOutBitSets = quickRuleOutPatterns.stream()
                .map(TimestampFormatFinder::stringToNumberPosBitSet)
                .collect(Collectors.toList());
            assert maxCharsBeforeQuickRuleOutMatch >= 0;
            this.maxCharsBeforeQuickRuleOutMatch = maxCharsBeforeQuickRuleOutMatch;
            assert maxCharsAfterQuickRuleOutMatch >= 0;
            this.maxCharsAfterQuickRuleOutMatch = maxCharsAfterQuickRuleOutMatch;
        }

        Map<String, String> customGrokPatternDefinitions() {
            return CUSTOM_TIMESTAMP_GROK_NAME.equals(outputGrokPatternName)
                ? Collections.singletonMap(CUSTOM_TIMESTAMP_GROK_NAME, strictGrokPattern)
                : Collections.emptyMap();
        }

        static List<String> iso8601FormatFromExample(String example) {

            // The Elasticsearch ISO8601 parser requires a literal T between the date and time, so
            // longhand formats are needed if there's a space instead
            return (example.indexOf('T') >= 0) ? Collections.singletonList("ISO8601") : iso8601LikeFormatFromExample(example, " ", "");
        }

        static List<String> iso8601LikeFormatFromExample(String example, String timeSeparator, String timezoneSeparator) {

            StringBuilder builder = new StringBuilder("yyyy-MM-dd");
            builder.append(timeSeparator).append("HH:mm");

            // Seconds are optional in ISO8601
            if (example.length() > builder.length() && example.charAt(builder.length()) == ':') {
                builder.append(":ss");
            }

            if (example.length() > builder.length()) {

                // Add fractional seconds pattern if appropriate
                char nextChar = example.charAt(builder.length());
                if (FRACTIONAL_SECOND_SEPARATORS.indexOf(nextChar) >= 0) {
                    builder.append(nextChar);
                    for (int pos = builder.length(); pos < example.length(); ++pos) {
                        if (Character.isDigit(example.charAt(pos))) {
                            builder.append('S');
                        } else {
                            break;
                        }
                    }
                }

                // Add timezone if appropriate - in the case of a literal Z, XX is preferred
                if (example.length() > builder.length()) {
                    builder.append(timezoneSeparator).append((example.indexOf(':', builder.length()) > 0) ? "XXX" : "XX");
                }
            } else {
                // This method should not have been called if the example didn't include the bare minimum of date and time
                assert example.length() == builder.length() : "Expected [" + example + "] and [" + builder + "] to be the same length";
            }

            return Collections.singletonList(builder.toString());
        }

        static List<String> adjustTrailingTimezoneFromExample(String example, String formatWithSecondsAndXX) {
            return Collections.singletonList(
                TRAILING_OFFSET_WITHOUT_COLON_FINDER.matcher(example).find() ? formatWithSecondsAndXX : formatWithSecondsAndXX + "X"
            );
        }

        private static String adjustFractionalSecondsFromEndOfExample(String example, String formatNoFraction) {

            Matcher matcher = FRACTIONAL_SECOND_INTERPRETER.matcher(example);
            return matcher.find()
                ? (formatNoFraction + matcher.group(1).charAt(0) + "SSSSSSSSS".substring(0, matcher.group(2).length()))
                : formatNoFraction;
        }

        static List<String> expandDayAndAdjustFractionalSecondsFromExample(String example, String formatWithddAndNoFraction) {

            String formatWithdd = adjustFractionalSecondsFromEndOfExample(example, formatWithddAndNoFraction);
            return Arrays.asList(formatWithdd, formatWithdd.replace(" dd", "  d"), formatWithdd.replace(" dd", " d"));
        }

        static List<String> indeterminateDayMonthFormatFromExample(String example) {

            StringBuilder builder = new StringBuilder();
            int examplePos = 0;

            // INDETERMINATE_FIELD_PLACEHOLDER here could represent either a day number (d) or month number (M) - it
            // will get changed later based on evidence from many examples
            for (Character patternChar : Arrays.asList(
                INDETERMINATE_FIELD_PLACEHOLDER,
                INDETERMINATE_FIELD_PLACEHOLDER,
                'y',
                'H',
                'm',
                's'
            )) {

                boolean foundDigit = false;
                while (examplePos < example.length() && Character.isDigit(example.charAt(examplePos))) {
                    foundDigit = true;
                    builder.append(patternChar);
                    ++examplePos;
                }

                if (patternChar == 's' || examplePos >= example.length() || foundDigit == false) {
                    break;
                }

                builder.append(example.charAt(examplePos));
                ++examplePos;
            }

            String format = builder.toString();
            // The Grok pattern should ensure we got at least as far as the year
            assert format.contains("yy") : "Unexpected format [" + format + "] from example [" + example + "]";

            if (examplePos < example.length()) {
                // If we haven't consumed the whole example then we should have got as far as
                // the (whole) seconds, and the bit afterwards should be the fractional seconds
                assert builder.toString().endsWith("ss") : "Unexpected format [" + format + "] from example [" + example + "]";
                format = adjustFractionalSecondsFromEndOfExample(example, format);
            }

            assert Character.isLetter(format.charAt(format.length() - 1))
                : "Unexpected format [" + format + "] from example [" + example + "]";
            assert format.length() == example.length() : "Unexpected format [" + format + "] from example [" + example + "]";

            return Collections.singletonList(format);
        }
    }
}
