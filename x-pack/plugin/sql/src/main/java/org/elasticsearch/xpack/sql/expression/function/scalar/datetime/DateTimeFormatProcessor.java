/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
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
import java.util.Set;
import java.util.function.Function;

import static org.elasticsearch.xpack.sql.util.DateUtils.asTimeAtZone;

public class DateTimeFormatProcessor extends BinaryDateTimeProcessor {

    public static final String NAME = "dtformat";

    /**
     * these characters have a meaning in MS date patterns.
     * If a character is not in this set, then it's still allowed in MS FORMAT patters
     * but not in Java, so it has to be translated or quoted
     */
    private static final Set<Character> MS_DATETIME_PATTERN_CHARS = Set.of(
        'd',
        'f',
        'F',
        'g',
        'h',
        'H',
        'K',
        'm',
        'M',
        's',
        't',
        'y',
        'z',
        ':',
        '/',
        ' ',
        '-'
    );

    /**
     * characters that start a quoting block in MS patterns
     */
    private static final Set<Character> MS_QUOTING_CHARS = Set.of('\\', '\'', '"');

    /**
     * list of MS datetime patterns with the corresponding translation in Java DateTimeFormat
     * (patterns that are the same in Java and in MS are not listed here)
     */
    private static final String[][] MS_TO_JAVA_PATTERNS = {
        { "tt", "a" },
        { "t", "a" },
        { "dddd", "eeee" },
        { "ddd", "eee" },
        { "K", "v" },
        { "g", "G" },
        { "f", "S" },
        { "F", "S" },
        { "z", "X" } };

    private final Formatter formatter;

    public enum Formatter {
        FORMAT {
            @Override
            protected Function<TemporalAccessor, String> formatterFor(String pattern) {
                if (pattern.isEmpty()) {
                    return null;
                }
                final String javaPattern = msToJavaPattern(pattern);
                return DateTimeFormatter.ofPattern(javaPattern, Locale.ROOT)::format;
            }
        },
        DATE_FORMAT {
            @Override
            protected Function<TemporalAccessor, String> formatterFor(String pattern) {
                return DateFormatter.ofPattern(pattern);
            }
        },
        DATE_TIME_FORMAT {
            @Override
            protected Function<TemporalAccessor, String> formatterFor(String pattern) {
                return DateTimeFormatter.ofPattern(pattern, Locale.ROOT)::format;
            }
        },
        TO_CHAR {
            @Override
            protected Function<TemporalAccessor, String> formatterFor(String pattern) {
                return ToCharFormatter.ofPattern(pattern);
            }
        };

        protected static String msToJavaPattern(String pattern) {
            StringBuilder result = new StringBuilder(pattern.length());
            StringBuilder partialQuotedString = new StringBuilder();

            boolean originalCharacterQuoted = false;
            boolean lastTargetCharacterQuoted = false;
            char quotingChar = '\\';

            for (int i = 0; i < pattern.length(); i++) {
                char c = pattern.charAt(i);
                if (originalCharacterQuoted) {
                    if (quotingChar == '\\') {
                        // in the original pattern, this is a single quoted character, add it to the partial string
                        // that will be quoted in Java
                        originalCharacterQuoted = false;
                        lastTargetCharacterQuoted = true;
                        partialQuotedString.append(c);
                    } else if (c == quotingChar) {
                        // the original pattern is closing the quoting,
                        // do nothing for now, next character could open a new quoting block
                        originalCharacterQuoted = false;
                    } else {
                        // any character that is not a quoting char is just added to the partial quoting string
                        // because there could be more characters to quote after that
                        partialQuotedString.append(c);
                    }
                } else {
                    boolean characterProcessed = false;
                    // the original pattern is not quoting
                    if (MS_QUOTING_CHARS.contains(c)) {
                        // next character(s) is quoted, start a quoted block on the target
                        originalCharacterQuoted = true;
                        lastTargetCharacterQuoted = true;
                        quotingChar = c;
                        characterProcessed = true;
                    } else {
                        // manage patterns that are different from MS to Java and have to be translated
                        for (String[] item : MS_TO_JAVA_PATTERNS) {
                            int fragmentLength = item[0].length();
                            if (i + fragmentLength <= pattern.length() && item[0].equals(pattern.substring(i, i + fragmentLength))) {
                                if (lastTargetCharacterQuoted) {
                                    // now origin is not quoting for sure and the next block is a valid datetime pattern,
                                    // that has to be translated and written as is (not quoted).
                                    // Before doing this, let's flush the previously quoted string
                                    // and quote it properly with Java syntax
                                    lastTargetCharacterQuoted = false;
                                    quoteAndAppend(result, partialQuotedString);
                                    partialQuotedString = new StringBuilder();
                                }
                                // and then translate the pattern
                                result.append(item[1]);
                                characterProcessed = true;
                                i += (fragmentLength - 1); // fast-forward, because the replaced pattern could be longer than one character
                                break;
                            }
                        }
                    }
                    if (characterProcessed == false) {
                        if (MS_DATETIME_PATTERN_CHARS.contains(c) == false) {
                            // this character is allowed in MS, but not in Java, so it has to be quoted in the result
                            lastTargetCharacterQuoted = true;
                            partialQuotedString.append(c);
                        } else {
                            // any other character is a valid datetime pattern in both Java and MS
                            if (lastTargetCharacterQuoted) {
                                // flush the quoted string first, if any
                                lastTargetCharacterQuoted = false;
                                quoteAndAppend(result, partialQuotedString);
                                partialQuotedString = new StringBuilder();
                            }
                            // and then add the character itself, as it is
                            result.append(c);
                        }
                    }
                }
            }
            // if the original pattern ended with a quoted block, flush it to the result and quote it in Java
            if (lastTargetCharacterQuoted) {
                quoteAndAppend(result, partialQuotedString);
            }
            return result.toString();
        }

        private static void quoteAndAppend(StringBuilder mainBuffer, StringBuilder fragmentToQuote) {
            mainBuffer.append("'");
            mainBuffer.append(fragmentToQuote.toString().replaceAll("'", "''"));
            mainBuffer.append("'");
        }

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
