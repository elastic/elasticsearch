/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.parser.promql;

import org.elasticsearch.core.Tuple;
import org.elasticsearch.xpack.esql.core.tree.Location;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.parser.ParsingException;

import java.time.Duration;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.LinkedHashMap;
import java.util.Map;

import static java.util.Collections.unmodifiableMap;

public class PromqlParserUtils {
    // time units recognized by Prometheus
    private static final Map<String, Long> TIME_UNITS;

    static {
        // NB: using JDK TimeUnit or ChronoUnits turns out to be verbose
        // hence the basic approach used below
        // NB2: using LHM to preserve insertion order for consistent strings around keys
        Map<String, Long> map = new LinkedHashMap<>();
        map.put("y", 1000L * 60 * 60 * 24 * 365);
        map.put("w", 1000L * 60 * 60 * 24 * 7);
        map.put("d", 1000L * 60 * 60 * 24);
        map.put("h", 1000L * 60 * 60);
        map.put("m", 1000L * 60);
        map.put("s", 1000L);
        map.put("ms", 1L);
        TIME_UNITS = unmodifiableMap(map);
    }

    private PromqlParserUtils() {}

    public static Duration parseDuration(Source source, String string) {
        char[] chars = string.toCharArray();

        long millis = 0;

        String errorPrefix = "Invalid time duration [{}], ";
        int current;
        Tuple<String, Long> lastUnit = null;
        for (int i = 0; i < chars.length;) {
            current = i;
            // number - look for digits
            while (current < chars.length && Character.isDigit(chars[current])) {
                current++;
            }
            // at least one digit needs to be specified
            if (current == i) {
                throw new ParsingException(source, errorPrefix + "no number specified at index [{}]", string, current);
            }
            String token = new String(chars, i, current - i);
            int number;
            try {
                number = Integer.parseInt(token);
            } catch (NumberFormatException ex) {
                throw new ParsingException(source, errorPrefix + "invalid number [{}]", string, token);
            }
            i = current;
            // unit - look for letters
            while (current < chars.length && Character.isLetter(chars[current])) {
                current++;
            }
            // at least one letter needs to be specified
            if (current == i) {
                throw new ParsingException(source, errorPrefix + "no unit specified at index [{}]", string, current);
            }
            token = new String(chars, i, current - i);
            i = current;

            Long msMultiplier = TIME_UNITS.get(token);
            if (msMultiplier == null) {
                throw new ParsingException(
                    source,
                    errorPrefix + "unrecognized time unit [{}], must be one of {}",
                    string,
                    token,
                    TIME_UNITS.keySet()
                );
            }
            if (lastUnit != null) {
                if (lastUnit.v2() < msMultiplier) {
                    throw new ParsingException(
                        source,
                        errorPrefix + "units must be ordered from the longest to the shortest, found [{}] before [{}]",
                        string,
                        lastUnit.v1(),
                        token
                    );
                } else if (lastUnit.v2().equals(msMultiplier)) {
                    throw new ParsingException(
                        source,
                        errorPrefix + "a given unit must only appear once, found [{}] multiple times",
                        string,
                        token
                    );
                }
            }
            lastUnit = new Tuple<>(token, msMultiplier);

            millis += number * msMultiplier;
        }

        return Duration.ofMillis(millis);
    }

    static String unquote(Source source) {
        // remove leading and trailing ' for strings and also eliminate escaped single quotes
        if (source == null) {
            return null;
        }

        String text = source.text();
        boolean unescaped = text.startsWith("`");

        // remove leading/trailing chars
        text = text.substring(1, text.length() - 1);

        if (unescaped) {
            return text;
        }
        StringBuilder sb = new StringBuilder();

        // https://prometheus.io/docs/prometheus/latest/querying/basics/#string-literals
        // Go: https://golang.org/ref/spec#Rune_literals
        char[] chars = text.toCharArray();
        for (int i = 0; i < chars.length;) {
            if (chars[i] == '\\') {
                // ANTLR4 Grammar guarantees there is always a character after the `\`
                switch (chars[++i]) {
                    case 'a':
                        sb.append('\u0007');
                        break;
                    case 'b':
                        sb.append('\b');
                        break;
                    case 'f':
                        sb.append('\f');
                        break;
                    case 'n':
                        sb.append('\n');
                        break;
                    case 'r':
                        sb.append('\r');
                        break;
                    case 't':
                        sb.append('\t');
                        break;
                    case 'v':
                        sb.append('\u000B');
                        break;
                    case '\\':
                        sb.append('\\');
                        break;
                    case '\'':
                        sb.append('\'');
                        break;
                    case '"':
                        sb.append('"');
                        break;
                    case 'x':
                    case 'u':
                    case 'U':
                        // all 3 cases rely on hex characters - only the number of chars between them differ
                        // get the current chat and move to the next offset
                        int ch = chars[i++];
                        int count = ch == 'U' ? 8 : (ch == 'u' ? 4 : 2);
                        sb.append(fromRadix(source, chars, i, count, 16));
                        i += count - 1;
                        break;
                    default:
                        // octal declaration - eats 3 chars
                        // there's no escape character, no need to move the offset
                        count = 3;
                        sb.append(fromRadix(source, chars, i, count, 8));
                        i += count - 1;
                }
                i++;
            } else {
                sb.append(chars[i++]);
            }
        }
        return sb.toString();
    }

    // parse the given number of strings to
    private static String fromRadix(Source source, char[] chars, int offset, int count, int radix) {
        if (offset + count > chars.length) {
            throw new ParsingException(
                source,
                "Incomplete escape sequence at [{}], expected [{}] found [{}]",
                offset,
                count,
                chars.length - offset - 1 // offset starts at 0
            );
        }

        String toParse = new String(chars, offset, count);
        int code;
        try {
            code = Integer.parseInt(toParse, radix);
        } catch (NumberFormatException ex) {
            throw new ParsingException(source, "Invalid unicode character code [{}]", toParse);
        }

        // For \x escapes (2-digit hex), validate UTF-8 compliance
        // Single-byte UTF-8 characters must be in range 0x00-0x7F
        if (radix == 16 && count == 2) {
            if (code >= 0x80 && code <= 0xFF) {
                throw new ParsingException(
                    source,
                    "Invalid unicode character code [\\x{}], single-byte UTF-8 characters must be in range 0x00-0x7F",
                    toParse
                );
            }
        }

        return String.valueOf(Character.toChars(code));
    }

    /**
     * Adjusts the location of the source by the line and column offsets.
     * @see #adjustLocation(Location, int, int)
     */
    public static Source adjustSource(Source source, int startLine, int startColumn) {
        return new Source(adjustLocation(source.source(), startLine, startColumn), source.text());
    }

    /**
     * Adjusts the location by the line and column offsets.
     * The PromQL query inside the PROMQL command is parsed separately,
     * so its line and column numbers need to be adjusted to match their
     * position inside the full ES|QL query.
     */
    public static Location adjustLocation(Location location, int startLine, int startColumn) {
        int lineNumber = location.getLineNumber();
        int columnNumber = location.getColumnNumber();
        return new Location(adjustLine(lineNumber, startLine), adjustColumn(lineNumber, columnNumber, startColumn));
    }

    /**
     * Adjusts the line and column numbers of the given {@code ParsingException}
     * by the provided offsets.
     * The PromQL query inside the PROMQL command is parsed separately,
     * so its line and column numbers need to be adjusted to match their
     * position inside the full ES|QL query.
     */
    public static ParsingException adjustParsingException(ParsingException pe, int promqlStartLine, int promqlStartColumn) {
        ParsingException adjusted = new ParsingException(
            pe.getErrorMessage(),
            pe.getCause() instanceof Exception ? (Exception) pe.getCause() : null,
            adjustLine(pe.getLineNumber(), promqlStartLine),
            adjustColumn(pe.getLineNumber(), pe.getColumnNumber(), promqlStartColumn)
        );
        adjusted.setStackTrace(pe.getStackTrace());
        return adjusted;
    }

    private static int adjustLine(int lineNumber, int startLine) {
        return lineNumber + startLine - 1;
    }

    private static int adjustColumn(int lineNumber, int columnNumber, int startColumn) {
        // the column offset only applies to the first line of the PROMQL command
        return lineNumber == 1 ? columnNumber + startColumn - 1 : columnNumber;
    }

    /*
     * Parses a Prometheus date which can be either a float representing epoch seconds or an RFC3339 date string.
     */
    public static Instant parseDate(Source source, String value) {
        try {
            return Instant.ofEpochMilli((long) (Double.parseDouble(value) * 1000));
        } catch (NumberFormatException ignore) {
            // Not a float, try parsing as date string
        }
        try {
            return Instant.parse(value);
        } catch (DateTimeParseException e) {
            throw new ParsingException(source, "Invalid date format [{}]", value);
        }
    }
}
