/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License, v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.todo;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Finds to-do and fix-me markers in Java comments. The marker may appear anywhere after the
 * comment starts. A marker inside a string or character literal on the same line is ignored, as is
 * a marker that is only part of a longer identifier. This is still a line scan, not a full lexer.
 */
public final class TodoCommentParser {

    private static final Pattern JAVADOC_LINE = Pattern.compile("^\\s*\\*(?!/)\\s*");

    /**
     * One marker on one line.
     *
     * @param marker   upper-case marker text
     * @param message  comment text after the marker; a block comment ends at {@code *&#47;}
     * @param assignee text inside parentheses immediately after the marker, or null
     * @param line     1-based line number
     */
    public record Hit(String marker, String message, String assignee, int line) {}

    private TodoCommentParser() {}

    /**
     * Returns every comment marker in {@code content}, in line order. Matching is case-insensitive.
     * An empty {@code markers} list matches nothing.
     */
    public static List<Hit> find(String content, List<String> markers) {
        if (markers.isEmpty()) {
            return List.of();
        }
        String keywords = markers.stream().map(TodoCommentParser::escapeLiteral).collect(Collectors.joining("|"));
        // The marker may follow other comment text. It still has to be its own word, with an optional
        // (assignee) immediately after it. A marker in quotes names the token and is not a task.
        Pattern markerPattern = Pattern.compile("(?i)(" + keywords + ")\\b\\s*(?:\\(([^)]*)\\))?\\s*:?\\s*");

        String[] lines = content.split("\n", -1);
        List<Hit> hits = new ArrayList<>();
        for (int i = 0; i < lines.length; i++) {
            String line = stripCarriageReturn(lines[i]);
            Hit hit = matchLine(line, i + 1, markerPattern);
            if (hit != null) {
                hits.add(hit);
            }
        }
        return hits;
    }

    private static Hit matchLine(String line, int lineNumber, Pattern markerPattern) {
        CommentStart commentStart = commentContentStart(line);
        if (commentStart == null || commentStart.index() > line.length()) {
            return null;
        }
        Matcher marker = markerPattern.matcher(line);
        marker.region(commentStart.index(), line.length());
        while (marker.find()) {
            int start = marker.start(1);
            if (start > 0 && isQuote(line.charAt(start - 1))) {
                continue;
            }
            return new Hit(
                marker.group(1).toUpperCase(Locale.ROOT),
                message(commentText(line.substring(marker.end()), commentStart.block())),
                assignee(marker.group(2)),
                lineNumber
            );
        }
        return null;
    }

    /** Where comment text begins, and whether a {@code *&#47;} on this line ends it. */
    private record CommentStart(int index, boolean block) {}

    private static boolean isQuote(char c) {
        return c == '"' || c == '\'';
    }

    /**
     * Index of the first character of comment text, or -1 when the line has no comment.
     * {@code //} and {@code /*} are recognized only outside string and character literals.
     * A javadoc continuation line ({@code *} after optional indentation) is comment text too.
     */
    private static CommentStart commentContentStart(String line) {
        boolean inLiteral = false;
        char quote = 0;
        for (int i = 0; i < line.length(); i++) {
            char c = line.charAt(i);
            if (inLiteral) {
                if (c == '\\' && i + 1 < line.length()) {
                    i++;
                    continue;
                }
                if (c == quote) {
                    inLiteral = false;
                }
                continue;
            }
            if (c == '"' || c == '\'') {
                inLiteral = true;
                quote = c;
                continue;
            }
            if (c == '/' && i + 1 < line.length()) {
                char next = line.charAt(i + 1);
                if (next == '*') {
                    return new CommentStart(i + 2, true);
                }
                // Skip scheme:// so a URL in the comment is not treated as a second line comment.
                if (next == '/' && (i == 0 || line.charAt(i - 1) != ':')) {
                    return new CommentStart(i + 2, false);
                }
            }
        }
        Matcher javadoc = JAVADOC_LINE.matcher(line);
        if (javadoc.find() && javadoc.start() == 0) {
            return new CommentStart(javadoc.end(), true);
        }
        return null;
    }

    /**
     * Text after the marker. A block comment stops at {@code *&#47;} so code after the comment
     * is not part of the message. A line comment runs to the end of the line.
     */
    private static String commentText(String raw, boolean block) {
        if (block) {
            int close = raw.indexOf("*/");
            if (close >= 0) {
                raw = raw.substring(0, close);
            }
        }
        return message(raw);
    }

    private static String message(String raw) {
        String trimmed = raw.trim();
        if (trimmed.endsWith("*/")) {
            trimmed = trimmed.substring(0, trimmed.length() - 2).trim();
        }
        return trimmed;
    }

    private static String assignee(String raw) {
        if (raw == null) {
            return null;
        }
        String trimmed = raw.trim();
        return trimmed.isEmpty() ? null : trimmed;
    }

    private static String stripCarriageReturn(String line) {
        if (line.endsWith("\r")) {
            return line.substring(0, line.length() - 1);
        }
        return line;
    }

    /**
     * Escapes a marker so it is matched as text. {@code \Q...\E} is not used because Java treats
     * quoted text as case-sensitive even when the rest of the pattern is not.
     */
    private static String escapeLiteral(String literal) {
        StringBuilder escaped = new StringBuilder(literal.length());
        for (int i = 0; i < literal.length(); i++) {
            char c = literal.charAt(i);
            if ("\\.^$|?*+()[]{}".indexOf(c) >= 0) {
                escaped.append('\\');
            }
            escaped.append(c);
        }
        return escaped.toString();
    }
}
