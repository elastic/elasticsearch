/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.datafeed.extractor.esql;

import org.elasticsearch.common.Strings;

/**
 * A deliberately narrow ES|QL lexer for ML datafeed query shaping. It avoids an ML dependency on the ES|QL parser.
 * <p>
 * MUST FIX before production: hand-written ES|QL parsing is brittle (misses nested comments, subqueries, FORK, settings,
 * backtick-quoted fields, ...) and drifts as the ES|QL syntax evolves. Replace with EsqlParser; tracked in
 * https://github.com/elastic/elasticsearch/issues/161207
 */
final class EsqlQueryClauseScanner {

    private EsqlQueryClauseScanner() {}

    static ScanResult scan(String query, String timeField) {
        boolean hasOuterLimit = false;
        boolean hasOuterTimeWhere = false;
        boolean hasOuterTimeSort = false;
        boolean hasOuterStats = false;
        boolean scanTimeField = Strings.hasLength(timeField);
        for (int index = 0, nestingDepth = 0; index < query.length();) {
            char character = query.charAt(index);
            if (character == '"') {
                index = skipQuotedString(query, index);
            } else if (character == '`') {
                index = skipQuotedIdentifier(query, index);
            } else if (query.startsWith("//", index)) {
                index = skipLineComment(query, index + 2);
            } else if (query.startsWith("/*", index)) {
                index = skipBlockComment(query, index + 2);
            } else if (isOpeningDelimiter(character)) {
                nestingDepth++;
                index++;
            } else if (isClosingDelimiter(character)) {
                nestingDepth = Math.max(0, nestingDepth - 1);
                index++;
            } else if (character == '|' && nestingDepth == 0) {
                int commandStart = skipWhitespaceAndComments(query, index + 1);
                Command command = commandAt(query, commandStart);
                if (command == Command.LIMIT) {
                    hasOuterLimit = true;
                } else if (scanTimeField
                    && command == Command.WHERE
                    && containsTimeField(query, commandStart + command.text.length(), timeField)) {
                        hasOuterTimeWhere = true;
                    } else if (scanTimeField
                        && command == Command.SORT
                        && containsTimeField(query, commandStart + command.text.length(), timeField)) {
                            hasOuterTimeSort = true;
                        } else if (command == Command.STATS) {
                            hasOuterStats = true;
                        }
                index++;
            } else {
                index++;
            }
        }
        return new ScanResult(hasOuterLimit, hasOuterTimeWhere, hasOuterTimeSort, hasOuterStats);
    }

    /** Whether a depth-zero {@code STATS} command makes source doc count a poor proxy for output rows. */
    static boolean hasAggregation(String query) {
        return scan(query, null).hasOuterStats();
    }

    static boolean hasOuterLimit(String query) {
        return scan(query, null).hasOuterLimit();
    }

    /**
     * Appends a generated pipeline (e.g. {@code " | LIMIT 0"}) to a user-supplied query. When the query ends in a
     * {@code //} line comment, inserts a newline first so the generated text is not swallowed by that comment.
     */
    static String appendGeneratedPipeline(String query, String pipeline) {
        return query + (endsInLineComment(query) ? "\n" : "") + pipeline;
    }

    /** Leading pipeline segment before the first depth-zero {@code |}, or the whole query when unpiped. */
    static String extractLeadingCommand(String query) {
        for (int index = 0, nestingDepth = 0; index < query.length();) {
            char character = query.charAt(index);
            if (character == '"') {
                index = skipQuotedString(query, index);
            } else if (character == '`') {
                index = skipQuotedIdentifier(query, index);
            } else if (query.startsWith("//", index)) {
                index = skipLineComment(query, index + 2);
            } else if (query.startsWith("/*", index)) {
                index = skipBlockComment(query, index + 2);
            } else if (isOpeningDelimiter(character)) {
                nestingDepth++;
                index++;
            } else if (isClosingDelimiter(character)) {
                nestingDepth = Math.max(0, nestingDepth - 1);
                index++;
            } else if (character == '|' && nestingDepth == 0) {
                return query.substring(0, index);
            } else {
                index++;
            }
        }
        return query;
    }

    /** Index after leading whitespace and comments (for FROM/TS checks on {@link #extractLeadingCommand} text). */
    static int skipLeadingWhitespaceAndComments(String query) {
        return skipWhitespaceAndComments(query, 0);
    }

    static boolean endsInLineComment(String query) {
        for (int index = 0; index < query.length();) {
            if (query.charAt(index) == '"') {
                index = skipQuotedString(query, index);
            } else if (query.charAt(index) == '`') {
                index = skipQuotedIdentifier(query, index);
            } else if (query.startsWith("//", index)) {
                index = skipLineComment(query, index + 2);
                if (index == query.length()) {
                    return true;
                }
            } else if (query.startsWith("/*", index)) {
                index = skipBlockComment(query, index + 2);
            } else {
                index++;
            }
        }
        return false;
    }

    private static boolean containsTimeField(String query, int index, String timeField) {
        for (int nestingDepth = 0; index < query.length();) {
            char character = query.charAt(index);
            if (character == '"') {
                index = skipQuotedString(query, index);
            } else if (character == '`') {
                index = skipQuotedIdentifier(query, index);
            } else if (query.startsWith("//", index)) {
                index = skipLineComment(query, index + 2);
            } else if (query.startsWith("/*", index)) {
                index = skipBlockComment(query, index + 2);
            } else if (isOpeningDelimiter(character)) {
                nestingDepth++;
                index++;
            } else if (isClosingDelimiter(character)) {
                if (nestingDepth == 0) {
                    return false;
                }
                nestingDepth--;
                index++;
            } else if (character == '|' && nestingDepth == 0) {
                return false;
            } else if (nestingDepth == 0 && matchesIdentifier(query, index, timeField)) {
                return true;
            } else {
                index++;
            }
        }
        return false;
    }

    private static Command commandAt(String query, int index) {
        for (Command command : Command.values()) {
            if (query.regionMatches(true, index, command.text, 0, command.text.length())
                && (index + command.text.length() == query.length() || isCommandBoundary(query.charAt(index + command.text.length())))) {
                return command;
            }
        }
        return Command.OTHER;
    }

    private static boolean matchesIdentifier(String query, int index, String identifier) {
        int end = index + identifier.length();
        return end <= query.length()
            && query.regionMatches(false, index, identifier, 0, identifier.length())
            && (index == 0 || isIdentifierCharacter(query.charAt(index - 1)) == false)
            && (end == query.length() || isIdentifierCharacter(query.charAt(end)) == false);
    }

    private static boolean isIdentifierCharacter(char character) {
        return Character.isLetterOrDigit(character) || character == '_' || character == '@' || character == '.';
    }

    private static boolean isOpeningDelimiter(char character) {
        return character == '(' || character == '[' || character == '{';
    }

    private static boolean isClosingDelimiter(char character) {
        return character == ')' || character == ']' || character == '}';
    }

    private static boolean isCommandBoundary(char character) {
        return Character.isWhitespace(character) || character == '/' || character == '(' || character == ')';
    }

    static int skipWhitespaceAndComments(String query, int index) {
        while (index < query.length()) {
            if (Character.isWhitespace(query.charAt(index))) {
                index++;
            } else if (query.startsWith("//", index)) {
                index = skipLineComment(query, index + 2);
            } else if (query.startsWith("/*", index)) {
                index = skipBlockComment(query, index + 2);
            } else {
                break;
            }
        }
        return index;
    }

    static int skipQuotedString(String query, int index) {
        boolean tripleQuoted = query.startsWith("\"\"\"", index);
        int closingQuoteLength = tripleQuoted ? 3 : 1;
        index += closingQuoteLength;
        while (index < query.length()) {
            if (tripleQuoted && query.startsWith("\"\"\"", index)) {
                index += closingQuoteLength;
                for (int optionalQuote = 0; optionalQuote < 2 && index < query.length() && query.charAt(index) == '"'; optionalQuote++) {
                    index++;
                }
                return index;
            }
            if (tripleQuoted == false && query.charAt(index) == '\\') {
                index += 2;
            } else if (tripleQuoted == false && query.charAt(index) == '"') {
                return index + 1;
            } else {
                index++;
            }
        }
        return index;
    }

    private static int skipQuotedIdentifier(String query, int index) {
        index++;
        while (index < query.length()) {
            if (query.charAt(index) == '`') {
                if (index + 1 < query.length() && query.charAt(index + 1) == '`') {
                    index += 2;
                } else {
                    return index + 1;
                }
            } else {
                index++;
            }
        }
        return index;
    }

    private static int skipLineComment(String query, int index) {
        while (index < query.length() && query.charAt(index) != '\n' && query.charAt(index) != '\r') {
            index++;
        }
        return index;
    }

    private static int skipBlockComment(String query, int index) {
        int depth = 1;
        while (index < query.length() && depth > 0) {
            if (query.startsWith("/*", index)) {
                depth++;
                index += 2;
            } else if (query.startsWith("*/", index)) {
                depth--;
                index += 2;
            } else {
                index++;
            }
        }
        return index;
    }

    record ScanResult(boolean hasOuterLimit, boolean hasOuterTimeWhere, boolean hasOuterTimeSort, boolean hasOuterStats) {}

    private enum Command {
        WHERE("WHERE"),
        SORT("SORT"),
        LIMIT("LIMIT"),
        STATS("STATS"),
        OTHER("");

        private final String text;

        Command(String text) {
            this.text = text;
        }
    }
}
