/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.datafeed.extractor.esql;

/** Narrow ES|QL scan helpers for ML datafeeds without pulling in the ES|QL parser. */
final class EsqlQueryClauseScanner {

    private EsqlQueryClauseScanner() {}

    static boolean hasAggregation(String query) {
        for (int index = 0, nestingDepth = 0; index < query.length();) {
            ScanStep step = scanStep(query, index, nestingDepth);
            if (step.foundOuterStats) {
                return true;
            }
            index = step.nextIndex;
            nestingDepth = step.nestingDepth;
        }
        return false;
    }

    private record ScanStep(int nextIndex, int nestingDepth, boolean foundOuterStats) {}

    private static ScanStep scanStep(String query, int index, int nestingDepth) {
        char character = query.charAt(index);
        if (character == '"') {
            return new ScanStep(skipQuotedString(query, index), nestingDepth, false);
        }
        if (character == '`') {
            return new ScanStep(skipQuotedIdentifier(query, index), nestingDepth, false);
        }
        if (query.startsWith("//", index)) {
            return new ScanStep(skipLineComment(query, index + 2), nestingDepth, false);
        }
        if (query.startsWith("/*", index)) {
            return new ScanStep(skipBlockComment(query, index + 2), nestingDepth, false);
        }
        if (isOpeningDelimiter(character)) {
            return new ScanStep(index + 1, nestingDepth + 1, false);
        }
        if (isClosingDelimiter(character)) {
            return new ScanStep(index + 1, Math.max(0, nestingDepth - 1), false);
        }
        if (character == '|' && nestingDepth == 0) {
            int commandStart = skipWhitespaceAndComments(query, index + 1);
            boolean foundStats = isStatsCommand(query, commandStart);
            return new ScanStep(index + 1, nestingDepth, foundStats);
        }
        return new ScanStep(index + 1, nestingDepth, false);
    }

    private static boolean isStatsCommand(String query, int index) {
        return query.regionMatches(true, index, "STATS", 0, 5)
            && (index + 5 == query.length() || isCommandBoundary(query.charAt(index + 5)));
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

    private static int skipWhitespaceAndComments(String query, int index) {
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

    private static int skipQuotedString(String query, int index) {
        if (query.startsWith("\"\"\"", index)) {
            return skipTripleQuotedString(query, index + 3);
        }
        return skipSingleQuotedString(query, index + 1);
    }

    private static int skipTripleQuotedString(String query, int index) {
        while (index < query.length()) {
            if (query.startsWith("\"\"\"", index)) {
                index += 3;
                return skipTrailingQuotes(query, index);
            }
            index++;
        }
        return index;
    }

    private static int skipTrailingQuotes(String query, int index) {
        for (int optionalQuote = 0; optionalQuote < 2 && index < query.length() && query.charAt(index) == '"'; optionalQuote++) {
            index++;
        }
        return index;
    }

    private static int skipSingleQuotedString(String query, int index) {
        while (index < query.length()) {
            if (query.charAt(index) == '\\') {
                index += 2;
            } else if (query.charAt(index) == '"') {
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
}
