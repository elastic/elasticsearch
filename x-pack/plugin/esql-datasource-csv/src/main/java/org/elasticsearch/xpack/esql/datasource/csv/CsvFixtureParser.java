/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.csv;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * Standalone CSV parser for fixture generation. Parses CSV files with bracket-aware
 * multi-value support, matching the behavior of {@link CsvFormatReader}.
 * <p>
 * Used by OrcFixtureGenerator, ParquetFixtureGenerator, NdJsonFixtureGenerator, and TsvFixtureGenerator to read CSV fixtures
 * with correct multi-value handling (e.g. {@code [a,b,c]} as a list, not just first element).
 * <p>
 * Minimal dependencies: only java.util, java.io, java.nio. No esql-core or server.
 */
public final class CsvFixtureParser {

    private static final char DEFAULT_DELIMITER = ',';
    private static final char DEFAULT_QUOTE = '"';
    private static final char DEFAULT_ESCAPE = '\\';
    private static final String DEFAULT_COMMENT_PREFIX = "//";

    private CsvFixtureParser() {}

    /**
     * Parse a CSV file and return schema plus rows with proper multi-value handling.
     * Header must be in {@code column:type} format. Types: integer, long, double, keyword,
     * boolean, date, ip, etc.
     */
    public static CsvFixtureResult parseCsvFile(Path path) throws IOException {
        return parseCsvFile(path, DEFAULT_DELIMITER, DEFAULT_QUOTE, DEFAULT_ESCAPE, DEFAULT_COMMENT_PREFIX);
    }

    /**
     * Parse a CSV file with custom delimiter, quote, escape, and comment prefix.
     */
    public static CsvFixtureResult parseCsvFile(Path path, char delimiter, char quote, char escape, String commentPrefix)
        throws IOException {
        List<ColumnSpec> schema = new ArrayList<>();
        List<Object[]> rows = new ArrayList<>();

        try (BufferedReader reader = Files.newBufferedReader(path, StandardCharsets.UTF_8)) {
            String line;
            int lineNumber = 0;
            String[] headerEntries = null;

            while ((line = reader.readLine()) != null) {
                line = line.trim();
                if (line.isEmpty() || (commentPrefix != null && line.startsWith(commentPrefix))) {
                    continue;
                }
                StringBuilder logicalLine = new StringBuilder(line);
                while (hasUnclosedQuote(logicalLine.toString(), quote)) {
                    String next = reader.readLine();
                    if (next == null) {
                        break;
                    }
                    logicalLine.append('\n').append(next);
                }
                String[] entries = splitLineBracketAware(logicalLine.toString(), delimiter, quote, escape);

                if (headerEntries == null) {
                    headerEntries = entries;
                    for (String h : entries) {
                        int colon = h.indexOf(':');
                        String name = colon >= 0 ? h.substring(0, colon).trim() : h.trim();
                        String type = colon >= 0 ? h.substring(colon + 1).trim().toLowerCase(Locale.ROOT) : "keyword";
                        schema.add(new ColumnSpec(name, type));
                    }
                } else {
                    if (entries.length != schema.size()) {
                        throw new IllegalArgumentException(
                            "Line " + lineNumber + ": expected " + schema.size() + " columns, got " + entries.length
                        );
                    }
                    Object[] row = new Object[entries.length];
                    for (int i = 0; i < entries.length; i++) {
                        row[i] = parseCell(entries[i], schema.get(i).type(), quote, escape);
                    }
                    rows.add(row);
                }
                lineNumber++;
            }

            if (schema.isEmpty()) {
                throw new IllegalArgumentException("CSV has no header");
            }
        }

        return new CsvFixtureResult(schema, rows);
    }

    /**
     * RFC-4180-style: a {@code "} only opens quoting at field start (after {@code ,} or line-start, optionally
     * preceded by whitespace) and is ignored inside {@code [..]} MVC cells. Stray {@code "} chars in unquoted
     * cells are literal bytes and must not cause multi-line gluing — kept consistent with
     * {@link CsvFormatReader} so fixture parsing matches runtime parsing.
     */
    private static boolean hasUnclosedQuote(String s, char quote) {
        boolean inQuotes = false;
        int bracketDepth = 0;
        boolean fieldHasNonWhitespace = false;
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (inQuotes) {
                if (c == quote) {
                    if (i + 1 < s.length() && s.charAt(i + 1) == quote) {
                        i++;
                        continue;
                    }
                    inQuotes = false;
                }
                continue;
            }
            if (bracketDepth > 0) {
                if (c == '[') {
                    bracketDepth++;
                } else if (c == ']') {
                    bracketDepth--;
                }
                continue;
            }
            if (c == ',') {
                fieldHasNonWhitespace = false;
                continue;
            }
            if (c == quote && fieldHasNonWhitespace == false) {
                inQuotes = true;
                continue;
            }
            if (c == '[' && fieldHasNonWhitespace == false) {
                // No hasMvcBracketClose check here: see CsvFormatReader.CsvBatchIterator.hasUnclosedQuote
                // for the rationale — unconditional bracket entry is safe and avoids false multi-line gluing.
                bracketDepth = 1;
                continue;
            }
            if (Character.isWhitespace(c) == false) {
                fieldHasNonWhitespace = true;
            }
        }
        return inQuotes;
    }

    /** Same as {@link CsvFormatReader}: whitespace-only prefix still allows bracket MVC to open at {@code [}. */
    private static boolean isWhitespaceOnlyFieldPrefix(StringBuilder current) {
        for (int k = 0; k < current.length(); k++) {
            if (Character.isWhitespace(current.charAt(k)) == false) {
                return false;
            }
        }
        return true;
    }

    /**
     * Whether {@code line} starting at {@code openBracketIndex} contains a balanced bracket suffix that closes the
     * MVC cell. Only {@code [} and {@code ]} adjust depth — quote/escape/delimiter characters inside the bracket
     * cell are treated as literal data, matching the splitter's {@code bracketDepth > 0} branch.
     */
    private static boolean hasMvcBracketClose(String line, int openBracketIndex) {
        if (openBracketIndex < 0 || openBracketIndex >= line.length() || line.charAt(openBracketIndex) != '[') {
            return false;
        }
        int depth = 0;
        for (int j = openBracketIndex; j < line.length(); j++) {
            char c = line.charAt(j);
            if (c == '[') {
                depth++;
            } else if (c == ']') {
                depth--;
                if (depth == 0) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Splits a CSV line by delimiter, treating quoted fields and {@code [..,..,..]} as single cells.
     */
    private static String[] splitLineBracketAware(String line, char delim, char quote, char esc) {
        List<String> entries = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        boolean inQuotes = false;
        int bracketDepth = 0;
        int i = 0;
        while (i < line.length()) {
            char c = line.charAt(i);
            if (inQuotes) {
                if (c == quote) {
                    if (i + 1 < line.length() && line.charAt(i + 1) == quote) {
                        current.append(quote);
                        i += 2;
                        continue;
                    }
                    inQuotes = false;
                } else if (c == esc && i + 1 < line.length() && line.charAt(i + 1) == delim) {
                    current.append(delim);
                    i += 2;
                    continue;
                } else {
                    current.append(c);
                }
                i++;
            } else if (bracketDepth > 0) {
                // See {@link CsvFormatReader} for the rationale: keep accumulating after the cell closes,
                // so a field like `[37] Title` stays a single field instead of producing a phantom column.
                current.append(c);
                if (c == '[') {
                    bracketDepth++;
                } else if (c == ']') {
                    bracketDepth--;
                }
                i++;
            } else if (c == quote && (current.length() == 0 || isWhitespaceOnlyFieldPrefix(current))) {
                inQuotes = true;
                i++;
            } else if (c == '[' && (current.length() == 0 || isWhitespaceOnlyFieldPrefix(current))) {
                if (hasMvcBracketClose(line, i)) {
                    bracketDepth = 1;
                }
                current.append(c);
                i++;
            } else if (c == delim) {
                if (i > 0 && line.charAt(i - 1) == esc) {
                    current.append(c);
                } else {
                    entries.add(current.toString().trim());
                    current = new StringBuilder();
                }
                i++;
            } else {
                current.append(c);
                i++;
            }
        }
        if (inQuotes) {
            throw new IllegalArgumentException("Unclosed quoted field in line [" + line + "]");
        }
        if (bracketDepth > 0) {
            throw new IllegalArgumentException("Unclosed bracket cell in line [" + line + "]");
        }
        if (current.length() > 0) {
            entries.add(current.toString().trim());
        }
        // Trailing delimiter (RFC 4180): one more empty field after the last comma, unless escaped as \,
        // No inQuotes guard needed: if we reach here with an unclosed quote the throw above fires first.
        if (line.length() > 0 && line.charAt(line.length() - 1) == delim) {
            int last = line.length() - 1;
            if (last == 0 || line.charAt(last - 1) != esc) {
                entries.add("");
            }
        }
        return entries.toArray(String[]::new);
    }

    private static Object parseCell(String value, String type, char quote, char esc) {
        if (value == null || (value = value.trim()).isEmpty() || value.equalsIgnoreCase("null")) {
            return null;
        }
        if (value.startsWith("[") && value.endsWith("]")) {
            return parseMultiValue(value, type, quote, esc);
        }
        return parseScalar(value, type, quote);
    }

    private static Object parseMultiValue(String value, String type, char quote, char esc) {
        String content = value.substring(1, value.length() - 1).trim();
        if (content.isEmpty()) {
            return null;
        }
        List<String> parts = splitBracketContent(content, quote, esc);
        List<Object> result = new ArrayList<>(parts.size());
        for (String part : parts) {
            Object elem = parseScalar(part, type, quote);
            if (elem != null) {
                result.add(elem);
            }
        }
        return result.isEmpty() ? null : result;
    }

    private static List<String> splitBracketContent(String content, char quote, char esc) {
        List<String> result = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        boolean inQuotes = false;
        int i = 0;
        while (i < content.length()) {
            char c = content.charAt(i);
            if (c == quote) {
                if (inQuotes) {
                    if (i + 1 < content.length() && content.charAt(i + 1) == quote) {
                        current.append(quote);
                        i += 2;
                        continue;
                    }
                    inQuotes = false;
                } else {
                    inQuotes = true;
                }
                i++;
            } else if (c == ',' && inQuotes == false) {
                result.add(current.toString().trim());
                current = new StringBuilder();
                i++;
            } else if (c == esc && inQuotes == false && i + 1 < content.length() && content.charAt(i + 1) == ',') {
                current.append(',');
                i += 2;
            } else {
                current.append(c);
                i++;
            }
        }
        result.add(current.toString().trim());
        return result;
    }

    private static Object parseScalar(String value, String type, char quote) {
        if (value == null || (value = value.trim()).isEmpty() || value.equalsIgnoreCase("null")) {
            return null;
        }
        value = unquoteElement(value, quote);
        if (value.isEmpty()) {
            return null;
        }
        return switch (type) {
            case "integer", "short", "byte" -> tryParseInt(value);
            case "long" -> tryParseLong(value);
            case "double", "scaled_float", "float", "half_float" -> tryParseDouble(value);
            case "boolean", "bool" -> tryParseBoolean(value);
            case "date", "datetime", "dt" -> tryParseDatetime(value);
            case "ip" -> value;
            case "null", "n" -> null;
            default -> value; // keyword, text, string, etc.
        };
    }

    private static String unquoteElement(String value, char quote) {
        if (value.length() >= 2 && value.charAt(0) == quote && value.charAt(value.length() - 1) == quote) {
            String inner = value.substring(1, value.length() - 1);
            return inner.replace(String.valueOf(quote) + quote, String.valueOf(quote));
        }
        return value;
    }

    private static Integer tryParseInt(String value) {
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException e) {
            return null;
        }
    }

    private static Long tryParseLong(String value) {
        try {
            return Long.parseLong(value);
        } catch (NumberFormatException e) {
            return null;
        }
    }

    private static Double tryParseDouble(String value) {
        try {
            return Double.parseDouble(value);
        } catch (NumberFormatException e) {
            return null;
        }
    }

    private static Boolean tryParseBoolean(String value) {
        String v = value.toLowerCase(Locale.ROOT);
        if ("true".equals(v) || "1".equals(v)) {
            return Boolean.TRUE;
        }
        if ("false".equals(v) || "0".equals(v)) {
            return Boolean.FALSE;
        }
        return null;
    }

    private static Long tryParseDatetime(String value) {
        if (looksNumeric(value)) {
            try {
                return Long.parseLong(value);
            } catch (NumberFormatException e) {
                // fall through
            }
        }
        try {
            return Instant.parse(value).toEpochMilli();
        } catch (Exception e) {
            return null;
        }
    }

    private static boolean looksNumeric(String value) {
        if (value == null || value.isEmpty()) {
            return false;
        }
        char c = value.charAt(0);
        return c == '-' || c == '+' || (c >= '0' && c <= '9');
    }

    public record ColumnSpec(String name, String type) {}

    public record CsvFixtureResult(List<ColumnSpec> schema, List<Object[]> rows) {}
}
