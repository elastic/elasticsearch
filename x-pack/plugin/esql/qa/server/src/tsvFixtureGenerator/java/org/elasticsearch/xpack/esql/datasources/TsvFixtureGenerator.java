/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.xpack.esql.datasource.csv.CsvFixtureParser;
import org.elasticsearch.xpack.esql.datasource.csv.CsvFixtureParser.ColumnSpec;
import org.elasticsearch.xpack.esql.datasource.csv.CsvFixtureParser.CsvFixtureResult;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;

/**
 * Build-time generator: converts a {@link CsvFixtureParser}-compatible CSV fixture to TSV.
 * <p>
 * Uses {@link CsvFixtureParser} for bracket-aware parsing (same multi-value semantics as
 * {@link org.elasticsearch.xpack.esql.datasource.csv.CsvFormatReader} with {@code multi_value_syntax: brackets}).
 * Output uses TAB as the field delimiter; list cells are written as {@code [a,b,c]} with commas inside the brackets.
 */
public final class TsvFixtureGenerator {

    private static final char TAB = '\t';

    private TsvFixtureGenerator() {}

    @SuppressForbidden(reason = "main method for Gradle JavaExec task needs System.err and Path.of")
    public static void main(String[] args) throws IOException {
        if (args.length != 2) {
            System.err.println("Usage: TsvFixtureGenerator <source-csv-path> <output-tsv-path>");
            System.exit(1);
        }
        Path sourcePath = Path.of(args[0]);
        Path outputPath = Path.of(args[1]);
        if (Files.exists(sourcePath) == false) {
            throw new IOException("Source CSV not found: " + sourcePath);
        }
        byte[] tsv = generateFromCsv(sourcePath);
        Files.createDirectories(outputPath.getParent());
        Files.write(outputPath, tsv);
        System.out.println("Generated TSV fixture: " + outputPath);
    }

    static byte[] generateFromCsv(Path sourcePath) throws IOException {
        CsvFixtureResult parsed = CsvFixtureParser.parseCsvFile(sourcePath);
        List<ColumnSpec> schema = parsed.schema();
        List<Object[]> rows = parsed.rows();

        boolean[] isListColumn = new boolean[schema.size()];
        for (int c = 0; c < schema.size(); c++) {
            for (Object[] row : rows) {
                if (c < row.length && row[c] instanceof List<?>) {
                    isListColumn[c] = true;
                    break;
                }
            }
        }

        StringBuilder out = new StringBuilder(rows.size() * 256);
        appendHeader(out, schema);
        for (Object[] row : rows) {
            appendRow(out, schema, isListColumn, row);
        }
        return out.toString().getBytes(StandardCharsets.UTF_8);
    }

    private static void appendHeader(StringBuilder out, List<ColumnSpec> schema) {
        for (int i = 0; i < schema.size(); i++) {
            if (i > 0) {
                out.append(TAB);
            }
            ColumnSpec col = schema.get(i);
            out.append(col.name()).append(':').append(col.type());
        }
        out.append('\n');
    }

    private static void appendRow(StringBuilder out, List<ColumnSpec> schema, boolean[] isListColumn, Object[] row) {
        for (int i = 0; i < schema.size(); i++) {
            if (i > 0) {
                out.append(TAB);
            }
            Object value = i < row.length ? row[i] : null;
            out.append(formatCell(schema.get(i), isListColumn[i], value));
        }
        out.append('\n');
    }

    private static String formatCell(ColumnSpec spec, boolean isListColumn, Object value) {
        String type = spec.type();
        if (isListColumn) {
            if (value == null) {
                return "[]";
            }
            if (value instanceof List<?> list) {
                if (list.isEmpty()) {
                    return "[]";
                }
                return formatBracketList(type, list);
            }
            // Same column can mix bracket lists and plain scalars (e.g. books.author).
            return formatScalar(type, value);
        }
        if (value == null) {
            return "";
        }
        return formatScalar(type, value);
    }

    private static String formatBracketList(String type, List<?> list) {
        StringBuilder sb = new StringBuilder("[");
        for (int i = 0; i < list.size(); i++) {
            if (i > 0) {
                sb.append(',');
            }
            sb.append(formatBracketElement(type, list.get(i)));
        }
        sb.append(']');
        return sb.toString();
    }

    private static String formatBracketElement(String type, Object value) {
        if (value == null) {
            return "";
        }
        String raw = formatScalarUnescaped(type, value);
        if (needsBracketQuotes(raw)) {
            return '"' + raw.replace("\"", "\"\"") + '"';
        }
        return raw;
    }

    private static boolean needsBracketQuotes(String s) {
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c == ',' || c == '"' || c == '[' || c == ']') {
                return true;
            }
        }
        return false;
    }

    private static String formatScalar(String type, Object value) {
        return escapeCellIfNeeded(formatScalarUnescaped(type, value));
    }

    /**
     * Scalar textual form without outer TSV quoting (TSV cells may still need quoting if they contain TAB / newline).
     */
    private static String formatScalarUnescaped(String type, Object value) {
        return switch (type) {
            case "integer", "short", "byte" -> String.valueOf(((Number) value).intValue());
            case "long" -> {
                if (value instanceof Long l) {
                    yield String.valueOf(l);
                }
                yield String.valueOf(((Number) value).longValue());
            }
            case "double", "scaled_float", "float", "half_float" -> Double.toString(((Number) value).doubleValue());
            case "boolean", "bool" -> Boolean.TRUE.equals(value) ? "true" : "false";
            case "date", "datetime", "dt" -> Instant.ofEpochMilli(((Number) value).longValue()).toString();
            default -> value.toString();
        };
    }

    private static String escapeCellIfNeeded(String cell) {
        if (cell.isEmpty()) {
            return "";
        }
        boolean needsQuotes = false;
        for (int i = 0; i < cell.length(); i++) {
            char c = cell.charAt(i);
            if (c == TAB || c == '\n' || c == '\r' || c == '"') {
                needsQuotes = true;
                break;
            }
        }
        if (needsQuotes == false) {
            return cell;
        }
        return '"' + cell.replace("\"", "\"\"") + '"';
    }
}
