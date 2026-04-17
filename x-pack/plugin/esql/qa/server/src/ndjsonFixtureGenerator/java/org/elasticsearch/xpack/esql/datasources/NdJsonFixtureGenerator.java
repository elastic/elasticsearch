/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xpack.esql.datasource.csv.CsvFixtureParser;
import org.elasticsearch.xpack.esql.datasource.csv.CsvFixtureParser.ColumnSpec;
import org.elasticsearch.xpack.esql.datasource.csv.CsvFixtureParser.CsvFixtureResult;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.Locale;

/**
 * Build-time generator: converts a {@link CsvFixtureParser}-compatible CSV fixture to newline-delimited JSON (NDJSON).
 * <p>
 * The output is fully determined by the CSV header ({@code name:type} columns) and cell values:
 * <ul>
 *   <li>One JSON object per data row, with property names equal to column names (including dotted names such as
 *       {@code languages.long}).</li>
 *   <li>Properties with {@code null} values are omitted (including empty multi-value cells parsed as {@code null}).</li>
 *   <li>Keyword-like values are trimmed; if the result is empty, the property is omitted.</li>
 *   <li>Multi-value cells (lists from bracket syntax) are written as JSON arrays only when at least one element
 *       produces a concrete JSON value; elements that would encode as JSON {@code null} are skipped. If no elements
 *       remain, the property is omitted (never {@code []}), matching {@code NdJsonPageDecoder} which cannot close an
 *       empty multi-value position on {@code BytesRefBlock} builders.</li>
 *   <li>Date columns are written as ISO-8601 UTC strings (from epoch millis produced by the CSV parser).</li>
 * </ul>
 */
public final class NdJsonFixtureGenerator {

    private NdJsonFixtureGenerator() {}

    @SuppressForbidden(reason = "main method for Gradle JavaExec task needs System.err and Path.of")
    public static void main(String[] args) throws IOException {
        if (args.length != 2) {
            System.err.println("Usage: NdJsonFixtureGenerator <source-csv-path> <output-ndjson-path>");
            System.exit(1);
        }
        Path sourcePath = Path.of(args[0]);
        Path outputPath = Path.of(args[1]);
        if (Files.exists(sourcePath) == false) {
            throw new IOException("Source CSV not found: " + sourcePath);
        }
        byte[] ndjson = generateFromCsv(sourcePath);
        Files.createDirectories(outputPath.getParent());
        Files.write(outputPath, ndjson);
        System.out.println("Generated NDJSON fixture: " + outputPath);
    }

    public static byte[] generateFromCsv(Path sourcePath) throws IOException {
        CsvFixtureResult parsed = CsvFixtureParser.parseCsvFile(sourcePath);
        List<ColumnSpec> schema = parsed.schema();
        List<Object[]> rows = parsed.rows();
        StringBuilder out = new StringBuilder(rows.size() * 256);
        for (Object[] row : rows) {
            out.append(renderLine(schema, row));
            out.append('\n');
        }
        return out.toString().getBytes(StandardCharsets.UTF_8);
    }

    private static String renderLine(List<ColumnSpec> schema, Object[] row) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (XContentBuilder b = XContentFactory.jsonBuilder(baos)) {
            b.startObject();
            for (int i = 0; i < schema.size(); i++) {
                writeColumn(b, schema.get(i), row[i]);
            }
            b.endObject();
        }
        return baos.toString(StandardCharsets.UTF_8).trim();
    }

    private static void writeColumn(XContentBuilder b, ColumnSpec spec, Object value) throws IOException {
        if (value == null) {
            return;
        }
        String name = spec.name();
        String type = normalizeType(spec.type());
        if (value instanceof List<?> list) {
            if (list.isEmpty()) {
                return;
            }
            int renderable = 0;
            for (Object element : list) {
                if (rendersAsJsonArrayElement(type, element)) {
                    renderable++;
                }
            }
            if (renderable == 0) {
                return;
            }
            b.startArray(name);
            for (Object element : list) {
                if (rendersAsJsonArrayElement(type, element)) {
                    writeJsonArrayElement(b, type, element);
                }
            }
            b.endArray();
        } else {
            writeScalarField(b, name, type, value);
        }
    }

    private static String normalizeType(String rawType) {
        return rawType.trim().toLowerCase(Locale.ROOT);
    }

    private static void writeScalarField(XContentBuilder b, String name, String type, Object v) throws IOException {
        switch (type) {
            case "integer", "short", "byte" -> b.field(name, asInt(v));
            case "long" -> b.field(name, asLong(v));
            case "double", "float", "half_float", "scaled_float" -> b.field(name, asDouble(v));
            case "boolean", "bool" -> b.field(name, asBoolean(v));
            case "date", "datetime", "dt" -> b.field(name, formatDate(v));
            case "null", "n" -> {
                /* omit */
            }
            default -> writeStringField(b, name, v);
        }
    }

    private static void writeStringField(XContentBuilder b, String name, Object v) throws IOException {
        String s = ((String) v).trim();
        if (s.isEmpty() == false) {
            b.field(name, s);
        }
    }

    /**
     * Whether {@code element} becomes a non-null JSON token inside an array. NDJSON decoding does not support JSON
     * {@code null} inside multi-value arrays nor empty {@code []} arrays.
     */
    private static boolean rendersAsJsonArrayElement(String type, Object element) {
        if (element == null) {
            return false;
        }
        return switch (type) {
            case "integer", "short", "byte", "long", "double", "float", "half_float", "scaled_float", "boolean", "bool", "date", "datetime",
                "dt" -> true;
            case "null", "n" -> false;
            default -> ((String) element).trim().isEmpty() == false;
        };
    }

    /** Writes a single concrete JSON value for an array element (never JSON null). */
    private static void writeJsonArrayElement(XContentBuilder b, String type, Object v) throws IOException {
        switch (type) {
            case "integer", "short", "byte" -> b.value(asInt(v));
            case "long" -> b.value(asLong(v));
            case "double", "float", "half_float", "scaled_float" -> b.value(asDouble(v));
            case "boolean", "bool" -> b.value(asBoolean(v));
            case "date", "datetime", "dt" -> b.value(formatDate(v));
            case "null", "n" -> throw new IllegalStateException("Unexpected null-typed cell in list");
            default -> b.value(((String) v).trim());
        }
    }

    private static String formatDate(Object v) {
        if (v instanceof Long millis) {
            return Instant.ofEpochMilli(millis).toString();
        }
        throw new IllegalArgumentException("Expected epoch millis for date, got " + v);
    }

    private static int asInt(Object v) {
        if (v instanceof Integer i) {
            return i;
        }
        if (v instanceof Long l) {
            return Math.toIntExact(l);
        }
        throw new IllegalArgumentException("Expected integer, got " + v);
    }

    private static long asLong(Object v) {
        if (v instanceof Long l) {
            return l;
        }
        if (v instanceof Integer i) {
            return i.longValue();
        }
        throw new IllegalArgumentException("Expected long, got " + v);
    }

    private static double asDouble(Object v) {
        if (v instanceof Double d) {
            return d;
        }
        if (v instanceof Float f) {
            return f.doubleValue();
        }
        if (v instanceof Number n) {
            return n.doubleValue();
        }
        throw new IllegalArgumentException("Expected number, got " + v);
    }

    private static boolean asBoolean(Object v) {
        if (v instanceof Boolean b) {
            return b;
        }
        throw new IllegalArgumentException("Expected boolean, got " + v);
    }
}
