/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License, v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.todo;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * Reads and writes the to-do report. Rows are sorted by module, file, and line so the files stay stable.
 */
public final class TodoReportWriter {

    static final List<String> CSV_COLUMNS = List.of(
        "marker",
        "message",
        "assignee",
        "file",
        "module",
        "area",
        "sourceSet",
        "line",
        "authorName",
        "authorEmail",
        "authoredAt",
        "authoredDate",
        "commit",
        "blameStatus"
    );

    private static final ObjectMapper MAPPER = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);
    private static final Comparator<TodoEntry> ORDER = Comparator.comparing(TodoEntry::module)
        .thenComparing(TodoEntry::file)
        .thenComparingInt(TodoEntry::line);

    private TodoReportWriter() {}

    /** Returns {@code items} ordered for the report. */
    public static List<TodoEntry> sorted(List<TodoEntry> items) {
        return items.stream().sorted(ORDER).toList();
    }

    /** Reads one module report. */
    public static TodoReportFile read(Path json) {
        try {
            return MAPPER.readValue(json.toFile(), TodoReportFile.class);
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to read TODO report " + json, e);
        }
    }

    /** Writes {@code items} as indented JSON. Parent directories are created. */
    public static void writeJson(Path json, List<TodoEntry> items) {
        try {
            Files.createDirectories(json.getParent());
            MAPPER.writeValue(json.toFile(), new TodoReportFile(sorted(items)));
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to write TODO report " + json, e);
        }
    }

    /** Writes the same rows as CSV, with a header, using RFC 4180 quoting. */
    public static void writeCsv(Path csv, List<TodoEntry> items) {
        try {
            Files.createDirectories(csv.getParent());
            Files.writeString(csv, toCsv(sorted(items)), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to write TODO report " + csv, e);
        }
    }

    static String toCsv(List<TodoEntry> items) {
        StringBuilder csv = new StringBuilder();
        csv.append(String.join(",", CSV_COLUMNS)).append('\n');
        for (TodoEntry item : items) {
            csv.append(cell(item.marker()))
                .append(',')
                .append(cell(item.message()))
                .append(',')
                .append(cell(item.assignee()))
                .append(',')
                .append(cell(item.file()))
                .append(',')
                .append(cell(item.module()))
                .append(',')
                .append(cell(item.area()))
                .append(',')
                .append(cell(item.sourceSet()))
                .append(',')
                .append(item.line())
                .append(',')
                .append(cell(item.authorName()))
                .append(',')
                .append(cell(item.authorEmail()))
                .append(',')
                .append(cell(item.authoredAt()))
                .append(',')
                .append(cell(item.authoredDate()))
                .append(',')
                .append(cell(item.commit()))
                .append(',')
                .append(cell(item.blameStatus()))
                .append('\n');
        }
        return csv.toString();
    }

    private static String cell(String value) {
        if (value == null) {
            return "";
        }
        if (value.indexOf(',') >= 0 || value.indexOf('"') >= 0 || value.indexOf('\n') >= 0 || value.indexOf('\r') >= 0) {
            return '"' + value.replace("\"", "\"\"") + '"';
        }
        return value;
    }

    /** Concatenates module reports and returns the sorted rows. */
    public static List<TodoEntry> merge(List<Path> reports) {
        List<TodoEntry> items = new ArrayList<>();
        for (Path report : reports) {
            items.addAll(read(report).items());
        }
        return sorted(items);
    }
}
