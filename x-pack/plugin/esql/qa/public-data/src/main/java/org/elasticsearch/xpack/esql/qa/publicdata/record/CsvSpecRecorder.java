/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.publicdata.record;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

/**
 * Renders actual ES|QL results in csv-spec expected-table syntax and writes them under the run's
 * output directory. A mismatch diagnostic only ({@code -Dtests.public_data.record=true}): it
 * captures what ES|QL actually returned so the stop-and-ask gate can present it next to the
 * oracle's answer. It is never a source of expected values — those come from the oracle at
 * authoring time — and its output is never checked in.
 */
public class CsvSpecRecorder {

    private final Path outputDir;

    public CsvSpecRecorder(Path outputDir) {
        this.outputDir = outputDir;
    }

    /**
     * Renders {@code columns} (each a map with {@code name}/{@code type}) and {@code values} the
     * way a csv-spec expected table is written: a {@code name:type | ...} header, then one
     * pipe-separated line per row.
     */
    public static String renderTable(List<Map<String, String>> columns, List<List<Object>> values) {
        StringBuilder table = new StringBuilder();
        for (int i = 0; i < columns.size(); i++) {
            if (i > 0) {
                table.append(" | ");
            }
            table.append(columns.get(i).get("name")).append(':').append(columns.get(i).get("type"));
        }
        table.append('\n');
        for (List<Object> row : values) {
            for (int i = 0; i < row.size(); i++) {
                if (i > 0) {
                    table.append(" | ");
                }
                table.append(renderValue(row.get(i)));
            }
            table.append('\n');
        }
        return table.toString();
    }

    /** Renders a single value the way csv-spec expected tables express it. */
    static String renderValue(Object value) {
        if (value == null) {
            return "null";
        }
        if (value instanceof List<?> multiValue) {
            StringBuilder rendered = new StringBuilder("[");
            for (int i = 0; i < multiValue.size(); i++) {
                if (i > 0) {
                    rendered.append(", ");
                }
                rendered.append(renderValue(multiValue.get(i)));
            }
            return rendered.append(']').toString();
        }
        return value.toString();
    }

    /** Writes one captured fragment as {@code <spec>.<test>.<variant>.recorded}. */
    public void record(String specFileName, RecordedFragment fragment) {
        try {
            Files.createDirectories(outputDir);
            Path file = outputDir.resolve(specFileName + "." + fragment.testName() + "." + fragment.variantLabel() + ".recorded");
            StringBuilder content = new StringBuilder();
            content.append("// ACTUAL results captured for diagnosis; NEVER a source of expected values.\n");
            content.append("// test: ").append(fragment.testName()).append('\n');
            content.append("// variant: ").append(fragment.variantLabel()).append('\n');
            content.append(fragment.renderedTable());
            Files.writeString(file, content.toString(), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new UncheckedIOException("failed to write recorded fragment for " + fragment.testName(), e);
        }
    }
}
