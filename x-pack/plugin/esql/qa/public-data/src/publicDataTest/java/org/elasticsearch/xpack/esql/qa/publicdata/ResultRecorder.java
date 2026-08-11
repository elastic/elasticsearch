/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.qa.publicdata;

import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xpack.esql.qa.publicdata.catalog.SourceVariant;
import org.supercsv.io.CsvListWriter;
import org.supercsv.prefs.CsvPreference;

import java.io.IOException;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

/**
 * Opt-in ({@code -Dtests.public_data.record=true}) writer for csv-spec-formatted expected-result
 * fragments (plan section 4). Never mutates a checked-in spec: it writes one fragment file per
 * (source, variant, test) under {@code build/public-data-results/}, for a human to diff against and
 * paste into the real spec after independently confirming the answer against DuckDB/ClickHouse
 * (plan section 6) -- the Elasticsearch run recorded here is never, on its own, treated as ground truth.
 * <p>
 * Uses the exact {@link CsvPreference} ({@code quote='"'}, {@code delimiter='|'}, {@code eol="\r\n"}) that
 * {@code CsvTestUtils#loadCsvSpecValues} reads with, so a recorded fragment round-trips byte-for-byte
 * through the same parser the suite uses to check it in.
 */
public final class ResultRecorder {

    private static final Logger logger = LogManager.getLogger(ResultRecorder.class);
    private static final CsvPreference CSV_SPEC_PREFERENCES = new CsvPreference.Builder('"', '|', "\r\n").build();

    private ResultRecorder() {}

    /**
     * Writes the header (from {@code columns}, each {@code name:type}) and every row of {@code values} as
     * a csv-spec expected-results fragment, terminated by a lone {@code ;} line, to
     * {@code build/public-data-results/<sourceId>/<variantId>/<testName>.csv-spec-fragment} under
     * {@code buildDir}.
     */
    public static void record(
        Path buildDir,
        String sourceId,
        SourceVariant variant,
        String testName,
        List<Map<String, String>> columns,
        List<List<Object>> values
    ) throws IOException {
        Path dir = buildDir.resolve("public-data-results").resolve(sourceId).resolve(variant.id());
        Files.createDirectories(dir);
        Path out = dir.resolve(testName + ".csv-spec-fragment");
        try (StringWriter sw = new StringWriter(); CsvListWriter writer = new CsvListWriter(sw, CSV_SPEC_PREFERENCES)) {
            List<String> header = columns.stream().map(c -> c.get("name") + ":" + c.get("type")).toList();
            writer.write(header);
            for (List<Object> row : values) {
                writer.write(row.stream().map(ResultRecorder::formatCell).toList());
            }
            writer.flush();
            Files.writeString(out, sw.toString() + ";\r\n", StandardCharsets.UTF_8);
        }
        logger.info("Recorded [{}] rows for [{}]/[{}]/[{}] to [{}]", values.size(), sourceId, variant.id(), testName, out);
    }

    /**
     * Formats one response cell back into csv-spec cell text: {@code null} as the literal {@code null},
     * a {@link List} (a multi-valued column) as {@code [v1,v2,...]} with internal commas backslash-escaped
     * (mirroring {@code CsvTestUtils.COMMA_ESCAPING_REGEX}), everything else via {@link String#valueOf}.
     * {@link CsvListWriter} applies its own quoting on top of this (for the {@code |}/{@code "} delimiter
     * and quote characters), exactly as the reader expects.
     */
    private static String formatCell(Object value) {
        if (value == null) {
            return "null";
        }
        if (value instanceof List<?> multiValue) {
            StringBuilder sb = new StringBuilder("[");
            for (int i = 0; i < multiValue.size(); i++) {
                if (i > 0) {
                    sb.append(',');
                }
                sb.append(String.valueOf(multiValue.get(i)).replace(",", "\\,"));
            }
            return sb.append(']').toString();
        }
        return String.valueOf(value);
    }
}
