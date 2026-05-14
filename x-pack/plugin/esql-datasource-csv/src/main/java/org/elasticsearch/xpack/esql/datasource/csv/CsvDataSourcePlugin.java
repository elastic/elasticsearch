/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.csv;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xpack.esql.datasources.spi.DataSourcePlugin;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReaderFactory;
import org.elasticsearch.xpack.esql.datasources.spi.FormatSpec;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Data source plugin that provides CSV and TSV format support for ESQL external data sources.
 *
 * <p>This plugin registers two format readers:
 * <ul>
 *   <li>{@code csv} — comma-delimited ({@code .csv} files)</li>
 *   <li>{@code tsv} — tab-delimited ({@code .tsv} files)</li>
 * </ul>
 *
 * <p>Both readers use Jackson's CSV parser for robust parsing with
 * proper quote and escape handling. They support:
 * <ul>
 *   <li>Schema discovery from file headers (column_name:type_name format)</li>
 *   <li>Column projection for efficient reads</li>
 *   <li>Batch reading with configurable batch sizes</li>
 *   <li>Direct conversion to ESQL Page format</li>
 * </ul>
 *
 * <p>The Jackson CSV dependency is isolated in this module to keep
 * the core ESQL plugin free of third-party format libraries.
 */
public class CsvDataSourcePlugin extends Plugin implements DataSourcePlugin {

    /**
     * Per-dataset configuration keys accepted by the CSV/TSV format reader.
     * Must stay in sync with {@code CsvFormatReader.RECOGNIZED_KEYS}; verified
     * by {@code CsvFormatReaderRecognizedKeysTests.testFormatSpecConfigKeysMatchRecognizedKeys}.
     *
     * <p>Note: {@code schema_sample_size} also appears as a base dataset field in
     * {@link org.elasticsearch.xpack.esql.datasources.spi.FileDataSourceValidator} —
     * this duplication is intentional to maintain symmetry with the reader's
     * {@code RECOGNIZED_KEYS}. At CRUD time, the base-field validation path handles it
     * (with range checking); it is not passed through as a raw format option.
     */
    static final Set<String> FORMAT_CONFIG_KEYS = Set.of(
        "delimiter",
        "quote",
        "escape",
        "comment",
        "null_value",
        "encoding",
        "datetime_format",
        "max_field_size",
        "multi_value_syntax",
        "header_row",
        "column_prefix",
        "schema_sample_size"
    );

    @Override
    public Set<FormatSpec> formatSpecs() {
        return Set.of(FormatSpec.of("csv", ".csv", FORMAT_CONFIG_KEYS), FormatSpec.of("tsv", ".tsv", FORMAT_CONFIG_KEYS));
    }

    @Override
    public Map<String, FormatReaderFactory> formatReaders(Settings settings) {
        return Map.of(
            "csv",
            (s, blockFactory) -> new CsvFormatReader(blockFactory, "csv", List.of(".csv")),
            "tsv",
            (s, blockFactory) -> new CsvFormatReader(blockFactory, CsvFormatOptions.TSV, "tsv", List.of(".tsv"))
        );
    }
}
