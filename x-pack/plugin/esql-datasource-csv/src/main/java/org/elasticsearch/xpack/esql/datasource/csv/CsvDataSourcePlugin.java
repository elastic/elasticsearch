/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.csv;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.Setting;
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
 * <p>Eligible reads parse logical records straight into typed {@code Block} builders (the
 * direct-to-block path, gated by {@link #CSV_DIRECT_BLOCK_ENABLED}); all other reads fall back to
 * Jackson's CSV parser. Both paths produce equivalent output and support:
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
        "mode",
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
        "trim_spaces",
        "schema_sample_size"
    );

    /**
     * Node-level toggle for the direct-to-block CSV/TSV read path. When enabled (default), eligible
     * reads (plain unquoted, or RFC 4180 quoted with or without backslash escapes) parse logical
     * records straight into typed {@code Block} builders, skipping the Jackson tokenizer and its
     * per-cell {@code String} allocation. Disabling it forces every read back onto the Jackson bulk
     * path, which is byte-for-byte equivalent: a safety valve if the direct path is ever suspected of
     * a parity regression.
     */
    public static final Setting<Boolean> CSV_DIRECT_BLOCK_ENABLED = Setting.boolSetting(
        "esql.csv.direct_block.enabled",
        true,
        Setting.Property.NodeScope
    );

    @Override
    public Set<FormatSpec> formatSpecs() {
        return Set.of(FormatSpec.of("csv", ".csv", FORMAT_CONFIG_KEYS), FormatSpec.of("tsv", ".tsv", FORMAT_CONFIG_KEYS));
    }

    @Override
    public List<Setting<?>> getSettings() {
        return List.of(CSV_DIRECT_BLOCK_ENABLED);
    }

    @Override
    public Map<String, FormatReaderFactory> formatReaders(Settings settings) {
        return Map.of(
            "csv",
            (s, blockFactory) -> new CsvFormatReader(blockFactory, "csv", List.of(".csv")).withDirectBlockEnabled(
                CSV_DIRECT_BLOCK_ENABLED.get(s)
            ),
            "tsv",
            (s, blockFactory) -> new CsvFormatReader(blockFactory, CsvFormatOptions.TSV, "tsv", List.of(".tsv")).withDirectBlockEnabled(
                CSV_DIRECT_BLOCK_ENABLED.get(s)
            )
        );
    }

    @Override
    public List<NamedWriteableRegistry.Entry> getNamedWriteables() {
        // One entry serves both csv and tsv — the format (csv/tsv) is carried in CsvReaderStatus.format().
        return List.of(CsvReaderStatus.ENTRY);
    }
}
