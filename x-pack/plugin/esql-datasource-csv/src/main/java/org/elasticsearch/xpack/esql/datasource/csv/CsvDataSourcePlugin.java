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

    @Override
    public Set<FormatSpec> formatSpecs() {
        return Set.of(FormatSpec.of("csv", ".csv"), FormatSpec.of("tsv", ".tsv"));
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
