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

import java.util.Map;

/**
 * Data source plugin that provides CSV format support for ESQL external data sources.
 *
 * <p>This plugin provides:
 * <ul>
 *   <li>CSV format reader for reading CSV files from any storage provider</li>
 * </ul>
 *
 * <p>The CSV format reader uses Jackson's CSV parser for robust CSV parsing with
 * proper quote and escape handling. It supports:
 * <ul>
 *   <li>Schema discovery from CSV file headers (column_name:type_name format)</li>
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
    public Map<String, FormatReaderFactory> formatReaders(Settings settings) {
        return Map.of("csv", (s, blockFactory) -> new CsvFormatReader(blockFactory));
    }
}
