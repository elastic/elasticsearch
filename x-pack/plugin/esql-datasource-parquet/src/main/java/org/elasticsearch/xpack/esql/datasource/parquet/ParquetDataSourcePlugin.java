/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.parquet;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xpack.esql.datasources.spi.DataSourcePlugin;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReaderFactory;

import java.util.Map;

/**
 * Data source plugin that provides Parquet format support for ESQL external data sources.
 *
 * <p>This plugin provides:
 * <ul>
 *   <li>Parquet format reader for reading Parquet files from any storage provider</li>
 * </ul>
 *
 * <p>The Parquet format reader uses Apache Parquet's native ParquetFileReader with
 * Iceberg's schema conversion utilities. It supports:
 * <ul>
 *   <li>Schema discovery from Parquet file metadata</li>
 *   <li>Column projection for efficient reads</li>
 *   <li>Batch reading with configurable batch sizes</li>
 *   <li>Direct conversion to ESQL Page format</li>
 * </ul>
 *
 * <p>Heavy dependencies (Parquet, Hadoop, Iceberg, Arrow) are isolated in this module
 * to avoid jar hell issues in the core ESQL plugin.
 */
public class ParquetDataSourcePlugin extends Plugin implements DataSourcePlugin {

    @Override
    public Map<String, FormatReaderFactory> formatReaders(Settings settings) {
        return Map.of("parquet", (s, blockFactory) -> new ParquetFormatReader(blockFactory));
    }
}
