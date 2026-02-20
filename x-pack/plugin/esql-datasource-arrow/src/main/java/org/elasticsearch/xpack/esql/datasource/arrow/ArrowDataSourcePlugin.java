/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.arrow;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xpack.esql.datasources.spi.DataSourcePlugin;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReaderFactory;

import java.util.Map;

/**
 * Data source plugin that provides Arrow IPC format support for ESQL external data sources.
 *
 * <p>This plugin provides:
 * <ul>
 *   <li>Arrow IPC format reader for reading Arrow streaming files from any storage provider</li>
 * </ul>
 *
 * <p>The Arrow IPC format reader uses Apache Arrow's ArrowStreamReader to read
 * Arrow IPC streaming format files. It supports:
 * <ul>
 *   <li>Schema discovery from the Arrow stream header</li>
 *   <li>Column projection for efficient reads</li>
 *   <li>Batch reading with configurable batch sizes</li>
 *   <li>Direct conversion to ESQL Page format via ArrowToBlockConverter</li>
 * </ul>
 *
 * <p>Arrow dependencies are isolated in this module to avoid jar hell issues
 * in the core ESQL plugin.
 */
public class ArrowDataSourcePlugin extends Plugin implements DataSourcePlugin {

    @Override
    public Map<String, FormatReaderFactory> formatReaders(Settings settings) {
        return Map.of("arrow", (s, blockFactory) -> new ArrowFormatReader(blockFactory));
    }
}
