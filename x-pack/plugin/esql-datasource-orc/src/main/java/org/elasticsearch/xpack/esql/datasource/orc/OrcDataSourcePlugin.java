/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.orc;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xpack.esql.datasources.spi.DataSourcePlugin;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReaderFactory;

import java.util.Map;
import java.util.Set;

/**
 * Data source plugin that provides Apache ORC format support for ESQL external data sources.
 *
 * <p>This plugin provides an ORC format reader for reading ORC files from any storage provider.
 * ORC (Optimized Row Columnar) is a columnar storage format optimized for analytics workloads,
 * providing efficient compression and encoding schemes with built-in indexing.
 *
 * <p>Heavy dependencies (ORC, Hadoop) are isolated in this module to avoid jar hell issues
 * in the core ESQL plugin.
 */
public class OrcDataSourcePlugin extends Plugin implements DataSourcePlugin {

    @Override
    public Set<String> supportedFormats() {
        return Set.of("orc");
    }

    @Override
    public Set<String> supportedExtensions() {
        return Set.of(".orc");
    }

    @Override
    public Map<String, FormatReaderFactory> formatReaders(Settings settings) {
        return Map.of("orc", (s, blockFactory) -> new OrcFormatReader(blockFactory));
    }
}
