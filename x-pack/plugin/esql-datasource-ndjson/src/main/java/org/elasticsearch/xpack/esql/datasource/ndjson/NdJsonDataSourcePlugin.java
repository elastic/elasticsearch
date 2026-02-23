/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.ndjson;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xpack.esql.datasources.spi.DataSourcePlugin;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReaderFactory;

import java.util.Map;
import java.util.Set;

/**
 * Data source plugin that provides NDJSON format support for ESQL external data sources.
 * <p>
 * The NDJSON reader performs schema inference from the first 100 non-empty lines,
 * resolves type conflicts to KEYWORD, marks nested objects/arrays as UNSUPPORTED,
 * and ignores malformed lines while logging warnings with their line numbers.
 */
public class NdJsonDataSourcePlugin extends Plugin implements DataSourcePlugin {

    @Override
    public Set<String> supportedFormats() {
        return Set.of("ndjson");
    }

    @Override
    public Set<String> supportedExtensions() {
        return Set.of(".ndjson", ".jsonl");
    }

    @Override
    public Map<String, FormatReaderFactory> formatReaders(Settings settings) {
        return Map.of("ndjson", (s, blockFactory) -> new NdJsonFormatReader(blockFactory));
    }
}
