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
import org.elasticsearch.xpack.esql.datasources.spi.FormatSpec;

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

    /**
     * Per-dataset configuration keys accepted by the NDJSON format reader.
     * Must stay in sync with {@code NdJsonFormatReader.RECOGNIZED_KEYS}; verified
     * by {@code NdJsonFormatReaderRecognizedKeysTests.testFormatSpecConfigKeysMatchRecognizedKeys}.
     */
    static final Set<String> FORMAT_CONFIG_KEYS = Set.of("schema_sample_size", "segment_size");

    @Override
    public Set<FormatSpec> formatSpecs() {
        return Set.of(new FormatSpec("ndjson", Set.of(".ndjson", ".jsonl", ".json"), FORMAT_CONFIG_KEYS));
    }

    @Override
    public Map<String, FormatReaderFactory> formatReaders(Settings settings) {
        return Map.of("ndjson", NdJsonFormatReader::new);
    }
}
