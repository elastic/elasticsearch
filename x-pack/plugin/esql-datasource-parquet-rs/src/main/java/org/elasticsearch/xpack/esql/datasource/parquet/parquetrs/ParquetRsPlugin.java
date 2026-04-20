/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.parquet.parquetrs;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xpack.esql.datasources.spi.DataSourcePlugin;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReaderFactory;
import org.elasticsearch.xpack.esql.datasources.spi.FormatSpec;

import java.util.Map;
import java.util.Set;

/**
 * Data source plugin providing a parquet-rs backed native Parquet reader.
 */
public class ParquetRsPlugin extends Plugin implements DataSourcePlugin {

    @Override
    public Set<FormatSpec> formatSpecs() {
        return Set.of(FormatSpec.of("parquet", ".parquet"));
    }

    @Override
    public Map<String, FormatReaderFactory> formatReaders(Settings settings) {
        FormatReaderFactory factory = (s, blockFactory) -> new ParquetRsFormatReader(blockFactory);
        return Map.of("parquet", factory);
    }
}
