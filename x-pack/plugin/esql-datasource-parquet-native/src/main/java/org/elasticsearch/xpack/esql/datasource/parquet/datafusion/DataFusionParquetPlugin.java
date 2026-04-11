/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.parquet.datafusion;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xpack.esql.datasources.spi.DataSourcePlugin;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReaderFactory;
import org.elasticsearch.xpack.esql.datasources.spi.FormatSpec;

import java.util.Map;
import java.util.Set;

/**
 * Data source plugin providing a DataFusion-backed native Parquet reader.
 * <p>
 * Registers for the same "parquet" format as the Java-based ParquetDataSourcePlugin.
 * Only one should be active at runtime, controlled by the toggle in EsqlPlugin.
 */
public class DataFusionParquetPlugin extends Plugin implements DataSourcePlugin {

    @Override
    public Set<FormatSpec> formatSpecs() {
        return Set.of(FormatSpec.of("parquet", ".parquet"));
    }

    @Override
    public Map<String, FormatReaderFactory> formatReaders(Settings settings) {
        return Map.of("parquet", (s, blockFactory) -> new DataFusionFormatReader(blockFactory));
    }
}
