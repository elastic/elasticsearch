/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.lakehouse.spi;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xpack.esql.datasource.lakehouse.LakehouseRegistry;

import java.util.Map;

/**
 * Extension point for format reader implementations.
 *
 * <p>Plugins implementing this interface provide file format reading for
 * lakehouse data sources. Each reader factory is keyed by format name
 * (e.g., "parquet", "csv", "orc", "avro").
 *
 * <p>Format plugins are lakehouse infrastructure — they are discovered by
 * {@link LakehouseRegistry} and used by {@link LakehouseDataSource}
 * implementations. The core {@link org.elasticsearch.xpack.esql.datasource.spi.DataSource}
 * SPI knows nothing about format plugins.
 *
 * @see FormatReader
 * @see FormatReaderFactory
 * @see LakehouseRegistry
 */
public interface FormatPlugin {

    /**
     * Returns factories for creating {@link FormatReader} instances, keyed
     * by format name.
     *
     * <p>The key is the format identifier (e.g., "parquet", "csv", "orc", "avro").
     *
     * @param settings the node settings
     * @return map of format name to format reader factory
     */
    Map<String, FormatReaderFactory> formatReaders(Settings settings);
}
