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
 * Extension point for table catalog implementations.
 *
 * <p>Plugins implementing this interface provide table-level metadata
 * for lakehouse data sources. Each catalog factory is keyed by catalog type
 * (e.g., "iceberg", "delta", "hudi").
 *
 * <p>Catalog plugins are lakehouse infrastructure — they are discovered by
 * {@link LakehouseRegistry} and used by {@link LakehouseDataSource}
 * implementations. The core {@link org.elasticsearch.xpack.esql.datasource.spi.DataSource}
 * SPI knows nothing about catalog plugins.
 *
 * @see TableCatalog
 * @see TableCatalogFactory
 * @see LakehouseRegistry
 */
public interface CatalogPlugin {

    /**
     * Returns factories for creating {@link TableCatalog} instances, keyed
     * by catalog type.
     *
     * <p>The key is the catalog type identifier (e.g., "iceberg", "delta", "hudi").
     *
     * @param settings the node settings
     * @return map of catalog type to table catalog factory
     */
    Map<String, TableCatalogFactory> tableCatalogs(Settings settings);
}
