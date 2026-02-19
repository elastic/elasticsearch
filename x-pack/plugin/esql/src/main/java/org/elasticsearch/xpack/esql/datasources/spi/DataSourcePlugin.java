/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.Settings;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;

/**
 * Extension point for data source implementations.
 * Plugins implementing this interface will be discovered by ESQL at startup
 * via Elasticsearch's plugin discovery mechanism.
 *
 * <p>This interface allows plugins to provide:
 * <ul>
 *   <li>Storage providers (S3, GCS, Azure, HTTP) for accessing data - keyed by URI scheme</li>
 *   <li>Format readers (Parquet, CSV, ORC) for parsing data files - keyed by format name</li>
 *   <li>Table catalog connectors (Iceberg, Delta Lake) for table metadata - keyed by catalog type</li>
 *   <li>Custom operator factories for complex datasources - keyed by source type</li>
 *   <li>Filter pushdown support for predicate pushdown optimization - keyed by source type</li>
 * </ul>
 *
 * <p>All methods have default implementations returning empty maps/lists, allowing
 * plugins to implement only the capabilities they provide.
 *
 * <p>Note: Method names follow the project convention of omitting the "get" prefix
 * since there are no corresponding setters.
 */
public interface DataSourcePlugin {

    default Map<String, StorageProviderFactory> storageProviders(Settings settings) {
        return Map.of();
    }

    default Map<String, StorageProviderFactory> storageProviders(Settings settings, ExecutorService executor) {
        return storageProviders(settings);
    }

    default Map<String, FormatReaderFactory> formatReaders(Settings settings) {
        return Map.of();
    }

    default Map<String, TableCatalogFactory> tableCatalogs(Settings settings) {
        return Map.of();
    }

    default Map<String, SourceOperatorFactoryProvider> operatorFactories(Settings settings) {
        return Map.of();
    }

    default Map<String, FilterPushdownSupport> filterPushdownSupport(Settings settings) {
        return Map.of();
    }

    default List<NamedWriteableRegistry.Entry> namedWriteables() {
        return List.of();
    }
}
