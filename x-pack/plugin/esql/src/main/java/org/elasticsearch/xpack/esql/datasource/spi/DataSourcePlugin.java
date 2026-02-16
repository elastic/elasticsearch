/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.spi;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.Settings;

import java.util.List;
import java.util.Map;

/**
 * Extension point for data source implementations.
 *
 * <p>Plugins implementing this interface are discovered at startup and provide
 * factories for creating {@link DataSource} instances. Each factory is keyed
 * by a user-facing data source type identifier (e.g., "s3", "postgres", "iceberg").
 *
 * <p>This follows the standard Elasticsearch plugin pattern used by
 * {@code IngestPlugin}, {@code AnalysisPlugin}, {@code RepositoryPlugin}, etc.
 * A single plugin can provide multiple data source types.
 *
 * <p>All methods have default implementations returning empty maps/lists,
 * allowing plugins to implement only the capabilities they provide.
 *
 * @see DataSourceFactory
 * @see DataSource
 */
public interface DataSourcePlugin {

    /**
     * Returns factories for creating {@link DataSource} instances, keyed by
     * data source type identifier.
     *
     * <p>The key is the string used in {@code FROM datasource_name:expression}
     * to identify the data source type. Examples: "s3", "postgres", "iceberg".
     *
     * @param settings the node settings
     * @return map of data source type to factory
     */
    default Map<String, DataSourceFactory> dataSources(Settings settings) {
        return Map.of();
    }

    /**
     * Returns named writeable entries for serialization.
     *
     * <p>Data sources that support distributed execution must register their
     * plan nodes and other serializable types here.
     *
     * @return list of named writeable entries
     */
    default List<NamedWriteableRegistry.Entry> namedWriteables() {
        return List.of();
    }
}
