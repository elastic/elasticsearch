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
 * Extension point for storage provider implementations.
 *
 * <p>Plugins implementing this interface provide storage access for lakehouse
 * data sources. Each provider factory is keyed by URI scheme
 * (e.g., "s3", "gcs", "hdfs", "file", "http").
 *
 * <p>Storage plugins are lakehouse infrastructure — they are discovered by
 * {@link LakehouseRegistry} and used by {@link LakehouseDataSource}
 * implementations. The core {@link org.elasticsearch.xpack.esql.datasource.spi.DataSource}
 * SPI knows nothing about storage plugins.
 *
 * @see StorageProvider
 * @see StorageProviderFactory
 * @see LakehouseRegistry
 */
public interface StoragePlugin {

    /**
     * Returns factories for creating {@link StorageProvider} instances, keyed
     * by URI scheme.
     *
     * <p>The key is the URI scheme portion of a {@link StoragePath}
     * (e.g., "s3", "gcs", "hdfs", "file", "http", "https").
     *
     * @param settings the node settings
     * @return map of URI scheme to storage provider factory
     */
    Map<String, StorageProviderFactory> storageProviders(Settings settings);
}
