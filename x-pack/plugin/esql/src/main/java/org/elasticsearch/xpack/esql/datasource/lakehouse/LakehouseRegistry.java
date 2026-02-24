/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.lakehouse;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.xpack.esql.datasource.lakehouse.spi.CatalogPlugin;
import org.elasticsearch.xpack.esql.datasource.lakehouse.spi.FormatPlugin;
import org.elasticsearch.xpack.esql.datasource.lakehouse.spi.FormatReaderFactory;
import org.elasticsearch.xpack.esql.datasource.lakehouse.spi.LakehouseDataSource;
import org.elasticsearch.xpack.esql.datasource.lakehouse.spi.StoragePlugin;
import org.elasticsearch.xpack.esql.datasource.lakehouse.spi.StorageProviderFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Lakehouse-specific infrastructure registry.
 *
 * <p>Discovers {@link StoragePlugin}, {@link FormatPlugin}, and {@link CatalogPlugin}
 * implementations and populates the corresponding registries. Only
 * {@link LakehouseDataSource} implementations interact with this registry —
 * the core {@link org.elasticsearch.xpack.esql.datasource.spi.DataSource} SPI knows
 * nothing about it.
 *
 * <p>This class is responsible for:
 * <ul>
 *   <li>Collecting storage provider factories from all {@link StoragePlugin}s</li>
 *   <li>Collecting format reader factories from all {@link FormatPlugin}s</li>
 *   <li>Creating and caching storage providers and format readers</li>
 *   <li>Lifecycle management (closing all created resources)</li>
 * </ul>
 *
 * @see StoragePlugin
 * @see FormatPlugin
 * @see CatalogPlugin
 */
public final class LakehouseRegistry implements Closeable {

    private final StorageProviderRegistry storageProviderRegistry;
    private final FormatReaderRegistry formatReaderRegistry;
    private final Settings settings;
    private final BlockFactory blockFactory;

    /**
     * Create a lakehouse registry from discovered plugins.
     *
     * @param storagePlugins discovered storage plugins
     * @param formatPlugins discovered format plugins
     * @param catalogPlugins discovered catalog plugins (reserved for future use)
     * @param settings node settings
     * @param blockFactory factory for creating data blocks
     */
    public LakehouseRegistry(
        List<StoragePlugin> storagePlugins,
        List<FormatPlugin> formatPlugins,
        List<CatalogPlugin> catalogPlugins,
        Settings settings,
        BlockFactory blockFactory
    ) {
        this.settings = settings;
        this.blockFactory = blockFactory;
        this.storageProviderRegistry = new StorageProviderRegistry(settings);
        this.formatReaderRegistry = new FormatReaderRegistry();

        // Register storage provider factories (lazily created on first access)
        for (StoragePlugin plugin : storagePlugins) {
            Map<String, StorageProviderFactory> factories = plugin.storageProviders(settings);
            for (Map.Entry<String, StorageProviderFactory> entry : factories.entrySet()) {
                storageProviderRegistry.registerFactory(entry.getKey(), entry.getValue());
            }
        }

        // Register format reader factories (lazily created on first access)
        for (FormatPlugin plugin : formatPlugins) {
            Map<String, FormatReaderFactory> factories = plugin.formatReaders(settings);
            for (Map.Entry<String, FormatReaderFactory> entry : factories.entrySet()) {
                formatReaderRegistry.registerLazy(entry.getKey(), entry.getValue(), settings, blockFactory);
            }
        }

        // Catalog plugins are reserved for future use (Iceberg, Delta, Hudi)
    }

    /**
     * Returns the storage provider registry for scheme-based provider lookup.
     */
    public StorageProviderRegistry storageProviderRegistry() {
        return storageProviderRegistry;
    }

    /**
     * Returns the format reader registry for format-based reader lookup.
     */
    public FormatReaderRegistry formatReaderRegistry() {
        return formatReaderRegistry;
    }

    /**
     * Returns the node settings.
     */
    public Settings settings() {
        return settings;
    }

    /**
     * Returns the block factory for creating data blocks.
     */
    public BlockFactory blockFactory() {
        return blockFactory;
    }

    @Override
    public void close() throws IOException {
        IOUtils.close(storageProviderRegistry);
    }
}
