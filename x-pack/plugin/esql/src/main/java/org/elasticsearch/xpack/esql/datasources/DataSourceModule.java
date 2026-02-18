/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.xpack.esql.datasources.spi.DataSourcePlugin;
import org.elasticsearch.xpack.esql.datasources.spi.FilterPushdownSupport;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReaderFactory;
import org.elasticsearch.xpack.esql.datasources.spi.SourceOperatorFactoryProvider;
import org.elasticsearch.xpack.esql.datasources.spi.StorageProviderFactory;
import org.elasticsearch.xpack.esql.datasources.spi.TableCatalog;
import org.elasticsearch.xpack.esql.datasources.spi.TableCatalogFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;

/**
 * Module that collects all data source implementations from plugins.
 * Follows the same pattern as RepositoriesModule in Elasticsearch core.
 *
 * <p>This module:
 * <ul>
 *   <li>Discovers all plugins implementing {@link DataSourcePlugin}</li>
 *   <li>Collects storage providers, format readers, table catalog connectors, and operator factories</li>
 *   <li>Populates registries for runtime lookup</li>
 *   <li>Validates that no duplicate registrations occur</li>
 *   <li>Creates an {@link OperatorFactoryRegistry} for unified operator factory lookup</li>
 * </ul>
 *
 * <p>This module implements Closeable to properly release resources held by storage providers
 * (such as HttpClient connections).
 *
 * <p>Note: Method names follow the project convention of omitting the "get" prefix.
 */
public final class DataSourceModule implements Closeable {

    private final StorageProviderRegistry storageProviderRegistry;
    private final FormatReaderRegistry formatReaderRegistry;
    private final Map<String, TableCatalogFactory> tableCatalogs;
    private final Map<String, SourceOperatorFactoryProvider> operatorFactories;
    private final FilterPushdownRegistry filterPushdownRegistry;
    private final Settings settings;

    public DataSourceModule(
        List<DataSourcePlugin> dataSourcePlugins,
        Settings settings,
        BlockFactory blockFactory,
        ExecutorService executor
    ) {
        this.settings = settings;
        this.storageProviderRegistry = new StorageProviderRegistry(settings);
        this.formatReaderRegistry = new FormatReaderRegistry();

        Map<String, StorageProviderFactory> storageFactories = new HashMap<>();
        Map<String, FormatReaderFactory> formatFactories = new HashMap<>();
        Map<String, TableCatalogFactory> catalogFactories = new HashMap<>();
        Map<String, SourceOperatorFactoryProvider> operatorFactoryProviders = new HashMap<>();
        Map<String, FilterPushdownSupport> filterPushdownProviders = new HashMap<>();

        for (DataSourcePlugin plugin : dataSourcePlugins) {

            Map<String, StorageProviderFactory> newStorageTypes = plugin.storageProviders(settings, executor);
            for (Map.Entry<String, StorageProviderFactory> entry : newStorageTypes.entrySet()) {
                String scheme = entry.getKey();
                if (storageFactories.put(scheme, entry.getValue()) != null) {
                    throw new IllegalArgumentException("Storage provider for scheme [" + scheme + "] is already registered");
                }
            }

            Map<String, FormatReaderFactory> newFormatTypes = plugin.formatReaders(settings);
            for (Map.Entry<String, FormatReaderFactory> entry : newFormatTypes.entrySet()) {
                String format = entry.getKey();
                if (formatFactories.put(format, entry.getValue()) != null) {
                    throw new IllegalArgumentException("Format reader for [" + format + "] is already registered");
                }
            }

            Map<String, TableCatalogFactory> newCatalogTypes = plugin.tableCatalogs(settings);
            for (Map.Entry<String, TableCatalogFactory> entry : newCatalogTypes.entrySet()) {
                String catalogType = entry.getKey();
                if (catalogFactories.put(catalogType, entry.getValue()) != null) {
                    throw new IllegalArgumentException("Table catalog for [" + catalogType + "] is already registered");
                }
            }

            Map<String, SourceOperatorFactoryProvider> newOperatorFactories = plugin.operatorFactories(settings);
            for (Map.Entry<String, SourceOperatorFactoryProvider> entry : newOperatorFactories.entrySet()) {
                String sourceType = entry.getKey();
                if (operatorFactoryProviders.put(sourceType, entry.getValue()) != null) {
                    throw new IllegalArgumentException("Operator factory for source type [" + sourceType + "] is already registered");
                }
            }

            Map<String, FilterPushdownSupport> newFilterPushdown = plugin.filterPushdownSupport(settings);
            for (Map.Entry<String, FilterPushdownSupport> entry : newFilterPushdown.entrySet()) {
                String sourceType = entry.getKey();
                if (filterPushdownProviders.put(sourceType, entry.getValue()) != null) {
                    throw new IllegalArgumentException(
                        "Filter pushdown support for source type [" + sourceType + "] is already registered"
                    );
                }
            }
        }

        // Register factories lazily -- providers and readers are created on first access
        for (Map.Entry<String, StorageProviderFactory> entry : storageFactories.entrySet()) {
            storageProviderRegistry.registerFactory(entry.getKey(), entry.getValue());
        }

        for (Map.Entry<String, FormatReaderFactory> entry : formatFactories.entrySet()) {
            formatReaderRegistry.registerLazy(entry.getKey(), entry.getValue(), settings, blockFactory);
        }

        this.tableCatalogs = Map.copyOf(catalogFactories);
        this.operatorFactories = Map.copyOf(operatorFactoryProviders);
        this.filterPushdownRegistry = new FilterPushdownRegistry(filterPushdownProviders);
    }

    @Override
    public void close() throws IOException {
        storageProviderRegistry.close();
    }

    public StorageProviderRegistry storageProviderRegistry() {
        return storageProviderRegistry;
    }

    public FormatReaderRegistry formatReaderRegistry() {
        return formatReaderRegistry;
    }

    public Map<String, SourceOperatorFactoryProvider> operatorFactories() {
        return operatorFactories;
    }

    public FilterPushdownRegistry filterPushdownRegistry() {
        return filterPushdownRegistry;
    }

    public OperatorFactoryRegistry createOperatorFactoryRegistry(Executor executor) {
        return new OperatorFactoryRegistry(operatorFactories, storageProviderRegistry, formatReaderRegistry, executor, settings);
    }

    public TableCatalog createTableCatalog(String catalogType, Settings settings) {
        TableCatalogFactory factory = tableCatalogs.get(catalogType);
        if (factory == null) {
            throw new IllegalArgumentException("No table catalog registered for type: " + catalogType);
        }
        return factory.create(settings);
    }

    public boolean hasTableCatalog(String catalogType) {
        return tableCatalogs.containsKey(catalogType);
    }
}
