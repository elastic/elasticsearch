/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.xpack.esql.datasources.spi.ConnectorFactory;
import org.elasticsearch.xpack.esql.datasources.spi.DataSourcePlugin;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalSourceFactory;
import org.elasticsearch.xpack.esql.datasources.spi.FilterPushdownSupport;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReaderFactory;
import org.elasticsearch.xpack.esql.datasources.spi.SourceOperatorFactoryProvider;
import org.elasticsearch.xpack.esql.datasources.spi.StorageProviderFactory;
import org.elasticsearch.xpack.esql.datasources.spi.TableCatalog;
import org.elasticsearch.xpack.esql.datasources.spi.TableCatalogFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
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
 *   <li>Collects storage providers, format readers, and external source factories</li>
 *   <li>Populates registries for runtime lookup</li>
 *   <li>Validates that no duplicate registrations occur</li>
 *   <li>Creates an {@link OperatorFactoryRegistry} for unified operator factory lookup</li>
 * </ul>
 *
 * <p>This module implements Closeable to properly release resources held by storage providers
 * (such as HttpClient connections) and managed TableCatalog instances.
 *
 * <p>Note: Method names follow the project convention of omitting the "get" prefix.
 */
public final class DataSourceModule implements Closeable {

    private final StorageProviderRegistry storageProviderRegistry;
    private final FormatReaderRegistry formatReaderRegistry;
    private final Map<String, ExternalSourceFactory> sourceFactories;
    // FIXME: pluginFactories is a backward-compat bridge for DataSourcePlugin.operatorFactories().
    // Once plugins migrate to ExternalSourceFactory.operatorFactory(), remove this field.
    private final Map<String, SourceOperatorFactoryProvider> pluginFactories;
    private final FilterPushdownRegistry filterPushdownRegistry;
    private final List<Closeable> managedCloseables;
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
        Map<String, ExternalSourceFactory> sourceFactoryMap = new LinkedHashMap<>();
        // FIXME: pluginFactories is a backward-compat bridge for DataSourcePlugin.operatorFactories().
        // Once plugins migrate to ExternalSourceFactory.operatorFactory(), remove this field.
        Map<String, SourceOperatorFactoryProvider> operatorFactoryProviders = new HashMap<>();
        List<Closeable> closeables = new ArrayList<>();

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

            // New unified path: sourceFactories()
            Map<String, ExternalSourceFactory> newSourceFactories = plugin.sourceFactories(settings);
            for (Map.Entry<String, ExternalSourceFactory> entry : newSourceFactories.entrySet()) {
                String sourceType = entry.getKey();
                if (sourceFactoryMap.put(sourceType, entry.getValue()) != null) {
                    throw new IllegalArgumentException("Source factory for type [" + sourceType + "] is already registered");
                }
            }

            // Bridge: connectors() → sourceFactoryMap (ConnectorFactory is ExternalSourceFactory)
            Map<String, ConnectorFactory> newConnectors = plugin.connectors(settings);
            for (Map.Entry<String, ConnectorFactory> entry : newConnectors.entrySet()) {
                String connectorType = entry.getKey();
                if (sourceFactoryMap.put(connectorType, entry.getValue()) != null) {
                    throw new IllegalArgumentException("Source factory for type [" + connectorType + "] is already registered");
                }
            }

            // Bridge: tableCatalogs() → create TableCatalog, add to sourceFactoryMap + closeables
            Map<String, TableCatalogFactory> newCatalogTypes = plugin.tableCatalogs(settings);
            for (Map.Entry<String, TableCatalogFactory> entry : newCatalogTypes.entrySet()) {
                String catalogType = entry.getKey();
                TableCatalog catalog = entry.getValue().create(settings);
                closeables.add(catalog);
                if (sourceFactoryMap.put(catalogType, catalog) != null) {
                    throw new IllegalArgumentException("Source factory for type [" + catalogType + "] is already registered");
                }
            }

            // FIXME: standalone operatorFactories() and filterPushdownSupport() on DataSourcePlugin
            // are unused by all production plugins; remove bridge once confirmed.
            Map<String, SourceOperatorFactoryProvider> newOperatorFactories = plugin.operatorFactories(settings);
            for (Map.Entry<String, SourceOperatorFactoryProvider> entry : newOperatorFactories.entrySet()) {
                String sourceType = entry.getKey();
                if (operatorFactoryProviders.put(sourceType, entry.getValue()) != null) {
                    throw new IllegalArgumentException("Operator factory for source type [" + sourceType + "] is already registered");
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

        // Register the framework-internal FileSourceFactory as a catch-all fallback.
        // It must be last so that plugin-provided factories (Iceberg, Flight) get priority.
        FileSourceFactory fileFallback = new FileSourceFactory(storageProviderRegistry, formatReaderRegistry, settings);
        sourceFactoryMap.put("file", fileFallback);
        // Also register under each format name so OperatorFactoryRegistry can look up
        // by the sourceType returned from FormatReader.formatName() (e.g. "parquet", "csv").
        for (String formatName : formatFactories.keySet()) {
            sourceFactoryMap.putIfAbsent(formatName, fileFallback);
        }

        this.sourceFactories = Map.copyOf(sourceFactoryMap);
        this.pluginFactories = Map.copyOf(operatorFactoryProviders);
        this.managedCloseables = List.copyOf(closeables);

        // Build FilterPushdownRegistry from ExternalSourceFactory capabilities
        Map<String, FilterPushdownSupport> filterPushdownProviders = new HashMap<>();
        for (Map.Entry<String, ExternalSourceFactory> entry : this.sourceFactories.entrySet()) {
            FilterPushdownSupport fps = entry.getValue().filterPushdownSupport();
            if (fps != null) {
                filterPushdownProviders.put(entry.getKey(), fps);
            }
        }
        // FIXME: standalone filterPushdownSupport() bridge omitted — no production plugin uses it.
        this.filterPushdownRegistry = new FilterPushdownRegistry(filterPushdownProviders);
    }

    @Override
    public void close() throws IOException {
        List<Closeable> all = new ArrayList<>();
        all.add(storageProviderRegistry);
        all.addAll(managedCloseables);
        IOUtils.close(all);
    }

    public StorageProviderRegistry storageProviderRegistry() {
        return storageProviderRegistry;
    }

    public FormatReaderRegistry formatReaderRegistry() {
        return formatReaderRegistry;
    }

    public Map<String, ExternalSourceFactory> sourceFactories() {
        return sourceFactories;
    }

    public FilterPushdownRegistry filterPushdownRegistry() {
        return filterPushdownRegistry;
    }

    public OperatorFactoryRegistry createOperatorFactoryRegistry(Executor executor) {
        return new OperatorFactoryRegistry(sourceFactories, pluginFactories, executor);
    }
}
