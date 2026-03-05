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
import org.elasticsearch.xpack.esql.datasources.spi.Connector;
import org.elasticsearch.xpack.esql.datasources.spi.ConnectorFactory;
import org.elasticsearch.xpack.esql.datasources.spi.DataSourcePlugin;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalSourceFactory;
import org.elasticsearch.xpack.esql.datasources.spi.FilterPushdownSupport;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReaderFactory;
import org.elasticsearch.xpack.esql.datasources.spi.SourceMetadata;
import org.elasticsearch.xpack.esql.datasources.spi.SourceOperatorFactoryProvider;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.datasources.spi.StorageProvider;
import org.elasticsearch.xpack.esql.datasources.spi.StorageProviderFactory;
import org.elasticsearch.xpack.esql.datasources.spi.TableCatalog;
import org.elasticsearch.xpack.esql.datasources.spi.TableCatalogFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;

/**
 * Module that collects all data source implementations from plugins.
 *
 * <p>This module registers lazy delegating factories per-plugin so that heavy dependencies
 * (AWS SDK, Parquet, Arrow) are only loaded when a query targets that backend.
 * Capability declarations ({@link DataSourceCapabilities}) are used for cheap routing
 * and early validation without triggering classloading.
 */
public final class DataSourceModule implements Closeable {

    private final StorageProviderRegistry storageProviderRegistry;
    private final FormatReaderRegistry formatReaderRegistry;
    private final Map<String, ExternalSourceFactory> sourceFactories;
    // TODO(#142815): backward-compat bridge — remove once table functions land.
    private final Map<String, SourceOperatorFactoryProvider> pluginFactories;
    private final FilterPushdownRegistry filterPushdownRegistry;
    private final List<Closeable> managedCloseables;
    private final Settings settings;
    private final DataSourceCapabilities capabilities;

    public DataSourceModule(
        List<DataSourcePlugin> dataSourcePlugins,
        DataSourceCapabilities capabilities,
        Settings settings,
        BlockFactory blockFactory,
        ExecutorService executor
    ) {
        this.settings = settings;
        this.capabilities = capabilities;
        this.storageProviderRegistry = new StorageProviderRegistry(settings);
        this.formatReaderRegistry = new FormatReaderRegistry();

        Map<String, ExternalSourceFactory> sourceFactoryMap = new LinkedHashMap<>();
        Map<String, SourceOperatorFactoryProvider> operatorFactoryProviders = new HashMap<>();
        List<Closeable> closeables = new ArrayList<>();
        Map<String, String> registeredSchemes = new HashMap<>();

        for (DataSourcePlugin plugin : dataSourcePlugins) {
            LazyPluginState state = new LazyPluginState(plugin, settings, executor);

            // Storage providers: register a delegating factory per declared scheme
            for (String scheme : plugin.supportedSchemes()) {
                String existing = registeredSchemes.put(scheme, plugin.getClass().getName());
                if (existing != null) {
                    throw new IllegalArgumentException("Storage provider for scheme [" + scheme + "] is already registered");
                }
                StorageProviderFactory delegating = new StorageProviderFactory() {
                    @Override
                    public StorageProvider create(Settings s) {
                        Map<String, StorageProviderFactory> factories = state.storageFactories();
                        StorageProviderFactory real = factories.get(scheme);
                        if (real == null) {
                            throw new IllegalArgumentException(
                                "Plugin "
                                    + plugin.getClass().getName()
                                    + " declared scheme ["
                                    + scheme
                                    + "] but storageProviders() did not return it"
                            );
                        }
                        return real.create(s);
                    }

                    @Override
                    public StorageProvider create(Settings s, Map<String, Object> config) {
                        Map<String, StorageProviderFactory> factories = state.storageFactories();
                        StorageProviderFactory real = factories.get(scheme);
                        if (real == null) {
                            throw new IllegalArgumentException(
                                "Plugin "
                                    + plugin.getClass().getName()
                                    + " declared scheme ["
                                    + scheme
                                    + "] but storageProviders() did not return it"
                            );
                        }
                        return real.create(s, config);
                    }
                };
                storageProviderRegistry.registerFactory(scheme, delegating);
            }

            // Format readers: register a delegating factory per declared format
            for (String format : plugin.supportedFormats()) {
                FormatReaderFactory delegating = (s, bf) -> {
                    Map<String, FormatReaderFactory> factories = state.formatFactories();
                    FormatReaderFactory real = factories.get(format);
                    if (real == null) {
                        throw new IllegalArgumentException(
                            "Plugin "
                                + plugin.getClass().getName()
                                + " declared format ["
                                + format
                                + "] but formatReaders() did not return it"
                        );
                    }
                    return real.create(s, bf);
                };
                formatReaderRegistry.registerLazy(format, delegating, settings, blockFactory);
            }

            // Pre-register extensions from capabilities so hasExtension() works without triggering lazy init.
            // Each extension maps to a single format; cross-product with multiple formats is not supported.
            Set<String> pluginFormats = plugin.supportedFormats();
            Set<String> pluginExtensions = plugin.supportedExtensions();
            if (pluginFormats.size() > 1 && pluginExtensions.isEmpty() == false) {
                throw new IllegalArgumentException(
                    "Plugin "
                        + plugin.getClass().getName()
                        + " declares multiple formats "
                        + pluginFormats
                        + " with extensions "
                        + pluginExtensions
                        + "; each plugin must declare at most one format when extensions are present"
                );
            }
            for (String ext : pluginExtensions) {
                for (String format : pluginFormats) {
                    formatReaderRegistry.registerExtension(ext, format);
                }
            }

            // Connectors: register lazy wrappers only for explicitly declared connector schemes
            Set<String> connectorSchemes = plugin.supportedConnectorSchemes();
            if (connectorSchemes.isEmpty() == false) {
                LazyConnectorFactory lazyConnector = new LazyConnectorFactory(state, connectorSchemes, plugin.getClass().getName());
                for (String scheme : connectorSchemes) {
                    sourceFactoryMap.putIfAbsent(scheme, lazyConnector);
                }
            }

            // Table catalogs: register lazy wrappers
            for (String catalogType : plugin.supportedCatalogs()) {
                LazyTableCatalogWrapper lazyCatalog = new LazyTableCatalogWrapper(state, catalogType, closeables, settings);
                if (sourceFactoryMap.put(catalogType, lazyCatalog) != null) {
                    throw new IllegalArgumentException("Source factory for type [" + catalogType + "] is already registered");
                }
            }

            // TODO(#142815): operatorFactories() is a backward-compat bridge for plugins that haven't
            // migrated to ExternalSourceFactory.operatorFactory(). Remove once table functions land.
            Map<String, SourceOperatorFactoryProvider> newOperatorFactories = plugin.operatorFactories(settings);
            if (newOperatorFactories.isEmpty() == false) {
                for (Map.Entry<String, SourceOperatorFactoryProvider> entry : newOperatorFactories.entrySet()) {
                    String sourceType = entry.getKey();
                    if (operatorFactoryProviders.put(sourceType, entry.getValue()) != null) {
                        throw new IllegalArgumentException("Operator factory for source type [" + sourceType + "] is already registered");
                    }
                }
            }
        }

        // Register the framework-internal FileSourceFactory as a catch-all fallback.
        // It must be last so that plugin-provided factories (Iceberg, Flight) get priority.
        FileSourceFactory fileFallback = new FileSourceFactory(storageProviderRegistry, formatReaderRegistry, settings);
        sourceFactoryMap.put("file", fileFallback);
        // Also register under each format name so OperatorFactoryRegistry can look up
        // by the sourceType returned from FormatReader.formatName() (e.g. "parquet", "csv").
        for (String formatName : capabilities.formats()) {
            sourceFactoryMap.putIfAbsent(formatName, fileFallback);
        }

        this.sourceFactories = Map.copyOf(sourceFactoryMap);
        this.pluginFactories = Map.copyOf(operatorFactoryProviders);
        this.managedCloseables = closeables;

        // Build FilterPushdownRegistry -- only from non-lazy factories to avoid triggering loading.
        // Lazy wrappers (LazyConnectorFactory, LazyTableCatalogWrapper) return null from
        // filterPushdownSupport() by default, which is correct since the real support
        // is only available after loading.
        Map<String, FilterPushdownSupport> filterPushdownProviders = new HashMap<>();
        for (Map.Entry<String, ExternalSourceFactory> entry : this.sourceFactories.entrySet()) {
            ExternalSourceFactory factory = entry.getValue();
            // Skip lazy wrappers to avoid triggering classloading
            if (factory instanceof LazyConnectorFactory || factory instanceof LazyTableCatalogWrapper) {
                continue;
            }
            FilterPushdownSupport fps = factory.filterPushdownSupport();
            if (fps != null) {
                filterPushdownProviders.put(entry.getKey(), fps);
            }
        }
        this.filterPushdownRegistry = new FilterPushdownRegistry(filterPushdownProviders);
    }

    @Override
    public void close() throws IOException {
        List<Closeable> all = new ArrayList<>();
        all.add(storageProviderRegistry);
        all.addAll(managedCloseables);
        IOUtils.close(all);
    }

    public DataSourceCapabilities capabilities() {
        return capabilities;
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

    /**
     * Per-plugin lazy state that caches the results of each factory method.
     * Factory methods are only called when first needed, deferring heavy classloading.
     */
    static class LazyPluginState {
        private final DataSourcePlugin plugin;
        private final Settings settings;
        private final ExecutorService executor;
        private volatile Map<String, StorageProviderFactory> storageFactoriesCache;
        private volatile Map<String, FormatReaderFactory> formatFactoriesCache;
        private volatile Map<String, ConnectorFactory> connectorFactoriesCache;
        private volatile Map<String, TableCatalogFactory> catalogFactoriesCache;

        LazyPluginState(DataSourcePlugin plugin, Settings settings, ExecutorService executor) {
            this.plugin = plugin;
            this.settings = settings;
            this.executor = executor;
        }

        Map<String, StorageProviderFactory> storageFactories() {
            if (storageFactoriesCache == null) {
                synchronized (this) {
                    if (storageFactoriesCache == null) {
                        storageFactoriesCache = plugin.storageProviders(settings, executor);
                    }
                }
            }
            return storageFactoriesCache;
        }

        Map<String, FormatReaderFactory> formatFactories() {
            if (formatFactoriesCache == null) {
                synchronized (this) {
                    if (formatFactoriesCache == null) {
                        formatFactoriesCache = plugin.formatReaders(settings);
                    }
                }
            }
            return formatFactoriesCache;
        }

        Map<String, ConnectorFactory> connectorFactories() {
            if (connectorFactoriesCache == null) {
                synchronized (this) {
                    if (connectorFactoriesCache == null) {
                        connectorFactoriesCache = plugin.connectors(settings);
                    }
                }
            }
            return connectorFactoriesCache;
        }

        Map<String, TableCatalogFactory> catalogFactories() {
            if (catalogFactoriesCache == null) {
                synchronized (this) {
                    if (catalogFactoriesCache == null) {
                        catalogFactoriesCache = plugin.tableCatalogs(settings);
                    }
                }
            }
            return catalogFactoriesCache;
        }
    }

    /**
     * Lazy connector wrapper whose canHandle() checks declared schemes without loading the plugin.
     */
    static class LazyConnectorFactory implements ConnectorFactory {
        private final LazyPluginState state;
        private final Set<String> declaredSchemes;
        private final String pluginName;
        private volatile ConnectorFactory delegate;

        LazyConnectorFactory(LazyPluginState state, Set<String> declaredSchemes, String pluginName) {
            this.state = state;
            this.declaredSchemes = declaredSchemes;
            this.pluginName = pluginName;
        }

        @Override
        public String type() {
            return resolveDelegate().type();
        }

        @Override
        public boolean canHandle(String location) {
            if (location == null) {
                return false;
            }
            try {
                String scheme = extractScheme(location);
                return declaredSchemes.contains(scheme);
            } catch (IllegalArgumentException e) {
                return false;
            }
        }

        @Override
        public SourceMetadata resolveMetadata(String location, Map<String, Object> config) {
            return resolveDelegate().resolveMetadata(location, config);
        }

        @Override
        public Connector open(Map<String, Object> config) {
            return resolveDelegate().open(config);
        }

        @Override
        public FilterPushdownSupport filterPushdownSupport() {
            if (delegate == null) {
                return null;
            }
            return delegate.filterPushdownSupport();
        }

        @Override
        public SourceOperatorFactoryProvider operatorFactory() {
            return resolveDelegate().operatorFactory();
        }

        private ConnectorFactory resolveDelegate() {
            if (delegate == null) {
                synchronized (this) {
                    if (delegate == null) {
                        Map<String, ConnectorFactory> connectors = state.connectorFactories();
                        if (connectors.isEmpty()) {
                            throw new IllegalStateException("Plugin " + pluginName + " declared schemes but connectors() returned empty");
                        }
                        if (connectors.size() > 1) {
                            throw new IllegalStateException(
                                "Plugin "
                                    + pluginName
                                    + " returned multiple connectors "
                                    + connectors.keySet()
                                    + "; lazy mode supports a single connector per plugin"
                            );
                        }
                        delegate = connectors.values().iterator().next();
                    }
                }
            }
            return delegate;
        }

        private static String extractScheme(String location) {
            int colonIdx = location.indexOf("://");
            if (colonIdx <= 0) {
                throw new IllegalArgumentException("No scheme in location: " + location);
            }
            return location.substring(0, colonIdx).toLowerCase(Locale.ROOT);
        }
    }

    /**
     * Lazy table catalog wrapper whose canHandle() uses heuristics (S3 scheme + no file extension)
     * without loading Iceberg classes.
     */
    static class LazyTableCatalogWrapper implements ExternalSourceFactory {
        private final LazyPluginState state;
        private final String catalogType;
        private final List<Closeable> managedCloseables;
        private final Settings settings;
        private volatile TableCatalog delegate;

        LazyTableCatalogWrapper(LazyPluginState state, String catalogType, List<Closeable> managedCloseables, Settings settings) {
            this.state = state;
            this.catalogType = catalogType;
            this.managedCloseables = managedCloseables;
            this.settings = settings;
        }

        @Override
        public String type() {
            return catalogType;
        }

        @Override
        public boolean canHandle(String path) {
            if (path == null) {
                return false;
            }
            try {
                StoragePath sp = StoragePath.of(path);
                String scheme = sp.scheme();
                if (Set.of("s3", "s3a", "s3n").contains(scheme) == false) {
                    return false;
                }
                String obj = sp.objectName();
                if (obj != null) {
                    int dot = obj.lastIndexOf('.');
                    if (dot >= 0 && dot < obj.length() - 1) {
                        return false;
                    }
                }
                return true;
            } catch (IllegalArgumentException e) {
                return false;
            }
        }

        @Override
        public SourceMetadata resolveMetadata(String location, Map<String, Object> config) {
            return resolveDelegate().resolveMetadata(location, config);
        }

        @Override
        public FilterPushdownSupport filterPushdownSupport() {
            if (delegate == null) {
                return null;
            }
            return delegate.filterPushdownSupport();
        }

        @Override
        public SourceOperatorFactoryProvider operatorFactory() {
            return resolveDelegate().operatorFactory();
        }

        private TableCatalog resolveDelegate() {
            if (delegate == null) {
                synchronized (this) {
                    if (delegate == null) {
                        Map<String, TableCatalogFactory> factories = state.catalogFactories();
                        TableCatalogFactory factory = factories.get(catalogType);
                        if (factory == null) {
                            throw new IllegalStateException(
                                "Plugin declared catalog [" + catalogType + "] but tableCatalogs() did not return it"
                            );
                        }
                        TableCatalog catalog = factory.create(settings);
                        synchronized (managedCloseables) {
                            managedCloseables.add(catalog);
                        }
                        delegate = catalog;
                    }
                }
            }
            return delegate;
        }
    }
}
