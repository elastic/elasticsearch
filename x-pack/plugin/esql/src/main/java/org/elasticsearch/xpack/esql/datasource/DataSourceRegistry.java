/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xpack.esql.datasource.spi.DataSource;
import org.elasticsearch.xpack.esql.datasource.spi.DataSourceFactory;
import org.elasticsearch.xpack.esql.datasource.spi.DataSourcePlugin;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Generic data source registry.
 *
 * <p>Discovers {@link DataSourcePlugin} instances, collects their
 * {@link DataSourceFactory} maps, and provides lookup by data source type.
 * This registry knows nothing about lakehouse specifics — that is
 * {@link org.elasticsearch.xpack.esql.datasource.lakehouse.LakehouseRegistry}'s job.
 *
 * <p>The registry is created once at startup and holds all registered
 * {@link DataSourceFactory} instances. Actual {@link DataSource} instances
 * are created on demand during query resolution via
 * {@link DataSourceFactory#create}.
 *
 * @see DataSourcePlugin
 * @see DataSourceFactory
 */
public final class DataSourceRegistry implements Closeable {

    private final Map<String, DataSourceFactory> factories;

    /**
     * Create a data source registry from discovered plugins.
     *
     * @param plugins discovered data source plugins
     * @param settings node settings
     */
    public DataSourceRegistry(List<DataSourcePlugin> plugins, Settings settings) {
        this.factories = new HashMap<>();
        for (DataSourcePlugin plugin : plugins) {
            Map<String, DataSourceFactory> pluginFactories = plugin.dataSources(settings);
            for (Map.Entry<String, DataSourceFactory> entry : pluginFactories.entrySet()) {
                String type = entry.getKey();
                if (factories.containsKey(type)) {
                    throw new IllegalArgumentException("Duplicate data source type [" + type + "] registered by multiple plugins");
                }
                factories.put(type, entry.getValue());
            }
        }
    }

    /**
     * Look up a data source factory by type.
     *
     * @param type the data source type identifier
     * @return the factory, or null if not registered
     */
    public DataSourceFactory factory(String type) {
        return factories.get(type);
    }

    /**
     * Check whether a data source type is registered.
     *
     * @param type the data source type identifier
     * @return true if a factory is registered for the type
     */
    public boolean hasType(String type) {
        return factories.containsKey(type);
    }

    /**
     * Returns all registered data source type identifiers.
     */
    public Collection<String> registeredTypes() {
        return factories.keySet();
    }

    @Override
    public void close() throws IOException {
        // Factories are lightweight — nothing to close.
        // DataSource instances are created per-query and closed by callers.
    }
}
