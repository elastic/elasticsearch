/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xpack.esql.datasources.spi.Configured;
import org.elasticsearch.xpack.esql.datasources.spi.DataSourcePlugin;
import org.elasticsearch.xpack.esql.datasources.spi.StorageProvider;
import org.elasticsearch.xpack.esql.datasources.spi.StorageProviderFactory;

import java.util.Map;
import java.util.Set;

/**
 * Serves {@link CountingStorageProvider} under its scheme, so a test can register a dataset at
 * {@code countfs://<abs-path>/...} and then assert what the query asked of the store.
 * <p>
 * A top-level class, and listed in the internalClusterTest {@code META-INF/services} file, because storage-provider
 * schemes are only picked up for {@link DataSourcePlugin}s discovered through SPI — returning this from a test's
 * {@code nodePlugins()} registers its thread pools a second time and the node refuses to start. Being found that
 * way it is installed on every node in this source set, which is also why it must stay inert until a dataset names
 * its scheme.
 */
public final class CountingDataSourcePlugin extends Plugin implements DataSourcePlugin {

    @Override
    public Set<String> supportedSchemes() {
        return Set.of(CountingStorageProvider.SCHEME);
    }

    @Override
    public Map<String, StorageProviderFactory> storageProviders(Settings settings) {
        return Map.of(CountingStorageProvider.SCHEME, new Factory());
    }

    private static final class Factory implements StorageProviderFactory {
        @Override
        public StorageProvider create(Settings settings) {
            return new CountingStorageProvider();
        }

        @Override
        public Configured<StorageProvider> createTrackingConsumedKeys(Settings settings, Map<String, Object> config) {
            // No config keys are consumed, so nothing identifies this provider's configuration.
            return new Configured<>(new CountingStorageProvider(), Set.of(), "", "");
        }
    }
}
