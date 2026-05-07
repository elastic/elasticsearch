/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

import org.elasticsearch.common.settings.Settings;

import java.util.Map;

/**
 * Factory for creating {@link StorageProvider} instances.
 * This functional interface allows data source plugins to provide
 * storage provider implementations without exposing implementation details.
 */
@FunctionalInterface
public interface StorageProviderFactory {

    StorageProvider create(Settings settings);

    /**
     * Per-query overload that takes a configuration map.
     * Default delegates to {@link #createTrackingConsumedKeys(Settings, Map)} and discards the consumed-keys set;
     * use this overload when the caller does not need to validate against the consumed keys.
     */
    default StorageProvider create(Settings settings, Map<String, Object> config) {
        return createTrackingConsumedKeys(settings, config).value();
    }

    /**
     * Per-query overload paired with the keys consumed from {@code config}.
     * Default ignores config and returns an empty consumed-keys set.
     * Implementations that read configuration from the map should override this method
     * (not {@link #create(Settings, Map)}); the simple overload is a default convenience.
     */
    default Configured<StorageProvider> createTrackingConsumedKeys(Settings settings, Map<String, Object> config) {
        return Configured.empty(create(settings));
    }
}
