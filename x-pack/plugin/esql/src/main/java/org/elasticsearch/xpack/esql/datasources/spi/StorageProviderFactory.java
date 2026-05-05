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
     * Returns a {@link StorageProvider} configured from a per-query WITH-clause map, paired
     * with the set of keys this storage layer consumed from {@code config}. The coordinator
     * unions this set with the format layer's consumed set and rejects any key that no layer
     * claims, making typos surface as planning errors rather than silently-dropped settings.
     * <p>
     * Default returns {@link Configured#empty(Object) Configured.empty(create(settings))} —
     * suitable for storage backends that take no per-query config.
     */
    default Configured<StorageProvider> create(Settings settings, Map<String, Object> config) {
        return Configured.empty(create(settings));
    }
}
