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

    default StorageProvider create(Settings settings, Map<String, Object> config) {
        return create(settings);
    }
}
