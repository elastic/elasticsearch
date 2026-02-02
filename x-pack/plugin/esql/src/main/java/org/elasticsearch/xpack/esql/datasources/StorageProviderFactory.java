/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.datasources.spi.StorageProvider;

/**
 * Factory for creating StorageProvider instances.
 * Allows configuration per-location based on the StoragePath.
 */
@FunctionalInterface
public interface StorageProviderFactory {
    /**
     * Creates a StorageProvider for the given path.
     *
     * @param path the storage path that determines provider configuration
     * @return a StorageProvider instance
     */
    StorageProvider create(StoragePath path);
}
