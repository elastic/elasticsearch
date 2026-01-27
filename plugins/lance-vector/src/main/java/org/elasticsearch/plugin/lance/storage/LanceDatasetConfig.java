/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.plugin.lance.storage;

/**
 * Configuration for Lance dataset operations.
 * <p>
 * Simple configuration class for MVP. Can be extended with additional
 * parameters (nprobes, timeouts, etc.) as needed.
 */
public record LanceDatasetConfig(
    String idColumn,
    String vectorColumn,
    int expectedDims,
    String ossEndpoint,
    String ossAccessKeyId,
    String ossAccessKeySecret
) {
    /**
     * Create config with defaults.
     */
    public static LanceDatasetConfig defaults() {
        return new LanceDatasetConfig("_id", "vector", 0, null, null, null);
    }

    /**
     * Create config with specific dimensions.
     */
    public static LanceDatasetConfig withDims(int dims) {
        return new LanceDatasetConfig("_id", "vector", dims, null, null, null);
    }

    /**
     * Create config with OSS storage options.
     */
    public static LanceDatasetConfig withOssConfig(String idColumn, String vectorColumn, int dims,
                                                     String ossEndpoint, String ossAccessKeyId, String ossAccessKeySecret) {
        return new LanceDatasetConfig(idColumn, vectorColumn, dims, ossEndpoint, ossAccessKeyId, ossAccessKeySecret);
    }

    /**
     * Check if this is an OSS configuration.
     */
    public boolean isOssConfigured() {
        return ossEndpoint != null && ossEndpoint.isEmpty() == false;
    }
}
