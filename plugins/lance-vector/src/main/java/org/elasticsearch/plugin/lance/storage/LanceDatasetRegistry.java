/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.plugin.lance.storage;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

/**
 * Unified registry for Lance datasets with caching support.
 * <p>
 * This registry manages both {@link RealLanceDataset} (for .lance format files)
 * and {@link FakeLanceDataset} (for JSON test files), providing a single entry
 * point for dataset access with automatic caching.
 * <p>
 * Thread-safe: Uses ConcurrentHashMap for cache storage.
 */
public class LanceDatasetRegistry {
    private static final Logger logger = LogManager.getLogger(LanceDatasetRegistry.class);

    private static final Map<String, LanceDataset> CACHE = new ConcurrentHashMap<>();

    /**
     * Get or load a dataset from the registry.
     * <p>
     * If the dataset is already cached, returns the cached instance.
     * Otherwise, calls the loader to create the dataset and caches it.
     *
     * @param uri Dataset URI (used as cache key)
     * @param loader Supplier that loads the dataset if not cached
     * @return The cached or newly loaded dataset
     * @throws IOException if the loader fails
     */
    public static LanceDataset get(String uri, Supplier<LanceDataset> loader) throws IOException {
        try {
            return CACHE.computeIfAbsent(uri, u -> {
                try {
                    logger.debug("Loading dataset into registry: {}", uri);
                    return loader.get();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
        } catch (RuntimeException e) {
            // Unwrap nested exceptions to find IOException
            Throwable cause = e.getCause();
            while (cause != null) {
                if (cause instanceof IOException io) {
                    throw io;
                }
                cause = cause.getCause();
            }
            throw e;
        }
    }

    /**
     * Get a dataset from the registry, loading it if necessary.
     * <p>
     * Automatically selects the appropriate dataset type based on URI:
     * <ul>
     *   <li>Object storage URIs (oss://, s3://) → RealLanceDataset</li>
     *   <li>Local file:// URIs ending with .lance or containing .lance/ → RealLanceDataset</li>
     *   <li>URIs starting with embedded: → FakeLanceDataset (embedded JSON for testing)</li>
     *   <li>Other URIs → FakeLanceDataset (external JSON file for testing)</li>
     * </ul>
     * <p>
     * <b>IMPORTANT:</b> Object storage URIs must always use RealLanceDataset, never FakeLanceDataset.
     * FakeLanceDataset is only for test fixtures and should not be used in production code paths.
     *
     * @param uri Dataset URI
     * @param dims Expected vector dimensions
     * @param config Configuration for real datasets
     * @return The dataset
     * @throws IOException if loading fails
     */
    public static LanceDataset getOrLoad(String uri, int dims, LanceDatasetConfig config) throws IOException {
        return get(uri, () -> {
            try {
                if (isLanceFormat(uri)) {
                    return RealLanceDataset.open(uri, config);
                } else {
                    return FakeLanceDataset.load(uri, dims);
                }
            } catch (IOException e) {
                throw new RuntimeException("Failed to load dataset: " + uri, e);
            }
        });
    }

    /**
     * Check if the URI points to a real Lance format dataset.
     * <p>
     * Real Lance datasets include:
     * <ul>
     *   <li>Local file:// URIs ending with .lance</li>
     *   <li>Object storage URIs (oss://, s3://) - these are Lance datasets stored remotely</li>
     *   <li>Paths containing .lance/ directory marker</li>
     * </ul>
     *
     * @param uri Dataset URI to check
     * @return true if the URI should use RealLanceDataset, false for test JSON files
     */
    public static boolean isLanceFormat(String uri) {
        // Object storage URIs (OSS, S3) are always real Lance datasets
        if (uri.startsWith("oss://") || uri.startsWith("s3://")) {
            return true;
        }
        // Local .lance files or directories
        return uri.endsWith(".lance") || uri.contains(".lance/") || uri.contains(".lance\\");
    }

    /**
     * Invalidate and close a specific dataset from the cache.
     *
     * @param uri Dataset URI to invalidate
     */
    public static void invalidate(String uri) {
        LanceDataset removed = CACHE.remove(uri);
        if (removed != null) {
            try {
                logger.debug("Invalidating dataset from registry: {}", uri);
                removed.close();
            } catch (IOException e) {
                logger.warn("Error closing invalidated dataset {}: {}", uri, e.getMessage());
            }
        }
    }

    /**
     * Clear all datasets from the cache.
     * <p>
     * This closes all cached datasets and removes them from the registry.
     * Useful for testing or shutdown.
     */
    public static void clear() {
        logger.debug("Clearing all datasets from registry");
        CACHE.values().forEach(ds -> {
            try {
                ds.close();
            } catch (IOException e) {
                logger.warn("Error closing dataset during clear: {}", e.getMessage());
            }
        });
        CACHE.clear();
    }

    /**
     * Get the number of cached datasets.
     */
    public static int size() {
        return CACHE.size();
    }

    /**
     * Check if a dataset is cached.
     */
    public static boolean contains(String uri) {
        return CACHE.containsKey(uri);
    }
}
