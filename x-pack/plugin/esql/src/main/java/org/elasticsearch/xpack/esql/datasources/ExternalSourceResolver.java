/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.datasources.datalake.iceberg.IcebergCatalogAdapter;
import org.elasticsearch.xpack.esql.datasources.format.parquet.ParquetFileAdapter;
import org.elasticsearch.xpack.esql.datasources.http.HttpConfiguration;
import org.elasticsearch.xpack.esql.datasources.http.HttpStorageProvider;
import org.elasticsearch.xpack.esql.datasources.s3.S3Configuration;
import org.elasticsearch.xpack.esql.datasources.spi.StorageManager;

import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;

/**
 * Resolver for external data sources (Iceberg tables and Parquet files).
 * This runs in parallel with IndexResolver to resolve external source metadata.
 * <p>
 * Following the same pattern as IndexResolver, this resolver:
 * - Takes a list of external source paths to resolve
 * - Performs I/O operations to fetch metadata (from S3/Iceberg catalogs)
 * - Returns ExternalSourceResolution containing resolved metadata
 * - Runs asynchronously to avoid blocking
 */
public class ExternalSourceResolver {

    private static final Logger LOGGER = LogManager.getLogger(ExternalSourceResolver.class);

    private final Executor executor;

    /**
     * Create a new ExternalSourceResolver.
     *
     * @param executor executor for async resolution operations
     */
    public ExternalSourceResolver(Executor executor) {
        this.executor = executor;
    }

    /**
     * Resolve external sources asynchronously.
     * This method runs in parallel with ES index resolution.
     *
     * @param paths list of external source paths to resolve
     * @param pathParams parameters for each path (S3 credentials, options, etc.)
     * @param listener callback with resolution result
     */
    public void resolve(
        List<String> paths,
        Map<String, Map<String, Expression>> pathParams,
        ActionListener<ExternalSourceResolution> listener
    ) {
        if (paths == null || paths.isEmpty()) {
            listener.onResponse(ExternalSourceResolution.EMPTY);
            return;
        }

        // Run resolution asynchronously to avoid blocking
        executor.execute(() -> {
            try {
                // Create a StorageManager with HttpStorageProvider registered for HTTP/HTTPS schemes
                // This allows ParquetFileAdapter to resolve HTTP URLs using the registry
                StorageProviderRegistry registry = createStorageProviderRegistry();
                StorageManager storageManager = new StorageManager(registry);

                try {
                    Map<String, ExternalSourceMetadata> resolved = new HashMap<>();

                    for (String path : paths) {
                        Map<String, Expression> params = pathParams.get(path);

                        // Detect the scheme and create appropriate configuration
                        String scheme = detectScheme(path);
                        Object config = createConfigForScheme(scheme, params);

                        try {
                            ExternalSourceMetadata metadata = resolveSingleSource(path, config, storageManager);
                            resolved.put(path, metadata);
                            LOGGER.info("Successfully resolved external source: {}", path);
                        } catch (Exception e) {
                            LOGGER.error("Failed to resolve external source [{}]: {}", path, e.getMessage(), e);
                            // For now, fail fast to surface errors during development/testing
                            // In production, we may want to allow partial resolution or return error info
                            String errorMessage = String.format(
                                "Failed to resolve external source [%s]: %s",
                                path,
                                e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName()
                            );
                            listener.onFailure(new RuntimeException(errorMessage, e));
                            return;
                        }
                    }

                    listener.onResponse(new ExternalSourceResolution(resolved));
                } finally {
                    storageManager.close();
                }
            } catch (Exception e) {
                listener.onFailure(e);
            }
        });
    }

    /**
     * Detect the URI scheme from a path.
     *
     * @param path the path to analyze
     * @return the scheme (e.g., "s3", "http", "https", "file"), or "s3" as default
     */
    private String detectScheme(String path) {
        if (path == null) {
            return "s3"; // Default
        }

        int colonIndex = path.indexOf(':');
        if (colonIndex > 0) {
            String scheme = path.substring(0, colonIndex).toLowerCase(Locale.ROOT);
            return scheme;
        }

        return "s3"; // Default for paths without explicit scheme
    }

    /**
     * Create appropriate configuration object based on the URI scheme.
     *
     * @param scheme the URI scheme
     * @param params the query parameters
     * @return configuration object (S3Configuration, HttpConfiguration, or null)
     */
    private Object createConfigForScheme(String scheme, Map<String, Expression> params) {
        if (scheme.equals("http") || scheme.equals("https")) {
            // For HTTP/HTTPS, return null to use registry-based provider
            // HttpConfiguration would require an ExecutorService, which we don't have here
            return null;
        } else if (scheme.equals("s3") || scheme.equals("s3a") || scheme.equals("s3n")) {
            // For S3 schemes, create S3Configuration
            return S3Configuration.fromParams(params);
        } else {
            // For file:// and other schemes, return null
            return null;
        }
    }

    /**
     * Create a StorageProviderRegistry with HttpStorageProvider registered for HTTP/HTTPS schemes.
     * This allows ParquetFileAdapter to resolve HTTP URLs.
     */
    private StorageProviderRegistry createStorageProviderRegistry() {
        StorageProviderRegistry registry = new StorageProviderRegistry();

        // Register HttpStorageProvider for HTTP and HTTPS schemes
        // Use the executor from this resolver to create HttpStorageProvider
        HttpConfiguration httpConfig = HttpConfiguration.defaults();

        // Try to cast executor to ExecutorService using pattern matching
        // ThreadPool executors are typically ExecutorService instances
        if (executor instanceof ExecutorService executorService) {
            try {
                HttpStorageProvider httpProvider = new HttpStorageProvider(httpConfig, executorService);
                registry.register("http", path -> httpProvider);
                registry.register("https", path -> httpProvider);
                LOGGER.debug("Registered HttpStorageProvider for http and https schemes");
            } catch (Exception e) {
                LOGGER.warn("Failed to register HttpStorageProvider: {}", e.getMessage());
            }
        } else {
            // If executor is not an ExecutorService, we can't create HttpStorageProvider
            // StorageManager will handle HTTP schemes via registry fallback or throw appropriate error
            LOGGER.debug("Executor is not an ExecutorService, skipping HttpStorageProvider registration");
        }

        return registry;
    }

    /**
     * Resolve a single external source path.
     * Determines the source type (Iceberg table vs Parquet file) and resolves accordingly.
     *
     * @param path the external source path
     * @param config configuration object (S3Configuration, HttpConfiguration, or null)
     * @param storageManager the StorageManager to use for Parquet file resolution
     * @return resolved metadata
     * @throws Exception if resolution fails
     */
    private ExternalSourceMetadata resolveSingleSource(String path, @Nullable Object config, StorageManager storageManager)
        throws Exception {
        SourceType type = detectSourceType(path);
        LOGGER.info("Resolving external source: path=[{}], detected type=[{}]", path, type);

        return switch (type) {
            case ICEBERG -> {
                // Iceberg currently only supports S3Configuration
                S3Configuration s3Config = config instanceof S3Configuration s3 ? s3 : null;
                yield IcebergCatalogAdapter.resolveTable(path, s3Config);
            }
            case PARQUET -> ParquetFileAdapter.resolveParquetFile(path, config, storageManager);
        };
    }

    /**
     * Detect the type of external source from the path.
     * Uses simple heuristics:
     * - If path ends with .parquet, it's a Parquet file
     * - Otherwise, assume it's an Iceberg table
     *
     * Future enhancements could include:
     * - Check for Iceberg metadata files
     * - Allow explicit type specification in parameters
     * - Support more source types (Delta Lake, Hudi, etc.)
     */
    private SourceType detectSourceType(String path) {
        String lowerPath = path.toLowerCase(Locale.ROOT);
        boolean isParquet = lowerPath.endsWith(".parquet");
        LOGGER.debug("Detecting source type for path: [{}], ends with .parquet: [{}]", path, isParquet);

        if (isParquet) {
            LOGGER.debug("Detected as PARQUET file");
            return SourceType.PARQUET;
        }

        // Check if path looks like an Iceberg table path
        // Iceberg tables typically have metadata directories
        // Default to Iceberg if not explicitly Parquet
        LOGGER.debug("Detected as ICEBERG table");
        return SourceType.ICEBERG;
    }

    private enum SourceType {
        ICEBERG,
        PARQUET
    }
}
