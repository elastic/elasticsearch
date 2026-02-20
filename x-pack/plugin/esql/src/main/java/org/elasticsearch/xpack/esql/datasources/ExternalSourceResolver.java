/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.datasources;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReader;
import org.elasticsearch.xpack.esql.datasources.spi.SourceMetadata;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.datasources.spi.StorageProvider;
import org.elasticsearch.xpack.esql.datasources.spi.TableCatalog;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.Executor;

/**
 * Resolver for external data sources (Iceberg tables, Parquet files, etc.).
 * This runs in parallel with IndexResolver to resolve external source metadata.
 * <p>
 * Following the same pattern as IndexResolver, this resolver:
 * <ul>
 *   <li>Takes a list of external source paths to resolve</li>
 *   <li>Performs I/O operations to fetch metadata (from S3/Iceberg catalogs)</li>
 *   <li>Returns ExternalSourceResolution containing resolved metadata</li>
 *   <li>Runs asynchronously to avoid blocking</li>
 * </ul>
 * <p>
 * <b>Registry-based resolution:</b> This resolver uses the registries from {@link DataSourceModule}
 * to find appropriate handlers for different source types:
 * <ul>
 *   <li>{@link TableCatalog} for table-based sources (Iceberg, Delta Lake)</li>
 *   <li>{@link FormatReader} for file-based sources (Parquet, CSV)</li>
 * </ul>
 * <p>
 * <b>Configuration handling:</b> Query parameters are converted to a generic {@code Map<String, Object>}
 * instead of source-specific classes like S3Configuration. This allows the SPI to remain generic
 * while source-specific implementations can interpret the configuration as needed.
 */
public class ExternalSourceResolver {

    private static final Logger LOGGER = LogManager.getLogger(ExternalSourceResolver.class);

    private final Executor executor;
    private final DataSourceModule dataSourceModule;
    private final Settings settings;

    public ExternalSourceResolver(Executor executor, DataSourceModule dataSourceModule) {
        this(executor, dataSourceModule, Settings.EMPTY);
    }

    public ExternalSourceResolver(Executor executor, DataSourceModule dataSourceModule, Settings settings) {
        this.executor = executor;
        this.dataSourceModule = dataSourceModule;
        this.settings = settings;
    }

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
                // Use the StorageProviderRegistry from DataSourceModule
                // This registry is populated with all discovered storage providers
                StorageProviderRegistry registry = dataSourceModule.storageProviderRegistry();
                StorageManager storageManager = new StorageManager(registry, settings);

                try {
                    Map<String, ExternalSourceResolution.ResolvedSource> resolved = new HashMap<>();

                    for (String path : paths) {
                        Map<String, Expression> params = pathParams.get(path);

                        // Convert query parameters to generic config map
                        Map<String, Object> config = paramsToConfigMap(params);

                        try {
                            ExternalSourceResolution.ResolvedSource resolvedSource = resolveSource(path, config, storageManager);
                            resolved.put(path, resolvedSource);
                            LOGGER.info("Successfully resolved external source: {}", path);
                        } catch (Exception e) {
                            LOGGER.error("Failed to resolve external source [{}]: {}", path, e.getMessage(), e);
                            String exceptionMessage = e.getMessage();
                            String errorDetail = exceptionMessage != null ? exceptionMessage : e.getClass().getSimpleName();
                            String errorMessage = String.format(
                                Locale.ROOT,
                                "Failed to resolve external source [%s]: %s",
                                path,
                                errorDetail
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

    private ExternalSourceResolution.ResolvedSource resolveSource(String path, Map<String, Object> config, StorageManager storageManager)
        throws Exception {
        LOGGER.info("Resolving external source: path=[{}]", path);

        if (GlobExpander.isMultiFile(path)) {
            return resolveMultiFileSource(path, config, storageManager);
        }

        SourceMetadata metadata = resolveSingleSource(path, config, storageManager);
        ExternalSourceMetadata extMetadata = wrapAsExternalSourceMetadata(metadata, config);
        return new ExternalSourceResolution.ResolvedSource(extMetadata, FileSet.UNRESOLVED);
    }

    private ExternalSourceResolution.ResolvedSource resolveMultiFileSource(
        String path,
        Map<String, Object> config,
        StorageManager storageManager
    ) throws Exception {
        StoragePath storagePath = StoragePath.of(path);
        StorageProvider provider = storageManager.provider(storagePath, config);

        FileSet fileSet;
        if (path.indexOf(',') >= 0) {
            fileSet = GlobExpander.expandCommaSeparated(path, provider);
        } else {
            fileSet = GlobExpander.expandGlob(path, provider);
        }

        if (fileSet.isEmpty()) {
            throw new IllegalArgumentException("Glob pattern matched no files: " + path);
        }

        // Use the first file to infer format and read metadata
        StoragePath firstFile = fileSet.files().get(0).path();
        FormatReaderRegistry formatRegistry = dataSourceModule.formatReaderRegistry();
        FormatReader reader = formatRegistry.byExtension(firstFile.objectName());

        StorageObject storageObject = storageManager.newStorageObject(firstFile.toString(), config);
        SourceMetadata metadata = reader.metadata(storageObject);

        ExternalSourceMetadata extMetadata = wrapAsExternalSourceMetadata(metadata, config);
        return new ExternalSourceResolution.ResolvedSource(extMetadata, fileSet);
    }

    private SourceMetadata resolveSingleSource(String path, Map<String, Object> config, StorageManager storageManager) throws Exception {
        // Strategy 1: Try registered TableCatalogs
        SourceMetadata metadata = tryTableCatalogs(path, config);
        if (metadata != null) {
            LOGGER.debug("Resolved via TableCatalog: {}", metadata.sourceType());
            return metadata;
        }

        // Strategy 2: Try FormatReader based on file extension
        metadata = tryFormatReaders(path, config, storageManager);
        if (metadata != null) {
            LOGGER.debug("Resolved via FormatReader: {}", metadata.sourceType());
            return metadata;
        }

        // Strategy 3: Fall back to legacy adapters for backward compatibility
        return resolveLegacy(path, config, storageManager);
    }

    @Nullable
    private SourceMetadata tryTableCatalogs(String path, Map<String, Object> config) {
        // Check if any registered TableCatalog can handle this path
        // Currently, we check for "iceberg" catalog if the path looks like an Iceberg table
        SourceType detectedType = detectSourceType(path);

        if (detectedType == SourceType.ICEBERG && dataSourceModule.hasTableCatalog("iceberg")) {
            try (TableCatalog catalog = dataSourceModule.createTableCatalog("iceberg", settings)) {
                if (catalog.canHandle(path)) {
                    return catalog.metadata(path, config);
                }
            } catch (IOException e) {
                LOGGER.debug("TableCatalog 'iceberg' failed for path [{}]: {}", path, e.getMessage());
            }
        }

        // Try other registered catalogs
        // Future: iterate over all registered catalogs and check canHandle()
        return null;
    }

    @Nullable
    private SourceMetadata tryFormatReaders(String path, Map<String, Object> config, StorageManager storageManager) {
        FormatReaderRegistry formatRegistry = dataSourceModule.formatReaderRegistry();

        // Try to get a format reader by file extension
        try {
            FormatReader reader = formatRegistry.byExtension(path);
            if (reader != null) {
                // Get storage object for the path
                StorageObject storageObject = getStorageObject(path, config, storageManager);
                return reader.metadata(storageObject);
            }
        } catch (Exception e) {
            LOGGER.debug("FormatReader lookup failed for path [{}]: {}", path, e.getMessage());
        }

        return null;
    }

    private SourceMetadata resolveLegacy(String path, Map<String, Object> config, StorageManager storageManager) throws Exception {
        SourceType type = detectSourceType(path);
        LOGGER.info("Attempting legacy resolution for path=[{}], detected type=[{}]", path, type);

        // Legacy adapters have been moved to separate modules
        throw new UnsupportedOperationException(
            "No handler found for source type ["
                + type
                + "] at path ["
                + path
                + "]. "
                + "Please ensure the appropriate data source plugin is installed."
        );
    }

    private StorageObject getStorageObject(String path, Map<String, Object> config, StorageManager storageManager) throws Exception {
        StoragePath storagePath = StoragePath.of(path);
        String scheme = storagePath.scheme().toLowerCase(Locale.ROOT);

        if ((scheme.equals("http") || scheme.equals("https")) && config.isEmpty()) {
            // For HTTP/HTTPS with no config, use registry-based approach
            return storageManager.newStorageObject(path);
        } else {
            // For S3 and file schemes, or HTTP with config, use config-based approach
            // StorageManager now accepts Map<String, Object> directly
            return storageManager.newStorageObject(path, config);
        }
    }

    private Map<String, Object> paramsToConfigMap(@Nullable Map<String, Expression> params) {
        if (params == null || params.isEmpty()) {
            return Map.of();
        }

        Map<String, Object> config = new HashMap<>();
        for (Map.Entry<String, Expression> entry : params.entrySet()) {
            String key = entry.getKey();
            Expression expr = entry.getValue();
            if (expr instanceof Literal literal) {
                Object value = literal.value();
                if (value instanceof BytesRef bytesRef) {
                    config.put(key, BytesRefs.toString(bytesRef));
                } else if (value != null) {
                    config.put(key, value.toString());
                }
            }
        }
        return config;
    }

    private ExternalSourceMetadata wrapAsExternalSourceMetadata(SourceMetadata metadata, Map<String, Object> queryConfig) {
        if (metadata instanceof ExternalSourceMetadata extMetadata) {
            // If the metadata already carries config (e.g. from a TableCatalog), preserve it.
            // Otherwise, overlay the query-level config (from WITH clause) so that connection
            // parameters (endpoint, credentials) reach the execution phase.
            if (extMetadata.config() != null && extMetadata.config().isEmpty() == false) {
                return extMetadata;
            }
        }

        // Create a wrapper that delegates to the SourceMetadata but uses the query-level
        // config. This is scheme-agnostic: S3, HTTP, LOCAL, or any future backend — the
        // config from the WITH clause is forwarded transparently to the execution phase.
        return new ExternalSourceMetadata() {
            @Override
            public String location() {
                return metadata.location();
            }

            @Override
            public java.util.List<org.elasticsearch.xpack.esql.core.expression.Attribute> schema() {
                return metadata.schema();
            }

            @Override
            public String sourceType() {
                return metadata.sourceType();
            }

            @Override
            public Map<String, Object> sourceMetadata() {
                return metadata.sourceMetadata();
            }

            @Override
            public Map<String, Object> config() {
                return queryConfig;
            }
        };
    }

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
