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
import org.elasticsearch.xpack.esql.datasources.spi.ExternalSourceFactory;
import org.elasticsearch.xpack.esql.datasources.spi.SourceMetadata;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.datasources.spi.StorageProvider;

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
 * <b>Registry-based resolution:</b> This resolver iterates the {@link ExternalSourceFactory}
 * instances collected by {@link DataSourceModule} to find the first factory that can handle
 * a given path. File-based sources (Parquet, CSV) are handled by the framework-internal
 * {@code FileSourceFactory} registered as a catch-all fallback.
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

        executor.execute(() -> {
            try {
                Map<String, ExternalSourceResolution.ResolvedSource> resolved = new HashMap<>();

                for (String path : paths) {
                    Map<String, Expression> params = pathParams.get(path);
                    Map<String, Object> config = paramsToConfigMap(params);

                    try {
                        ExternalSourceResolution.ResolvedSource resolvedSource = resolveSource(path, config);
                        resolved.put(path, resolvedSource);
                        LOGGER.info("Successfully resolved external source: {}", path);
                    } catch (Exception e) {
                        LOGGER.error("Failed to resolve external source [{}]: {}", path, e.getMessage(), e);
                        String exceptionMessage = e.getMessage();
                        String errorDetail = exceptionMessage != null ? exceptionMessage : e.getClass().getSimpleName();
                        String errorMessage = String.format(Locale.ROOT, "Failed to resolve external source [%s]: %s", path, errorDetail);
                        listener.onFailure(new RuntimeException(errorMessage, e));
                        return;
                    }
                }

                listener.onResponse(new ExternalSourceResolution(resolved));
            } catch (Exception e) {
                listener.onFailure(e);
            }
        });
    }

    private ExternalSourceResolution.ResolvedSource resolveSource(String path, Map<String, Object> config) throws Exception {
        LOGGER.info("Resolving external source: path=[{}]", path);

        if (GlobExpander.isMultiFile(path)) {
            return resolveMultiFileSource(path, config);
        }

        SourceMetadata metadata = resolveSingleSource(path, config);
        ExternalSourceMetadata extMetadata = wrapAsExternalSourceMetadata(metadata, config);
        return new ExternalSourceResolution.ResolvedSource(extMetadata, FileSet.UNRESOLVED);
    }

    private ExternalSourceResolution.ResolvedSource resolveMultiFileSource(String path, Map<String, Object> config) throws Exception {
        StoragePath storagePath = StoragePath.of(path);
        StorageProviderRegistry registry = dataSourceModule.storageProviderRegistry();

        StorageProvider provider;
        if (config != null && config.isEmpty() == false) {
            provider = registry.createProvider(storagePath.scheme(), settings, config);
        } else {
            provider = registry.provider(storagePath);
        }

        FileSet fileSet;
        if (path.indexOf(',') >= 0) {
            fileSet = GlobExpander.expandCommaSeparated(path, provider);
        } else {
            fileSet = GlobExpander.expandGlob(path, provider);
        }

        if (fileSet.isEmpty()) {
            throw new IllegalArgumentException("Glob pattern matched no files: " + path);
        }

        StoragePath firstFile = fileSet.files().get(0).path();
        SourceMetadata metadata = resolveSingleSource(firstFile.toString(), config);

        ExternalSourceMetadata extMetadata = wrapAsExternalSourceMetadata(metadata, config);
        return new ExternalSourceResolution.ResolvedSource(extMetadata, fileSet);
    }

    private SourceMetadata resolveSingleSource(String path, Map<String, Object> config) {
        // Early scheme validation: reject unsupported schemes without loading any plugin factories
        try {
            StoragePath parsed = StoragePath.of(path);
            DataSourceCapabilities capabilities = dataSourceModule.capabilities();
            if (capabilities != null && capabilities.supportsScheme(parsed.scheme()) == false) {
                throw new UnsupportedSchemeException(
                    "Unsupported storage scheme [" + parsed.scheme() + "]. Supported: " + capabilities.supportedSchemesString()
                );
            }
        } catch (UnsupportedSchemeException e) {
            throw e;
        } catch (IllegalArgumentException e) {
            // Path parsing failed -- let the factory iteration handle it
        }

        Exception lastFailure = null;
        for (ExternalSourceFactory factory : dataSourceModule.sourceFactories().values()) {
            if (factory.canHandle(path)) {
                try {
                    return factory.resolveMetadata(path, config);
                } catch (Exception e) {
                    LOGGER.debug("Factory [{}] claimed path [{}] but failed: {}", factory.type(), path, e.getMessage());
                    lastFailure = e;
                }
            }
        }
        if (lastFailure != null) {
            throw new IllegalArgumentException("Failed to resolve metadata for [" + path + "]", lastFailure);
        }
        var sources = String.join(", ", dataSourceModule.sourceFactories().keySet());
        throw new UnsupportedOperationException(
            "No handler found for source at path ["
                + path
                + "]. "
                + "Please ensure the appropriate data source plugin is installed. "
                + "Known handlers: ["
                + sources
                + "]."
        );
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
            if (extMetadata.config() != null && extMetadata.config().isEmpty() == false) {
                return extMetadata;
            }
        }

        // Merge the config from resolveMetadata (e.g. endpoint for Flight) with query-level params (WITH clause).
        // Query-level params take precedence so users can override connector-resolved values.
        Map<String, Object> mergedConfig;
        Map<String, Object> metadataConfig = metadata.config();
        if (metadataConfig != null && metadataConfig.isEmpty() == false) {
            mergedConfig = new HashMap<>(metadataConfig);
            if (queryConfig != null) {
                mergedConfig.putAll(queryConfig);
            }
        } else {
            mergedConfig = queryConfig;
        }

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
                return mergedConfig;
            }
        };
    }
}
