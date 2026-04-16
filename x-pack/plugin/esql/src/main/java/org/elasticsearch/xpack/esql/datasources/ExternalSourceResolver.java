/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.datasources;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.Nullability;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.datasources.cache.ExternalSourceCacheService;
import org.elasticsearch.xpack.esql.datasources.cache.ListingCacheKey;
import org.elasticsearch.xpack.esql.datasources.cache.SchemaCacheEntry;
import org.elasticsearch.xpack.esql.datasources.cache.SchemaCacheKey;
import org.elasticsearch.xpack.esql.datasources.glob.GlobExpander;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalSourceFactory;
import org.elasticsearch.xpack.esql.datasources.spi.FileList;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReader;
import org.elasticsearch.xpack.esql.datasources.spi.SourceMetadata;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.datasources.spi.StorageProvider;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;

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

    static final String CONFIG_SCHEMA_RESOLUTION = "schema_resolution";

    private static final int MAX_PARALLEL_METADATA_READS = 16;

    private final Executor executor;
    private final DataSourceModule dataSourceModule;
    private final Settings settings;
    private final ExternalSourceCacheService cacheService;

    public ExternalSourceResolver(Executor executor, DataSourceModule dataSourceModule) {
        this(executor, dataSourceModule, Settings.EMPTY, null);
    }

    public ExternalSourceResolver(Executor executor, DataSourceModule dataSourceModule, Settings settings) {
        this(executor, dataSourceModule, settings, null);
    }

    public ExternalSourceResolver(
        Executor executor,
        DataSourceModule dataSourceModule,
        Settings settings,
        @Nullable ExternalSourceCacheService cacheService
    ) {
        this.executor = executor;
        this.dataSourceModule = dataSourceModule;
        this.settings = settings;
        this.cacheService = cacheService;
    }

    public void resolve(
        List<String> paths,
        Map<String, Map<String, Expression>> pathParams,
        ActionListener<ExternalSourceResolution> listener
    ) {
        resolve(paths, pathParams, null, listener);
    }

    public void resolve(
        List<String> paths,
        Map<String, Map<String, Expression>> pathParams,
        @Nullable Map<String, List<PartitionFilterHintExtractor.PartitionFilterHint>> filterHints,
        ActionListener<ExternalSourceResolution> listener
    ) {
        if (paths == null || paths.isEmpty()) {
            listener.onResponse(ExternalSourceResolution.EMPTY);
            return;
        }

        executor.execute(() -> {
            try {
                Map<String, ExternalSourceResolution.ResolvedSource> resolved = Maps.newHashMapWithExpectedSize(paths.size());

                for (String path : paths) {
                    Map<String, Expression> params = pathParams.get(path);
                    Map<String, Object> config = paramsToConfigMap(params);
                    List<PartitionFilterHintExtractor.PartitionFilterHint> hints = filterHints != null ? filterHints.get(path) : null;
                    boolean hivePartitioning = isHivePartitioningEnabled(config);

                    try {
                        ExternalSourceResolution.ResolvedSource resolvedSource = resolveSource(path, config, hints, hivePartitioning);
                        resolved.put(path, resolvedSource);
                        LOGGER.info("Successfully resolved external source: {}", path);
                    } catch (Exception e) {
                        LOGGER.error("Failed to resolve external source [{}]: {}", path, e.getMessage(), e);
                        String exceptionMessage = e.getMessage();
                        String errorDetail = exceptionMessage != null ? exceptionMessage : e.getClass().getSimpleName();
                        String errorMessage = String.format(Locale.ROOT, "Failed to resolve external source [%s]: %s", path, errorDetail);
                        listener.onFailure(new ElasticsearchException(errorMessage, e));
                        return;
                    }
                }

                listener.onResponse(new ExternalSourceResolution(resolved));
            } catch (Exception e) {
                listener.onFailure(e);
            }
        });
    }

    private ExternalSourceResolution.ResolvedSource resolveSource(
        String path,
        Map<String, Object> config,
        @Nullable List<PartitionFilterHintExtractor.PartitionFilterHint> hints,
        boolean hivePartitioning
    ) throws Exception {
        LOGGER.info("Resolving external source: path=[{}]", path);

        if (GlobExpander.isMultiFile(path)) {
            return resolveMultiFileSource(path, config, hints, hivePartitioning);
        }

        SourceMetadata metadata = resolveSingleSource(path, config);
        ExternalSourceMetadata extMetadata = wrapAsExternalSourceMetadata(metadata, config);
        return new ExternalSourceResolution.ResolvedSource(extMetadata, FileList.UNRESOLVED);
    }

    private ExternalSourceResolution.ResolvedSource resolveMultiFileSource(
        String path,
        Map<String, Object> config,
        @Nullable List<PartitionFilterHintExtractor.PartitionFilterHint> hints,
        boolean hivePartitioning
    ) throws Exception {
        StoragePath storagePath = StoragePath.of(path);

        FormatReader.SchemaResolution schemaResolution = parseSchemaResolution(config);

        if (schemaResolution != FormatReader.SchemaResolution.FIRST_FILE_WINS) {
            StorageProvider provider = resolveProvider(storagePath, config);
            int maxDiscoveredFiles = ExternalSourceSettings.MAX_DISCOVERED_FILES.get(settings);
            int maxGlobExpansion = ExternalSourceSettings.MAX_GLOB_EXPANSION.get(settings);
            FileList raw = path.indexOf(',') >= 0
                ? GlobExpander.expandCommaSeparated(path, provider, hints, hivePartitioning, maxDiscoveredFiles, maxGlobExpansion)
                : GlobExpander.expandGlob(path, provider, hints, hivePartitioning, maxDiscoveredFiles, maxGlobExpansion);
            if (raw.fileCount() == 0) {
                throw new IllegalArgumentException("Glob pattern matched no files: " + path);
            }
            return resolveMultiFileWithReconciliation(raw, config, schemaResolution);
        }

        boolean cacheable = cacheService != null
            && cacheService.isEnabled()
            && "http".equals(storagePath.scheme()) == false
            && "https".equals(storagePath.scheme()) == false;

        FileList listing;
        if (cacheable) {
            ListingCacheKey listingKey = ListingCacheKey.build(storagePath.scheme(), storagePath.host(), storagePath.path(), config);
            listing = cacheService.getOrComputeListing(
                listingKey,
                k -> expandAndCompact(path, config, hints, hivePartitioning, storagePath)
            );
        } else {
            listing = expandAndCompact(path, config, hints, hivePartitioning, storagePath);
        }

        if (listing.fileCount() == 0) {
            throw new IllegalArgumentException("Glob pattern matched no files: " + path);
        }

        int anchor = 0;
        for (int i = 1; i < listing.fileCount(); i++) {
            if (listing.path(i).toString().compareTo(listing.path(anchor).toString()) < 0) {
                anchor = i;
            }
        }

        StoragePath anchorPath = listing.path(anchor);
        long anchorMtime = listing.lastModifiedMillis(anchor);

        ExternalSourceMetadata extMetadata;
        if (cacheable) {
            String formatType = detectFormatType(anchorPath);
            SchemaCacheKey schemaKey = SchemaCacheKey.build(anchorPath.toString(), anchorMtime, formatType, config);
            SchemaCacheEntry schemaEntry = cacheService.getOrComputeSchema(schemaKey, k -> {
                SourceMetadata meta = resolveSingleSource(anchorPath.toString(), config);
                return SchemaCacheEntry.from(meta.schema(), meta.sourceType(), meta.location(), meta.sourceMetadata());
            });
            List<Attribute> schema = schemaEntry.toAttributes();
            extMetadata = buildMetadataFromCache(schemaEntry, schema, config);
        } else {
            SourceMetadata metadata = resolveSingleSource(anchorPath.toString(), config);
            extMetadata = wrapAsExternalSourceMetadata(metadata, config);
        }

        PartitionMetadata partitionMetadata = listing.partitionMetadata();
        if (partitionMetadata != null && partitionMetadata.isEmpty() == false) {
            extMetadata = enrichSchemaWithPartitionColumns(extMetadata, partitionMetadata);
        }

        return new ExternalSourceResolution.ResolvedSource(extMetadata, listing);
    }

    private FileList expandAndCompact(
        String path,
        Map<String, Object> config,
        @Nullable List<PartitionFilterHintExtractor.PartitionFilterHint> hints,
        boolean hivePartitioning,
        StoragePath storagePath
    ) throws Exception {
        StorageProvider provider = resolveProvider(storagePath, config);
        int maxDiscoveredFiles = ExternalSourceSettings.MAX_DISCOVERED_FILES.get(settings);
        int maxGlobExpansion = ExternalSourceSettings.MAX_GLOB_EXPANSION.get(settings);
        return GlobExpander.expandAndCompact(path, provider, hints, hivePartitioning, storagePath, maxDiscoveredFiles, maxGlobExpansion);
    }

    private StorageProvider resolveProvider(StoragePath storagePath, Map<String, Object> config) {
        StorageProviderRegistry registry = dataSourceModule.storageProviderRegistry();
        if (config != null && config.isEmpty() == false) {
            return registry.createProvider(storagePath.scheme(), settings, config);
        }
        return registry.provider(storagePath);
    }

    private static String detectFormatType(StoragePath path) {
        String name = path.objectName();
        if (name == null) {
            return "";
        }
        int dot = name.lastIndexOf('.');
        return dot >= 0 ? name.substring(dot) : "";
    }

    private static ExternalSourceMetadata buildMetadataFromCache(
        SchemaCacheEntry entry,
        List<Attribute> schema,
        Map<String, Object> config
    ) {
        return new ExternalSourceMetadata() {
            @Override
            public String location() {
                return entry.location();
            }

            @Override
            public List<Attribute> schema() {
                return schema;
            }

            @Override
            public String sourceType() {
                return entry.sourceType();
            }

            @Override
            public Map<String, Object> sourceMetadata() {
                return entry.safeMetadata();
            }

            @Override
            public Map<String, Object> config() {
                return config != null ? config : Map.of();
            }
        };
    }

    private ExternalSourceResolution.ResolvedSource resolveMultiFileWithReconciliation(
        FileList fileList,
        Map<String, Object> config,
        FormatReader.SchemaResolution schemaResolution
    ) throws Exception {
        long startNanos = System.nanoTime();
        Map<StoragePath, SourceMetadata> allMetadata = readAllFileMetadata(fileList, config);
        long durationMs = (System.nanoTime() - startNanos) / 1_000_000;

        LOGGER.debug("Schema reconciliation [{}]: scanned {} files in {}ms", schemaResolution, allMetadata.size(), durationMs);

        StoragePath firstFile = fileList.path(0);
        SchemaReconciliation.Result result;
        if (schemaResolution == FormatReader.SchemaResolution.STRICT) {
            result = SchemaReconciliation.reconcileStrict(firstFile, allMetadata);
        } else {
            result = SchemaReconciliation.reconcileUnionByName(allMetadata);
        }

        List<Attribute> unifiedSchema = result.unifiedSchema();
        SourceMetadata firstMeta = allMetadata.get(firstFile);
        ExternalSourceMetadata extMetadata = buildUnifiedMetadata(firstMeta, unifiedSchema, config);

        PartitionMetadata partitionMetadata = fileList.partitionMetadata();
        if (partitionMetadata != null && partitionMetadata.isEmpty() == false) {
            extMetadata = enrichSchemaWithPartitionColumns(extMetadata, partitionMetadata);
        }

        FileList enriched = GlobExpander.withSchemaInfo(fileList, result.perFileInfo());
        return new ExternalSourceResolution.ResolvedSource(extMetadata, enriched);
    }

    /**
     * Reads metadata from all files in parallel with bounded concurrency.
     * Uses a semaphore to cap the number of concurrent metadata reads.
     */
    private Map<StoragePath, SourceMetadata> readAllFileMetadata(FileList fileList, Map<String, Object> config) throws Exception {
        int fileCount = fileList.fileCount();

        if (fileCount == 1) {
            StoragePath filePath = fileList.path(0);
            SourceMetadata meta = resolveSingleSource(filePath.toString(), config);
            return Map.of(filePath, meta);
        }

        Semaphore semaphore = new Semaphore(Math.min(fileCount, MAX_PARALLEL_METADATA_READS));
        AtomicBoolean failed = new AtomicBoolean(false);

        @SuppressWarnings("unchecked")
        CompletableFuture<Map.Entry<StoragePath, SourceMetadata>>[] futures = new CompletableFuture[fileCount];

        for (int i = 0; i < fileCount; i++) {
            StoragePath filePath = fileList.path(i);
            futures[i] = CompletableFuture.supplyAsync(() -> {
                if (failed.get()) {
                    return null;
                }
                try {
                    semaphore.acquire();
                    try {
                        SourceMetadata meta = resolveSingleSource(filePath.toString(), config);
                        return Map.entry(filePath, meta);
                    } finally {
                        semaphore.release();
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException("Interrupted while reading metadata from [" + filePath + "]", e);
                } catch (Exception e) {
                    failed.set(true);
                    throw new RuntimeException("Failed to read metadata from [" + filePath + "]: " + e.getMessage(), e);
                }
            }, executor);
        }

        try {
            CompletableFuture.allOf(futures).join();
        } catch (CompletionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof RuntimeException rte) {
                throw rte;
            }
            throw new RuntimeException("Failed to read file metadata in parallel", cause != null ? cause : e);
        }

        Map<StoragePath, SourceMetadata> result = new LinkedHashMap<>();
        for (CompletableFuture<Map.Entry<StoragePath, SourceMetadata>> future : futures) {
            Map.Entry<StoragePath, SourceMetadata> entry = future.get();
            if (entry != null) {
                result.put(entry.getKey(), entry.getValue());
            }
        }
        return result;
    }

    private ExternalSourceMetadata buildUnifiedMetadata(
        SourceMetadata referenceMeta,
        List<Attribute> unifiedSchema,
        Map<String, Object> queryConfig
    ) {
        Map<String, Object> mergedConfig = mergeConfigs(referenceMeta.config(), queryConfig);
        List<Attribute> schema = List.copyOf(unifiedSchema);
        return new ExternalSourceMetadata() {
            @Override
            public String location() {
                return referenceMeta.location();
            }

            @Override
            public List<Attribute> schema() {
                return schema;
            }

            @Override
            public String sourceType() {
                return referenceMeta.sourceType();
            }

            @Override
            public Map<String, Object> sourceMetadata() {
                return referenceMeta.sourceMetadata();
            }

            @Override
            public Map<String, Object> config() {
                return mergedConfig;
            }
        };
    }

    static FormatReader.SchemaResolution parseSchemaResolution(@Nullable Map<String, Object> config) {
        if (config == null) {
            return FormatReader.SchemaResolution.FIRST_FILE_WINS;
        }
        Object value = config.get(CONFIG_SCHEMA_RESOLUTION);
        if (value == null) {
            return FormatReader.SchemaResolution.FIRST_FILE_WINS;
        }
        return switch (value.toString().toLowerCase(Locale.ROOT)) {
            case "first_file_wins" -> FormatReader.SchemaResolution.FIRST_FILE_WINS;
            case "strict" -> FormatReader.SchemaResolution.STRICT;
            case "union_by_name" -> FormatReader.SchemaResolution.UNION_BY_NAME;
            default -> throw new IllegalArgumentException(
                "Unknown schema_resolution value [" + value + "]. Valid values are: first_file_wins, strict, union_by_name"
            );
        };
    }

    private static Map<String, Object> mergeConfigs(
        @Nullable Map<String, Object> metadataConfig,
        @Nullable Map<String, Object> queryConfig
    ) {
        if (metadataConfig == null || metadataConfig.isEmpty()) {
            return queryConfig != null ? queryConfig : Map.of();
        }
        if (queryConfig == null || queryConfig.isEmpty()) {
            return metadataConfig;
        }
        Map<String, Object> merged = new HashMap<>(metadataConfig);
        merged.putAll(queryConfig);
        return merged;
    }

    private static boolean isHivePartitioningEnabled(Map<String, Object> config) {
        if (config == null) {
            return true;
        }
        Object value = config.get(PartitionConfig.CONFIG_PARTITIONING_HIVE);
        if (value == null) {
            return true;
        }
        return "false".equalsIgnoreCase(value.toString()) == false;
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

    static ExternalSourceMetadata enrichSchemaWithPartitionColumns(ExternalSourceMetadata metadata, PartitionMetadata partitionMetadata) {
        List<Attribute> originalSchema = metadata.schema();
        Map<String, DataType> partitionColumns = partitionMetadata.partitionColumns();

        Set<String> partitionNames = new LinkedHashSet<>(partitionColumns.keySet());
        List<Attribute> enrichedSchema = new ArrayList<>();

        for (Attribute attr : originalSchema) {
            if (partitionNames.contains(attr.name()) == false) {
                enrichedSchema.add(attr);
            }
        }

        for (Map.Entry<String, DataType> entry : partitionColumns.entrySet()) {
            String name = entry.getKey();
            DataType type = entry.getValue();
            enrichedSchema.add(new ReferenceAttribute(Source.EMPTY, null, name, type, Nullability.TRUE, null, true));
        }

        List<Attribute> finalSchema = List.copyOf(enrichedSchema);

        return new ExternalSourceMetadata() {
            @Override
            public String location() {
                return metadata.location();
            }

            @Override
            public List<Attribute> schema() {
                return finalSchema;
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
                return metadata.config();
            }
        };
    }

    private Map<String, Object> paramsToConfigMap(@Nullable Map<String, Expression> params) {
        if (params == null || params.isEmpty()) {
            return Map.of();
        }

        Map<String, Object> config = Maps.newHashMapWithExpectedSize(params.size());
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

    /**
     * Validates that datasource plugins export ReferenceAttribute only.
     * Called when receiving schema from any plugin (FormatReader, TableCatalog, Connector).
     */
    private static void validateSchemaUsesOnlyReferenceAttributes(List<Attribute> schema) {
        for (Attribute attr : schema) {
            if (attr instanceof ReferenceAttribute == false) {
                throw new IllegalArgumentException(
                    "Datasource schema must contain only ReferenceAttribute, but found "
                        + attr.getClass().getSimpleName()
                        + " for column ["
                        + attr.name()
                        + "]"
                );
            }
        }
    }

    private ExternalSourceMetadata wrapAsExternalSourceMetadata(SourceMetadata metadata, Map<String, Object> queryConfig) {
        validateSchemaUsesOnlyReferenceAttributes(metadata.schema());

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
            public java.util.List<Attribute> schema() {
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
