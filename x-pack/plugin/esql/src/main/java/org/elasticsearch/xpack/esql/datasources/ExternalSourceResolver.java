/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
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
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.datasources.spi.StorageProvider;
import org.elasticsearch.xpack.esql.datasources.utils.BoundedParallelGather;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
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

    static final String CONFIG_SCHEMA_RESOLUTION = "schema_resolution";

    public static final Set<String> CONFIG_KEYS = Set.of(CONFIG_SCHEMA_RESOLUTION);

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
        Map<String, Map<String, Object>> pathConfigs,
        ActionListener<ExternalSourceResolution> listener
    ) {
        resolve(paths, pathConfigs, null, listener);
    }

    public void resolve(
        List<String> paths,
        Map<String, Map<String, Object>> pathConfigs,
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
                    Map<String, Object> config = pathConfigs.getOrDefault(path, Map.of());
                    List<PartitionFilterHintExtractor.PartitionFilterHint> hints = filterHints != null ? filterHints.get(path) : null;
                    boolean hivePartitioning = isHivePartitioningEnabled(config);

                    try {
                        ExternalSourceResolution.ResolvedSource resolvedSource = resolveSource(path, config, hints, hivePartitioning);
                        resolved.put(path, resolvedSource);
                        LOGGER.debug("Successfully resolved external source: {}", path);
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
        LOGGER.debug("Resolving external source: path=[{}]", path);

        if (GlobExpander.isMultiFile(path)) {
            return resolveMultiFileSource(path, config, hints, hivePartitioning);
        }

        /*
         * A concrete one-entry FileList is required so {@link org.elasticsearch.xpack.esql.datasources.FileSplitProvider}
         * can discover block-aligned splits for compressed files (e.g. .json.bz2). UNRESOLVED lists skip split discovery,
         * which forces coordinator execution down paths that never see per-split byte ranges and yields incorrect counts.
         */
        StoragePath storagePath = StoragePath.of(path);
        StorageProvider provider = resolveProvider(storagePath, config);

        ExternalSourceMetadata extMetadata;
        StorageObject object;
        if (isCacheable(provider)) {
            // Stat the file first (cheap HEAD/stat) to get mtime for the cache key.
            // Null mtime (e.g. gRPC/Flight, GCS/Azure fixtures) falls back to EPOCH so the
            // cache key is stable; providers that never report trustworthy mtime should
            // eventually return supportsStableMetadata() == false to bypass caching entirely.
            object = provider.newObject(storagePath);
            Instant lastMod = object.lastModified();
            long mtime = lastMod != null ? lastMod.toEpochMilli() : Instant.EPOCH.toEpochMilli();
            String formatType = detectFormatType(storagePath);
            SchemaCacheKey schemaKey = SchemaCacheKey.build(storagePath.toString(), mtime, formatType, config);
            SchemaCacheEntry schemaEntry = cacheService.getOrComputeSchema(schemaKey, k -> {
                SourceMetadata meta = resolveSingleSource(path, config);
                Map<String, Object> enrichedMeta = meta.statistics()
                    .map(stats -> SourceStatisticsSerializer.embedStatistics(meta.sourceMetadata(), stats))
                    .orElse(meta.sourceMetadata());
                return SchemaCacheEntry.from(meta.schema(), meta.sourceType(), meta.location(), enrichedMeta, meta.config());
            });
            List<Attribute> schema = schemaEntry.toAttributes();
            extMetadata = buildMetadataFromCache(schemaEntry, schema, config);
        } else {
            SourceMetadata metadata = resolveSingleSource(path, config);
            extMetadata = wrapAsExternalSourceMetadata(metadata, config);
            object = provider.newObject(storagePath);
        }

        FileList singletonList = GlobExpander.fileListOf(
            List.of(new StorageEntry(storagePath, object.length(), object.lastModified())),
            path
        );
        // Single-file: degenerate case of the general flow — one-entry schemaMap, identity mapping.
        Map<StoragePath, SchemaReconciliation.FileSchemaInfo> schemaMap = singleEntrySchemaMap(storagePath, extMetadata.schema());
        return new ExternalSourceResolution.ResolvedSource(extMetadata, singletonList, schemaMap);
    }

    private static Map<StoragePath, SchemaReconciliation.FileSchemaInfo> singleEntrySchemaMap(
        StoragePath path,
        @Nullable List<Attribute> schema
    ) {
        if (schema == null || schema.isEmpty()) {
            return Map.of();
        }
        SchemaReconciliation.ColumnMapping identityMapping = new SchemaReconciliation.ColumnMapping(identityMapping(schema.size()), null);
        return Map.of(path, new SchemaReconciliation.FileSchemaInfo(schema, identityMapping, null));
    }

    private ExternalSourceResolution.ResolvedSource resolveMultiFileSource(
        String path,
        Map<String, Object> config,
        @Nullable List<PartitionFilterHintExtractor.PartitionFilterHint> hints,
        boolean hivePartitioning
    ) throws Exception {
        StoragePath storagePath = StoragePath.of(path);
        StorageProvider provider = resolveProvider(storagePath, config);

        FormatReader.SchemaResolution schemaResolution = parseSchemaResolution(config);

        if (schemaResolution != FormatReader.SchemaResolution.FIRST_FILE_WINS) {
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

        boolean cacheable = isCacheable(provider);

        FileList listing;
        if (cacheable) {
            ListingCacheKey listingKey = ListingCacheKey.build(storagePath.scheme(), storagePath.host(), storagePath.path(), config);
            listing = cacheService.getOrComputeListing(
                listingKey,
                k -> expandAndCompact(path, provider, hints, hivePartitioning, storagePath)
            );
        } else {
            listing = expandAndCompact(path, provider, hints, hivePartitioning, storagePath);
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
                Map<String, Object> enrichedMeta = meta.statistics()
                    .map(stats -> SourceStatisticsSerializer.embedStatistics(meta.sourceMetadata(), stats))
                    .orElse(meta.sourceMetadata());
                return SchemaCacheEntry.from(meta.schema(), meta.sourceType(), meta.location(), enrichedMeta, meta.config());
            });
            List<Attribute> schema = schemaEntry.toAttributes();
            extMetadata = buildMetadataFromCache(schemaEntry, schema, config);
        } else {
            SourceMetadata metadata = resolveSingleSource(anchorPath.toString(), config);
            extMetadata = wrapAsExternalSourceMetadata(metadata, config);
        }

        extMetadata = enrichWithFileCount(extMetadata, listing.fileCount());
        if (listing.fileCount() > 1) {
            // For multi-file FIRST_FILE_WINS, read all files' metadata in parallel during Phase 1
            // to aggregate statistics across all files. This allows aggregate pushdown
            // (COUNT/MIN/MAX) to use accurate global stats and to skip Phase 2 (split discovery)
            // entirely for those queries.
            //
            // Tradeoff: this performs N footer reads up-front for *every* multi-file resolve,
            // including queries that don't use the stats (e.g. SELECT *). For those queries,
            // Phase 2 still runs, so the per-file footer is read twice (once here, once during
            // split discovery). The cost is generally acceptable because:
            // - the cacheable path consults the schema cache, so repeat resolves are free;
            // - the non-cacheable path reads footers in parallel up to MAX_PARALLEL_METADATA_READS;
            // - the aggregated stats unlock skipping Phase 2 entirely for pushable aggregates
            // (see ComputeService#canSkipSplitDiscovery), which dominates the savings.
            // We don't gate this on the query (which isn't known here) — see issue #148086 for the
            // design notes.
            Map<String, Object> aggregatedStats = cacheable
                ? readAndAggregateAllFileStatsWithCache(listing, config)
                : readAndAggregateAllFileStats(listing, config);
            if (aggregatedStats != null) {
                // Replace anchor-only stats with globally-aggregated stats.
                // Preserve all non-stats keys from the current extMetadata (e.g. file_count, config).
                Map<String, Object> current = extMetadata.sourceMetadata();
                Map<String, Object> merged = current != null ? new HashMap<>(current) : new HashMap<>();
                merged.putAll(aggregatedStats);
                // Do NOT add STATS_PARTIAL — stats are now complete across all files.
                merged.remove(SourceStatisticsSerializer.STATS_PARTIAL);
                final Map<String, Object> finalMerged = Map.copyOf(merged);
                final ExternalSourceMetadata baseMetadata = extMetadata;
                extMetadata = new ExternalSourceMetadata() {
                    @Override
                    public String location() {
                        return baseMetadata.location();
                    }

                    @Override
                    public List<Attribute> schema() {
                        return baseMetadata.schema();
                    }

                    @Override
                    public String sourceType() {
                        return baseMetadata.sourceType();
                    }

                    @Override
                    public Map<String, Object> sourceMetadata() {
                        return finalMerged;
                    }

                    @Override
                    public Map<String, Object> config() {
                        return baseMetadata.config();
                    }
                };
            } else {
                // Could not aggregate stats (some files lacked statistics) — mark as partial
                // so the optimizer does not rely on incomplete sourceMetadata stats.
                extMetadata = markStatsAsPartial(extMetadata);
            }
        }

        // Capture pre-enrichment schema: partition columns are injected by VirtualColumnInjector
        // at read time, so per-file readSchema must NOT include them.
        List<Attribute> dataOnlySchema = extMetadata.schema();

        PartitionMetadata partitionMetadata = listing.partitionMetadata();
        if (partitionMetadata != null && partitionMetadata.isEmpty() == false) {
            extMetadata = enrichSchemaWithPartitionColumns(extMetadata, partitionMetadata);
        }

        // FFW: every file's readSchema is the anchor's data-only schema, identity mapping.
        Map<StoragePath, SchemaReconciliation.FileSchemaInfo> schemaMap;
        if (dataOnlySchema != null && dataOnlySchema.isEmpty() == false) {
            Map<StoragePath, SchemaReconciliation.FileSchemaInfo> perFileInfo = Maps.newHashMapWithExpectedSize(listing.fileCount());
            SchemaReconciliation.ColumnMapping identityMapping = new SchemaReconciliation.ColumnMapping(
                identityMapping(dataOnlySchema.size()),
                null
            );
            for (int i = 0; i < listing.fileCount(); i++) {
                perFileInfo.put(listing.path(i), new SchemaReconciliation.FileSchemaInfo(dataOnlySchema, identityMapping, null));
            }
            schemaMap = Collections.unmodifiableMap(perFileInfo);
        } else {
            schemaMap = Map.of();
        }

        return new ExternalSourceResolution.ResolvedSource(extMetadata, listing, schemaMap);
    }

    private static int[] identityMapping(int n) {
        int[] m = new int[n];
        for (int i = 0; i < n; i++) {
            m[i] = i;
        }
        return m;
    }

    private FileList expandAndCompact(
        String path,
        StorageProvider provider,
        @Nullable List<PartitionFilterHintExtractor.PartitionFilterHint> hints,
        boolean hivePartitioning,
        StoragePath storagePath
    ) throws Exception {
        int maxDiscoveredFiles = ExternalSourceSettings.MAX_DISCOVERED_FILES.get(settings);
        int maxGlobExpansion = ExternalSourceSettings.MAX_GLOB_EXPANSION.get(settings);
        return GlobExpander.expandAndCompact(path, provider, hints, hivePartitioning, storagePath, maxDiscoveredFiles, maxGlobExpansion);
    }

    /**
     * Returns {@code true} when the schema cache can be consulted for the given provider.
     * Providers that do not support stable metadata (e.g. HTTP) are excluded because
     * mtime-based cache invalidation is not reliable for them.
     */
    private boolean isCacheable(StorageProvider provider) {
        return cacheService != null && cacheService.isEnabled() && provider.supportsStableMetadata();
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
        Map<String, Object> queryConfig
    ) {
        // Merge cached connector config (e.g. Flight endpoint/target) with query-level params.
        // Query-level params take precedence, matching the merge in wrapAsExternalSourceMetadata.
        Map<String, Object> cachedConnectorConfig = entry.connectorConfig();
        Map<String, Object> mergedConfig;
        if (cachedConnectorConfig != null && cachedConnectorConfig.isEmpty() == false) {
            mergedConfig = new HashMap<>(cachedConnectorConfig);
            if (queryConfig != null) {
                mergedConfig.putAll(queryConfig);
            }
        } else {
            mergedConfig = queryConfig != null ? queryConfig : Map.of();
        }

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
                return mergedConfig;
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
        Map<String, Object> aggregatedStats = aggregateFileStatistics(allMetadata.values());
        ExternalSourceMetadata extMetadata = buildUnifiedMetadata(firstMeta, unifiedSchema, config, aggregatedStats);

        PartitionMetadata partitionMetadata = fileList.partitionMetadata();
        if (partitionMetadata != null && partitionMetadata.isEmpty() == false) {
            extMetadata = enrichSchemaWithPartitionColumns(extMetadata, partitionMetadata);
        }

        Map<StoragePath, SchemaReconciliation.FileSchemaInfo> schemaMap = result.perFileInfo();
        return new ExternalSourceResolution.ResolvedSource(extMetadata, fileList, schemaMap);
    }

    /**
     * Reads metadata from all files in parallel with bounded concurrency.
     * Delegates to {@link BoundedParallelGather} which handles semaphore backpressure,
     * fast-fail on error, and result ordering.
     */
    private Map<StoragePath, SourceMetadata> readAllFileMetadata(FileList fileList, Map<String, Object> config) throws Exception {
        int fileCount = fileList.fileCount();
        List<StoragePath> paths = new ArrayList<>(fileCount);
        for (int i = 0; i < fileCount; i++) {
            paths.add(fileList.path(i));
        }

        List<Map.Entry<StoragePath, SourceMetadata>> entries = BoundedParallelGather.gather(
            paths,
            filePath -> Map.entry(filePath, resolveSingleSource(filePath.toString(), config)),
            MAX_PARALLEL_METADATA_READS,
            executor
        );

        Map<StoragePath, SourceMetadata> result = new LinkedHashMap<>();
        for (Map.Entry<StoragePath, SourceMetadata> entry : entries) {
            result.put(entry.getKey(), entry.getValue());
        }
        return result;
    }

    /**
     * Aggregates statistics from all files' metadata into a single merged flat stats map.
     * For each file, embeds its per-file statistics into its flat sourceMetadata map,
     * then merges all maps using {@link SourceStatisticsSerializer#mergeStatistics}.
     * Returns {@code null} if any file lacks statistics (prevents incorrect partial results).
     */
    @Nullable
    private static Map<String, Object> aggregateFileStatistics(Collection<SourceMetadata> allMetadata) {
        List<Map<String, Object>> perFileFlatStats = new ArrayList<>(allMetadata.size());
        for (SourceMetadata meta : allMetadata) {
            if (meta.statistics().isEmpty()) {
                // At least one file has no statistics — cannot produce accurate global stats.
                return null;
            }
            Map<String, Object> flat = SourceStatisticsSerializer.embedStatistics(meta.sourceMetadata(), meta.statistics().get());
            perFileFlatStats.add(flat);
        }
        return SourceStatisticsSerializer.mergeStatistics(perFileFlatStats);
    }

    /**
     * Reads metadata from all files in {@code listing} in parallel (bounded concurrency)
     * via {@link BoundedParallelGather}, then aggregates statistics across all files.
     * Returns a merged flat stats map, or {@code null} if any file fails or lacks statistics.
     * Errors reading individual files are logged at DEBUG and cause the method to return {@code null}
     * (the caller will then mark stats as partial instead of using incomplete aggregations).
     */
    @Nullable
    private Map<String, Object> readAndAggregateAllFileStats(FileList listing, Map<String, Object> config) {
        int fileCount = listing.fileCount();
        List<StoragePath> paths = new ArrayList<>(fileCount);
        for (int i = 0; i < fileCount; i++) {
            paths.add(listing.path(i));
        }
        List<SourceMetadata> allMeta;
        try {
            allMeta = BoundedParallelGather.gather(
                paths,
                filePath -> resolveSingleSource(filePath.toString(), config),
                MAX_PARALLEL_METADATA_READS,
                executor
            );
        } catch (Exception e) {
            LOGGER.debug(() -> "Failed to read per-file stats in parallel, will use partial stats: " + e.getMessage());
            return null;
        }
        return aggregateFileStatistics(allMeta);
    }

    /**
     * Cache-aware variant of {@link #readAndAggregateAllFileStats}.
     * Uses the schema cache (keyed by path + mtime) for each file so that repeated
     * multi-file resolves do not re-read footers unnecessarily.
     * Returns {@code null} if any file cannot be resolved or lacks statistics.
     */
    @Nullable
    private Map<String, Object> readAndAggregateAllFileStatsWithCache(FileList listing, Map<String, Object> config) {
        int fileCount = listing.fileCount();
        List<Map<String, Object>> perFileStats = new ArrayList<>(fileCount);
        for (int i = 0; i < fileCount; i++) {
            StoragePath filePath = listing.path(i);
            long mtime = listing.lastModifiedMillis(i);
            String formatType = detectFormatType(filePath);
            SchemaCacheKey schemaKey = SchemaCacheKey.build(filePath.toString(), mtime, formatType, config);
            try {
                SchemaCacheEntry entry = cacheService.getOrComputeSchema(schemaKey, k -> {
                    SourceMetadata meta = resolveSingleSource(filePath.toString(), config);
                    Map<String, Object> enrichedMeta = meta.statistics()
                        .map(stats -> SourceStatisticsSerializer.embedStatistics(meta.sourceMetadata(), stats))
                        .orElse(meta.sourceMetadata());
                    return SchemaCacheEntry.from(meta.schema(), meta.sourceType(), meta.location(), enrichedMeta, meta.config());
                });
                Map<String, Object> fileMeta = entry.safeMetadata();
                if (fileMeta == null || fileMeta.containsKey(SourceStatisticsSerializer.STATS_ROW_COUNT) == false) {
                    // This file has no statistics — cannot produce accurate global stats.
                    return null;
                }
                perFileStats.add(fileMeta);
            } catch (Exception e) {
                LOGGER.debug(() -> "Failed to get cached stats for [" + filePath + "], will use partial stats: " + e.getMessage());
                return null;
            }
        }
        return SourceStatisticsSerializer.mergeStatistics(perFileStats);
    }

    private ExternalSourceMetadata buildUnifiedMetadata(
        SourceMetadata referenceMeta,
        List<Attribute> unifiedSchema,
        Map<String, Object> queryConfig,
        @Nullable Map<String, Object> aggregatedStats
    ) {
        Map<String, Object> mergedConfig = mergeConfigs(referenceMeta.config(), queryConfig);
        List<Attribute> schema = List.copyOf(unifiedSchema);
        Map<String, Object> enrichedSourceMetadata;
        if (aggregatedStats != null) {
            // Aggregated stats already contain all the _stats.* keys merged across all files.
            // Start from the reference meta's base map and overlay the aggregated stats.
            Map<String, Object> base = referenceMeta.sourceMetadata();
            Map<String, Object> merged = base != null ? new HashMap<>(base) : new HashMap<>();
            merged.putAll(aggregatedStats);
            enrichedSourceMetadata = Map.copyOf(merged);
        } else {
            enrichedSourceMetadata = referenceMeta.statistics()
                .map(stats -> SourceStatisticsSerializer.embedStatistics(referenceMeta.sourceMetadata(), stats))
                .orElse(referenceMeta.sourceMetadata());
        }
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
                return enrichedSourceMetadata;
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

    /**
     * Returns a wrapper that delegates everything to {@code metadata} except {@code sourceMetadata()},
     * which is enriched with the given extra entries.
     */
    static ExternalSourceMetadata withExtraSourceMetadata(ExternalSourceMetadata metadata, Map<String, Object> extra) {
        Map<String, Object> original = metadata.sourceMetadata();
        Map<String, Object> enriched = original != null ? new HashMap<>(original) : new HashMap<>();
        enriched.putAll(extra);
        Map<String, Object> finalMetadata = Map.copyOf(enriched);
        return new ExternalSourceMetadata() {
            @Override
            public String location() {
                return metadata.location();
            }

            @Override
            public List<Attribute> schema() {
                return metadata.schema();
            }

            @Override
            public String sourceType() {
                return metadata.sourceType();
            }

            @Override
            public Map<String, Object> sourceMetadata() {
                return finalMetadata;
            }

            @Override
            public Map<String, Object> config() {
                return metadata.config();
            }
        };
    }

    static ExternalSourceMetadata enrichWithFileCount(ExternalSourceMetadata metadata, int fileCount) {
        return withExtraSourceMetadata(metadata, Map.of(SourceStatisticsSerializer.STATS_FILE_COUNT, (long) fileCount));
    }

    static ExternalSourceMetadata markStatsAsPartial(ExternalSourceMetadata metadata) {
        return withExtraSourceMetadata(metadata, Map.of(SourceStatisticsSerializer.STATS_PARTIAL, Boolean.TRUE));
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

    /**
     * Validates that data source plugins export ReferenceAttribute only.
     * Called when receiving schema from any plugin (FormatReader, TableCatalog, Connector).
     */
    private static void validateSchemaUsesOnlyReferenceAttributes(List<Attribute> schema) {
        for (Attribute attr : schema) {
            if (attr instanceof ReferenceAttribute == false) {
                throw new IllegalArgumentException(
                    "Data source schema must contain only ReferenceAttribute, but found "
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
                // Stats are embedded into sourceMetadata() below for the general path; for
                // ExternalSourceMetadata instances that already carry config (e.g. Iceberg),
                // their factory is responsible for populating sourceMetadata() — statistics()
                // is typically empty so there is nothing extra to embed.
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

        Map<String, Object> enrichedSourceMetadata = metadata.statistics()
            .map(stats -> SourceStatisticsSerializer.embedStatistics(metadata.sourceMetadata(), stats))
            .orElse(metadata.sourceMetadata());

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
                return enrichedSourceMetadata;
            }

            @Override
            public Map<String, Object> config() {
                return mergedConfig;
            }
        };
    }
}
