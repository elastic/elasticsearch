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
import org.elasticsearch.common.util.concurrent.ThrottledIterator;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.ExternalMetadataAttribute;
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
import org.elasticsearch.xpack.esql.datasources.spi.ListingHint;
import org.elasticsearch.xpack.esql.datasources.spi.SourceMetadata;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.datasources.spi.StorageProvider;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicReferenceArray;

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

    public static final String CONFIG_SCHEMA_RESOLUTION = "schema_resolution";

    /**
     * Config key under which {@link DatasetRewriter} stores data-source-level settings
     * (auth credentials, region, etc.) when building the merged config for a dataset query.
     * These are kept separate from the dataset format settings so that file-format factories
     * can validate only the keys they own, and so that credential values are not embedded in
     * the serialized plan sent to data nodes.
     */
    public static final String DATASOURCE_CONFIG_KEY = "_datasource";

    public static final Set<String> CONFIG_KEYS = Set.of(CONFIG_SCHEMA_RESOLUTION, DATASOURCE_CONFIG_KEY);

    /**
     * Default cap on the number of in-flight per-file metadata reads during a multi-file discovery
     * when a caller does not supply one. Production wires the {@code esql_worker} pool size here (via
     * {@code PlanExecutor}/{@code TransportEsqlQueryAction}) so the fan-out is bounded by an in-flight
     * permit equal to the pool size; because footer reads are async (released across the network
     * round-trip), that permit does not translate into that many pinned threads. Kept only as a
     * fallback for the constructors used by tests and by callers that do not thread the pool size.
     */
    static final int DEFAULT_METADATA_READ_CONCURRENCY = 16;

    /**
     * Returns a config suitable for passing to a storage provider: merges the {@link #DATASOURCE_CONFIG_KEY}
     * sub-map (data-source auth/connection settings) into the top level so that the provider can
     * access credentials. For queries that do not originate from a dataset (no {@code _datasource}
     * key), the input map is returned unchanged.
     */
    static Map<String, Object> storageConfig(Map<String, Object> config) {
        if (config == null || config.isEmpty()) {
            return config;
        }
        Object raw = config.get(DATASOURCE_CONFIG_KEY);
        if (raw == null) {
            return config;
        }
        @SuppressWarnings("unchecked")
        Map<String, Object> datasource = (Map<String, Object>) raw;
        if (datasource.isEmpty()) {
            return config;
        }
        Map<String, Object> result = new HashMap<>(datasource);
        config.forEach((k, v) -> {
            if (DATASOURCE_CONFIG_KEY.equals(k) == false) {
                result.put(k, v);
            }
        });
        return result;
    }

    /**
     * Returns a config with the {@link #DATASOURCE_CONFIG_KEY} sub-map removed. Used as the fallback
     * when serializing a plan to a data node whose transport version predates the encrypted-secret
     * carrier (see {@code ExternalSourceExec.writeTo}): such a node cannot deserialize the carrier, so
     * the credentials are stripped and it reverts to pre-credential-forwarding behavior.
     */
    public static Map<String, Object> planConfig(Map<String, Object> config) {
        if (config == null || config.isEmpty() || config.containsKey(DATASOURCE_CONFIG_KEY) == false) {
            return config;
        }
        Map<String, Object> result = new HashMap<>(config);
        result.remove(DATASOURCE_CONFIG_KEY);
        return result;
    }

    private final Executor executor;
    private final DataSourceModule dataSourceModule;
    private final Settings settings;
    private final ExternalSourceCacheService cacheService;
    private final int metadataReadConcurrency;

    /** Coordinator-side accessor used by EsqlSession to reconcile data-node-captured source stats post-query. */
    public ExternalSourceCacheService cacheService() {
        return cacheService;
    }

    /** Maximum in-flight per-file metadata reads for a multi-file discovery. Visible for wiring tests. */
    public int metadataReadConcurrency() {
        return metadataReadConcurrency;
    }

    /** Executor the discovery fan-out runs on. Visible for wiring/isolation tests. */
    public Executor executor() {
        return executor;
    }

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
        this(executor, dataSourceModule, settings, cacheService, DEFAULT_METADATA_READ_CONCURRENCY);
    }

    /**
     * @param metadataReadConcurrency maximum number of in-flight per-file metadata reads during a
     *            multi-file discovery. Production passes the {@code esql_worker} pool size.
     */
    public ExternalSourceResolver(
        Executor executor,
        DataSourceModule dataSourceModule,
        Settings settings,
        @Nullable ExternalSourceCacheService cacheService,
        int metadataReadConcurrency
    ) {
        if (metadataReadConcurrency < 1) {
            throw new IllegalArgumentException("metadataReadConcurrency must be >= 1, got: " + metadataReadConcurrency);
        }
        this.executor = executor;
        this.dataSourceModule = dataSourceModule;
        this.settings = settings;
        this.cacheService = cacheService;
        this.metadataReadConcurrency = metadataReadConcurrency;
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

        // Kick off the (listener-driven) per-path resolution on the resolver executor. The initial
        // dispatch performs any cheap synchronous prep (glob expansion) and then hands off to async
        // footer reads, so the executor thread is not held across the network round-trips.
        Map<String, ExternalSourceResolution.ResolvedSource> resolved = Maps.newHashMapWithExpectedSize(paths.size());
        executor.execute(() -> resolveNextPath(paths, 0, pathConfigs, filterHints, resolved, listener));
    }

    /**
     * Resolves {@code paths[index]} asynchronously, then chains to the next path on success. Paths
     * are resolved sequentially so the accumulation map needs no synchronization and the first
     * failure short-circuits the rest — mirroring the previous synchronous loop's semantics.
     */
    private void resolveNextPath(
        List<String> paths,
        int index,
        Map<String, Map<String, Object>> pathConfigs,
        @Nullable Map<String, List<PartitionFilterHintExtractor.PartitionFilterHint>> filterHints,
        Map<String, ExternalSourceResolution.ResolvedSource> resolved,
        ActionListener<ExternalSourceResolution> listener
    ) {
        if (index == paths.size()) {
            listener.onResponse(new ExternalSourceResolution(resolved));
            return;
        }
        String path = paths.get(index);
        Map<String, Object> config = pathConfigs.getOrDefault(path, Map.of());
        List<PartitionFilterHintExtractor.PartitionFilterHint> hints = filterHints != null ? filterHints.get(path) : null;
        boolean hivePartitioning = isHivePartitioningEnabled(config);

        resolveSource(path, config, hints, hivePartitioning, ActionListener.wrap(resolvedSource -> {
            resolved.put(path, resolvedSource);
            LOGGER.debug("Successfully resolved external source: {}", path);
            resolveNextPath(paths, index + 1, pathConfigs, filterHints, resolved, listener);
        }, e -> listener.onFailure(mapResolveFailure(path, e))));
    }

    /**
     * Reproduces the previous loop's error contract: {@link IllegalArgumentException} and
     * {@link UnsupportedOperationException} (client-caused) propagate unwrapped, while any other
     * failure is wrapped in an {@link ElasticsearchException} carrying the path and detail.
     */
    private static RuntimeException mapResolveFailure(String path, Exception e) {
        if (e instanceof IllegalArgumentException || e instanceof UnsupportedOperationException) {
            LOGGER.error("Failed to resolve external source [{}]: {}", path, e.getMessage(), e);
            return (RuntimeException) e;
        }
        LOGGER.error("Failed to resolve external source [{}]: {}", path, e.getMessage(), e);
        String detail = e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName();
        return new ElasticsearchException(String.format(Locale.ROOT, "Failed to resolve external source [%s]: %s", path, detail), e);
    }

    private void resolveSource(
        String path,
        Map<String, Object> config,
        @Nullable List<PartitionFilterHintExtractor.PartitionFilterHint> hints,
        boolean hivePartitioning,
        ActionListener<ExternalSourceResolution.ResolvedSource> listener
    ) {
        LOGGER.debug("Resolving external source: path=[{}]", path);
        try {
            if (GlobExpander.isMultiFile(path)) {
                resolveMultiFileSource(path, config, hints, hivePartitioning, listener);
            } else {
                resolveSingleFileSource(path, config, listener);
            }
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    private void resolveSingleFileSource(
        String path,
        Map<String, Object> config,
        ActionListener<ExternalSourceResolution.ResolvedSource> listener
    ) throws Exception {
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
                return SchemaCacheEntry.from(resolveSingleSource(path, config));
            });
            List<Attribute> schema = schemaEntry.toAttributes();
            extMetadata = buildMetadataFromCache(schemaEntry, schema, config);
        } else {
            SourceMetadata metadata = resolveSingleSource(path, config);
            extMetadata = wrapAsExternalSourceMetadata(metadata, config);
            object = provider.newObject(storagePath);
        }

        // Capture the raw file schema before enriching with virtual columns: schemaMap describes
        // the physical schema each reader actually sees, not the user-facing projection.
        List<Attribute> fileSchema = extMetadata.schema();
        extMetadata = enrichSchemaWithFileMetadataColumns(extMetadata);

        FileList singletonList = GlobExpander.fileListOf(
            List.of(new StorageEntry(storagePath, object.length(), object.lastModified())),
            path
        );
        // Single-file: degenerate case of the general flow — one-entry schemaMap, identity mapping.
        Map<StoragePath, SchemaReconciliation.FileSchemaInfo> schemaMap = singleEntrySchemaMap(storagePath, fileSchema);
        listener.onResponse(new ExternalSourceResolution.ResolvedSource(extMetadata, singletonList, schemaMap));
    }

    private static Map<StoragePath, SchemaReconciliation.FileSchemaInfo> singleEntrySchemaMap(
        StoragePath path,
        @Nullable List<Attribute> schema
    ) {
        if (schema == null || schema.isEmpty()) {
            return Map.of();
        }
        ColumnMapping identityMapping = new ColumnMapping(identityMapping(schema.size()), null);
        return Map.of(path, new SchemaReconciliation.FileSchemaInfo(new ExternalSchema(schema), identityMapping, null));
    }

    private void resolveMultiFileSource(
        String path,
        Map<String, Object> config,
        @Nullable List<PartitionFilterHintExtractor.PartitionFilterHint> hints,
        boolean hivePartitioning,
        ActionListener<ExternalSourceResolution.ResolvedSource> listener
    ) throws Exception {
        StoragePath storagePath = StoragePath.of(path);
        StorageProvider provider = resolveProvider(storagePath, config);

        FormatReader.SchemaResolution schemaResolution = parseSchemaResolution(config);
        boolean cacheable = isCacheable(provider);

        if (schemaResolution != FormatReader.SchemaResolution.FIRST_FILE_WINS) {
            int maxDiscoveredFiles = ExternalSourceSettings.MAX_DISCOVERED_FILES.get(settings);
            int maxGlobExpansion = ExternalSourceSettings.MAX_GLOB_EXPANSION.get(settings);
            FileList raw = path.indexOf(',') >= 0
                ? GlobExpander.expandCommaSeparated(path, provider, hints, hivePartitioning, maxDiscoveredFiles, maxGlobExpansion)
                : GlobExpander.expandGlob(path, provider, hints, hivePartitioning, maxDiscoveredFiles, maxGlobExpansion);
            if (raw.fileCount() == 0) {
                throw new IllegalArgumentException("Glob pattern matched no files: " + path);
            }
            resolveMultiFileWithReconciliation(raw, config, schemaResolution, cacheable, listener);
            return;
        }

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

        ExternalSourceMetadata anchorMetadata;
        if (cacheable) {
            String formatType = detectFormatType(anchorPath);
            SchemaCacheKey schemaKey = SchemaCacheKey.build(anchorPath.toString(), anchorMtime, formatType, config);
            SchemaCacheEntry schemaEntry = cacheService.getOrComputeSchema(schemaKey, k -> {
                return SchemaCacheEntry.from(resolveSingleSource(anchorPath.toString(), config));
            });
            List<Attribute> schema = schemaEntry.toAttributes();
            anchorMetadata = buildMetadataFromCache(schemaEntry, schema, config);
        } else {
            SourceMetadata metadata = resolveSingleSource(anchorPath.toString(), config);
            anchorMetadata = wrapAsExternalSourceMetadata(metadata, config);
        }

        final FileList finalListing = listing;
        final ExternalSourceMetadata baseMetadata = enrichWithFileCount(anchorMetadata, listing.fileCount());
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
            // - the non-cacheable path reads footers with an async fan-out bounded by an in-flight
            // permit (metadataReadConcurrency), releasing the pool thread across each footer read;
            // - the aggregated stats unlock skipping Phase 2 entirely for pushable aggregates
            // (see ComputeService#canSkipSplitDiscovery), which dominates the savings.
            // We don't gate this on the query (which isn't known here) — see issue #148086 for the
            // design notes.
            ActionListener<Map<String, Object>> statsListener = ActionListener.wrap(aggregatedStats -> {
                try {
                    ExternalSourceMetadata withStats = applyFirstFileWinsAggregatedStats(baseMetadata, aggregatedStats);
                    listener.onResponse(finishFirstFileWins(finalListing, withStats));
                } catch (Exception e) {
                    listener.onFailure(e);
                }
            }, listener::onFailure);
            if (cacheable) {
                readAndAggregateAllFileStatsWithCache(listing, config, statsListener);
            } else {
                readAndAggregateAllFileStats(listing, config, statsListener);
            }
        } else {
            listener.onResponse(finishFirstFileWins(finalListing, baseMetadata));
        }
    }

    /**
     * Overlays globally-aggregated statistics onto the anchor metadata for the FIRST_FILE_WINS path.
     * When {@code aggregatedStats} is {@code null} (some file lacked statistics or a read failed) the
     * stats are marked partial so the optimizer does not rely on incomplete aggregations.
     */
    private static ExternalSourceMetadata applyFirstFileWinsAggregatedStats(
        ExternalSourceMetadata baseMetadata,
        @Nullable Map<String, Object> aggregatedStats
    ) {
        if (aggregatedStats == null) {
            return markStatsAsPartial(baseMetadata);
        }
        // Replace anchor-only stats with globally-aggregated stats.
        // Preserve all non-stats keys from the current extMetadata (e.g. file_count, config).
        Map<String, Object> current = baseMetadata.sourceMetadata();
        Map<String, Object> merged = current != null ? new HashMap<>(current) : new HashMap<>();
        merged.putAll(aggregatedStats);
        // Do NOT add STATS_PARTIAL — stats are now complete across all files.
        merged.remove(SourceStatisticsSerializer.STATS_PARTIAL);
        final Map<String, Object> finalMerged = Map.copyOf(merged);
        return new ExternalSourceMetadata() {
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
    }

    /**
     * Completes the FIRST_FILE_WINS path once per-file stats (if any) have been folded into
     * {@code extMetadata}: enriches the schema with partition and file-metadata columns and pins the
     * anchor's data-only schema for every file via an identity-mapped schemaMap. Purely CPU-bound.
     */
    private ExternalSourceResolution.ResolvedSource finishFirstFileWins(FileList listing, ExternalSourceMetadata extMetadata) {
        // Capture pre-enrichment schema: partition columns are added by VirtualColumnIterator
        // at read time, so per-file readSchema must NOT include them.
        List<Attribute> dataOnlySchema = extMetadata.schema();

        PartitionMetadata partitionMetadata = listing.partitionMetadata();
        if (partitionMetadata != null && partitionMetadata.isEmpty() == false) {
            extMetadata = enrichSchemaWithPartitionColumns(extMetadata, partitionMetadata);
        }

        extMetadata = enrichSchemaWithFileMetadataColumns(extMetadata);

        // FFW: every file's readSchema is the anchor's data-only schema, identity mapping.
        Map<StoragePath, SchemaReconciliation.FileSchemaInfo> schemaMap;
        if (dataOnlySchema != null && dataOnlySchema.isEmpty() == false) {
            Map<StoragePath, SchemaReconciliation.FileSchemaInfo> perFileInfo = Maps.newHashMapWithExpectedSize(listing.fileCount());
            ColumnMapping identityMapping = new ColumnMapping(identityMapping(dataOnlySchema.size()), null);
            ExternalSchema dataOnly = new ExternalSchema(dataOnlySchema);
            for (int i = 0; i < listing.fileCount(); i++) {
                perFileInfo.put(listing.path(i), new SchemaReconciliation.FileSchemaInfo(dataOnly, identityMapping, null));
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
            return registry.createProvider(storagePath.scheme(), settings, storageConfig(config));
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

        // Warm stats live in the entry's safeMetadata, reconciled there from the data-node capture
        // (DriverCompletionInfo → ExternalSourceCacheService.reconcileSourceStats). The optimizer
        // reads the _stats.* keys straight off this map; no separate cache lookup.
        final Map<String, Object> finalMetadata = entry.safeMetadata();

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
                return finalMetadata;
            }

            @Override
            public Map<String, Object> config() {
                return mergedConfig;
            }
        };
    }

    private void resolveMultiFileWithReconciliation(
        FileList fileList,
        Map<String, Object> config,
        FormatReader.SchemaResolution schemaResolution,
        boolean cacheable,
        ActionListener<ExternalSourceResolution.ResolvedSource> listener
    ) {
        long startNanos = System.nanoTime();
        readAllFileMetadata(fileList, config, cacheable, ActionListener.wrap(allMetadata -> {
            try {
                long durationMs = (System.nanoTime() - startNanos) / 1_000_000;
                LOGGER.debug("Schema reconciliation [{}]: scanned {} files in {}ms", schemaResolution, allMetadata.size(), durationMs);

                StoragePath firstFile = fileList.path(0);
                SchemaReconciliation.Result result;
                if (schemaResolution == FormatReader.SchemaResolution.STRICT) {
                    result = SchemaReconciliation.reconcileStrict(firstFile, allMetadata);
                } else {
                    result = SchemaReconciliation.reconcileUnionByName(allMetadata);
                }

                List<Attribute> unifiedSchema = result.unifiedSchema().attributes();
                SourceMetadata firstMeta = allMetadata.get(firstFile);
                // Aggregate from the per-file metadata already fetched by readAllFileMetadata —
                // no second cache or storage hit per file.
                Map<String, Object> aggregatedStats = aggregateFileStatistics(allMetadata.values());
                ExternalSourceMetadata extMetadata = buildUnifiedMetadata(firstMeta, unifiedSchema, config, aggregatedStats);

                // Mirror the FFW invariants: file count enables canSkipSplitDiscovery; partial-stats
                // marking is gated on fileCount > 1 (single-file globs have no "other file" missing stats).
                extMetadata = enrichWithFileCount(extMetadata, fileList.fileCount());
                if (aggregatedStats == null && fileList.fileCount() > 1) {
                    extMetadata = markStatsAsPartial(extMetadata);
                }

                PartitionMetadata partitionMetadata = fileList.partitionMetadata();
                if (partitionMetadata != null && partitionMetadata.isEmpty() == false) {
                    extMetadata = enrichSchemaWithPartitionColumns(extMetadata, partitionMetadata);
                }

                extMetadata = enrichSchemaWithFileMetadataColumns(extMetadata);

                Map<StoragePath, SchemaReconciliation.FileSchemaInfo> schemaMap = result.perFileInfo();
                listener.onResponse(new ExternalSourceResolution.ResolvedSource(extMetadata, fileList, schemaMap));
            } catch (Exception e) {
                listener.onFailure(e);
            }
        }, listener::onFailure));
    }

    /**
     * Per-file metadata, read with an async fan-out bounded by {@link #metadataReadConcurrency}
     * in-flight reads (see {@link #gatherPerFile}). When {@code cacheable} is true each resolve
     * peeks the schema cache (keyed on path + mtime) and, on a miss, resolves asynchronously and
     * stores the result so warm queries against the same paths hit cache. The result preserves the
     * file order of {@code fileList}.
     */
    private void readAllFileMetadata(
        FileList fileList,
        Map<String, Object> config,
        boolean cacheable,
        ActionListener<Map<StoragePath, SourceMetadata>> listener
    ) {
        int fileCount = fileList.fileCount();
        gatherPerFile(fileList, config, cacheable, ActionListener.wrap(perFile -> {
            Map<StoragePath, SourceMetadata> result = new LinkedHashMap<>();
            for (int i = 0; i < fileCount; i++) {
                result.put(fileList.path(i), perFile.get(i));
            }
            listener.onResponse(result);
        }, listener::onFailure));
    }

    /**
     * Runs an async, bounded fan-out over every file in {@code fileList}, resolving each file's
     * {@link SourceMetadata} and returning results in file order. Concurrency is capped at
     * {@link #metadataReadConcurrency} in-flight reads via {@link ThrottledIterator}; because the
     * per-file resolve is itself async (the footer read is released across the network round-trip),
     * that permit bounds in-flight reads rather than pinning that many executor threads. The first
     * failure is propagated to {@code listener} and short-circuits the remaining files.
     */
    private void gatherPerFile(
        FileList fileList,
        Map<String, Object> config,
        boolean cacheable,
        ActionListener<List<SourceMetadata>> listener
    ) {
        int fileCount = fileList.fileCount();
        AtomicReferenceArray<SourceMetadata> results = new AtomicReferenceArray<>(fileCount);
        AtomicReference<Exception> failure = new AtomicReference<>();
        Iterator<Integer> indices = indexIterator(fileCount);
        ThrottledIterator.run(indices, (releasable, i) -> {
            if (failure.get() != null) {
                // A previous file already failed — drain the remaining items without issuing reads.
                releasable.close();
                return;
            }
            ActionListener<SourceMetadata> itemListener = ActionListener.runAfter(
                ActionListener.wrap(meta -> results.set(i, meta), e -> failure.compareAndSet(null, e)),
                releasable::close
            );
            // ThrottledIterator's itemConsumer must not throw: an escaped exception would leave this
            // item's ref permanently held (its releasable never closed) and onCompletion would never
            // fire — a hang. A synchronous throw from the resolve dispatch (e.g. a factory that rejects
            // the executor submission or throws before completing the listener) is therefore funnelled
            // into itemListener.onFailure so runAfter(..., releasable::close) always runs.
            try {
                StoragePath filePath = fileList.path(i);
                // Length + mtime come from the directory listing: thread them through so the factory
                // can build the storage object without a synchronous existence/HEAD probe on the
                // executor thread before the async footer read.
                ListingHint hint = new ListingHint(fileList.size(i), fileList.lastModifiedMillis(i));
                if (cacheable) {
                    cachedResolveSingleSourceAsync(filePath, hint, config, itemListener);
                } else {
                    resolveSingleSourceAsync(filePath.toString(), hint, config, itemListener);
                }
            } catch (Exception e) {
                itemListener.onFailure(e);
            }
        }, metadataReadConcurrency, () -> {
            Exception e = failure.get();
            if (e != null) {
                listener.onFailure(e);
                return;
            }
            List<SourceMetadata> out = new ArrayList<>(fileCount);
            for (int i = 0; i < fileCount; i++) {
                out.add(results.get(i));
            }
            listener.onResponse(out);
        }, executor, e -> {
            // A continuation was rejected/failed (e.g. executor shutdown): record it so onCompletion
            // surfaces the failure rather than returning a partially-populated result.
            failure.compareAndSet(null, e);
        });
    }

    /**
     * Cache-aware async single-file resolve for the multi-file fan-out. Peeks the schema cache and,
     * on a miss, resolves asynchronously (without pinning a thread across the footer read) and stores
     * the result. Unlike the single-file {@code getOrComputeSchema} path this does not coalesce
     * concurrent misses for the same key, which is acceptable here because each fan-out file is
     * distinct; see {@link ExternalSourceCacheService#getSchemaIfPresent}.
     */
    private void cachedResolveSingleSourceAsync(
        StoragePath filePath,
        ListingHint hint,
        Map<String, Object> config,
        ActionListener<SourceMetadata> listener
    ) {
        String formatType = detectFormatType(filePath);
        SchemaCacheKey schemaKey = SchemaCacheKey.build(filePath.toString(), hint.lastModifiedMillis(), formatType, config);
        SchemaCacheEntry cached = cacheService.getSchemaIfPresent(schemaKey);
        if (cached != null) {
            listener.onResponse(buildMetadataFromCache(cached, cached.toAttributes(), config));
            return;
        }
        resolveSingleSourceAsync(filePath.toString(), hint, config, listener.map(meta -> {
            SchemaCacheEntry entry = SchemaCacheEntry.from(meta);
            cacheService.putSchema(schemaKey, entry);
            return buildMetadataFromCache(entry, entry.toAttributes(), config);
        }));
    }

    /**
     * Aggregates statistics from all files' metadata into a single merged flat stats map.
     * For each file, embeds its per-file statistics into its flat sourceMetadata map,
     * then merges all maps using {@link SourceStatisticsSerializer#mergeStatistics}.
     * Returns {@code null} if any file lacks statistics (prevents incorrect partial results).
     */
    @Nullable
    static Map<String, Object> aggregateFileStatistics(Collection<SourceMetadata> allMetadata) {
        List<Map<String, Object>> perFileFlatStats = new ArrayList<>(allMetadata.size());
        for (SourceMetadata meta : allMetadata) {
            // Cached entries embed stats in sourceMetadata(); uncached entries use typed statistics().
            Map<String, Object> base = meta.sourceMetadata();
            if (base != null && base.containsKey(SourceStatisticsSerializer.STATS_ROW_COUNT)) {
                perFileFlatStats.add(base);
            } else if (meta.statistics().isPresent()) {
                perFileFlatStats.add(SourceStatisticsSerializer.embedStatistics(base, meta.statistics().get()));
            } else {
                // At least one file has no statistics — cannot produce accurate global stats.
                return null;
            }
        }
        return SourceStatisticsSerializer.mergeStatistics(perFileFlatStats);
    }

    /**
     * Reads metadata from all files in {@code listing} with an async fan-out bounded by
     * {@link #metadataReadConcurrency} in-flight reads, then aggregates statistics across all files.
     * Completes with a merged flat stats map, or {@code null} if any file fails or lacks statistics.
     * A read failure is logged at DEBUG and completes with {@code null} (the caller then marks stats
     * as partial instead of using incomplete aggregations) — it never propagates as a failure.
     */
    private void readAndAggregateAllFileStats(FileList listing, Map<String, Object> config, ActionListener<Map<String, Object>> listener) {
        gatherPerFile(listing, config, false, ActionListener.wrap(allMeta -> listener.onResponse(aggregateFileStatistics(allMeta)), e -> {
            LOGGER.debug(() -> "Failed to read per-file stats in parallel, will use partial stats: " + e.getMessage());
            listener.onResponse(null);
        }));
    }

    /**
     * Cache-aware variant of {@link #readAndAggregateAllFileStats}. Peeks the schema cache (keyed by
     * path + mtime) for each file and resolves misses asynchronously, so repeated multi-file resolves
     * do not re-read footers unnecessarily. Completes with {@code null} if any file cannot be resolved
     * or lacks statistics.
     */
    private void readAndAggregateAllFileStatsWithCache(
        FileList listing,
        Map<String, Object> config,
        ActionListener<Map<String, Object>> listener
    ) {
        int fileCount = listing.fileCount();
        gatherPerFile(listing, config, true, ActionListener.wrap(allMeta -> {
            List<Map<String, Object>> perFileStats = new ArrayList<>(fileCount);
            for (SourceMetadata meta : allMeta) {
                Map<String, Object> fileMeta = meta.sourceMetadata();
                if (fileMeta == null || fileMeta.containsKey(SourceStatisticsSerializer.STATS_ROW_COUNT) == false) {
                    // This file has no statistics — cannot produce accurate global stats.
                    listener.onResponse(null);
                    return;
                }
                perFileStats.add(fileMeta);
            }
            listener.onResponse(SourceStatisticsSerializer.mergeStatistics(perFileStats));
        }, e -> {
            LOGGER.debug(() -> "Failed to get cached stats in parallel, will use partial stats: " + e.getMessage());
            listener.onResponse(null);
        }));
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
            return FormatReader.DEFAULT_SCHEMA_RESOLUTION;
        }
        Object value = config.get(CONFIG_SCHEMA_RESOLUTION);
        if (value == null) {
            return FormatReader.DEFAULT_SCHEMA_RESOLUTION;
        }
        return FormatReader.SchemaResolution.parse(value.toString());
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
     * Async counterpart of {@link #resolveSingleSource}: performs the same early scheme validation and
     * factory selection, then routes to {@link ExternalSourceFactory#resolveMetadataAsync} so a
     * file-based factory can issue the footer read without pinning an executor thread across the
     * network round-trip. The error contract matches the synchronous path: an
     * {@link UnsupportedSchemeException} for an unsupported scheme, the last factory failure wrapped in
     * an {@link IllegalArgumentException} when one or more factories claimed the path but failed, or an
     * {@link UnsupportedOperationException} when no factory can handle the path.
     */
    private void resolveSingleSourceAsync(
        String path,
        @Nullable ListingHint hint,
        Map<String, Object> config,
        ActionListener<SourceMetadata> listener
    ) {
        try {
            StoragePath parsed = StoragePath.of(path);
            DataSourceCapabilities capabilities = dataSourceModule.capabilities();
            if (capabilities != null && capabilities.supportsScheme(parsed.scheme()) == false) {
                listener.onFailure(
                    new UnsupportedSchemeException(
                        "Unsupported storage scheme [" + parsed.scheme() + "]. Supported: " + capabilities.supportedSchemesString()
                    )
                );
                return;
            }
        } catch (UnsupportedSchemeException e) {
            listener.onFailure(e);
            return;
        } catch (IllegalArgumentException e) {
            // Path parsing failed -- let the factory iteration handle it
        }

        List<ExternalSourceFactory> candidates = new ArrayList<>();
        for (ExternalSourceFactory factory : dataSourceModule.sourceFactories().values()) {
            if (factory.canHandle(path)) {
                candidates.add(factory);
            }
        }
        resolveWithFactory(path, hint, config, candidates, 0, null, listener);
    }

    /**
     * Tries each candidate factory in order, falling back to the next on failure. Mirrors the
     * synchronous loop in {@link #resolveSingleSource}: records the last failure so that, once all
     * candidates are exhausted, it reports either that wrapped failure or (when no factory claimed the
     * path) an {@link UnsupportedOperationException}.
     */
    private void resolveWithFactory(
        String path,
        @Nullable ListingHint hint,
        Map<String, Object> config,
        List<ExternalSourceFactory> candidates,
        int index,
        @Nullable Exception lastFailure,
        ActionListener<SourceMetadata> listener
    ) {
        if (index == candidates.size()) {
            if (lastFailure != null) {
                listener.onFailure(new IllegalArgumentException("Failed to resolve metadata for [" + path + "]", lastFailure));
            } else {
                var sources = String.join(", ", dataSourceModule.sourceFactories().keySet());
                listener.onFailure(
                    new UnsupportedOperationException(
                        "No handler found for source at path ["
                            + path
                            + "]. "
                            + "Please ensure the appropriate data source plugin is installed. "
                            + "Known handlers: ["
                            + sources
                            + "]."
                    )
                );
            }
            return;
        }
        ExternalSourceFactory factory = candidates.get(index);
        try {
            factory.resolveMetadataAsync(path, hint, config, executor, ActionListener.wrap(listener::onResponse, e -> {
                LOGGER.debug("Factory [{}] claimed path [{}] but failed: {}", factory.type(), path, e.getMessage());
                resolveWithFactory(path, hint, config, candidates, index + 1, e, listener);
            }));
        } catch (Exception e) {
            // A factory that throws synchronously (rather than completing the listener) must not abort
            // the whole resolve: mirror the synchronous resolveSingleSource loop and fall through to the
            // next candidate, carrying this failure as the last error.
            LOGGER.debug("Factory [{}] claimed path [{}] but threw synchronously: {}", factory.type(), path, e.getMessage());
            resolveWithFactory(path, hint, config, candidates, index + 1, e, listener);
        }
    }

    /** Sequential {@code 0..count} iterator for {@link ThrottledIterator}; avoids a stream in production code. */
    private static Iterator<Integer> indexIterator(int count) {
        return new Iterator<>() {
            private int next = 0;

            @Override
            public boolean hasNext() {
                return next < count;
            }

            @Override
            public Integer next() {
                if (next >= count) {
                    throw new java.util.NoSuchElementException();
                }
                return next++;
            }
        };
    }

    /**
     * Returns a wrapper that delegates everything to {@code metadata} except {@code schema()},
     * which is replaced by the provided schema. Used by the schema-enrichment helpers so each
     * caller doesn't have to spell out a fresh anonymous {@link ExternalSourceMetadata}.
     */
    private static ExternalSourceMetadata withSchema(ExternalSourceMetadata metadata, List<Attribute> newSchema) {
        return new ExternalSourceMetadata() {
            @Override
            public String location() {
                return metadata.location();
            }

            @Override
            public List<Attribute> schema() {
                return newSchema;
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

        // Per-query nullability: a partition column is non-nullable when no file in the matched
        // fileset has a null value for it. The Hive sentinel __HIVE_DEFAULT_PARTITION__ is decoded
        // to null in PartitionMetadata#filePartitionValues, so this is precise rather than
        // pessimistic. The same dataset may yield different nullability across globs depending on
        // which files match.
        Set<String> nullableColumns = partitionMetadata.nullablePartitionColumns();

        for (Map.Entry<String, DataType> entry : partitionColumns.entrySet()) {
            String name = entry.getKey();
            DataType type = entry.getValue();
            Nullability nullability = nullableColumns.contains(name) ? Nullability.TRUE : Nullability.FALSE;
            // synthetic=false: partition columns are user-addressable (referenceable in WHERE, STATS BY, EVAL, ...).
            // Marking them synthetic causes AnalyzerRules.maybeResolveAgainstList to skip them during name resolution
            // and produces "Unknown column [X], did you mean [X]?" errors.
            enrichedSchema.add(new ReferenceAttribute(Source.EMPTY, null, name, type, nullability, null, false));
        }

        return withSchema(metadata, List.copyOf(enrichedSchema));
    }

    public static ExternalSourceMetadata enrichSchemaWithFileMetadataColumns(ExternalSourceMetadata metadata) {
        List<Attribute> originalSchema = metadata.schema();
        Set<String> existingNames = new LinkedHashSet<>();
        for (Attribute attr : originalSchema) {
            existingNames.add(attr.name());
        }

        List<Attribute> enrichedSchema = new ArrayList<>(originalSchema);
        for (Map.Entry<String, DataType> entry : FileMetadataColumns.COLUMNS.entrySet()) {
            String name = entry.getKey();
            if (existingNames.contains(name) == false) {
                enrichedSchema.add(new ExternalMetadataAttribute(Source.EMPTY, name, entry.getValue()));
            }
        }

        if (enrichedSchema.size() == originalSchema.size()) {
            return metadata;
        }

        return withSchema(metadata, List.copyOf(enrichedSchema));
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
        // Query-level params take precedence so users can override connector-resolved values. _datasource is
        // retained (carrying encrypted secrets) so it can travel to data nodes; ExternalSourceExec.writeTo
        // gates it on the transport version and strips it for older targets.
        Map<String, Object> mergedConfig;
        Map<String, Object> metadataConfig = metadata.config();
        if (metadataConfig != null && metadataConfig.isEmpty() == false) {
            mergedConfig = new HashMap<>(metadataConfig);
            if (queryConfig != null) {
                mergedConfig.putAll(queryConfig);
            }
        } else {
            mergedConfig = queryConfig != null ? queryConfig : Map.of();
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
