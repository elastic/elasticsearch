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
import org.elasticsearch.tasks.TaskCancelledException;
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
import org.elasticsearch.xpack.esql.datasources.spi.ExternalSourceMetrics;
import org.elasticsearch.xpack.esql.datasources.spi.FileList;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReader;
import org.elasticsearch.xpack.esql.datasources.spi.SkipWarnings;
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
import java.util.function.BooleanSupplier;

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

    private static final int MAX_PARALLEL_METADATA_READS = 16;

    private static final String RESOLUTION_CANCELLED_MESSAGE = "ES|QL external source resolution cancelled";

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
    /** Node telemetry sink, taken from the module ({@link ExternalSourceMetrics#NOOP} when no module is wired, e.g. tests). */
    private final ExternalSourceMetrics metrics;

    /**
     * Supplier consulted before each per-file footer read so that an in-flight resolution of a large
     * glob aborts promptly when the originating query is cancelled. {@code null} means "never cancelled"
     * (used by tests and call sites that do not carry a {@code CancellableTask}).
     */
    @Nullable
    private final BooleanSupplier isCancelled;

    /** Coordinator-side accessor used by EsqlSession to reconcile data-node-captured source stats post-query. */
    public ExternalSourceCacheService cacheService() {
        return cacheService;
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
        this(executor, dataSourceModule, settings, cacheService, null);
    }

    public ExternalSourceResolver(
        Executor executor,
        DataSourceModule dataSourceModule,
        Settings settings,
        @Nullable ExternalSourceCacheService cacheService,
        @Nullable BooleanSupplier isCancelled
    ) {
        this.executor = executor;
        this.dataSourceModule = dataSourceModule;
        this.settings = settings;
        this.cacheService = cacheService;
        this.isCancelled = isCancelled;
        this.metrics = dataSourceModule == null ? ExternalSourceMetrics.NOOP : dataSourceModule.externalSourceMetrics();
    }

    /**
     * Publishes one discovery pass (wall time + the discovered file count and estimated byte total) to node
     * telemetry. Best-effort: {@link ExternalSourceMetrics#recordDiscovery} self-guards, so an instrumentation
     * failure never fails resolution.
     */
    private void recordDiscovery(FileList list, long startNanos, String scheme) {
        long durationMs = (System.nanoTime() - startNanos) / 1_000_000;
        metrics.recordDiscovery(durationMs, list.fileCount(), list.estimatedBytes(), scheme);
    }

    /** Records one failed discovery/resolution attempt. Best-effort ({@link ExternalSourceMetrics#recordDiscoveryFailure} self-guards). */
    private void recordDiscoveryFailure() {
        metrics.recordDiscoveryFailure();
    }

    /** Returns {@code true} when the originating query has been cancelled. Safe to call when no supplier is wired. */
    private boolean isCancelled() {
        return isCancelled != null && isCancelled.getAsBoolean();
    }

    /**
     * Throws {@link TaskCancelledException} if the originating query has been cancelled, so that an in-flight
     * resolution of a large glob aborts promptly. Called both before doing storage I/O and (defensively) when a
     * per-file read fails while the query is cancelled, so cancellation is never masked as a partial-stats result.
     */
    private void throwIfCancelled() {
        if (isCancelled()) {
            throw new TaskCancelledException(RESOLUTION_CANCELLED_MESSAGE);
        }
    }

    /**
     * If the originating query has been cancelled, reports a clean {@link TaskCancelledException} to {@code listener}
     * and returns {@code true}; otherwise returns {@code false}. Used in the resolution failure path so that a footer
     * read which failed <em>because</em> the query was cancelled mid-flight surfaces as cancellation rather than as a
     * generic resolution error. Such a failure can arrive wrapped — {@code resolveSingleSource} wraps reader failures
     * in {@link IllegalArgumentException} and the schema cache wraps loader failures in {@code ExecutionException} —
     * so the cancellation state is consulted directly rather than matched on the exception type.
     */
    private boolean reportIfCancelled(String path, ActionListener<?> listener) {
        if (isCancelled()) {
            LOGGER.debug("External source resolution cancelled for [{}]", path);
            listener.onFailure(new TaskCancelledException(RESOLUTION_CANCELLED_MESSAGE));
            return true;
        }
        return false;
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
        resolve(paths, pathConfigs, filterHints, null, listener);
    }

    /**
     * Resolves external sources, gating the FIRST_FILE_WINS eager all-file stats aggregation on
     * {@code pathsRequiringStats}.
     *
     * @param pathsRequiringStats paths whose multi-file FFW resolution must eagerly aggregate global
     *        statistics across all files (the ungrouped-aggregate metadata fast path). A {@code null}
     *        value selects legacy behavior — every path resolves eagerly — preserving existing call
     *        sites and tests. When non-null, a path absent from the set defers the per-file footer
     *        reads (keeping {@code STATS_FILE_COUNT}, marking stats partial). See
     *        {@link ExternalStatsRequirementExtractor#pathsRequiringEagerStats}.
     */
    public void resolve(
        List<String> paths,
        Map<String, Map<String, Object>> pathConfigs,
        @Nullable Map<String, List<PartitionFilterHintExtractor.PartitionFilterHint>> filterHints,
        @Nullable Set<String> pathsRequiringStats,
        ActionListener<ExternalSourceResolution> listener
    ) {
        if (paths == null || paths.isEmpty()) {
            listener.onResponse(ExternalSourceResolution.EMPTY);
            return;
        }

        // Resolution runs on the caller-supplied executor (esql_worker in production, isolated from SEARCH
        // so a wide wildcard cannot starve regular ES searches). A multi-file resolve pins one worker on
        // the BoundedParallelGather latch while dispatching up to MAX_PARALLEL_METADATA_READS more tasks
        // on the same pool (join pattern), for a peak of MAX_PARALLEL_METADATA_READS + 1 running slots.
        // When the pool is smaller, ThrottledTaskRunner throttles submission rather than overflowing the
        // queue; on saturation across concurrent ES|QL queries it fails fast per-slot via running-slot
        // rejection rather than deadlocking. Splitting the coordinator from the footer fan-out onto a
        // dedicated I/O pool would require a second executor here and is deferred until the
        // esql_external_blocking_io pool's sizing and lifecycle are resolved; the caller-supplied
        // executor is the current re-routing hook.
        executor.execute(() -> {
            try {
                Map<String, ExternalSourceResolution.ResolvedSource> resolved = Maps.newHashMapWithExpectedSize(paths.size());

                for (String path : paths) {
                    Map<String, Object> config = pathConfigs.getOrDefault(path, Map.of());
                    List<PartitionFilterHintExtractor.PartitionFilterHint> hints = filterHints != null ? filterHints.get(path) : null;
                    boolean hivePartitioning = isHivePartitioningEnabled(config);
                    // null => legacy eager for every path; non-null => eager only for listed paths.
                    boolean requiresStats = pathsRequiringStats == null || pathsRequiringStats.contains(path);

                    try {
                        ExternalSourceResolution.ResolvedSource resolvedSource = resolveSource(
                            path,
                            config,
                            hints,
                            hivePartitioning,
                            requiresStats
                        );
                        resolved.put(path, resolvedSource);
                        LOGGER.debug("Successfully resolved external source: {}", path);
                    } catch (TaskCancelledException e) {
                        // Surface cancellation unwrapped so the client sees a clean cancellation (4xx) rather
                        // than a generic "Failed to resolve external source" wrapper (500).
                        LOGGER.debug("External source resolution cancelled for [{}]", path);
                        listener.onFailure(e);
                        return;
                    } catch (IllegalArgumentException | UnsupportedOperationException e) {
                        // A footer read (e.g. the FFW anchor or a single-file source) can fail because the query was
                        // cancelled mid-read; surface that as cancellation rather than a bad-request/unsupported error.
                        if (reportIfCancelled(path, listener)) {
                            return;
                        }
                        recordDiscoveryFailure();
                        LOGGER.error("Failed to resolve external source [{}]: {}", path, e.getMessage(), e);
                        listener.onFailure(e);
                        return;
                    } catch (Exception e) {
                        // Same guard for wrapped failures (e.g. the schema cache wraps a cancelled anchor read in
                        // ExecutionException): a cancelled query must not surface as a generic 500.
                        if (reportIfCancelled(path, listener)) {
                            return;
                        }
                        recordDiscoveryFailure();
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
        boolean hivePartitioning,
        boolean requiresStats
    ) throws Exception {
        LOGGER.debug("Resolving external source: path=[{}]", path);

        // A query cancelled before resolution starts must do no storage I/O at all: bail before glob
        // expansion, cache listing, or any footer read.
        throwIfCancelled();

        if (GlobExpander.isMultiFile(path)) {
            return resolveMultiFileSource(path, config, hints, hivePartitioning, requiresStats);
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
                return SchemaCacheEntry.from(resolveSingleSource(path, config));
            });
            List<Attribute> schema = schemaEntry.toAttributes();
            extMetadata = buildMetadataFromCache(schemaEntry, schema, config);
        } else {
            SourceMetadata metadata = resolveSingleSource(path, config);
            extMetadata = wrapAsExternalSourceMetadata(metadata, config);
            object = provider.newObject(storagePath);
        }

        // Capture the raw file schema: schemaMap describes the physical schema each reader actually
        // sees, not the user-facing projection. _file.* columns are no longer glued onto the schema
        // here — they are request-driven (FROM ... METADATA _file.path, or the temporary EXTERNAL
        // shim that injects them into the relation's metadataFields). See ResolveExternalRelations.
        List<Attribute> fileSchema = extMetadata.schema();

        FileList singletonList = GlobExpander.fileListOf(
            List.of(new StorageEntry(storagePath, object.length(), object.lastModified())),
            path
        );
        // Single-file: degenerate case of the general flow — one-entry schemaMap, identity mapping.
        Map<StoragePath, SchemaReconciliation.FileSchemaInfo> schemaMap = singleEntrySchemaMap(storagePath, fileSchema);
        return new ExternalSourceResolution.ResolvedSource(extMetadata, singletonList, schemaMap);
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

    private ExternalSourceResolution.ResolvedSource resolveMultiFileSource(
        String path,
        Map<String, Object> config,
        @Nullable List<PartitionFilterHintExtractor.PartitionFilterHint> hints,
        boolean hivePartitioning,
        boolean requiresStats
    ) throws Exception {
        StoragePath storagePath = StoragePath.of(path);
        StorageProvider provider = resolveProvider(storagePath, config);

        FormatReader.SchemaResolution schemaResolution = parseSchemaResolution(config);
        boolean cacheable = isCacheable(provider);

        if (schemaResolution != FormatReader.SchemaResolution.FIRST_FILE_WINS) {
            int maxDiscoveredFiles = ExternalSourceSettings.MAX_DISCOVERED_FILES.get(settings);
            int maxGlobExpansion = ExternalSourceSettings.MAX_GLOB_EXPANSION.get(settings);
            long discoveryStartNanos = System.nanoTime();
            FileList raw = path.indexOf(',') >= 0
                ? GlobExpander.expandCommaSeparated(path, provider, hints, hivePartitioning, maxDiscoveredFiles, maxGlobExpansion)
                : GlobExpander.expandGlob(path, provider, hints, hivePartitioning, maxDiscoveredFiles, maxGlobExpansion);
            recordDiscovery(raw, discoveryStartNanos, storagePath.scheme());
            if (raw.fileCount() == 0) {
                throw new IllegalArgumentException("Glob pattern matched no files: " + path);
            }
            return resolveMultiFileWithReconciliation(raw, config, schemaResolution, cacheable);
        }

        FileList listing;
        long discoveryStartNanos = System.nanoTime();
        if (cacheable) {
            ListingCacheKey listingKey = ListingCacheKey.build(storagePath.scheme(), storagePath.host(), storagePath.path(), config);
            listing = cacheService.getOrComputeListing(
                listingKey,
                k -> expandAndCompact(path, provider, hints, hivePartitioning, storagePath)
            );
        } else {
            listing = expandAndCompact(path, provider, hints, hivePartitioning, storagePath);
        }
        recordDiscovery(listing, discoveryStartNanos, storagePath.scheme());

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

        // Glob expansion / cache listing above can be slow on wide globs; re-check before the anchor footer read.
        throwIfCancelled();

        ExternalSourceMetadata extMetadata;
        if (cacheable) {
            String formatType = detectFormatType(anchorPath);
            SchemaCacheKey schemaKey = SchemaCacheKey.build(anchorPath.toString(), anchorMtime, formatType, config);
            SchemaCacheEntry schemaEntry = cacheService.getOrComputeSchema(schemaKey, k -> {
                return SchemaCacheEntry.from(resolveSingleSource(anchorPath.toString(), config));
            });
            List<Attribute> schema = schemaEntry.toAttributes();
            extMetadata = buildMetadataFromCache(schemaEntry, schema, config);
        } else {
            SourceMetadata metadata = resolveSingleSource(anchorPath.toString(), config);
            extMetadata = wrapAsExternalSourceMetadata(metadata, config);
        }

        extMetadata = enrichWithFileCount(extMetadata, listing.fileCount());
        if (listing.fileCount() > 1 && requiresStats) {
            // For multi-file FIRST_FILE_WINS, read all files' metadata in parallel during Phase 1
            // to aggregate statistics across all files. This allows aggregate pushdown
            // (COUNT/MIN/MAX) to use accurate global stats and to skip Phase 2 (split discovery)
            // entirely for those queries.
            //
            // This eager all-file aggregation is gated on requiresStats: only query shapes that can
            // consume the global stats — an ungrouped aggregate over the relation, detected by
            // ExternalStatsRequirementExtractor#pathsRequiringEagerStats — pay the N footer reads.
            // Every other shape (LIMIT, SELECT *, grouped STATS ... BY, INLINESTATS) takes the
            // defer branch below: it keeps STATS_FILE_COUNT (from enrichWithFileCount above) but
            // marks stats partial, so Phase 2 split discovery reads footers once instead of twice.
            // Legacy callers pass requiresStats == true for every path (the resolve overload with a
            // null pathsRequiringStats set), preserving the original eager-for-all behavior.
            //
            // For an eager (requiresStats) resolve the cost is acceptable because:
            // - the cacheable path consults the schema cache, so repeat resolves are free;
            // - the non-cacheable path reads footers in parallel up to MAX_PARALLEL_METADATA_READS;
            // - the aggregated stats unlock skipping Phase 2 entirely for pushable aggregates
            // (see ComputeService#canSkipSplitDiscovery), which dominates the savings.
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
        } else if (listing.fileCount() > 1) {
            // Defer branch (requiresStats == false): skip the N footer reads. The anchor-only stats
            // are not representative of the whole glob, so mark them partial — exactly the state the
            // failed-aggregation path above produces, which downstream already handles
            // (SplitStats.resolveEffectiveStats returns null rather than consuming anchor stats as
            // global). STATS_FILE_COUNT, stamped by enrichWithFileCount above, is preserved.
            extMetadata = markStatsAsPartial(extMetadata);
        }

        // The anchor's pre-enrichment schema is the physical read schema every file's reader parses.
        // Partition columns are path-derived (injected by VirtualColumnIterator at read time), so they
        // are never part of the physical read schema; the data-only view below drives the mapping
        // output width.
        List<Attribute> physicalSchema = extMetadata.schema();
        List<Attribute> dataOnlySchema = physicalSchema;

        PartitionMetadata partitionMetadata = listing.partitionMetadata();
        if (partitionMetadata != null && partitionMetadata.isEmpty() == false) {
            // Shadow same-named physical columns: when a physical column collides with a partition
            // key, the partition (path-derived) value wins (Spark/DuckDB semantics). The reader still
            // parses the physical column (CSV is positional), but the per-file mapping drops it from
            // the output so the mapping width agrees with the data-only unified schema at
            // ColumnMapping#pruneToPerFileQuery and with queryDataSchema at the SchemaAdaptingIterator
            // guard. enrichSchemaWithPartitionColumns appends the partition column and warns.
            dataOnlySchema = ExternalSchema.dataAttributesOf(physicalSchema, partitionMetadata.partitionColumns().keySet()).attributes();
            extMetadata = enrichSchemaWithPartitionColumns(extMetadata, partitionMetadata);
        }

        // _file.* columns are request-driven now; no auto-attach to the schema. See
        // ResolveExternalRelations / the EXTERNAL shim.

        // FFW: every file's readSchema is the anchor's physical schema; the mapping is identity unless
        // a partition key shadows a physical column, in which case it narrows the output to the
        // data-only columns (the shadowed physical column is parsed but not emitted).
        Map<StoragePath, SchemaReconciliation.FileSchemaInfo> schemaMap;
        if (physicalSchema != null && physicalSchema.isEmpty() == false) {
            Map<StoragePath, SchemaReconciliation.FileSchemaInfo> perFileInfo = Maps.newHashMapWithExpectedSize(listing.fileCount());
            ExternalSchema fileSchema = new ExternalSchema(physicalSchema);
            ColumnMapping mapping = dataOnlySchema.size() == physicalSchema.size()
                ? new ColumnMapping(identityMapping(physicalSchema.size()), null)
                : SchemaReconciliation.computeMapping(dataOnlySchema, physicalSchema);
            for (int i = 0; i < listing.fileCount(); i++) {
                perFileInfo.put(listing.path(i), new SchemaReconciliation.FileSchemaInfo(fileSchema, mapping, null));
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

    private ExternalSourceResolution.ResolvedSource resolveMultiFileWithReconciliation(
        FileList fileList,
        Map<String, Object> config,
        FormatReader.SchemaResolution schemaResolution,
        boolean cacheable
    ) throws Exception {
        long startNanos = System.nanoTime();
        Map<StoragePath, SourceMetadata> allMetadata = readAllFileMetadata(fileList, config, cacheable);
        long durationMs = (System.nanoTime() - startNanos) / 1_000_000;

        LOGGER.debug("Schema reconciliation [{}]: scanned {} files in {}ms", schemaResolution, allMetadata.size(), durationMs);

        StoragePath firstFile = fileList.path(0);
        SchemaReconciliation.Result result;
        if (schemaResolution == FormatReader.SchemaResolution.STRICT) {
            result = SchemaReconciliation.reconcileStrict(firstFile, allMetadata);
        } else {
            result = SchemaReconciliation.reconcileUnionByName(allMetadata);
        }

        // Shadow physical columns that collide with Hive partition keys: the partition (path-derived)
        // value wins (Spark/DuckDB semantics), so the unified schema and every per-file mapping's
        // output must drop the physical column. The file (physical) schema is preserved so positional
        // readers (CSV) still parse the column; enrichSchemaWithPartitionColumns re-adds the partition
        // column to the coordinator-facing schema below. Keeping the mapping width data-only keeps it
        // in agreement with the data-only unified schema at ColumnMapping#pruneToPerFileQuery and with
        // queryDataSchema at the data-node SchemaAdaptingIterator guard.
        //
        // Ordering matters: shadowPartitionCollisions emits the single shadow warning and prunes the
        // collision here, so the enrichSchemaWithPartitionColumns call below sees a data-only schema and
        // does not warn again (the no-double-warning invariant, asserted at that call). Do not reorder.
        PartitionMetadata partitionMetadata = fileList.partitionMetadata();
        Set<String> partitionNames = partitionMetadata != null ? partitionMetadata.partitionColumns().keySet() : Set.of();
        result = shadowPartitionCollisions(result, partitionNames);

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

        if (partitionMetadata != null && partitionMetadata.isEmpty() == false) {
            // No-double-warning invariant: shadowPartitionCollisions above already pruned any physical
            // column that collides with a partition key (and emitted the one shadow warning), so the
            // post-shadow schema must be collision-free before enrich runs its own shadow detection.
            assert extMetadata.schema().stream().noneMatch(a -> partitionNames.contains(a.name()))
                : "shadowPartitionCollisions must run before enrichSchemaWithPartitionColumns: a physical "
                    + "column still collides with a partition key, which would warn twice";
            extMetadata = enrichSchemaWithPartitionColumns(extMetadata, partitionMetadata);
        }

        // _file.* columns are request-driven now; no auto-attach to the schema. See
        // ResolveExternalRelations / the EXTERNAL shim.

        Map<StoragePath, SchemaReconciliation.FileSchemaInfo> schemaMap = result.perFileInfo();
        return new ExternalSourceResolution.ResolvedSource(extMetadata, fileList, schemaMap);
    }

    /**
     * Returns a copy of {@code result} with physical columns shadowed by Hive partition keys removed
     * from the unified schema and from every per-file mapping's output, while each file's physical
     * schema is preserved so positional readers (e.g. CSV) still parse every column. The partition
     * (path-derived) value wins (Spark/DuckDB semantics); {@link #enrichSchemaWithPartitionColumns}
     * later re-adds the partition column to the coordinator schema. Emits one client-facing warning
     * per shadowed column (see {@link #warnOnShadowedColumns(List)}). Returns {@code result}
     * unchanged when {@code partitionColumnNames} is empty or nothing is shadowed.
     */
    private static SchemaReconciliation.Result shadowPartitionCollisions(
        SchemaReconciliation.Result result,
        Set<String> partitionColumnNames
    ) {
        if (partitionColumnNames.isEmpty()) {
            return result;
        }
        List<Attribute> unified = result.unifiedSchema().attributes();
        List<Attribute> dataOnlyUnified = new ArrayList<>(unified.size());
        List<String> shadowedColumns = new ArrayList<>();
        for (Attribute attr : unified) {
            if (partitionColumnNames.contains(attr.name())) {
                shadowedColumns.add(attr.name());
            } else {
                dataOnlyUnified.add(attr);
            }
        }
        if (shadowedColumns.isEmpty()) {
            return result;
        }
        warnOnShadowedColumns(shadowedColumns);
        // Order is irrelevant: Map.copyOf below discards insertion order and FileSplitProvider looks
        // up per-file info by key (matches reconcileUnionByName's own Map.copyOf pattern).
        Map<StoragePath, SchemaReconciliation.FileSchemaInfo> perFileInfo = Maps.newHashMapWithExpectedSize(result.perFileInfo().size());
        for (Map.Entry<StoragePath, SchemaReconciliation.FileSchemaInfo> entry : result.perFileInfo().entrySet()) {
            SchemaReconciliation.FileSchemaInfo info = entry.getValue();
            // Recompute the mapping against the data-only unified schema; the file (physical) schema is
            // unchanged so the reader still parses every column, including the shadowed one.
            ColumnMapping mapping = SchemaReconciliation.computeMapping(dataOnlyUnified, info.fileSchema().attributes());
            perFileInfo.put(entry.getKey(), new SchemaReconciliation.FileSchemaInfo(info.fileSchema(), mapping, info.statistics()));
        }
        return new SchemaReconciliation.Result(new ExternalSchema(dataOnlyUnified), Map.copyOf(perFileInfo));
    }

    /** Per-file metadata, in parallel. When {@code cacheable} is true, each resolve goes through
     *  the schema cache (keyed on path + mtime) so warm queries against the same paths hit cache. */
    private Map<StoragePath, SourceMetadata> readAllFileMetadata(FileList fileList, Map<String, Object> config, boolean cacheable)
        throws Exception {
        int fileCount = fileList.fileCount();
        List<Integer> indices = new ArrayList<>(fileCount);
        for (int i = 0; i < fileCount; i++) {
            indices.add(i);
        }

        List<Map.Entry<StoragePath, SourceMetadata>> entries = BoundedParallelGather.gather(indices, i -> {
            throwIfCancelled();
            StoragePath filePath = fileList.path(i);
            // Carry the cancellation signal across the synchronous footer read so a backoff sleep in
            // the storage retry layer aborts promptly on cancel.
            SourceMetadata meta = StorageRetryCancellation.callWithCancellation(
                this::isCancelled,
                () -> cacheable
                    ? cachedResolveSingleSource(filePath, fileList.lastModifiedMillis(i), config)
                    : resolveSingleSource(filePath.toString(), config)
            );
            return Map.entry(filePath, meta);
        }, MAX_PARALLEL_METADATA_READS, executor);

        Map<StoragePath, SourceMetadata> result = new LinkedHashMap<>();
        for (Map.Entry<StoragePath, SourceMetadata> entry : entries) {
            result.put(entry.getKey(), entry.getValue());
        }
        return result;
    }

    /** Cache-aware single-file resolve. Mirrors the FFW path — exceptions propagate (no catch). */
    private SourceMetadata cachedResolveSingleSource(StoragePath filePath, long mtime, Map<String, Object> config) throws Exception {
        String formatType = detectFormatType(filePath);
        SchemaCacheKey schemaKey = SchemaCacheKey.build(filePath.toString(), mtime, formatType, config);
        SchemaCacheEntry entry = cacheService.getOrComputeSchema(
            schemaKey,
            k -> SchemaCacheEntry.from(resolveSingleSource(filePath.toString(), config))
        );
        return buildMetadataFromCache(entry, entry.toAttributes(), config);
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
            allMeta = BoundedParallelGather.gather(paths, filePath -> {
                throwIfCancelled();
                return StorageRetryCancellation.callWithCancellation(
                    this::isCancelled,
                    () -> resolveSingleSource(filePath.toString(), config)
                );
            }, MAX_PARALLEL_METADATA_READS, executor);
        } catch (TaskCancelledException e) {
            // Cancellation is not a "could not aggregate stats" condition — propagate it so the query
            // aborts promptly instead of silently degrading to partial stats and continuing.
            throw e;
        } catch (Exception e) {
            // If the query was cancelled, a read may have failed for that reason; surface cancellation
            // rather than masking it as partial stats.
            throwIfCancelled();
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
            // Cancellation is checked before the per-file try so it is never swallowed as "partial stats".
            throwIfCancelled();
            StoragePath filePath = listing.path(i);
            long mtime = listing.lastModifiedMillis(i);
            String formatType = detectFormatType(filePath);
            SchemaCacheKey schemaKey = SchemaCacheKey.build(filePath.toString(), mtime, formatType, config);
            try {
                SchemaCacheEntry entry = StorageRetryCancellation.callWithCancellation(
                    this::isCancelled,
                    () -> cacheService.getOrComputeSchema(
                        schemaKey,
                        k -> SchemaCacheEntry.from(resolveSingleSource(filePath.toString(), config))
                    )
                );
                Map<String, Object> fileMeta = entry.safeMetadata();
                if (fileMeta == null || fileMeta.containsKey(SourceStatisticsSerializer.STATS_ROW_COUNT) == false) {
                    // This file has no statistics — cannot produce accurate global stats.
                    return null;
                }
                perFileStats.add(fileMeta);
            } catch (TaskCancelledException e) {
                // A bare cancellation (e.g. from a cache wait point) must abort, not degrade to partial stats.
                throw e;
            } catch (Exception e) {
                // The schema cache wraps loader failures in ExecutionException, so a cancellation observed
                // while reading a footer can arrive wrapped here; re-check the supplier so it surfaces as
                // cancellation rather than being masked as partial stats.
                throwIfCancelled();
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
            if (factory.canHandle(path, config)) {
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
        // Physical columns dropped because a same-named partition key shadows them. Collected in
        // schema order (deduped) so we can emit one client-facing warning per shadowed column.
        List<String> shadowedColumns = new ArrayList<>();

        for (Attribute attr : originalSchema) {
            if (partitionNames.contains(attr.name()) == false) {
                enrichedSchema.add(attr);
            } else if (shadowedColumns.contains(attr.name()) == false) {
                // Partition (path-derived) value wins; the physical column is hidden (Spark/DuckDB
                // semantics). The escape hatch to read the physical column is hive_partitioning:false.
                shadowedColumns.add(attr.name());
            }
        }

        warnOnShadowedColumns(shadowedColumns);

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

    /**
     * Emits one client-facing response-header WARN per physical column that a same-named Hive
     * partition key shadows. Shadowing follows Spark (SPARK-27356) and DuckDB: the partition
     * (path-derived) value wins and the physical column is hidden. The warning lets clients notice
     * silent data substitution and points at the {@code hive_partitioning: false} escape hatch.
     * <p>
     * Delegates to {@link SkipWarnings}, which emits the summary once on the first detail and
     * routes through {@link org.elasticsearch.common.logging.HeaderWarning}; when no thread context
     * is bound (e.g. some unit tests) the writes are silently dropped. A no-op when nothing is
     * shadowed.
     */
    private static void warnOnShadowedColumns(List<String> shadowedColumns) {
        if (shadowedColumns.isEmpty()) {
            return;
        }
        SkipWarnings warnings = new SkipWarnings(
            "one or more physical columns are shadowed by same-named Hive partition keys; "
                + "the partition (path-derived) value is used. Set hive_partitioning to false to read the physical column instead."
        );
        for (String name : shadowedColumns) {
            warnings.add("physical column [" + name + "] is shadowed by a same-named Hive partition key");
        }
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
