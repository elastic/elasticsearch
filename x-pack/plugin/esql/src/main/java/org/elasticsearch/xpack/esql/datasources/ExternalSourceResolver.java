/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ContextPreservingActionListener;
import org.elasticsearch.cluster.metadata.DatasetFieldMapping;
import org.elasticsearch.cluster.metadata.DatasetMapping;
import org.elasticsearch.common.logging.HeaderWarning;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.util.concurrent.ThrottledIterator;
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
import org.elasticsearch.xpack.esql.datasources.cache.ExternalStats;
import org.elasticsearch.xpack.esql.datasources.cache.FileMetadata;
import org.elasticsearch.xpack.esql.datasources.cache.FileMetadataCacheKey;
import org.elasticsearch.xpack.esql.datasources.cache.ListingCacheKey;
import org.elasticsearch.xpack.esql.datasources.cache.SchemaCacheEntry;
import org.elasticsearch.xpack.esql.datasources.cache.SchemaCacheKey;
import org.elasticsearch.xpack.esql.datasources.glob.GlobExpander;
import org.elasticsearch.xpack.esql.datasources.spi.DeclaredTypeCoercions;
import org.elasticsearch.xpack.esql.datasources.spi.ErrorPolicy;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalSourceFactory;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalSourceMetrics;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalUnavailableException;
import org.elasticsearch.xpack.esql.datasources.spi.FileList;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReader;
import org.elasticsearch.xpack.esql.datasources.spi.ListingHint;
import org.elasticsearch.xpack.esql.datasources.spi.SimpleSourceMetadata;
import org.elasticsearch.xpack.esql.datasources.spi.SkipWarnings;
import org.elasticsearch.xpack.esql.datasources.spi.SourceMetadata;
import org.elasticsearch.xpack.esql.datasources.spi.SourceStatistics;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.datasources.spi.StorageProvider;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.function.Supplier;

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
     * Default cap on in-flight per-file metadata reads during a multi-file discovery when a caller does not supply
     * one. Production wires the {@code esql_worker} pool size here (via {@code TransportEsqlQueryAction}/{@code
     * PlanExecutor}) so the fan-out is bounded by an in-flight permit equal to the pool size; because footer reads
     * are async (released across the network round-trip) that permit does not translate into that many pinned
     * threads. Kept as a fallback for the constructors used by tests and by callers that do not thread the pool size.
     */
    static final int DEFAULT_METADATA_READ_CONCURRENCY = 16;

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
    private final int metadataReadConcurrency;

    /**
     * Supplier consulted before each per-file footer read so that an in-flight resolution of a large
     * glob aborts promptly when the originating query is cancelled. {@code null} means "never cancelled"
     * (used by tests and call sites that do not carry a {@code CancellableTask}).
     */
    @Nullable
    private final BooleanSupplier isCancelled;

    /**
     * A restorable snapshot of the coordinating request's {@link ThreadContext}, captured at construction time
     * (see {@code ThreadContext#newRestorableContext}) while still on the transport thread that carries the
     * caller's {@code Authentication} (see {@code PlanExecutor#esql}). Restored around the outward {@link #resolve}
     * completion listener so that when a factory's async metadata read completes on a non-ES thread (e.g. a Netty
     * I/O thread owned by a native async storage SDK client, which never had the ES context installed), the rest of
     * the synchronous continuation — back through {@code EsqlSession} and into the compute transport send — runs
     * with the original request's security context rather than an empty one. Capturing the snapshot eagerly here,
     * rather than lazily from {@link #resolve}, means the guarantee holds even if a future refactor moves
     * {@link #resolve} itself onto a thread that no longer carries the caller's context.
     * {@code null} means "no context to preserve" (used by tests and call sites that do not run inside a real
     * request, matching the existing {@code isCancelled == null} convention).
     */
    @Nullable
    private final Supplier<ThreadContext.StoredContext> restorableContext;

    /**
     * Hive-partition shadow-column warning messages collected during one {@link #resolve} call's schema-resolution
     * chain (see {@link #warnOnShadowedColumns}). That chain runs on {@link #metadataReadExecutor} — a real thread
     * pool in production, so a direct {@code HeaderWarning.addWarning} call from inside it would land on that
     * executor thread's {@link ThreadContext} rather than the originating request's, and never reach the client.
     * Messages are instead buffered here and replayed via {@code HeaderWarning} from {@link #resolve}'s completion
     * listener, once {@link #restorableContext} has restored the caller's original context. Cleared at the start of
     * each {@link #resolve} call; safe for concurrent per-file callbacks (see {@link #metadataReadConcurrency}) since
     * it is append-only until the single flush at completion.
     */
    private final List<String> pendingShadowWarnings = new CopyOnWriteArrayList<>();

    /**
     * The {@link #executor} decorated so that every task it runs has the query cancellation signal installed as the
     * ambient {@link StorageRetryCancellation} scope. This is the executor handed to factories for the async footer
     * reads: for storage backends whose {@code readBytesAsync} is an executor-backed synchronous read (local, GCS,
     * S3-without-async-client) the blocking read — and its retry/throttle backoff — runs inside one of these tasks, so
     * a query cancelled mid-backoff aborts promptly (the backoff polls the ambient signal). Backends with a native
     * async client return from the executor task before the SDK callback and are not covered here (nor by the
     * synchronous path), matching {@link StorageRetryCancellation}'s documented thread-affinity limits.
     */
    private final Executor metadataReadExecutor;

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
        this(executor, dataSourceModule, settings, cacheService, (BooleanSupplier) null, DEFAULT_METADATA_READ_CONCURRENCY);
    }

    public ExternalSourceResolver(
        Executor executor,
        DataSourceModule dataSourceModule,
        Settings settings,
        @Nullable ExternalSourceCacheService cacheService,
        @Nullable BooleanSupplier isCancelled
    ) {
        this(executor, dataSourceModule, settings, cacheService, isCancelled, DEFAULT_METADATA_READ_CONCURRENCY);
    }

    /**
     * @param metadataReadConcurrency maximum number of in-flight per-file metadata reads during a multi-file
     *            discovery. Production passes the {@code esql_worker} pool size.
     */
    public ExternalSourceResolver(
        Executor executor,
        DataSourceModule dataSourceModule,
        Settings settings,
        @Nullable ExternalSourceCacheService cacheService,
        int metadataReadConcurrency
    ) {
        this(executor, dataSourceModule, settings, cacheService, null, metadataReadConcurrency);
    }

    /**
     * @param isCancelled consulted before each per-file footer read so a wide-glob discovery aborts promptly on
     *            cancellation; {@code null} means "never cancelled".
     * @param metadataReadConcurrency maximum number of in-flight per-file metadata reads during a multi-file
     *            discovery. Production passes the {@code esql_worker} pool size.
     */
    public ExternalSourceResolver(
        Executor executor,
        DataSourceModule dataSourceModule,
        Settings settings,
        @Nullable ExternalSourceCacheService cacheService,
        @Nullable BooleanSupplier isCancelled,
        int metadataReadConcurrency
    ) {
        this(executor, dataSourceModule, settings, cacheService, isCancelled, metadataReadConcurrency, null);
    }

    /**
     * @param isCancelled consulted before each per-file footer read so a wide-glob discovery aborts promptly on
     *            cancellation; {@code null} means "never cancelled".
     * @param metadataReadConcurrency maximum number of in-flight per-file metadata reads during a multi-file
     *            discovery. Production passes the {@code esql_worker} pool size.
     * @param threadContext the calling request's transport {@link ThreadContext}, captured while still on the
     *            authenticated calling thread; restored around the outward {@link #resolve} completion listener so
     *            that async completions on non-ES threads don't lose the request's security context. {@code null}
     *            when there is no context to preserve (tests, non-request call sites).
     */
    public ExternalSourceResolver(
        Executor executor,
        DataSourceModule dataSourceModule,
        Settings settings,
        @Nullable ExternalSourceCacheService cacheService,
        @Nullable BooleanSupplier isCancelled,
        int metadataReadConcurrency,
        @Nullable ThreadContext threadContext
    ) {
        if (metadataReadConcurrency < 1) {
            throw new IllegalArgumentException("metadataReadConcurrency must be >= 1, got: " + metadataReadConcurrency);
        }
        this.executor = executor;
        this.dataSourceModule = dataSourceModule;
        this.settings = settings;
        this.cacheService = cacheService;
        this.isCancelled = isCancelled;
        this.metrics = dataSourceModule == null ? ExternalSourceMetrics.NOOP : dataSourceModule.externalSourceMetrics();
        this.metadataReadConcurrency = metadataReadConcurrency;
        this.restorableContext = threadContext == null ? null : threadContext.newRestorableContext(true);
        // Install the query cancellation signal as the ambient StorageRetryCancellation scope for every footer read
        // dispatched to the executor, so an executor-backed synchronous read's backoff aborts promptly on cancel.
        this.metadataReadExecutor = command -> executor.execute(
            () -> StorageRetryCancellation.runWithCancellation(this::isCancelled, command::run)
        );
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
        resolve(paths, pathConfigs, filterHints, null, null, listener);
    }

    /**
     * Resolves external sources. A per-path declared mapping drives strict/non-strict schema resolution and carries
     * column renames; {@code pathsRequiringStats} gates the FIRST_FILE_WINS eager all-file stats aggregation.
     *
     * @param declaredMappings    per-path declared mapping — strict skips inference, non-strict overlays it, and its
     *        derived read-instructions (renames, {@code _id.path}, date formats) ride a typed {@code DeclaredReadSpec} to
     *        the reader boundary; {@code null} when no path declares a mapping.
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
        @Nullable Map<String, DatasetMapping> declaredMappings,
        @Nullable Set<String> pathsRequiringStats,
        ActionListener<ExternalSourceResolution> listener
    ) {
        if (paths == null || paths.isEmpty()) {
            listener.onResponse(ExternalSourceResolution.EMPTY);
            return;
        }

        // Fresh per-call: resolve() is the single entry point for one query's external-source resolution, so
        // clearing here (rather than after the previous call's flush) also covers a resolver instance reused
        // across resolve() calls in tests.
        pendingShadowWarnings.clear();

        // Resolution runs on the caller-supplied executor (esql_worker in production, isolated from SEARCH so a wide
        // wildcard cannot starve regular ES searches). The initial dispatch performs the cheap synchronous prep (glob
        // expansion, the FFW anchor / single-file footer read) and then hands the multi-file fan-out to async footer
        // reads bounded by metadataReadConcurrency in-flight reads, so the executor thread is not held across the N
        // network round-trips. Paths are resolved sequentially (see resolveNextPath) so the accumulation map needs no
        // synchronization and the first failure short-circuits the rest — mirroring the previous synchronous loop.
        //
        // Dispatch on metadataReadExecutor (not the bare executor) so the query cancellation signal is installed as the
        // ambient StorageRetryCancellation scope for the whole sequential prep: a cancelled wide-glob discovery then
        // aborts its glob-expansion and anchor/single-file read backoff promptly, matching the per-read wrapping the
        // async fan-out already gets.
        //
        // Flush any Hive-partition shadow-column warnings buffered in pendingShadowWarnings (see its javadoc) before
        // delegating to the caller's listener, so HeaderWarning.addWarning is called from here rather than from
        // whatever executor thread actually ran the schema reconciliation.
        ActionListener<ExternalSourceResolution> withShadowWarnings = ActionListener.runBefore(
            listener,
            () -> pendingShadowWarnings.forEach(HeaderWarning::addWarning)
        );
        // Wrap the outward listener so that when a factory's async metadata read completes on a non-ES thread (e.g.
        // a Netty I/O thread owned by a native async storage SDK client), the caller's authenticated ThreadContext is
        // restored before the listener's continuation runs — covering the rest of the synchronous chain back through
        // EsqlSession and into the compute transport send, and (per above) the shadow-warning flush itself. See the
        // field javadoc on restorableContext for details.
        ActionListener<ExternalSourceResolution> resolveListener = restorableContext == null
            ? withShadowWarnings
            : new ContextPreservingActionListener<>(restorableContext, withShadowWarnings);
        Map<String, ExternalSourceResolution.ResolvedSource> resolved = Maps.newHashMapWithExpectedSize(paths.size());
        metadataReadExecutor.execute(
            () -> resolveNextPath(paths, 0, pathConfigs, filterHints, declaredMappings, pathsRequiringStats, resolved, resolveListener)
        );
    }

    /**
     * Resolves {@code paths[index]} asynchronously, then chains to the next path on success. Paths are resolved
     * sequentially so the accumulation map needs no synchronization and the first failure short-circuits the rest.
     */
    private void resolveNextPath(
        List<String> paths,
        int index,
        Map<String, Map<String, Object>> pathConfigs,
        @Nullable Map<String, List<PartitionFilterHintExtractor.PartitionFilterHint>> filterHints,
        @Nullable Map<String, DatasetMapping> declaredMappings,
        @Nullable Set<String> pathsRequiringStats,
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
        // null => legacy eager for every path; non-null => eager only for listed paths.
        boolean requiresStats = pathsRequiringStats == null || pathsRequiringStats.contains(path);
        DatasetMapping declaredMapping = declaredMappings != null ? declaredMappings.get(path) : null;
        // The declared mapping's read-instructions (logical->physical column renames, _id.path, date formats) travel as a
        // typed DeclaredReadSpec on the ResolvedSource -> ExternalRelation -> ExternalSourceExec seam, rather than as
        // string keys in the untyped config map. Renames are consumed on the data node by the centralized last-mile
        // physicalization (PhysicalNames) and the pushdown planner rules (readers stay rename-agnostic); _id.path makes
        // the data node stamp _id from that column rather than the synthetic (file+row-position) identity.
        DeclaredReadSpec declaredReadSpec = declaredReadSpecOf(declaredMapping);

        resolveSource(path, config, hints, hivePartitioning, declaredMapping, requiresStats, ActionListener.wrap(resolvedSource -> {
            // Strict is built directly from the declaration inside resolveSource; non-strict infers first and then
            // overlays the declaration onto the resolved result (works the same for single- and multi-file).
            ExternalSourceResolution.ResolvedSource finalSource = declaredMapping != null && isStrict(declaredMapping) == false
                ? applyNonStrictOverlay(resolvedSource, declaredMapping)
                : resolvedSource;
            resolved.put(path, finalSource.withDeclaredReadSpec(declaredReadSpec));
            LOGGER.debug("Successfully resolved external source: {}", path);
            resolveNextPath(paths, index + 1, pathConfigs, filterHints, declaredMappings, pathsRequiringStats, resolved, listener);
        }, e -> listener.onFailure(mapResolveFailure(path, e))));
    }

    /**
     * Reproduces the previous loop's error contract: a cancelled query surfaces {@link TaskCancelledException}
     * unwrapped (so the client sees a clean 4xx rather than a generic 500), {@link IllegalArgumentException} and
     * {@link UnsupportedOperationException} (client-caused) propagate unwrapped, and any other failure is wrapped in
     * an {@link ElasticsearchException} carrying the path and detail. A footer read can fail <em>because</em> the
     * query was cancelled mid-read and arrive wrapped (e.g. the schema cache wraps loader failures), so the
     * cancellation state is consulted directly rather than matched on the exception type.
     * <p>
     * A retryable back-pressure failure (permit exhaustion / remote-store unavailability) reaches here as an
     * {@link ExternalUnavailableException} (503), but the factory loop always re-wraps a factory failure in an
     * {@link IllegalArgumentException} (400). So the 503 is recovered from the cause chain <em>before</em> the
     * {@link IllegalArgumentException} branch and surfaced as a 503, otherwise a transient capacity condition would be
     * masked as a non-retryable client error and the client's retry path would never engage. An interrupt during permit
     * acquisition arrives the same way as an {@link EsRejectedExecutionException} (429) and is recovered identically so a
     * node-level rejection is not masked as a 400.
     */
    private RuntimeException mapResolveFailure(String path, Exception e) {
        if (e instanceof TaskCancelledException tce) {
            LOGGER.debug("External source resolution cancelled for [{}]", path);
            return tce;
        }
        if (isCancelled()) {
            LOGGER.debug("External source resolution cancelled for [{}]", path);
            return new TaskCancelledException(RESOLUTION_CANCELLED_MESSAGE);
        }
        // A buried 503 (retryable back-pressure) must not be masked as a 400 by the factory loop's IllegalArgumentException
        // wrapper. unwrap walks the root + cause chain (cycle-guarded), so it catches the 503 raw or wrapped. Re-wrap so
        // the client message keeps the path context while the 503 status and the throttling flag survive.
        ExternalUnavailableException unavailable = (ExternalUnavailableException) ExceptionsHelper.unwrap(
            e,
            ExternalUnavailableException.class
        );
        if (unavailable != null) {
            recordDiscoveryFailure();
            LOGGER.warn("Failed to resolve external source [{}]: {}", path, e.getMessage(), e);
            return new ExternalUnavailableException(
                unavailable.throttling(),
                unavailable,
                "Failed to resolve external source [{}]: {}",
                path,
                unavailable.getMessage()
            );
        }
        // A permit-acquisition interrupt surfaces as an EsRejectedExecutionException (429). The factory loop wraps it
        // in an IllegalArgumentException (400), so recover it from the cause chain before the IllegalArgumentException
        // branch: a node-level rejection must keep its 429 status instead of being masked as a client error. Re-wrap
        // so the client message keeps the path context while the 429 status survives (the type has no cause constructor).
        EsRejectedExecutionException rejected = (EsRejectedExecutionException) ExceptionsHelper.unwrap(
            e,
            EsRejectedExecutionException.class
        );
        if (rejected != null) {
            recordDiscoveryFailure();
            LOGGER.warn("Failed to resolve external source [{}]: {}", path, e.getMessage(), e);
            EsRejectedExecutionException wrapped = new EsRejectedExecutionException(
                String.format(Locale.ROOT, "Failed to resolve external source [%s]: %s", path, rejected.getMessage())
            );
            wrapped.initCause(rejected);
            return wrapped;
        }
        if (e instanceof IllegalArgumentException || e instanceof UnsupportedOperationException) {
            recordDiscoveryFailure();
            LOGGER.error("Failed to resolve external source [{}]: {}", path, e.getMessage(), e);
            return (RuntimeException) e;
        }
        recordDiscoveryFailure();
        LOGGER.error("Failed to resolve external source [{}]: {}", path, e.getMessage(), e);
        String detail = e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName();
        return new ElasticsearchException(String.format(Locale.ROOT, "Failed to resolve external source [%s]: %s", path, detail), e);
    }

    private void resolveSource(
        String path,
        Map<String, Object> config,
        @Nullable List<PartitionFilterHintExtractor.PartitionFilterHint> hints,
        boolean hivePartitioning,
        @Nullable DatasetMapping declaredMapping,
        boolean requiresStats,
        ActionListener<ExternalSourceResolution.ResolvedSource> listener
    ) {
        LOGGER.debug("Resolving external source: path=[{}]", path);
        try {
            resolveSourceInner(path, config, hints, hivePartitioning, declaredMapping, requiresStats, listener);
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    private void resolveSourceInner(
        String path,
        Map<String, Object> config,
        @Nullable List<PartitionFilterHintExtractor.PartitionFilterHint> hints,
        boolean hivePartitioning,
        @Nullable DatasetMapping declaredMapping,
        boolean requiresStats,
        ActionListener<ExternalSourceResolution.ResolvedSource> listener
    ) throws Exception {
        // A query cancelled before resolution starts must do no storage I/O at all: bail before glob
        // expansion, cache listing, or any footer read.
        throwIfCancelled();

        if (GlobExpander.isMultiFile(path)) {
            resolveMultiFileSource(path, config, hints, hivePartitioning, declaredMapping, requiresStats, listener);
        } else {
            resolveSingleFileSource(path, config, declaredMapping, listener);
        }
    }

    /**
     * Resolves a single, explicitly-referenced file. The footer read here is one bounded read on the resolver
     * executor thread (not the fan-out), so it stays synchronous — matching the coordinator/anchor read cost. The
     * multi-file fan-out is what goes async (see {@link #gatherPerFile}).
     */
    private void resolveSingleFileSource(
        String path,
        Map<String, Object> config,
        @Nullable DatasetMapping declaredMapping,
        ActionListener<ExternalSourceResolution.ResolvedSource> listener
    ) throws Exception {
        /*
         * A concrete one-entry FileList is required so {@link org.elasticsearch.xpack.esql.datasources.FileSplitProvider}
         * can discover block-aligned splits for compressed files (e.g. .json.bz2). UNRESOLVED lists skip split discovery,
         * which forces coordinator execution down paths that never see per-split byte ranges and yields incorrect counts.
         */
        StoragePath storagePath = StoragePath.of(path);
        StorageProvider provider = resolveProvider(storagePath, config);

        // Strict declaration is the entire schema: build directly from the declaration (one bounded anchor footer read
        // for columnar coercibility), no inference. The non-strict overlay is applied by the caller after this returns.
        if (isStrict(declaredMapping)) {
            listener.onResponse(resolveStrictSingleFile(path, storagePath, provider, config, declaredMapping));
            return;
        }

        ExternalSourceMetadata extMetadata;
        StorageEntry storageEntry;
        if (isCacheable(provider)) {
            // Warm path is zero-I/O: the file-metadata cache holds {length, mtime} within the schema TTL, so a warm
            // single-file resolve never touches a live object (fileMetadataOf). mtime is the cache key's version token;
            // length + mtime rebuild the singleton FileList.
            FileMetadata meta = fileMetadataOf(storagePath, provider, config);
            String formatType = detectFormatType(storagePath);
            SchemaCacheKey schemaKey = SchemaCacheKey.build(storagePath.toString(), meta.mtimeMillis(), formatType, config);
            SchemaCacheEntry schemaEntry = cacheService.getOrComputeSchema(schemaKey, k -> {
                return SchemaCacheEntry.from(resolveSingleSource(path, config));
            });
            List<Attribute> schema = schemaEntry.toAttributes();
            extMetadata = buildMetadataFromCache(schemaEntry, schema, config);
            storageEntry = new StorageEntry(storagePath, meta.length(), Instant.ofEpochMilli(meta.mtimeMillis()));
        } else {
            SourceMetadata metadata = resolveSingleSource(path, config);
            extMetadata = wrapAsExternalSourceMetadata(metadata, config, declaredReadSpecOf(declaredMapping));
            StorageObject object = provider.newObject(storagePath);
            storageEntry = new StorageEntry(storagePath, object.length(), object.lastModified());
        }

        // Capture the raw file schema: schemaMap describes the physical schema each reader actually
        // sees, not the user-facing projection. _file.* columns are no longer glued onto the schema
        // here — they are request-driven (FROM ... METADATA _file.path, or the temporary EXTERNAL
        // shim that injects them into the relation's metadataFields). See ResolveExternalRelations.
        List<Attribute> fileSchema = extMetadata.schema();

        FileList singletonList = GlobExpander.fileListOf(List.of(storageEntry), path);
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
        @Nullable DatasetMapping declaredMapping,
        boolean requiresStats,
        ActionListener<ExternalSourceResolution.ResolvedSource> listener
    ) throws Exception {
        StoragePath storagePath = StoragePath.of(path);
        StorageProvider provider = resolveProvider(storagePath, config);

        // Strict declaration is the whole schema for every file, so inference (FIRST_FILE_WINS / reconciliation) is
        // skipped entirely — only the glob listing plus, for columnar formats, one anchor footer read to validate
        // declared-type coercibility. The non-strict overlay is applied by the caller after this returns.
        if (isStrict(declaredMapping)) {
            listener.onResponse(resolveStrictMultiFile(path, storagePath, provider, hints, hivePartitioning, config, declaredMapping));
            return;
        }

        FormatReader.SchemaResolution schemaResolution = parseSchemaResolution(config);
        boolean cacheable = isCacheable(provider);

        if (schemaResolution != FormatReader.SchemaResolution.FIRST_FILE_WINS) {
            int maxDiscoveredFiles = ExternalSourceSettings.MAX_DISCOVERED_FILES.get(settings);
            int maxGlobExpansion = ExternalSourceSettings.MAX_GLOB_EXPANSION.get(settings);
            long discoveryStartNanos = System.nanoTime();
            FileList raw = GlobExpander.expand(path, provider, hints, hivePartitioning, maxDiscoveredFiles, maxGlobExpansion);
            recordDiscovery(raw, discoveryStartNanos, storagePath.scheme());
            if (raw.fileCount() == 0) {
                throw new IllegalArgumentException("Glob pattern matched no files: " + path);
            }
            resolveMultiFileWithReconciliation(raw, config, schemaResolution, cacheable, listener);
            return;
        }

        FileList listing;
        long discoveryStartNanos = System.nanoTime();
        if (cacheable) {
            listing = cachedListing(path, storagePath, provider, hints, hivePartitioning, config);
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

        // The anchor's length/mtime are already known from the listing, so seed a ListingHint and resolve it on the
        // async footer-read path (like the fan-out) rather than a synchronous resolveSingleSource. This both skips
        // the existence/HEAD + length probe and, more importantly, avoids pinning the metadata-read executor thread
        // across the anchor footer read. Unlike the single-file getOrComputeSchema path this does not coalesce
        // concurrent misses for the same anchor key; that matches the fan-out's peek/put trade-off and is safe
        // because footer resolution is idempotent (see cachedResolveSingleSourceAsync).
        ListingHint anchorHint = new ListingHint(listing.size(anchor), anchorMtime);
        final FileList finalListing = listing;
        ActionListener<ExternalSourceMetadata> anchorListener = ActionListener.wrap(
            anchorMetadata -> completeFirstFileWins(anchorMetadata, finalListing, config, requiresStats, cacheable, listener),
            listener::onFailure
        );
        if (cacheable) {
            // cachedResolveSingleSourceAsync always completes with the ExternalSourceMetadata built by
            // buildMetadataFromCache, so the cast is safe.
            cachedResolveSingleSourceAsync(anchorPath, anchorHint, config, anchorListener.map(meta -> (ExternalSourceMetadata) meta));
        } else {
            resolveSingleSourceAsync(
                anchorPath.toString(),
                anchorHint,
                config,
                anchorListener.map(meta -> wrapAsExternalSourceMetadata(meta, config, declaredReadSpecOf(declaredMapping)))
            );
        }
    }

    /**
     * Builds the FIRST_FILE_WINS result from the resolved anchor metadata and the discovered listing. Runs as the
     * continuation of the async anchor footer read, so any synchronous failure here is funnelled to
     * {@code listener::onFailure} rather than escaping onto the executor thread.
     */
    private void completeFirstFileWins(
        ExternalSourceMetadata anchorMetadata,
        FileList listing,
        Map<String, Object> config,
        boolean requiresStats,
        boolean cacheable,
        ActionListener<ExternalSourceResolution.ResolvedSource> listener
    ) {
        try {
            final ExternalSourceMetadata base = enrichWithFileCount(anchorMetadata, listing.fileCount());
            if (listing.fileCount() > 1 && requiresStats) {
                // For multi-file FIRST_FILE_WINS, read all files' metadata during Phase 1 to aggregate statistics
                // across all files. This allows aggregate pushdown (COUNT/MIN/MAX) to use accurate global stats and
                // to skip Phase 2 (split discovery) entirely for those queries.
                //
                // This eager all-file aggregation is gated on requiresStats: only query shapes that can consume the
                // global stats — an ungrouped aggregate over the relation, detected by
                // ExternalStatsRequirementExtractor#pathsRequiringEagerStats — pay the N footer reads. Every other
                // shape (LIMIT, SELECT *, grouped STATS ... BY, INLINESTATS) takes the defer branch below: it keeps
                // STATS_FILE_COUNT (from enrichWithFileCount above) but marks stats partial, so Phase 2 split
                // discovery reads footers once instead of twice. Legacy callers pass requiresStats == true for every
                // path (the resolve overload with a null pathsRequiringStats set), preserving the original
                // eager-for-all behavior.
                //
                // For an eager (requiresStats) resolve the cost is acceptable because:
                // - the cacheable path consults the schema cache, so repeat resolves are free;
                // - the non-cacheable path reads footers with an async fan-out bounded by an in-flight permit
                // (metadataReadConcurrency), releasing the pool thread across each footer read;
                // - the aggregated stats unlock skipping Phase 2 entirely for pushable aggregates
                // (see ComputeService#canSkipSplitDiscovery), which dominates the savings.
                // implicitNulls: whether an absent per-column stat means "all rows null" (footer formats) vs
                // "not harvested" (line-oriented text). Computed once from the anchor's sourceType and threaded
                // into the async aggregation so text-format multi-file merges force a re-scan instead of serving
                // a subset COUNT/MIN/MAX (see foldsAbsentColumnAsImplicitNull / SourceStatisticsSerializer).
                boolean implicitNulls = foldsAbsentColumnAsImplicitNull(base.sourceType());
                // Prefetch the dataset-level aggregate BEFORE the per-file stats gather — see
                // applyDatasetAggregate for why post-gather reads self-defeat under cache pressure.
                DatasetAggregatePrefetch datasetPrefetch = prefetchDatasetAggregate(listing, config, cacheable);
                ActionListener<Map<String, Object>> statsListener = ActionListener.wrap(aggregatedStats -> {
                    try {
                        Map<String, Object> effective = applyDatasetAggregate(datasetPrefetch, aggregatedStats, listing, base, config);
                        listener.onResponse(finishFirstFileWins(listing, applyFirstFileWinsAggregatedStats(base, effective)));
                    } catch (Exception e) {
                        listener.onFailure(e);
                    }
                }, listener::onFailure);
                if (cacheable) {
                    readAndAggregateAllFileStatsWithCache(listing, config, implicitNulls, statsListener);
                } else {
                    readAndAggregateAllFileStats(listing, config, implicitNulls, statsListener);
                }
            } else if (listing.fileCount() > 1) {
                // Defer branch (requiresStats == false): skip the N footer reads. The anchor-only stats are not
                // representative of the whole glob, so mark them partial — exactly the state the failed-aggregation
                // path produces, which downstream already handles (SplitStats.resolveEffectiveStats returns null
                // rather than consuming anchor stats as global). STATS_FILE_COUNT, stamped above, is preserved.
                listener.onResponse(finishFirstFileWins(listing, markStatsAsPartial(base)));
            } else {
                listener.onResponse(finishFirstFileWins(listing, base));
            }
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    /**
     * Overlays globally-aggregated statistics onto the anchor metadata for the FIRST_FILE_WINS path. When
     * {@code aggregatedStats} is {@code null} (some file lacked statistics or a read failed) the stats are marked
     * partial so the optimizer does not rely on incomplete aggregations.
     */
    private static ExternalSourceMetadata applyFirstFileWinsAggregatedStats(
        ExternalSourceMetadata base,
        @Nullable Map<String, Object> aggregatedStats
    ) {
        if (aggregatedStats == null) {
            // Could not aggregate stats (some files lacked statistics) — mark as partial so the optimizer does not
            // rely on incomplete sourceMetadata stats.
            return markStatsAsPartial(base);
        }
        // Replace anchor-only stats with globally-aggregated stats. The aggregated stats are authoritative across
        // all files: a column the cross-file merge dropped (e.g. a text column harvested in only some files, under
        // implicitNulls=false) must NOT survive via the anchor file's own per-column keys. So strip every _stats.*
        // key from the anchor base before overlaying — putAll alone only overwrites keys the aggregate still has,
        // which would leak the anchor's stale per-column stats (value_count/min/max/null_count) against the GLOBAL
        // row count and serve a wrong COUNT/MIN/MAX. Non-stats keys (file_count, config) are preserved. Mirrors
        // buildUnifiedMetadata's strip.
        Map<String, Object> current = base.sourceMetadata();
        Map<String, Object> merged = new HashMap<>();
        if (current != null) {
            for (Map.Entry<String, Object> entry : current.entrySet()) {
                // Strip the anchor's stale per-file stats, but PRESERVE STATS_FILE_COUNT: it was stamped on base by
                // enrichWithFileCount before this call and is not part of aggregatedStats (which carries only row
                // count + per-column stats), so dropping it would lose the file count the eager path needs for
                // canSkipSplitDiscovery. (buildUnifiedMetadata re-stamps it after its strip; here base already has it.)
                boolean isStale = entry.getKey().startsWith(SourceStatisticsSerializer.STATS_KEY_PREFIX)
                    && entry.getKey().equals(SourceStatisticsSerializer.STATS_FILE_COUNT) == false;
                if (isStale == false) {
                    merged.put(entry.getKey(), entry.getValue());
                }
            }
        }
        merged.putAll(aggregatedStats);
        // Do NOT add STATS_PARTIAL — stats are now complete across all files.
        merged.remove(SourceStatisticsSerializer.STATS_PARTIAL);
        return replaceSourceMetadata(base, Map.copyOf(merged));
    }

    /** Returns a wrapper that delegates to {@code base} but replaces {@code sourceMetadata()} with {@code replacement}. */
    private static ExternalSourceMetadata replaceSourceMetadata(ExternalSourceMetadata base, Map<String, Object> replacement) {
        return new ExternalSourceMetadata() {
            @Override
            public String location() {
                return base.location();
            }

            @Override
            public List<Attribute> schema() {
                return base.schema();
            }

            @Override
            public String sourceType() {
                return base.sourceType();
            }

            @Override
            public Map<String, Object> sourceMetadata() {
                return replacement;
            }

            @Override
            public Map<String, Object> config() {
                return base.config();
            }
        };
    }

    /**
     * Completes the FIRST_FILE_WINS path once per-file stats (if any) have been folded into {@code extMetadata}:
     * applies Hive partition-key shadowing (the partition value wins over a same-named physical column), enriches the
     * coordinator schema with partition columns, and pins the anchor's physical schema for every file via an
     * (identity or narrowing) per-file mapping. Purely CPU-bound.
     */
    private ExternalSourceResolution.ResolvedSource finishFirstFileWins(FileList listing, ExternalSourceMetadata extMetadata) {
        // The anchor's pre-enrichment schema is the physical read schema every file's reader parses. Partition
        // columns are path-derived (injected by VirtualColumnIterator at read time), so they are never part of the
        // physical read schema; the data-only view below drives the mapping output width.
        List<Attribute> physicalSchema = extMetadata.schema();
        List<Attribute> dataOnlySchema = physicalSchema;

        PartitionMetadata partitionMetadata = listing.partitionMetadata();
        if (partitionMetadata != null && partitionMetadata.isEmpty() == false) {
            // Shadow same-named physical columns: when a physical column collides with a partition key, the partition
            // (path-derived) value wins (Spark/DuckDB semantics). The reader still parses the physical column (CSV is
            // positional), but the per-file mapping drops it from the output so the mapping width agrees with the
            // data-only unified schema at ColumnMapping#pruneToPerFileQuery and with queryDataSchema at the
            // SchemaAdaptingIterator guard. enrichSchemaWithPartitionColumns appends the partition column and warns.
            dataOnlySchema = ExternalSchema.dataAttributesOf(physicalSchema, partitionMetadata.partitionColumns().keySet()).attributes();
            extMetadata = enrichSchemaWithPartitionColumns(extMetadata, partitionMetadata, pendingShadowWarnings::add);
        }

        // _file.* columns are request-driven now; no auto-attach to the schema. See
        // ResolveExternalRelations / the EXTERNAL shim.

        // FFW: every file's readSchema is the anchor's physical schema; the mapping is identity unless a partition
        // key shadows a physical column, in which case it narrows the output to the data-only columns (the shadowed
        // physical column is parsed but not emitted).
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
     * Looks up, or computes and caches, the compacted listing for a cacheable provider. The cache-key build and the
     * compute lambda are kept together on purpose: the discriminator folded into the key must describe exactly the
     * {@code (path, hints, hivePartitioning)} the lambda expands, or a filtered query's narrowed listing can be
     * served to a later unfiltered one. Every cacheable resolution rail routes through here so that pairing lives in
     * one place. See {@link ListingCacheKey}.
     */
    private FileList cachedListing(
        String path,
        StoragePath storagePath,
        StorageProvider provider,
        @Nullable List<PartitionFilterHintExtractor.PartitionFilterHint> hints,
        boolean hivePartitioning,
        Map<String, Object> config
    ) throws Exception {
        ListingCacheKey listingKey = ListingCacheKey.build(
            storagePath.scheme(),
            storagePath.host(),
            storagePath.path(),
            config,
            GlobExpander.listingCacheDiscriminator(path, hints, hivePartitioning)
        );
        return cacheService.getOrComputeListing(listingKey, k -> expandAndCompact(path, provider, hints, hivePartitioning, storagePath));
    }

    /**
     * Returns {@code true} when the schema cache can be consulted for the given provider.
     * Providers that do not support stable metadata (e.g. HTTP) are excluded because
     * mtime-based cache invalidation is not reliable for them.
     */
    private boolean isCacheable(StorageProvider provider) {
        return cacheService != null && cacheService.isEnabled() && provider.supportsStableMetadata();
    }

    /**
     * The single file's {@link FileMetadata} ({@code {length, mtime}}), shared by both single-file rails (inferred
     * {@link #resolveSingleFileSource} and strict {@link #resolveStrictSingleFile}). A cacheable provider serves it
     * from the file-metadata cache within the schema TTL, so a warm resolve is zero-I/O; a miss — or a non-cacheable
     * provider — probes the live object exactly once via {@link #probeFileMetadata(StoragePath, StorageProvider)}. The
     * mtime is the version token that rebuilds the {@link SchemaCacheKey}; length + mtime rebuild the singleton
     * {@code StorageEntry}.
     */
    private FileMetadata fileMetadataOf(StoragePath storagePath, StorageProvider provider, Map<String, Object> config) throws Exception {
        if (isCacheable(provider)) {
            FileMetadataCacheKey metaKey = FileMetadataCacheKey.build(storagePath.toString(), config);
            return cacheService.getOrComputeFileMetadata(metaKey, k -> probeFileMetadata(storagePath, provider));
        }
        return probeFileMetadata(storagePath, provider);
    }

    /**
     * One live object probe: a cheap HEAD/stat that on S3 is a single {@code bytes=-1} GET serving both length and
     * mtime. Null mtime (e.g. gRPC/Flight, GCS/Azure fixtures) falls back to EPOCH so the derived cache key is stable;
     * providers that never report a trustworthy mtime should return {@code supportsStableMetadata() == false} to bypass
     * caching entirely.
     */
    private static FileMetadata probeFileMetadata(StoragePath storagePath, StorageProvider provider) throws Exception {
        StorageObject probe = provider.newObject(storagePath);
        Instant lastMod = probe.lastModified();
        long mtime = lastMod != null ? lastMod.toEpochMilli() : Instant.EPOCH.toEpochMilli();
        return new FileMetadata(probe.length(), mtime);
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

    /**
     * Returns {@code safeMetadata} without the coordinator-cache stripe bookkeeping ({@code _stats.stripe.<k>},
     * {@code _stats.stripe_last_index}, {@code _stats.stripe_grid}) — those feed the cache-side 0..K fold and are
     * never read from the plan, but would otherwise ride the plan wire per fragment. Returns the input unchanged
     * when it carries no stripe keys (no copy on the common already-folded warm-serve path).
     */
    private static Map<String, Object> stripStripeBookkeeping(Map<String, Object> safeMetadata) {
        return withoutKeys(safeMetadata, ExternalStats::isStripeBookkeeping);
    }

    /**
     * Returns {@code map} without the entries whose key matches {@code drop}, or the input unchanged when nothing
     * matches (no allocation on the common no-match path). Shared by the source-metadata filters that must not mutate a
     * possibly-shared cache map ({@link #stripStripeBookkeeping}, {@link #rowCountOnlyStats}).
     */
    private static Map<String, Object> withoutKeys(Map<String, Object> map, Predicate<String> drop) {
        if (map == null || map.isEmpty()) {
            return map;
        }
        boolean anyMatch = false;
        for (String k : map.keySet()) {
            if (drop.test(k)) {
                anyMatch = true;
                break;
            }
        }
        if (anyMatch == false) {
            return map;
        }
        Map<String, Object> filtered = new HashMap<>(map.size());
        for (Map.Entry<String, Object> e : map.entrySet()) {
            if (drop.test(e.getKey()) == false) {
                filtered.put(e.getKey(), e.getValue());
            }
        }
        return filtered;
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
        // reads the whole-file _stats.* keys (row count, per-column min/max/null/value-count, mtime,
        // fingerprint) straight off this map; no separate cache lookup. But safeMetadata ALSO carries the
        // coordinator-cache stripe bookkeeping (_stats.stripe.<k> committed stripes, _stats.stripe_last_index,
        // _stats.stripe_grid), which only ExternalSourceCacheService's 0..K fold reads — it has no plan-side
        // consumer, yet this map rides ExternalRelation.writeTo onto the wire in every fragment of every query.
        // Strip it here so the plan carries only what the optimizer actually reads.
        final Map<String, Object> finalMetadata = stripStripeBookkeeping(entry.safeMetadata());

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

    /**
     * The dataset-level aggregate key for one multi-file resolve, or {@code null} when the shape does not
     * qualify: single file, no file-set fingerprint, a non-cacheable provider (handled by callers), or — the
     * format gate — a format that folds an absent column stat as implicit nulls (Parquet/ORC). The
     * aggregate is ROW-COUNT-ONLY, and under the footer implicit-nulls contract an absent per-column
     * stat reads as "all null", so serving the aggregate to a footer-format {@code COUNT(col)} would
     * fold {@code rowCount - rowCount = 0} — a wrong answer. Text formats safe-miss absent columns
     * instead, so only they may carry the aggregate; a {@code null} key disables put, serve, and
     * promise registration together. (Precedent: {@code strictSingleFileMetadata} refuses
     * {@code FILE_TYPED_FORMATS} on its warm rail for the same reason family.)
     * <p>
     * Keyed on the listing's file-set fingerprint (see {@link SchemaCacheKey#forDatasetAggregate}), so it
     * needs no invalidation: any add/remove/mtime/size change in the set derives a different key. The
     * {@code formatType} slot uses the same extension-based detection the per-file keys use — a stable
     * identity input; the logical source type may only diverge from it via config keys that are already
     * part of the key's config fingerprint, with one benign exception: the {@code reader} override is
     * absent from the fingerprint but can only select footer-format (Parquet-family) readers, which the
     * format gate above refuses — so no dataset key is ever minted for a reader-overridden resolve.
     * Package-private for testing.
     */
    @Nullable
    SchemaCacheKey datasetAggregateKey(FileList listing, Map<String, Object> config) {
        if (listing == null || listing.fileSetFingerprint() == null || listing.fileCount() < 2) {
            return null;
        }
        if (datasetAggregateSafeForFormat(listing, config) == false) {
            return null;
        }
        return SchemaCacheKey.forDatasetAggregate(
            listing.originalPattern(),
            listing.fileSetFingerprint(),
            detectFormatType(listing.path(0)),
            config
        );
    }

    /**
     * Whether the listing's format treats an ABSENT per-column stat as "not harvested" (safe-miss to a
     * re-scan) rather than "all null" — the text-format contract that makes a row-count-only aggregate
     * safe to serve. The format is resolved exactly the way the READ path resolves it
     * ({@link FormatNameResolver#resolveFormatName}: config {@code format}/{@code reader} override
     * first, then the compound-extension-aware registry lookup), so this gate can never disagree with
     * the reader that actually scans — a footer file under a {@code .ndjson} name with
     * {@code format=parquet} is gated as parquet, and compressed text ({@code .ndjson.gz}) resolves to
     * {@code ndjson} and qualifies. The {@code formatName() -> findByName} round-trip deliberately
     * unwraps {@code CompressionDelegatingFormatReader} to the inner reader, whose
     * {@code aggregatePushdownSupport} is the authoritative one (the wrapper does not forward it).
     * Any resolution failure refuses: the registry throws {@code QlIllegalArgumentException} (not
     * {@code java.lang.IllegalArgumentException}) on an unregistered extension, and the aggregate is an
     * optimization that must never turn a resolvable read into a throw — hence the broad catch.
     */
    private boolean datasetAggregateSafeForFormat(FileList listing, Map<String, Object> config) {
        try {
            String formatName = FormatNameResolver.resolveFormatName(
                config,
                listing.path(0).objectName(),
                dataSourceModule.formatReaderRegistry()
            );
            // Same predicate as foldsAbsentColumnAsImplicitNull, negated: an absent-column stat is only
            // safe to fold as an implicit null (and thus a dataset COUNT aggregate only correct) for a
            // KNOWN format that does NOT treat an absent column as implicit null. Delegate so the two
            // callers can't drift. This rail re-resolves the format from config because it prefetches
            // before any SourceMetadata (and thus sourceType) exists.
            return foldsAbsentColumnAsImplicitNull(formatName) == false;
        } catch (Exception e) {
            return false;
        }
    }

    /**
     * One multi-file resolve's dataset-aggregate prefetch: the key (or {@code null} when the shape does
     * not qualify — including the non-cacheable case, gated here so it lives in one place) and the
     * memoized aggregate if present. Read BEFORE the per-file gather; see {@link #applyDatasetAggregate}
     * for why post-gather reads self-defeat under cache pressure. Package-private for testing.
     */
    record DatasetAggregatePrefetch(@Nullable SchemaCacheKey key, @Nullable Map<String, Object> prefetched) {}

    private DatasetAggregatePrefetch prefetchDatasetAggregate(FileList listing, Map<String, Object> config, boolean cacheable) {
        SchemaCacheKey key = cacheable ? datasetAggregateKey(listing, config) : null;
        return new DatasetAggregatePrefetch(key, key != null ? cacheService.getDatasetAggregate(key) : null);
    }

    /**
     * Resolves the effective multi-file aggregate: the per-file merge when it succeeded, else the
     * memoized dataset-level aggregate. Centralizes the dataset-aggregate lifecycle both multi-file
     * rails share:
     * <ul>
     *   <li><b>Per-file merge succeeded</b> — write-through on the FIRST such merge for this file set
     *   (i.e. the prefetch missed): memoize its row count under the dataset key so the NEXT warm query
     *   survives per-file entry loss (LRU pressure) without re-merging. A later warm resolve whose
     *   prefetch already hit skips the re-put (the entry is current and was just revived).</li>
     *   <li><b>Per-file merge failed, prefetch hit</b> — serve the memoized aggregate. The prefetch was
     *   read (and TTL/LRU-revived) BEFORE the per-file gather on purpose: under cache pressure the
     *   gather's own {@code putSchema} calls can evict the dataset entry, so reading it after the gather
     *   would lose exactly the entry this fallback exists to serve. The served map is row-count-only
     *   ({@code _stats.row_count}), so only COUNT(*) warms from it — MIN/MAX keep re-scanning until the
     *   per-file rail can serve them (see {@code ExternalSourceCacheService#getDatasetAggregate}).</li>
     *   <li><b>Both missed (the cold query)</b> — register a pending-descriptor promise so the scan's
     *   reconcile materializes the aggregate the moment every file folds to whole-file completeness;
     *   the FIRST warm query is then already protected.</li>
     * </ul>
     * The descriptor's expected fingerprint is taken from the reference file's reader-stamped metadata
     * (the exact value data-node contributions will carry), falling back to the coordinator-side
     * canonical config only when the probe did not stamp one. Package-private for testing.
     */
    @Nullable
    Map<String, Object> applyDatasetAggregate(
        DatasetAggregatePrefetch prefetch,
        @Nullable Map<String, Object> aggregatedStats,
        FileList listing,
        SourceMetadata referenceMeta,
        Map<String, Object> config
    ) {
        SchemaCacheKey datasetKey = prefetch.key();
        if (datasetKey == null) {
            return aggregatedStats;
        }
        if (aggregatedStats != null) {
            // Write-through only on the FIRST successful merge for this file set — i.e. when the prefetch
            // missed. Once the aggregate is memoized under the fingerprint key (a set-identity key: same
            // files => same key => same count), repeat warm resolves needn't re-scan paths or re-put; the
            // prefetch's getDatasetAggregate already LRU/TTL-revived the entry, so skipping the put here
            // costs it no liveness. Keeps the common warm-non-evicted path off the O(N) scan + write.
            if (prefetch.prefetched() == null) {
                Object rowCount = aggregatedStats.get(SourceStatisticsSerializer.STATS_ROW_COUNT);
                // Duplicate-path guard on the write-through: a comma-separated list can name the same file
                // twice, and the reconciliation rail's per-file merge folds a per-path MAP (deduplicated)
                // while the scan reads the listing MULTISET — memoizing that merge under the fingerprint would
                // persist an undercount beyond eviction. NOTE: the per-file rail SERVING that dedup merge
                // immediately is a pre-existing main bug tracked separately (GA issue); this guard only
                // keeps the dataset aggregate from memoizing it.
                if (rowCount instanceof Number n && listingPathsAreDistinct(listing)) {
                    cacheService.putDatasetAggregate(datasetKey, n.longValue(), referenceMeta.sourceType(), listing.originalPattern());
                }
            }
            return aggregatedStats;
        }
        cacheService.recordStatsAggregateIncomplete();
        if (prefetch.prefetched() != null) {
            // Needed (per-file merge incomplete) AND present: the fallback actually served.
            cacheService.recordDatasetAggregateHit();
            return prefetch.prefetched();
        }
        // Needed AND absent: the fallback was wanted but missing. Counted symmetrically with the hit above
        // (both only on the needed path — healthy warm resolves never reach here), so hits/(hits+misses)
        // is a real fallback hit rate.
        cacheService.recordDatasetAggregateMiss();
        Map<String, Long> pathToMtime = new HashMap<>(listing.fileCount());
        for (int i = 0; i < listing.fileCount(); i++) {
            pathToMtime.put(listing.path(i).toString(), listing.lastModifiedMillis(i));
        }
        Map<String, Object> referenceMetadata = referenceMeta.sourceMetadata();
        Object stamped = referenceMetadata != null ? referenceMetadata.get(ExternalStats.CONFIG_FINGERPRINT_KEY) : null;
        String fingerprint = stamped instanceof String s ? s : SchemaCacheKey.buildFormatConfig(config);
        cacheService.registerPendingDatasetAggregate(
            datasetKey,
            pathToMtime,
            listing.fileCount(),
            fingerprint,
            referenceMeta.sourceType(),
            listing.originalPattern()
        );
        return null;
    }

    /**
     * True when no path appears twice in the listing (comma-lists may repeat a file; globs cannot). O(N),
     * resolve-time only. Same duplicate-path guard as the promise rail's {@code size != expectedFileCount}
     * check in {@code ExternalSourceCacheService#registerPendingDatasetAggregate}, encoded here as a set on
     * the write-through path so the common warm-non-evicted resolve pays the O(N) scan only when it writes,
     * not on every key-mint.
     */
    private static boolean listingPathsAreDistinct(FileList listing) {
        Set<String> unique = new HashSet<>(listing.fileCount());
        for (int i = 0; i < listing.fileCount(); i++) {
            if (unique.add(listing.path(i).toString()) == false) {
                return false;
            }
        }
        return true;
    }

    private void resolveMultiFileWithReconciliation(
        FileList fileList,
        Map<String, Object> config,
        FormatReader.SchemaResolution schemaResolution,
        boolean cacheable,
        ActionListener<ExternalSourceResolution.ResolvedSource> listener
    ) {
        long startNanos = System.nanoTime();
        DatasetAggregatePrefetch datasetPrefetch = prefetchDatasetAggregate(fileList, config, cacheable);
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
                result = shadowPartitionCollisions(result, partitionNames, pendingShadowWarnings::add);

                List<Attribute> unifiedSchema = result.unifiedSchema().attributes();
                SourceMetadata firstMeta = allMetadata.get(firstFile);
                // Aggregate from the per-file metadata already fetched by readAllFileMetadata —
                // no second cache or storage hit per file. Each file's stats are in its OWN unit/representation;
                // normalize every file's min/max to the reconciled unified type (temporal rescale, or safe-miss
                // marker for a non-normalizable representation) BEFORE folding, so the source-level warm
                // COUNT/MIN/MAX is not a unit-blind numeric mix across DATETIME(millis)/DATE_NANOS(nanos) files.
                Map<String, DataType> reconciledTypes = attributesToTypeMap(unifiedSchema);
                Map<StoragePath, Map<String, DataType>> perFileTypes = new HashMap<>();
                Map<StoragePath, Set<String>> perFilePinnedColumns = new HashMap<>();
                for (Map.Entry<StoragePath, SchemaReconciliation.FileSchemaInfo> e : result.perFileInfo().entrySet()) {
                    SchemaReconciliation.FileSchemaInfo info = e.getValue();
                    perFileTypes.put(e.getKey(), attributesToTypeMap(info.fileSchema().attributes()));
                    Set<String> pinnedColumns = pinnedColumnsOf(info);
                    if (pinnedColumns.isEmpty() == false) {
                        perFilePinnedColumns.put(e.getKey(), pinnedColumns);
                    }
                }
                // Under SKIP_ROW a narrow-read parse failure on a pinned column drops the whole row, so a pinned
                // file's cached row count is untrustworthy too; NULL_FIELD keeps the row (only the cell nulls) and
                // FAIL_FAST aborts the read cold before it can cache, so both keep the row count.
                boolean dropPinnedRowCount = resolvesToSkipRow(firstMeta.sourceType(), config);
                Map<String, Object> aggregatedStats = aggregateFileStatistics(
                    allMetadata,
                    perFileTypes,
                    reconciledTypes,
                    perFilePinnedColumns,
                    dropPinnedRowCount,
                    foldsAbsentColumnAsImplicitNull(firstMeta.sourceType())
                );
                aggregatedStats = applyDatasetAggregate(datasetPrefetch, aggregatedStats, fileList, firstMeta, config);
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
                    final ExternalSourceMetadata metaForAssert = extMetadata;
                    assert metaForAssert.schema().stream().noneMatch(a -> partitionNames.contains(a.name()))
                        : "shadowPartitionCollisions must run before enrichSchemaWithPartitionColumns: a physical "
                            + "column still collides with a partition key, which would warn twice";
                    extMetadata = enrichSchemaWithPartitionColumns(extMetadata, partitionMetadata, pendingShadowWarnings::add);
                }

                // _file.* columns are request-driven now; no auto-attach to the schema. See
                // ResolveExternalRelations / the EXTERNAL shim.

                Map<StoragePath, SchemaReconciliation.FileSchemaInfo> schemaMap = result.perFileInfo();
                listener.onResponse(new ExternalSourceResolution.ResolvedSource(extMetadata, fileList, schemaMap));
            } catch (Exception e) {
                listener.onFailure(e);
            }
        }, listener::onFailure));
    }

    /**
     * Returns a copy of {@code result} with physical columns shadowed by Hive partition keys removed
     * from the unified schema and from every per-file mapping's output, while each file's physical
     * schema is preserved so positional readers (e.g. CSV) still parse every column. The partition
     * (path-derived) value wins (Spark/DuckDB semantics); {@link #enrichSchemaWithPartitionColumns}
     * later re-adds the partition column to the coordinator schema. Emits one client-facing warning
     * per shadowed column (see {@link #warnOnShadowedColumns}) through {@code warningSink} — see that
     * method's javadoc for why a direct-to-{@code HeaderWarning} write is not safe from this call site.
     * Returns {@code result} unchanged when {@code partitionColumnNames} is empty or nothing is shadowed.
     */
    private static SchemaReconciliation.Result shadowPartitionCollisions(
        SchemaReconciliation.Result result,
        Set<String> partitionColumnNames,
        @Nullable Consumer<String> warningSink
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
        warnOnShadowedColumns(shadowedColumns, warningSink);
        // Order is irrelevant: Map.copyOf below discards insertion order and FileSplitProvider looks
        // up per-file info by key (matches reconcileUnionByName's own Map.copyOf pattern).
        Map<StoragePath, SchemaReconciliation.FileSchemaInfo> perFileInfo = Maps.newHashMapWithExpectedSize(result.perFileInfo().size());
        for (Map.Entry<StoragePath, SchemaReconciliation.FileSchemaInfo> entry : result.perFileInfo().entrySet()) {
            SchemaReconciliation.FileSchemaInfo info = entry.getValue();
            // Recompute the mapping against the data-only unified schema; the file (physical) schema is
            // unchanged so the reader still parses every column, including the shadowed one. Carry the pin's
            // inferredTypes forward so a Hive-partitioned glob keeps the pinned-column stats boundary.
            ColumnMapping mapping = SchemaReconciliation.computeMapping(dataOnlyUnified, info.fileSchema().attributes());
            perFileInfo.put(
                entry.getKey(),
                new SchemaReconciliation.FileSchemaInfo(info.fileSchema(), mapping, info.statistics(), info.inferredTypes())
            );
        }
        return new SchemaReconciliation.Result(new ExternalSchema(dataOnlyUnified), Map.copyOf(perFileInfo));
    }

    /**
     * Per-file metadata, read with an async fan-out bounded by {@link #metadataReadConcurrency} in-flight reads (see
     * {@link #gatherPerFile}). When {@code cacheable} is true each resolve peeks the schema cache (keyed on path +
     * mtime) and, on a miss, resolves asynchronously and stores the result so warm queries against the same paths hit
     * cache. The result preserves the file order of {@code fileList}.
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
     * Runs an async, bounded fan-out over every file in {@code fileList}, resolving each file's {@link SourceMetadata}
     * and returning results in file order. Concurrency is capped at {@link #metadataReadConcurrency} in-flight reads
     * via {@link ThrottledIterator}; because the per-file resolve is itself async (the footer read is released across
     * the network round-trip), that permit bounds in-flight reads rather than pinning that many executor threads. The
     * cancellation signal is checked before each dispatch (a cancelled wide glob stops issuing reads promptly and
     * surfaces {@link TaskCancelledException}), and the async reads run on {@link #metadataReadExecutor} so an
     * executor-backed synchronous read's backoff aborts on cancel. The first failure is propagated to {@code listener}
     * and short-circuits the remaining files.
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
                // A previous file already failed (or the query was cancelled) — drain the remaining items
                // without issuing reads.
                releasable.close();
                return;
            }
            ActionListener<SourceMetadata> itemListener = ActionListener.runAfter(
                ActionListener.wrap(meta -> results.set(i, meta), e -> failure.compareAndSet(null, e)),
                releasable::close
            );
            // ThrottledIterator's itemConsumer must not throw: an escaped exception would leave this item's ref
            // permanently held (its releasable never closed) and onCompletion would never fire — a hang. A check-
            // before-dispatch cancellation and any synchronous throw from the resolve dispatch (e.g. a factory that
            // rejects the executor submission or throws before completing the listener) are therefore funnelled into
            // itemListener.onFailure so runAfter(..., releasable::close) always runs.
            try {
                throwIfCancelled();
                StoragePath filePath = fileList.path(i);
                // Length + mtime come from the directory listing: thread them through so the factory can build the
                // storage object without a synchronous existence/HEAD probe on the executor thread before the async
                // footer read.
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
            // A continuation was rejected/failed (e.g. executor shutdown): record it so onCompletion surfaces the
            // failure rather than returning a partially-populated result.
            failure.compareAndSet(null, e);
        });
    }

    /**
     * Cache-aware async single-file resolve for the multi-file fan-out. Peeks the schema cache and, on a miss,
     * resolves asynchronously (without pinning a thread across the footer read) and stores the result. Unlike the
     * single-file {@code getOrComputeSchema} path this does not coalesce concurrent misses for the same key: two
     * concurrent misses may both fetch. That is acceptable here because each fan-out file is a distinct key and
     * footer resolution is idempotent; see {@link ExternalSourceCacheService#getSchemaIfPresent}.
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
     * Reconciliation-path aggregate: normalizes each file's per-column min/max to the reconciled unified type
     * ({@link SourceStatisticsSerializer#normalizeStatsToReconciled}) BEFORE the cross-file fold, so a column that
     * mixes units/representations across files (DATETIME epoch-millis vs DATE_NANOS epoch-nanos; numeric vs the
     * KEYWORD non-widenable fallback) is folded in ONE type and served result-identical to a full scan — or
     * safe-misses when a value cannot be normalized. {@code perFileTypes} maps each file's path to its own column
     * types; {@code reconciledTypes} is the unified schema's types. Without this, the source-level warm
     * MIN/MAX/COUNT would compare raw file-local values unit-blind (a wrong answer).
     * <p>
     * {@code perFilePinnedColumns} names, per file, the columns a text-format UNION_BY_NAME pin retyped above their
     * inferred type (see {@link SchemaReconciliation#reconcileUnionByName}). Their cached per-file stats were harvested
     * at the narrower read type, but the schema cache identity is read-schema-blind, so a stat harvested by a solo
     * narrow read is shared with this widened read. That stat is not merely a wrong unit but a wrong value (the narrow
     * read null-filled or row-dropped the out-of-sample cell that the wide read keeps), which normalization cannot
     * rescue. So each pinned column is safe-missed via {@link SourceStatisticsSerializer#overlayPinnedColumnsOnStats}:
     * its extrema are poisoned and its value/null counts dropped, and when {@code dropPinnedRowCount} (SKIP_ROW, where
     * a narrow-read parse failure dropped the whole row) the file's row count is dropped too, forcing a full re-scan.
     */
    @Nullable
    static Map<String, Object> aggregateFileStatistics(
        Map<StoragePath, SourceMetadata> allMetadata,
        Map<StoragePath, Map<String, DataType>> perFileTypes,
        Map<String, DataType> reconciledTypes,
        Map<StoragePath, Set<String>> perFilePinnedColumns,
        boolean dropPinnedRowCount,
        boolean implicitNullsForAbsentColumn
    ) {
        List<Map<String, Object>> perFileFlatStats = new ArrayList<>(allMetadata.size());
        for (Map.Entry<StoragePath, SourceMetadata> entry : allMetadata.entrySet()) {
            Map<String, Object> flat = flatStatsOf(entry.getValue());
            if (flat == null) {
                // At least one file has no statistics — cannot produce accurate global stats. Name the
                // first offender: an all-or-nothing miss over hundreds of files is otherwise
                // undiagnosable (the 20-minute warm re-scan with no trace of WHICH file broke it).
                LOGGER.debug("multi-file stats aggregate incomplete: [{}] has no statistics", entry.getKey());
                return null;
            }
            Map<String, DataType> fileTypes = perFileTypes.get(entry.getKey());
            if (fileTypes != null) {
                flat = SourceStatisticsSerializer.normalizeStatsToReconciled(flat, fileTypes, reconciledTypes);
            }
            Set<String> pinnedColumns = perFilePinnedColumns.get(entry.getKey());
            if (pinnedColumns != null && pinnedColumns.isEmpty() == false) {
                flat = SourceStatisticsSerializer.overlayPinnedColumnsOnStats(flat, pinnedColumns, dropPinnedRowCount);
            }
            perFileFlatStats.add(flat);
        }
        return SourceStatisticsSerializer.mergeStatistics(perFileFlatStats, implicitNullsForAbsentColumn);
    }

    /**
     * FFW path. FIRST_FILE_WINS reads every file with the anchor's schema and assumes the others match, but does
     * NOT enforce it. A column whose physical type DIVERGES across files (e.g. DATETIME/epoch-millis in the anchor
     * and DATE_NANOS/epoch-nanos in another) would fold its extrema unit-blind here; worse, the divergent file's
     * data is itself misread under the anchor schema, so no warm extremum can match a scan. We cannot normalize to
     * a common unit (the cold path is already wrong), so we POISON such columns' extrema — safe-miss to a scan.
     */
    @Nullable
    static Map<String, Object> aggregateFileStatistics(Collection<SourceMetadata> allMetadata, boolean implicitNullsForAbsentColumn) {
        List<Map<String, Object>> perFileFlatStats = new ArrayList<>(allMetadata.size());
        Map<String, DataType> anchorTypes = null;
        Set<String> divergentColumns = new HashSet<>();
        for (SourceMetadata meta : allMetadata) {
            Map<String, Object> flat = flatStatsOf(meta);
            if (flat == null) {
                LOGGER.debug("multi-file stats aggregate incomplete: [{}] has no statistics", meta.location());
                return null;
            }
            perFileFlatStats.add(flat);
            Map<String, DataType> fileTypes = attributesToTypeMap(meta.schema());
            if (anchorTypes == null) {
                anchorTypes = fileTypes;
            } else {
                for (Map.Entry<String, DataType> entry : fileTypes.entrySet()) {
                    DataType anchorType = anchorTypes.get(entry.getKey());
                    if (anchorType != null && anchorType != entry.getValue()) {
                        divergentColumns.add(entry.getKey());
                    }
                }
            }
        }
        Map<String, Object> merged = SourceStatisticsSerializer.mergeStatistics(perFileFlatStats, implicitNullsForAbsentColumn);
        if (merged != null && divergentColumns.isEmpty() == false) {
            merged = new HashMap<>(merged); // mergeStatistics may hand back an unmodifiable/shared map
            for (String column : divergentColumns) {
                SourceStatisticsSerializer.poisonColumnExtrema(merged, column);
            }
        }
        return merged;
    }

    /** A file's flat stat map — cached in sourceMetadata(), or embedded from typed statistics() — or null if absent. */
    private static Map<String, Object> flatStatsOf(SourceMetadata meta) {
        Map<String, Object> base = meta.sourceMetadata();
        if (base != null && base.containsKey(SourceStatisticsSerializer.STATS_ROW_COUNT)) {
            return base;
        }
        if (meta.statistics().isPresent()) {
            return SourceStatisticsSerializer.embedStatistics(base, meta.statistics().get());
        }
        return null;
    }

    private static Map<String, DataType> attributesToTypeMap(List<Attribute> attributes) {
        Map<String, DataType> types = new HashMap<>(attributes.size());
        for (Attribute a : attributes) {
            types.put(a.name(), a.dataType());
        }
        return types;
    }

    /**
     * The columns a UNION_BY_NAME pin retyped above their inferred type for this file, i.e. the columns whose read-time
     * type differs from the type their cached stats were harvested at. Derived as {@code inferredTypes != fileSchema}:
     * {@link SchemaReconciliation.FileSchemaInfo#inferredTypes()} snapshots the pre-pin types and is populated only when
     * the pin actually retyped a column, so a null (nothing retyped) or type-equal entry yields the empty set.
     */
    public static Set<String> pinnedColumnsOf(SchemaReconciliation.FileSchemaInfo info) {
        Map<String, DataType> inferred = info.inferredTypes();
        if (inferred == null) {
            return Set.of();
        }
        Map<String, DataType> fileTypes = attributesToTypeMap(info.fileSchema().attributes());
        Set<String> pinned = new HashSet<>();
        for (Map.Entry<String, DataType> e : inferred.entrySet()) {
            DataType fileType = fileTypes.get(e.getKey());
            if (fileType != null && fileType != e.getValue()) {
                pinned.add(e.getKey());
            }
        }
        return pinned;
    }

    /**
     * Whether the effective {@link ErrorPolicy} for {@code sourceType} under {@code config} resolves to
     * {@link ErrorPolicy.Mode#SKIP_ROW}, in which a narrow-read parse failure on a pinned column drops the whole row and
     * so makes the file's cached row count untrustworthy. Resolved through {@link ErrorPolicy#fromConfig} against the
     * reader's own default (mirrors {@link #warmsRowCountSafely}) so it is format-agnostic and catches the implicit
     * SKIP_ROW that a bare {@code max_errors} selects. An invalid policy conservatively drops the row count: it must not
     * fail resolution (the data node rejects it at scan time), and dropping only forces a safe re-scan.
     */
    public boolean resolvesToSkipRow(String sourceType, Map<String, Object> config) {
        FormatReader reader = dataSourceModule.formatReaderRegistry().findByName(sourceType);
        ErrorPolicy defaultPolicy = reader != null ? reader.defaultErrorPolicy() : ErrorPolicy.STRICT;
        try {
            return ErrorPolicy.fromConfig(config, defaultPolicy).mode() == ErrorPolicy.Mode.SKIP_ROW;
        } catch (IllegalArgumentException e) {
            return true;
        }
    }

    /**
     * Whether {@code sourceType}'s format folds an absent column into implicit nulls when merging
     * per-file statistics. Footer formats (Parquet/ORC) do: a file physically lacking a column
     * contributes it as all-null. Text formats (CSV/TSV/NDJSON) do not — a column absent from a
     * file's harvested stats may still be physically present but unharvested, so the cross-file merge
     * must drop it and force a re-scan rather than serve a subset COUNT/MIN/MAX. Unknown formats keep
     * the footer default (no behavior change).
     */
    private boolean foldsAbsentColumnAsImplicitNull(String sourceType) {
        FormatReader reader = dataSourceModule.formatReaderRegistry().findByName(sourceType);
        return reader == null || reader.aggregatePushdownSupport().appliesImplicitNullsForAbsentColumn();
    }

    /**
     * Reads metadata from all files in {@code listing} with an async, bounded fan-out (see {@link #gatherPerFile}),
     * then aggregates statistics across all files. Responds with a merged flat stats map, or {@code null} if any file
     * lacks statistics (via {@link #aggregateFileStatistics}). A read failure is treated as "could not aggregate" and
     * responds with {@code null} so the caller marks stats partial — except cancellation, which is surfaced as a
     * failure so the query aborts promptly instead of silently degrading.
     */
    private void readAndAggregateAllFileStats(
        FileList listing,
        Map<String, Object> config,
        boolean implicitNulls,
        ActionListener<Map<String, Object>> listener
    ) {
        gatherPerFile(listing, config, false, ActionListener.wrap(allMeta -> {
            listener.onResponse(aggregateFileStatistics(allMeta, implicitNulls));
        }, e -> {
            // Cancellation is not a "could not aggregate stats" condition — propagate it so the query aborts promptly
            // instead of silently degrading to partial stats and continuing. A read that failed *because* the query
            // was cancelled mid-flight can arrive wrapped (the schema cache wraps loader failures), so consult the
            // cancellation state directly rather than matching only on the exception type.
            if (e instanceof TaskCancelledException) {
                listener.onFailure(e);
                return;
            }
            if (isCancelled()) {
                listener.onFailure(new TaskCancelledException(RESOLUTION_CANCELLED_MESSAGE));
                return;
            }
            LOGGER.debug(() -> "Failed to read per-file stats in parallel, will use partial stats: " + e.getMessage());
            listener.onResponse(null);
        }));
    }

    /**
     * Cache-aware variant of {@link #readAndAggregateAllFileStats}. Peeks the schema cache (keyed by path + mtime) for
     * each file so repeated multi-file resolves do not re-read footers. Responds with {@code null} if any file cannot
     * be resolved or lacks statistics; a bare cancellation is surfaced as a failure so it is never masked as partial
     * stats.
     */
    private void readAndAggregateAllFileStatsWithCache(
        FileList listing,
        Map<String, Object> config,
        boolean implicitNulls,
        ActionListener<Map<String, Object>> listener
    ) {
        gatherPerFile(listing, config, true, ActionListener.wrap(allMeta -> {
            List<Map<String, Object>> perFileStats = new ArrayList<>(allMeta.size());
            for (SourceMetadata meta : allMeta) {
                Map<String, Object> fileMeta = meta.sourceMetadata();
                if (fileMeta == null || fileMeta.containsKey(SourceStatisticsSerializer.STATS_ROW_COUNT) == false) {
                    // This file has no statistics — cannot produce accurate global stats. Name the first
                    // offender; the all-or-nothing miss is otherwise undiagnosable at glob scale.
                    LOGGER.debug("multi-file stats aggregate incomplete: [{}] has no row count", meta.location());
                    listener.onResponse(null);
                    return;
                }
                perFileStats.add(fileMeta);
            }
            listener.onResponse(SourceStatisticsSerializer.mergeStatistics(perFileStats, implicitNulls));
        }, e -> {
            // A bare cancellation, or a read that failed because the query was cancelled mid-flight (the cache wraps
            // loader failures, so consult the state directly), must abort rather than degrade to partial stats.
            if (e instanceof TaskCancelledException) {
                listener.onFailure(e);
                return;
            }
            if (isCancelled()) {
                listener.onFailure(new TaskCancelledException(RESOLUTION_CANCELLED_MESSAGE));
                return;
            }
            LOGGER.debug(() -> "Failed to get cached stats, will use partial stats: " + e.getMessage());
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
            // Aggregated stats already contain all the _stats.* keys merged across all files and are
            // authoritative: a column the cross-file merge dropped (e.g. a text column harvested in
            // only some files) must NOT survive via the anchor file's own per-column keys. So strip
            // every _stats.* key from the anchor base before overlaying — overlaying alone would leak
            // the anchor's stale columns, since putAll only overwrites keys the aggregate still has.
            Map<String, Object> base = referenceMeta.sourceMetadata();
            Map<String, Object> merged = new HashMap<>();
            if (base != null) {
                for (Map.Entry<String, Object> entry : base.entrySet()) {
                    if (entry.getKey().startsWith(SourceStatisticsSerializer.STATS_KEY_PREFIX) == false) {
                        merged.put(entry.getKey(), entry.getValue());
                    }
                }
            }
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
                // Validate outside the try block so a user config error (unknown key) propagates
                // immediately rather than being swallowed as a factory failure and retried against
                // the next factory in the registry.
                factory.validateConfig(path, config);
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
     * Async counterpart of {@link #resolveSingleSource}. Performs the cheap scheme validation synchronously (no I/O),
     * then dispatches to the first factory that claims the path via {@link #resolveWithFactory}, falling through to
     * the next candidate on failure exactly like the synchronous path. The async factory read runs on
     * {@link #metadataReadExecutor} so the read's retry backoff aborts on cancellation.
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
     * Tries each claiming factory in order, asynchronously. On a factory failure (sync throw from dispatch or async
     * {@code onFailure}) it records the failure and advances to the next candidate, mirroring the synchronous
     * fall-through in {@link #resolveSingleSource}. When no candidate remains it fails with the last recorded error,
     * or a "no handler" error if none claimed the path.
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
        if (index >= candidates.size()) {
            if (lastFailure != null) {
                listener.onFailure(new IllegalArgumentException("Failed to resolve metadata for [" + path + "]", lastFailure));
                return;
            }
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
            return;
        }
        ExternalSourceFactory factory = candidates.get(index);
        ActionListener<SourceMetadata> next = ActionListener.wrap(listener::onResponse, e -> {
            LOGGER.debug("Factory [{}] claimed path [{}] but failed: {}", factory.type(), path, e.getMessage());
            resolveWithFactory(path, hint, config, candidates, index + 1, e, listener);
        });
        try {
            factory.resolveMetadataAsync(path, hint, config, metadataReadExecutor, next);
        } catch (Exception e) {
            // A factory that throws synchronously from dispatch (before invoking the listener) must not abort the
            // whole resolve: fall through to the next candidate exactly as the async onFailure path does.
            next.onFailure(e);
        }
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
        return enrichSchemaWithPartitionColumns(metadata, partitionMetadata, null);
    }

    /**
     * Like {@link #enrichSchemaWithPartitionColumns(ExternalSourceMetadata, PartitionMetadata)}, but routes any
     * shadowed-column warning through {@code warningSink} instead of writing to {@link HeaderWarning} directly —
     * see {@link #warnOnShadowedColumns} for why that matters for callers running inside the async resolution chain.
     */
    static ExternalSourceMetadata enrichSchemaWithPartitionColumns(
        ExternalSourceMetadata metadata,
        PartitionMetadata partitionMetadata,
        @Nullable Consumer<String> warningSink
    ) {
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

        warnOnShadowedColumns(shadowedColumns, warningSink);

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

        ExternalSourceMetadata schemaEnriched = withSchema(metadata, List.copyOf(enrichedSchema));
        if (partitionNames.isEmpty()) {
            return schemaEnriched;
        }
        // Stamp the partition column names into the serialized sourceMetadata. The fileList that carries
        // PartitionMetadata is coordinator-only (ExternalRelation deserializes it to UNRESOLVED), so the data-node
        // fold reads this key instead to safe-miss COUNT(partition_col). Read by
        // SourceStatisticsSerializer.partitionColumnNames (via the partitionColumnNames() accessors on
        // ExternalSourceExec / ExternalRelation, which every node-agnostic consumer goes through).
        Map<String, Object> stampedMetadata = new HashMap<>(schemaEnriched.sourceMetadata());
        stampedMetadata.put(SourceStatisticsSerializer.PARTITION_COLUMNS_KEY, List.copyOf(partitionNames));
        return replaceSourceMetadata(schemaEnriched, Map.copyOf(stampedMetadata));
    }

    /**
     * Emits one client-facing response-header WARN per physical column that a same-named Hive
     * partition key shadows. Shadowing follows Spark (SPARK-27356) and DuckDB: the partition
     * (path-derived) value wins and the physical column is hidden. The warning lets clients notice
     * silent data substitution and points at the {@code hive_partitioning: false} escape hatch.
     * <p>
     * Delegates to {@link SkipWarnings}, which emits the summary once on the first detail. Every
     * caller reachable from {@link #resolve}'s async schema-resolution chain (which runs on
     * {@link #metadataReadExecutor}, not the originating request thread) MUST pass a non-null
     * {@code warningSink} — e.g. {@code pendingShadowWarnings::add} — so the message is buffered and
     * replayed via {@link org.elasticsearch.common.logging.HeaderWarning} once back on a thread whose
     * {@code ThreadContext} response headers actually feed the client response (see
     * {@link #pendingShadowWarnings} and the flush in {@link #resolve}). A direct-to-{@code HeaderWarning}
     * write (passing {@code null}) is only safe for callers that are themselves already on such a
     * thread, e.g. tests exercising this method directly on the test thread. A no-op when nothing is
     * shadowed.
     */
    private static void warnOnShadowedColumns(List<String> shadowedColumns, @Nullable Consumer<String> warningSink) {
        if (shadowedColumns.isEmpty()) {
            return;
        }
        SkipWarnings warnings = new SkipWarnings(
            "one or more physical columns are shadowed by same-named Hive partition keys; "
                + "the partition (path-derived) value is used. Set hive_partitioning to false to read the physical column instead.",
            warningSink
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

    private static boolean isStrict(@Nullable DatasetMapping declaredMapping) {
        return declaredMapping != null
            && declaredMapping.mappings() != null
            && declaredMapping.mappings().dynamic() == DatasetMapping.Dynamic.FALSE;
    }

    /**
     * The typed read-instructions a declared mapping produces for the data node: the logical&rarr;physical column
     * renames of a {@code path} move, the declared {@code _id.path}, and per-column date parse-patterns (keyed by
     * logical column name). {@link DeclaredReadSpec#NONE} when there is no mapping or it declares none of these. Built
     * once per path in {@link #resolve} and carried on the {@code ResolvedSource}.
     */
    private static DeclaredReadSpec declaredReadSpecOf(@Nullable DatasetMapping declaredMapping) {
        Map<String, String> renames = DeclaredSchemaResolver.renameMap(declaredMapping);
        DatasetMapping.Mappings mappings = declaredMapping == null ? null : declaredMapping.mappings();
        String idPath = mappings == null ? null : mappings.idPath();
        Map<String, String> dateFormats = Map.of();
        // Every mapped field carries an explicit declared type (DatasetFieldMapping requires it), so the mapping's
        // logical column names ARE the declared-type columns — the ones licensed to coerce (incl. narrow) toward their
        // target at read time. Keyed by LOGICAL name; FileSourceFactory physicalizes them via renames.
        Set<String> declaredTypeColumns = mappings == null ? Set.of() : Set.copyOf(mappings.properties().keySet());
        if (mappings != null) {
            Map<String, String> collected = new HashMap<>();
            for (Map.Entry<String, DatasetFieldMapping> e : mappings.properties().entrySet()) {
                String format = e.getValue().format();
                if (format != null) {
                    // Keyed by the LOGICAL column name; FileSourceFactory physicalizes the keys via renames.
                    collected.put(e.getKey(), format);
                }
            }
            dateFormats = collected;
        }
        return DeclaredReadSpec.of(renames, idPath, dateFormats, declaredTypeColumns);
    }

    /**
     * Strict single-file resolution: the declared mapping is the entire schema, so text formats need no inference — the
     * declaration is content-independent and only the file's size/mtime is read (for split planning, the same data-read
     * requirement the inferred path has). Columnar formats are the one exception:
     * {@link #rejectStrictColumnarUncoercibleTypes} reads the footer (cached) to validate the declared types against
     * the file's own — a coercible pair (e.g. {@code int64} declared {@code datetime}) is coerced by the reader at
     * decode time, an uncoercible one fails loud rather than yielding silent nulls. Both the user-facing output and the
     * per-file schema carry the declared <b>logical</b> names (identity column mapping); a {@code path} rename is
     * applied to physical only at
     * the reader boundary via {@link PhysicalNames}, so the operator and reconciliation stay in logical space.
     */
    private ExternalSourceResolution.ResolvedSource resolveStrictSingleFile(
        String path,
        StoragePath storagePath,
        StorageProvider provider,
        Map<String, Object> config,
        DatasetMapping declaredMapping
    ) throws Exception {
        // Same warm-probe amortization as the inferred single-file rail (resolveSingleFileSource): a cacheable
        // provider serves {length, mtime} from the file-metadata cache within the schema TTL, so a warm strict
        // resolve never probes the live object; a miss (or a non-cacheable provider) probes exactly once. Strict
        // resolution reads no file body, so length + mtime are the only per-query object metadata it needs.
        FileMetadata meta = fileMetadataOf(storagePath, provider, config);
        // Declared mapping is the whole schema, in LOGICAL names; a `path` rename is applied at the reader, so the
        // operator (and file schema) work purely in logical names.
        List<Attribute> logicalSchema = DeclaredSchemaResolver.declaredAttributes(declaredMapping);
        // sourceType drives operator-factory dispatch (OperatorFactoryRegistry keys on it), so it must equal the
        // reader's formatName() the inferred path would have produced — derive it without reading the file via
        // FormatNameResolver.resolveFormatName, which routes through the registry (reader-then-format-then-extension
        // precedence, and compound-extension aware, so hits.csv.gz -> "csv" not the "gz" codec suffix). A hand-rolled
        // `format` check or the last-dot FormatNameResolver.resolve here would mis-key the dispatch on compressed text.
        String sourceType = FormatNameResolver.resolveFormatName(config, storagePath.objectName(), dataSourceModule.formatReaderRegistry());
        // Cheap no-I/O guard first (no partitions on a single file), then the columnar coercibility check which reads
        // this file's footer (cached when the provider is).
        rejectDeclaredMappingViolations(null, declaredMapping);
        long mtimeMillis = meta.mtimeMillis();
        rejectStrictColumnarUncoercibleTypes(sourceType, provider, storagePath, mtimeMillis, config, declaredMapping);
        ExternalSourceMetadata extMetadata = strictSingleFileMetadata(
            path,
            storagePath,
            provider,
            config,
            declaredMapping,
            logicalSchema,
            sourceType,
            mtimeMillis
        );
        FileList singletonList = GlobExpander.fileListOf(
            List.of(new StorageEntry(storagePath, meta.length(), Instant.ofEpochMilli(meta.mtimeMillis()))),
            path
        );
        Map<StoragePath, SchemaReconciliation.FileSchemaInfo> schemaMap = singleEntrySchemaMap(storagePath, logicalSchema);
        return new ExternalSourceResolution.ResolvedSource(extMetadata, singletonList, schemaMap);
    }

    /**
     * Marker appended to the strict single-file schema-cache key's format-type component so a strict (declared-schema)
     * entry never shares a {@code getOrComputeSchema} slot with an inferred (discovered-schema) entry over the same
     * file+config: the two hold different schemas and neither may be served the other's. This disambiguates ONLY the
     * schema-cache lookup — the post-query stats reconcile keys on path+mtime+config_fingerprint (NOT the format-type),
     * so the harvested row-count still lands on this entry. It does NOT isolate the *stats* across declarations; that is
     * why {@link #strictSingleFileMetadata} serves only the declaration-independent row-count (see there).
     * <p>
     * Declared in {@link SchemaCacheKey} next to its sibling reserved suffix so the two markers'
     * distinctness is visible at one declaration site.
     */
    private static final String STRICT_DECLARED_SCHEMA_MARKER = SchemaCacheKey.STRICT_DECLARED_SCHEMA_MARKER;

    /**
     * Builds the strict single-file source metadata, warming the ungrouped-{@code COUNT(*)} metadata fast path for
     * footerless text. Strict resolution reads no file body, so it cannot harvest the row-count itself; instead it seeds
     * a schema-cache entry (keyed via {@link #STRICT_DECLARED_SCHEMA_MARKER}) that the first query's data-node stats
     * capture enriches with the row-count, so query 2+ folds {@code COUNT(*)} to a {@code LocalSourceOperator} instead
     * of re-scanning + type-coercing every declared column.
     * <p>
     * The stats cache is keyed by file+config, NOT by the declared schema, so one entry can be shared by datasets that
     * declare the same file's columns differently (strict-vs-strict under the marker, or strict-vs-inferred via the
     * schema-agnostic reconcile). Two guards keep the served row-count VALUE correct — a shared entry never serves a
     * declaration-DEPENDENT statistic to a foreign declaration:
     * <ul>
     *   <li>the returned schema is always the declaration ({@code logicalSchema}), never the entry's, and only the
     *       ROW-COUNT is served back ({@link #rowCountOnlyStats} strips the per-column {@code _stats.columns.*}). A
     *       column MIN/MAX value depends on the declared column type (numeric vs lexicographic order), so it is never
     *       folded from a possibly differently-declared harvest — strict MIN/MAX re-scans instead of warming;</li>
     *   <li>the row-count is served only under a {@link ErrorPolicy.Mode#FAIL_FAST} error policy — the one policy where
     *       a SUCCESSFUL scan's row-count equals the physical record count for ANY declaration (any structural error,
     *       e.g. a row wider than the declared column count, aborts the query before publish; width underflow keeps the
     *       row). Under {@code skip_row} or {@code null_field} a width-overflow row is DROPPED (the drop is declaration-
     *       independent of value coercion but NOT of the declared column count), so the row-count would become
     *       declaration-dependent; {@link #warmsRowCountSafely} keeps those off the warm path — they re-scan, still
     *       returning the correct count.</li>
     * </ul>
     * The two guards make the served row-count a correct NUMBER for every declaration, but the file+config key leaves one
     * residual — disclosed here, closed only by the fingerprint follow-up: a strict dataset that declares FEWER columns
     * than the file has (a legal narrower binding) would on its own ERROR on {@code COUNT(*)} (its rows overflow its
     * declared width under {@code FAIL_FAST}); once a wider — or inferred — dataset over the same file+config has warmed
     * the shared entry, that dataset's {@code COUNT(*)} folds to the physical row-count instead of erroring. That is a
     * masked abort, not a wrong count (every materializing query on such a mis-bound dataset still fails loudly), and it
     * flaps with cache state. Weaving the declared schema into the cross-node stats fingerprint (see the follow-up) is the
     * complete closure; a width-equality serve gate would close only the strict-vs-strict half (the entry does not record
     * who harvested it, so it cannot gate the inferred-vs-strict half).
     * <p>
     * File-typed (columnar) formats are excluded: they already warm via split-discovery per-split stats, and the strict
     * columnar coercibility check seeds a physical-schema entry under the inferred key. The non-cacheable branch (e.g.
     * HTTP, no stable mtime) keeps the stat-less metadata: there is nothing to warm from. Warming MIN/MAX and the
     * multi-file glob correctly needs the declared schema woven into the cross-node stats fingerprint (a larger change);
     * tracked separately.
     */
    private ExternalSourceMetadata strictSingleFileMetadata(
        String path,
        StoragePath storagePath,
        StorageProvider provider,
        Map<String, Object> config,
        DatasetMapping declaredMapping,
        List<Attribute> logicalSchema,
        String sourceType,
        long mtimeMillis
    ) throws Exception {
        if (isCacheable(provider) && FILE_TYPED_FORMATS.contains(sourceType) == false && warmsRowCountSafely(sourceType, config)) {
            String formatType = detectFormatType(storagePath) + STRICT_DECLARED_SCHEMA_MARKER;
            SchemaCacheKey schemaKey = SchemaCacheKey.build(storagePath.toString(), mtimeMillis, formatType, config);
            // Seed only mtime + config fingerprint; the row-count is absent until the first query's data node harvests
            // it (reconcileSourceStats matches on those two + the path, then overlays STATS_ROW_COUNT). Store no
            // connector config (Map.of()): the inferred text rail stores none either, and the schema cache is shared
            // across users, so the seed must not retain the dataset's credentials. buildMetadataFromCache re-merges the
            // live query config on read. Build the seed inside the loader so its buildFormatConfig runs only on a cache
            // MISS, not on every (mostly warm) resolve.
            SchemaCacheEntry entry = cacheService.getOrComputeSchema(
                schemaKey,
                k -> SchemaCacheEntry.from(
                    logicalSchema,
                    sourceType,
                    path,
                    Map.of(
                        ExternalStats.MTIME_MILLIS_KEY,
                        mtimeMillis,
                        ExternalStats.CONFIG_FINGERPRINT_KEY,
                        SchemaCacheKey.buildFormatConfig(config)
                    ),
                    Map.of()
                )
            );
            // Schema stays the declaration; serve ONLY the (declaration-independent) row-count, never per-column stats.
            // Mirror the cold branch's wrapAsExternalSourceMetadata schema guard here — buildMetadataFromCache does not
            // validate — so warm and cold enforce the same invariant.
            validateSchemaUsesOnlyReferenceAttributes(logicalSchema);
            ExternalSourceMetadata full = buildMetadataFromCache(entry, logicalSchema, config);
            return replaceSourceMetadata(full, rowCountOnlyStats(full.sourceMetadata()));
        }
        return wrapAsExternalSourceMetadata(
            new SimpleSourceMetadata(logicalSchema, sourceType, path),
            config,
            declaredReadSpecOf(declaredMapping)
        );
    }

    /**
     * Returns {@code sourceMetadata} without any per-column statistic ({@code _stats.columns.*}). A strict dataset
     * shares its file+config cache entry with other declarations of the same file, so it may fold back only the
     * declaration-independent row-count; a per-column MIN/MAX — whose value depends on the declared column type — must
     * be re-scanned rather than served from a foreign declaration's harvest. Returns the input unchanged when it has no
     * per-column keys (the common cold path).
     */
    private static Map<String, Object> rowCountOnlyStats(Map<String, Object> sourceMetadata) {
        return withoutKeys(sourceMetadata, k -> k.startsWith(SourceStatisticsSerializer.STATS_COL_PREFIX));
    }

    /**
     * True when a successful scan's row-count is INDEPENDENT of the declared schema, so it is safe to serve from a
     * file+config-shared cache entry. This holds only under a {@link ErrorPolicy.Mode#FAIL_FAST} policy: any structural
     * error (e.g. a row wider than the declared column count) aborts the query before publish, and width underflow keeps
     * the row, so a committed {@code FAIL_FAST} row-count equals the physical record count for any declaration. Under
     * {@link ErrorPolicy.Mode#SKIP_ROW} or {@link ErrorPolicy.Mode#NULL_FIELD} a width-overflow row is DROPPED (the CSV
     * reader drops a structurally-malformed row "even under NULL_FIELD"), and the width depends on the declared column
     * count, so the row-count becomes declaration-dependent — a shared entry could then serve a wrong {@code COUNT(*)} to
     * a differently-declared dataset. Resolved through {@link ErrorPolicy#fromConfig} against the reader's own default so
     * it is format-agnostic (and catches the implicit {@code SKIP_ROW} a bare {@code max_errors} selects).
     */
    private boolean warmsRowCountSafely(String sourceType, Map<String, Object> config) {
        FormatReader reader = dataSourceModule.formatReaderRegistry().findByName(sourceType);
        ErrorPolicy defaultPolicy = reader != null ? reader.defaultErrorPolicy() : ErrorPolicy.STRICT;
        try {
            return ErrorPolicy.fromConfig(config, defaultPolicy).isStrict();
        } catch (IllegalArgumentException e) {
            // The warm decision is an optimization and must NOT introduce a plan-time failure. An invalid error policy
            // (e.g. error_mode=bogus, or fail_fast + a budget) is left for the data node's operator factory to reject at
            // scan time — exactly as it was before this warm path existed — so a query that prunes the scan away (e.g.
            // LIMIT 0) still succeeds rather than erroring during resolution. Conservatively do not warm.
            return false;
        }
    }

    /**
     * Strict multi-file resolution: the declared mapping is the entire schema for every file, so text formats need only
     * the glob listed — no per-file metadata reads. Columnar formats are the one exception:
     * {@link #rejectStrictColumnarUncoercibleTypes} reads one anchor file's footer (cached) to validate the declared
     * types (coercible pairs are coerced by the reader at decode time). Each file resolves to the declared schema
     * (identity mapping).
     */
    private ExternalSourceResolution.ResolvedSource resolveStrictMultiFile(
        String path,
        StoragePath storagePath,
        StorageProvider provider,
        @Nullable List<PartitionFilterHintExtractor.PartitionFilterHint> hints,
        boolean hivePartitioning,
        Map<String, Object> config,
        DatasetMapping declaredMapping
    ) throws Exception {
        FileList listing;
        // Strict multi-file still does the same glob listing as the inferred path — record it as discovery too, so
        // strict resolutions are not invisible in the discovery telemetry (mirrors resolveMultiFileSource).
        long discoveryStartNanos = System.nanoTime();
        if (path.indexOf(',') >= 0) {
            int maxDiscoveredFiles = ExternalSourceSettings.MAX_DISCOVERED_FILES.get(settings);
            int maxGlobExpansion = ExternalSourceSettings.MAX_GLOB_EXPANSION.get(settings);
            listing = GlobExpander.expand(path, provider, hints, hivePartitioning, maxDiscoveredFiles, maxGlobExpansion);
        } else if (isCacheable(provider)) {
            listing = cachedListing(path, storagePath, provider, hints, hivePartitioning, config);
        } else {
            listing = expandAndCompact(path, provider, hints, hivePartitioning, storagePath);
        }
        recordDiscovery(listing, discoveryStartNanos, storagePath.scheme());
        if (listing.fileCount() == 0) {
            throw new IllegalArgumentException("Glob pattern matched no files: " + path);
        }

        // Declared mapping is the whole schema, in LOGICAL names; a `path` rename is applied at the reader, so the
        // operator (and file schema) work purely in logical names.
        List<Attribute> logicalSchema = DeclaredSchemaResolver.declaredAttributes(declaredMapping);
        // Same compound-extension-aware dispatch as the single-file strict path above. Derive from a CONCRETE listed
        // object name (listing.path(0)), not the raw glob string: a pattern like `*.csv.gz` and a comma-separated path
        // list both make the raw path unreliable for extension parsing, whereas a listed entry is a real file name.
        // Like the inferred FIRST_FILE_WINS path, the anchor's format governs every file with no cross-listing format
        // check — so a heterogeneous, extension-less glob (e.g. `logs/*` matching mixed formats) reads through the
        // first file's reader rather than failing; consistent with the pre-existing multi-file design, not validated here.
        String sourceType = FormatNameResolver.resolveFormatName(
            config,
            listing.path(0).objectName(),
            dataSourceModule.formatReaderRegistry()
        );

        // Partition columns are path-derived (no file I/O), so strict mode surfaces them exactly like the inferred
        // path does. One divergence: the inferred path SHADOWS a physical column that collides with a partition key
        // (partition wins, Spark/DuckDB semantics), but under strict the declaration drives the reader's file schema
        // — text formats bind positionally — so silently dropping a declared column would silently re-bind the rest.
        // A declared column colliding with a partition key is rejected instead: partition columns need no declaring.
        // Cheap no-I/O guard first (partition collision); strict skips only rejectUncoercibleFileTypedRetypes against
        // the unified schema, which needs an inferred schema strict never reads — the anchor-footer check below covers it.
        PartitionMetadata partitionMetadata = listing.partitionMetadata();
        rejectDeclaredMappingViolations(partitionMetadata, declaredMapping);
        // Then the columnar coercibility check, which reads the anchor footer — re-check cancellation first, as a wide
        // glob's listing above can be slow (mirrors resolveMultiFileSource's pre-footer re-check).
        throwIfCancelled();
        rejectStrictColumnarUncoercibleTypes(sourceType, provider, listing.path(0), listing.lastModifiedMillis(0), config, declaredMapping);

        ExternalSourceMetadata extMetadata = wrapAsExternalSourceMetadata(
            new SimpleSourceMetadata(logicalSchema, sourceType, path),
            config,
            declaredReadSpecOf(declaredMapping)
        );
        extMetadata = enrichWithFileCount(extMetadata, listing.fileCount());
        if (partitionMetadata != null && partitionMetadata.isEmpty() == false) {
            extMetadata = enrichSchemaWithPartitionColumns(extMetadata, partitionMetadata, pendingShadowWarnings::add);
        }

        Map<StoragePath, SchemaReconciliation.FileSchemaInfo> schemaMap = new HashMap<>();
        for (int i = 0; i < listing.fileCount(); i++) {
            schemaMap.putAll(singleEntrySchemaMap(listing.path(i), logicalSchema));
        }
        return new ExternalSourceResolution.ResolvedSource(extMetadata, listing, schemaMap);
    }

    /**
     * Apply a non-strict declared mapping onto an already-resolved (inferred) source: retype/rename the declared
     * columns in the user-facing schema (strict — every declared column must appear in the unified schema) and in
     * each per-file schema (lenient — a column may be absent from one file under union-by-name), preserving the
     * inferred stats/sourceMetadata and the per-file column mappings.
     */
    /**
     * Formats whose readers emit blocks in the FILE's own types (self-typed / columnar) rather than parsing text into
     * whatever type the schema requests. For these, a declared retype only works when the reader can coerce the
     * physical type into the declared one at decode time ({@link DeclaredTypeCoercions#supports}); any other pair
     * would fail deep in the engine with a block type mismatch — or worse, silently read as {@code null} — so it is
     * rejected at resolution instead. Text formats (CSV/TSV/NDJSON) parse into the declared type, so they are absent.
     * {@code parquet-rs} is the native parquet reader (feature-flagged) — columnar like {@code parquet}, so it belongs
     * here too.
     * <p>
     * Two checks gate on this set: {@link #rejectStrictColumnarUncoercibleTypes} and the non-strict
     * {@link #rejectUncoercibleFileTypedRetypes}. Removing an entry silently disables both for that format; adding a
     * columnar reader without adding its {@code formatName()} here lets a declared retype slip through unvalidated.
     * {@code ExternalSourceResolverTests#testFileTypedFormatsGatesColumnarRejects} pins the membership so either drift
     * is a test failure.
     * TODO: this classification belongs on the {@code FormatReader} SPI (a capability method) — move it there with the
     * typed DeclaredReadSpec carrier; a single documented constant beats threading a new SPI method for three formats.
     */
    static final Set<String> FILE_TYPED_FORMATS = Set.of("parquet", "orc", FormatNameResolver.FORMAT_PARQUET_RS);

    /**
     * The file-typed formats whose readers implement declared-type coercion in their decode paths
     * ({@link DeclaredTypeCoercions}). {@code parquet-rs} is deliberately absent: its zero-copy Arrow-buffer blocks
     * are produced by the Arrow type alone (see {@code ArrowToEsql}) with no per-column coercion hook yet, so it keeps
     * the strict declared-type-must-equal-file-type check — the pre-coercion behavior — until its conversion layer
     * grows the same {@link DeclaredTypeCoercions} calls. Pinned alongside {@link #FILE_TYPED_FORMATS} by
     * {@code ExternalSourceResolverTests#testFileTypedFormatsGatesColumnarRejects}.
     */
    static final Set<String> COERCING_FILE_TYPED_FORMATS = Set.of("parquet", "orc");

    /**
     * The declaration-vs-source violations detectable without reading file content: a declared column colliding with a
     * hive partition key. Every declaration resolution path (strict single/multi and the non-strict overlay) funnels
     * through this one guard so enforcement stays uniform and a future path cannot silently skip a check. The
     * type-vs-file checks ({@link #rejectUncoercibleFileTypedRetypes}, {@link #rejectStrictColumnarUncoercibleTypes})
     * live apart because they need a physical schema this no-I/O guard never reads.
     */
    private static void rejectDeclaredMappingViolations(@Nullable PartitionMetadata partitionMetadata, DatasetMapping declaredMapping) {
        rejectDeclaredPartitionCollision(partitionMetadata, declaredMapping);
    }

    /**
     * Rejects a declared column that collides with a hive partition key, for BOTH strict and non-strict declarations. A
     * partition column is path-derived (the partition value is the same for every row of a file) and needs no declaring;
     * declaring one either silently re-binds the positional text columns (strict) or overlays/retypes the partition
     * attribute against the value the injector stamps with the partition's own type (non-strict) — a silent misbind
     * either way. Checks both the declared logical name and its {@code path} physical, since the shadowed physical
     * column is the partition value too. No-op when the dataset is not partitioned.
     */
    private static void rejectDeclaredPartitionCollision(PartitionMetadata partitionMetadata, DatasetMapping declaredMapping) {
        if (partitionMetadata == null || partitionMetadata.isEmpty() || declaredMapping == null || declaredMapping.mappings() == null) {
            return;
        }
        Map<String, DataType> partitionColumns = partitionMetadata.partitionColumns();
        for (Map.Entry<String, DatasetFieldMapping> e : declaredMapping.mappings().properties().entrySet()) {
            String logical = e.getKey();
            String physical = e.getValue().path() != null ? e.getValue().path() : logical;
            if (partitionColumns.containsKey(logical) || partitionColumns.containsKey(physical)) {
                throw new IllegalArgumentException(
                    "declared column ["
                        + logical
                        + "] collides with a partition column derived from the path; partition columns are "
                        + "path-derived and must not be declared under [properties]"
                );
            }
        }
    }

    /**
     * For a STRICT declaration on a columnar (file-typed) format, validate the declared column types against the file's
     * physical schema. Strict builds its output purely from the declaration and never reads content, but a columnar
     * reader emits the file's OWN types unless it can coerce them to the declared type at decode time
     * ({@link DeclaredTypeCoercions#supports}) — an uncoercible declared type does not error and does not reinterpret;
     * it yields silent nulls. Read one anchor file's physical schema — through the schema cache when the provider is
     * cacheable, exactly like the inferred path, so repeat queries against a strict columnar dataset do not re-read
     * the footer — and run the same coercibility check the non-strict overlay runs, turning the silent-null trap into
     * either a working read-time coercion or an actionable resolution error. No-op for text formats, which parse into
     * the declared type.
     */
    private void rejectStrictColumnarUncoercibleTypes(
        String sourceType,
        StorageProvider provider,
        StoragePath anchor,
        long anchorMtime,
        Map<String, Object> config,
        DatasetMapping declaredMapping
    ) throws Exception {
        // The strict callers now derive sourceType via FormatNameResolver.resolveFormatName, which returns non-null or
        // throws, so the null branch is defensive-only today; it is kept so a future null-returning resolution path
        // cannot silently NPE on Set.contains(null) and resurrect the earlier NPE-wrapped-500.
        if (sourceType == null || FILE_TYPED_FORMATS.contains(sourceType) == false || declaredMapping.mappings() == null) {
            return;
        }
        List<Attribute> physicalSchema = (isCacheable(provider)
            ? cachedResolveSingleSource(anchor, anchorMtime, config)
            : resolveSingleSource(anchor.toString(), config)).schema();
        rejectUncoercibleFileTypedRetypes(physicalSchema, sourceType, declaredMapping);
    }

    /**
     * For a file-typed format, every declared column's type must either equal the reconciled (inferred) type of the
     * physical column it reads, or be a type the reader can coerce it into at decode time
     * ({@link DeclaredTypeCoercions#supports} — the field mappers' bulk-ingest coercion set: e.g. a {@code long}
     * column declared {@code double}, a string column declared {@code long}/{@code ip}, a string column declared
     * {@code datetime} parsed with the column's declared {@code format}). The rare pair even ingest cannot coerce
     * (e.g. a timestamp column declared {@code ip}) would surface as an internal block type mismatch deep in the
     * engine or as silent nulls; reject it here, at resolution, with an actionable message instead.
     * <p>
     * The same walk polices a declared date {@code format}: on a file-typed format it only ever takes effect as the
     * string&rarr;date parse pattern, so a format on a column whose physical type is not a string could never apply
     * and is rejected rather than silently ignored. (On text formats the format is always honored — the parse IS the
     * coercion — so text never reaches this check.)
     * <p>
     * {@code parquet-rs} (in {@link #FILE_TYPED_FORMATS} but not {@link #COERCING_FILE_TYPED_FORMATS}) keeps the
     * strict equality check: its Arrow conversion layer has no coercion hook yet.
     */
    private static void rejectUncoercibleFileTypedRetypes(
        List<Attribute> inferredSchema,
        String sourceType,
        DatasetMapping declaredMapping
    ) {
        Map<String, DataType> inferredTypes = new HashMap<>();
        for (Attribute a : inferredSchema) {
            inferredTypes.put(a.name(), a.dataType());
        }
        boolean coercing = COERCING_FILE_TYPED_FORMATS.contains(sourceType);
        for (Map.Entry<String, DatasetFieldMapping> e : declaredMapping.mappings().properties().entrySet()) {
            String physical = e.getValue().path() != null ? e.getValue().path() : e.getKey();
            DataType inferredType = inferredTypes.get(physical);
            if (inferredType == null) {
                continue; // absence is handled by the overlay's own missing-column check
            }
            DataType declaredType = DataType.fromNameOrAlias(e.getValue().type());
            boolean coercible = coercing ? DeclaredTypeCoercions.supports(inferredType, declaredType) : declaredType == inferredType;
            if (coercible == false) {
                throw new IllegalArgumentException(
                    "declared type ["
                        + e.getValue().type()
                        + "] for column ["
                        + e.getKey()
                        + "] cannot be read from the file's type ["
                        + inferredType.typeName().toLowerCase(Locale.ROOT)
                        + "] — ["
                        + sourceType
                        + "] columns carry their own type and no read-time conversion exists for this pair;"
                        + " declare the file's type and cast in the query if needed"
                );
            }
            if (e.getValue().format() != null && isStringType(inferredType) == false) {
                throw new IllegalArgumentException(
                    "[format] on column ["
                        + e.getKey()
                        + "] is not supported for ["
                        + sourceType
                        + "] datasets when the file's column type is ["
                        + inferredType.typeName().toLowerCase(Locale.ROOT)
                        + "]; a format only applies when parsing a string column into a date"
                );
            }
        }
    }

    private static boolean isStringType(DataType type) {
        return type == DataType.KEYWORD || type == DataType.TEXT;
    }

    private ExternalSourceResolution.ResolvedSource applyNonStrictOverlay(
        ExternalSourceResolution.ResolvedSource resolved,
        DatasetMapping declaredMapping
    ) {
        final ExternalSourceMetadata inferred = resolved.metadata();
        PartitionMetadata partitionMetadata = resolved.fileList() != null ? resolved.fileList().partitionMetadata() : null;
        // Partition collision: the same guard the strict paths run. The inferred schema already carries the partition
        // columns, so a declared column colliding with a partition key would overlay/retype it and misbind at read time.
        rejectDeclaredMappingViolations(partitionMetadata, declaredMapping);
        // Non-strict-only: for a self-typed (columnar) reader the declared type must equal the file's inferred type or
        // be coercible from it at decode time. Checked against the unified schema here and per file below (under
        // union-by-name a single file's inferred type can differ from the unified one).
        boolean fileTyped = FILE_TYPED_FORMATS.contains(inferred.sourceType());
        if (fileTyped) {
            rejectUncoercibleFileTypedRetypes(inferred.schema(), inferred.sourceType(), declaredMapping);
        }
        DeclaredSchemaResolver.Overlaid unified = DeclaredSchemaResolver.overlayNonStrict(inferred.schema(), declaredMapping, false);
        // S1 boundary: the warm-aggregate _stats.* map on sourceMetadata is keyed PHYSICAL and holds INFERRED-type values;
        // the declared overlay renames/retypes the plan afterwards. Rekey renames (a pure `path` move changes no value, so
        // the rekeyed stats stay exactly correct — warm serving survives the rename) and poison extrema + drop counts for
        // retyped / declared-date-format columns (the read-time coercion nulls failing cells and re-represents values, so
        // no pre-coercion stat matches a scan). row_count/file_count/size_bytes stay: COUNT(*) stays warm.
        Map<String, DataType> inferredTypes = attributesToTypeMap(inferred.schema());   // physical names, inferred types
        Map<String, DataType> overlaidTypes = attributesToTypeMap(unified.output());    // logical names, declared types
        Map<String, String> physicalToLogical = new HashMap<>();
        Set<String> poisonColumns = new HashSet<>();
        if (declaredMapping.mappings() != null) {
            // Renames are re-derived here (rather than via DeclaredSchemaResolver.renameMap) because the same loop also
            // needs each property's declared type/format for the poison decision; keep the physical-name rule in sync
            // with renameMap (path() != null ? path() : logical).
            for (Map.Entry<String, DatasetFieldMapping> me : declaredMapping.mappings().properties().entrySet()) {
                String logical = me.getKey();
                String physical = me.getValue().path() != null ? me.getValue().path() : logical;
                if (physical.equals(logical) == false) {
                    physicalToLogical.put(physical, logical);
                }
                DataType declaredType = overlaidTypes.get(logical);
                DataType inferredType = inferredTypes.get(physical);
                // inferredType == null is unreachable (overlayNonStrict(lenient=false) above rejects a declared column
                // absent from the unified schema), but poison defensively rather than trust an unkeyed stat.
                if (me.getValue().format() != null || inferredType == null || inferredType != declaredType) {
                    poisonColumns.add(logical);
                }
            }
        }
        // Copy-of to match every sibling producer on this seam (buildUnifiedMetadata / applyFirstFileWinsAggregatedStats
        // / SchemaCacheEntry.safeMetadata are all immutable): this map becomes the long-lived sourceMetadata() below.
        Map<String, Object> overlaidSourceMetadata = Map.copyOf(
            SourceStatisticsSerializer.overlayDeclaredSchemaOnStats(inferred.sourceMetadata(), physicalToLogical, poisonColumns)
        );
        ExternalSourceMetadata overlaidMetadata = new ExternalSourceMetadata() {
            @Override
            public List<Attribute> schema() {
                return unified.output();
            }

            @Override
            public String sourceType() {
                return inferred.sourceType();
            }

            @Override
            public String location() {
                return inferred.location();
            }

            @Override
            public Optional<SourceStatistics> statistics() {
                // Load-bearing: ExternalRelation.toPhysicalExec() re-embeds statistics() over sourceMetadata(). Keeping a
                // typed-stats delegate here would resurrect PHYSICAL-keyed extrema/counts next to the rekeyed logical ones
                // (and un-poison a non-renamed retyped column's counts). The declared read has no typed SourceStatistics
                // to offer — the warm channel is the rekeyed/poisoned _stats.* map on sourceMetadata() below.
                return Optional.empty();
            }

            @Override
            public Optional<List<String>> partitionColumns() {
                return inferred.partitionColumns();
            }

            @Override
            public Map<String, Object> sourceMetadata() {
                return overlaidSourceMetadata;
            }

            @Override
            public Map<String, Object> config() {
                return inferred.config();
            }
        };
        // Recompute per-file mappings when the declaration can retype columns. The inherited mapping was built against
        // the INFERRED unified schema; a declared retype changes both the unified target type and (via the per-file
        // overlay) the type the reader emits, so a stale cast slot — e.g. a union-by-name KEYWORD fallback for a column
        // now declared datetime — would corrupt the page. Rebuilding from the two overlaid schemas keeps the
        // union-by-name widening casts for undeclared columns and drops the now-satisfied ones for declared columns.
        // The mapping width is the data-only unified schema (partition columns are path-derived, never read), matching
        // the width every inherited mapping already has.
        boolean hasDeclaredColumns = declaredMapping.mappings() != null && declaredMapping.mappings().properties().isEmpty() == false;
        List<Attribute> dataOnlyUnifiedOverlaid = unified.output();
        if (partitionMetadata != null && partitionMetadata.isEmpty() == false) {
            dataOnlyUnifiedOverlaid = ExternalSchema.dataAttributesOf(
                dataOnlyUnifiedOverlaid,
                partitionMetadata.partitionColumns().keySet()
            ).attributes();
        }
        Map<StoragePath, SchemaReconciliation.FileSchemaInfo> overlaidSchemaMap = new HashMap<>();
        for (Map.Entry<StoragePath, SchemaReconciliation.FileSchemaInfo> e : resolved.schemaMap().entrySet()) {
            SchemaReconciliation.FileSchemaInfo info = e.getValue();
            if (fileTyped) {
                // Per-file coercibility: under union-by-name this file's inferred type for a declared column can
                // differ from the unified type checked above; every file the declared column reads from must be
                // coercible on its own or the read would silently null. Names are physical on both sides here
                // (pre-overlay), matching the declared `path` physicals.
                rejectUncoercibleFileTypedRetypes(info.fileSchema().attributes(), inferred.sourceType(), declaredMapping);
            }
            DeclaredSchemaResolver.Overlaid perFile = DeclaredSchemaResolver.overlayNonStrict(
                info.fileSchema().attributes(),
                declaredMapping,
                true
            );
            ColumnMapping mapping = hasDeclaredColumns
                ? SchemaReconciliation.computeMapping(dataOnlyUnifiedOverlaid, perFile.fileSchema())
                : info.mapping();
            // PRE-retype file types, physical-keyed, so the stats boundaries recover the file's real inferred types
            // (the split-level footer normalize and the resolve/commit pinned-column safe-miss), not the overlaid
            // declared ones. A UNION_BY_NAME pin already retyped this file's read schema and snapshotted the pre-pin
            // inferred types onto info.inferredTypes(); preserve that snapshot so a widened+pinned column stays
            // identifiable after the overlay. Only when nothing upstream retyped the file (inferredTypes null) does
            // info.fileSchema() still carry the inferred types, so fall back to it for the declared-overlay-only path.
            Map<String, DataType> preRetypeInferredTypes = info.inferredTypes() != null
                ? info.inferredTypes()
                : attributesToTypeMap(info.fileSchema().attributes());
            overlaidSchemaMap.put(
                e.getKey(),
                new SchemaReconciliation.FileSchemaInfo(
                    new ExternalSchema(perFile.fileSchema()),
                    mapping,
                    info.statistics(),
                    preRetypeInferredTypes
                )
            );
        }
        return new ExternalSourceResolution.ResolvedSource(overlaidMetadata, resolved.fileList(), overlaidSchemaMap);
    }

    private SourceMetadata cachedResolveSingleSource(StoragePath filePath, long mtime, Map<String, Object> config) throws Exception {
        String formatType = detectFormatType(filePath);
        SchemaCacheKey schemaKey = SchemaCacheKey.build(filePath.toString(), mtime, formatType, config);
        SchemaCacheEntry entry = cacheService.getOrComputeSchema(
            schemaKey,
            k -> SchemaCacheEntry.from(resolveSingleSource(filePath.toString(), config))
        );
        return buildMetadataFromCache(entry, entry.toAttributes(), config);
    }

    private ExternalSourceMetadata wrapAsExternalSourceMetadata(
        SourceMetadata metadata,
        Map<String, Object> queryConfig,
        DeclaredReadSpec declaredReadSpec
    ) {
        validateSchemaUsesOnlyReferenceAttributes(metadata.schema());

        if (metadata instanceof ExternalSourceMetadata extMetadata) {
            if (extMetadata.config() != null && extMetadata.config().isEmpty() == false) {
                // Stats are embedded into sourceMetadata() below for the general path; for
                // ExternalSourceMetadata instances that already carry config (e.g. Iceberg),
                // their factory is responsible for populating sourceMetadata() — statistics()
                // is typically empty so there is nothing extra to embed.
                //
                // This early return does NOT carry the declared read-instructions onto this rail's metadata — so a
                // declared mapping's renames / _id.path would silently vanish. Until this rail supports them, reject
                // loudly rather than ignore a mapping the user declared.
                if (declaredReadSpec.isEmpty() == false) {
                    throw new IllegalArgumentException(
                        "declared mappings with column types, [path] renames, [_id.path], or a column [format] "
                            + "are not supported for this source type"
                    );
                }
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
