/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.util.Check;
import org.elasticsearch.xpack.esql.datasources.cache.ExternalSourceCacheSettings;
import org.elasticsearch.xpack.esql.datasources.spi.ColumnExtractorAware;
import org.elasticsearch.xpack.esql.datasources.spi.ConfigKeyValidator;
import org.elasticsearch.xpack.esql.datasources.spi.Configured;
import org.elasticsearch.xpack.esql.datasources.spi.ErrorPolicy;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalSourceFactory;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalSourceMetrics;
import org.elasticsearch.xpack.esql.datasources.spi.FileList;
import org.elasticsearch.xpack.esql.datasources.spi.FilterPushdownSupport;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReader;
import org.elasticsearch.xpack.esql.datasources.spi.ListingHint;
import org.elasticsearch.xpack.esql.datasources.spi.SourceMetadata;
import org.elasticsearch.xpack.esql.datasources.spi.SourceOperatorFactoryProvider;
import org.elasticsearch.xpack.esql.datasources.spi.SplitProvider;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.datasources.spi.StorageProvider;

import java.io.Closeable;
import java.io.IOException;
import java.time.Instant;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;

/**
 * Framework-internal factory that bridges the building-block registries
 * ({@link StorageProviderRegistry} + {@link FormatReaderRegistry}) into the
 * unified {@link ExternalSourceFactory} contract.
 *
 * <p>This is NOT an SPI extension — it is never returned by any DataSourcePlugin.
 * It is created by {@link DataSourceModule} itself and registered as a catch-all
 * fallback entry (key {@code "file"}) in the sourceFactories map.
 */
final class FileSourceFactory implements ExternalSourceFactory {

    static final String CONFIG_FORMAT = "format";

    /**
     * Aggregated set of keys the coordinator-side path claims from a per-query configuration map.
     * Built from each component's own {@code CONFIG_KEYS} set so adding a new coordinator-level
     * configuration consumer requires updating only the consumer's own constant — the union here
     * picks it up automatically. Components contributing today: {@link ErrorPolicy},
     * {@link FileSplitProvider}, {@link PartitionConfig}, the {@link #CONFIG_FORMAT} override read
     * by this class, and the {@link FormatNameResolver#CONFIG_READER} override read by the
     * format-name resolver.
     */
    static final Set<String> COORDINATOR_KEYS;

    /**
     * Coordinator keys deliberately NOT exposed as dataset settings: the
     * {@link FormatNameResolver#CONFIG_READER} override remains an EXTERNAL-only development knob
     * (a reader alias selects between interchangeable readers for one format). {@link #CONFIG_FORMAT}
     * is a first-class dataset setting and is therefore part of the dataset vocabulary, not listed
     * here. Pinned against {@link #COORDINATOR_KEYS} and the dataset key set by
     * {@code FileSourceFactoryValidationTests} so neither can drift: any new coordinator key must
     * either be added to the dataset vocabulary or explicitly listed here.
     */
    static final Set<String> EXTERNAL_ONLY_KEYS = Set.of(FormatNameResolver.CONFIG_READER);

    static {
        Set<String> keys = new HashSet<>();
        keys.add(CONFIG_FORMAT);
        keys.add(FormatNameResolver.CONFIG_READER);
        keys.addAll(ErrorPolicy.CONFIG_KEYS);
        keys.addAll(FileSplitProvider.CONFIG_KEYS);
        keys.addAll(ExternalSourceResolver.CONFIG_KEYS);
        keys.addAll(PartitionConfig.CONFIG_KEYS);
        COORDINATOR_KEYS = Set.copyOf(keys);
    }

    private final StorageProviderRegistry storageRegistry;
    private final FormatReaderRegistry formatRegistry;
    private final DecompressionCodecRegistry codecRegistry;
    private final Settings settings;
    @Nullable
    private final ExecutorService splitDiscoveryExecutor;
    /**
     * Node-level (root) {@link BlockFactory}, threaded into
     * {@link AsyncExternalSourceOperatorFactory.Builder#producerBlockFactory(BlockFactory)} so that
     * producer-thread allocations performed by iterator wrappers ({@link VirtualColumnIterator},
     * {@link SchemaAdaptingIterator}) route through the global request circuit breaker rather than
     * the driver-local breaker. May be {@code null} in tests where the factory falls back to
     * {@link org.elasticsearch.compute.operator.DriverContext#blockFactory()}.
     */
    @Nullable
    private final BlockFactory blockFactory;
    /**
     * Gate for {@code file://} local-disk reads. Defaults to {@link LocalFileAccess#UNRESTRICTED} in
     * test-only constructors; production always goes through the full-arg constructor via {@link DataSourceModule}.
     */
    private final LocalFileAccess localFileAccess;
    // Node telemetry sink, threaded into the operator factory so opened storage objects publish read metrics.
    private final ExternalSourceMetrics externalSourceMetrics;
    /**
     * Per-node gate bounding concurrent stream-only-compressed segmentators on the shared {@code esql_external_io}
     * pool so their per-chunk parser tasks always have a free thread (elastic/esql-planning #1093, item 4). This
     * factory is a per-node singleton (built once in {@link DataSourceModule}) and every read resolves to the same
     * node-level pool, so one controller here is shared across all queries/operators — no external registry needed.
     */
    private final StreamingSegmentatorAdmission segmentatorAdmission;

    FileSourceFactory(
        StorageProviderRegistry storageRegistry,
        FormatReaderRegistry formatRegistry,
        DecompressionCodecRegistry codecRegistry,
        Settings settings
    ) {
        this(
            storageRegistry,
            formatRegistry,
            codecRegistry,
            settings,
            null,
            null,
            LocalFileAccess.UNRESTRICTED,
            ExternalSourceMetrics.NOOP
        );
    }

    FileSourceFactory(
        StorageProviderRegistry storageRegistry,
        FormatReaderRegistry formatRegistry,
        DecompressionCodecRegistry codecRegistry,
        Settings settings,
        @Nullable ExecutorService splitDiscoveryExecutor
    ) {
        this(
            storageRegistry,
            formatRegistry,
            codecRegistry,
            settings,
            splitDiscoveryExecutor,
            null,
            LocalFileAccess.UNRESTRICTED,
            ExternalSourceMetrics.NOOP
        );
    }

    FileSourceFactory(
        StorageProviderRegistry storageRegistry,
        FormatReaderRegistry formatRegistry,
        DecompressionCodecRegistry codecRegistry,
        Settings settings,
        @Nullable ExecutorService splitDiscoveryExecutor,
        @Nullable BlockFactory blockFactory
    ) {
        this(
            storageRegistry,
            formatRegistry,
            codecRegistry,
            settings,
            splitDiscoveryExecutor,
            blockFactory,
            LocalFileAccess.UNRESTRICTED,
            ExternalSourceMetrics.NOOP
        );
    }

    FileSourceFactory(
        StorageProviderRegistry storageRegistry,
        FormatReaderRegistry formatRegistry,
        DecompressionCodecRegistry codecRegistry,
        Settings settings,
        @Nullable ExecutorService splitDiscoveryExecutor,
        @Nullable BlockFactory blockFactory,
        LocalFileAccess localFileAccess,
        ExternalSourceMetrics externalSourceMetrics
    ) {
        Check.notNull(storageRegistry, "storageRegistry cannot be null");
        Check.notNull(formatRegistry, "formatRegistry cannot be null");
        this.storageRegistry = storageRegistry;
        this.formatRegistry = formatRegistry;
        this.codecRegistry = codecRegistry != null ? codecRegistry : new DecompressionCodecRegistry();
        this.settings = settings != null ? settings : Settings.EMPTY;
        this.splitDiscoveryExecutor = splitDiscoveryExecutor;
        this.blockFactory = blockFactory;
        this.localFileAccess = localFileAccess != null ? localFileAccess : LocalFileAccess.UNRESTRICTED;
        this.externalSourceMetrics = externalSourceMetrics != null ? externalSourceMetrics : ExternalSourceMetrics.NOOP;
        this.segmentatorAdmission = new StreamingSegmentatorAdmission(ExternalSourceSettings.maxConcurrentSegmentators(this.settings));
    }

    @Override
    public String type() {
        return "file";
    }

    @Override
    public boolean canHandle(String location) {
        if (location == null) {
            return false;
        }
        try {
            StoragePath path = StoragePath.of(location);
            String scheme = path.scheme();
            String objectName = path.objectName();
            if (objectName == null || objectName.isEmpty()) {
                return false;
            }
            int lastDot = objectName.lastIndexOf('.');
            if (lastDot < 0 || lastDot == objectName.length() - 1) {
                return false;
            }
            if (storageRegistry.hasProvider(scheme) == false) {
                return false;
            }
            String ext = objectName.substring(objectName.lastIndexOf('.'));
            if (formatRegistry.hasExtension(ext)) {
                return true;
            }
            if (codecRegistry.hasCompressionExtension(ext) && formatRegistry.hasCompressedExtension(objectName)) {
                return true;
            }
            return false;
        } catch (IllegalArgumentException e) {
            return false;
        }
    }

    @Override
    public boolean canHandle(String location, Map<String, Object> config) {
        // The path-only form already claims any resource whose extension maps to a known format.
        if (canHandle(location)) {
            return true;
        }
        // Otherwise the resource carries no extension to infer a format from (an extensionless object, a
        // bare prefix, or an authority). An explicit `format` (or `reader` alias) in the config is
        // authoritative: it names the reader directly, so detection is moot and we claim the resource
        // regardless of its object name — matching resolveReader, which honors an explicit format
        // unconditionally. `auto`/absent leave `format` null here and stay on the extension-based
        // path-only form above.
        if (location == null || config == null || config.isEmpty()) {
            return false;
        }
        try {
            StoragePath path = StoragePath.of(location);
            if (storageRegistry.hasProvider(path.scheme()) == false) {
                return false;
            }
            // Reject a location that names nothing to read — neither an authority nor a path (e.g. "s3://").
            // A file:// URI has an empty authority but a real absolute path, so it is not rejected here.
            boolean noHost = path.host() == null || path.host().isEmpty();
            boolean noPath = path.path() == null || path.path().isEmpty();
            if (noHost && noPath) {
                return false;
            }
            String format = FormatNameResolver.resolve(config, "");
            return format != null && formatRegistry.hasFormat(format);
        } catch (IllegalArgumentException e) {
            return false;
        }
    }

    @Override
    public void validateConfig(String location, Map<String, Object> config) {
        // Gate file:// reads at planning time so the failure is clean and pre-execution.
        // This check runs before the empty-config early-return so bare file:// reads (no WITH clause)
        // are also validated — resolveMetadata calls validateConfig first, covering both paths.
        localFileAccess.check(location);
        if (config == null || config.isEmpty()) {
            return;
        }
        StoragePath storagePath = StoragePath.of(location);
        Configured<StorageProvider> resolvedStorage = storageRegistry.createProviderTrackingConsumedKeys(
            storagePath.scheme(),
            settings,
            ExternalSourceResolver.storageConfig(config)
        );
        Configured<FormatReader> resolvedReader = resolveFormatReader(storagePath.objectName(), config).withConfigTrackingConsumedKeys(
            config
        );
        ConfigKeyValidator.check(config, List.of(resolvedStorage.consumedKeys(), resolvedReader.consumedKeys(), COORDINATOR_KEYS));
    }

    @Override
    public SourceMetadata resolveMetadata(String location, Map<String, Object> config) {
        try {
            StoragePath storagePath = StoragePath.of(location);
            String scheme = storagePath.scheme();

            StorageProvider provider;
            FormatReader reader;
            if (config != null && config.isEmpty() == false) {
                provider = storageRegistry.createProvider(scheme, settings, ExternalSourceResolver.storageConfig(config));
                reader = resolveFormatReader(storagePath.objectName(), config).withConfig(config);
            } else {
                provider = storageRegistry.provider(storagePath);
                reader = resolveFormatReader(storagePath.objectName(), config).withConfig(config);
            }

            StorageObject storageObject = provider.newObject(storagePath);
            if (storageObject.exists() == false) {
                throw new IOException("File does not exist: " + location);
            }
            return reader.metadata(storageObject);
        } catch (IOException e) {
            throw new IllegalArgumentException("Failed to resolve metadata for [" + location + "]", e);
        }
    }

    /**
     * Async metadata resolution. When {@code hint} is non-null the length/mtime came from a directory
     * listing: the storage object is built with those values (so {@code length()} serves the cached
     * value without I/O) and the existence probe is skipped, so no synchronous HEAD/range round-trip
     * runs on the executor before the async footer read. When {@code hint} is null the object is
     * created bare and its existence is verified up front, matching the synchronous path.
     */
    @Override
    public void resolveMetadataAsync(
        String location,
        @Nullable ListingHint hint,
        Map<String, Object> config,
        Executor executor,
        ActionListener<SourceMetadata> listener
    ) {
        final StorageObject storageObject;
        final FormatReader reader;
        try {
            // Reject unknown configuration keys before any provider/reader work — same single source
            // of truth as the synchronous resolveMetadata path.
            validateConfig(location, config);
            StoragePath storagePath = StoragePath.of(location);
            String scheme = storagePath.scheme();

            StorageProvider provider;
            if (config != null && config.isEmpty() == false) {
                provider = storageRegistry.createProviderTrackingConsumedKeys(
                    scheme,
                    settings,
                    ExternalSourceResolver.storageConfig(config)
                ).value();
                reader = resolveFormatReader(storagePath.objectName(), config).withConfigTrackingConsumedKeys(config).value();
            } else {
                provider = storageRegistry.provider(storagePath);
                reader = resolveFormatReader(storagePath.objectName(), config).withConfig(config);
            }

            if (hint != null) {
                storageObject = provider.newObject(storagePath, hint.length(), Instant.ofEpochMilli(hint.lastModifiedMillis()));
            } else {
                storageObject = provider.newObject(storagePath);
                if (storageObject.exists() == false) {
                    listener.onFailure(
                        new IllegalArgumentException(
                            "Failed to resolve metadata for [" + location + "]",
                            new IOException("File does not exist: " + location)
                        )
                    );
                    return;
                }
            }
        } catch (Exception e) {
            listener.onFailure(e);
            return;
        }
        // Map an I/O failure from the async metadata read to the same IllegalArgumentException shape
        // the synchronous path produces, so callers see identical exceptions regardless of path.
        reader.metadataAsync(storageObject, executor, listener.delegateResponse((l, e) -> {
            if (e instanceof IOException) {
                l.onFailure(new IllegalArgumentException("Failed to resolve metadata for [" + location + "]", e));
            } else {
                l.onFailure(e);
            }
        }));
    }

    @Override
    public SplitProvider splitProvider() {
        return new FileSplitProvider(
            FileSplitProvider.DEFAULT_TARGET_SPLIT_SIZE,
            codecRegistry,
            storageRegistry,
            formatRegistry,
            settings,
            splitDiscoveryExecutor
        );
    }

    @Override
    public SourceOperatorFactoryProvider operatorFactory() {
        return context -> {
            StoragePath path = context.path();
            Map<String, Object> config = context.config();

            // Enforce the file:// allowlist confinement at execution time on the data node, before either branch.
            // The bare-read branch (provider(path)) checks this internally, but the WITH-config branch goes through
            // createProvider, which only enforces the scheme-level on/off gate; checking here keeps both paths uniform.
            localFileAccess.check(path);

            StorageProvider storage;
            if (config != null && config.isEmpty() == false) {
                storage = storageRegistry.createProvider(path.scheme(), settings, ExternalSourceResolver.storageConfig(config));
            } else {
                storage = storageRegistry.provider(path);
            }

            FormatReader format = resolveFormatReader(path.objectName(), config).withConfig(config)
                .withPushedFilter(context.pushedFilter())
                .withSchema(context.attributes())
                // Declared per-column date formats: the spec keys them by logical name, but the reader sees physical
                // (file) column names, so physicalize the keys through the same `path` renames here at the last mile.
                .withDeclaredDateFormats(physicalDateFormats(context.declaredReadSpec()))
                // Declared-type columns (licensed to narrow toward their target): same logical->physical last-mile
                // translation, so the by-name columnar readers can key their null-fill escape on the physical names.
                .withDeclaredTypeColumns(physicalDeclaredTypeColumns(context.declaredReadSpec()))
                // Keyed on provenance, not renames: a DECLARED schema binds by name even with no `path`, and an
                // INFERRED (dynamic) schema must never re-bind at the reader (its positions already came from the file).
                .withDeclaredPathBinding(context.declaredReadSpec().provenance() == SchemaProvenance.DECLARED);
            ErrorPolicy errorPolicy = resolveErrorPolicy(config, format);

            Map<String, Object> partitionValues = Map.of();
            if (context.split() instanceof FileSplit fileSplit) {
                partitionValues = fileSplit.partitionValues();
            }

            List<Expression> pushedExpressions = context.pushedExpressions();
            FilterPushdownSupport pushdownSupport = (pushedExpressions != null && pushedExpressions.isEmpty() == false)
                ? format.filterPushdownSupport()
                : null;

            // Per-query fairness: draw a dynamic slice of the per-scheme permit budget so one query cannot starve the
            // rest on the same backend. Storage also carries reactive retry/backoff (per-store 503 backoff) from the
            // registry (see StorageProviderRegistry#wrapProvider), and in-flight reads are additionally bounded by
            // the per-scheme permit semaphore. Blocking reads run on the dedicated esql_external_io pool.
            Closeable onClose = null;
            ConcurrencyBudgetAllocator allocator = storageRegistry.allocatorForScheme(path.scheme().toLowerCase(Locale.ROOT));
            if (allocator != null) {
                QueryBudgetedStorageProvider budgeted = new QueryBudgetedStorageProvider(storage, allocator.register());
                storage = budgeted;
                onClose = budgeted;
            }

            // Read/parse pool: the dedicated esql_external_io pool (blocking opens + parser workers), falling back to
            // the compute pool when no distinct file-read pool is wired. The producer/drain loop runs on the compute
            // pool (context.executor(), esql_worker) instead — see AsyncExternalSourceOperatorFactory — so a full
            // read/parse pool of blocked parser workers cannot starve the drain that consumes their pages.
            Executor readExecutor = context.fileReadExecutor() != null ? context.fileReadExecutor() : context.executor();
            Executor producerExecutor = context.executor();
            // Deferred extraction fires when both signals are present: the reader is
            // ColumnExtractorAware AND the plan paired this source with an ExternalFieldExtractExec
            // (the context flag InsertExternalFieldExtraction sets). _rowPosition presence in the
            // projection is NOT a valid signal on its own — InjectRowPositionForExternalId also
            // injects it for plain _id composition, where enabling deferred mode would create a
            // SourceExtractors registry no extract operator ever closes.
            boolean deferredExtraction = format instanceof ColumnExtractorAware && context.deferredExtraction();

            return AsyncExternalSourceOperatorFactory.builder(
                storage,
                format,
                path,
                context.attributes(),
                context.batchSize(),
                context.maxBufferSize(),
                readExecutor
            )
                .producerExecutor(producerExecutor)
                .externalSourceMetrics(externalSourceMetrics)
                .rowLimit(context.rowLimit())
                .fileList(context.fileList())
                .schemaMap(context.schemaMap())
                .partitionColumnNames(context.partitionColumnNames())
                .partitionValues(partitionValues)
                .producerBlockFactory(blockFactory)
                .sliceQueue(context.sliceQueue())
                .errorPolicy(errorPolicy)
                .parsingParallelism(context.parsingParallelism())
                .maxConcurrentOpenSegments(context.maxConcurrentOpenSegments())
                .maxRecordBytes(context.maxRecordBytes())
                .statsStripeSize(ExternalSourceCacheSettings.STRIPE_SIZE.get(settings).getBytes())
                .statsColumnScope(ExternalSourceCacheSettings.STRIPE_COLUMNS.get(settings))
                .streamingSegmentatorAdmission(segmentatorAdmission)
                .parallelism(context.parallelism())
                .pushedExpressions(pushedExpressions)
                .pushdownSupport(pushdownSupport)
                .onClose(onClose)
                .deferredExtraction(deferredExtraction)
                // datasetName drives the per-file _index synthesizer in
                // {@link ExternalMetadataColumns#extractPerFileConstants}; null when the query
                // came from inline EXTERNAL (no dataset mapping), populated when it came from
                // FROM <dataset>.
                .datasetName(context.datasetName())
                // Declared `path` renames, applied to reader-facing names (projection + read schema) at the last mile.
                .renames(context.declaredReadSpec().renames())
                // Declared _id.path (logical column name): stamps _id from that column instead of the synthetic id.
                .idPath(context.declaredReadSpec().idPath())
                // Single-file producer paths (sync-wrapper, native-async) carry no per-file mtime
                // carrier; without this wire-up _version would silently render as SQL NULL even
                // on resolved single-file plans. The slice-queue / multi-file paths still source
                // mtime from FileSplit.partitionValues / per-FileList entry respectively and
                // ignore this builder value.
                .lastModifiedMillis(firstFileMtime(context.fileList()))
                .build();
        };
    }

    /**
     * Returns the {@code lastModifiedMillis} of the first entry in {@code fileList}, or {@code null}
     * when the list is absent / unresolved / empty. Threaded into
     * {@link AsyncExternalSourceOperatorFactory.Builder#lastModifiedMillis(Long)} so that the
     * single-file producer paths render {@code _version} from the file's mtime instead of SQL
     * {@code NULL}. Returning a boxed {@code Long} lets the builder distinguish "no mtime available"
     * from "mtime is zero (epoch)".
     */
    @Nullable
    private static Long firstFileMtime(@Nullable FileList fileList) {
        if (fileList == null || fileList.fileCount() == 0) {
            return null;
        }
        return fileList.lastModifiedMillis(0);
    }

    /**
     * Re-keys the spec's declared date formats from logical to physical (file) column names, applying the declared
     * {@code path} renames — a column read as physical name {@code p} carries the format declared on its logical name.
     * The text readers key their per-column formatters by the physical names they actually see. Empty in, empty out.
     */
    private static Map<String, String> physicalDateFormats(DeclaredReadSpec spec) {
        Map<String, String> logical = spec.dateFormats();
        if (logical.isEmpty()) {
            return Map.of();
        }
        Map<String, String> renames = spec.renames();
        Map<String, String> physical = new HashMap<>(logical.size());
        for (Map.Entry<String, String> e : logical.entrySet()) {
            // Route through the PhysicalNames chokepoint (the single source of truth for logical->physical) rather than
            // hand-rolling the lookup, so this reader-facing name surface stays consistent with the others.
            physical.put(PhysicalNames.translate(e.getKey(), renames), e.getValue());
        }
        return physical;
    }

    /**
     * The declared-type columns as the physical (file) names the by-name columnar readers see. The spec keys them by
     * logical name; physicalize through the same {@code path} renames as {@link #physicalDateFormats}. Empty in, empty
     * out. A declared-type column is licensed to coerce (including narrow) toward its target; an inferred column may only
     * widen, so the reader keys its whole-column incompatibility null-fill on membership in this set.
     */
    private static Set<String> physicalDeclaredTypeColumns(DeclaredReadSpec spec) {
        Set<String> logical = spec.declaredTypeColumns();
        if (logical.isEmpty()) {
            return Set.of();
        }
        Map<String, String> renames = spec.renames();
        Set<String> physical = new HashSet<>(logical.size());
        for (String col : logical) {
            physical.add(PhysicalNames.translate(col, renames));
        }
        return physical;
    }

    /** Delegates to {@link ErrorPolicy#fromConfig(Map, ErrorPolicy)} with the format's default
     *  policy as the fallback. Kept here so existing call sites and tests do not have to change. */
    static ErrorPolicy resolveErrorPolicy(Map<String, Object> config, FormatReader format) {
        return ErrorPolicy.fromConfig(config, format.defaultErrorPolicy());
    }

    private FormatReader resolveFormatReader(String objectName, Map<String, Object> config) {
        return FormatNameResolver.resolveReader(config, objectName, formatRegistry);
    }
}
