/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.expression.predicate.operator.comparison.BinaryComparison;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.util.Check;
import org.elasticsearch.xpack.esql.datasources.spi.DecompressionCodec;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalSplit;
import org.elasticsearch.xpack.esql.datasources.spi.FileList;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReader;
import org.elasticsearch.xpack.esql.datasources.spi.FrameIndex;
import org.elasticsearch.xpack.esql.datasources.spi.IndexedDecompressionCodec;
import org.elasticsearch.xpack.esql.datasources.spi.RangeAwareFormatReader;
import org.elasticsearch.xpack.esql.datasources.spi.RangeAwareFormatReader.SplitRange;
import org.elasticsearch.xpack.esql.datasources.spi.RecordSplitter;
import org.elasticsearch.xpack.esql.datasources.spi.SegmentableFormatReader;
import org.elasticsearch.xpack.esql.datasources.spi.SplitDiscoveryContext;
import org.elasticsearch.xpack.esql.datasources.spi.SplitDiscoveryResult;
import org.elasticsearch.xpack.esql.datasources.spi.SplitProvider;
import org.elasticsearch.xpack.esql.datasources.spi.SplittableDecompressionCodec;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.datasources.spi.StorageProvider;
import org.elasticsearch.xpack.esql.datasources.utils.BoundedParallelGather;
import org.elasticsearch.xpack.esql.expression.predicate.logical.And;
import org.elasticsearch.xpack.esql.expression.predicate.logical.Not;
import org.elasticsearch.xpack.esql.expression.predicate.logical.Or;
import org.elasticsearch.xpack.esql.expression.predicate.nulls.IsNotNull;
import org.elasticsearch.xpack.esql.expression.predicate.nulls.IsNull;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.Equals;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThan;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThanOrEqual;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.In;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.LessThan;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.LessThanOrEqual;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.NotEquals;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.function.BiFunction;
import java.util.function.BooleanSupplier;

/**
 * Default {@link SplitProvider} for file-based sources.
 * Converts each file in the {@link FileList} into a {@link FileSplit},
 * applying L1 partition pruning when filter hints and partition metadata are available.
 *
 * <p>When filter hints contain resolved {@link Expression} objects, evaluates them against
 * each file's partition values to prune files that cannot match the filter.
 *
 * <p><b>Splitting modes.</b>
 * This provider supports two distinct splitting strategies. The downstream reader's behaviour
 * (partial-line skip vs. no skip) differs between them, gated by
 * {@link org.elasticsearch.xpack.esql.datasources.spi.FormatReadContext#recordAligned()}.
 *
 * <ul>
 *   <li><b>Record-aligned macro splits</b> — for uncompressed line-oriented formats
 *       (NDJSON/JSONL/JSON, CSV/TSV). {@link RecordSplitter#findNextRecordBoundary}
 *       probes near {@code target_split_size} strides so each {@link FileSplit} starts on a
 *       record boundary. Splits are tagged with {@link #RECORD_ALIGNED_MACRO_SPLIT_KEY} and
 *       readers receive {@code recordAligned=true}, so they must <em>not</em> drop any leading
 *       bytes.
 *       See {@link #tryNewlineAlignedMacroSplits}.</li>
 *   <li><b>Block-aligned splits</b> — for splittable compressed formats (e.g. bzip2) via
 *       {@link SplittableDecompressionCodec#findBlockBoundaries}. Splits land on compression
 *       block boundaries, not record boundaries. Readers receive {@code recordAligned=false}
 *       and must skip a leading partial record on every non-first split.
 *       See {@link #tryBlockAlignedSplits}.</li>
 * </ul>
 */
public class FileSplitProvider implements SplitProvider {

    private static final Logger LOGGER = LogManager.getLogger(FileSplitProvider.class);

    // 64 MB — 2x the maximum compression block target (DEFAULT_MACRO_SPLIT_TARGET) to keep
    // memory pressure low while still enabling meaningful cross-node parallelism.
    // DuckDB uses ~32 MB buffers; increase to 128+ MB for high-throughput clusters.
    static final long DEFAULT_TARGET_SPLIT_SIZE = 64 * 1024 * 1024;
    static final long DEFAULT_MACRO_SPLIT_TARGET = 32 * 1024 * 1024; // 32MB compressed
    static final String FIRST_SPLIT_KEY = "_first_split";
    static final String LAST_SPLIT_KEY = "_last_split";

    static final String RANGE_SPLIT_KEY = "_range_split";
    static final String FILE_LENGTH_KEY = "_file_length";
    public static final String CONFIG_TARGET_SPLIT_SIZE = "target_split_size";

    /**
     * Configuration keys this splitter consumes from a query-time configuration map. Aggregated by
     * {@link FileSourceFactory#COORDINATOR_KEYS}. New keys read by this class via {@code config.get(...)}
     * must be added here so the {@link org.elasticsearch.xpack.esql.datasources.spi.ConfigKeyValidator}
     * recognises them — pinned by {@code FileSourceFactoryValidationTests}.
     */
    public static final Set<String> CONFIG_KEYS = Set.of(CONFIG_TARGET_SPLIT_SIZE);

    /**
     * Macro-split starts on a newline-aligned record boundary (see {@link #tryNewlineAlignedMacroSplits}).
     * Downstream readers set {@link org.elasticsearch.xpack.esql.datasources.spi.FormatReadContext#recordAligned()}
     * and pass this flag into {@link ParallelParsingCoordinator#parallelRead}
     * so single-threaded fallback paths do not skip or trim aligned ranges.
     */
    static final String RECORD_ALIGNED_MACRO_SPLIT_KEY = "_record_aligned_macro_split";
    /**
     * Marks splits whose {@code offset()} is a COMPRESSED byte position (bzip2 block-aligned /
     * zstd-indexed frame groups). Text readers anchor {@code _rowPosition} as
     * {@code splitStartByte + decompressed-bytes-consumed}; a compressed anchor plus a
     * decompressed delta is a value on no axis — not split-invariant and collision-prone across
     * splits — so the dispatcher must not compose {@code _id} from these splits (it null-splices
     * the {@code _rowPosition} slot instead).
     */
    static final String COMPRESSED_OFFSET_SPLIT_KEY = "_compressed_offset_split";

    /** Maximum parallel per-file I/O tasks during split discovery (Parquet footer reads, etc.). */
    static final int MAX_PARALLEL_SPLIT_DISCOVERY = 16;

    private static final String DISCOVERY_CANCELLED_MESSAGE = "ES|QL external split discovery cancelled";

    private final long targetSplitSizeBytes;
    private final DecompressionCodecRegistry codecRegistry;
    private final StorageProviderRegistry storageRegistry;
    private final FormatReaderRegistry formatRegistry;
    private final Settings settings;
    @Nullable
    private final Executor executor;

    public FileSplitProvider() {
        this(DEFAULT_TARGET_SPLIT_SIZE, null, null, null, Settings.EMPTY, null);
    }

    public FileSplitProvider(long targetSplitSizeBytes) {
        this(targetSplitSizeBytes, null, null, null, Settings.EMPTY, null);
    }

    public FileSplitProvider(
        long targetSplitSizeBytes,
        DecompressionCodecRegistry codecRegistry,
        StorageProviderRegistry storageRegistry,
        Settings settings
    ) {
        this(targetSplitSizeBytes, codecRegistry, storageRegistry, null, settings, null);
    }

    public FileSplitProvider(
        long targetSplitSizeBytes,
        DecompressionCodecRegistry codecRegistry,
        StorageProviderRegistry storageRegistry,
        FormatReaderRegistry formatRegistry,
        Settings settings
    ) {
        this(targetSplitSizeBytes, codecRegistry, storageRegistry, formatRegistry, settings, null);
    }

    public FileSplitProvider(
        long targetSplitSizeBytes,
        DecompressionCodecRegistry codecRegistry,
        StorageProviderRegistry storageRegistry,
        FormatReaderRegistry formatRegistry,
        Settings settings,
        @Nullable Executor executor
    ) {
        this.targetSplitSizeBytes = targetSplitSizeBytes;
        this.codecRegistry = codecRegistry;
        this.storageRegistry = storageRegistry;
        this.formatRegistry = formatRegistry;
        this.settings = settings != null ? settings : Settings.EMPTY;
        this.executor = executor;
    }

    @Override
    public SplitDiscoveryResult discoverSplits(SplitDiscoveryContext context) {
        FileList fileList = context.fileList();
        if (fileList == null || fileList.isResolved() == false) {
            return SplitDiscoveryResult.EMPTY;
        }

        PartitionMetadata partitionInfo = context.partitionInfo();
        Map<String, Object> config = context.config();
        List<Expression> filterHints = context.filterHints();
        // Strip partition columns from the Query schema before per-file work: their values come
        // from the storage path, not from file bytes, so they don't participate in the file-read
        // narrowing or in the no-overlap skip check.
        ExternalSchema fileBackedQuerySchema = stripPartitionColumns(context.querySchema(), partitionInfo);
        Map<StoragePath, SchemaReconciliation.FileSchemaInfo> schemaInfo = context.schemaMap();

        // Side effect: validates optional {@link #CONFIG_TARGET_SPLIT_SIZE} when users pass WITH options.
        @SuppressWarnings("unused")
        long validatedTargetSplitConfig = resolveTargetSplitSize(config);

        // Hoist provider creation outside the per-file loop when config is non-empty.
        // This avoids constructing a new S3/GCS/Azure client per file.
        // For empty config, storageRegistry.provider() returns a cached singleton per scheme.
        StorageProvider sharedProvider = null;
        if (config != null && config.isEmpty() == false && storageRegistry != null) {
            // Derive scheme from the first file (all files in a FileList share the same scheme).
            if (fileList.fileCount() > 0) {
                String scheme = fileList.path(0).scheme();
                sharedProvider = storageRegistry.createProvider(scheme, settings, config);
            }
        }

        // Dedup cache for ColumnMapping: concurrent-safe when split discovery is parallel.
        Map<ColumnMapping, ColumnMapping> mappingCache = new ConcurrentHashMap<>();

        // Unified schema for the prune-to-per-file-query transformation. When null (legacy
        // callers, data-node paths) the per-file mapping stays at Unified width — the data node
        // still works, the on-wire cost is just slightly higher.
        ExternalSchema unifiedSchema = context.unifiedSchema();

        // Bail before doing any per-file work if the originating query is already cancelled.
        throwIfCancelled(context);

        // Phase 1: sequential filtering — cheap, in-memory predicates applied per file to
        // build the list of FileTask items that need I/O (footer reads, boundary scans).
        List<FileTask> tasks = new ArrayList<>(fileList.fileCount());
        for (int i = 0; i < fileList.fileCount(); i++) {
            StoragePath filePath = fileList.path(i);

            Map<String, Object> partitionValues = new HashMap<>();
            if (partitionInfo != null && partitionInfo.isEmpty() == false) {
                Map<String, Object> filePartitions = partitionInfo.filePartitionValues().get(filePath);
                if (filePartitions != null) {
                    partitionValues.putAll(filePartitions);
                }
            }
            partitionValues.putAll(FileMetadataColumns.extractValues(fileList, i));

            if (partitionValues.isEmpty() == false && filterHints.isEmpty() == false) {
                if (matchesPartitionFilters(partitionValues, filterHints) == false) {
                    // Partition pruning: the path values alone disprove the filter, so the file is skipped unread.
                    continue;
                }
            }

            SchemaReconciliation.FileSchemaInfo fileSchemaInfo = schemaInfo != null ? schemaInfo.get(filePath) : null;

            if (fileBackedQuerySchema.isEmpty() == false && fileSchemaInfo != null) {
                if (skipIfNoColumnOverlap(fileSchemaInfo.fileSchema(), fileBackedQuerySchema)) {
                    continue;
                }
            }

            if (filterHints.isEmpty() == false && fileSchemaInfo != null) {
                Set<String> fileColumnNames = new LinkedHashSet<>(fileSchemaInfo.fileSchema().names());
                // Partition columns are always available (values come from paths, not file data)
                fileColumnNames.addAll(partitionValues.keySet());
                if (skipIfFilterOnMissingColumns(filterHints, fileColumnNames)) {
                    continue;
                }
            }

            String objectName = filePath.objectName();
            String format = null;
            if (objectName != null) {
                int lastDot = objectName.lastIndexOf('.');
                if (lastDot >= 0 && lastDot < objectName.length() - 1) {
                    format = objectName.substring(lastDot);
                }
            }

            long fileLength = fileList.size(i);

            ColumnMapping columnMapping = null;
            List<Attribute> readSchema = null;
            Map<String, DataType> inferredFileTypes = null;
            if (schemaInfo != null) {
                SchemaReconciliation.FileSchemaInfo info = schemaInfo.get(filePath);
                if (info != null) {
                    inferredFileTypes = info.inferredTypes();
                    ColumnMapping mapping = info.mapping();
                    if (mapping != null && unifiedSchema != null && fileBackedQuerySchema.isEmpty() == false) {
                        // Fused narrowing: output dimension goes from Unified to Query, read
                        // dimension goes from File to per-file Query projection. See the
                        // four-schema doc on SchemaReconciliation. For Hive-partitioned sources
                        // context.unifiedSchema() is the post-shadow data-only schema (partition
                        // columns are appended only to the coordinator-facing schema, never here), so
                        // its width matches each per-file mapping built by shadowPartitionCollisions and
                        // satisfies pruneToPerFileQuery's unifiedSchema.size() == index.length assertion.
                        mapping = mapping.pruneToPerFileQuery(unifiedSchema, info.fileSchema(), fileBackedQuerySchema);
                    }
                    if (mapping != null && mapping.isIdentity() == false) {
                        columnMapping = mappingCache.computeIfAbsent(mapping, k -> k);
                    }
                    // Pin the reader to the coordinator's reconciled per-file read schema so it
                    // doesn't re-infer at runtime and disagree with the planner's view of this file.
                    // For text formats this schema already carries each widened column's reconciled
                    // type (see SchemaReconciliation), so the reader reads at that type directly.
                    readSchema = info.fileSchema().attributes();
                }
            }

            tasks.add(
                new FileTask(
                    filePath,
                    fileLength,
                    format,
                    config,
                    partitionValues,
                    columnMapping,
                    readSchema,
                    // Reconciled query types (by unified name). Under UNION_BY_NAME (the only path that can widen
                    // a mixed-temporal column) file column names equal unified names, so footer split stats key
                    // by the same names and normalize by-name. Strict reconciliation rejects differing types, so
                    // there is no mixed-unit column to normalize on that path.
                    unifiedSchema != null ? attributesToTypeMap(unifiedSchema.attributes()) : null,
                    context.maxRecordBytes(),
                    context.declaredReadSpec(),
                    inferredFileTypes
                )
            );
        }

        if (tasks.isEmpty()) {
            return SplitDiscoveryResult.EMPTY;
        }

        // Phase 2: I/O-bound split discovery — parallelize when executor is available.
        final StorageProvider hoistedProvider = sharedProvider;
        final BooleanSupplier isCancelled = context.isCancelled();
        List<List<ExternalSplit>> perFileSplits;
        try {
            if (executor != null && tasks.size() > 1) {
                perFileSplits = BoundedParallelGather.gather(
                    tasks,
                    task -> processFileForSplits(task, hoistedProvider, isCancelled),
                    MAX_PARALLEL_SPLIT_DISCOVERY,
                    executor
                );
            } else {
                perFileSplits = new ArrayList<>(tasks.size());
                for (FileTask task : tasks) {
                    perFileSplits.add(processFileForSplits(task, hoistedProvider, isCancelled));
                }
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to discover splits", e);
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException("Failed to discover splits", e);
        }

        // Flatten per-file split lists into a single ordered list.
        List<ExternalSplit> splits = new ArrayList<>();
        for (List<ExternalSplit> fileSplits : perFileSplits) {
            splits.addAll(fileSplits);
        }
        // Each surviving task produces at least one split, so the task count is the number of
        // distinct files that are actually scanned after coordinator-side pruning.
        return new SplitDiscoveryResult(splits, tasks.size());
    }

    /**
     * Throws {@link TaskCancelledException} when the originating query has been cancelled, so that a
     * long-running split discovery (e.g. thousands of Parquet footer reads) aborts promptly. Mirrors
     * {@code ExternalSourceResolver.throwIfCancelled}. Thrown from {@code processFileForSplits} it is
     * the {@code fn} passed to {@link BoundedParallelGather#gather}, whose documented fast-fail
     * short-circuits not-yet-started files and rethrows the exception, so cancel latency is bounded to
     * the in-flight slots.
     */
    private static void throwIfCancelled(SplitDiscoveryContext context) {
        if (context.isCancelled().getAsBoolean()) {
            throw new TaskCancelledException(DISCOVERY_CANCELLED_MESSAGE);
        }
    }

    /**
     * Input tuple for per-file split discovery, holding all data needed to compute splits
     * for a single file without accessing shared mutable state.
     */
    private record FileTask(
        StoragePath filePath,
        long fileLength,
        @Nullable String format,
        Map<String, Object> config,
        Map<String, Object> partitionValues,
        @Nullable ColumnMapping columnMapping,
        @Nullable List<Attribute> readSchema,
        @Nullable Map<String, DataType> reconciledTypes,
        int maxRecordBytes,
        DeclaredReadSpec declaredReadSpec,
        // PRE-overlay inferred file types (physical-keyed), or null when no declared overlay ran. The stats-type
        // authority for normalizing footer range stats — NOT the overlaid readSchema types.
        @Nullable Map<String, DataType> inferredFileTypes
    ) {}

    private static Map<String, DataType> attributesToTypeMap(List<Attribute> attributes) {
        Map<String, DataType> types = new HashMap<>(attributes.size());
        for (Attribute a : attributes) {
            types.put(a.name(), a.dataType());
        }
        return types;
    }

    /**
     * Computes the splits for a single file. Uses the hoisted provider when provided (non-null),
     * otherwise falls back to the registry for per-call provider resolution.
     * This method is safe to call concurrently from multiple threads.
     */
    private List<ExternalSplit> processFileForSplits(FileTask task, @Nullable StorageProvider hoistedProvider, BooleanSupplier isCancelled)
        throws IOException {
        if (isCancelled.getAsBoolean()) {
            throw new TaskCancelledException(DISCOVERY_CANCELLED_MESSAGE);
        }
        // Carry the cancellation signal as ambient thread-local state so the synchronous retry/throttle
        // backoff inside the footer reads below can abort a parked sleep on cancel.
        return StorageRetryCancellation.callWithCancellation(isCancelled, () -> computeFileSplits(task, hoistedProvider, isCancelled));
    }

    private List<ExternalSplit> computeFileSplits(FileTask task, @Nullable StorageProvider hoistedProvider, BooleanSupplier isCancelled)
        throws IOException {
        List<ExternalSplit> fileSplits = new ArrayList<>();

        // Resolve the config-aware reader once and reuse it for both the sequential-whole-file gate and the
        // newline-aligned macro-split attempt below, which would otherwise each resolve it independently.
        FormatReader configuredReader = resolveConfiguredReader(task.filePath(), task.config());

        // Quoted or escaped CSV/TSV cannot be probed at arbitrary offsets (an in-quote newline, or a
        // backslash-escaped raw newline, would be misread as a record terminator), so no start-anywhere
        // splitting is safe: not newline-aligned macro-splits, nor compressed block/frame-aligned splits.
        // Emit a single whole-file split (identical to the fallback below); the reader consumes it as one
        // sequential stream and finds boundaries quote/escape-aware.
        if (requiresSequentialWholeFileRead(configuredReader)) {
            fileSplits.add(
                FileSplit.withReadSchema(
                    "file",
                    task.filePath(),
                    0,
                    task.fileLength(),
                    task.format(),
                    task.config(),
                    task.partitionValues(),
                    task.columnMapping(),
                    task.readSchema()
                )
            );
            return fileSplits;
        }

        // Try block-aligned splitting for splittable compressed files (e.g. .ndjson.bz2).
        // This is independent of targetSplitSizeBytes — compressed files with splittable
        // codecs are always split at block boundaries when possible.
        if (tryBlockAlignedSplits(
            task.filePath(),
            task.fileLength(),
            task.format(),
            task.config(),
            task.partitionValues(),
            task.columnMapping(),
            task.readSchema(),
            fileSplits,
            hoistedProvider
        )) {
            return fileSplits;
        }

        if (tryRangeAwareSplits(
            task.filePath(),
            task.fileLength(),
            task.format(),
            task.config(),
            task.partitionValues(),
            task.columnMapping(),
            task.readSchema(),
            task.reconciledTypes(),
            task.declaredReadSpec(),
            task.inferredFileTypes(),
            fileSplits,
            hoistedProvider
        )) {
            return fileSplits;
        }

        Map<String, Object> config = task.config();
        StoragePath filePath = task.filePath();
        long fileLength = task.fileLength();
        String format = task.format();
        Map<String, Object> partitionValues = task.partitionValues();
        ColumnMapping columnMapping = task.columnMapping();
        List<Attribute> readSchema = task.readSchema();
        long effectiveTargetSplitBytes = resolveTargetSplitSize(config);

        if (tryNewlineAlignedMacroSplits(
            filePath,
            fileLength,
            format,
            config,
            partitionValues,
            columnMapping,
            readSchema,
            effectiveTargetSplitBytes,
            task.maxRecordBytes(),
            fileSplits,
            hoistedProvider,
            configuredReader,
            isCancelled
        )) {
            return fileSplits;
        }

        // Whole-file split when macro splitting does not apply (small files, unsupported formats, or single aligned span).
        fileSplits.add(
            FileSplit.withReadSchema("file", filePath, 0, fileLength, format, config, partitionValues, columnMapping, readSchema)
        );
        return fileSplits;
    }

    /**
     * Resolves the config-aware {@link FormatReader} for a file, or {@code null} when it cannot be resolved
     * (no {@code formatRegistry}, no object name, or an unknown extension). Config-aware so a {@code WITH}
     * override (e.g. {@code mode=plain}, {@code quote=none}) selects the same reader/splitter the read path
     * will actually use: {@code byExtension} alone yields the extension default (quoted for {@code .csv}),
     * whose non-strided splitter would trip {@link #computeRecordAlignedMacroSplitStarts}' guard for a
     * plain-mode file. {@code withConfig} returns {@code null} only for test mocks; the base reader is used
     * in that case. The compression suffix is stripped by {@link FormatNameResolver}, so this resolves the
     * inner text reader for compressed files (e.g. {@code .csv.bz2}) too.
     */
    @Nullable
    private FormatReader resolveConfiguredReader(StoragePath filePath, Map<String, Object> config) {
        if (formatRegistry == null) {
            return null;
        }
        String objectName = filePath.objectName();
        if (objectName == null) {
            return null;
        }
        try {
            FormatReader base = FormatNameResolver.resolveReader(config, objectName, formatRegistry);
            FormatReader configured = base.withConfig(config);
            return configured != null ? configured : base;
        } catch (RuntimeException e) {
            LOGGER.debug(() -> Strings.format("Cannot resolve reader for [%s]; treating it as non-segmentable", objectName), e);
            return null;
        }
    }

    /**
     * Whether the file's config-resolved record splitter forces one sequential whole-file stream instead of any
     * start-anywhere split. A strided splitter (plain CSV/TSV, NDJSON) is always splittable. A non-strided
     * splitter (quoted or escaped CSV/TSV, whose records may span a raw newline) is splittable only when it can
     * <em>prove</em> a record start at an arbitrary offset ({@link RecordSplitter#supportsProvenProbing()}) and it
     * is not a compression-delegating reader (a quoted {@code .csv.bz2} stays whole-file: the probe would run
     * against compressed bytes). Returns {@code false} (splitting allowed) when the reader could not be resolved,
     * so an unresolvable reader is treated as splittable.
     */
    private boolean requiresSequentialWholeFileRead(@Nullable FormatReader reader) {
        if (reader == null) {
            return false;
        }
        SegmentableFormatReader seg = AsyncExternalSourceOperatorFactory.resolveSegmentableReader(reader);
        if (seg == null) {
            return false;
        }
        RecordSplitter splitter = seg.recordSplitter();
        // A null splitter (only reachable from mocks) keeps the strided default: splitting stays enabled.
        if (splitter == null || splitter.supportsStridedProbing()) {
            return false;
        }
        boolean provenMacroSplittable = splitter.supportsProvenProbing() && reader instanceof CompressionDelegatingFormatReader == false;
        return provenMacroSplittable == false;
    }

    /**
     * Builds a {@link StorageObject} that exposes only the bytes for the given {@link FileSplit}.
     * Always wraps the provider's base object in {@link RangeStorageObject} so format readers and
     * splittable decompressors only see the split's compressed byte span (including offset {@code 0}).
     */
    public static StorageObject storageObjectForSplit(StorageProvider storageProvider, FileSplit fileSplit) {
        return new RangeStorageObject(storageProvider.newObject(fileSplit.path()), fileSplit.offset(), fileSplit.length());
    }

    /**
     * Attempts to create block-aligned splits for files with splittable compression.
     * Returns true if block-aligned splits were created, false if the file should
     * fall through to normal splitting logic.
     *
     * <p>Macro-splits are disjoint: split {@code m} ends exactly where split {@code m+1}
     * begins. Records that straddle a macro-split boundary are handled by the codec's
     * decompression wrapper, which switches to "finish-current-line" mode once the split
     * boundary is reached at a block end and emits bytes from the next block up to (and
     * including) the first {@code '\n'}. The subsequent split drops that same tail via
     * {@code skipFirstLine}. This yields exact record counts without duplicates or loss.
     *
     * <p>Protocol cross-references (kept as prose since the datasource plugins are not compile-
     * time dependencies of this module):
     * <ul>
     *   <li>Codec side — {@code Bzip2DecompressionCodec.BlockBoundedDecompressStream}
     *       implements finish-current-line on the split boundary.</li>
     *   <li>Reader side — {@code NdJsonPageIterator.skipToNextLine}, wired through
     *       {@code NdJsonFormatReader.read}'s {@code skipFirstLine} flag, drops the leading
     *       partial record on every non-first split.</li>
     * </ul>
     */
    private boolean tryBlockAlignedSplits(
        StoragePath filePath,
        long fileLength,
        String format,
        Map<String, Object> config,
        Map<String, Object> partitionValues,
        @Nullable ColumnMapping columnMapping,
        @Nullable List<Attribute> readSchema,
        List<ExternalSplit> splits,
        @Nullable StorageProvider hoistedProvider
    ) {
        if (codecRegistry == null || storageRegistry == null || format == null) {
            return false;
        }

        DecompressionCodec codec = codecRegistry.byExtension(format);

        // Prefer IndexedDecompressionCodec (e.g. zstd seekable) over SplittableDecompressionCodec
        // (e.g. bzip2) when an index is available, since index-based splitting avoids scanning.
        if (codec instanceof IndexedDecompressionCodec indexedCodec) {
            if (tryIndexedSplits(
                indexedCodec,
                filePath,
                fileLength,
                format,
                config,
                partitionValues,
                columnMapping,
                readSchema,
                splits,
                hoistedProvider
            )) {
                return true;
            }
        }

        if (codec instanceof SplittableDecompressionCodec == false) {
            return false;
        }
        SplittableDecompressionCodec splittableCodec = (SplittableDecompressionCodec) codec;

        try {
            // Use the hoisted provider when available to avoid constructing a new cloud client
            // per file. Fall back to the registry for zero-config or legacy callers.
            StorageProvider provider = resolveProvider(filePath, config, hoistedProvider);
            StorageObject object = provider.newObject(filePath, fileLength);
            long[] boundaries = splittableCodec.findBlockBoundaries(object, 0, fileLength);

            if (boundaries.length == 0) {
                splits.add(
                    FileSplit.withReadSchema("file", filePath, 0, fileLength, format, config, partitionValues, columnMapping, readSchema)
                );
                return true;
            }

            // Coalesce block boundaries into macro-splits targeting DEFAULT_MACRO_SPLIT_TARGET
            // compressed bytes. This reduces hundreds of tiny per-block splits into 10-40
            // macro-splits while preserving parallelism.
            int[][] macroSplitRanges = groupBoundaries(boundaries, fileLength, DEFAULT_MACRO_SPLIT_TARGET);
            LOGGER.debug(
                "block-aligned splits for [{}]: boundaries={}, macro-splits={}, fileLength={}",
                filePath,
                boundaries.length,
                macroSplitRanges.length,
                fileLength
            );

            for (int m = 0; m < macroSplitRanges.length; m++) {
                int firstBlockIdx = macroSplitRanges[m][0];
                int lastBlockIdx = macroSplitRanges[m][1];
                long start = boundaries[firstBlockIdx];
                boolean isLastMacroSplit = (m == macroSplitRanges.length - 1);

                long end;
                if (isLastMacroSplit) {
                    end = fileLength;
                } else {
                    // Disjoint macro-splits: split m ends exactly where split m+1 begins.
                    // Records straddling the boundary are completed by the codec's
                    // decompression wrapper (finish-current-line mode), and the
                    // subsequent split drops the same tail via skipFirstLine.
                    int nextMacroFirstBlock = macroSplitRanges[m + 1][0];
                    end = boundaries[nextMacroFirstBlock];
                }

                Map<String, Object> splitConfig = new HashMap<>(config);
                splitConfig.put(COMPRESSED_OFFSET_SPLIT_KEY, "true");
                if (m == 0) {
                    splitConfig.put(FIRST_SPLIT_KEY, "true");
                }
                if (isLastMacroSplit) {
                    splitConfig.put(LAST_SPLIT_KEY, "true");
                }
                splits.add(
                    FileSplit.withReadSchema(
                        "file",
                        filePath,
                        start,
                        end - start,
                        format,
                        splitConfig,
                        partitionValues,
                        columnMapping,
                        readSchema
                    )
                );
            }
            return true;
        } catch (IOException e) {
            LOGGER.warn("Failed to scan block boundaries for [{}], falling back to single split", filePath, e);
            return false;
        }
    }

    /**
     * Attempts to create range-aware splits for columnar formats (e.g. Parquet row groups).
     * The format reader reads file metadata (e.g. Parquet footer) to discover independently
     * readable byte ranges. Returns true if range-aware splits were created.
     */
    private boolean tryRangeAwareSplits(
        StoragePath filePath,
        long fileLength,
        String format,
        Map<String, Object> config,
        Map<String, Object> partitionValues,
        @Nullable ColumnMapping columnMapping,
        @Nullable List<Attribute> readSchema,
        @Nullable Map<String, DataType> reconciledTypes,
        DeclaredReadSpec declaredReadSpec,
        @Nullable Map<String, DataType> inferredFileTypes,
        List<ExternalSplit> splits,
        @Nullable StorageProvider hoistedProvider
    ) {
        if (formatRegistry == null || storageRegistry == null || format == null) {
            return false;
        }

        FormatReader reader;
        try {
            reader = FormatNameResolver.resolveReader(config, filePath.objectName(), formatRegistry).withConfig(config);
        } catch (Exception e) {
            return false;
        }

        if (reader instanceof RangeAwareFormatReader == false) {
            return false;
        }
        RangeAwareFormatReader rangeReader = (RangeAwareFormatReader) reader;

        try {
            StorageProvider provider = resolveProvider(filePath, config, hoistedProvider);
            StorageObject object = provider.newObject(filePath, fileLength);

            List<SplitRange> ranges = rangeReader.discoverSplitRanges(object);
            if (ranges.isEmpty()) {
                return false;
            }

            Map<String, Object> splitConfig = new HashMap<>(config);
            splitConfig.put(RANGE_SPLIT_KEY, "true");
            splitConfig.put(FILE_LENGTH_KEY, Long.toString(fileLength));

            for (SplitRange range : ranges) {
                Map<String, Object> rangeStats = range.statistics().isEmpty() ? null : range.statistics();
                if (rangeStats != null && readSchema != null && reconciledTypes != null) {
                    // The type authority for normalizing footer range stats. Without a declaration the footer values ARE
                    // in the readSchema (inferred) types — today's behavior. With a declaration, readSchema is the OVERLAID
                    // (declared) schema, so it lies about the raw footer values; use the file's PRE-overlay inferred types.
                    Map<String, DataType> statsFileTypes;
                    if (declaredReadSpec.isEmpty()) {
                        statsFileTypes = attributesToTypeMap(readSchema);
                    } else {
                        // S1 boundary, split edition. Rekey the `path` renames (a pure move changes no value, so rekeyed
                        // stats stay exact) and poison declared-retyped / date-format columns (the scan's per-value
                        // coercion makes pre-coercion stats untrustworthy), BEFORE unit-normalizing.
                        Map<String, String> physicalToLogical = PhysicalNames.inverse(declaredReadSpec.renames());
                        Set<String> poison = new HashSet<>(declaredReadSpec.dateFormats().keySet());
                        if (inferredFileTypes != null) {
                            Map<String, DataType> overlaidTypes = attributesToTypeMap(readSchema); // logical, declared types
                            for (String logical : declaredReadSpec.declaredTypeColumns()) {
                                String physical = declaredReadSpec.renames().getOrDefault(logical, logical);
                                DataType inferredType = inferredFileTypes.get(physical);
                                // Absent from THIS file (lenient union-by-name overlay skipped it): no footer stat exists
                                // for it here either, so nothing to poison.
                                if (inferredType != null && inferredType != overlaidTypes.get(logical)) {
                                    poison.add(logical);
                                }
                            }
                            rangeStats = SourceStatisticsSerializer.overlayDeclaredSchemaOnStats(rangeStats, physicalToLogical, poison);
                            // Inferred file types, rekeyed to logical so they align with the rekeyed stats + reconciledTypes.
                            statsFileTypes = new HashMap<>(inferredFileTypes.size());
                            for (Map.Entry<String, DataType> e : inferredFileTypes.entrySet()) {
                                statsFileTypes.put(physicalToLogical.getOrDefault(e.getKey(), e.getKey()), e.getValue());
                            }
                        } else {
                            // Declared read but no captured inference (strict paths skip inference): the declared-vs-inferred
                            // comparison is impossible, so conservatively poison EVERY declared column. row_count survives.
                            poison.addAll(declaredReadSpec.declaredTypeColumns());
                            rangeStats = SourceStatisticsSerializer.overlayDeclaredSchemaOnStats(rangeStats, physicalToLogical, poison);
                            statsFileTypes = attributesToTypeMap(readSchema);
                        }
                    }
                    // Footer stats are in each file's LOCAL unit/representation; normalize to the reconciled query type so
                    // the split-filter classifier (which compares a reconciled-unit literal) and the filtered merge
                    // compare/serve in ONE unit across mixed DATETIME(millis)/DATE_NANOS(nanos) files, not unit-blind. A
                    // non-normalizable representation safe-misses via the marker.
                    rangeStats = SourceStatisticsSerializer.normalizeStatsToReconciled(rangeStats, statsFileTypes, reconciledTypes);
                }
                splits.add(
                    FileSplit.withStatisticsAndReadSchema(
                        "file",
                        filePath,
                        range.offset(),
                        range.length(),
                        format,
                        splitConfig,
                        partitionValues,
                        columnMapping,
                        rangeStats,
                        readSchema
                    )
                );
            }
            return true;
        } catch (IOException e) {
            LOGGER.warn("Failed to discover split ranges for [{}], falling back to single split", filePath, e);
            return false;
        }
    }

    /**
     * Macro-splits supported line-oriented formats at record boundaries near {@code targetStrideBytes},
     * enabling multiple workers per file without mid-record cuts.
     */
    private boolean tryNewlineAlignedMacroSplits(
        StoragePath filePath,
        long fileLength,
        @Nullable String format,
        Map<String, Object> config,
        Map<String, Object> partitionValues,
        @Nullable ColumnMapping columnMapping,
        @Nullable List<Attribute> readSchema,
        long targetStrideBytes,
        int maxRecordBytes,
        List<ExternalSplit> splits,
        @Nullable StorageProvider hoistedProvider,
        @Nullable FormatReader reader,
        BooleanSupplier isCancelled
    ) throws IOException {
        if (formatRegistry == null || storageRegistry == null || targetStrideBytes <= 0 || fileLength <= targetStrideBytes) {
            return false;
        }
        if (isNewlineMacroSplitCandidateExtension(format) == false) {
            return false;
        }
        // Reuses the reader resolved once in processFileForSplits (config-aware; see resolveConfiguredReader).
        if (reader == null) {
            return false;
        }
        if (reader instanceof CompressionDelegatingFormatReader) {
            return false;
        }
        if (reader instanceof SegmentableFormatReader == false) {
            return false;
        }
        SegmentableFormatReader segmentableReader = (SegmentableFormatReader) reader;
        StorageProvider provider = resolveProvider(filePath, config, hoistedProvider);
        StorageObject object = provider.newObject(filePath, fileLength);
        List<Long> starts = computeRecordAlignedMacroSplitStarts(
            segmentableReader,
            object,
            fileLength,
            targetStrideBytes,
            maxRecordBytes,
            isCancelled
        );
        if (starts.size() <= 1) {
            return false;
        }
        for (int i = 0; i < starts.size(); i++) {
            long start = starts.get(i);
            long end = (i + 1 < starts.size()) ? starts.get(i + 1) : fileLength;
            long length = Math.subtractExact(end, start);
            Map<String, Object> splitConfig = new HashMap<>(config);
            splitConfig.put(RECORD_ALIGNED_MACRO_SPLIT_KEY, "true");
            if (i == 0) {
                splitConfig.put(FIRST_SPLIT_KEY, "true");
            }
            if (i == starts.size() - 1) {
                splitConfig.put(LAST_SPLIT_KEY, "true");
            }
            splits.add(
                FileSplit.withReadSchema("file", filePath, start, length, format, splitConfig, partitionValues, columnMapping, readSchema)
            );
        }
        return true;
    }

    static boolean isNewlineMacroSplitCandidateExtension(@Nullable String format) {
        if (format == null) {
            return false;
        }
        String f = format.toLowerCase(Locale.ROOT);
        return ".ndjson".equals(f) || ".jsonl".equals(f) || ".json".equals(f) || ".csv".equals(f) || ".tsv".equals(f);
    }

    /** Whether this leaf split came from {@link #tryNewlineAlignedMacroSplits}. */
    public static boolean isRecordAlignedMacroSplit(FileSplit split) {
        return split != null && "true".equals(split.config().get(RECORD_ALIGNED_MACRO_SPLIT_KEY));
    }

    /**
     * Absolute byte offsets where each macro-split starts (always begins with {@code 0}), mirroring
     * {@link ParallelParsingCoordinator#computeSegments} stride semantics with {@code targetStrideBytes}.
     */
    static List<Long> computeRecordAlignedMacroSplitStarts(
        SegmentableFormatReader reader,
        StorageObject storageObject,
        long fileLength,
        long targetStrideBytes,
        int maxRecordBytes,
        BooleanSupplier isCancelled
    ) throws IOException {
        List<Long> boundaries = new ArrayList<>();
        boundaries.add(0L);
        long minSegment = reader.minimumSegmentSize();
        RecordSplitter splitter = reader.recordSplitter(maxRecordBytes);
        boolean strided = splitter.supportsStridedProbing();
        boolean proven = splitter.supportsProvenProbing();
        // A strided splitter (plain CSV/TSV, NDJSON) probes any stride offset directly. A non-strided splitter
        // (quoted/escaped CSV/TSV) can still be macro-split when it supports proven probing: the file start is a
        // known record start, so exactCursor seeds at 0 and every emitted boundary is a proven record start.
        // A splitter that is neither must have been routed to a whole-file split upstream; if one arrives here
        // that gate failed, so fail loud rather than emit mis-aligned macro-splits that silently mis-count.
        if (strided == false && proven == false) {
            throw new IllegalStateException(
                "record splitter ["
                    + splitter.getClass().getName()
                    + "] supports neither strided nor proven probing and cannot be macro-split"
            );
        }
        // The last proven record start, i.e. the base offset the exact walk streams from when the probe is
        // AMBIGUOUS. The file start is always a record start, so it seeds at 0.
        long exactCursor = 0L;
        long pos = targetStrideBytes;
        while (pos < fileLength) {
            long remaining = fileLength - pos;
            if (remaining < minSegment) {
                break;
            }
            long boundary;
            if (strided) {
                InputStream stream = storageObject.newStream(pos, remaining);
                // Abort rather than close: findNextRecordBoundary reads only a prefix of the range
                // (fileLength - pos bytes), but close() on providers like S3 drains the remainder.
                try (Closeable abortOnExit = () -> storageObject.abortStream(stream)) {
                    long skipped = splitter.findNextRecordBoundary(stream);
                    if (skipped == RecordSplitter.RECORD_TOO_LARGE || skipped < 0) {
                        break;
                    }
                    boundary = pos + skipped;
                }
            } else {
                long probed;
                InputStream probeStream = storageObject.newStream(pos, remaining);
                try (Closeable abortOnExit = () -> storageObject.abortStream(probeStream)) {
                    probed = splitter.findProvenRecordBoundary(probeStream);
                }
                if (probed >= 0) {
                    boundary = pos + probed;
                } else if (probed == RecordSplitter.AMBIGUOUS) {
                    // Bounded probe could not prove a boundary near pos; fall back to an exact walk from the last
                    // proven record start. minSkip is stream-relative (pos - exactCursor) and always > 0.
                    long walkRemaining = fileLength - exactCursor;
                    InputStream walkStream = storageObject.newStream(exactCursor, walkRemaining);
                    long start;
                    try (Closeable abortOnExit = () -> storageObject.abortStream(walkStream)) {
                        start = splitter.findRecordStartAtOrAfter(walkStream, pos - exactCursor, isCancelled);
                    }
                    if (start == RecordSplitter.RECORD_TOO_LARGE || start < 0) {
                        break;
                    }
                    boundary = exactCursor + start;
                } else {
                    // findProvenRecordBoundary only ever returns a boundary (>= 0) or AMBIGUOUS.
                    assert false : "findProvenRecordBoundary returned an unexpected sentinel: " + probed;
                    break;
                }
            }
            if (boundary >= fileLength) {
                break;
            }
            if (fileLength - boundary < minSegment) {
                break;
            }
            assert boundary > boundaries.get(boundaries.size() - 1) : "macro-split boundary must be strictly increasing";
            boundaries.add(boundary);
            // Every emitted boundary is a proven record start, so it becomes the next exact-walk base.
            exactCursor = boundary;
            pos = boundary + targetStrideBytes;
            if (isCancelled.getAsBoolean()) {
                throw new TaskCancelledException("split discovery cancelled");
            }
        }
        return boundaries;
    }

    private boolean tryIndexedSplits(
        IndexedDecompressionCodec indexedCodec,
        StoragePath filePath,
        long fileLength,
        String format,
        Map<String, Object> config,
        Map<String, Object> partitionValues,
        @Nullable ColumnMapping columnMapping,
        @Nullable List<Attribute> readSchema,
        List<ExternalSplit> splits,
        @Nullable StorageProvider hoistedProvider
    ) {
        try {
            StorageProvider provider = resolveProvider(filePath, config, hoistedProvider);
            StorageObject object = provider.newObject(filePath, fileLength);

            if (indexedCodec.hasIndex(object) == false) {
                return false;
            }

            FrameIndex index = indexedCodec.readIndex(object);
            List<FrameIndex.FrameEntry> frames = index.frames();
            if (frames.isEmpty()) {
                splits.add(
                    FileSplit.withReadSchema("file", filePath, 0, fileLength, format, config, partitionValues, columnMapping, readSchema)
                );
                return true;
            }

            // Group frames into macro-splits targeting DEFAULT_MACRO_SPLIT_TARGET
            long accumulated = 0;
            long groupStart = frames.get(0).compressedOffset();
            int splitCount = 0;

            for (int i = 0; i < frames.size(); i++) {
                FrameIndex.FrameEntry frame = frames.get(i);
                accumulated += frame.compressedSize();
                boolean isLast = (i == frames.size() - 1);

                if (accumulated >= DEFAULT_MACRO_SPLIT_TARGET || isLast) {
                    long groupEnd = frame.compressedOffset() + frame.compressedSize();
                    Map<String, Object> splitConfig = new HashMap<>(config);
                    splitConfig.put(COMPRESSED_OFFSET_SPLIT_KEY, "true");
                    if (splitCount == 0) {
                        splitConfig.put(FIRST_SPLIT_KEY, "true");
                    }
                    if (isLast) {
                        splitConfig.put(LAST_SPLIT_KEY, "true");
                    }
                    splits.add(
                        FileSplit.withReadSchema(
                            "file",
                            filePath,
                            groupStart,
                            groupEnd - groupStart,
                            format,
                            splitConfig,
                            partitionValues,
                            columnMapping,
                            readSchema
                        )
                    );
                    splitCount++;
                    accumulated = 0;
                    if (isLast == false) {
                        groupStart = frames.get(i + 1).compressedOffset();
                    }
                }
            }
            return true;
        } catch (IOException e) {
            LOGGER.warn("Failed to read frame index for [{}], falling back", filePath, e);
            return false;
        }
    }

    /**
     * Resolves the {@link StorageProvider} to use for a single-file operation.
     * Returns the hoisted provider if available (non-null), otherwise falls back to the
     * registry: per-config provider for non-empty config, or cached default for empty config.
     */
    private StorageProvider resolveProvider(StoragePath filePath, Map<String, Object> config, @Nullable StorageProvider hoistedProvider) {
        if (hoistedProvider != null) {
            return hoistedProvider;
        }
        if (config != null && config.isEmpty() == false) {
            return storageRegistry.createProvider(filePath.scheme(), settings, config);
        }
        return storageRegistry.provider(filePath);
    }

    /**
     * Groups consecutive block boundary indices into macro-splits, each targeting
     * approximately {@code targetSize} compressed bytes. Returns an array of
     * {@code [firstBlockIndex, lastBlockIndex]} pairs (inclusive).
     */
    static int[][] groupBoundaries(long[] boundaries, long fileLength, long targetSize) {
        if (boundaries.length == 0) {
            return new int[0][];
        }
        if (boundaries.length == 1) {
            return new int[][] { { 0, 0 } };
        }

        List<int[]> groups = new ArrayList<>();
        int groupStart = 0;

        for (int i = 1; i < boundaries.length; i++) {
            long groupSpan = boundaries[i] - boundaries[groupStart];
            if (groupSpan >= targetSize) {
                groups.add(new int[] { groupStart, i - 1 });
                groupStart = i;
            }
        }
        // Last group
        groups.add(new int[] { groupStart, boundaries.length - 1 });

        return groups.toArray(new int[0][]);
    }

    /**
     * Resolves the effective target split size from the config map, falling back to the
     * constructor-provided value. Delegates to {@link ByteSizeValue#parseBytesSizeValue} for
     * unit parsing (accepts {@code "64mb"}, {@code "1gb"}, {@code "1024b"}, etc.).
     * Unitless values (e.g. {@code "1024"}) are rejected — a unit suffix is always required.
     *
     * <p>{@code ByteSizeValue} throws {@link org.elasticsearch.ElasticsearchParseException}
     * on malformed input — an {@link org.elasticsearch.ElasticsearchException} subclass that
     * {@code SplitDiscoveryPhase} already handles without wrapping.
     */
    private long resolveTargetSplitSize(Map<String, Object> config) {
        if (config == null) {
            return targetSplitSizeBytes;
        }
        Object value = config.get(CONFIG_TARGET_SPLIT_SIZE);
        if (value == null) {
            return targetSplitSizeBytes;
        }
        String s = value.toString().trim();
        if (s.isEmpty()) {
            return targetSplitSizeBytes;
        }
        return validateTargetSplitSize(s);
    }

    /**
     * Parses and validates an already-trimmed {@code target_split_size} value, returning the size in
     * bytes. Shared by the query path ({@link #resolveTargetSplitSize}) and the dataset CRUD validator
     * so both accept exactly the same inputs. The caller owns trimming and the null/empty fallback to a
     * default; this method always parses.
     *
     * @throws org.elasticsearch.ElasticsearchParseException if the unit suffix is missing or malformed
     * @throws IllegalArgumentException                      if the resulting size is not positive
     */
    public static long validateTargetSplitSize(String value) {
        long result = ByteSizeValue.parseBytesSizeValue(value, CONFIG_TARGET_SPLIT_SIZE).getBytes();
        Check.isTrue(result > 0, "Invalid value for [{}]: [{}]; must be positive", CONFIG_TARGET_SPLIT_SIZE, value);
        return result;
    }

    /**
     * Returns the Query schema with partition columns removed — those columns' values come from
     * paths, not file bytes, so they don't participate in file-read narrowing.
     */
    static ExternalSchema stripPartitionColumns(ExternalSchema querySchema, @Nullable PartitionMetadata partitionInfo) {
        if (querySchema.isEmpty() || partitionInfo == null || partitionInfo.isEmpty()) {
            return querySchema;
        }
        Set<String> partitionColumns = partitionInfo.partitionColumns().keySet();
        if (partitionColumns.isEmpty()) {
            return querySchema;
        }
        List<Attribute> filtered = new ArrayList<>(querySchema.size());
        for (Attribute attr : querySchema) {
            if (partitionColumns.contains(attr.name()) == false) {
                filtered.add(attr);
            }
        }
        if (filtered.size() == querySchema.size()) {
            return querySchema;
        }
        return new ExternalSchema(filtered);
    }

    /**
     * Returns {@code true} when the file's data columns have zero overlap with the query schema,
     * meaning this file would produce only NULL rows for all needed columns.
     */
    static boolean skipIfNoColumnOverlap(ExternalSchema fileSchema, ExternalSchema querySchema) {
        Set<String> queryNames = querySchema.names();
        for (Attribute attr : fileSchema) {
            if (queryNames.contains(attr.name())) {
                return false;
            }
        }
        return true;
    }

    /**
     * Returns {@code true} when the file can be skipped because a filter conjunct references a
     * column absent from the file and evaluates to UNKNOWN (which becomes FALSE in WHERE context).
     * <p>
     * Only simple leaf predicates are checked: comparisons ({@code =, !=, <, >, <=, >=}),
     * {@link In}, and {@link IsNotNull}. These all evaluate to UNKNOWN/FALSE for a missing column.
     * {@link IsNull} on a missing column evaluates to TRUE (all rows match), so it does NOT
     * trigger a skip.
     * <p>
     * Compound expressions (OR, NOT) and multi-column expressions are conservatively kept.
     *
     * @param filterHints AND-separated filter conjuncts from ancestor FilterExec nodes
     * @param fileColumnNames names of columns present in this file's schema
     * @return {@code true} if the file can be safely skipped
     */
    static boolean skipIfFilterOnMissingColumns(List<Expression> filterHints, Set<String> fileColumnNames) {
        for (Expression conjunct : filterHints) {
            String columnName = extractFilterColumnName(conjunct);
            if (columnName == null) {
                continue;
            }
            if (fileColumnNames.contains(columnName)) {
                continue;
            }
            // Column is missing from this file — determine the skip decision based on predicate type
            if (conjunct instanceof IsNull) {
                // IS NULL on missing column → TRUE (all rows match) → do NOT skip
                continue;
            }
            // All other recognized leaf predicates evaluate to UNKNOWN → FALSE in WHERE context → skip
            return true;
        }
        return false;
    }

    /**
     * Extracts the single column name from a simple leaf predicate, or {@code null} for
     * compound/multi-column expressions that cannot be evaluated for file skipping.
     */
    private static String extractFilterColumnName(Expression expr) {
        if (expr instanceof BinaryComparison bc) {
            String left = extractColumnName(bc.left());
            String right = extractColumnName(bc.right());
            // Only handle single-column leaf predicates (column op literal)
            if (left != null && bc.right() instanceof Literal) {
                return left;
            }
            if (right != null && bc.left() instanceof Literal) {
                return right;
            }
            return null;
        }
        if (expr instanceof In in) {
            return extractColumnName(in.value());
        }
        if (expr instanceof IsNull isNull) {
            return extractColumnName(isNull.field());
        }
        if (expr instanceof IsNotNull isNotNull) {
            return extractColumnName(isNotNull.field());
        }
        return null;
    }

    static boolean matchesPartitionFilters(Map<String, Object> partitionValues, List<Expression> filters) {
        for (Expression filter : filters) {
            Boolean result = evaluateFilter(filter, partitionValues);
            if (result != null && result == false) {
                return false;
            }
        }
        return true;
    }

    static Boolean evaluateFilter(Expression filter, Map<String, Object> partitionValues) {
        return switch (filter) {
            case Equals eq -> evaluateComparison(eq.left(), eq.right(), partitionValues, FileSplitProvider::compareEquals);
            case NotEquals neq -> {
                Boolean result = evaluateComparison(neq.left(), neq.right(), partitionValues, FileSplitProvider::compareEquals);
                yield result != null ? result == false : null;
            }
            case GreaterThanOrEqual gte -> evaluateComparison(gte.left(), gte.right(), partitionValues, (a, b) -> compareValues(a, b) >= 0);
            case GreaterThan gt -> evaluateComparison(gt.left(), gt.right(), partitionValues, (a, b) -> compareValues(a, b) > 0);
            case LessThanOrEqual lte -> evaluateComparison(lte.left(), lte.right(), partitionValues, (a, b) -> compareValues(a, b) <= 0);
            case LessThan lt -> evaluateComparison(lt.left(), lt.right(), partitionValues, (a, b) -> compareValues(a, b) < 0);
            case In in -> {
                String columnName = extractColumnName(in.value());
                if (columnName == null || partitionValues.containsKey(columnName) == false) {
                    yield null;
                }
                Object partitionValue = partitionValues.get(columnName);
                if (partitionValue == null) {
                    yield null;
                }
                Boolean found = false;
                for (Expression listItem : in.list()) {
                    if (listItem instanceof Literal lit) {
                        if (compareEquals(partitionValue, lit.value())) {
                            found = true;
                            break;
                        }
                    } else {
                        yield null;
                    }
                }
                yield found;
            }
            case IsNull isNull -> {
                String columnName = extractColumnName(isNull.field());
                if (columnName == null || partitionValues.containsKey(columnName) == false) {
                    yield null;
                }
                yield partitionValues.get(columnName) == null;
            }
            case IsNotNull isNotNull -> {
                String columnName = extractColumnName(isNotNull.field());
                if (columnName == null || partitionValues.containsKey(columnName) == false) {
                    yield null;
                }
                yield partitionValues.get(columnName) != null;
            }
            case And and -> nullableAnd(evaluateFilter(and.left(), partitionValues), evaluateFilter(and.right(), partitionValues));
            case Or or -> nullableOr(evaluateFilter(or.left(), partitionValues), evaluateFilter(or.right(), partitionValues));
            case Not not -> nullableNot(evaluateFilter(not.field(), partitionValues));
            default -> null;
        };
    }

    private static Boolean nullableAnd(Boolean a, Boolean b) {
        if (Boolean.FALSE.equals(a) || Boolean.FALSE.equals(b)) {
            return false;
        }
        if (a == null || b == null) {
            return null;
        }
        return a && b;
    }

    private static Boolean nullableOr(Boolean a, Boolean b) {
        if (Boolean.TRUE.equals(a) || Boolean.TRUE.equals(b)) {
            return true;
        }
        if (a == null || b == null) {
            return null;
        }
        return false;
    }

    private static Boolean nullableNot(Boolean a) {
        return a == null ? null : a == false;
    }

    private static Boolean evaluateComparison(
        Expression left,
        Expression right,
        Map<String, Object> partitionValues,
        BiFunction<Object, Object, Boolean> comparator
    ) {
        String columnName = extractColumnName(left);
        Object literalValue = extractLiteralValue(right);
        if (columnName != null && literalValue != null && partitionValues.containsKey(columnName)) {
            Object partitionValue = partitionValues.get(columnName);
            // `column OP literal`
            return partitionValue != null ? comparator.apply(partitionValue, literalValue) : null;
        }
        columnName = extractColumnName(right);
        literalValue = extractLiteralValue(left);
        if (columnName != null && literalValue != null && partitionValues.containsKey(columnName)) {
            Object partitionValue = partitionValues.get(columnName);
            // `literal OP column` — the operands keep their sides. Passing the column first would evaluate
            // `column OP literal`, which for an asymmetric operator is the exact inverse: `2024 > year` would be
            // tested as `year > 2024` and prune precisely the files that match. LiteralsOnTheRight normalizes this
            // shape away before we ever see it, so the bug is unreachable today — but the matcher must not depend on
            // an optimizer rule it has no way to enforce.
            return partitionValue != null ? comparator.apply(literalValue, partitionValue) : null;
        }
        return null;
    }

    private static String extractColumnName(Expression expr) {
        return switch (expr) {
            case FieldAttribute fa -> fa.name();
            case NamedExpression ne -> ne.name();
            default -> null;
        };
    }

    private static Object extractLiteralValue(Expression expr) {
        return switch (expr) {
            case Literal lit -> lit.value();
            default -> null;
        };
    }

    /**
     * String form of a partition value or filter literal. Keyword partition values arrive as Java {@code String}
     * (from {@code HivePartitionDetector.castValue}) while an ES|QL keyword literal is a Lucene {@code BytesRef}
     * whose {@code toString()} is a hex dump — so a raw {@code toString()} comparison of the two never matches.
     * {@link BytesRefs#toString(Object)} UTF8-decodes a {@code BytesRef} and falls back to {@code toString()}
     * otherwise, so both sides normalize to the same text before any string compare or numeric parse.
     */
    private static String stringOf(Object value) {
        return BytesRefs.toString(value);
    }

    private static boolean compareEquals(Object a, Object b) {
        if (a == null || b == null) {
            return false;
        }
        if (a instanceof Number na && b instanceof Number nb) {
            return compareNumbers(na, nb) == 0;
        }
        return stringOf(a).equals(stringOf(b));
    }

    private static int compareValues(Object a, Object b) {
        if (a == null || b == null) {
            throw new IllegalArgumentException("Cannot compare null partition values");
        }
        if (a instanceof Number na && b instanceof Number nb) {
            return compareNumbers(na, nb);
        }
        // Coerce mixed Number/text cases: a partition value may be stored as "2024" (String) while the literal from
        // the filter is Integer 2024, or vice versa. Only when exactly one side is already a Number — two text values
        // are compared as text, so a KEYWORD partition never has "0123" and "123" collapse into the same value.
        if (a instanceof Number na) {
            Number nb = parseNumber(stringOf(b));
            return nb != null ? compareNumbers(na, nb) : keywordCompare(a, b);
        }
        if (b instanceof Number nb) {
            Number na = parseNumber(stringOf(a));
            return na != null ? compareNumbers(na, nb) : keywordCompare(a, b);
        }
        return keywordCompare(a, b);
    }

    /**
     * Orders two numeric values. Integral types are compared as {@code long}, never as {@code double}: above
     * 2^53 a {@code double} cannot separate adjacent longs, so an epoch-micros or snowflake-id partition value
     * would compare <em>equal</em> to its neighbour. That is not a rounding nit — it makes the matcher return a
     * confident {@code false} for {@code ts != <adjacent>} and prune a file whose every row matches the filter.
     */
    private static int compareNumbers(Number a, Number b) {
        if (isIntegral(a) && isIntegral(b)) {
            return Long.compare(a.longValue(), b.longValue());
        }
        return Double.compare(a.doubleValue(), b.doubleValue());
    }

    private static boolean isIntegral(Number n) {
        return n instanceof Long || n instanceof Integer || n instanceof Short || n instanceof Byte;
    }

    /** The text parsed as a number, or {@code null} if it is not numeric. */
    private static Number parseNumber(String text) {
        try {
            return Long.valueOf(text);
        } catch (NumberFormatException notALong) {
            try {
                return Double.valueOf(text);
            } catch (NumberFormatException notANumber) {
                return null;
            }
        }
    }

    /**
     * Orders two non-numeric values the way ES|QL orders keywords: by UTF-8 bytes, which is code-point order.
     * {@link String#compareTo} would order by UTF-16 code units instead, and the two disagree whenever one side is a
     * supplementary-plane character (a folder named {@code region=<emoji>}) and the other sits in {@code U+E000..U+FFFF}
     * — the surrogate compares low, the engine compares it high, and a range predicate would prune a matching file.
     */
    private static int keywordCompare(Object a, Object b) {
        return new BytesRef(stringOf(a)).compareTo(new BytesRef(stringOf(b)));
    }
}
