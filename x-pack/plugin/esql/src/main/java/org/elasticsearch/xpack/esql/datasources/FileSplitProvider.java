/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.logging.HeaderWarning;
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
import org.elasticsearch.xpack.esql.datasources.cache.StorageProviderCache;
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

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
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
 *       See {@link #newlineMacroSplitCandidate} and {@link #buildNewlineMacroSplits}.</li>
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

    /**
     * Config for a split that covers an entire file, so it is both the first and the last split of that file.
     * <p>
     * Readers run a split-boundary protocol off these flags: a non-first split drops its leading partial
     * record and a non-last split drops its trailing one, because the neighbouring split owns those bytes.
     * A whole-file split has no neighbours, so leaving the flags unstamped makes a reader discard a final
     * record that is not newline-terminated — no other split will read it. Multi-split paths stamp the same
     * keys on their edge splits, so after this every line-oriented split states its own position rather than
     * leaving a whole-file read to be inferred from an absent key. Range splits are the exception: they share
     * one config across all ranges and carry no position keys, because byte ranges are not a record-boundary
     * protocol and their readers never consult these flags.
     */
    private static Map<String, Object> wholeFileSplitConfig(Map<String, Object> config) {
        Map<String, Object> splitConfig = new HashMap<>(config);
        splitConfig.put(FIRST_SPLIT_KEY, "true");
        splitConfig.put(LAST_SPLIT_KEY, "true");
        return splitConfig;
    }

    /**
     * The single split covering a whole file, stamped as both first and last per {@link #wholeFileSplitConfig}.
     * Every path in this class that gives up on splitting a file ends here, so they all stamp the same way.
     */
    private static FileSplit wholeFileSplit(
        StoragePath filePath,
        long fileLength,
        @Nullable String format,
        Map<String, Object> config,
        Map<String, Object> partitionValues,
        @Nullable ColumnMapping columnMapping,
        @Nullable List<Attribute> readSchema
    ) {
        return FileSplit.withReadSchema(
            "file",
            filePath,
            0,
            fileLength,
            format,
            wholeFileSplitConfig(config),
            partitionValues,
            columnMapping,
            readSchema
        );
    }

    static final String RANGE_SPLIT_KEY = "_range_split";
    static final String FILE_LENGTH_KEY = "_file_length";
    public static final String CONFIG_TARGET_SPLIT_SIZE = "target_split_size";

    /**
     * Bytes one record-boundary probe may read, defaulting to the width in
     * {@code RecordBoundaryProbe.DEFAULT_SPLIT_PROBE_WINDOW}. It is a property of the data rather than of the
     * node: how wide a window a probe needs follows from how long the dataset's records are, so a dataset whose
     * records outgrow the default raises it here and every query over that dataset resolves the offsets the
     * default lost.
     * <p>
     * It bounds the probes of a strided scan (NDJSON, plain CSV/TSV). Quoted or escaped CSV/TSV is not probed at
     * a fixed offset at all but walked, and what bounds that walk is the splitter's own convergence window
     * together with {@code external_max_record_size}; neither of those is this key.
     * <p>
     * It is independent of {@link #CONFIG_MAX_SPLIT_PROBES}, and the two multiply into the bytes a strided query
     * may read while probing, which {@link #MAX_PROBE_BUDGET_BYTES} caps. Below roughly 136kb a probe transfers
     * its whole window rather than abandoning it, because draining what is left of a window that small costs less
     * than the handshake a fresh connection pays, so lowering this key past that point raises the bytes a probe
     * moves instead of lowering them.
     */
    public static final String CONFIG_SPLIT_PROBE_WINDOW = "split_probe_window";

    /**
     * Record-boundary probes a query may issue, and so the macro-splits its files may be cut into, defaulting
     * to the count in {@code DEFAULT_MAX_SPLIT_PROBES}. A scan large enough to want more stop points than it allows has its
     * stride widened to fit them, so raising it is what gets the requested stride on a very large scan.
     */
    public static final String CONFIG_MAX_SPLIT_PROBES = "max_split_probes";

    /**
     * Configuration keys this splitter consumes from a query-time configuration map. Aggregated by
     * {@link FileSourceFactory#COORDINATOR_KEYS}. New keys read by this class via {@code config.get(...)}
     * must be added here so the {@link org.elasticsearch.xpack.esql.datasources.spi.ConfigKeyValidator}
     * recognises them — pinned by {@code FileSourceFactoryValidationTests}.
     */
    public static final Set<String> CONFIG_KEYS = Set.of(CONFIG_TARGET_SPLIT_SIZE, CONFIG_SPLIT_PROBE_WINDOW, CONFIG_MAX_SPLIT_PROBES);

    /**
     * Macro-split starts on a newline-aligned record boundary (see {@link #buildNewlineMacroSplits}).
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

    /**
     * Ceiling on concurrent I/O tasks during split discovery, applied separately to the per-file planning pass
     * (Parquet footer reads, etc.) and to the record-boundary probes that follow it. The two passes run one after
     * the other, so this bounds in-flight reads at any instant rather than being multiplied between them.
     */
    static final int MAX_PARALLEL_SPLIT_DISCOVERY = 16;

    /**
     * Default ceiling on the record-boundary probes one query may issue, and so on the macro-splits that come
     * out of them; a dataset that needs a different one sets {@link #CONFIG_MAX_SPLIT_PROBES}. Probing is the
     * only part of split discovery that costs a read per split it produces, and a file's offsets are all
     * materialized before any of them is read, so an unbounded count costs both planning latency and
     * planning-time heap.
     * <p>
     * The budget covers the query rather than a single file because the probes of every file are pooled into
     * one batch: a per-file ceiling would be multiplied by the number of files. A file too small to be cut at
     * the stride is outside the budget entirely, since it costs no probe and yields the one whole-file split
     * that {@code esql.external.max_discovered_files} already bounds.
     * <p>
     * It bounds the reads a query issues, not the bytes they move: a probe's cost in bytes is set by the window
     * it opens, which {@link #CONFIG_SPLIT_PROBE_WINDOW} bounds instead. The two multiply, so a strided query
     * reads up to this count times that window while probing, which {@link #MAX_PROBE_BUDGET_BYTES} caps.
     */
    static final int DEFAULT_MAX_SPLIT_PROBES = 1_000;

    /**
     * Ceiling accepted for {@link #CONFIG_MAX_SPLIT_PROBES}. The count is what materializes a query's offsets,
     * each carrying a probe task, a result slot and a listener before any read is issued, so an unbounded count
     * spends coordinator heap during planning with no circuit breaker behind it. Ten thousand of them is a few
     * megabytes, which is the point: the bound is where the transient cost stops being ignorable, well above the
     * counts a very large scan asks for.
     * <p>
     * It is also what keeps a query's total offset count inside an {@code int}: the stride is widened to fit the
     * budget, so no query accumulates more offsets than this however many files it has.
     */
    static final int MAX_SPLIT_PROBES_CEILING = 10_000;

    /**
     * Ceiling on the bytes one query may read while probing, which is {@link #CONFIG_MAX_SPLIT_PROBES} times
     * {@link #CONFIG_SPLIT_PROBE_WINDOW}. The keys are independent, so neither alone says what a query costs;
     * this bounds the product they form. A dataset wanting a window wider than this divided by its probe count
     * has to lower the count to get it, which is the trade a shared budget exists to make explicit.
     * <p>
     * Sized to leave both keys usable well past their defaults, since a value that forced one down every time the
     * other went up would contradict the advice to size the window from the dataset's longest record and the
     * count from the splits the scan needs. What it is there to stop is the two extremes multiplied: at the
     * {@link #MAX_SPLIT_PROBES_CEILING} count and a window the size of a whole record, the reads would otherwise
     * run to hundreds of gigabytes.
     */
    static final long MAX_PROBE_BUDGET_BYTES = ByteSizeValue.ofGb(4).getBytes();

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

        // Also validates the optional {@link #CONFIG_TARGET_SPLIT_SIZE} when users pass WITH options. Every file
        // of a query is planned against the same config, so the requested stride is resolved once here.
        long requestedStrideBytes = resolveTargetSplitSize(config);
        // The two probe settings, resolved here for the same reason and validated the same way. They form one
        // budget, so the pair is checked once both are known rather than by either key's own parser.
        int maxSplitProbes = resolveMaxSplitProbes(config);
        long probeWindowBytes = resolveSplitProbeWindow(config);
        validateProbeBudget(probeWindowBytes, maxSplitProbes);

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

        try {
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
            // Tracks whether any file was dropped by the row-count-unsafe no-column-overlap heuristic:
            // such a file still contributes rows to COUNT(*), so an all-dropped result driven by it is
            // NOT an exhaustive prune and must fall back to a full read (see SplitDiscoveryResult).
            boolean droppedByColumnOverlap = false;
            // Bytes of the files that will be cut at a stride, which are the only ones that cost probe reads and so
            // the only ones the probe budget is shared between. See strideBoundedByProbeBudget.
            long probedFileBytes = 0;
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
                        // Row-count-unsafe: the file's rows still exist (they would be all-NULL for the query's
                        // columns), so COUNT(*) and other row-count-sensitive queries need them. Skipping is a
                        // best-effort optimization that relies on a full-read fallback when it empties the plan.
                        droppedByColumnOverlap = true;
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
                // A file at or below the stride is not cut at all, and an unlisted length reads as 0, so both fall
                // through to a whole-file split without contending for the budget. The extension is as much as is
                // known here; a file whose reader turns out not to be splittable only makes the stride wider than
                // it needed to be.
                if (fileLength > requestedStrideBytes && isNewlineMacroSplitCandidateExtension(format)) {
                    probedFileBytes += fileLength;
                }

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
                // Every file was dropped. Only a resolved, non-empty file list whose files were all removed by
                // row-count-preserving filter contradictions (no no-overlap heuristic among them) is a true
                // exhaustive prune the phase may trust to read nothing; otherwise fall back to a full read.
                boolean exhaustivelyPruned = fileList.fileCount() > 0 && droppedByColumnOverlap == false;
                return new SplitDiscoveryResult(List.of(), 0, exhaustivelyPruned);
            }

            // Phase 2: I/O-bound split planning, parallelized across files when an executor is available. Files
            // whose record boundaries still need probing come back as deferred descriptors; everything else
            // (Parquet footers, block-aligned compressed, range splits, whole-file) finishes here.
            final StorageProvider hoistedProvider = sharedProvider;
            final BooleanSupplier isCancelled = context.isCancelled();
            final long strideBytes = strideBoundedByProbeBudget(requestedStrideBytes, probedFileBytes, maxSplitProbes);
            if (strideBytes > requestedStrideBytes) {
                // The setting being overridden is the query's, not the cluster's, so this goes to the query's
                // response rather than the node log.
                HeaderWarning.addWarning(
                    "[{}] of [{}] would probe more than {} record boundaries across [{}] of files; using [{}] instead. "
                        + "A larger [{}] allows the requested size",
                    CONFIG_TARGET_SPLIT_SIZE,
                    ByteSizeValue.ofBytes(requestedStrideBytes),
                    maxSplitProbes,
                    ByteSizeValue.ofBytes(probedFileBytes),
                    ByteSizeValue.ofBytes(strideBytes),
                    CONFIG_MAX_SPLIT_PROBES
                );
            }
            List<PlanResult> planResults;
            try {
                if (executor != null && tasks.size() > 1) {
                    planResults = BoundedParallelGather.gather(
                        tasks,
                        task -> processFileForSplits(task, hoistedProvider, strideBytes, isCancelled),
                        splitDiscoveryConcurrency(),
                        executor
                    );
                } else {
                    planResults = new ArrayList<>(tasks.size());
                    for (FileTask task : tasks) {
                        planResults.add(processFileForSplits(task, hoistedProvider, strideBytes, isCancelled));
                    }
                }
            } catch (IOException e) {
                throw new RuntimeException("Failed to discover splits", e);
            } catch (RuntimeException e) {
                throw e;
            } catch (Exception e) {
                throw new RuntimeException("Failed to discover splits", e);
            }

            // Phase 3: probe the deferred files' record boundaries. Every deferred file's stride offsets go into one
            // flat batch under a single concurrency budget, so the number of in-flight probe reads is bounded by that
            // budget no matter how many files are being probed. Probing per file instead would multiply the per-file
            // budget by the number of files in flight.
            Map<DeferredNewlineSplits, List<RecordBoundaryProbe.Outcome>> probedOutcomes = probeDeferredBoundaries(
                planResults,
                probeWindowBytes,
                isCancelled
            );

            // Phase 4: turn the plan results into splits, now that every boundary either was known at planning time
            // or has been probed.
            List<ExternalSplit> splits = splitsFromPlanResults(planResults, probedOutcomes, probeWindowBytes);

            // Each surviving task produces at least one split, so the task count is the number of
            // distinct files that are actually scanned after coordinator-side pruning.
            return new SplitDiscoveryResult(splits, tasks.size());
        } finally {
            StorageProviderCache.closeLease(sharedProvider);
        }
    }

    /**
     * Assembles the query's splits from the planned files and the probed boundaries, and reports what the
     * boundary search failed to cut.
     * <p>
     * Splits come out in file order: walking the plan results keeps a probed file's macro-splits in the position
     * its file occupied in the file list.
     *
     * @param probeWindowBytes the configured probe window, which the shortfall report needs because it is one of
     *                         the bounds a probe read may have stopped at and the only one not reachable from a
     *                         plan result
     */
    private static List<ExternalSplit> splitsFromPlanResults(
        List<PlanResult> planResults,
        Map<DeferredNewlineSplits, List<RecordBoundaryProbe.Outcome>> probedOutcomes,
        long probeWindowBytes
    ) {
        List<ExternalSplit> splits = new ArrayList<>();
        SplitShortfall shortfall = new SplitShortfall(probeWindowBytes);
        for (PlanResult planResult : planResults) {
            switch (planResult) {
                case PlanResult.Splits planned -> splits.addAll(planned.splits());
                case PlanResult.NeedsProbing needsProbing -> {
                    DeferredNewlineSplits deferred = needsProbing.deferred();
                    List<RecordBoundaryProbe.Outcome> outcomes = probedOutcomes.get(deferred);
                    if (outcomes == null) {
                        // A file is only deferred when it has offsets to probe, so the probe phase answers for
                        // every deferred file. Fail loud rather than on a null if that ever stops holding.
                        throw new IllegalStateException("no probed boundaries for deferred file " + deferred.task().filePath());
                    }
                    List<Long> starts = RecordBoundaryProbe.reduce(outcomes);
                    shortfall.recordProbed(deferred, outcomes, starts);
                    splits.addAll(buildNewlineMacroSplits(deferred, starts));
                }
                case PlanResult.Walked walked -> {
                    shortfall.recordWalk(walked);
                    splits.addAll(buildNewlineMacroSplits(walked.deferred(), walked.starts()));
                }
            }
        }
        shortfall.warnIfAny();
        return splits;
    }

    /**
     * How much less of a query got cut than was asked for, because the boundary search found nothing where it
     * was to cut.
     * <p>
     * It counts a shortfall rather than diagnosing one, since the several ways an offset comes back empty are
     * not distinguishable from here and mostly not distinguishable at all: a record longer than the search
     * reads, a terminator on the last byte of a window that stopped short of end-of-file, a walk reaching
     * end-of-file with no record start left to prove. What they have in common is the only thing the caller can
     * act on, which is that the file was cut into fewer pieces than the stride asked for.
     * <p>
     * The interesting case is partial. A scan of records a little wider than a probe window loses most of its
     * offsets but keeps some, so it comes back with a fraction of the splits it asked for, and nothing about the
     * splits themselves says so: whoever wonders why the query is slow has only the count, and no reason to think
     * that count is not the one they requested. Reporting only the files that lost every offset would say nothing
     * about that case, and the total loss falls out of the same tally anyway.
     * <p>
     * The tally is per query rather than per file because the cause is a property of the dataset: a scan whose
     * records are too wide is too wide on every file it has, and one warning per file would bury the point.
     * <p>
     * A file that lost a little is not tallied at all. A warning that fires when one offset of a thousand came
     * back empty is a warning people learn to skip, and then it is gone when the same sentence means the file was
     * read whole. So a file is only counted once what it lost is worth acting on; see
     * {@link #SIGNIFICANT_SHORTFALL_FRACTION}.
     */
    private static final class SplitShortfall {

        /**
         * The share of a file that has to go uncut before the file is worth reporting: a tenth of its probe
         * offsets finding nothing, or a tenth of its bytes left on the span a stopped walk gave up in. A file
         * read whole is always reported whatever the fraction says, since it got no parallelism at all and that
         * is the case the warning most needs to survive for.
         */
        private static final double SIGNIFICANT_SHORTFALL_FRACTION = 0.1;

        /**
         * Name of the query pragma that bounds one record, which is what a stopped walk ran into. Held as a
         * literal because this package does not depend on the one declaring the pragma;
         * {@code QueryPragmas.MAX_RECORD_SIZE} is its definition.
         */
        private static final String MAX_RECORD_SIZE_PRAGMA = "external_max_record_size";

        /**
         * The configured probe window, one of the three bounds a probe's read is the smallest of. The other two
         * are read off the example file; this one is resolved per query and never reaches a plan result.
         */
        private final long probeWindowBytes;

        private long offsetsProbed;
        private long offsetsWithoutBoundary;
        private int filesAffected;
        private int filesReadWhole;
        private int filesProbed;
        private int filesWalked;
        private DeferredNewlineSplits firstAffected;

        SplitShortfall(long probeWindowBytes) {
            this.probeWindowBytes = probeWindowBytes;
        }

        /** Tallies a strided file, whose every offset that found nothing is one split the query did not get. */
        void recordProbed(DeferredNewlineSplits deferred, List<RecordBoundaryProbe.Outcome> outcomes, List<Long> starts) {
            long missing = outcomes.stream().filter(outcome -> outcome.kind() == RecordBoundaryProbe.Outcome.Kind.NONE).count();
            if (missing == 0) {
                // Nothing was lost. A file that comes out of this whole was cut the way its length and the stride
                // meant it to be, which is not a shortfall and must not be reported as one.
                return;
            }
            boolean readWhole = starts.size() <= 1;
            if (readWhole == false && missing < outcomes.size() * SIGNIFICANT_SHORTFALL_FRACTION) {
                return;
            }
            // Counted only for a file the warning is about, so the ratio reported describes those files. Counting
            // every probed file would put the offsets of files that came out whole in the denominator, and a
            // query that lost only walked files would report none missing of many.
            offsetsProbed += outcomes.size();
            offsetsWithoutBoundary += missing;
            note(deferred, readWhole, true);
        }

        /**
         * Tallies a sequentially walked file. The walk stops at the record it cannot get past rather than
         * skipping it, so one such record costs every boundary after it and there is no offset count to
         * report; what it cost is the rest of the file, which is what decides whether it is worth reporting.
         */
        void recordWalk(PlanResult.Walked walked) {
            if (walked.stoppedBeforeEndOfFile() == false) {
                return;
            }
            List<Long> starts = walked.starts();
            boolean readWhole = starts.size() <= 1;
            long fileLength = walked.deferred().task().fileLength();
            long uncut = fileLength - starts.getLast();
            if (readWhole == false && uncut < fileLength * SIGNIFICANT_SHORTFALL_FRACTION) {
                return;
            }
            note(walked.deferred(), readWhole, false);
        }

        /**
         * @param probed whether the file's boundaries were probed at fixed offsets or walked, which decides
         *               which setting can recover them
         */
        private void note(DeferredNewlineSplits deferred, boolean readWhole, boolean probed) {
            filesAffected++;
            if (readWhole) {
                filesReadWhole++;
            }
            if (probed) {
                filesProbed++;
            } else {
                filesWalked++;
            }
            if (firstAffected == null) {
                firstAffected = deferred;
            }
        }

        /**
         * Reports the shortfall to the query's response rather than the node log: the remedies are dataset
         * settings and a query pragma, all of which belong to whoever ran the query rather than to the operator
         * of the node that would see the log.
         */
        void warnIfAny() {
            if (firstAffected == null) {
                return;
            }
            // Every file of a query is cut at the same stride, so the one named here is the stride of all of
            // them. It is the stride they were cut at rather than the one the query asked for, which differ when
            // the probe count widened it.
            HeaderWarning.addWarning(
                "{} file(s) were cut into fewer splits than [{}] of [{}] would give, because no record boundary could be found "
                    + "where they were to be cut{}; {} of them are read as a single whole-file split; e.g., [{}] ({}). {}",
                filesAffected,
                CONFIG_TARGET_SPLIT_SIZE,
                ByteSizeValue.ofBytes(firstAffected.strideBytes()),
                probedOffsetDetail(),
                filesReadWhole,
                firstAffected.task().filePath(),
                ByteSizeValue.ofBytes(firstAffected.task().fileLength()),
                remedy()
            );
        }

        /**
         * What to change, which differs by how the affected files were cut. A probed file's boundaries come from
         * a bounded read, so what recovers them is whichever bound stopped that read; see {@link #probeBound()}.
         * A walked file reads none of those bounds: quoted and escaped records cannot be probed at a fixed
         * offset, so they are walked instead, and the walk stops at a record the reader will not span. Naming a
         * probe's bound to that user sends them to a setting that cannot move their outcome, which is why the two
         * get different sentences and a query that lost both kinds gets both.
         */
        private String remedy() {
            if (filesWalked == 0) {
                return Strings.format(
                    "Records longer than the bytes a probe reads are the usual cause: a probe reads at most %s, and a "
                        + "value above the longest record may recover the splits",
                    probeBound()
                );
            }
            if (filesProbed == 0) {
                return Strings.format(
                    "Quoted or escaped records are walked rather than probed at a fixed offset, and the walk stops "
                        + "at a record longer than [%s] of [%s]: raising it above the longest record may recover "
                        + "the splits",
                    MAX_RECORD_SIZE_PRAGMA,
                    maxRecordSize()
                );
            }
            return Strings.format(
                "%d of them were probed, where records longer than the bytes a probe reads are the usual cause: a probe "
                    + "reads at most %s, and a value above the longest record may recover their splits. The other %d hold "
                    + "quoted or escaped records, which are walked rather than probed, and the walk stops at a record "
                    + "longer than [%s] of [%s]: raising it above the longest record may recover theirs",
                filesProbed,
                probeBound(),
                filesWalked,
                MAX_RECORD_SIZE_PRAGMA,
                maxRecordSize()
            );
        }

        /**
         * The record bound the walk ran into. Read off the example file, which is any affected file, because the
         * pragma applies to the whole query and so every file of it carries the same value.
         */
        private ByteSizeValue maxRecordSize() {
            return ByteSizeValue.ofBytes(firstAffected.task().maxRecordBytes());
        }

        /**
         * How far a probe read, and which setting stopped it there. A probe's window is the smallest of the query's
         * record cap, its stride and its configured probe window, so only the smallest of the three can recover a
         * boundary: raising either of the others leaves the read where it was. Naming the bound rather than the
         * settings a probe generally reads is what keeps the advice actionable for a query that has already raised
         * one of them past the one that binds.
         * <p>
         * Every one of the three is a property of the query rather than of a file, so they are read off the example
         * file for the same reason the stride in the warning is.
         * <p>
         * Two of them can tie, and then both are named, because moving either alone leaves the minimum where it
         * was. What is left out is the fourth term of the window, the bytes remaining before end-of-file, which is
         * a property of the offset rather than something a query can set.
         */
        private String probeBound() {
            long maxRecordBytes = firstAffected.task().maxRecordBytes();
            long strideBytes = firstAffected.strideBytes();
            long bound = Math.min(maxRecordBytes, RecordBoundaryProbe.gridWindow(strideBytes, probeWindowBytes));
            List<String> keys = new ArrayList<>(3);
            if (probeWindowBytes == bound) {
                keys.add("[" + CONFIG_SPLIT_PROBE_WINDOW + "]");
            }
            if (strideBytes == bound) {
                keys.add("[" + CONFIG_TARGET_SPLIT_SIZE + "]");
            }
            if (maxRecordBytes == bound) {
                keys.add("[" + MAX_RECORD_SIZE_PRAGMA + "]");
            }
            return Strings.format("[%s], bounded by %s", ByteSizeValue.ofBytes(bound), String.join(" and ", keys));
        }

        /**
         * How many of the affected files' probe offsets came back empty, which only a strided file has. A query
         * whose affected files were all walked sequentially has no offsets to report, and "0 of 0" would read as
         * the opposite of what it means.
         */
        private String probedOffsetDetail() {
            if (offsetsProbed == 0) {
                return "";
            }
            return Strings.format(" (%d of %d probe offsets found no record boundary)", offsetsWithoutBoundary, offsetsProbed);
        }
    }

    /**
     * Probes the record boundaries of every deferred file, keyed by the descriptor they belong to.
     * <p>
     * With an executor, all files' stride offsets are gathered into one flat batch so a single concurrency budget
     * ({@link #splitDiscoveryConcurrency()}) bounds the in-flight probe reads across the whole query, rather than
     * one budget per file multiplied by the files in flight. Without one, each file is walked serially. Both
     * produce the same per-offset outcomes; the caller reduces them to split starts.
     * <p>
     * Keying on the descriptor's identity rather than on a file's position in {@code planResults} is what keeps
     * the two phases from having to agree on an ordering: a probe task already carries the descriptor it
     * contributes to, so filtering or reordering the plan results between planning and probing cannot hand one
     * file another file's boundaries.
     *
     * @param probeWindowBytes the bytes each of these probes may read, from {@link #CONFIG_SPLIT_PROBE_WINDOW}
     */
    private Map<DeferredNewlineSplits, List<RecordBoundaryProbe.Outcome>> probeDeferredBoundaries(
        List<PlanResult> planResults,
        long probeWindowBytes,
        BooleanSupplier isCancelled
    ) {
        List<DeferredNewlineSplits> deferredFiles = new ArrayList<>();
        int probeCount = 0;
        for (PlanResult planResult : planResults) {
            if (planResult instanceof PlanResult.NeedsProbing needsProbing) {
                deferredFiles.add(needsProbing.deferred());
                probeCount += needsProbing.deferred().positions().size();
            }
        }
        if (probeCount == 0) {
            return Map.of();
        }
        // Every file's stride was widened to fit the query's probe budget, which MAX_SPLIT_PROBES_CEILING bounds,
        // so the pooled count cannot approach the range of the int it is held in.
        assert probeCount <= MAX_SPLIT_PROBES_CEILING : "pooled probe count [" + probeCount + "] above the ceiling";
        // A cancel between planning and probing should be seen before any probe read is issued.
        if (isCancelled.getAsBoolean()) {
            throw new TaskCancelledException(RecordBoundaryProbe.CANCELLED_MESSAGE);
        }
        try {
            Map<DeferredNewlineSplits, List<RecordBoundaryProbe.Outcome>> outcomesByFile = new IdentityHashMap<>(deferredFiles.size());
            if (executor == null) {
                for (DeferredNewlineSplits deferred : deferredFiles) {
                    outcomesByFile.put(
                        deferred,
                        RecordBoundaryProbe.stridedOutcomes(
                            deferred.splitter(),
                            deferred.storageObject(),
                            deferred.task().fileLength(),
                            deferred.positions(),
                            deferred.minSegment(),
                            deferred.strideBytes(),
                            deferred.task().maxRecordBytes(),
                            probeWindowBytes,
                            isCancelled
                        )
                    );
                }
            } else {
                List<ProbeTask> probeTasks = new ArrayList<>(probeCount);
                for (DeferredNewlineSplits deferred : deferredFiles) {
                    for (long position : deferred.positions()) {
                        probeTasks.add(new ProbeTask(deferred, position));
                    }
                }
                List<RecordBoundaryProbe.Outcome> outcomes = BoundedParallelGather.gather(
                    probeTasks,
                    probe -> runProbe(probe, probeWindowBytes, isCancelled),
                    splitDiscoveryConcurrency(),
                    executor
                );

                // Results come back in input order, and each file's offsets were added in ascending order, so
                // grouping by file preserves the ascending order the reduction requires.
                for (int i = 0; i < probeTasks.size(); i++) {
                    outcomesByFile.computeIfAbsent(probeTasks.get(i).deferred(), k -> new ArrayList<>()).add(outcomes.get(i));
                }
            }
            // A cancel landing after the last probe has read is seen by no probe, so check once more here rather
            // than returning a split set the caller will discard.
            if (isCancelled.getAsBoolean()) {
                throw new TaskCancelledException(RecordBoundaryProbe.CANCELLED_MESSAGE);
            }
            return outcomesByFile;
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException("Failed to discover splits", e);
        }
    }

    /**
     * Probes the one stride offset a task carries, against the file the task belongs to.
     * <p>
     * Carries the cancellation signal as ambient thread-local state so the synchronous retry/throttle backoff
     * inside the probe read can abort a parked sleep on cancel. The scope is thread-local and a probe runs on
     * whichever gather thread picks it up, so it is installed per probe rather than once around the gather.
     */
    private static RecordBoundaryProbe.Outcome runProbe(ProbeTask probe, long windowBytes, BooleanSupplier isCancelled) throws IOException {
        DeferredNewlineSplits deferred = probe.deferred();
        return StorageRetryCancellation.callWithCancellation(
            isCancelled,
            () -> RecordBoundaryProbe.probeAt(
                deferred.splitter(),
                deferred.storageObject(),
                probe.position(),
                deferred.task().fileLength(),
                deferred.minSegment(),
                deferred.task().maxRecordBytes(),
                RecordBoundaryProbe.gridWindow(deferred.strideBytes(), windowBytes),
                isCancelled
            )
        );
    }

    /**
     * How many split-discovery reads may be in flight at once across the whole query, governing both the per-file
     * planning pass and the boundary probes that follow it.
     * <p>
     * Bounded by {@link #MAX_PARALLEL_SPLIT_DISCOVERY}, and clamped to the node's blob-store concurrency because
     * a planning read and a probe alike hold one of those permits for as long as their stream is open: asking for
     * more in flight than there are permits buys a query nothing but a thread parked on the semaphore. The clamp
     * is per query where the permits are per node and per scheme, so it bounds one query's contribution to that
     * contention rather than the contention itself, and concurrent queries still queue against each other.
     * A configured concurrency of {@code 0} disables permit limiting altogether rather than meaning "no
     * concurrency", so the ceiling applies as-is.
     * <p>
     * These reads block the thread they run on, which is {@link #executor}'s. That executor must not be the pool
     * the caller of split discovery is itself running on: the caller waits for this fan-out to finish, so drawing
     * both from one bounded pool would let a thread wait on work only that pool can run. Production keeps them
     * apart, {@code GENERIC} here against the {@code esql_external_io} thread the resolver calls in on.
     */
    int splitDiscoveryConcurrency() {
        int permits = ExternalSourceSettings.blobStoreConcurrency(settings);
        return permits > 0 ? Math.min(MAX_PARALLEL_SPLIT_DISCOVERY, permits) : MAX_PARALLEL_SPLIT_DISCOVERY;
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
            throw new TaskCancelledException(RecordBoundaryProbe.CANCELLED_MESSAGE);
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

    /**
     * A file that will be macro-split at record boundaries, carrying what the {@link FileTask} does not: how to
     * read the file's bytes, how to recognise a record in them, and where to look.
     * <p>
     * The task is held rather than unpacked so that the two stay one thing. Everything that describes the file
     * itself, and everything the splits are stamped with, already lives on the task, and copying that across
     * would mean a field added there is silently absent from the macro-split path.
     * <p>
     * {@code positions} holds the fixed stride offsets to probe, and so also says which of the two walks this
     * file needs. It is non-empty for a strided splitter, whose offsets are each resolvable without reference to
     * any other and are therefore deferred into the query-wide probe batch. It is empty for a
     * provable-but-not-strided one (quoted or escaped CSV/TSV, selected by
     * {@link RecordSplitter#supportsProvenProbing()}), whose every step depends on the parse state left by the
     * one before it, so only the walk itself can say where to look next. A strided file with no offsets worth
     * probing is never described here at all; see {@link #newlineMacroSplitCandidate}.
     * <p>
     * {@code strideBytes} is the spacing those offsets were laid out at, which is the requested
     * {@code target_split_size} unless {@link #strideBoundedByProbeBudget} widened it, and is what the probes
     * cap their read windows at. The requested size is not carried: nothing past planning needs it.
     * <p>
     * {@code splitter} is shared by every probe of this file, which the {@link RecordSplitter} contract allows:
     * implementations are immutable and safe to call concurrently. {@code storageObject} is shared too, and holds
     * no open resources of its own; each probe owns only the stream it opens.
     */
    private record DeferredNewlineSplits(
        FileTask task,
        StorageObject storageObject,
        RecordSplitter splitter,
        long minSegment,
        long strideBytes,
        List<Long> positions
    ) {}

    /**
     * The outcome of planning one file: its final splits, a descriptor whose record boundaries still need
     * probing, or a sequential walk that has already resolved them. Deferring the probing lets every strided file's
     * probes share a single concurrency budget.
     */
    private sealed interface PlanResult {
        /** A file whose splits are settled, because planning already did whatever reading they needed. */
        record Splits(List<ExternalSplit> splits) implements PlanResult {}

        /** A file whose macro-splits can only be built once the probe phase has resolved its record boundaries. */
        record NeedsProbing(DeferredNewlineSplits deferred) implements PlanResult {}

        /**
         * A file the sequential walk has already resolved. It carries its starts rather than its splits so that
         * both macro-split paths build splits the same way, and it carries whether the walk gave up early so that
         * a quoted file that under-split is reported alongside the strided ones that did.
         */
        record Walked(DeferredNewlineSplits deferred, List<Long> starts, boolean stoppedBeforeEndOfFile) implements PlanResult {}
    }

    /** One stride offset to probe, tied back to the file whose boundaries it contributes to. */
    private record ProbeTask(DeferredNewlineSplits deferred, long position) {}

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
    private PlanResult processFileForSplits(
        FileTask task,
        @Nullable StorageProvider hoistedProvider,
        long strideBytes,
        BooleanSupplier isCancelled
    ) throws IOException {
        if (isCancelled.getAsBoolean()) {
            throw new TaskCancelledException(RecordBoundaryProbe.CANCELLED_MESSAGE);
        }
        // Carry the cancellation signal as ambient thread-local state so the synchronous retry/throttle
        // backoff inside the footer reads below can abort a parked sleep on cancel.
        return StorageRetryCancellation.callWithCancellation(
            isCancelled,
            () -> computeFileSplits(task, hoistedProvider, strideBytes, isCancelled)
        );
    }

    private PlanResult computeFileSplits(
        FileTask task,
        @Nullable StorageProvider hoistedProvider,
        long strideBytes,
        BooleanSupplier isCancelled
    ) throws IOException {
        List<ExternalSplit> fileSplits = new ArrayList<>();

        // Resolve the config-aware reader once and reuse it for both the sequential-whole-file gate and the
        // newline-aligned macro-split attempt below, which would otherwise each resolve it independently. The
        // declared-name binding bit rides the typed DeclaredReadSpec (NOT the config map), so it must be applied
        // here too, or the split-side reader's declaredNameBindingNeedsFileStart() is silently false and the gate
        // below never fires — the read-side reader would then hit a chunk with no header line to bind against.
        FormatReader configuredReader = resolveConfiguredReader(task.filePath(), task.config());
        if (configuredReader != null && task.declaredReadSpec().provenance() == SchemaProvenance.DECLARED) {
            configuredReader = configuredReader.withDeclaredProvenanceBinding(true);
        }

        // Quoted or escaped CSV/TSV cannot be probed at arbitrary offsets (an in-quote newline, or a
        // backslash-escaped raw newline, would be misread as a record terminator), so no start-anywhere
        // splitting is safe: not newline-aligned macro-splits, nor compressed block/frame-aligned splits.
        // Emit a single whole-file split (identical to the fallback below); the reader consumes it as one
        // sequential stream and finds boundaries quote/escape-aware.
        if (requiresSequentialWholeFileRead(configuredReader)) {
            fileSplits.add(
                wholeFileSplit(
                    task.filePath(),
                    task.fileLength(),
                    task.format(),
                    task.config(),
                    task.partitionValues(),
                    task.columnMapping(),
                    task.readSchema()
                )
            );
            return new PlanResult.Splits(fileSplits);
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
            return new PlanResult.Splits(fileSplits);
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
            return new PlanResult.Splits(fileSplits);
        }

        DeferredNewlineSplits deferred = newlineMacroSplitCandidate(task, strideBytes, hoistedProvider, configuredReader);
        if (deferred == null) {
            // Whole-file split when macro splitting does not apply: small files, unsupported formats, or a file
            // with no stride offset far enough from end-of-file to be worth cutting at.
            fileSplits.add(
                wholeFileSplit(
                    task.filePath(),
                    task.fileLength(),
                    task.format(),
                    task.config(),
                    task.partitionValues(),
                    task.columnMapping(),
                    task.readSchema()
                )
            );
            return new PlanResult.Splits(fileSplits);
        }
        // Offsets to probe mean a strided splitter, and its probes are deferred so they can share one
        // concurrency budget with every other file's. A candidate with none can only be walked sequentially,
        // so resolve it here, where it is at least concurrent with the planning of other files.
        if (deferred.positions().isEmpty() == false) {
            return new PlanResult.NeedsProbing(deferred);
        }
        RecordBoundaryProbe.ProvenWalk walk = provenMacroSplitStarts(deferred, isCancelled);
        return new PlanResult.Walked(deferred, walk.boundaries(), walk.stoppedBeforeEndOfFile());
    }

    /**
     * Resolves a non-strided candidate's macro-split starts with the sequential proven walk.
     * <p>
     * A splitter that can be probed neither at a fixed offset nor by proving a record start must have been routed
     * to a whole-file split upstream; if one arrives here that gate failed, so fail loud rather than emit
     * mis-aligned macro-splits that silently mis-count rows.
     */
    private static RecordBoundaryProbe.ProvenWalk provenMacroSplitStarts(DeferredNewlineSplits deferred, BooleanSupplier isCancelled)
        throws IOException {
        RecordSplitter splitter = deferred.splitter();
        if (splitter.supportsProvenProbing() == false) {
            throw new IllegalStateException(
                "record splitter ["
                    + splitter.getClass().getName()
                    + "] supports neither strided nor proven probing and cannot be macro-split"
            );
        }
        return RecordBoundaryProbe.provenBoundaries(
            splitter,
            deferred.storageObject(),
            deferred.task().fileLength(),
            deferred.strideBytes(),
            deferred.minSegment(),
            isCancelled
        );
    }

    /**
     * Resolves the config-aware {@link FormatReader} for a file, or {@code null} when it cannot be resolved
     * (no {@code formatRegistry}, no object name, or an unknown extension). Config-aware so a {@code WITH}
     * override (e.g. {@code mode=plain}, {@code quote=none}) selects the same reader/splitter the read path
     * will actually use: {@code byExtension} alone yields the extension default (quoted for {@code .csv}),
     * whose non-strided splitter would send a plain-mode file down the sequential proven walk instead of the
     * strided one. {@code withConfig} returns {@code null} only for test mocks; the base reader is used
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
        if (reader.declaredNameBindingNeedsFileStart()) {
            // Binding is resolved against the header, which only a split starting at byte 0 can read.
            return true;
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
                splits.add(wholeFileSplit(filePath, fileLength, format, config, partitionValues, columnMapping, readSchema));
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
     * Decides how a file's record boundaries near {@code targetStrideBytes} are to be found and, if they can be,
     * returns everything needed to find them and build the splits, including which of the two walks applies; see
     * {@link DeferredNewlineSplits}. Performs <b>no I/O</b>: for a strided splitter the probe positions are pure
     * arithmetic, so the caller is free to run the probes later and concurrently with other files' probes.
     * <p>
     * Returns {@code null} for every file the caller should read whole, which is both a file that is no
     * macro-split candidate at all and a strided one whose offsets all fall within a minimum segment of
     * end-of-file. Those are one answer rather than two because they call for the same split, and telling them
     * apart downstream would mean carrying a descriptor that describes nothing to cut.
     */
    @Nullable
    private DeferredNewlineSplits newlineMacroSplitCandidate(
        FileTask task,
        long targetStrideBytes,
        @Nullable StorageProvider hoistedProvider,
        @Nullable FormatReader reader
    ) throws IOException {
        long fileLength = task.fileLength();
        if (formatRegistry == null || storageRegistry == null || targetStrideBytes <= 0 || fileLength <= targetStrideBytes) {
            return null;
        }
        if (isNewlineMacroSplitCandidateExtension(task.format()) == false) {
            return null;
        }
        // Reuses the reader resolved once in processFileForSplits (config-aware; see resolveConfiguredReader).
        if (reader == null) {
            return null;
        }
        if (reader instanceof CompressionDelegatingFormatReader) {
            return null;
        }
        if (reader instanceof SegmentableFormatReader == false) {
            return null;
        }
        SegmentableFormatReader segmentableReader = (SegmentableFormatReader) reader;
        RecordSplitter splitter = segmentableReader.recordSplitter(task.maxRecordBytes());
        long minSegment = segmentableReader.minimumSegmentSize();
        boolean strided = splitter.supportsStridedProbing();
        // A strided splitter probes fixed offsets, so its positions are known here without reading anything.
        // The sequential walk chooses its own as it goes, so it carries none.
        List<Long> positions = strided ? RecordBoundaryProbe.stridedPositions(fileLength, targetStrideBytes, minSegment) : List.of();
        if (strided && positions.isEmpty()) {
            return null;
        }
        StorageProvider provider = resolveProvider(task.filePath(), task.config(), hoistedProvider);
        StorageObject object = provider.newObject(task.filePath(), fileLength);
        return new DeferredNewlineSplits(task, object, splitter, minSegment, targetStrideBytes, positions);
    }

    /**
     * The stride every file of a query is cut at: the requested one, or the wider one that keeps the query
     * within {@code maxSplitProbes} record-boundary probes.
     * <p>
     * Consecutive probe offsets are a stride apart on both walks (the strided one probes
     * {@code stride, 2 * stride, ...}, the proven one resumes a stride past each boundary it finds), so a file
     * costs about {@code fileLength / stride} probes and the files being cut collectively cost
     * {@code probedFileBytes / stride}. Dividing by the budget therefore yields the stride at which they spend
     * exactly it. It bounds the offsets, not the spans they resolve to, which come out a stride apart only
     * approximately; see {@link RecordBoundaryProbe#reduce}.
     * <p>
     * {@code probedFileBytes} counts the files that exceed the <em>requested</em> stride, and widening can only
     * take a file below the stride, where it becomes a single whole-file split that costs no probe at all. The
     * budget is thus an upper bound on the probes actually issued, and errs towards spending less than it.
     * <p>
     * It is an estimate off the file extension, not off the reader each file resolves to, so a candidate
     * extension whose reader turns out to be unsplittable is counted even though it issues no probe. That only
     * widens the stride, and only in a scan whose candidate bytes already exceed {@code maxSplitProbes}
     * strides; resolving a reader per file to sharpen it would cost more than the coarser cut does.
     * <p>
     * Widening rather than failing keeps a {@code target_split_size} that suits most of a scan from being
     * rejected because the scan as a whole is large. Telling the user that the size they asked for is not the
     * size they got is the caller's to do, so that this stays arithmetic the caller can evaluate without
     * emitting anything.
     *
     * @param maxSplitProbes the probes this query may issue, from {@link #CONFIG_MAX_SPLIT_PROBES}
     */
    private static long strideBoundedByProbeBudget(long requestedStrideBytes, long probedFileBytes, int maxSplitProbes) {
        return Math.max(requestedStrideBytes, Math.ceilDiv(probedFileBytes, maxSplitProbes));
    }

    /**
     * Builds a candidate file's splits from its resolved macro-split starts: one contiguous split per boundary,
     * the last extending to end-of-file, each stamped so the read side can tell where it sits in the file.
     * Falls back to a single whole-file split when no usable boundary was found.
     * <p>
     * That fallback is silent here because a single start does not say why there was only one, and this method
     * cannot see the walk that produced it: a file whose one boundary would have left a short tail and a file no
     * probe could cut arrive here identically. Only the caller holds the walk's own account of what it found, so
     * reporting a file that was cut into less than it asked for is left to {@link SplitShortfall}.
     */
    private static List<ExternalSplit> buildNewlineMacroSplits(DeferredNewlineSplits deferred, List<Long> starts) {
        FileTask task = deferred.task();
        long fileLength = task.fileLength();
        Map<String, Object> config = task.config();
        if (starts.size() <= 1) {
            return List.of(
                wholeFileSplit(
                    task.filePath(),
                    fileLength,
                    task.format(),
                    config,
                    task.partitionValues(),
                    task.columnMapping(),
                    task.readSchema()
                )
            );
        }
        List<ExternalSplit> splits = new ArrayList<>(starts.size());
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
                FileSplit.withReadSchema(
                    "file",
                    task.filePath(),
                    start,
                    length,
                    task.format(),
                    splitConfig,
                    task.partitionValues(),
                    task.columnMapping(),
                    task.readSchema()
                )
            );
        }
        return splits;
    }

    static boolean isNewlineMacroSplitCandidateExtension(@Nullable String format) {
        if (format == null) {
            return false;
        }
        String f = format.toLowerCase(Locale.ROOT);
        return ".ndjson".equals(f) || ".jsonl".equals(f) || ".json".equals(f) || ".csv".equals(f) || ".tsv".equals(f);
    }

    /** Whether this leaf split came from {@link #buildNewlineMacroSplits}. */
    public static boolean isRecordAlignedMacroSplit(FileSplit split) {
        return split != null && "true".equals(split.config().get(RECORD_ALIGNED_MACRO_SPLIT_KEY));
    }

    /**
     * Whether this split covers the start of its file, and so owns the file's leading bytes (a header line,
     * a leading partial record that belongs to no predecessor).
     * <p>
     * Position — where a split sits in its file — is stamped by this class on every split it produces and read
     * back through this method and {@link #isLastInFile}. Deriving it anywhere else risks the two answers
     * drifting apart, which is precisely how a whole-file read came to be treated as "not the last split" and
     * discarded its final record.
     */
    public static boolean isFirstInFile(FileSplit split) {
        return split != null && ("true".equals(split.config().get(FIRST_SPLIT_KEY)) || split.offset() == 0);
    }

    /**
     * Whether this split covers the end of its file, and so owns the file's trailing bytes. Readers key their
     * record-boundary protocol off this: a split that is not last drops its trailing partial record because the
     * next split re-reads those bytes, while a last split must keep it — nothing else will read it.
     * <p>
     * See {@link #isFirstInFile} on why position is derived here and nowhere else.
     */
    public static boolean isLastInFile(FileSplit split) {
        return split != null && ("true".equals(split.config().get(LAST_SPLIT_KEY)) || legacyUnstampedWholeFile(split));
    }

    /**
     * Recognises a whole-file split produced before this class stamped position keys, so a data node still reads
     * such a split correctly during a rolling upgrade. Splits are built on the coordinator and sent to data nodes,
     * so an older coordinator emits no position keys at all.
     * <p>
     * This is the only place an absent LAST-position key is interpreted, and it is BWC-only: delete it once no
     * supported coordinator predates the stamping. (An absent FIRST key is read from {@code offset() == 0}
     * permanently and by design — range splits carry no position keys at all and rely on it.) It recognises
     * legacy shapes by ruling out every protocol that
     * implies a split covers part of a file, which is sound only because that list is closed over the producers
     * in this class as of the stamping change — <b>a new split shape must stamp its position keys</b> rather than
     * rely on being absent from this list.
     */
    private static boolean legacyUnstampedWholeFile(FileSplit split) {
        Map<String, Object> config = split.config();
        return split.offset() == 0
            && "true".equals(config.get(RECORD_ALIGNED_MACRO_SPLIT_KEY)) == false
            && "true".equals(config.get(COMPRESSED_OFFSET_SPLIT_KEY)) == false
            && "true".equals(config.get(RANGE_SPLIT_KEY)) == false;
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
                splits.add(wholeFileSplit(filePath, fileLength, format, config, partitionValues, columnMapping, readSchema));
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
     * Returns the hoisted WITH-config lease when present; empty-config reads use the
     * registry default. A missing hoist with non-empty config is a programming error
     * (creating a provider here would leak a pool lease). Unreachable when the hoist
     * in {@code discoverSplits} ran; fails as {@link AssertionError}, not a user ISE.
     */
    private StorageProvider resolveProvider(StoragePath filePath, Map<String, Object> config, @Nullable StorageProvider hoistedProvider) {
        if (hoistedProvider != null) {
            return hoistedProvider;
        }
        if (config != null && config.isEmpty() == false) {
            throw new AssertionError("WITH-config split discovery requires a hoisted storage provider");
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
        Check.clientError(result > 0, "Invalid value for [{}]: [{}]; must be positive", CONFIG_TARGET_SPLIT_SIZE, value);
        return result;
    }

    /**
     * Resolves the bytes each of this query's record-boundary probes may read, falling back to
     * {@link RecordBoundaryProbe#DEFAULT_SPLIT_PROBE_WINDOW}. The default lives on the constant rather than on a
     * field of this provider because every constructor overload would otherwise have to carry a value none of
     * them has anything to say about.
     */
    private static long resolveSplitProbeWindow(Map<String, Object> config) {
        if (config == null) {
            return RecordBoundaryProbe.DEFAULT_SPLIT_PROBE_WINDOW;
        }
        Object value = config.get(CONFIG_SPLIT_PROBE_WINDOW);
        if (value == null) {
            return RecordBoundaryProbe.DEFAULT_SPLIT_PROBE_WINDOW;
        }
        String s = value.toString().trim();
        if (s.isEmpty()) {
            return RecordBoundaryProbe.DEFAULT_SPLIT_PROBE_WINDOW;
        }
        return validateSplitProbeWindow(s);
    }

    /**
     * Parses and validates an already-trimmed {@code split_probe_window} value, returning the size in bytes.
     * Shared by the query path ({@link #resolveSplitProbeWindow}) and the dataset CRUD validator so both accept
     * exactly the same inputs. The caller owns trimming and the null/empty fallback to a default; this method
     * always parses.
     *
     * @throws org.elasticsearch.ElasticsearchParseException if the unit suffix is missing or malformed
     * @throws IllegalArgumentException                      if the resulting size is not positive
     */
    public static long validateSplitProbeWindow(String value) {
        long result = ByteSizeValue.parseBytesSizeValue(value, CONFIG_SPLIT_PROBE_WINDOW).getBytes();
        Check.clientError(result > 0, "Invalid value for [{}]: [{}]; must be positive", CONFIG_SPLIT_PROBE_WINDOW, value);
        return result;
    }

    /**
     * Resolves the record-boundary probes this query may issue, falling back to
     * {@link #DEFAULT_MAX_SPLIT_PROBES}.
     */
    private static int resolveMaxSplitProbes(Map<String, Object> config) {
        if (config == null) {
            return DEFAULT_MAX_SPLIT_PROBES;
        }
        Object value = config.get(CONFIG_MAX_SPLIT_PROBES);
        if (value == null) {
            return DEFAULT_MAX_SPLIT_PROBES;
        }
        String s = value.toString().trim();
        if (s.isEmpty()) {
            return DEFAULT_MAX_SPLIT_PROBES;
        }
        return validateMaxSplitProbes(s);
    }

    /**
     * Parses and validates an already-trimmed {@code max_split_probes} value, returning the count. It is a
     * count of reads rather than a size, so it parses as a plain integer and takes no unit suffix, which is what
     * its rejection message says where {@link #validateSplitProbeWindow} says only that the value must be
     * positive. Both reject as client errors, so a bad value for either key answers 400.
     * <p>
     * Unlike the window, this one has a ceiling of its own ({@link #MAX_SPLIT_PROBES_CEILING}) because it is the
     * key that costs planning heap rather than only bytes read.
     *
     * @throws IllegalArgumentException if the value is not an integer, is not positive, or is above the ceiling
     */
    public static int validateMaxSplitProbes(String value) {
        int result;
        try {
            result = Integer.parseInt(value);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(
                Strings.format("Invalid value for [%s]: [%s]; must be a positive integer", CONFIG_MAX_SPLIT_PROBES, value),
                e
            );
        }
        Check.clientError(result > 0, "Invalid value for [{}]: [{}]; must be positive", CONFIG_MAX_SPLIT_PROBES, value);
        Check.clientError(
            result <= MAX_SPLIT_PROBES_CEILING,
            "Invalid value for [{}]: [{}]; must not exceed [{}]",
            CONFIG_MAX_SPLIT_PROBES,
            value,
            MAX_SPLIT_PROBES_CEILING
        );
        return result;
    }

    /**
     * Validates that a dataset's two probe keys do not together ask for more than {@link #MAX_PROBE_BUDGET_BYTES}
     * of reads. Resolves the effective values, defaulting whichever key is absent, because one key on its own is
     * enough to blow the budget against the other's default: a check that skipped absent keys would accept a
     * dataset at registration and then reject every query over it.
     */
    public static void validateProbeBudget(Map<String, Object> config) {
        validateProbeBudget(resolveSplitProbeWindow(config), resolveMaxSplitProbes(config));
    }

    /**
     * Rejects a probe budget above {@link #MAX_PROBE_BUDGET_BYTES}, expressed as the widest window the given
     * probe count leaves room for. The message names a window rather than the product because the product is what
     * the user did not ask for, and dividing also keeps the check itself away from a multiplication that a large
     * enough window would overflow.
     */
    static void validateProbeBudget(long splitProbeWindowBytes, int maxSplitProbes) {
        long widestWindow = MAX_PROBE_BUDGET_BYTES / maxSplitProbes;
        Check.clientError(
            splitProbeWindowBytes <= widestWindow,
            "Invalid combination of [{}] of [{}] and [{}] of [{}]: a query may read at most [{}] while probing, "
                + "which at that many probes leaves a window of [{}]; lower either value",
            CONFIG_SPLIT_PROBE_WINDOW,
            ByteSizeValue.ofBytes(splitProbeWindowBytes),
            CONFIG_MAX_SPLIT_PROBES,
            maxSplitProbes,
            ByteSizeValue.ofBytes(MAX_PROBE_BUDGET_BYTES),
            ByteSizeValue.ofBytes(widestWindow)
        );
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
