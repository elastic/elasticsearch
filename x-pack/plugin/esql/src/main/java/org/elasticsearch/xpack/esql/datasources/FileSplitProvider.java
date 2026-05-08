/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.expression.predicate.operator.comparison.BinaryComparison;
import org.elasticsearch.xpack.esql.core.util.Check;
import org.elasticsearch.xpack.esql.datasources.spi.DecompressionCodec;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalSplit;
import org.elasticsearch.xpack.esql.datasources.spi.FileList;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReader;
import org.elasticsearch.xpack.esql.datasources.spi.FrameIndex;
import org.elasticsearch.xpack.esql.datasources.spi.IndexedDecompressionCodec;
import org.elasticsearch.xpack.esql.datasources.spi.RangeAwareFormatReader;
import org.elasticsearch.xpack.esql.datasources.spi.RangeAwareFormatReader.SplitRange;
import org.elasticsearch.xpack.esql.datasources.spi.SegmentableFormatReader;
import org.elasticsearch.xpack.esql.datasources.spi.SplitDiscoveryContext;
import org.elasticsearch.xpack.esql.datasources.spi.SplitProvider;
import org.elasticsearch.xpack.esql.datasources.spi.SplittableDecompressionCodec;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.datasources.spi.StorageProvider;
import org.elasticsearch.xpack.esql.datasources.utils.BoundedParallelGather;
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
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.function.BiFunction;

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
 *       (NDJSON/JSONL/JSON, CSV/TSV). {@link SegmentableFormatReader#findNextRecordBoundary}
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
    static final String CONFIG_TARGET_SPLIT_SIZE = "target_split_size";

    /**
     * Macro-split starts on a newline-aligned record boundary (see {@link #tryNewlineAlignedMacroSplits}).
     * Downstream readers set {@link org.elasticsearch.xpack.esql.datasources.spi.FormatReadContext#recordAligned()}
     * and pass this flag into {@link ParallelParsingCoordinator#parallelRead}
     * so single-threaded fallback paths do not skip or trim aligned ranges.
     */
    static final String RECORD_ALIGNED_MACRO_SPLIT_KEY = "_record_aligned_macro_split";

    /** Maximum parallel per-file I/O tasks during split discovery (Parquet footer reads, etc.). */
    static final int MAX_PARALLEL_SPLIT_DISCOVERY = 16;

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
    public List<ExternalSplit> discoverSplits(SplitDiscoveryContext context) {
        FileList fileList = context.fileList();
        if (fileList == null || fileList.isResolved() == false) {
            return List.of();
        }

        PartitionMetadata partitionInfo = context.partitionInfo();
        Map<String, Object> config = context.config();
        List<Expression> filterHints = context.filterHints();
        Set<String> projectedDataColumns = fileBackedProjectedColumns(context.projectedDataColumns(), partitionInfo);
        Map<StoragePath, SchemaReconciliation.FileSchemaInfo> schemaInfo = fileList.fileSchemaInfo();

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
        Map<SchemaReconciliation.ColumnMapping, SchemaReconciliation.ColumnMapping> mappingCache = new ConcurrentHashMap<>();

        // Phase 1: sequential filtering — cheap, in-memory predicates applied per file to
        // build the list of FileTask items that need I/O (footer reads, boundary scans).
        List<FileTask> tasks = new ArrayList<>(fileList.fileCount());
        for (int i = 0; i < fileList.fileCount(); i++) {
            StoragePath filePath = fileList.path(i);

            Map<String, Object> partitionValues = Map.of();
            if (partitionInfo != null && partitionInfo.isEmpty() == false) {
                Map<String, Object> filePartitions = partitionInfo.filePartitionValues().get(filePath);
                if (filePartitions != null) {
                    partitionValues = filePartitions;
                }
            }

            if (partitionValues.isEmpty() == false && filterHints.isEmpty() == false) {
                if (matchesPartitionFilters(partitionValues, filterHints) == false) {
                    continue;
                }
            }

            SchemaReconciliation.FileSchemaInfo fileSchemaInfo = schemaInfo != null ? schemaInfo.get(filePath) : null;

            if (projectedDataColumns.isEmpty() == false && fileSchemaInfo != null) {
                if (skipIfNoColumnOverlap(fileSchemaInfo.fileSchema(), projectedDataColumns)) {
                    continue;
                }
            }

            if (filterHints.isEmpty() == false && fileSchemaInfo != null) {
                Set<String> fileColumnNames = new LinkedHashSet<>();
                for (Attribute attr : fileSchemaInfo.fileSchema()) {
                    fileColumnNames.add(attr.name());
                }
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

            SchemaReconciliation.ColumnMapping columnMapping = null;
            if (schemaInfo != null) {
                SchemaReconciliation.FileSchemaInfo info = schemaInfo.get(filePath);
                if (info != null && info.mapping() != null && info.mapping().isIdentity() == false) {
                    columnMapping = mappingCache.computeIfAbsent(info.mapping(), k -> k);
                }
            }

            tasks.add(new FileTask(filePath, fileLength, format, config, partitionValues, columnMapping));
        }

        if (tasks.isEmpty()) {
            return List.of();
        }

        // Phase 2: I/O-bound split discovery — parallelize when executor is available.
        final StorageProvider hoistedProvider = sharedProvider;
        List<List<ExternalSplit>> perFileSplits;
        try {
            if (executor != null && tasks.size() > 1) {
                perFileSplits = BoundedParallelGather.gather(
                    tasks,
                    task -> processFileForSplits(task, hoistedProvider),
                    MAX_PARALLEL_SPLIT_DISCOVERY,
                    executor
                );
            } else {
                perFileSplits = new ArrayList<>(tasks.size());
                for (FileTask task : tasks) {
                    perFileSplits.add(processFileForSplits(task, hoistedProvider));
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
        return List.copyOf(splits);
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
        @Nullable SchemaReconciliation.ColumnMapping columnMapping
    ) {}

    /**
     * Computes the splits for a single file. Uses the hoisted provider when provided (non-null),
     * otherwise falls back to the registry for per-call provider resolution.
     * This method is safe to call concurrently from multiple threads.
     */
    private List<ExternalSplit> processFileForSplits(FileTask task, @Nullable StorageProvider hoistedProvider) throws IOException {
        List<ExternalSplit> fileSplits = new ArrayList<>();

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
        SchemaReconciliation.ColumnMapping columnMapping = task.columnMapping();
        long effectiveTargetSplitBytes = resolveTargetSplitSize(config);

        if (tryNewlineAlignedMacroSplits(
            filePath,
            fileLength,
            format,
            config,
            partitionValues,
            columnMapping,
            effectiveTargetSplitBytes,
            fileSplits,
            hoistedProvider
        )) {
            return fileSplits;
        }

        // Whole-file split when macro splitting does not apply (small files, unsupported formats, or single aligned span).
        fileSplits.add(new FileSplit("file", filePath, 0, fileLength, format, config, partitionValues, columnMapping));
        return fileSplits;
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
        @Nullable SchemaReconciliation.ColumnMapping columnMapping,
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
                splits.add(new FileSplit("file", filePath, 0, fileLength, format, config, partitionValues, columnMapping));
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
                if (m == 0) {
                    splitConfig.put(FIRST_SPLIT_KEY, "true");
                }
                if (isLastMacroSplit) {
                    splitConfig.put(LAST_SPLIT_KEY, "true");
                }
                splits.add(new FileSplit("file", filePath, start, end - start, format, splitConfig, partitionValues, columnMapping));
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
        @Nullable SchemaReconciliation.ColumnMapping columnMapping,
        List<ExternalSplit> splits,
        @Nullable StorageProvider hoistedProvider
    ) {
        if (formatRegistry == null || storageRegistry == null || format == null) {
            return false;
        }

        FormatReader reader;
        try {
            reader = formatRegistry.byExtension(filePath.objectName());
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
                splits.add(
                    new FileSplit(
                        "file",
                        filePath,
                        range.offset(),
                        range.length(),
                        format,
                        splitConfig,
                        partitionValues,
                        columnMapping,
                        rangeStats
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
        @Nullable SchemaReconciliation.ColumnMapping columnMapping,
        long targetStrideBytes,
        List<ExternalSplit> splits,
        @Nullable StorageProvider hoistedProvider
    ) throws IOException {
        if (formatRegistry == null || storageRegistry == null || targetStrideBytes <= 0 || fileLength <= targetStrideBytes) {
            return false;
        }
        if (isNewlineMacroSplitCandidateExtension(format) == false) {
            return false;
        }
        String objectName = filePath.objectName();
        if (objectName == null) {
            return false;
        }
        final FormatReader reader;
        try {
            reader = formatRegistry.byExtension(objectName);
        } catch (RuntimeException e) {
            LOGGER.debug(() -> "Skipping newline-aligned macro splits: cannot resolve reader for [" + objectName + "]", e);
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
        List<Long> starts = computeRecordAlignedMacroSplitStarts(segmentableReader, object, fileLength, targetStrideBytes);
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
            splits.add(new FileSplit("file", filePath, start, length, format, splitConfig, partitionValues, columnMapping));
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
        long targetStrideBytes
    ) throws IOException {
        List<Long> boundaries = new ArrayList<>();
        boundaries.add(0L);
        long minSegment = reader.minimumSegmentSize();
        long pos = targetStrideBytes;
        while (pos < fileLength) {
            long remaining = fileLength - pos;
            if (remaining < minSegment) {
                break;
            }
            try (InputStream stream = storageObject.newStream(pos, remaining)) {
                long skipped = reader.findNextRecordBoundary(stream);
                if (skipped < 0) {
                    break;
                }
                long boundary = pos + skipped;
                if (boundary >= fileLength) {
                    break;
                }
                if (fileLength - boundary < minSegment) {
                    break;
                }
                boundaries.add(boundary);
                pos = boundary + targetStrideBytes;
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
        @Nullable SchemaReconciliation.ColumnMapping columnMapping,
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
                splits.add(new FileSplit("file", filePath, 0, fileLength, format, config, partitionValues, columnMapping));
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
                    if (splitCount == 0) {
                        splitConfig.put(FIRST_SPLIT_KEY, "true");
                    }
                    if (isLast) {
                        splitConfig.put(LAST_SPLIT_KEY, "true");
                    }
                    splits.add(
                        new FileSplit(
                            "file",
                            filePath,
                            groupStart,
                            groupEnd - groupStart,
                            format,
                            splitConfig,
                            partitionValues,
                            columnMapping
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
        long result = ByteSizeValue.parseBytesSizeValue(s, CONFIG_TARGET_SPLIT_SIZE).getBytes();
        Check.isTrue(result > 0, "Invalid value for [{}]: [{}]; must be positive", CONFIG_TARGET_SPLIT_SIZE, value);
        return result;
    }

    /**
     * Returns the subset of projected data columns that must come from file bytes,
     * excluding partition columns whose values come from paths, not file data.
     */
    static Set<String> fileBackedProjectedColumns(Set<String> projectedDataColumns, PartitionMetadata partitionInfo) {
        if (projectedDataColumns.isEmpty() || partitionInfo == null || partitionInfo.isEmpty()) {
            return projectedDataColumns;
        }
        Set<String> result = new LinkedHashSet<>(projectedDataColumns);
        result.removeAll(partitionInfo.partitionColumns().keySet());
        return result;
    }

    /**
     * Returns {@code true} when the file's data columns have zero overlap with the projected set,
     * meaning this file would produce only NULL rows for all needed columns.
     */
    static boolean skipIfNoColumnOverlap(List<Attribute> fileSchema, Set<String> projectedDataColumns) {
        for (Attribute attr : fileSchema) {
            if (projectedDataColumns.contains(attr.name())) {
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
            default -> null;
        };
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
            return partitionValue != null ? comparator.apply(partitionValue, literalValue) : null;
        }
        columnName = extractColumnName(right);
        literalValue = extractLiteralValue(left);
        if (columnName != null && literalValue != null && partitionValues.containsKey(columnName)) {
            Object partitionValue = partitionValues.get(columnName);
            return partitionValue != null ? comparator.apply(partitionValue, literalValue) : null;
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

    private static boolean compareEquals(Object a, Object b) {
        if (a == null || b == null) {
            return false;
        }
        if (a instanceof Number na && b instanceof Number nb) {
            return na.doubleValue() == nb.doubleValue();
        }
        return a.toString().equals(b.toString());
    }

    private static int compareValues(Object a, Object b) {
        if (a == null || b == null) {
            throw new IllegalArgumentException("Cannot compare null partition values");
        }
        if (a instanceof Number na && b instanceof Number nb) {
            return Double.compare(na.doubleValue(), nb.doubleValue());
        }
        // Coerce mixed Number/String cases: a partition value may be stored as "2024" (String)
        // while the literal from the filter is Integer 2024, or vice versa.
        if (a instanceof Number && b instanceof Number == false) {
            try {
                return Double.compare(((Number) a).doubleValue(), Double.parseDouble(b.toString()));
            } catch (NumberFormatException e) {
                return a.toString().compareTo(b.toString());
            }
        }
        if (b instanceof Number && a instanceof Number == false) {
            try {
                return Double.compare(Double.parseDouble(a.toString()), ((Number) b).doubleValue());
            } catch (NumberFormatException e) {
                return a.toString().compareTo(b.toString());
            }
        }
        if (a instanceof Comparable<?> && b instanceof Comparable<?> && a.getClass() == b.getClass()) {
            @SuppressWarnings("unchecked")
            Comparable<Object> ca = (Comparable<Object>) a;
            return ca.compareTo(b);
        }
        return a.toString().compareTo(b.toString());
    }
}
