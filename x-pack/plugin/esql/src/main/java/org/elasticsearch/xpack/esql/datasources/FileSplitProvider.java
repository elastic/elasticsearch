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
import org.elasticsearch.xpack.esql.datasources.spi.SplitDiscoveryContext;
import org.elasticsearch.xpack.esql.datasources.spi.SplitProvider;
import org.elasticsearch.xpack.esql.datasources.spi.SplittableDecompressionCodec;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.datasources.spi.StorageProvider;
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
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;

/**
 * Default {@link SplitProvider} for file-based sources.
 * Converts each file in the {@link FileList} into a {@link FileSplit},
 * applying L1 partition pruning when filter hints and partition metadata are available.
 *
 * <p>When filter hints contain resolved {@link Expression} objects, evaluates them against
 * each file's partition values to prune files that cannot match the filter.
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

    private final long targetSplitSizeBytes;
    private final DecompressionCodecRegistry codecRegistry;
    private final StorageProviderRegistry storageRegistry;
    private final FormatReaderRegistry formatRegistry;
    private final Settings settings;

    public FileSplitProvider() {
        this(DEFAULT_TARGET_SPLIT_SIZE, null, null, null, Settings.EMPTY);
    }

    public FileSplitProvider(long targetSplitSizeBytes) {
        this(targetSplitSizeBytes, null, null, null, Settings.EMPTY);
    }

    public FileSplitProvider(
        long targetSplitSizeBytes,
        DecompressionCodecRegistry codecRegistry,
        StorageProviderRegistry storageRegistry,
        Settings settings
    ) {
        this(targetSplitSizeBytes, codecRegistry, storageRegistry, null, settings);
    }

    public FileSplitProvider(
        long targetSplitSizeBytes,
        DecompressionCodecRegistry codecRegistry,
        StorageProviderRegistry storageRegistry,
        FormatReaderRegistry formatRegistry,
        Settings settings
    ) {
        this.targetSplitSizeBytes = targetSplitSizeBytes;
        this.codecRegistry = codecRegistry;
        this.storageRegistry = storageRegistry;
        this.formatRegistry = formatRegistry;
        this.settings = settings != null ? settings : Settings.EMPTY;
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
        List<ExternalSplit> splits = new ArrayList<>();
        // Dedup cache: files with content-equal mappings share the same ColumnMapping
        // instance on the coordinator, avoiding redundant allocations.
        Map<SchemaReconciliation.ColumnMapping, SchemaReconciliation.ColumnMapping> mappingCache = new HashMap<>();

        long effectiveSplitSize = resolveTargetSplitSize(config);

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

            // Try block-aligned splitting for splittable compressed files (e.g. .ndjson.bz2).
            // This is independent of targetSplitSizeBytes — compressed files with splittable
            // codecs are always split at block boundaries when possible.
            if (tryBlockAlignedSplits(filePath, fileLength, format, config, partitionValues, columnMapping, splits)) {
                continue;
            }

            if (tryRangeAwareSplits(fileList, i, filePath, fileLength, format, config, partitionValues, columnMapping, splits)) {
                continue;
            }

            if (effectiveSplitSize > 0 && fileLength > effectiveSplitSize && isSplittableFormat(format)) {
                long offset = 0;
                boolean isFirst = true;
                while (offset < fileLength) {
                    long chunkLength = Math.min(effectiveSplitSize, fileLength - offset);
                    boolean isLast = (offset + chunkLength >= fileLength);
                    Map<String, Object> splitConfig = new HashMap<>(config);
                    if (isFirst) {
                        splitConfig.put(FIRST_SPLIT_KEY, "true");
                    }
                    if (isLast) {
                        splitConfig.put(LAST_SPLIT_KEY, "true");
                    }
                    splits.add(new FileSplit("file", filePath, offset, chunkLength, format, splitConfig, partitionValues, columnMapping));
                    offset += chunkLength;
                    isFirst = false;
                }
            } else {
                splits.add(new FileSplit("file", filePath, 0, fileLength, format, config, partitionValues, columnMapping));
            }
        }

        return List.copyOf(splits);
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
        List<ExternalSplit> splits
    ) {
        if (codecRegistry == null || storageRegistry == null || format == null) {
            return false;
        }

        DecompressionCodec codec = codecRegistry.byExtension(format);

        // Prefer IndexedDecompressionCodec (e.g. zstd seekable) over SplittableDecompressionCodec
        // (e.g. bzip2) when an index is available, since index-based splitting avoids scanning.
        if (codec instanceof IndexedDecompressionCodec indexedCodec) {
            if (tryIndexedSplits(indexedCodec, filePath, fileLength, format, config, partitionValues, columnMapping, splits)) {
                return true;
            }
        }

        if (codec instanceof SplittableDecompressionCodec == false) {
            return false;
        }
        SplittableDecompressionCodec splittableCodec = (SplittableDecompressionCodec) codec;

        try {
            // When config is empty, provider() returns a cached instance (no leak).
            // When config is non-empty, createProvider() returns a fresh instance that
            // is not tracked by the registry. This is acceptable because the same provider
            // will be created for actual reads, and the boundary-scan provider only holds
            // lightweight state (no persistent connections).
            StorageProvider provider;
            if (config != null && config.isEmpty() == false) {
                provider = storageRegistry.createProvider(filePath.scheme(), settings, config);
            } else {
                provider = storageRegistry.provider(filePath);
            }
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
     * <p>
     * Fast path: when the {@link FileList} carries pre-resolved ranges from single-pass file
     * layout resolution (see {@link RangeAwareFormatReader#resolveFileLayout}), the file is not
     * re-opened here -- splits are built directly from the cached ranges.
     * <p>
     * Slow path: when no pre-resolved ranges are available, the format reader reads file metadata
     * (e.g. Parquet footer) to discover independently readable byte ranges. Returns true if
     * range-aware splits were created.
     */
    private boolean tryRangeAwareSplits(
        FileList fileList,
        int fileIndex,
        StoragePath filePath,
        long fileLength,
        String format,
        Map<String, Object> config,
        Map<String, Object> partitionValues,
        @Nullable SchemaReconciliation.ColumnMapping columnMapping,
        List<ExternalSplit> splits
    ) {
        // Fast path: pre-resolved ranges captured during metadata resolution. This avoids a
        // second open of the file (e.g. Parquet footer read) on remote storage.
        if (fileList != null) {
            int rc = fileList.rangeCount(fileIndex);
            if (rc > 0) {
                LOGGER.trace("Using {} pre-resolved split ranges for [{}]", rc, filePath);
                buildSplitsFromIndexedRanges(
                    fileList,
                    fileIndex,
                    rc,
                    filePath,
                    fileLength,
                    format,
                    config,
                    partitionValues,
                    columnMapping,
                    splits
                );
                return true;
            }
            // rc == 0: resolution ran but produced no ranges — skip range-aware, fall through to single-split
            if (rc == 0) {
                return false;
            }
        }

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
            StorageProvider provider;
            if (config != null && config.isEmpty() == false) {
                provider = storageRegistry.createProvider(filePath.scheme(), settings, config);
            } else {
                provider = storageRegistry.provider(filePath);
            }
            StorageObject object = provider.newObject(filePath, fileLength);

            List<SplitRange> ranges = rangeReader.resolveFileLayout(object).splitRanges();
            if (ranges.isEmpty()) {
                return false;
            }

            buildSplitsFromRanges(ranges, filePath, fileLength, format, config, partitionValues, columnMapping, splits);
            return true;
        } catch (IOException e) {
            LOGGER.warn("Failed to discover split ranges for [{}], falling back to single split", filePath, e);
            return false;
        }
    }

    private static void buildSplitsFromIndexedRanges(
        FileList fileList,
        int fileIndex,
        int rangeCount,
        StoragePath filePath,
        long fileLength,
        String format,
        Map<String, Object> config,
        Map<String, Object> partitionValues,
        @Nullable SchemaReconciliation.ColumnMapping columnMapping,
        List<ExternalSplit> splits
    ) {
        Map<String, Object> splitConfig = new HashMap<>(config);
        splitConfig.put(RANGE_SPLIT_KEY, "true");
        splitConfig.put(FILE_LENGTH_KEY, Long.toString(fileLength));

        for (int r = 0; r < rangeCount; r++) {
            SplitStats stats = fileList.rangeStats(fileIndex, r);
            splits.add(
                FileSplit.withSplitStats(
                    "file",
                    filePath,
                    fileList.rangeOffset(fileIndex, r),
                    fileList.rangeLength(fileIndex, r),
                    format,
                    splitConfig,
                    partitionValues,
                    columnMapping,
                    stats
                )
            );
        }
    }

    private static void buildSplitsFromRanges(
        List<SplitRange> ranges,
        StoragePath filePath,
        long fileLength,
        String format,
        Map<String, Object> config,
        Map<String, Object> partitionValues,
        @Nullable SchemaReconciliation.ColumnMapping columnMapping,
        List<ExternalSplit> splits
    ) {
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
    }

    private boolean tryIndexedSplits(
        IndexedDecompressionCodec indexedCodec,
        StoragePath filePath,
        long fileLength,
        String format,
        Map<String, Object> config,
        Map<String, Object> partitionValues,
        @Nullable SchemaReconciliation.ColumnMapping columnMapping,
        List<ExternalSplit> splits
    ) {
        try {
            StorageProvider provider;
            if (config != null && config.isEmpty() == false) {
                provider = storageRegistry.createProvider(filePath.scheme(), settings, config);
            } else {
                provider = storageRegistry.provider(filePath);
            }
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

    static boolean isSplittableFormat(String format) {
        if (format == null) {
            return false;
        }
        return switch (format) {
            case ".csv", ".tsv", ".ndjson", ".jsonl", ".json", ".txt" -> true;
            default -> false;
        };
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
        if (a instanceof Comparable<?> && b instanceof Comparable<?>) {
            try {
                @SuppressWarnings("rawtypes")
                Comparable ca = (Comparable) a;
                @SuppressWarnings("unchecked")
                int result = ca.compareTo(b);
                return result;
            } catch (ClassCastException e) {
                return a.toString().compareTo(b.toString());
            }
        }
        return a.toString().compareTo(b.toString());
    }
}
