/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.automaton.ByteRunAutomaton;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.ThrottledIterator;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.core.CheckedFunction;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.expression.predicate.operator.comparison.BinaryComparison;
import org.elasticsearch.xpack.esql.core.expression.predicate.regex.AbstractStringPattern;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.util.ByteMatchers;
import org.elasticsearch.xpack.esql.core.util.Check;
import org.elasticsearch.xpack.esql.datasources.cache.StorageProviderCache;
import org.elasticsearch.xpack.esql.datasources.glob.ListingExtents;
import org.elasticsearch.xpack.esql.datasources.glob.PlanningMemory;
import org.elasticsearch.xpack.esql.datasources.spi.Configured;
import org.elasticsearch.xpack.esql.datasources.spi.DecompressionCodec;
import org.elasticsearch.xpack.esql.datasources.spi.ErrorPolicy;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalFailures;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalPlanningIo;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalSplit;
import org.elasticsearch.xpack.esql.datasources.spi.FileList;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReader;
import org.elasticsearch.xpack.esql.datasources.spi.FrameIndex;
import org.elasticsearch.xpack.esql.datasources.spi.IndexedDecompressionCodec;
import org.elasticsearch.xpack.esql.datasources.spi.RangeAwareFormatReader;
import org.elasticsearch.xpack.esql.datasources.spi.RangeAwareFormatReader.SplitRange;
import org.elasticsearch.xpack.esql.datasources.spi.RecordSplitter;
import org.elasticsearch.xpack.esql.datasources.spi.SegmentableFormatReader;
import org.elasticsearch.xpack.esql.datasources.spi.SourceMetadata;
import org.elasticsearch.xpack.esql.datasources.spi.SourceStatistics;
import org.elasticsearch.xpack.esql.datasources.spi.SplitDiscoveryContext;
import org.elasticsearch.xpack.esql.datasources.spi.SplitDiscoveryResult;
import org.elasticsearch.xpack.esql.datasources.spi.SplitProvider;
import org.elasticsearch.xpack.esql.datasources.spi.SplitStats;
import org.elasticsearch.xpack.esql.datasources.spi.SplittableDecompressionCodec;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.datasources.spi.StorageProvider;
import org.elasticsearch.xpack.esql.datasources.spi.ThreadCpuTimer;
import org.elasticsearch.xpack.esql.datasources.utils.BoundedParallelGather;
import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvCompare;
import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvContains;
import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvGreater;
import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvInRange;
import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvIntersects;
import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvLess;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.AutomataMatch;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.StartsWith;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.regex.RLike;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.regex.WildcardLike;
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
import java.io.UncheckedIOException;
import java.time.Instant;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.function.BiConsumer;
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
 *
 * <p>Production Phase-2 ({@link #discoverSplitsAsync}) fans out footer/probe reads with
 * {@link ThrottledIterator} on {@code esql_external_io} and never joins: {@code SEARCH} and
 * {@code GENERIC} must not issue those GETs, and {@code esql_external_io} must not sit in a gather
 * latch. Parsed-footer cache hits ({@link RangeAwareFormatReader#cachedSplitRanges}) skip the
 * throttle entirely — the permit exists to bound in-flight GETs, not hash lookups.
 * {@link #discoverSplits} remains for tests and other non-pool callers that are allowed to join.
 */
public class FileSplitProvider implements SplitProvider {

    private static final Logger LOGGER = LogManager.getLogger(FileSplitProvider.class);

    /**
     * In-flight {@link FileTask} shells for this provider. One discovery runs at a time.
     * Released when the planning slot completes. Test-only; not a liveness GC signal.
     */
    private final AtomicInteger liveFileTasks = new AtomicInteger();
    private final AtomicInteger peakLiveFileTasks = new AtomicInteger();

    /** Test hook. Resets the in-flight {@link FileTask} window counters. */
    void resetLiveFileTasks() {
        liveFileTasks.set(0);
        peakLiveFileTasks.set(0);
    }

    /** Test hook. Highest simultaneous {@link FileTask} count since {@link #resetLiveFileTasks()}. */
    int peakLiveFileTasks() {
        return peakLiveFileTasks.get();
    }

    /** Test hook. {@link FileTask} shells whose planning slot has not completed. */
    int liveFileTasks() {
        return liveFileTasks.get();
    }

    /**
     * Test hook. Concurrency argument of the last miss-planning {@link #gatherAsync} call.
     * Zero until that call. One discovery runs at a time.
     */
    private volatile int planningGatherConcurrency;

    int planningGatherConcurrency() {
        return planningGatherConcurrency;
    }

    private void trackFileTaskCreated() {
        int now = liveFileTasks.incrementAndGet();
        peakLiveFileTasks.accumulateAndGet(now, Math::max);
    }

    private void releaseFileTask() {
        liveFileTasks.decrementAndGet();
    }

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
     * splits — so the dispatcher must not surface {@code _file.record_ref} from these splits (it
     * null-splices the {@code _rowPosition} slot instead).
     */
    static final String COMPRESSED_OFFSET_SPLIT_KEY = "_compressed_offset_split";

    /**
     * Ceiling on concurrent pinning I/O during leftover split discovery (ORC, probes, {@code file://},
     * {@code gs}). Native-async Parquet planning uses {@link ExternalSourceSettings#externalIoThreads}
     * instead. Applied separately to that leftover planning and to the record-boundary probes that
     * follow it. The two passes run one after the other, so this bounds in-flight pinning reads at any
     * instant rather than being multiplied between them.
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

    /**
     * True while this thread is already inside {@link #runRecordingDiscoveryCpu}. Nested
     * {@code fanOut.execute} on {@code DIRECT} (cache-hit hop, {@code parseTailOnExecutor})
     * must not add the same interval twice.
     */
    private static final ThreadLocal<Boolean> DISCOVERY_CPU_TIMING = ThreadLocal.withInitial(() -> Boolean.FALSE);

    private final long targetSplitSizeBytes;
    private final DecompressionCodecRegistry codecRegistry;
    private final StorageProviderRegistry storageRegistry;
    private final FormatReaderRegistry formatRegistry;
    private final Settings settings;
    @Nullable
    private final Executor executor;
    /**
     * How this provider lists a dataset when the schema's listing was a prefix of it: the node's live caps, and the
     * shared listing cache in front of them. A constructor that supplies none gets one over its own {@code settings}
     * with no cache, which is what a unit test wants and what every caller had before the cache reached here.
     */
    private final DatasetListingService listingService;
    /**
     * Keeps a warning about a dataset's layout to once per window per node rather than once per query. Shared through
     * {@link FileSourceFactory} in production; a constructor that supplies none gets its own.
     */
    private final NodeWarningThrottle warnings;
    private final AtomicLong splitDiscoveryCpuNanos = new AtomicLong();
    private final AtomicInteger splitDiscoveryProbes = new AtomicInteger();
    /**
     * Strided files whose probe grid was cut short because of {@code rowLimit}. Shortfall accounting must not
     * treat unprobed offsets as missing or warn that the file was read whole.
     */
    private final Set<DeferredNewlineSplits> demandTruncatedFiles = Collections.newSetFromMap(new IdentityHashMap<>());
    /**
     * What this discovery has to tell the query's author. Held per discovery, like
     * {@link #splitDiscoveryCpuNanos}: one discovery runs on a provider at a time, and the result is built after
     * the listing that produces these.
     */
    private final AtomicReference<List<String>> discoveryWarnings = new AtomicReference<>(List.of());

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
        this(targetSplitSizeBytes, codecRegistry, storageRegistry, formatRegistry, settings, executor, null, null);
    }

    public FileSplitProvider(
        long targetSplitSizeBytes,
        DecompressionCodecRegistry codecRegistry,
        StorageProviderRegistry storageRegistry,
        FormatReaderRegistry formatRegistry,
        Settings settings,
        @Nullable Executor executor,
        @Nullable DatasetListingService listingService,
        @Nullable NodeWarningThrottle warnings
    ) {
        this.targetSplitSizeBytes = targetSplitSizeBytes;
        this.codecRegistry = codecRegistry;
        this.storageRegistry = storageRegistry;
        this.formatRegistry = formatRegistry;
        this.settings = settings != null ? settings : Settings.EMPTY;
        this.executor = executor;
        this.listingService = listingService != null ? listingService : new DatasetListingService(this.settings, null, null, null, null);
        this.warnings = warnings != null ? warnings : new NodeWarningThrottle();
    }

    /**
     * A result that names the file set it was planned over.
     * <p>
     * When this provider discovered its own files, the plan is still holding the listing resolution had - a prefix
     * of the dataset. Anything downstream that reads the plan's file list rather than the splits would then read
     * part of the dataset: the zero-split fall-through does exactly that, and the scanned counts are folded over
     * it. Carrying the set back is what keeps those honest.
     */
    private static SplitDiscoveryResult resultOver(
        SplitDiscoveryContext context,
        List<ExternalSplit> splits,
        boolean exhaustivelyPruned,
        long cpuNanos,
        List<String> warnings,
        int splitDiscoveryProbes
    ) {
        return new SplitDiscoveryResult(
            splits,
            filesContributingASplit(splits),
            exhaustivelyPruned,
            cpuNanos,
            context.fileList(),
            context.schemaMap(),
            warnings,
            splitDiscoveryProbes
        );
    }

    /**
     * Distinct files that produced at least one split, which is what {@link SplitDiscoveryResult#filesScanned()}
     * promises and what the query profile shows an operator.
     * <p>
     * Counted from the splits rather than from the survivors, because the row budget stops planning once the
     * demand is covered and every file past that point survives pruning without being opened. Reporting those
     * would say the scan touched the whole dataset on exactly the queries this change exists to stop touching it -
     * the number an operator would look at to see whether the limit worked, saying it did not.
     */
    private static int filesContributingASplit(List<ExternalSplit> splits) {
        if (splits.isEmpty()) {
            return 0;
        }
        Set<StoragePath> files = Sets.newHashSetWithExpectedSize(splits.size());
        for (ExternalSplit split : splits) {
            if (split instanceof FileSplit fileSplit) {
                files.add(fileSplit.path());
            }
        }
        return files.size();
    }

    /**
     * The files this query must read. Resolution lists a dataset for the schema, which under some modes is a prefix
     * of it and under others the whole of it; when what it established does not cover the query's needs, this
     * discovers the rest for itself, with the query's own filters applied.
     * <p>
     * One place answers this so no caller has to ask whether the listing it was handed happens to be complete. A
     * complete one — {@code union_by_name}, {@code strict}, whose schemas span every file — is the query's file set
     * already and is returned unchanged. An inference-anchor listing is a schema stash, not a scan set, so this
     * swaps in empty rather than listing again. A prefix — {@code first_file_wins}, whose schema needed one file —
     * is not complete, so this lists the dataset with the query's own filters. Continuing from the prefix rather
     * than listing again is the obvious refinement and is not done yet: the page the schema read is listed twice,
     * one request against the full listing's many.
     */
    private SplitDiscoveryContext overTheQuerysFileSet(SplitDiscoveryContext handed, ListingExtents extents) throws Exception {
        DatasetDiscovery discovery = DatasetDiscovery.shared(handed.fileList());
        if (handed.fileList().isInferenceAnchor()) {
            // The leftover file is a schema stash. A re-list with the same hints is another anchor (cache hit);
            // skip it and scan nothing. Certified skip of that one file would also yield zero rows.
            return handed.withScanFileSet(FileList.EMPTY);
        }
        if (discovery.schemaListingIsComplete()) {
            // The listing is the query's file set, so there is nothing to swap and nothing derived from it to move.
            return handed;
        }
        FileList listed = listForQuery(handed, extents);
        rejectConflictingFormatsPastTheSchemasListing(handed, listed);
        // Everything phase 2 sizes per file is sized from this count, and the coordinator could not charge for it:
        // it ran before this listing existed and stood down because the list it had was a prefix. So this is the one
        // charge for those structures, and it is taken here because the first of them is allocated on the next line -
        // withScanFileSet builds the columnar partition values and the per-file schema map over every discovered
        // path, before any survivor or split shell exists.
        scanMemory(handed).reserve(
            Phase2Reservation.bytesForDiscovered(handed.querySchema(), handed.metadataColumnNames(), listed, listed.fileCount())
        );
        LOGGER.debug(
            () -> Strings.format(
                "the schema's listing held %d files of [%s]; discovered %d for the query",
                discovery.schemaListing().fileCount(),
                handed.metadata() == null ? "?" : handed.metadata().location(),
                listed.fileCount()
            )
        );
        SplitDiscoveryContext rebound = handed.withScanFileSet(listed);
        discoveryWarnings.set(warnIfPartitionValuesDoNotFit(handed, listed, rebound));
        return rebound;
    }

    /**
     * Refuses a file whose own name implies a format the dataset does not read.
     * <p>
     * Resolution asks this of the listing it holds, and under a bounded listing that is a prefix of the dataset: a
     * file past it carrying a {@code .json} name under a dataset read as {@code csv} would reach the reader
     * unchallenged and be parsed as csv, which is wrong data rather than an error. The question belongs to whichever
     * listing names the files being read, so it is asked again over this one.
     * <p>
     * The format is the one resolution settled on and stamped as the source type - the same value that chose this
     * provider - so this cannot disagree with the reader that will scan. An unrecognized extension is still allowed:
     * a declared format over names the registry does not claim is a dataset, not a conflict.
     */
    private void rejectConflictingFormatsPastTheSchemasListing(SplitDiscoveryContext handed, FileList listed) {
        if (handed.metadata() == null) {
            return;
        }
        FormatNameResolver.rejectConflictingListedFormats(listed, handed.metadata().sourceType(), formatRegistry);
    }

    /**
     * Warns about a partition value the dataset's own type cannot hold.
     * <p>
     * A partition column's type is inferred at resolution from the paths that listing saw, which under a bounded
     * listing is a sample of the dataset. A value outside that sample need not fit the type it produced -
     * {@code year} sampled as a number, and a {@code year=unknown} folder beyond it - and the plan's attributes are
     * already built from that type, so the value cannot be widened by the time we are here: it has no
     * representation under the column's type and the rows of that file read null for it.
     * <p>
     * That much is the sampling's own consequence, and raising {@code partition_sample_size} is the answer to it.
     * What must not happen is it happening quietly. This goes to the node log rather than the query's response
     * because nothing at this point can reach the response, which is worth fixing separately; a null column nobody
     * can account for is the failure this exists to prevent.
     * <p>
     * It describes the dataset rather than the query, so it is written once per dataset and column set per
     * {@link NodeWarningThrottle} window, not on every query that reads the dataset.
     */
    private List<String> warnIfPartitionValuesDoNotFit(SplitDiscoveryContext handed, FileList listed, SplitDiscoveryContext rebound) {
        PartitionMetadata conformed = rebound.partitionInfo();
        if (conformed == null || conformed.isEmpty()) {
            return List.of();
        }
        PartitionMetadata scanned = listed.partitionMetadata();
        String location = handed.metadata() == null ? "?" : handed.metadata().location();
        if (scanned == null || scanned.isEmpty()) {
            String response = Strings.format(
                "the dataset's partition columns %s were detected over a sample of its paths and the full listing "
                    + "agrees with none of them, so every row reads null for them. Raise [%s], or the dataset has no "
                    + "partition columns to report.",
                conformed.partitionColumns().keySet(),
                PartitionConfig.CONFIG_PARTITION_SAMPLE_SIZE
            );
            // The node log is throttled; the response is not. Which rows a query answers with is the query's own
            // business, so it is told every time even when the operator has heard it already this hour.
            if (warnings.firstInWindow(location + "|shares-no-partition-key|" + conformed.partitionColumns().keySet()) == false) {
                return List.of(response);
            }
            // The dataset declares partition columns and the scan's listing detected none, which happens when the
            // paths past the sample do not agree with it on the key set - a detector answers all or nothing. Every
            // file then reads null for every partition column, so this is the loudest case rather than a quiet one.
            LOGGER.warn(
                "[{}]: the dataset's partition columns {} were detected over a sample of its paths, and the full "
                    + "listing agrees with none of them, so every file reads null for them. Either those paths do not "
                    + "share one key set, in which case the dataset has no partition columns to report, or [{}] is too "
                    + "small to have reached the ones they do share.",
                location,
                conformed.partitionColumns().keySet(),
                PartitionConfig.CONFIG_PARTITION_SAMPLE_SIZE
            );
            return List.of(response);
        }
        // One example per column is what a reader needs to find the folder; the walk stops once every column has
        // one, so a dataset whose values all fit pays one pass and a dataset whose values do not pays less.
        Map<String, Object> examples = new LinkedHashMap<>();
        PartitionConfig partitionConfig = PartitionConfig.fromConfig(handed.config());
        // Read from the paths, like the values themselves: a bounded listing carries no parsed values to compare
        // against, but it still names its files. A column is reported when the path says something and the
        // declared type could not hold it - which is exactly the case that reads null without saying so.
        for (int file = 0; file < listed.fileCount() && examples.size() < conformed.partitionColumns().size(); file++) {
            StoragePath path = listed.path(file);
            for (String column : conformed.partitionColumns().keySet()) {
                if (conformed.getValue(file, path, column) != null) {
                    continue;
                }
                String token = PartitionMetadata.tokenFor(path, column, partitionConfig);
                if (token != null) {
                    examples.putIfAbsent(column, token);
                }
            }
        }
        // Keyed on the columns rather than the example values: a dataset with many folders that do not fit would
        // otherwise pick a different example on each query and defeat the throttle.
        if (examples.isEmpty()) {
            return List.of();
        }
        String response = Strings.format(
            "partition values outside the sampled paths do not fit the type the sample produced, so those rows read "
                + "null for them: %s. Raise [%s] so the type is decided over them.",
            examples,
            PartitionConfig.CONFIG_PARTITION_SAMPLE_SIZE
        );
        if (warnings.firstInWindow(location + "|does-not-fit|" + examples.keySet())) {
            LOGGER.warn(
                "[{}]: partition values outside the sampled paths do not fit the type the sample produced, so those "
                    + "files read null for them: {}. Raise [{}] so the type is decided over them.",
                location,
                examples,
                PartitionConfig.CONFIG_PARTITION_SAMPLE_SIZE
            );
        }
        return List.of(response);
    }

    /**
     * How far this query's own listing has to run.
     * <p>
     * A demand the budget can use is covered by some prefix of the dataset, because rows come from whichever files the
     * listing returns and no command between the limit and the relation changes how many come out. So the listing can
     * stop early, and {@link ExternalSourceSettings#FIRST_ATTEMPT_LISTING_FILES} is the guess at how early.
     * <p>
     * It is only a guess: how many rows a file holds is read from its footer, after the listing. A prefix that turns
     * out to hold too few rows is not a slow answer but a wrong one, so {@link #discoverSplits} plans over the prefix,
     * asks the budget whether it was covered, and lists the whole dataset if it was not. That retry is what makes the
     * guess safe to make.
     * <p>
     * It declines wherever the budget is known before any footer is read to be unable to stop the scan, because a
     * prefix could then never cover the demand and a bounded attempt would only be a listing thrown away before the
     * real one:
     * <ul>
     *   <li>no usable demand, or an error policy that may drop rows - {@link RowBudget#of}'s own first two guards;</li>
     *   <li>a format that plans without record counts. Only a {@link RangeAwareFormatReader} plans from a footer that
     *       says how many rows each unit holds; text formats plan whole files or probe record boundaries, and the
     *       budget gives up on the first such unit. Declining on the reader's kind is conservative in one direction
     *       only - a range-aware reader whose units turn out uncountable still gets the retry, which keeps it correct;
     *       it just costs a listing;</li>
     *   <li>a list with no file to learn the format from.</li>
     * </ul>
     */
    private ListingExtents listingExtentsForTheDemand(SplitDiscoveryContext context) {
        FileList handed = context.fileList();
        if (handed.fileCount() == 0) {
            return ListingExtents.UNBOUNDED;
        }
        FormatReader reader = resolveConfiguredReader(handed.path(0), context.config());
        if (reader instanceof RangeAwareFormatReader == false) {
            return ListingExtents.UNBOUNDED;
        }
        if (RowBudget.of(context, reader).usableForABoundedListing() == false) {
            return ListingExtents.UNBOUNDED;
        }
        return new ListingExtents(ExternalSourceSettings.FIRST_ATTEMPT_LISTING_FILES.get(settings));
    }

    /**
     * Reserves this query's own listing. The context carries the reservation when the query has one; a provider
     * reached outside a query (tests) reserves nothing.
     */
    private static PlanningMemory scanMemory(SplitDiscoveryContext context) {
        return context.listingMemory() == null ? PlanningMemory.NONE : context.listingMemory();
    }

    /**
     * Lists the dataset this query reads, narrowing by the filters bound to this relation occurrence. Those filters
     * are this occurrence's alone, so unlike the pre-analysis extraction — which serves every occurrence of a path
     * with one listing and must therefore intersect them — narrowing to them starves no sibling branch.
     */

    private FileList listForQuery(SplitDiscoveryContext context, ListingExtents extents) throws Exception {
        String pattern = context.metadata() == null ? null : context.metadata().location();
        Map<String, Object> config = context.config();
        StorageProvider provider = null;
        // Take the identities the registry reports when it makes the provider, rather than letting the listing key
        // default them: resolution's listing is cached under what its provider reported, and a scan that listed the
        // same pattern under a different identity would cache a second copy and never be served the first.
        String storageIdentity = "";
        String secretIdentity = "";
        if (pattern != null && storageRegistry != null) {
            Configured<StorageProvider> resolved = storageRegistry.createProviderTrackingConsumedKeys(
                StoragePath.of(pattern).scheme(),
                settings,
                config
            );
            provider = resolved.value();
            storageIdentity = resolved.identity();
            secretIdentity = resolved.secretIdentity();
        }
        if (provider == null) {
            // Returning what we were handed would turn a prefix of the dataset into the query's file set, and the
            // query would answer from part of it without saying so. A schema's listing is not a scan's: if the scan
            // cannot discover its own files, it must not pretend the schema's listing will do.
            throw new IllegalStateException(
                "cannot discover the files for ["
                    + pattern
                    + "]: the schema's listing covers part of the dataset and no storage provider is available to "
                    + "list the rest"
            );
        }
        StoragePath storagePath = StoragePath.of(pattern);
        try {
            PartitionMetadata partitionInfo = context.partitionInfo();
            Set<String> partitionKeys = partitionInfo == null ? Set.of() : partitionInfo.partitionColumns().keySet();
            List<PartitionFilterHintExtractor.PartitionFilterHint> hints = PartitionFilterHintExtractor.fromConjuncts(
                context.filterHints(),
                context.metadataColumnNames(),
                partitionKeys
            );
            List<PartitionFilterHintExtractor.PartitionFilterHint> narrowing = hints.isEmpty() ? null : hints;
            if (extents.boundsFileSet()) {
                // Never the cache: what comes back is a prefix of the dataset, and the cache's entries are answers
                // other queries are served whole. This query may read a prefix because its own demand is covered by
                // one; the next query's demand is not this one's.
                return listingService.expand(
                    pattern,
                    provider,
                    narrowing,
                    config,
                    storagePath,
                    extents,
                    scanMemory(context),
                    context.isCancelled()
                );
            }
            // The whole pattern either way, so it is cacheable: the query's file set is the dataset's, narrowed by
            // filters the cache key already distinguishes. Without this a warm second query over the same dataset
            // pays the listing again, where resolution's own listing would have been served from the cache.
            return listingService.isCacheable(provider)
                ? listingService.cachedListing(
                    pattern,
                    storagePath,
                    provider,
                    storageIdentity,
                    secretIdentity,
                    narrowing,
                    config,
                    scanMemory(context),
                    context.isCancelled()
                )
                : listingService.expand(
                    pattern,
                    provider,
                    narrowing,
                    config,
                    storagePath,
                    ListingExtents.UNBOUNDED,
                    scanMemory(context),
                    context.isCancelled()
                );
        } finally {
            StorageProviderCache.closeLease(provider);
        }
    }

    @Override
    public SplitDiscoveryResult discoverSplits(SplitDiscoveryContext handedContext) {
        if (handedContext.fileList() == null || handedContext.fileList().isResolved() == false) {
            return SplitDiscoveryResult.EMPTY;
        }
        ListingExtents extents = listingExtentsForTheDemand(handedContext);
        Attempt attempt = discoverSplitsOver(handedContext, extents);
        // Only a listing that actually stopped short can have missed rows. Asking for a bound and getting the whole
        // dataset back - a dataset smaller than the bound - leaves nothing to list again, and retrying there would
        // plan every file a second time for no reason.
        if (attempt.listingStoppedShort() == false || attempt.coveredTheDemand()) {
            return attempt.result();
        }
        // The prefix held fewer rows than the query asked for, so it is not this query's file set after all: reading it
        // would answer LIMIT n with fewer than n rows and say nothing. Everything the first attempt planned is
        // discarded and the dataset is listed in full. The cost of guessing wrong is one extra listing request; the
        // cost of not retrying would be a short answer.
        // Checked here because the retry is a second listing of the whole dataset, and the walk itself takes no
        // cancellation - so this is the last point before committing to it. That the walk cannot be interrupted is
        // older than the bounded attempt and unchanged by it; what is new is that there can be two of them.
        throwIfCancelled(handedContext);
        LOGGER.debug(
            () -> Strings.format(
                "a prefix of [%s] did not cover the query's demand of %d rows; listing the whole dataset",
                handedContext.metadata() == null ? "?" : handedContext.metadata().location(),
                handedContext.rowLimit()
            )
        );
        return discoverSplitsOver(handedContext, ListingExtents.UNBOUNDED).result();
    }

    /**
     * One pass of discovery: whether the listing it ran over stopped short of the dataset, and whether the rows it
     * planned reached what the query asked for. Together they decide whether the prefix was this query's file set
     * after all - short and covered is an answer, short and uncovered has to be thrown away.
     */
    private record Attempt(SplitDiscoveryResult result, boolean listingStoppedShort, boolean coveredTheDemand) {}

    private Attempt discoverSplitsOver(SplitDiscoveryContext handedContext, ListingExtents extents) {
        final SplitDiscoveryContext context;
        try {
            context = overTheQuerysFileSet(handedContext, extents);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } catch (Exception e) {
            // The listing cache reports a loader failure as a checked ExecutionException, and nothing above this
            // takes one.
            throw ExceptionsHelper.convertToRuntime(e);
        }
        final FileList fileList = context.fileList();

        Map<String, Object> config = context.config();
        long requestedStrideBytes = resolveTargetSplitSize(config);
        int maxSplitProbes = resolveMaxSplitProbes(config);
        long probeWindowBytes = resolveSplitProbeWindow(config);
        validateProbeBudget(probeWindowBytes, maxSplitProbes);

        StorageProvider sharedProvider = hoistSharedProvider(fileList, config);

        try {
            throwIfCancelled(context);

            SurvivorBatch batch = buildSurvivors(context, requestedStrideBytes);
            int certifiedSkips = batch.certifiedSkips();
            long probedFileBytes = batch.probedFileBytes();

            if (batch.size() == 0) {
                // Exhaustive only when every file was dropped by a certified row-count-preserving skip.
                // An unresolved or already-empty file list is not a prune (fileCount == 0). A skip that
                // is not counted above leaves certifiedSkips < fileCount and falls back to a full read.
                boolean exhaustivelyPruned = fileList.fileCount() > 0 && certifiedSkips == fileList.fileCount();
                // No rows were planned, so a demand above zero was not covered. Over a prefix that sends the caller
                // back for the whole dataset; over a complete listing the flag is never read.
                return new Attempt(
                    resultOver(context, List.of(), exhaustivelyPruned, 0L, discoveryWarnings.get(), 0),
                    fileList.isTruncated(),
                    false
                );
            }

            // Phase 2: I/O-bound split planning, parallelized across files when an executor is available. Files
            // whose record boundaries still need probing come back as deferred descriptors; everything else
            // (Parquet footers, block-aligned compressed, range splits, whole-file) finishes here.
            final StorageProvider hoistedProvider = sharedProvider;
            final BooleanSupplier isCancelled = context.isCancelled();
            final long strideBytes = strideBoundedByProbeBudget(requestedStrideBytes, probedFileBytes, maxSplitProbes);
            warnIfStrideWidened(requestedStrideBytes, strideBytes, maxSplitProbes, probedFileBytes);
            // Only single split discovery is performed on each FileSplitProvider at a time
            splitDiscoveryCpuNanos.set(0L);
            splitDiscoveryProbes.set(0);
            demandTruncatedFiles.clear();
            List<PlanResult> planResults;
            int survivorCount = batch.size();
            // A file is planned only while the rows already covered fall short of what the query asked for; see
            // RowBudget for the three ways that arithmetic fails closed.
            RowBudget budget = RowBudget.of(context, resolveConfiguredReader(fileList.path(0), config));
            try {
                if (executor != null && survivorCount > 1) {
                    planResults = BoundedParallelGather.gather(slotList(survivorCount), slot -> {
                        long cpuStart = ThreadCpuTimer.currentNanos();
                        try {
                            if (budget.satisfied()) {
                                return new PlanResult.Splits(List.of());
                            }
                            PlanResult planned = planSurvivor(batch, slot, hoistedProvider, strideBytes, isCancelled);
                            budget.account(planned);
                            return planned;
                        } finally {
                            if (cpuStart >= 0) splitDiscoveryCpuNanos.addAndGet(ThreadCpuTimer.elapsedNanos(cpuStart));
                        }
                    }, splitDiscoveryConcurrency(), executor);
                } else {
                    planResults = new ArrayList<>(survivorCount);
                    for (int slot = 0; slot < survivorCount; slot++) {
                        if (budget.satisfied()) {
                            planResults.add(new PlanResult.Splits(List.of()));
                            continue;
                        }
                        PlanResult planned = planSurvivor(batch, slot, hoistedProvider, strideBytes, isCancelled);
                        budget.account(planned);
                        planResults.add(planned);
                    }
                }
            } catch (Exception e) {
                throw ExternalFailures.surface(e, "Failed to discover splits");
            }

            // Phase 3: spend one demand-sized cut budget in listing order. Files past that budget stay
            // whole-file. Unlimited quoted files already walked in Phase 2.
            List<PlanResult> planned = new ArrayList<>(planResults);
            int remainingCuts = remainingDemandCuts(context, planned);
            try {
                remainingCuts = walkDeferredQuoted(planned, remainingCuts, isCancelled);
            } catch (Exception e) {
                throw ExternalFailures.surface(e, "Failed to discover splits");
            }
            Map<DeferredNewlineSplits, List<RecordBoundaryProbe.Outcome>> probedOutcomes = probeDeferredBoundaries(
                planned,
                probeWindowBytes,
                isCancelled,
                remainingCuts
            );

            // Phase 4: turn the plan results into splits, now that every boundary either was known at planning time
            // or has been probed.
            List<ExternalSplit> splits = splitsFromPlanResults(planned, probedOutcomes);

            // Each surviving file produces at least one split, so the survivor count is the number of
            // distinct files that are actually scanned after coordinator-side pruning.
            return new Attempt(
                resultOver(context, splits, false, splitDiscoveryCpuNanos.get(), discoveryWarnings.get(), splitDiscoveryProbes.get()),
                fileList.isTruncated(),
                budget.satisfied()
            );
        } finally {
            StorageProviderCache.closeLease(sharedProvider);
        }
    }

    /**
     * Non-joining Phase-2 discovery. Phase-1 filtering stays on the calling thread (no object-store IO).
     * Per-file planning and probes fan out through {@link ThrottledIterator}; the caller must not await.
     * Production wires {@code esql_external_io} as {@code requestedExecutor}.
     * <p>
     * When the listing we were handed answered the schema rather than the scan, discovering the query's own file
     * set is object-store IO, so that and everything after it move to {@code requestedExecutor} — the calling
     * thread still does none.
     */
    @Override
    public void discoverSplitsAsync(
        SplitDiscoveryContext handedContext,
        Executor requestedExecutor,
        ActionListener<SplitDiscoveryResult> listener
    ) {
        if (handedContext.fileList() == null || handedContext.fileList().isResolved() == false) {
            listener.onResponse(SplitDiscoveryResult.EMPTY);
            return;
        }
        if (DatasetDiscovery.shared(handedContext.fileList()).schemaListingIsComplete()) {
            // The listing is the query's file set already: nothing to discover, so nothing leaves this thread that
            // did not leave it before. Taking the same decision as the sync path through the same helper below is
            // what keeps the two entry points on one rule; this branch only decides which thread asks.
            planSplitsAsync(handedContext, requestedExecutor, listener.map(Attempt::result));
            return;
        }
        // Otherwise the file set is a walk of the object store, and this method's contract is that the calling thread
        // waits for no such thing. The same bounded first attempt and the same retry as discoverSplits: this is the
        // path production takes, so a rule that only the sync path applied would be a rule production never runs.
        ListingExtents extents = listingExtentsForTheDemand(handedContext);
        attemptAsync(handedContext, extents, requestedExecutor, listener.delegateFailureAndWrap((l, attempt) -> {
            if (attempt.listingStoppedShort() == false || attempt.coveredTheDemand()) {
                l.onResponse(attempt.result());
                return;
            }
            // See discoverSplits, including why cancellation is checked before committing to a second listing.
            throwIfCancelled(handedContext);
            LOGGER.debug(
                () -> Strings.format(
                    "a prefix of [%s] did not cover the query's demand of %d rows; listing the whole dataset",
                    handedContext.metadata() == null ? "?" : handedContext.metadata().location(),
                    handedContext.rowLimit()
                )
            );
            attemptAsync(handedContext, ListingExtents.UNBOUNDED, requestedExecutor, l.map(Attempt::result));
        }));
    }

    /** One async pass: list under {@code extents} off the calling thread, then plan over what was listed. */
    private void attemptAsync(
        SplitDiscoveryContext handedContext,
        ListingExtents extents,
        Executor requestedExecutor,
        ActionListener<Attempt> listener
    ) {
        discoveryFanOutExecutor(requestedExecutor).execute(
            ActionRunnable.wrap(
                listener,
                attempted -> planSplitsAsync(overTheQuerysFileSet(handedContext, extents), requestedExecutor, attempted)
            )
        );
    }

    /**
     * Whether the planned files hold at least the rows the query asked for, by the budget's own arithmetic replayed
     * over what was planned. Replayed rather than read off the budget that stopped the scan, because on the async path
     * that budget lives inside the gather and never reaches here; the same {@link RowBudget} rules - including every
     * way it fails closed - decide both, so the two cannot disagree about what counts.
     */
    private boolean coversTheDemand(SplitDiscoveryContext context, List<PlanResult> planResults) {
        FileList fileList = context.fileList();
        RowBudget replay = RowBudget.of(
            context,
            fileList.fileCount() > 0 ? resolveConfiguredReader(fileList.path(0), context.config()) : null
        );
        for (PlanResult planned : planResults) {
            replay.account(planned);
        }
        return replay.satisfied();
    }

    /** Phase-1 filtering and the Phase-2 fan-out, over a context whose file set is the query's own. */
    private void planSplitsAsync(SplitDiscoveryContext context, Executor requestedExecutor, ActionListener<Attempt> listener) {
        final FileList fileList = context.fileList();

        Map<String, Object> config = context.config();
        final long requestedStrideBytes;
        final int maxSplitProbes;
        final long probeWindowBytes;
        try {
            requestedStrideBytes = resolveTargetSplitSize(config);
            maxSplitProbes = resolveMaxSplitProbes(config);
            probeWindowBytes = resolveSplitProbeWindow(config);
            validateProbeBudget(probeWindowBytes, maxSplitProbes);
        } catch (Exception e) {
            listener.onFailure(e);
            return;
        }

        StorageProvider sharedProvider = null;
        boolean asyncStarted = false;
        try {
            sharedProvider = hoistSharedProvider(fileList, config);
            throwIfCancelled(context);
            SurvivorBatch batch = buildSurvivors(context, requestedStrideBytes);
            if (batch.size() == 0) {
                boolean exhaustivelyPruned = fileList.fileCount() > 0 && batch.certifiedSkips() == fileList.fileCount();
                // No rows were planned, so a demand above zero was not covered; see the sync path's same exit.
                listener.onResponse(
                    new Attempt(
                        resultOver(context, List.of(), exhaustivelyPruned, 0L, discoveryWarnings.get(), 0),
                        fileList.isTruncated(),
                        false
                    )
                );
                return;
            }

            final StorageProvider hoistedProvider = sharedProvider;
            final BooleanSupplier isCancelled = context.isCancelled();
            final long strideBytes = strideBoundedByProbeBudget(requestedStrideBytes, batch.probedFileBytes(), maxSplitProbes);
            warnIfStrideWidened(requestedStrideBytes, strideBytes, maxSplitProbes, batch.probedFileBytes());
            splitDiscoveryCpuNanos.set(0L);
            splitDiscoveryProbes.set(0);
            demandTruncatedFiles.clear();
            Executor fanOut = recordingDiscoveryCpu(withStorageRetryCancellation(discoveryFanOutExecutor(requestedExecutor), isCancelled));
            ActionListener<Attempt> completion = ActionListener.runAfter(listener, () -> StorageProviderCache.closeLease(hoistedProvider));
            gatherSkippingCachedFooters(
                batch,
                hoistedProvider,
                strideBytes,
                isCancelled,
                fanOut,
                ActionListener.<List<PlanResult>>wrap(planResults -> {
                    List<PlanResult> planned = new ArrayList<>(planResults);
                    int remainingCuts;
                    try {
                        remainingCuts = remainingDemandCuts(context, planned);
                        remainingCuts = walkDeferredQuoted(planned, remainingCuts, isCancelled);
                    } catch (Exception e) {
                        completion.onFailure(ExternalFailures.surface(e, "Failed to discover splits"));
                        return;
                    }
                    probeDeferredBoundariesAsync(
                        planned,
                        probeWindowBytes,
                        isCancelled,
                        fanOut,
                        remainingCuts,
                        ActionListener.wrap(probedOutcomes -> {
                            try {
                                if (isCancelled.getAsBoolean()) {
                                    completion.onFailure(new TaskCancelledException(RecordBoundaryProbe.CANCELLED_MESSAGE));
                                    return;
                                }
                                List<ExternalSplit> splits = splitsFromPlanResults(planned, probedOutcomes);
                                completion.onResponse(
                                    new Attempt(
                                        resultOver(
                                            context,
                                            splits,
                                            false,
                                            splitDiscoveryCpuNanos.get(),
                                            discoveryWarnings.get(),
                                            splitDiscoveryProbes.get()
                                        ),
                                        fileList.isTruncated(),
                                        coversTheDemand(context, planned)
                                    )
                                );
                            } catch (Exception e) {
                                completion.onFailure(ExternalFailures.surface(e, "Failed to discover splits"));
                            }
                        }, e -> completion.onFailure(ExternalFailures.surface(e, "Failed to discover splits")))
                    );
                }, e -> completion.onFailure(ExternalFailures.surface(e, "Failed to discover splits")))
            );
            asyncStarted = true;
        } catch (Exception e) {
            listener.onFailure(e);
        } finally {
            if (asyncStarted == false) {
                StorageProviderCache.closeLease(sharedProvider);
            }
        }
    }

    private StorageProvider hoistSharedProvider(FileList fileList, Map<String, Object> config) {
        if (config != null && config.isEmpty() == false && storageRegistry != null && fileList.fileCount() > 0) {
            return storageRegistry.createProvider(fileList.path(0).scheme(), settings, config);
        }
        return null;
    }

    /**
     * Phase-1 survivors: file-list indices and one frozen partition map each. Shared query state
     * stays on the batch. {@link FileTask} shells are built later, only for a slot that is planning.
     */
    private record SurvivorBatch(
        int[] fileIndices,
        List<Map<String, Object>> partitionValues,
        int certifiedSkips,
        long probedFileBytes,
        SplitDiscoveryContext context,
        Map<String, DataType> reconciledTypes,
        Map<ColumnMapping, ColumnMapping> mappingCache,
        boolean anchorPinnedFirstFileWins,
        ExternalSchema fileBackedQuerySchema
    ) {
        int size() {
            return fileIndices.length;
        }
    }

    /**
     * Per-file stamp used to emit a cache-hit split without allocating a {@link FileTask}.
     */
    private record ResolvedFile(
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
        @Nullable Map<String, DataType> inferredFileTypes,
        @Nullable SourceStatistics statistics,
        @Nullable Map<String, Object> foldedSourceMetadata,
        boolean unknownNativeTypes
    ) {
        private FileTask toTask() {
            return new FileTask(
                filePath,
                fileLength,
                format,
                config,
                partitionValues,
                columnMapping,
                readSchema,
                reconciledTypes,
                maxRecordBytes,
                declaredReadSpec,
                inferredFileTypes,
                statistics,
                foldedSourceMetadata,
                unknownNativeTypes
            );
        }
    }

    /**
     * Phase 1: sequential in-memory filter. No object-store IO. The filter loop reuses one scratch map
     * (hive values copied by reference, {@code _file.*} written in place) so a bound filter can see the
     * listing keys it names. Path, name, and directory are written only when such a filter reads them.
     * The map stored on the survivor is never that scratch and never keeps those three keys: files in one
     * directory share one unmodifiable tuple of directory-constant keys, and per-file keys sit on a
     * {@link LayeredPartitionMap}. No {@link FileTask}.
     */
    private SurvivorBatch buildSurvivors(SplitDiscoveryContext context, long requestedStrideBytes) {
        FileList fileList = context.fileList();
        PartitionMetadata partitionInfo = context.partitionInfo();
        Map<String, Object> config = context.config();
        List<Expression> filterHints = context.filterHints();
        ExternalSchema fileBackedQuerySchema = stripPartitionColumns(context.querySchema(), partitionInfo);
        Map<StoragePath, SchemaReconciliation.FileSchemaInfo> schemaInfo = context.schemaMap();
        Map<ColumnMapping, ColumnMapping> mappingCache = new ConcurrentHashMap<>();
        ExternalSchema unifiedSchema = context.unifiedSchema();
        boolean anchorPinnedFirstFileWins = ExternalSourceResolver.isAnchorPinnedFirstFileWins(
            fileList.originalPattern(),
            config,
            context.declaredReadSpec()
        );
        Set<String> metadataColumnNames = context.metadataColumnNames();
        Set<String> retainedPartitionKeys = context.retainedPartitionKeys();
        PartitionSpec spec = PartitionSpec.fromConfig(config);
        PartitionValueLayout layout = PartitionValueLayout.of(retainedPartitionKeys, partitionInfo);

        int fileCount = fileList.fileCount();
        int certifiedSkips = 0;
        long probedFileBytes = 0;
        int[] fileIndices = new int[fileCount];
        ArrayList<Map<String, Object>> partitionValues = new ArrayList<>(fileCount);
        // Survivor maps never store path, name, or directory. A bound filter still reads them from the
        // scratch. Directory BytesRefs are interned for that scan only, one per parent, and die with this
        // method. Full path URIs are never interned.
        boolean knownProjectionWithoutFilter = retainedPartitionKeys != null && filterHints.isEmpty();
        Map<String, Integer> hiveColumnIndex = hiveColumnIndex(partitionInfo);
        Map<String, Map<String, Object>> directoryTuples = new HashMap<>();
        LinkedHashMap<String, Object> scratch = new LinkedHashMap<>();
        int survivors = 0;
        // Unified schema is query-wide. One unmodifiable map is shared by every file; the
        // concurrent split path only reads it.
        Map<String, DataType> reconciledTypes = unifiedSchema == null ? null : Map.copyOf(attributesToTypeMap(unifiedSchema.attributes()));
        // Hive / _file.* listing values live in the temporary map the filter reads. Copy and strip
        // unbound _file.* (or overlay engine per-file constants) only when a hint names one of those keys.
        boolean overlayPerFileConstants = referencedNames(
            filterHints,
            namesInBoth(metadataColumnNames, ExternalMetadataColumns.PER_FILE_CONSTANT_NAMES)
        ).isEmpty() == false;
        Set<String> unboundFileMetadataNames = Set.of();
        if (filterHints.isEmpty() == false) {
            unboundFileMetadataNames = new LinkedHashSet<>();
            for (String name : FileMetadataColumns.NAMES) {
                if (metadataColumnNames.contains(name) == false) {
                    unboundFileMetadataNames.add(name);
                }
            }
        }
        boolean copyFilterValues = overlayPerFileConstants || referencedNames(filterHints, unboundFileMetadataNames).isEmpty() == false;
        // Only the location names a bound filter actually reads. A hive-only filter, or a name-only
        // filter, does not allocate the other location strings.
        Set<String> locationToWrite = referencedNames(filterHints, namesInBoth(metadataColumnNames, FileMetadataColumns.LOCATION_NAMES));
        Map<String, BytesRef> filterDirectoryIntern = locationToWrite.contains(FileMetadataColumns.DIRECTORY) ? new HashMap<>() : null;
        IdentityHashMap<Expression, ByteRunAutomaton> regexAutomata = new IdentityHashMap<>();
        for (int i = 0; i < fileCount; i++) {
            StoragePath filePath = fileList.path(i);
            Map<String, Object> frozen;
            if (knownProjectionWithoutFilter) {
                // No hint reads the listing map, so only the retained keys are built. An empty set is Map.of().
                frozen = layout.isEmpty()
                    ? Map.of()
                    : composeSurvivorPartitionMap(filePath, fileList, i, partitionInfo, layout, hiveColumnIndex, directoryTuples);
            } else {
                scratch.clear();
                if (partitionInfo != null && partitionInfo.isEmpty() == false) {
                    // Copy references only. Do not mutate the listing arrays.
                    partitionInfo.putValues(i, filePath, scratch);
                }
                long modifiedMillis = fileList.lastModifiedMillis(i);
                Instant modified = modifiedMillis == 0L ? null : Instant.ofEpochMilli(modifiedMillis);
                FileMetadataColumns.putValues(scratch, filePath, fileList.size(i), modified, filterDirectoryIntern, locationToWrite);
                spec.aliasIdentityValues(scratch);
                // Filter against the scratch. The survivor map is the shared tuple or the overlay view, never this map.
                Map<String, Object> listingValues = Collections.unmodifiableMap(scratch);
                SchemaReconciliation.FileSchemaInfo fileSchemaInfo = schemaInfo.get(filePath);

                if (filterHints.isEmpty() == false) {
                    Map<String, Object> filterValues = copyFilterValues
                        ? discoveryFilterValues(listingValues, metadataColumnNames, overlayPerFileConstants, unboundFileMetadataNames)
                        : listingValues;
                    if (filterValues.isEmpty() == false && matchesPartitionFilters(filterValues, filterHints, regexAutomata) == false) {
                        certifiedSkips++;
                        continue;
                    }
                    if (spec.overlapsExpressions(scratch, filterHints) == false) {
                        certifiedSkips++;
                        continue;
                    }
                    if (fileSchemaInfo != null) {
                        Set<String> fileColumnNames = new LinkedHashSet<>(fileSchemaInfo.fileSchema().names());
                        fileColumnNames.addAll(filterValues.keySet());
                        fileColumnNames.addAll(metadataColumnNames);
                        // _file.record_ref is composed per row, so it is present on every file whatever the
                        // file schema lists. The standard names are per-file constants and reach
                        // fileColumnNames through filterValues above, when bound as metadata.
                        fileColumnNames.add(FileMetadataColumns.RECORD_REF);
                        if (skipIfFilterOnMissingColumns(filterHints, fileColumnNames)) {
                            certifiedSkips++;
                            continue;
                        }
                    }
                }
                frozen = layout.isEmpty()
                    ? Map.of()
                    : composeSurvivorPartitionMap(filePath, fileList, i, partitionInfo, layout, hiveColumnIndex, directoryTuples);
            }

            long fileLength = fileList.size(i);
            if (fileLength > requestedStrideBytes && isNewlineMacroSplitCandidateExtension(extensionFormat(filePath))) {
                probedFileBytes += fileLength;
            }

            fileIndices[survivors++] = i;
            partitionValues.add(frozen);
        }
        if (survivors != fileCount) {
            fileIndices = Arrays.copyOf(fileIndices, survivors);
        }
        partitionValues.trimToSize();
        return new SurvivorBatch(
            fileIndices,
            partitionValues,
            certifiedSkips,
            probedFileBytes,
            context,
            reconciledTypes,
            mappingCache,
            anchorPinnedFirstFileWins,
            fileBackedQuerySchema
        );
    }

    /** Not a partition value. Marks a directory key this file does not include. */
    private static final Object ABSENT = new Object();

    /**
     * Directory-constant keys are interned per parent directory. Hive values are directory-bound; a file whose
     * tuple disagrees keeps a private map, and later files that match the first tuple still share it. A later
     * file compares against that tuple before allocating another map. Per-file keys sit on a sized overlay.
     * An empty overlay returns the shared tuple itself so siblings stay {@code ==}.
     */
    private static Map<String, Object> composeSurvivorPartitionMap(
        StoragePath filePath,
        FileList fileList,
        int index,
        @Nullable PartitionMetadata partitionInfo,
        PartitionValueLayout layout,
        Map<String, Integer> hiveColumnIndex,
        Map<String, Map<String, Object>> directoryTuples
    ) {
        boolean keepNulls = layout.keepNulls();
        int resolved = -1;
        if (partitionInfo != null && partitionInfo.isEmpty() == false) {
            resolved = partitionInfo.resolveFileIndex(index, filePath);
        }
        StoragePath parent = filePath.parentDirectory();
        String parentKey = parent == null ? null : parent.toString();
        Map<String, Object> existing = directoryTuples.get(parentKey);
        Map<String, Object> shared = existing != null
            && sameDirectoryTuple(existing, resolved, partitionInfo, layout, hiveColumnIndex, keepNulls)
                ? existing
                : publishDirectoryTuple(filePath, resolved, partitionInfo, layout, hiveColumnIndex, keepNulls, directoryTuples);
        Map<String, Object> overlay = perFileOverlay(fileList, index, layout, keepNulls);
        if (shared.isEmpty() && overlay.isEmpty()) {
            return Map.of();
        }
        if (shared.isEmpty()) {
            return overlay;
        }
        if (overlay.isEmpty()) {
            return shared;
        }
        return new LayeredPartitionMap(shared, overlay);
    }

    private static boolean sameDirectoryTuple(
        Map<String, Object> existing,
        int resolved,
        @Nullable PartitionMetadata partitionInfo,
        PartitionValueLayout layout,
        Map<String, Integer> hiveColumnIndex,
        boolean keepNulls
    ) {
        int included = 0;
        for (String key : layout.directoryKeys()) {
            Object value = directoryKeyValue(key, resolved, partitionInfo, hiveColumnIndex, keepNulls);
            if (value == ABSENT) {
                if (existing.containsKey(key)) {
                    return false;
                }
                continue;
            }
            included++;
            if (existing.containsKey(key) == false || Objects.equals(existing.get(key), value) == false) {
                return false;
            }
        }
        return included == existing.size();
    }

    private static Map<String, Object> publishDirectoryTuple(
        StoragePath filePath,
        int resolved,
        @Nullable PartitionMetadata partitionInfo,
        PartitionValueLayout layout,
        Map<String, Integer> hiveColumnIndex,
        boolean keepNulls,
        Map<String, Map<String, Object>> directoryTuples
    ) {
        List<String> keys = layout.directoryKeys();
        if (keys.isEmpty()) {
            return Map.of();
        }
        LinkedHashMap<String, Object> directory = Maps.newLinkedHashMapWithExpectedSize(keys.size());
        for (String key : keys) {
            Object value = directoryKeyValue(key, resolved, partitionInfo, hiveColumnIndex, keepNulls);
            if (value != ABSENT) {
                directory.put(key, value);
            }
        }
        if (directory.isEmpty()) {
            return Map.of();
        }
        return internDirectoryTuple(filePath, directory, directoryTuples);
    }

    /** The hive value {@code key} would take on this file, or {@link #ABSENT} when the key is not stored. */
    private static Object directoryKeyValue(
        String key,
        int resolved,
        @Nullable PartitionMetadata partitionInfo,
        Map<String, Integer> hiveColumnIndex,
        boolean keepNulls
    ) {
        if (resolved < 0) {
            return ABSENT;
        }
        Integer column = hiveColumnIndex.get(key);
        if (column == null) {
            return ABSENT;
        }
        Object value = partitionInfo.getValueAt(resolved, column);
        if (value == null && keepNulls == false) {
            return ABSENT;
        }
        return value;
    }

    private static Map<String, Object> perFileOverlay(FileList fileList, int index, PartitionValueLayout layout, boolean keepNulls) {
        List<String> keys = layout.perFileKeys();
        if (keys.isEmpty()) {
            return Map.of();
        }
        LinkedHashMap<String, Object> overlay = Maps.newLinkedHashMapWithExpectedSize(keys.size());
        for (String key : keys) {
            switch (key) {
                case FileMetadataColumns.SIZE -> overlay.put(key, fileList.size(index));
                case FileMetadataColumns.MODIFIED -> {
                    long modifiedMillis = fileList.lastModifiedMillis(index);
                    if (modifiedMillis != 0L) {
                        overlay.put(key, modifiedMillis);
                    } else if (keepNulls) {
                        overlay.put(key, null);
                    }
                }
                default -> throw new IllegalStateException("unexpected per-file partition key [" + key + "]");
            }
        }
        if (overlay.isEmpty()) {
            return Map.of();
        }
        return Collections.unmodifiableMap(overlay);
    }

    private static Map<String, Integer> hiveColumnIndex(@Nullable PartitionMetadata partitionInfo) {
        if (partitionInfo == null || partitionInfo.isEmpty()) {
            return Map.of();
        }
        Map<String, Integer> index = new HashMap<>();
        int column = 0;
        for (String key : partitionInfo.partitionColumns().keySet()) {
            index.put(key, column++);
        }
        return index;
    }

    /**
     * One unmodifiable tuple per parent directory. The first file publishes it. A later file with the same
     * values reuses it; a disagreement keeps a private map for that file only.
     */
    private static Map<String, Object> internDirectoryTuple(
        StoragePath filePath,
        LinkedHashMap<String, Object> directory,
        Map<String, Map<String, Object>> directoryTuples
    ) {
        StoragePath parent = filePath.parentDirectory();
        String parentKey = parent == null ? null : parent.toString();
        Map<String, Object> existing = directoryTuples.get(parentKey);
        if (existing != null && existing.equals(directory)) {
            return existing;
        }
        Map<String, Object> frozen = Collections.unmodifiableMap(directory);
        if (existing == null && directoryTuples.containsKey(parentKey) == false) {
            directoryTuples.put(parentKey, frozen);
        }
        return frozen;
    }

    @Nullable
    private static String extensionFormat(StoragePath filePath) {
        String objectName = filePath.objectName();
        if (objectName == null) {
            return null;
        }
        int lastDot = objectName.lastIndexOf('.');
        if (lastDot >= 0 && lastDot < objectName.length() - 1) {
            return objectName.substring(lastDot);
        }
        return null;
    }

    /** Survivor slots {@code 0 .. count-1}. Boxing happens when a slot is read, not as a {@link FileTask} list. */
    private static List<Integer> slotList(int count) {
        return new AbstractList<>() {
            @Override
            public int size() {
                return count;
            }

            @Override
            public Integer get(int index) {
                return index;
            }
        };
    }

    private FileTask openFileTask(SurvivorBatch batch, int slot) {
        FileTask task = resolveSurvivor(batch, slot).toTask();
        trackFileTaskCreated();
        return task;
    }

    /**
     * Builds a {@link FileTask} for one survivor slot, plans it, and drops the shell when planning returns.
     * Cancellation is checked before the shell exists and again before any read.
     */
    private PlanResult planSurvivor(
        SurvivorBatch batch,
        int slot,
        @Nullable StorageProvider hoistedProvider,
        long strideBytes,
        BooleanSupplier isCancelled
    ) throws IOException {
        if (isCancelled.getAsBoolean()) {
            throw new TaskCancelledException(RecordBoundaryProbe.CANCELLED_MESSAGE);
        }
        FileTask task = openFileTask(batch, slot);
        try {
            if (isCancelled.getAsBoolean()) {
                throw new TaskCancelledException(RecordBoundaryProbe.CANCELLED_MESSAGE);
            }
            return processFileForSplits(task, hoistedProvider, strideBytes, isCancelled, batch.context().rowLimit());
        } finally {
            releaseFileTask();
        }
    }

    private ResolvedFile resolveSurvivor(SurvivorBatch batch, int slot) {
        SplitDiscoveryContext context = batch.context();
        FileList fileList = context.fileList();
        int fileIndex = batch.fileIndices()[slot];
        StoragePath filePath = fileList.path(fileIndex);
        long fileLength = fileList.size(fileIndex);
        SchemaReconciliation.FileSchemaInfo fileSchemaInfo = context.schemaMap().get(filePath);
        ColumnMapping columnMapping = null;
        List<Attribute> readSchema = null;
        Map<String, DataType> inferredFileTypes = null;
        SourceStatistics fileStatistics = null;
        ExternalSchema unifiedSchema = context.unifiedSchema();
        if (fileSchemaInfo != null) {
            inferredFileTypes = fileSchemaInfo.inferredTypes();
            fileStatistics = fileSchemaInfo.statistics();
            ColumnMapping mapping = fileSchemaInfo.mapping();
            if (mapping != null && unifiedSchema != null && batch.fileBackedQuerySchema().isEmpty() == false) {
                mapping = mapping.pruneToPerFileQuery(unifiedSchema, fileSchemaInfo.fileSchema(), batch.fileBackedQuerySchema());
            }
            if (mapping != null && mapping.isIdentity() == false) {
                columnMapping = batch.mappingCache().computeIfAbsent(mapping, k -> k);
            }
            readSchema = fileSchemaInfo.fileSchema().attributes();
        }
        boolean unknownNativeTypes = ExternalSourceResolver.nativeTypesUnknown(fileSchemaInfo, batch.anchorPinnedFirstFileWins());
        return new ResolvedFile(
            filePath,
            fileLength,
            extensionFormat(filePath),
            context.config(),
            batch.partitionValues().get(slot),
            columnMapping,
            readSchema,
            batch.reconciledTypes(),
            context.maxRecordBytes(),
            context.declaredReadSpec(),
            inferredFileTypes,
            fileStatistics,
            context.metadata() == null ? null : context.metadata().sourceMetadata(),
            unknownNativeTypes
        );
    }

    /**
     * Logs a stride the probe budget widened. It goes to the node log rather than the query's response: results
     * are unaffected, and the split size in use is the operator's concern rather than the query author's.
     */
    private static void warnIfStrideWidened(long requestedStrideBytes, long strideBytes, int maxSplitProbes, long probedFileBytes) {
        if (strideBytes <= requestedStrideBytes) {
            return;
        }
        LOGGER.warn(
            "[{}] of [{}] raised to [{}] to stay within [{}] of [{}] over [{}] of files",
            CONFIG_TARGET_SPLIT_SIZE,
            ByteSizeValue.ofBytes(requestedStrideBytes),
            ByteSizeValue.ofBytes(strideBytes),
            CONFIG_MAX_SPLIT_PROBES,
            maxSplitProbes,
            ByteSizeValue.ofBytes(probedFileBytes)
        );
    }

    private Executor discoveryFanOutExecutor(Executor requestedExecutor) {
        if (requestedExecutor != null) {
            return requestedExecutor;
        }
        if (executor != null) {
            return executor;
        }
        return EsExecutors.DIRECT_EXECUTOR_SERVICE;
    }

    /**
     * Installs {@link StorageRetryCancellation} on every task {@code executor} runs so blocking
     * {@code discoverSplitRanges} leftover paths (ORC, text probes, {@code file://}) abort retry
     * backoff the same way sync {@link #processFileForSplits} wraps {@link #computeFileSplits}.
     */
    private static Executor withStorageRetryCancellation(Executor executor, BooleanSupplier isCancelled) {
        ExternalPlanningIo planningIo = ExternalPlanningIo.current();
        return ExternalIoExecutors.preserving(ExternalIoExecutors.restoring(executor, null, isCancelled), command -> {
            try (Releasable ignored = ExternalPlanningIo.activate(planningIo)) {
                command.run();
            }
        });
    }

    /**
     * Records {@link ThreadCpuTimer} for every task that lands on the Phase-2 fan-out executor,
     * including footer parse after an async GET and text planning that never probes.
     * Nested executes on the same thread share one interval so {@code DIRECT} does not double-count.
     */
    private Executor recordingDiscoveryCpu(Executor inner) {
        return ExternalIoExecutors.preserving(inner, this::runRecordingDiscoveryCpu);
    }

    private void runRecordingDiscoveryCpu(Runnable work) {
        if (DISCOVERY_CPU_TIMING.get()) {
            work.run();
            return;
        }
        DISCOVERY_CPU_TIMING.set(Boolean.TRUE);
        long cpuStart = ThreadCpuTimer.currentNanos();
        try {
            work.run();
        } finally {
            DISCOVERY_CPU_TIMING.set(Boolean.FALSE);
            if (cpuStart >= 0) {
                splitDiscoveryCpuNanos.addAndGet(ThreadCpuTimer.elapsedNanos(cpuStart));
            }
        }
    }

    private static <T, R> void gatherAsync(
        List<T> items,
        BiConsumer<T, ActionListener<R>> fn,
        int maxConcurrency,
        Executor executor,
        ActionListener<List<R>> listener
    ) {
        int size = items.size();
        if (size == 0) {
            listener.onResponse(List.of());
            return;
        }
        AtomicReferenceArray<R> results = new AtomicReferenceArray<>(size);
        AtomicReference<Exception> failure = new AtomicReference<>();
        ThrottledIterator.run(indexIterator(size), (releasable, i) -> {
            if (failure.get() != null) {
                releasable.close();
                return;
            }
            ActionListener<R> itemListener = ActionListener.runAfter(
                ActionListener.wrap(r -> results.set(i, r), e -> failure.compareAndSet(null, e)),
                releasable::close
            );
            try {
                fn.accept(items.get(i), itemListener);
            } catch (Exception e) {
                itemListener.onFailure(e);
            }
        }, Math.max(1, maxConcurrency), () -> {
            Exception e = failure.get();
            if (e != null) {
                listener.onFailure(e);
                return;
            }
            List<R> out = new ArrayList<>(size);
            for (int i = 0; i < size; i++) {
                out.add(results.get(i));
            }
            listener.onResponse(out);
        }, executor, e -> failure.compareAndSet(null, e));
    }

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
     * Phase-2 fan-out: parsed-footer cache hits skip {@link ThrottledIterator} so they do not occupy
     * GET permits. Misses (and formats with no cache) keep the existing throttled {@code readBytesAsync}
     * path. The cache partition is CPU-only (hash lookups, no I/O) and runs inline on the caller —
     * the same thread that previously ran {@link #gatherAsync} directly. Hits emit splits from path,
     * length, and config. Misses are survivor-slot indices; a {@link FileTask} exists only while its
     * throttle slot is in flight.
     */
    private void gatherSkippingCachedFooters(
        SurvivorBatch batch,
        @Nullable StorageProvider hoistedProvider,
        long strideBytes,
        BooleanSupplier isCancelled,
        Executor fanOut,
        ActionListener<List<PlanResult>> listener
    ) {
        try {
            int n = batch.size();
            PlanResult[] slots = new PlanResult[n];
            int[] missSlots = new int[n];
            int[] missCount = new int[1];
            FileList fileList = batch.context().fileList();
            Map<String, Object> config = batch.context().config();
            runRecordingDiscoveryCpu(() -> {
                for (int i = 0; i < n; i++) {
                    if (isCancelled.getAsBoolean()) {
                        throw new TaskCancelledException(RecordBoundaryProbe.CANCELLED_MESSAGE);
                    }
                    int fileIndex = batch.fileIndices()[i];
                    StoragePath path = fileList.path(fileIndex);
                    long length = fileList.size(fileIndex);
                    List<SplitRange> cached = peekCachedSplitRanges(path, length, config, hoistedProvider);
                    if (cached != null) {
                        slots[i] = planResultFromCachedRanges(resolveSurvivor(batch, i), cached);
                    } else {
                        missSlots[missCount[0]++] = i;
                    }
                }
            });
            // Cached ranges cost nothing to plan, so they count towards the demand before any file is opened.
            RowBudget budget = RowBudget.of(
                batch.context(),
                fileList.fileCount() > 0 ? resolveConfiguredReader(fileList.path(0), config) : null
            );
            for (int i = 0; i < n; i++) {
                if (slots[i] != null) {
                    budget.account(slots[i]);
                }
            }
            int misses = missCount[0];
            if (misses == 0) {
                listener.onResponse(List.of(slots));
                return;
            }
            int[] missed = Arrays.copyOf(missSlots, misses);
            int gatherConcurrency = planningDiscoveryConcurrency(batch, missed, hoistedProvider);
            planningGatherConcurrency = gatherConcurrency;
            gatherAsync(slotList(misses), (Integer ordinal, ActionListener<PlanResult> itemListener) -> {
                if (isCancelled.getAsBoolean()) {
                    itemListener.onFailure(new TaskCancelledException(RecordBoundaryProbe.CANCELLED_MESSAGE));
                    return;
                }
                // The rows already planned cover what the query asked for, so this file is not opened and produces
                // no splits. Work already in flight finishes, which is why the overshoot is one gather window rather
                // than the rest of the dataset.
                if (budget.satisfied()) {
                    itemListener.onResponse(new PlanResult.Splits(List.of()));
                    return;
                }
                FileTask task = openFileTask(batch, missed[ordinal]);
                // Release the shell before the throttle permit is returned, and only once if execute
                // both runs the item and then surfaces an exception.
                AtomicBoolean notified = new AtomicBoolean();
                ActionListener<PlanResult> released = new ActionListener<>() {
                    private void finish(Runnable notify) {
                        if (notified.compareAndSet(false, true) == false) {
                            return;
                        }
                        releaseFileTask();
                        notify.run();
                    }

                    @Override
                    public void onResponse(PlanResult result) {
                        budget.account(result);
                        finish(() -> itemListener.onResponse(result));
                    }

                    @Override
                    public void onFailure(Exception e) {
                        finish(() -> itemListener.onFailure(e));
                    }
                };
                try {
                    if (isCancelled.getAsBoolean()) {
                        released.onFailure(new TaskCancelledException(RecordBoundaryProbe.CANCELLED_MESSAGE));
                        return;
                    }
                    fanOut.execute(
                        () -> processFileForSplitsAsync(
                            task,
                            hoistedProvider,
                            strideBytes,
                            isCancelled,
                            fanOut,
                            batch.context().rowLimit(),
                            released
                        )
                    );
                } catch (Exception e) {
                    released.onFailure(e);
                }
            }, gatherConcurrency, fanOut, ActionListener.wrap(missResults -> {
                for (int j = 0; j < missResults.size(); j++) {
                    slots[missed[j]] = missResults.get(j);
                }
                listener.onResponse(List.of(slots));
            }, listener::onFailure));
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    /**
     * Listing-seeded {@link StorageObject#length()} plus {@link RangeAwareFormatReader#cachedSplitRanges}.
     * CPU-only: {@code cachedSplitRanges} must not read. Any failure is a miss so the throttled path can surface it.
     */
    @Nullable
    private List<SplitRange> peekCachedSplitRanges(
        StoragePath path,
        long length,
        Map<String, Object> config,
        @Nullable StorageProvider hoistedProvider
    ) {
        try {
            FormatReader reader = resolveConfiguredReader(path, config);
            if (reader instanceof RangeAwareFormatReader rangeReader) {
                StorageProvider provider = resolveProvider(path, config, hoistedProvider);
                StorageObject object = provider.newObject(path, length);
                return rangeReader.cachedSplitRanges(object);
            }
            return null;
        } catch (Exception e) {
            LOGGER.debug(
                () -> Strings.format("Footer cache peek failed for [%s]; falling back to throttled discovery", path.objectName()),
                e
            );
            return null;
        }
    }

    private PlanResult planResultFromCachedRanges(ResolvedFile file, List<SplitRange> ranges) {
        if (ranges.isEmpty()) {
            return new PlanResult.Splits(
                List.of(
                    wholeFileSplit(
                        file.filePath(),
                        file.fileLength(),
                        file.format(),
                        file.config(),
                        file.partitionValues(),
                        file.columnMapping(),
                        file.readSchema()
                    )
                )
            );
        }
        List<ExternalSplit> splits = new ArrayList<>(ranges.size());
        addRangeAwareSplits(
            file.filePath(),
            file.fileLength(),
            file.format(),
            file.config(),
            file.partitionValues(),
            file.columnMapping(),
            file.readSchema(),
            file.reconciledTypes(),
            file.declaredReadSpec(),
            file.inferredFileTypes(),
            ranges,
            splits,
            file.foldedSourceMetadata(),
            implicitNullsFor(file),
            file.unknownNativeTypes()
        );
        return new PlanResult.Splits(splits);
    }

    private boolean implicitNullsFor(ResolvedFile file) {
        FormatReader reader = resolveConfiguredReader(file.filePath(), file.config());
        return reader != null && implicitNullsFor(reader);
    }

    /**
     * Assembles the query's splits from the planned files and the probed boundaries, and reports what the
     * boundary search failed to cut.
     * <p>
     * Splits come out in file order: walking the plan results keeps a probed file's macro-splits in the position
     * its file occupied in the file list.
     */
    private List<ExternalSplit> splitsFromPlanResults(
        List<PlanResult> planResults,
        Map<DeferredNewlineSplits, List<RecordBoundaryProbe.Outcome>> probedOutcomes
    ) {
        List<ExternalSplit> splits = new ArrayList<>();
        SplitShortfall shortfall = new SplitShortfall();
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
                    if (demandTruncatedFiles.contains(deferred) == false) {
                        shortfall.recordProbed(deferred, outcomes, starts);
                    }
                    splits.addAll(buildNewlineMacroSplits(deferred, starts));
                }
                case PlanResult.NeedsWalk needsWalk -> throw new IllegalStateException(
                    "quoted walk still pending for " + needsWalk.deferred().task().filePath()
                );
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

        private long offsetsProbed;
        private long offsetsWithoutBoundary;
        private int filesAffected;
        private int filesReadWhole;
        private DeferredNewlineSplits firstAffected;

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
            note(deferred, readWhole);
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
            note(walked.deferred(), readWhole);
        }

        private void note(DeferredNewlineSplits deferred, boolean readWhole) {
            filesAffected++;
            if (readWhole) {
                filesReadWhole++;
            }
            if (firstAffected == null) {
                firstAffected = deferred;
            }
        }

        /**
         * Logs the shortfall rather than adding it to the query's response: results are unaffected, and the only
         * consequence, fewer splits to run in parallel, is the operator's to act on.
         */
        void warnIfAny() {
            if (firstAffected == null) {
                return;
            }
            // Every file of a query is cut at the same stride, so the one named here is the stride of all of
            // them. It is the stride they were cut at rather than the one the query asked for, which differ when
            // the probe count widened it.
            LOGGER.warn(
                "[{}] file(s) were cut into fewer splits than a [{}] split size gives, [{}] of them into a single split{}; "
                    + "e.g. [{}] ({}); the query may run slower",
                filesAffected,
                ByteSizeValue.ofBytes(firstAffected.strideBytes()),
                filesReadWhole,
                probedOffsetDetail(),
                firstAffected.task().filePath(),
                ByteSizeValue.ofBytes(firstAffected.task().fileLength())
            );
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
     * Probes the record boundaries of deferred files, keyed by the descriptor they belong to.
     * <p>
     * Under demand, only a leading prefix of offsets is probed, in listing order, from the leftover
     * cut budget after quoted walks. Files past that budget become whole-file splits with no probes.
     * If the wave finds no boundary for a file, the rest of that file's grid is probed so a run of
     * NONE cannot collapse it.
     * <p>
     * With an executor, selected offsets share {@link #splitDiscoveryConcurrency()}. Without one, they run
     * serially. Both produce the same per-offset outcomes; the caller reduces them to split starts.
     *
     * @param probeWindowBytes the bytes each of these probes may read, from {@link #CONFIG_SPLIT_PROBE_WINDOW}
     */
    private Map<DeferredNewlineSplits, List<RecordBoundaryProbe.Outcome>> probeDeferredBoundaries(
        List<PlanResult> planResults,
        long probeWindowBytes,
        BooleanSupplier isCancelled,
        int remainingCuts
    ) {
        Map<DeferredNewlineSplits, List<Long>> remainingPositions = new IdentityHashMap<>();
        List<ProbeTask> wave = selectLeadingProbeTasks(planResults, remainingCuts, remainingPositions);
        if (wave.isEmpty()) {
            return Map.of();
        }
        if (isCancelled.getAsBoolean()) {
            throw new TaskCancelledException(RecordBoundaryProbe.CANCELLED_MESSAGE);
        }
        try {
            List<RecordBoundaryProbe.Outcome> waveOutcomes = runProbeTasks(wave, probeWindowBytes, isCancelled);
            List<ProbeTask> fallback = fallbackProbeTasks(wave, waveOutcomes, remainingPositions);
            List<RecordBoundaryProbe.Outcome> fallbackOutcomes = fallback.isEmpty()
                ? List.of()
                : runProbeTasks(fallback, probeWindowBytes, isCancelled);
            if (isCancelled.getAsBoolean()) {
                throw new TaskCancelledException(RecordBoundaryProbe.CANCELLED_MESSAGE);
            }
            recordPooledProbeCount(wave.size() + fallback.size());
            Map<DeferredNewlineSplits, List<RecordBoundaryProbe.Outcome>> outcomesByFile = groupProbeOutcomes(
                wave,
                waveOutcomes,
                fallback,
                fallbackOutcomes
            );
            return outcomesByFile;
        } catch (Exception e) {
            throw ExternalFailures.surface(e, "Failed to discover splits");
        }
    }

    private void probeDeferredBoundariesAsync(
        List<PlanResult> planResults,
        long probeWindowBytes,
        BooleanSupplier isCancelled,
        Executor fanOut,
        int remainingCuts,
        ActionListener<Map<DeferredNewlineSplits, List<RecordBoundaryProbe.Outcome>>> listener
    ) {
        Map<DeferredNewlineSplits, List<Long>> remainingPositions = new IdentityHashMap<>();
        List<ProbeTask> wave = selectLeadingProbeTasks(planResults, remainingCuts, remainingPositions);
        if (wave.isEmpty()) {
            listener.onResponse(Map.of());
            return;
        }
        if (isCancelled.getAsBoolean()) {
            listener.onFailure(new TaskCancelledException(RecordBoundaryProbe.CANCELLED_MESSAGE));
            return;
        }
        runProbeTasksAsync(wave, probeWindowBytes, isCancelled, fanOut, ActionListener.wrap(waveOutcomes -> {
            List<ProbeTask> fallback = fallbackProbeTasks(wave, waveOutcomes, remainingPositions);
            if (fallback.isEmpty()) {
                finishProbeAsync(wave, waveOutcomes, List.of(), List.of(), isCancelled, listener);
                return;
            }
            runProbeTasksAsync(
                fallback,
                probeWindowBytes,
                isCancelled,
                fanOut,
                ActionListener.wrap(
                    fallbackOutcomes -> finishProbeAsync(wave, waveOutcomes, fallback, fallbackOutcomes, isCancelled, listener),
                    listener::onFailure
                )
            );
        }, listener::onFailure));
    }

    private void finishProbeAsync(
        List<ProbeTask> wave,
        List<RecordBoundaryProbe.Outcome> waveOutcomes,
        List<ProbeTask> fallback,
        List<RecordBoundaryProbe.Outcome> fallbackOutcomes,
        BooleanSupplier isCancelled,
        ActionListener<Map<DeferredNewlineSplits, List<RecordBoundaryProbe.Outcome>>> listener
    ) {
        if (isCancelled.getAsBoolean()) {
            listener.onFailure(new TaskCancelledException(RecordBoundaryProbe.CANCELLED_MESSAGE));
            return;
        }
        recordPooledProbeCount(wave.size() + fallback.size());
        listener.onResponse(groupProbeOutcomes(wave, waveOutcomes, fallback, fallbackOutcomes));
    }

    /**
     * Stride cuts a demand-limited scan may still issue, counting each planned file as a starting
     * point. {@link FormatReader#NO_LIMIT} is unbounded. Zero means every file stays whole-file.
     */
    private int remainingDemandCuts(SplitDiscoveryContext context, List<PlanResult> planned) {
        long stride = firstStride(planned);
        return ExternalLimitSplits.demandCuts(
            context.rowLimit(),
            ExternalLimitSplits.DEFAULT_PAGE_SIZE_ROWS,
            context.taskConcurrency(),
            planned.size(),
            stride > 0 ? stride : DEFAULT_TARGET_SPLIT_SIZE,
            rowBytes(context)
        );
    }

    private static long firstStride(List<PlanResult> planned) {
        for (PlanResult planResult : planned) {
            if (planResult instanceof PlanResult.NeedsProbing needsProbing) {
                return needsProbing.deferred().strideBytes();
            }
            if (planResult instanceof PlanResult.NeedsWalk needsWalk) {
                return needsWalk.deferred().strideBytes();
            }
        }
        return 0L;
    }

    /**
     * Bytes per row used to size {@code ceil(rowLimit * rowBytes / stride)}. Declared mappings keep
     * {@link ExternalLimitSplits#DEFAULT_ROW_BYTES}; inferred schemas use the sample width when present.
     */
    static long rowBytes(SplitDiscoveryContext context) {
        if (context.declaredReadSpec() != null && context.declaredReadSpec().isEmpty() == false) {
            return ExternalLimitSplits.DEFAULT_ROW_BYTES;
        }
        SourceMetadata metadata = context.metadata();
        if (metadata != null) {
            long sampleBytes = metadata.sampleBytes();
            int sampleRows = metadata.sampleRows();
            if (sampleBytes > 0 && sampleRows > 0) {
                return Math.max(1L, Math.ceilDiv(sampleBytes, (long) sampleRows));
            }
        }
        return ExternalLimitSplits.DEFAULT_ROW_BYTES;
    }

    /**
     * How many leading strided probe offsets a demand-limited scan may issue for one file.
     * Under demand this is {@link ExternalLimitSplits#demandCuts}, not a concurrency floor:
     * a single-driver LIMIT issues zero cuts. Gated on {@code rowLimit !=} {@link FormatReader#NO_LIMIT}.
     */
    int probeWaveSize(int rowLimit, long strideBytes, int positionCount) {
        return probeWaveSize(rowLimit, strideBytes, positionCount, ExternalLimitSplits.DEFAULT_TASK_CONCURRENCY, 1);
    }

    int probeWaveSize(int rowLimit, long strideBytes, int positionCount, int taskConcurrency, int fileCount) {
        if (positionCount <= 0) {
            return 0;
        }
        int cuts = ExternalLimitSplits.demandCuts(
            rowLimit,
            ExternalLimitSplits.DEFAULT_PAGE_SIZE_ROWS,
            taskConcurrency,
            fileCount,
            strideBytes,
            ExternalLimitSplits.DEFAULT_ROW_BYTES
        );
        if (cuts == Integer.MAX_VALUE) {
            return positionCount;
        }
        return Math.min(positionCount, cuts);
    }

    /**
     * Cap on proven-walk starts under demand, including the file start at 0.
     * {@link #probeWaveSize} counts remaining cuts (0 is implicit in {@link RecordBoundaryProbe#reduce}).
     * The walk's {@code maxBoundaries} includes 0, so the cap is cuts+1.
     */
    int provenBoundaryCap(int rowLimit, long strideBytes) {
        return provenBoundaryCap(rowLimit, strideBytes, ExternalLimitSplits.DEFAULT_TASK_CONCURRENCY, 1);
    }

    int provenBoundaryCap(int rowLimit, long strideBytes, int taskConcurrency, int fileCount) {
        int cuts = ExternalLimitSplits.demandCuts(
            rowLimit,
            ExternalLimitSplits.DEFAULT_PAGE_SIZE_ROWS,
            taskConcurrency,
            fileCount,
            strideBytes,
            ExternalLimitSplits.DEFAULT_ROW_BYTES
        );
        if (cuts == Integer.MAX_VALUE) {
            return Integer.MAX_VALUE;
        }
        return cuts + 1;
    }

    /**
     * Takes a listing-order prefix of strided offsets totalling the leftover cut budget, and rewrites files
     * past that budget to whole-file splits with no probes. Partial files keep their leftover positions for
     * {@link #fallbackProbeTasks}. Files rewritten to whole-file never re-enter fallback: leftover offsets
     * on a later file would spend GETs the demand already decided not to spend, and would cut a file the
     * query will not read past the LIMIT (2174-safe).
     */
    private List<ProbeTask> selectLeadingProbeTasks(
        List<PlanResult> planResults,
        int remainingCuts,
        Map<DeferredNewlineSplits, List<Long>> remainingPositions
    ) {
        int totalPositions = 0;
        for (PlanResult planResult : planResults) {
            if (planResult instanceof PlanResult.NeedsProbing needsProbing) {
                totalPositions += needsProbing.deferred().positions().size();
            }
        }
        int waveSize = remainingCuts == Integer.MAX_VALUE ? totalPositions : Math.min(totalPositions, Math.max(0, remainingCuts));
        List<ProbeTask> wave = new ArrayList<>(waveSize);
        int remainingBudget = waveSize;
        for (int i = 0; i < planResults.size(); i++) {
            if (planResults.get(i) instanceof PlanResult.NeedsProbing needsProbing) {
                DeferredNewlineSplits deferred = needsProbing.deferred();
                List<Long> positions = deferred.positions();
                if (remainingBudget == 0) {
                    planResults.set(i, new PlanResult.Splits(buildNewlineMacroSplits(deferred, List.of(0L))));
                    continue;
                }
                int take = Math.min(remainingBudget, positions.size());
                for (int p = 0; p < take; p++) {
                    wave.add(new ProbeTask(deferred, positions.get(p)));
                }
                remainingBudget -= take;
                if (take < positions.size()) {
                    remainingPositions.put(deferred, positions.subList(take, positions.size()));
                    demandTruncatedFiles.add(deferred);
                }
            }
        }
        return wave;
    }

    private void recordPooledProbeCount(int issued) {
        assert issued <= MAX_SPLIT_PROBES_CEILING : "pooled probe count [" + issued + "] above the ceiling";
        splitDiscoveryProbes.addAndGet(issued);
    }

    /**
     * When a truncated file's wave found no boundary, probe the rest of that file's grid so a run of NONE
     * cannot collapse it into a single whole-file split. Files past the listing-order budget are rewritten
     * to whole-file splits before this runs, so they never appear in {@code remainingPositions}.
     */
    private List<ProbeTask> fallbackProbeTasks(
        List<ProbeTask> wave,
        List<RecordBoundaryProbe.Outcome> waveOutcomes,
        Map<DeferredNewlineSplits, List<Long>> remainingPositions
    ) {
        if (remainingPositions.isEmpty()) {
            return List.of();
        }
        Map<DeferredNewlineSplits, List<RecordBoundaryProbe.Outcome>> byFile = new IdentityHashMap<>();
        for (int i = 0; i < wave.size(); i++) {
            byFile.computeIfAbsent(wave.get(i).deferred(), k -> new ArrayList<>()).add(waveOutcomes.get(i));
        }
        List<ProbeTask> fallback = new ArrayList<>();
        for (Map.Entry<DeferredNewlineSplits, List<Long>> entry : remainingPositions.entrySet()) {
            DeferredNewlineSplits deferred = entry.getKey();
            List<Long> starts = RecordBoundaryProbe.reduce(byFile.getOrDefault(deferred, List.of()));
            if (starts.size() <= 1) {
                demandTruncatedFiles.remove(deferred);
                for (long position : entry.getValue()) {
                    fallback.add(new ProbeTask(deferred, position));
                }
            }
        }
        return fallback;
    }

    private static Map<DeferredNewlineSplits, List<RecordBoundaryProbe.Outcome>> groupProbeOutcomes(
        List<ProbeTask> wave,
        List<RecordBoundaryProbe.Outcome> waveOutcomes,
        List<ProbeTask> fallback,
        List<RecordBoundaryProbe.Outcome> fallbackOutcomes
    ) {
        Map<DeferredNewlineSplits, List<RecordBoundaryProbe.Outcome>> outcomesByFile = new IdentityHashMap<>();
        for (int i = 0; i < wave.size(); i++) {
            outcomesByFile.computeIfAbsent(wave.get(i).deferred(), k -> new ArrayList<>()).add(waveOutcomes.get(i));
        }
        for (int i = 0; i < fallback.size(); i++) {
            outcomesByFile.computeIfAbsent(fallback.get(i).deferred(), k -> new ArrayList<>()).add(fallbackOutcomes.get(i));
        }
        return outcomesByFile;
    }

    private List<RecordBoundaryProbe.Outcome> runProbeTasks(List<ProbeTask> tasks, long probeWindowBytes, BooleanSupplier isCancelled)
        throws Exception {
        if (executor == null) {
            List<RecordBoundaryProbe.Outcome> outcomes = new ArrayList<>(tasks.size());
            for (ProbeTask task : tasks) {
                outcomes.add(runProbe(task, probeWindowBytes, isCancelled));
            }
            return outcomes;
        }
        return runGather(tasks, probe -> runProbe(probe, probeWindowBytes, isCancelled), splitDiscoveryConcurrency(), executor);
    }

    private void runProbeTasksAsync(
        List<ProbeTask> tasks,
        long probeWindowBytes,
        BooleanSupplier isCancelled,
        Executor fanOut,
        ActionListener<List<RecordBoundaryProbe.Outcome>> listener
    ) {
        gatherAsync(tasks, (ProbeTask probe, ActionListener<RecordBoundaryProbe.Outcome> itemListener) -> {
            try {
                fanOut.execute(() -> {
                    try {
                        itemListener.onResponse(runProbe(probe, probeWindowBytes, isCancelled));
                    } catch (Exception e) {
                        itemListener.onFailure(e);
                    }
                });
            } catch (Exception e) {
                itemListener.onFailure(e);
            }
        }, splitDiscoveryConcurrency(), fanOut, listener);
    }

    private <T, R> List<R> runGather(List<T> items, CheckedFunction<T, R, Exception> task, int concurrency, Executor executor)
        throws Exception {
        if (BoundedParallelGather.executesInline(items)) {
            return BoundedParallelGather.gather(items, task, concurrency, executor);
        } else {
            return BoundedParallelGather.gather(items, (T probe) -> {
                long cpuStart = ThreadCpuTimer.currentNanos();
                try {
                    return task.apply(probe);
                } finally {
                    if (cpuStart >= 0) splitDiscoveryCpuNanos.addAndGet(ThreadCpuTimer.elapsedNanos(cpuStart));
                }
            }, concurrency, executor);
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
     * How many split-discovery reads may be in flight at once for leftover pinning paths: ORC, text
     * probes, {@code file://}, and {@code gs}. Bounded by {@link #MAX_PARALLEL_SPLIT_DISCOVERY} and
     * clamped to the node's blob-store concurrency because a pinning read holds one of those permits
     * (and an {@code esql_external_io} thread) for as long as its stream is open.
     * <p>
     * The clamp is per query where the permits are per node and per scheme. A configured concurrency
     * of {@code 0} disables permit limiting altogether rather than meaning "no concurrency", so the
     * ceiling applies as-is.
     * <p>
     * Parquet planning that {@link StorageObject#readBytesAsyncReleasesExecutor() releases the executor} uses
     * {@link ExternalSourceSettings#externalIoThreads} instead, via {@link #planningDiscoveryConcurrency}.
     * Probes and sync {@link BoundedParallelGather} keep this 16-pin ceiling.
     * <p>
     * Production {@link #discoverSplitsAsync} must not join: the caller of Phase-2 is {@code SEARCH} or
     * {@code esql_external_io}, and neither may sit in a gather latch. {@code SEARCH} and {@code GENERIC}
     * must not issue these GETs.
     */
    int splitDiscoveryConcurrency() {
        int permits = ExternalSourceSettings.blobStoreConcurrency(settings);
        return permits > 0 ? Math.min(MAX_PARALLEL_SPLIT_DISCOVERY, permits) : MAX_PARALLEL_SPLIT_DISCOVERY;
    }

    /**
     * Planning {@link #gatherAsync} concurrency: when every miss is Parquet
     * ({@link FormatNameResolver#resolveFormatName} equals {@link FormatNameResolver#FORMAT_PARQUET},
     * so {@code .parq} counts) and
     * {@link StorageObject#readBytesAsyncReleasesExecutor()} on one peeked
     * {@link StorageProvider#newObject} per distinct scheme,
     * {@link ExternalSourceSettings#externalIoThreads} (never 0). Otherwise
     * {@link #splitDiscoveryConcurrency()}. A null {@code formatRegistry} or any
     * {@code newObject} / resolve failure is conservative (16).
     * Samples survivor indices, not a {@link FileTask} list.
     * Probes and sync {@link BoundedParallelGather} keep {@link #splitDiscoveryConcurrency()}.
     */
    private int planningDiscoveryConcurrency(SurvivorBatch batch, int[] missSlots, @Nullable StorageProvider hoistedProvider) {
        if (formatRegistry == null) {
            return splitDiscoveryConcurrency();
        }
        try {
            FileList fileList = batch.context().fileList();
            Map<String, Object> config = batch.context().config();
            Set<String> seenSchemes = new HashSet<>();
            for (int slot : missSlots) {
                int fileIndex = batch.fileIndices()[slot];
                StoragePath path = fileList.path(fileIndex);
                if (FormatNameResolver.FORMAT_PARQUET.equals(
                    FormatNameResolver.resolveFormatName(config, path.objectName(), formatRegistry)
                ) == false) {
                    return splitDiscoveryConcurrency();
                }
                String scheme = path.scheme();
                if (seenSchemes.add(scheme) == false) {
                    continue;
                }
                StorageProvider provider = resolveProvider(path, config, hoistedProvider);
                StorageObject object = provider.newObject(path, fileList.size(fileIndex));
                if (object.readBytesAsyncReleasesExecutor() == false) {
                    return splitDiscoveryConcurrency();
                }
            }
            return ExternalSourceSettings.externalIoThreads(settings);
        } catch (Exception e) {
            return splitDiscoveryConcurrency();
        }
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
        // Native file types, physical-keyed. Null when this file's types were not obtained or when
        // fileSchema itself is native. The stats-type authority for normalizing footer range stats,
        // not the overlaid or pinned readSchema types.
        @Nullable Map<String, DataType> inferredFileTypes,
        // File-level statistics: a live harvest from this query's schema resolution, or the same
        // harvest reconstructed from the schema cache's flat _stats.* map. Null when this file was
        // never harvested (no cache entry). tryRangeAwareSplits emits a whole-file split without
        // opening the footer again only when readableUnitCount is 1 and the harvest has column
        // statistics. A slim record still reports a unit count of 1 and must not skip.
        @Nullable SourceStatistics statistics,
        // Coordinator fold (sourceMetadata on the relation). Copied onto each harvest or
        // footer range so split merge cannot serve a column the fold already dropped.
        @Nullable Map<String, Object> foldedSourceMetadata,
        // True when this FIRST_FILE_WINS glob file has no native-type snapshot. Column statistics
        // must be withheld before alignment can interpret them against the pinned read schema.
        boolean unknownNativeTypes
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
     * probing, a quoted file waiting for the listing-order walk, or a sequential walk that has already
     * resolved them. Deferring strided probing lets every strided file's probes share a single concurrency
     * budget. Deferring quoted walks under demand lets every quoted file share one W of cuts in listing
     * order; unlimited quoted files still walk in Phase 2 so the fan-out stays parallel. Mixed
     * strided+quoted queries spend a separate W on each path.
     */
    private sealed interface PlanResult {
        /** A file whose splits are settled, because planning already did whatever reading they needed. */
        record Splits(List<ExternalSplit> splits) implements PlanResult {}

        /** A file whose macro-splits can only be built once the probe phase has resolved its record boundaries. */
        record NeedsProbing(DeferredNewlineSplits deferred) implements PlanResult {}

        /**
         * A quoted or escaped file whose sequential walk is deferred until the listing-order budget pass.
         * Unlimited scans never produce this: they walk in Phase 2.
         */
        record NeedsWalk(DeferredNewlineSplits deferred) implements PlanResult {}

        /**
         * A file the sequential walk has already resolved. It carries its starts rather than its splits so that
         * both macro-split paths build splits the same way, and it carries whether the walk gave up early so that
         * a quoted file that under-split is reported alongside the strided ones that did.
         */
        record Walked(DeferredNewlineSplits deferred, List<Long> starts, boolean stoppedBeforeEndOfFile) implements PlanResult {}
    }

    /**
     * How many rows are still wanted, and whether the count can be trusted.
     * <p>
     * A file's planned units say how many records they hold, so producing splits in listing order can stop once the
     * rows already covered reach what the query asked for: every file past that point is one nobody has to open.
     * The arithmetic only holds while three things are true, and each of them fails the budget closed rather than
     * narrowing it — an unusable budget produces every split, exactly as before a limit reached here.
     * <ul>
     *   <li>Nothing between the limit and the relation changes how many rows come out. The walk that recovered the
     *       demand carries it only through commands that promise that, so a filtered or sorted limit never arrives.</li>
     *   <li>The dataset's error policy does not drop rows: under it a unit's record count is what will be decoded,
     *       not what will be emitted.</li>
     *   <li>Every unit planned so far said how many records it holds. One that does not makes the running total a
     *       floor rather than a count, so the budget gives up.</li>
     * </ul>
     */
    private static final class RowBudget {
        private static final RowBudget UNUSABLE = new RowBudget(FormatReader.NO_LIMIT);

        private final long demand;
        private long covered;
        private boolean usable;

        private RowBudget(int demand) {
            // A demand of zero is satisfied before any file is accounted, so the whole planning loop is skipped and
            // the empty result falls through to reading every file in the list. Nothing here would be wrong, but
            // nothing here would be right either: the safety comes from SkipQueryOnLimitZero folding a zero limit
            // away before physical planning, two layers above and with nothing stating the dependency. This says it.
            assert demand == FormatReader.NO_LIMIT || demand > 0
                : "a demand of [" + demand + "] should have been folded away before split discovery";
            this.demand = demand;
            this.usable = demand != FormatReader.NO_LIMIT;
        }

        static RowBudget of(SplitDiscoveryContext context, @Nullable FormatReader reader) {
            if (context.rowLimit() == FormatReader.NO_LIMIT) {
                return UNUSABLE;
            }
            // forReader never returns null: FormatReader#defaultErrorPolicy defaults to STRICT and a null reader
            // resolves to STRICT too, so there is no absent-policy case to guard here. A mock that returns one is
            // a fixture that is not shaped like a reader.
            if (ErrorPolicy.forReader(context.config(), reader).mode() != ErrorPolicy.Mode.FAIL_FAST) {
                return UNUSABLE;
            }
            return new RowBudget(context.rowLimit());
        }

        synchronized boolean satisfied() {
            return usable && covered >= demand;
        }

        /**
         * Whether this budget could ever stop the scan, known without reading a footer. True does not promise the
         * budget will be satisfied - the per-file record counts decide that - only that a prefix of the dataset is
         * worth listing first.
         */
        boolean usableForABoundedListing() {
            return demand != FormatReader.NO_LIMIT;
        }

        /** Folds one planned file in, and gives up if it could not say how many records it holds. */
        synchronized void account(PlanResult result) {
            if (usable == false) {
                return;
            }
            if (result instanceof PlanResult.Splits planned) {
                for (ExternalSplit split : planned.splits()) {
                    SplitStats stats = split.splitStats();
                    long rows = stats == null ? -1 : stats.rowCount();
                    if (rows < 0) {
                        usable = false;
                        return;
                    }
                    covered += rows;
                }
            } else {
                // A file whose splits are settled later cannot be counted now.
                usable = false;
            }
        }
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
     * File types for footer-stat normalization when no declaration overlaid the read schema. Prefers the
     * pre-pin inferred map so a text UNION_BY_NAME pin on {@code readSchema} does not make
     * {@code file == reconciled} and skip conversion; falls back to {@code readSchema} when nothing
     * retyped this file.
     */
    private static Map<String, DataType> undeclaredStatsFileTypes(
        List<Attribute> readSchema,
        @Nullable Map<String, DataType> inferredFileTypes
    ) {
        return inferredFileTypes != null ? inferredFileTypes : attributesToTypeMap(readSchema);
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
        BooleanSupplier isCancelled,
        int rowLimit
    ) throws IOException {
        if (isCancelled.getAsBoolean()) {
            throw new TaskCancelledException(RecordBoundaryProbe.CANCELLED_MESSAGE);
        }
        // Carry the cancellation signal as ambient thread-local state so the synchronous retry/throttle
        // backoff inside the footer reads below can abort a parked sleep on cancel.
        return StorageRetryCancellation.callWithCancellation(
            isCancelled,
            () -> computeFileSplits(task, hoistedProvider, strideBytes, isCancelled, rowLimit)
        );
    }

    private void processFileForSplitsAsync(
        FileTask task,
        @Nullable StorageProvider hoistedProvider,
        long strideBytes,
        BooleanSupplier isCancelled,
        Executor fanOut,
        int rowLimit,
        ActionListener<PlanResult> listener
    ) {
        try {
            if (isCancelled.getAsBoolean()) {
                listener.onFailure(new TaskCancelledException(RecordBoundaryProbe.CANCELLED_MESSAGE));
                return;
            }
            FormatReader configuredReader = resolveConfiguredReader(task.filePath(), task.config());
            if (configuredReader != null && task.declaredReadSpec().provenance() == SchemaProvenance.DECLARED) {
                configuredReader = configuredReader.withDeclaredProvenanceBinding(true);
            }
            if (requiresSequentialWholeFileRead(configuredReader)) {
                listener.onResponse(
                    new PlanResult.Splits(
                        List.of(
                            wholeFileSplit(
                                task.filePath(),
                                task.fileLength(),
                                task.format(),
                                task.config(),
                                task.partitionValues(),
                                task.columnMapping(),
                                task.readSchema()
                            )
                        )
                    )
                );
                return;
            }
            List<ExternalSplit> fileSplits = new ArrayList<>();
            if (StorageRetryCancellation.callWithCancellation(
                isCancelled,
                () -> tryBlockAlignedSplits(
                    task.filePath(),
                    task.fileLength(),
                    task.format(),
                    task.config(),
                    task.partitionValues(),
                    task.columnMapping(),
                    task.readSchema(),
                    fileSplits,
                    hoistedProvider
                )
            )) {
                listener.onResponse(new PlanResult.Splits(fileSplits));
                return;
            }
            final FormatReader readerForText = configuredReader;
            tryRangeAwareSplitsAsync(task, hoistedProvider, fanOut, ActionListener.wrap(rangeSplits -> {
                runRecordingDiscoveryCpu(() -> {
                    try {
                        if (rangeSplits != null) {
                            listener.onResponse(new PlanResult.Splits(rangeSplits));
                            return;
                        }
                        listener.onResponse(
                            StorageRetryCancellation.callWithCancellation(
                                isCancelled,
                                () -> planTextOrWholeFile(task, hoistedProvider, strideBytes, isCancelled, readerForText, rowLimit)
                            )
                        );
                    } catch (Exception e) {
                        listener.onFailure(e);
                    }
                });
            }, listener::onFailure));
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    private PlanResult computeFileSplits(
        FileTask task,
        @Nullable StorageProvider hoistedProvider,
        long strideBytes,
        BooleanSupplier isCancelled,
        int rowLimit
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
            task.statistics(),
            task.foldedSourceMetadata(),
            fileSplits,
            hoistedProvider,
            task.unknownNativeTypes()
        )) {
            return new PlanResult.Splits(fileSplits);
        }

        return planTextOrWholeFile(task, hoistedProvider, strideBytes, isCancelled, configuredReader, rowLimit);
    }

    private PlanResult planTextOrWholeFile(
        FileTask task,
        @Nullable StorageProvider hoistedProvider,
        long strideBytes,
        BooleanSupplier isCancelled,
        @Nullable FormatReader configuredReader,
        int rowLimit
    ) throws IOException {
        List<ExternalSplit> fileSplits = new ArrayList<>();
        DeferredNewlineSplits deferred = newlineMacroSplitCandidate(task, strideBytes, hoistedProvider, configuredReader);
        if (deferred == null) {
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
        if (deferred.positions().isEmpty() == false) {
            return new PlanResult.NeedsProbing(deferred);
        }
        // Unlimited quoted files walk here so Phase 2's fan-out stays parallel. Under demand, defer so
        // every quoted file in listing order spends one shared W of cuts.
        if (rowLimit != FormatReader.NO_LIMIT) {
            return new PlanResult.NeedsWalk(deferred);
        }
        RecordBoundaryProbe.ProvenWalk walk = provenMacroSplitStarts(deferred, isCancelled, Integer.MAX_VALUE);
        return new PlanResult.Walked(deferred, walk.boundaries(), walk.stoppedBeforeEndOfFile());
    }

    /**
     * Spends the leftover demand-sized cut budget across quoted files in listing order. Files past
     * the budget become whole-file splits and never walk, matching the strided path. Unlimited scans
     * never reach here: they walk in Phase 2. Mixed strided+quoted queries share one remainingCuts
     * across this walk then the strided wave.
     */
    private int walkDeferredQuoted(List<PlanResult> planResults, int remainingCuts, BooleanSupplier isCancelled) throws IOException {
        boolean anyWalk = false;
        for (PlanResult planResult : planResults) {
            if (planResult instanceof PlanResult.NeedsWalk) {
                anyWalk = true;
                break;
            }
        }
        if (anyWalk == false) {
            return remainingCuts;
        }
        for (int i = 0; i < planResults.size(); i++) {
            if (planResults.get(i) instanceof PlanResult.NeedsWalk needsWalk) {
                DeferredNewlineSplits deferred = needsWalk.deferred();
                if (remainingCuts <= 0) {
                    planResults.set(i, new PlanResult.Splits(buildNewlineMacroSplits(deferred, List.of(0L))));
                    continue;
                }
                if (isCancelled.getAsBoolean()) {
                    throw new TaskCancelledException(RecordBoundaryProbe.CANCELLED_MESSAGE);
                }
                int cap = remainingCuts == Integer.MAX_VALUE ? Integer.MAX_VALUE : remainingCuts + 1;
                RecordBoundaryProbe.ProvenWalk walk = provenMacroSplitStarts(deferred, isCancelled, cap);
                remainingCuts -= Math.max(0, walk.boundaries().size() - 1);
                planResults.set(i, new PlanResult.Walked(deferred, walk.boundaries(), walk.stoppedBeforeEndOfFile()));
            }
        }
        return remainingCuts;
    }

    /**
     * Resolves a non-strided candidate's macro-split starts with the sequential proven walk.
     * <p>
     * A splitter that can be probed neither at a fixed offset nor by proving a record start must have been routed
     * to a whole-file split upstream; if one arrives here that gate failed, so fail loud rather than emit
     * mis-aligned macro-splits that silently mis-count rows.
     */
    private RecordBoundaryProbe.ProvenWalk provenMacroSplitStarts(DeferredNewlineSplits deferred, BooleanSupplier isCancelled, int cap)
        throws IOException {
        RecordSplitter splitter = deferred.splitter();
        if (splitter.supportsProvenProbing() == false) {
            throw new IllegalStateException(
                "record splitter ["
                    + splitter.getClass().getName()
                    + "] supports neither strided nor proven probing and cannot be macro-split"
            );
        }
        RecordBoundaryProbe.ProvenWalk walk = RecordBoundaryProbe.provenBoundaries(
            splitter,
            deferred.storageObject(),
            deferred.task().fileLength(),
            deferred.strideBytes(),
            deferred.minSegment(),
            isCancelled,
            cap
        );
        splitDiscoveryProbes.addAndGet(walk.getsIssued());
        return walk;
    }

    /**
     * Resolves the config-aware {@link FormatReader} for a file, or {@code null} when it cannot be resolved
     * (no {@code formatRegistry}, no object name, or an unknown extension). Config-aware so a config
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
     * Full-file {@link StorageObject} for {@code fileSplit}, seeded with the file length (and mtime
     * when known) so {@code length()} / {@code lastModified()} do not probe the object store. Size
     * {@code 0} is a real empty object; missing length falls back to the path-only constructor.
     * <p>
     * Used by {@link #storageObjectForSplit} (which then range-wraps the view span), COUNT(*) schema
     * bind (file-leading bytes, not the split window), and range-leaf / batch reads.
     */
    static StorageObject newObjectForFile(StorageProvider storageProvider, FileSplit fileSplit) {
        return newObject(storageProvider, fileSplit.path(), fileLengthHint(fileSplit), fileMtimeHint(fileSplit));
    }

    /**
     * Picks the {@link StorageProvider#newObject} overload. Missing {@code length} is path-only;
     * size {@code 0} is a known empty object; mtime {@code 0} is unknown.
     */
    static StorageObject newObject(StorageProvider storageProvider, StoragePath path, @Nullable Long length, long mtimeMillis) {
        if (length == null) {
            return storageProvider.newObject(path);
        }
        return mtimeMillis > 0
            ? storageProvider.newObject(path, length, Instant.ofEpochMilli(mtimeMillis))
            : storageProvider.newObject(path, length);
    }

    /**
     * Builds a {@link StorageObject} that exposes only the bytes for the given {@link FileSplit}.
     * Always wraps the provider's base object in {@link RangeStorageObject} so format readers and
     * splittable decompressors only see the split's compressed byte span (including offset {@code 0}).
     * The inner object is the full file. A span split carries that length in {@code _file_length};
     * {@link FileSplit#length()} is the view span there. A whole-file split (first and last) is not
     * stamped, and its {@link FileSplit#length()} is the file.
     */
    public static StorageObject storageObjectForSplit(StorageProvider storageProvider, FileSplit fileSplit) {
        return new RangeStorageObject(newObjectForFile(storageProvider, fileSplit), fileSplit.offset(), fileSplit.length());
    }

    /**
     * Full-file length. Span splits stamp {@code _file_length} because {@link FileSplit#length()} is
     * only the view. A split that is both first and last is the whole file, so its length is the file
     * even when {@code _file.size} was not retained. Any other split falls back to a retained
     * {@code _file.size}, then {@code null}.
     */
    @Nullable
    private static Long fileLengthHint(FileSplit fileSplit) {
        Object configured = fileSplit.config().get(FILE_LENGTH_KEY);
        if (configured instanceof String s) {
            return Long.parseLong(s);
        }
        if (isFirstInFile(fileSplit) && isLastInFile(fileSplit)) {
            return fileSplit.length();
        }
        Object listed = fileSplit.partitionValues().get(FileMetadataColumns.SIZE);
        return listed instanceof Number n ? n.longValue() : null;
    }

    private static long fileMtimeHint(FileSplit fileSplit) {
        Object modified = fileSplit.partitionValues().get(FileMetadataColumns.MODIFIED);
        return modified instanceof Number n ? n.longValue() : 0L;
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
            long[] boundaries = splittableCodec.findBlockBoundaries(object, 0, fileLength, splitDiscoveryCpuNanos::addAndGet);

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
                splitConfig.put(FILE_LENGTH_KEY, Long.toString(fileLength));
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
        @Nullable SourceStatistics fileStatistics,
        @Nullable Map<String, Object> foldedSourceMetadata,
        List<ExternalSplit> splits,
        @Nullable StorageProvider hoistedProvider,
        boolean unknownNativeTypes
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

        // One independently readable unit (one Parquet row group / ORC stripe): discovery would
        // reopen the same footer only to emit a single range. The file-level harvest is that
        // unit's extrema only when it carries column statistics. A slim record still reports
        // readableUnitCount == 1 but has no per-column map; skipping would stamp an empty harvest
        // and filtered MIN/MAX would scan. Those files fall through to discoverSplitRanges.
        if (singleUnitHarvest(fileStatistics)) {
            Map<String, Object> stats = normalizeSplitStats(
                SourceStatisticsSerializer.embedStatistics(Map.of(), fileStatistics),
                readSchema,
                reconciledTypes,
                declaredReadSpec,
                inferredFileTypes,
                foldedSourceMetadata,
                implicitNullsFor(reader),
                unknownNativeTypes
            );
            splits.add(
                FileSplit.withStatisticsAndReadSchema(
                    "file",
                    filePath,
                    0,
                    fileLength,
                    format,
                    wholeFileSplitConfig(config),
                    partitionValues,
                    columnMapping,
                    stats,
                    readSchema
                )
            );
            return true;
        }

        try {
            StorageProvider provider = resolveProvider(filePath, config, hoistedProvider);
            StorageObject object = provider.newObject(filePath, fileLength);

            List<SplitRange> ranges = rangeReader.discoverSplitRanges(object);
            if (ranges.isEmpty()) {
                return false;
            }
            addRangeAwareSplits(
                filePath,
                fileLength,
                format,
                config,
                partitionValues,
                columnMapping,
                readSchema,
                reconciledTypes,
                declaredReadSpec,
                inferredFileTypes,
                ranges,
                splits,
                foldedSourceMetadata,
                implicitNullsFor(reader),
                unknownNativeTypes
            );
            return true;
        } catch (IOException e) {
            LOGGER.warn("Failed to discover split ranges for [{}], falling back to single split", filePath, e);
            return false;
        }
    }

    /** A one-unit harvest can skip a second footer open only when it still carries column stats. */
    private static boolean singleUnitHarvest(@Nullable SourceStatistics fileStatistics) {
        if (fileStatistics == null || fileStatistics.readableUnitCount().orElse(-1) != 1) {
            return false;
        }
        Optional<Map<String, SourceStatistics.ColumnStatistics>> columns = fileStatistics.columnStatistics();
        return columns.isPresent() && columns.get().isEmpty() == false;
    }

    private void tryRangeAwareSplitsAsync(
        FileTask task,
        @Nullable StorageProvider hoistedProvider,
        Executor fanOut,
        ActionListener<List<ExternalSplit>> listener
    ) {
        if (formatRegistry == null || storageRegistry == null || task.format() == null) {
            listener.onResponse(null);
            return;
        }
        FormatReader reader;
        try {
            reader = FormatNameResolver.resolveReader(task.config(), task.filePath().objectName(), formatRegistry)
                .withConfig(task.config());
        } catch (Exception e) {
            listener.onResponse(null);
            return;
        }
        if (reader instanceof RangeAwareFormatReader == false) {
            listener.onResponse(null);
            return;
        }
        RangeAwareFormatReader rangeReader = (RangeAwareFormatReader) reader;
        try {
            StorageProvider provider = resolveProvider(task.filePath(), task.config(), hoistedProvider);
            StorageObject object = provider.newObject(task.filePath(), task.fileLength());
            rangeReader.discoverSplitRangesAsync(object, fanOut, ActionListener.wrap(ranges -> {
                runRecordingDiscoveryCpu(() -> {
                    if (ranges == null || ranges.isEmpty()) {
                        listener.onResponse(null);
                        return;
                    }
                    try {
                        List<ExternalSplit> splits = new ArrayList<>(ranges.size());
                        addRangeAwareSplits(
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
                            ranges,
                            splits,
                            task.foldedSourceMetadata(),
                            implicitNullsFor(reader),
                            task.unknownNativeTypes()
                        );
                        listener.onResponse(splits);
                    } catch (Exception e) {
                        listener.onFailure(e);
                    }
                });
            }, e -> {
                if (ExceptionsHelper.unwrap(e, IllegalArgumentException.class) == null
                    && ExceptionsHelper.unwrap(e, IOException.class) != null) {
                    LOGGER.warn("Failed to discover split ranges for [{}], falling back to single split", task.filePath(), e);
                    listener.onResponse(null);
                } else {
                    listener.onFailure(e);
                }
            }));
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    private static void addRangeAwareSplits(
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
        List<SplitRange> ranges,
        List<ExternalSplit> splits,
        @Nullable Map<String, Object> foldedSourceMetadata,
        boolean implicitNulls,
        boolean unknownNativeTypes
    ) {
        Map<String, Object> splitConfig = new HashMap<>(config);
        splitConfig.put(RANGE_SPLIT_KEY, "true");
        splitConfig.put(FILE_LENGTH_KEY, Long.toString(fileLength));

        for (SplitRange range : ranges) {
            Map<String, Object> rangeStats = range.statistics().isEmpty() ? null : range.statistics();
            rangeStats = normalizeSplitStats(
                rangeStats,
                readSchema,
                reconciledTypes,
                declaredReadSpec,
                inferredFileTypes,
                foldedSourceMetadata,
                implicitNulls,
                unknownNativeTypes
            );
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
    }

    /**
     * Normalizes raw footer statistics (the {@code _stats.*} map) for stamping onto a split: applies the
     * declared-overlay rekey/poison when a declaration ran, unit-normalizes values to the reconciled query
     * types, reapplies the FIRST_FILE_WINS rewrite or unsigned encode against this file's read schema,
     * then copies fold-level unservability from {@code foldedSourceMetadata}. Returns {@code null} for
     * absent/empty stats. Shared by the per-range path and the single-unit discovery skip in
     * {@link #tryRangeAwareSplits} so both stamp identical stats for one unit.
     */
    @Nullable
    private static Map<String, Object> normalizeSplitStats(
        @Nullable Map<String, Object> rawStats,
        @Nullable List<Attribute> readSchema,
        @Nullable Map<String, DataType> reconciledTypes,
        DeclaredReadSpec declaredReadSpec,
        @Nullable Map<String, DataType> inferredFileTypes,
        @Nullable Map<String, Object> foldedSourceMetadata,
        boolean implicitNulls,
        boolean unknownNativeTypes
    ) {
        Map<String, Object> stats = rawStats == null || rawStats.isEmpty() ? null : rawStats;
        if (stats == null) {
            return stats;
        }
        if (unknownNativeTypes) {
            stats = SourceStatisticsSerializer.overlayPinnedColumnsOnStats(
                stats,
                ExternalSourceResolver.fileBackedPhysicalColumns(readSchema, declaredReadSpec),
                false
            );
        }
        if (readSchema == null || reconciledTypes == null) {
            // Align against the read schema the reader is pinned to, not the unified type:
            // UNION_BY_NAME widens DATETIME+DATE_NANOS in the output and converts after the read.
            stats = ExternalSourceResolver.alignHarvestWithAnchorTypes(
                stats,
                inferredFileTypes,
                readSchema != null ? attributesToTypeMap(readSchema) : null,
                implicitNulls,
                declaredReadSpec.declaredTypeColumns()
            );
            return SourceStatisticsSerializer.alignHarvestWithFold(stats, foldedSourceMetadata);
        }
        // Type authority for the raw footer values. Prefer inferredFileTypes when set (pre-pin / pre-overlay):
        // a text UNION_BY_NAME pin stores the reconciled type on readSchema, which would make
        // file == reconciled and skip the LONG->DOUBLE convert. Fall back to readSchema when nothing
        // retyped this file. A declaration overlays readSchema, so the branch below rekeys and poisons
        // before normalizing with the inferred types.
        Map<String, DataType> statsFileTypes;
        if (declaredReadSpec.isEmpty()) {
            statsFileTypes = undeclaredStatsFileTypes(readSchema, inferredFileTypes);
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
                stats = SourceStatisticsSerializer.overlayDeclaredSchemaOnStats(stats, physicalToLogical, poison);
                // Inferred file types, rekeyed to logical so they align with the rekeyed stats + reconciledTypes.
                statsFileTypes = new HashMap<>(inferredFileTypes.size());
                for (Map.Entry<String, DataType> e : inferredFileTypes.entrySet()) {
                    statsFileTypes.put(physicalToLogical.getOrDefault(e.getKey(), e.getKey()), e.getValue());
                }
            } else {
                // Declared read but no captured inference (strict paths skip inference): the declared-vs-inferred
                // comparison is impossible, so conservatively poison EVERY declared column. row_count survives.
                poison.addAll(declaredReadSpec.declaredTypeColumns());
                stats = SourceStatisticsSerializer.overlayDeclaredSchemaOnStats(stats, physicalToLogical, poison);
                statsFileTypes = attributesToTypeMap(readSchema);
            }
        }
        // Footer stats are in each file's LOCAL unit/representation (footer or inferred types, not a
        // pinned or unified type); normalize to the reconciled query type so the split-filter classifier
        // and the filtered merge compare/serve in ONE unit across mixed DATETIME(millis)/DATE_NANOS(nanos)
        // files and LONG/INTEGER files reconciled to DOUBLE, not unit-blind. A non-normalizable
        // representation safe-misses via the marker.
        stats = SourceStatisticsSerializer.normalizeStatsToReconciled(stats, statsFileTypes, reconciledTypes);
        // FIRST_FILE_WINS pins every file to the anchor, so readSchema is the planner type the
        // footer reader null-fills against. UNION_BY_NAME keeps the per-file footer type on
        // readSchema and converts afterwards; comparing against reconciledTypes would treat a
        // representable DATETIME→DATE_NANOS widen as unrepresentable and rewrite the harvest
        // to value_count=0.
        stats = ExternalSourceResolver.alignHarvestWithAnchorTypes(
            stats,
            statsFileTypes,
            attributesToTypeMap(readSchema),
            implicitNulls,
            declaredReadSpec.declaredTypeColumns()
        );
        return SourceStatisticsSerializer.alignHarvestWithFold(stats, foldedSourceMetadata);
    }

    /**
     * Footer implicit-nulls for this file's configured reader, including an extensionless object
     * with an explicit {@code format}. An unresolvable reader answers {@code false}: this flag
     * licenses rewriting a present harvest to {@code value_count = 0}, so guessing footer
     * behavior would manufacture an undercount.
     */
    private boolean implicitNullsFor(FileTask task) {
        FormatReader reader = resolveConfiguredReader(task.filePath(), task.config());
        return reader != null && implicitNullsFor(reader);
    }

    private static boolean implicitNullsFor(FormatReader reader) {
        return reader.aggregatePushdownSupport().appliesImplicitNullsForAbsentColumn();
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
     * rejected because the scan as a whole is large. Logging that the size asked for is not the size in use is
     * the caller's to do, so that this stays arithmetic the caller can evaluate without emitting anything.
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
            splitConfig.put(FILE_LENGTH_KEY, Long.toString(fileLength));
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
                    splitConfig.put(FILE_LENGTH_KEY, Long.toString(fileLength));
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
     * Returns the hoisted inline-config lease when present; empty-config reads use the
     * registry default. A missing hoist with non-empty config is a programming error
     * (creating a provider here would leak a pool lease). Unreachable when the hoist
     * in {@code discoverSplits} ran; fails as {@link AssertionError}, not a user ISE.
     */
    private StorageProvider resolveProvider(StoragePath filePath, Map<String, Object> config, @Nullable StorageProvider hoistedProvider) {
        if (hoistedProvider != null) {
            return hoistedProvider;
        }
        if (config != null && config.isEmpty() == false) {
            throw new AssertionError("inline-config split discovery requires a hoisted storage provider");
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
            "[{}] of [{}] times [{}] of [{}] exceeds [{}]; lower either (at [{}] probes the window can be at most [{}])",
            CONFIG_SPLIT_PROBE_WINDOW,
            ByteSizeValue.ofBytes(splitProbeWindowBytes),
            CONFIG_MAX_SPLIT_PROBES,
            maxSplitProbes,
            ByteSizeValue.ofBytes(MAX_PROBE_BUDGET_BYTES),
            maxSplitProbes,
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
     * Discovery-only value map for filter evaluation: a fresh copy of the survivor's frozen
     * partition map (hive partitions and {@code _file.*} listing values). Unbound {@code _file.*}
     * keys are dropped, because those names are ordinary data columns and must not prune the
     * listing by storage stat or block a missing-column skip. Bound per-file constants (the
     * standard names, all but {@code _score} null) are overlaid only when a hint names one of
     * them, and only for names bound as metadata in the relation's output, matching the reader.
     * Data columns retain their physical values or missing-column null-fill. The frozen map
     * itself carries hive and {@code _file.*} only.
     */
    private static Map<String, Object> discoveryFilterValues(
        Map<String, Object> partitionValues,
        Set<String> metadataColumnNames,
        boolean overlayPerFileConstants,
        Set<String> unboundFileMetadataNames
    ) {
        Map<String, Object> filterValues = new HashMap<>(partitionValues.size() + ExternalMetadataColumns.PER_FILE_CONSTANT_NAMES.size());
        filterValues.putAll(partitionValues);
        for (String name : unboundFileMetadataNames) {
            filterValues.remove(name);
        }
        if (overlayPerFileConstants) {
            for (Map.Entry<String, Object> constant : ExternalMetadataColumns.extractPerFileConstants().entrySet()) {
                if (metadataColumnNames.contains(constant.getKey())) {
                    filterValues.put(constant.getKey(), constant.getValue());
                }
            }
        }
        return filterValues;
    }

    /**
     * Names from {@code candidates} that a filter references. Empty when nothing matches. Callers pass a
     * set that is already limited to the names this scan should treat as bound.
     */
    private static Set<String> referencedNames(List<Expression> filterHints, Set<String> candidates) {
        if (filterHints.isEmpty() || candidates.isEmpty()) {
            return Set.of();
        }
        Set<String> matched = null;
        for (Expression hint : filterHints) {
            for (Attribute attribute : hint.references()) {
                String name = attribute.name();
                if (candidates.contains(name)) {
                    if (matched == null) {
                        matched = new LinkedHashSet<>();
                    }
                    matched.add(name);
                }
            }
        }
        return matched == null ? Set.of() : matched;
    }

    /** Members of {@code candidates} that are also in {@code bound}. */
    private static Set<String> namesInBoth(Set<String> bound, Set<String> candidates) {
        if (bound.isEmpty() || candidates.isEmpty()) {
            return Set.of();
        }
        Set<String> both = null;
        for (String name : candidates) {
            if (bound.contains(name)) {
                if (both == null) {
                    both = new LinkedHashSet<>();
                }
                both.add(name);
            }
        }
        return both == null ? Set.of() : both;
    }

    /**
     * Returns {@code true} when the file can be skipped because a filter conjunct references a
     * column absent from the file and evaluates to UNKNOWN (which becomes FALSE in WHERE context).
     * <p>
     * Only simple leaf predicates are checked: comparisons ({@code =, !=, <, >, <=, >=}),
     * {@link In}, {@link IsNotNull}, {@link StartsWith}, {@link WildcardLike}, and {@link RLike}.
     * These all evaluate to UNKNOWN/FALSE for a missing column.
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

    /** Whether the operand is a literal that is not null, which is what makes a missing column answer false. */
    private static boolean isNonNullLiteral(Expression e) {
        return e instanceof Literal literal && literal.value() != null;
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
        // The multivalue comparison functions name their column the same way, when their other operands are literals. A
        // missing column is the empty set, so each of these is then false for every row of a file that lacks it — the
        // same answer Equals gives, and the reason such a file can be skipped unread. The literal requirement is
        // load-bearing for mv_contains: the empty set contains the empty set, so mv_contains(missing, b) is true on
        // every row where b is null, and a column b can be.
        if (expr instanceof MvContains mvContains) {
            return isNonNullLiteral(mvContains.right()) ? extractColumnName(mvContains.left()) : null;
        }
        if (expr instanceof MvIntersects mvIntersects) {
            return isNonNullLiteral(mvIntersects.right()) ? extractColumnName(mvIntersects.left()) : null;
        }
        if (expr instanceof MvInRange mvInRange) {
            return isNonNullLiteral(mvInRange.lower()) && isNonNullLiteral(mvInRange.upper()) ? extractColumnName(mvInRange.field()) : null;
        }
        if (expr instanceof MvCompare mvCompare) {
            return isNonNullLiteral(mvCompare.bound()) ? extractColumnName(mvCompare.field()) : null;
        }
        if (expr instanceof StartsWith startsWith) {
            return extractColumnName(startsWith.str());
        }
        if (expr instanceof WildcardLike like) {
            return extractColumnName(like.field());
        }
        if (expr instanceof RLike rlike) {
            return extractColumnName(rlike.field());
        }
        return null;
    }

    static boolean matchesPartitionFilters(Map<String, Object> partitionValues, List<Expression> filters) {
        return matchesPartitionFilters(partitionValues, filters, new IdentityHashMap<>());
    }

    static boolean matchesPartitionFilters(
        Map<String, Object> partitionValues,
        List<Expression> filters,
        IdentityHashMap<Expression, ByteRunAutomaton> regexAutomata
    ) {
        for (Expression filter : filters) {
            Boolean result = evaluateFilter(filter, partitionValues, regexAutomata);
            if (result != null && result == false) {
                return false;
            }
        }
        return true;
    }

    static Boolean evaluateFilter(Expression filter, Map<String, Object> partitionValues) {
        return evaluateFilter(filter, partitionValues, new IdentityHashMap<>());
    }

    private static Boolean evaluateFilter(
        Expression filter,
        Map<String, Object> partitionValues,
        IdentityHashMap<Expression, ByteRunAutomaton> regexAutomata
    ) {
        return switch (filter) {
            case Equals eq -> evaluateComparison(eq.left(), eq.right(), partitionValues, PartitionValueMatcher::compareEquals);
            case NotEquals neq -> {
                Boolean result = evaluateComparison(neq.left(), neq.right(), partitionValues, PartitionValueMatcher::compareEquals);
                yield result != null ? result == false : null;
            }
            case GreaterThanOrEqual gte -> evaluateComparison(
                gte.left(),
                gte.right(),
                partitionValues,
                (a, b) -> PartitionValueMatcher.compareValues(a, b) >= 0
            );
            case GreaterThan gt -> evaluateComparison(
                gt.left(),
                gt.right(),
                partitionValues,
                (a, b) -> PartitionValueMatcher.compareValues(a, b) > 0
            );
            case LessThanOrEqual lte -> evaluateComparison(
                lte.left(),
                lte.right(),
                partitionValues,
                (a, b) -> PartitionValueMatcher.compareValues(a, b) <= 0
            );
            case LessThan lt -> evaluateComparison(
                lt.left(),
                lt.right(),
                partitionValues,
                (a, b) -> PartitionValueMatcher.compareValues(a, b) < 0
            );
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
                        if (zerosOfOppositeSign(partitionValue, lit.value())) {
                            found = null;
                        } else if (PartitionValueMatcher.compareEquals(partitionValue, lit.value())) {
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
            // A partition value is single, so each of these reads as its scalar sibling does. Two differences live in
            // the helpers below: they read `field OP literal` only, and the ordered forms take a value exactly on the
            // bound from their default inclusivity.
            case MvContains mvContains -> evaluateMvLeaf(
                mvContains.left(),
                mvContains.right(),
                partitionValues,
                PartitionValueMatcher::compareEquals
            );
            case MvIntersects mvIntersects -> evaluateMvIntersects(mvIntersects, partitionValues);
            case MvInRange mvInRange -> {
                Boolean onBound = onTheBound(mvInRange.options(), true);
                yield nullableAnd(
                    evaluateMvLeaf(mvInRange.field(), mvInRange.lower(), partitionValues, (v, b) -> above(v, b, onBound)),
                    evaluateMvLeaf(mvInRange.field(), mvInRange.upper(), partitionValues, (v, b) -> below(v, b, onBound))
                );
            }
            case MvGreater mvGreater -> evaluateMvLeaf(
                mvGreater.field(),
                mvGreater.bound(),
                partitionValues,
                (v, b) -> above(v, b, onTheBound(mvGreater.options(), false))
            );
            case MvLess mvLess -> evaluateMvLeaf(
                mvLess.field(),
                mvLess.bound(),
                partitionValues,
                (v, b) -> below(v, b, onTheBound(mvLess.options(), false))
            );
            case And and -> nullableAnd(
                evaluateFilter(and.left(), partitionValues, regexAutomata),
                evaluateFilter(and.right(), partitionValues, regexAutomata)
            );
            case Or or -> nullableOr(
                evaluateFilter(or.left(), partitionValues, regexAutomata),
                evaluateFilter(or.right(), partitionValues, regexAutomata)
            );
            case Not not -> nullableNot(evaluateFilter(not.field(), partitionValues, regexAutomata));
            case StartsWith startsWith -> evaluateStartsWith(startsWith, partitionValues);
            case WildcardLike like -> evaluateRegexMatch(
                like,
                like.field(),
                like.pattern(),
                like.caseInsensitive(),
                partitionValues,
                regexAutomata
            );
            case RLike rlike -> evaluateRegexMatch(
                rlike,
                rlike.field(),
                rlike.pattern(),
                rlike.caseInsensitive(),
                partitionValues,
                regexAutomata
            );
            default -> null;
        };
    }

    /**
     * The engine's {@code IN} orders doubles with {@code Double.compare}, so unlike {@code ==} it tells {@code -0.0}
     * from {@code 0.0}, and the matcher does not. For such a pair {@code IN} is left unknown. The matcher's "equal"
     * prunes matching files under {@code NOT IN}; copying the engine's current "not equal" is right only while
     * {@code IN} disagrees with {@code ==}, and would prune matching files under plain {@code IN} once the two agree.
     */
    private static boolean zerosOfOppositeSign(Object a, Object b) {
        return a instanceof Number na
            && b instanceof Number nb
            && na.doubleValue() == 0.0
            && nb.doubleValue() == 0.0
            && Double.compare(na.doubleValue(), nb.doubleValue()) != 0;
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

    /**
     * {@code field OP literal} for a multivalue comparison function, and only that way round. A binary comparison is
     * symmetric under operand swap, which is why {@link #evaluateComparison} also tries {@code literal OP column}; these
     * are not — {@code mv_contains(literal, column)} asks whether the column's values are a subset of the literal's,
     * a different predicate — so a literal on the left is unknown rather than evaluated. So is a field that is not a
     * plain column: a case-insensitive DSL term arrives as {@code mv_contains(TO_LOWER(p), lowered)}, and partition
     * values hold the original case, so {@link #extractColumnName} returning null for it is what keeps that file.
     * <p>
     * A null partition value is unknown here, where the function itself would answer false (it reads a null as the
     * empty set). Unknown is strictly less informative than the true answer, and the connectives below are monotone in
     * that ordering, so the difference can only keep a file the exact answer would prune — never prune one it would
     * keep, under {@code Not} included.
     */
    private static Boolean evaluateMvLeaf(
        Expression field,
        Expression literal,
        Map<String, Object> partitionValues,
        BiFunction<Object, Object, Boolean> comparator
    ) {
        String columnName = extractColumnName(field);
        Object literalValue = extractLiteralValue(literal);
        // A list-valued literal is "contains all of these", not the scalar bound.
        if (columnName == null
            || literalValue == null
            || literalValue instanceof List
            || partitionValues.containsKey(columnName) == false) {
            return null;
        }
        Object partitionValue = partitionValues.get(columnName);
        return partitionValue != null ? comparator.apply(partitionValue, literalValue) : null;
    }

    /**
     * {@code mv_intersects(p, [v...])}: the partition value is in the set. The set arrives as a single list-valued
     * literal, not the list of literals {@code In} carries. A set with no non-null member is unknown rather than false.
     */
    private static Boolean evaluateMvIntersects(MvIntersects mvIntersects, Map<String, Object> partitionValues) {
        String columnName = extractColumnName(mvIntersects.left());
        Object literalValue = extractLiteralValue(mvIntersects.right());
        if (columnName == null || literalValue == null || partitionValues.containsKey(columnName) == false) {
            return null;
        }
        Object partitionValue = partitionValues.get(columnName);
        if (partitionValue == null) {
            return null;
        }
        List<?> values = literalValue instanceof List<?> list ? list : List.of(literalValue);
        boolean sawValue = false;
        for (Object value : values) {
            if (value != null) {
                sawValue = true;
                if (PartitionValueMatcher.compareEquals(partitionValue, value)) {
                    return true;
                }
            }
        }
        return sawValue ? false : null;
    }

    /**
     * What an ordered multivalue function answers for a value lying exactly on its bound. The inclusivity is an option
     * — {@code mv_in_range} defaults to inclusive, {@code mv_greater} / {@code mv_less} to strict — and when no options
     * were given, the default is the answer. That is not an edge case: a DSL {@code range} on an integer column always
     * arrives as an optionless {@code mv_in_range} with its bounds already made inclusive, and integer range bounds land
     * on partition values constantly ({@code year >= 2025} over {@code year=2025}). With options present the answer is
     * left unknown rather than parse them here.
     * <p>
     * Guessing instead would be wrong rather than loose: {@code NOT mv_greater(year, 2022)} is true for every row of a
     * {@code year=2022} file because the bound is strict, and reading it as inclusive negates {@code 2022 >= 2022} to
     * false and prunes that file.
     */
    private static Boolean onTheBound(Expression options, boolean defaultInclusive) {
        return options == null ? defaultInclusive : null;
    }

    /** TRUE strictly above {@code bound}, FALSE strictly below, {@code onBound} exactly on it. */
    private static Boolean above(Object value, Object bound, Boolean onBound) {
        int cmp = PartitionValueMatcher.compareValues(value, bound);
        return cmp > 0 ? Boolean.TRUE : cmp < 0 ? Boolean.FALSE : onBound;
    }

    /** TRUE strictly below {@code bound}, FALSE strictly above, {@code onBound} exactly on it. */
    private static Boolean below(Object value, Object bound, Boolean onBound) {
        int cmp = PartitionValueMatcher.compareValues(value, bound);
        return cmp < 0 ? Boolean.TRUE : cmp > 0 ? Boolean.FALSE : onBound;
    }

    private static String extractColumnName(Expression expr) {
        return switch (expr) {
            case FieldAttribute fa -> fa.name();
            // Metadata _score is per-row, not per-file; type-checked so a physical column named _score still prunes.
            case NamedExpression ne when MetadataAttribute.isScoreAttribute(ne) -> null;
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
     * Exact prefix match on a listing value. Missing key or a non-string value is unknown — the file
     * is kept. Never rewritten to a GTE/LT range here; that conversion is a listing-hint superset only.
     */
    private static Boolean evaluateStartsWith(StartsWith startsWith, Map<String, Object> partitionValues) {
        String columnName = extractColumnName(startsWith.str());
        Object literalValue = extractLiteralValue(startsWith.prefix());
        if (columnName == null || literalValue == null || partitionValues.containsKey(columnName) == false) {
            return null;
        }
        BytesRef value = bytesOf(partitionValues.get(columnName));
        BytesRef prefix = bytesOf(literalValue);
        if (value == null || prefix == null) {
            return null;
        }
        return ByteMatchers.startsWith(value, prefix);
    }

    /**
     * Exact LIKE / RLIKE match on a listing value via {@link AutomataMatch}. Compiles the automaton
     * once per expression identity in {@code regexAutomata} so a 10k-file listing does not
     * determinize the same pattern 10k times. A missing key, a non-string value, or an automaton
     * too complex to determinize is unknown.
     */
    private static Boolean evaluateRegexMatch(
        Expression regexExpr,
        Expression field,
        AbstractStringPattern pattern,
        boolean caseInsensitive,
        Map<String, Object> partitionValues,
        IdentityHashMap<Expression, ByteRunAutomaton> regexAutomata
    ) {
        String columnName = extractColumnName(field);
        if (columnName == null || partitionValues.containsKey(columnName) == false) {
            return null;
        }
        BytesRef value = bytesOf(partitionValues.get(columnName));
        if (value == null) {
            return null;
        }
        ByteRunAutomaton run;
        if (regexAutomata.containsKey(regexExpr)) {
            run = regexAutomata.get(regexExpr);
        } else {
            run = AutomataMatch.compile(pattern.createAutomaton(caseInsensitive));
            regexAutomata.put(regexExpr, run);
        }
        if (run == null) {
            return null;
        }
        return AutomataMatch.matches(value, run);
    }

    /**
     * Listing values for {@code _file.name}/{@code path}/{@code directory} are {@link BytesRef};
     * Hive string partitions are {@link String}. Anything else cannot be a string match.
     */
    @Nullable
    private static BytesRef bytesOf(Object value) {
        if (value instanceof BytesRef bytesRef) {
            return bytesRef;
        }
        if (value instanceof String string) {
            return new BytesRef(string);
        }
        return null;
    }

}
