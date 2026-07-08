/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.CloseableIterator;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.compute.operator.topn.SharedMinCompetitive;
import org.elasticsearch.compute.operator.topn.SharedNumericThreshold;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.ExternalMetadataAttribute;
import org.elasticsearch.xpack.esql.core.expression.VirtualAttribute;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.datasources.cache.StatsCapturingIterator;
import org.elasticsearch.xpack.esql.datasources.spi.ColumnExtractor;
import org.elasticsearch.xpack.esql.datasources.spi.ColumnExtractorAware;
import org.elasticsearch.xpack.esql.datasources.spi.ColumnExtractorProducer;
import org.elasticsearch.xpack.esql.datasources.spi.DecompressionCodec;
import org.elasticsearch.xpack.esql.datasources.spi.DynamicThreshold;
import org.elasticsearch.xpack.esql.datasources.spi.DynamicThresholdAware;
import org.elasticsearch.xpack.esql.datasources.spi.ErrorPolicy;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalSourceMetrics;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalSplit;
import org.elasticsearch.xpack.esql.datasources.spi.FileList;
import org.elasticsearch.xpack.esql.datasources.spi.FilterPushdownSupport;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReadContext;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReader;
import org.elasticsearch.xpack.esql.datasources.spi.IndexedDecompressionCodec;
import org.elasticsearch.xpack.esql.datasources.spi.NullSpliceRowPositionStrategy;
import org.elasticsearch.xpack.esql.datasources.spi.RangeAwareFormatReader;
import org.elasticsearch.xpack.esql.datasources.spi.RangeReadContext;
import org.elasticsearch.xpack.esql.datasources.spi.RowPositionStrategy;
import org.elasticsearch.xpack.esql.datasources.spi.SegmentableFormatReader;
import org.elasticsearch.xpack.esql.datasources.spi.SourceMetadata;
import org.elasticsearch.xpack.esql.datasources.spi.SourceOperatorContext;
import org.elasticsearch.xpack.esql.datasources.spi.SplittableDecompressionCodec;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObjectMetrics;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.datasources.spi.StorageProvider;
import org.elasticsearch.xpack.esql.datasources.spi.StripeColumnScope;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static org.elasticsearch.xpack.esql.datasources.ExternalSourceDrainUtils.drainPagesAsync;

/**
 * Dual-mode async factory for creating source operators that read from external storage.
 * <p>
 * This factory automatically selects the optimal execution mode based on the FormatReader's
 * capabilities:
 * <ul>
 *   <li><b>Sync Wrapper Mode</b>: For simple formats (CSV, JSON) that don't have native async
 *       support. The sync {@link FormatReader#read} method is wrapped in a background thread
 *       from the ES ThreadPool.</li>
 *   <li><b>Native Async Mode</b>: For async-capable formats (Parquet with parallel row groups)
 *       that implement {@link FormatReader#readAsync}. This avoids wrapper thread overhead
 *       by letting the reader control its own threading.</li>
 * </ul>
 * <p>
 * Key design principles:
 * <ul>
 *   <li>Simple things stay simple - CSV/JSON readers just implement sync read()</li>
 *   <li>Async when beneficial - Parquet can override readAsync() for parallel I/O</li>
 *   <li>ES ThreadPool integration - All executors come from ES, not standalone threads</li>
 *   <li>Backpressure via buffer - Uses {@link AsyncExternalSourceBuffer} with waitForSpace()</li>
 * </ul>
 * <p>
 * Two executors, deliberately on different pools so the parser workers and the consumer that drains their pages
 * never contend for the same threads:
 * <ul>
 *   <li>{@code executor} — the read/parse pool. Runs the blocking file opens ({@code length()},
 *       {@code computeSegments}) and the segment parser workers. Sourced from
 *       {@link org.elasticsearch.xpack.esql.datasources.spi.SourceOperatorContext#fileReadExecutor} — the dedicated
 *       {@code esql_external_io} pool — falling back to {@code context.executor()} when unset.</li>
 *   <li>{@code producerExecutor} — the consumer pool. Runs the (non-blocking) producer/drain loop that consumes the
 *       parser workers' pages into the buffer. Sourced from {@code context.executor()} — {@code esql_worker}, the
 *       same compute pool whose drivers {@link AsyncExternalSourceBuffer#pollPage()} — falling back to
 *       {@code executor} when unset (single-pool test callers).</li>
 * </ul>
 * A full read/parse pool of blocked parser workers therefore can never starve the drain that must consume their
 * pages (the multi-file parallel-parse stall). The drain is non-blocking: it runs synchronously while the buffer has
 * space and yields when full, resuming via {@code producerExecutor} when space is freed.
 *
 * @see AsyncExternalSourceBuffer
 * @see AsyncExternalSourceOperator
 */
public class AsyncExternalSourceOperatorFactory implements SourceOperator.SourceOperatorFactory, DeferredExtractionCapable {

    private static final Logger logger = LogManager.getLogger(AsyncExternalSourceOperatorFactory.class);

    private final StorageProvider storageProvider;
    private final FormatReader formatReader;
    private final StoragePath path;
    private final List<Attribute> attributes;
    // Node telemetry sink, attached to each storage object as it is opened (see attachStorageMetrics).
    private final ExternalSourceMetrics externalSourceMetrics;
    // Data-attribute view of {@link #attributes} (virtual columns and Hive-style partition columns
    // stripped). Built once at construction; used to shape pages handed to SchemaAdaptingIterator
    // and to scope filter adaptation in mapFilters. Partition columns are excluded so this width
    // matches the file-backed ColumnMapping even when a partition key shadows a same-named physical
    // column (see ExternalSchema#dataAttributesOf(List, Set)).
    private final ExternalSchema queryDataSchema;
    /**
     * {@link #attributes} minus the synthetic {@link ColumnExtractor#ROW_POSITION_COLUMN}, used when
     * we hand a "file's resolved schema" to a reader (e.g. {@link RangeReadContext#resolvedAttributes()}
     * or {@link FormatReadContext#readSchema()}). The synthetic is an optimizer-injected channel,
     * not a real column the file has — passing it to readers as a resolved file attribute invites
     * collision-style guards to misfire (see the production regression on the Parquet range path).
     * Equal to {@link #attributes} when deferred extraction is off.
     */
    private final List<Attribute> readerResolvedAttributes;
    private final int batchSize;
    private final int maxBufferSize;
    private final int rowLimit;
    private final Executor executor;
    /**
     * Executor for the page <em>consumer</em> — the producer loop ({@link #runProducerLoop}) and the
     * {@link ExternalSourceDrainUtils#drainPagesAsync} drain — as opposed to {@link #executor}, which runs the
     * blocking reads and parser workers. In production this is {@code esql_worker} (the compute pool that also runs
     * the Driver polling this operator's buffer) while {@link #executor} is the dedicated {@code esql_external_io}
     * pool. Keeping the consumer on a distinct pool from the parser workers is what prevents a full I/O pool of
     * blocked parsers from starving the drain that must consume their pages (the multi-file parallel-parse stall).
     * Defaults to {@link #executor} when the builder is given only one executor (tests / single-pool callers).
     */
    private final Executor producerExecutor;
    private final FileList fileList;
    // Per-file planner-resolved schemas; always non-null (empty for unresolved paths).
    private final Map<StoragePath, SchemaReconciliation.FileSchemaInfo> schemaMap;
    private final Set<String> partitionColumnNames;
    private final Map<String, Object> partitionValues;
    /**
     * Standard ES metadata column names ({@code _index}, {@code _version}, ...) present in
     * {@link #attributes} that the producer pipeline must materialise as per-file constants.
     * Derived once from {@link #attributes} at construction. Names whose values require per-row
     * composition ({@code _id}, {@code _source}) are not included here — they are handled by
     * separate operator steps.
     */
    private final Set<String> standardMetadataPerFileNames;
    /**
     * Whether the bound attributes include an {@link ExternalMetadataAttribute} named {@code _id}.
     * When true, the producer pipeline must compose {@code _id} per row via
     * {@link ExternalRowIdentity#composePage} and the optimizer must have injected
     * {@link ColumnExtractor#ROW_POSITION_COLUMN} into the source's projection so the iterator
     * has the input it needs.
     */
    private final boolean idColumnRequested;
    /**
     * Dataset name threaded from the planner ({@code DatasetRewriter} attaches it to
     * {@code UnresolvedExternalRelation}; {@code ExternalRelation} / {@code ExternalSourceExec} round-
     * trip it on the wire under {@code ESQL_EXTERNAL_DATASET_NAME}; {@code LocalExecutionPlanner}
     * sets it on the {@link org.elasticsearch.xpack.esql.datasources.spi.SourceOperatorContext}).
     * Used for {@code _index} resolution: a bare-glob {@code FROM} query has no dataset identity, so
     * the value is {@code null} and {@link VirtualColumnIterator} renders {@code _index} as
     * SQL {@code NULL}.
     */
    @Nullable
    private final String datasetName;
    // Declared logical->physical column renames (source). Applied to reader-facing names (projection + read schema)
    // at the last mile via PhysicalNames, so readers stay rename-agnostic. Empty when the dataset declares no rename.
    private final Map<String, String> renames;
    /**
     * Declared {@code _id.path} (the logical name of the data column whose value supplies each row's {@code _id}), or
     * {@code null} when the dataset declares no {@code mappings._id.path}. When set and {@code _id} is projected,
     * {@link VirtualColumnIterator} stamps {@code _id} from that column instead of the synthetic (file+row-position)
     * identity. Threaded to the iterator alongside {@code idPrefix}.
     */
    @Nullable
    private final String idPath;
    /**
     * File last-modified epoch-millis used to materialise {@code _version} on the single-file
     * producer paths ({@link #startSyncWrapperRead} / {@link #startNativeAsyncRead} /
     * {@link #consumePagesInBackground}). {@code null} when the caller did not supply an mtime
     * (test harnesses, paths whose backing storage has no mtime concept); {@code _version} then
     * renders as SQL {@code NULL} per {@link ExternalMetadataColumns#extractPerFileConstants}.
     * Not consulted on the slice-queue / multi-file paths — those carry per-file mtime via the
     * {@code FileSplit} / {@code FileList} entries respectively.
     */
    @Nullable
    private final Long lastModifiedMillis;
    /**
     * {@link BlockFactory} used by producer-thread iterator wrappers ({@link VirtualColumnIterator}
     * for {@code _file.*} / Hive-style partition columns, {@link SchemaAdaptingIterator} for
     * null-fill and type-casting under UBN). When {@code null}, the per-driver factory from
     * {@link DriverContext#blockFactory()} is used as a fallback for tests.
     * <p>
     * Production wires the node-level (root) factory here so producer-thread allocations route
     * through the global request circuit breaker instead of the driver-local breaker, which
     * asserts single-thread access during the driver's run loop. Allocating against the local
     * breaker from a generic-pool thread races with the driver's run loop and can corrupt the
     * breaker's reserved-bytes accounting in production (assertions are stripped); see the class
     * Javadoc on {@link org.elasticsearch.compute.data.LocalCircuitBreaker} for details.
     */
    @Nullable
    private final BlockFactory producerBlockFactory;
    private final ExternalSliceQueue sliceQueue;
    private final ErrorPolicy errorPolicy;
    private final int parsingParallelism;
    private final int maxConcurrentOpenSegments;
    private final int maxRecordBytes;
    /** Canonical-stripe grid for per-stripe stats accounting; {@code <= 0} disables. Accounting overlay only. */
    private final long statsStripeSize;
    /** How much per-stripe statistics a fresh scan harvests (row count only / + projected / + all / nothing). */
    private final StripeColumnScope statsColumnScope;
    /**
     * Node-level gate bounding concurrent streaming (stream-only compressed) segmentators on the shared
     * {@code esql_external_io} pool so their per-chunk parser tasks are never starved of a thread. Shared across
     * queries/operators (see {@link StreamingSegmentatorAdmission}). Never {@code null}: production threads the
     * per-node controller from {@link FileSourceFactory}; test builders default to
     * {@link StreamingSegmentatorAdmission#unbounded()}, which dispatches immediately.
     */
    private final StreamingSegmentatorAdmission streamingSegmentatorAdmission;
    private final List<Expression> pushedExpressions;
    private final FilterPushdownSupport pushdownSupport;
    private final Closeable onClose;
    private final AtomicInteger operatorRefCount = new AtomicInteger(0);
    /**
     * Refcount controlling when {@link #onClose} runs when {@link #deferredExtraction} is on.
     * Increments: one for the factory itself (held while any source operator is in flight; released
     * when {@link #operatorRefCount} drops to zero) plus one per {@link SourceExtractors} registry
     * created (released when the registry is closed by the paired
     * {@link ExternalFieldExtractOperator}). The last release closes {@code onClose}. This keeps
     * the per-query concurrency budget alive across the seam between the source's last produced
     * page and the late materialization read that runs after TopN — see
     * {@link #attachOnCloseToRegistry}.
     */
    private final AtomicInteger deferredCloseRefCount = new AtomicInteger(0);
    /** Number of driver instances created for this factory. Used for batch-size heuristics. */
    private final int parallelism;
    /**
     * True when the reader supports multi-file batch reads and there are no partition columns
     * that require per-split injection. When set, {@link #openNextSliceQueueLeaf} claims batches
     * of splits and calls {@link RangeAwareFormatReader#readAll} instead of individual
     * {@link RangeAwareFormatReader#readRange} calls.
     */
    private final boolean batchReadCapable;
    @Nullable
    private volatile SharedNumericThreshold.Supplier thresholdSupplier;
    /**
     * BytesRef competitive bound for a single keyword/text sort key, published by the generic
     * {@code TopNOperator}. Mutually exclusive with {@link #thresholdSupplier} (a query's TopN over
     * this source has a single sort key of one type).
     */
    @Nullable
    private volatile SharedMinCompetitive.Supplier minCompetitiveSupplier;
    @Nullable
    private volatile String thresholdColumnName;
    @Nullable
    private volatile ElementType thresholdElementType;
    private volatile boolean thresholdAscending;
    private volatile boolean thresholdNullsFirst;
    @Nullable
    private volatile DynamicThreshold cachedThreshold;

    /**
     * When set, the producer paths request the synthetic {@link ColumnExtractor#ROW_POSITION_COLUMN}
     * column from the reader and re-encode it via {@link SourceExtractors#encode(int, long)} so
     * downstream {@link ExternalFieldExtractOperator}s can locate the source extractor by id. The
     * source's {@code attributes} are expected to already contain a long-typed attribute named
     * {@link ColumnExtractor#ROW_POSITION_COLUMN} at this point (added by
     * {@code InsertExternalFieldExtraction} during local physical optimization).
     * <p>
     * Requires the configured {@link FormatReader} to implement {@link ColumnExtractorAware}; the
     * {@link Builder#build} validates this invariant.
     */
    private final boolean deferredExtraction;

    /**
     * Per-driver source-extractor registries. Lazily created the first time
     * {@link #sourceExtractorsFor} is called for a given driver, so the registry is shared
     * between this factory's source operator and the {@link ExternalFieldExtractOperator}'s
     * factory created from the same driver context.
     */
    private final Map<DriverContext, SourceExtractors> sourceExtractorsPerDriver = new ConcurrentHashMap<>();

    private AsyncExternalSourceOperatorFactory(
        StorageProvider storageProvider,
        FormatReader formatReader,
        StoragePath path,
        List<Attribute> attributes,
        int batchSize,
        int maxBufferSize,
        int rowLimit,
        Executor executor,
        Executor producerExecutor,
        FileList fileList,
        Map<StoragePath, SchemaReconciliation.FileSchemaInfo> schemaMap,
        Set<String> partitionColumnNames,
        Map<String, Object> partitionValues,
        @Nullable String datasetName,
        Map<String, String> renames,
        @Nullable String idPath,
        @Nullable Long lastModifiedMillis,
        @Nullable BlockFactory producerBlockFactory,
        ExternalSliceQueue sliceQueue,
        ErrorPolicy errorPolicy,
        int parsingParallelism,
        int maxConcurrentOpenSegments,
        int maxRecordBytes,
        long statsStripeSize,
        StripeColumnScope statsColumnScope,
        StreamingSegmentatorAdmission streamingSegmentatorAdmission,
        @Nullable List<Expression> pushedExpressions,
        @Nullable FilterPushdownSupport pushdownSupport,
        @Nullable Closeable onClose,
        int parallelism,
        boolean deferredExtraction,
        ExternalSourceMetrics externalSourceMetrics
    ) {
        if (storageProvider == null) {
            throw new IllegalArgumentException("storageProvider cannot be null");
        }
        if (formatReader == null) {
            throw new IllegalArgumentException("formatReader cannot be null");
        }
        if (path == null) {
            throw new IllegalArgumentException("path cannot be null");
        }
        if (attributes == null) {
            throw new IllegalArgumentException("attributes cannot be null");
        }
        if (executor == null) {
            throw new IllegalArgumentException("executor cannot be null");
        }
        if (batchSize <= 0) {
            throw new IllegalArgumentException("batchSize must be positive, got: " + batchSize);
        }
        if (maxBufferSize <= 0) {
            throw new IllegalArgumentException("maxBufferSize must be positive, got: " + maxBufferSize);
        }

        this.storageProvider = storageProvider;
        this.formatReader = formatReader;
        this.path = path;
        this.attributes = attributes;
        this.readerResolvedAttributes = stripRowPosition(attributes);
        this.executor = executor;
        // Fall back to the read/parse executor when no distinct consumer executor is supplied (single-pool callers,
        // e.g. tests). Production wires this to esql_worker via the builder so the drain is isolated from the parsers.
        this.producerExecutor = producerExecutor != null ? producerExecutor : executor;
        this.batchSize = batchSize;
        this.maxBufferSize = maxBufferSize;
        this.rowLimit = rowLimit;
        this.fileList = fileList;
        this.schemaMap = schemaMap != null ? schemaMap : Map.of();
        // Route requested standard metadata names (and _id when requested) through
        // VirtualColumnIterator's materialization paths by unioning them into the partition-column
        // set. Per-file constants take the constant-block path; _id takes the iterator's per-row
        // composition path; _source is handled by a separate operator wrapper.
        Set<String> stdMetaNames = new LinkedHashSet<>();
        boolean idRequested = false;
        boolean sourceRequested = false;
        for (Attribute attr : attributes) {
            if (attr instanceof ExternalMetadataAttribute) {
                String n = attr.name();
                if (ExternalMetadataColumns.PER_FILE_CONSTANT_NAMES.contains(n)) {
                    stdMetaNames.add(n);
                } else if (ExternalMetadataColumns.ID.equals(n)) {
                    idRequested = true;
                } else if (ExternalMetadataColumns.SOURCE.equals(n)) {
                    sourceRequested = true;
                }
            }
        }
        this.idColumnRequested = idRequested;
        this.standardMetadataPerFileNames = stdMetaNames.isEmpty() ? Set.of() : Set.copyOf(stdMetaNames);
        if (stdMetaNames.isEmpty() && idRequested == false && sourceRequested == false) {
            this.partitionColumnNames = partitionColumnNames != null ? partitionColumnNames : Set.of();
        } else {
            // Union the standard metadata names (plus {@code _id} / {@code _source} when projected)
            // into the effective partition-column set so VirtualColumnIterator routes them through
            // its constant-block / id-composition / source-synthesis path. Hive partition columns
            // and {@code _file.*} always take precedence on key collision (they overlay last in
            // the per-file merge).
            Set<String> union = new LinkedHashSet<>(stdMetaNames);
            if (idRequested) {
                union.add(ExternalMetadataColumns.ID);
            }
            if (sourceRequested) {
                union.add(ExternalMetadataColumns.SOURCE);
            }
            if (partitionColumnNames != null) {
                union.addAll(partitionColumnNames);
            }
            this.partitionColumnNames = Collections.unmodifiableSet(union);
        }
        // Resolve queryDataSchema AFTER the effective partitionColumnNames (including any standard
        // metadata / _id / _source names unioned above) is final: the data-only schema must exclude
        // partition and virtual/metadata columns so its width matches the file-backed ColumnMapping
        // (a partition key may shadow a same-named physical column). See
        // ExternalSchema#dataAttributesOf(List, Set).
        this.queryDataSchema = ExternalSchema.dataAttributesOf(attributes, this.partitionColumnNames);
        this.partitionValues = partitionValues != null ? partitionValues : Map.of();
        this.datasetName = datasetName;
        this.renames = renames == null ? Map.of() : renames;
        this.idPath = idPath;
        this.lastModifiedMillis = lastModifiedMillis;
        this.producerBlockFactory = producerBlockFactory;
        this.sliceQueue = sliceQueue;
        this.errorPolicy = errorPolicy != null ? errorPolicy : formatReader.defaultErrorPolicy();
        this.parsingParallelism = Math.max(1, parsingParallelism);
        this.statsStripeSize = statsStripeSize;
        this.statsColumnScope = statsColumnScope != null ? statsColumnScope : StripeColumnScope.PROJECTED;
        this.maxConcurrentOpenSegments = Math.max(1, maxConcurrentOpenSegments);
        this.maxRecordBytes = maxRecordBytes;
        this.streamingSegmentatorAdmission = streamingSegmentatorAdmission;
        this.pushedExpressions = pushedExpressions != null ? pushedExpressions : List.of();
        this.pushdownSupport = pushdownSupport;
        this.onClose = onClose;
        this.parallelism = Math.max(1, parallelism);
        this.deferredExtraction = deferredExtraction;
        this.externalSourceMetrics = externalSourceMetrics == null ? ExternalSourceMetrics.NOOP : externalSourceMetrics;
        if (deferredExtraction && onClose != null) {
            // Hold one ref on behalf of the factory; released when operatorRefCount hits zero.
            // Each registry creation (one per driver) takes an additional ref. See deferredCloseRefCount.
            deferredCloseRefCount.incrementAndGet();
        }
        if (deferredExtraction && (formatReader instanceof ColumnExtractorAware) == false) {
            throw new IllegalArgumentException(
                "deferredExtraction was enabled but format reader ["
                    + formatReader.formatName()
                    + "] does not implement ColumnExtractorAware"
            );
        }
        // Batch-read reads multiple files in a single iterator and would not let us register
        // one ColumnExtractor per file. Disable when deferred extraction is enabled.
        this.batchReadCapable = deferredExtraction == false
            && formatReader instanceof RangeAwareFormatReader rr
            && rr.supportsBatchRead()
            && this.partitionColumnNames.isEmpty();
    }

    /**
     * Test-only accessor for the {@code lastModifiedMillis} value wired in by the builder. Returned
     * as-is ({@code null} when the caller did not supply an mtime) so regression tests can pin the
     * single-file {@code _version} fallback wiring at the factory boundary without driving a full
     * page-drain through the producer iterator stack.
     */
    @Nullable
    Long lastModifiedMillis() {
        return lastModifiedMillis;
    }

    public static Builder builder(
        StorageProvider storageProvider,
        FormatReader formatReader,
        StoragePath path,
        List<Attribute> attributes,
        int batchSize,
        int maxBufferSize,
        Executor executor
    ) {
        return new Builder(storageProvider, formatReader, path, attributes, batchSize, maxBufferSize, executor);
    }

    /**
     * Fluent builder for {@link AsyncExternalSourceOperatorFactory}. Required parameters are captured
     * via {@link #builder(StorageProvider, FormatReader, StoragePath, List, int, int, Executor)}.
     * Optional parameters default to: {@link FormatReader#NO_LIMIT} for rowLimit, empty collections
     * for partition/pushed-expression lists, {@code null} for opt-in hooks (sliceQueue,
     * pushdownSupport, etc.), and {@code 1} for parsingParallelism.
     */
    public static final class Builder {
        private final StorageProvider storageProvider;
        private final FormatReader formatReader;
        private final StoragePath path;
        private final List<Attribute> attributes;
        private final int batchSize;
        private final int maxBufferSize;
        private final Executor executor;

        @Nullable
        private Executor producerExecutor;
        private int rowLimit = FormatReader.NO_LIMIT;
        private FileList fileList;
        private Map<StoragePath, SchemaReconciliation.FileSchemaInfo> schemaMap;
        private Set<String> partitionColumnNames;
        private Map<String, Object> partitionValues;
        @Nullable
        private String datasetName;
        private Map<String, String> renames = Map.of();
        @Nullable
        private String idPath;
        @Nullable
        private Long lastModifiedMillis;
        @Nullable
        private BlockFactory producerBlockFactory;
        private ExternalSliceQueue sliceQueue;
        private ErrorPolicy errorPolicy;
        private int parsingParallelism = 1;
        // Production sets this from the max_concurrent_open_segments pragma via LocalExecutionPlanner; this
        // is the test/internal fallback, sourced from the single source of truth.
        private int maxConcurrentOpenSegments = SourceOperatorContext.DEFAULT_MAX_CONCURRENT_OPEN_SEGMENTS;
        private int maxRecordBytes = SegmentableFormatReader.DEFAULT_MAX_RECORD_BYTES;
        private long statsStripeSize = -1L;
        private StripeColumnScope statsColumnScope = StripeColumnScope.PROJECTED;
        // Non-null default: test builders that never set one still get an (unbounded) controller, so the streaming
        // coordinator's admission is never null. Production (FileSourceFactory) overrides with the per-node gate.
        private StreamingSegmentatorAdmission streamingSegmentatorAdmission = StreamingSegmentatorAdmission.unbounded();
        private List<Expression> pushedExpressions;
        private FilterPushdownSupport pushdownSupport;
        private Closeable onClose;
        private int parallelism = 1;
        private boolean deferredExtraction = false;
        private ExternalSourceMetrics externalSourceMetrics = ExternalSourceMetrics.NOOP;

        private Builder(
            StorageProvider storageProvider,
            FormatReader formatReader,
            StoragePath path,
            List<Attribute> attributes,
            int batchSize,
            int maxBufferSize,
            Executor executor
        ) {
            this.storageProvider = storageProvider;
            this.formatReader = formatReader;
            this.path = path;
            this.attributes = attributes;
            this.batchSize = batchSize;
            this.maxBufferSize = maxBufferSize;
            this.executor = executor;
        }

        /**
         * Executor for the page consumer (producer loop + drain), distinct from the read/parse executor passed to
         * {@link #builder}. Production wires this to {@code esql_worker} while the read/parse executor is the
         * {@code esql_external_io} pool, so a full I/O pool of blocked parser workers cannot starve the drain that
         * consumes their pages. When unset (or {@code null}), the consumer shares the read/parse executor — the prior
         * single-pool behavior, retained for tests and callers that pass one executor.
         */
        public Builder producerExecutor(@Nullable Executor producerExecutor) {
            this.producerExecutor = producerExecutor;
            return this;
        }

        public Builder rowLimit(int rowLimit) {
            this.rowLimit = rowLimit;
            return this;
        }

        public Builder fileList(@Nullable FileList fileList) {
            this.fileList = fileList;
            return this;
        }

        public Builder schemaMap(@Nullable Map<StoragePath, SchemaReconciliation.FileSchemaInfo> schemaMap) {
            this.schemaMap = schemaMap;
            return this;
        }

        public Builder partitionColumnNames(@Nullable Set<String> partitionColumnNames) {
            this.partitionColumnNames = partitionColumnNames;
            return this;
        }

        public Builder partitionValues(@Nullable Map<String, Object> partitionValues) {
            this.partitionValues = partitionValues;
            return this;
        }

        /**
         * Sets the dataset name surfaced to query rows via the {@code _index} metadata column.
         * {@code null} when the {@code FROM} did not resolve to a single registered dataset (e.g.
         * bare-glob {@code FROM "s3://bucket/*.parquet"}); the {@code _index} column then renders
         * as SQL {@code NULL}. Only consulted when the bound attributes include an
         * {@code ExternalMetadataAttribute} named {@code _index}.
         */
        public Builder datasetName(@Nullable String datasetName) {
            this.datasetName = datasetName;
            return this;
        }

        /** Declared logical-&gt;physical column renames; applied to reader-facing names at the last mile. */
        public Builder renames(Map<String, String> renames) {
            this.renames = renames == null ? Map.of() : renames;
            return this;
        }

        /**
         * Declared {@code _id.path} (the logical name of the data column that supplies each row's {@code _id}), or
         * {@code null} when the dataset declares no {@code mappings._id.path}. When set, {@link VirtualColumnIterator}
         * stamps {@code _id} from that column instead of the synthetic (file+row-position) identity.
         */
        public Builder idPath(@Nullable String idPath) {
            this.idPath = idPath;
            return this;
        }

        /**
         * Sets the file last-modified epoch-millis used to materialise {@code _version} on the
         * single-file producer paths. {@code null} (the default) leaves {@code _version} as SQL
         * {@code NULL} on those paths. Only consulted when the source has no per-file mtime
         * source — the slice-queue path reads {@code _file.modified} out of the
         * {@code FileSplit}'s partition values, and the multi-file path reads it off the
         * {@code FileList} entry; both ignore this builder value.
         */
        public Builder lastModifiedMillis(@Nullable Long lastModifiedMillis) {
            this.lastModifiedMillis = lastModifiedMillis;
            return this;
        }

        /**
         * Sets the {@link BlockFactory} used by {@link VirtualColumnIterator} when materialising
         * constant blocks for virtual partition / {@code _file.*} columns. Production should pass the
         * node-level (root) factory so producer-thread allocations route through the global request
         * circuit breaker rather than the driver-local breaker (which asserts single-thread access
         * during the driver's run loop). Tests may leave this {@code null} to fall back to the
         * driver context's factory.
         */
        public Builder producerBlockFactory(@Nullable BlockFactory producerBlockFactory) {
            this.producerBlockFactory = producerBlockFactory;
            return this;
        }

        public Builder sliceQueue(@Nullable ExternalSliceQueue sliceQueue) {
            this.sliceQueue = sliceQueue;
            return this;
        }

        public Builder errorPolicy(@Nullable ErrorPolicy errorPolicy) {
            this.errorPolicy = errorPolicy;
            return this;
        }

        public Builder parsingParallelism(int parsingParallelism) {
            this.parsingParallelism = parsingParallelism;
            return this;
        }

        public Builder maxConcurrentOpenSegments(int maxConcurrentOpenSegments) {
            this.maxConcurrentOpenSegments = maxConcurrentOpenSegments;
            return this;
        }

        public Builder maxRecordBytes(int maxRecordBytes) {
            this.maxRecordBytes = maxRecordBytes;
            return this;
        }

        /**
         * Canonical-stripe grid for per-stripe stats accounting, in decompressed-stream bytes
         * ({@code <= 0} disables — the default). See {@code ExternalSourceCacheSettings#STRIPE_SIZE}
         * and {@code StreamingParallelParsingCoordinator}; stats-accounting overlay only, never a
         * partitioning or scheduling input.
         */
        public Builder statsStripeSize(long statsStripeSize) {
            this.statsStripeSize = statsStripeSize;
            return this;
        }

        /**
         * How much per-stripe statistics a fresh scan harvests (see
         * {@code ExternalSourceCacheSettings#STRIPE_COLUMNS} and {@link StripeColumnScope}). {@code null}
         * restores the {@link StripeColumnScope#PROJECTED} default. Orthogonal to {@link #statsStripeSize}.
         */
        public Builder statsColumnScope(@Nullable StripeColumnScope statsColumnScope) {
            this.statsColumnScope = statsColumnScope != null ? statsColumnScope : StripeColumnScope.PROJECTED;
            return this;
        }

        public Builder pushedExpressions(@Nullable List<Expression> pushedExpressions) {
            this.pushedExpressions = pushedExpressions;
            return this;
        }

        public Builder pushdownSupport(@Nullable FilterPushdownSupport pushdownSupport) {
            this.pushdownSupport = pushdownSupport;
            return this;
        }

        /**
         * @param onClose lifecycle callback owned by this factory, invoked exactly once when the last
         *                operator created by {@link AsyncExternalSourceOperatorFactory#get} completes
         *                (ref count drops to zero). Used by the per-source concurrency budget to
         *                deregister from the allocator. May be {@code null} when no per-source cleanup
         *                is needed. Callers must ensure that {@code get()} is called at least once;
         *                otherwise the callback never fires and the resource it guards leaks.
         */
        public Builder onClose(@Nullable Closeable onClose) {
            this.onClose = onClose;
            return this;
        }

        public Builder parallelism(int parallelism) {
            this.parallelism = parallelism;
            return this;
        }

        /**
         * Opt into deferred field extraction: the source emits a synthetic
         * {@link ColumnExtractor#ROW_POSITION_COLUMN} column in encoded form (extractor id packed
         * with file-local position) and registers a {@link ColumnExtractor} per opened file. The
         * caller (typically {@code InsertExternalFieldExtraction} via {@code LocalExecutionPlanner})
         * pairs this with an {@link ExternalFieldExtractOperator} that resolves the extractor
         * by id and materializes the deferred columns post-TopN.
         * <p>
         * The configured {@link FormatReader} must implement {@link ColumnExtractorAware} —
         * verified at {@link #build} time.
         */
        public Builder deferredExtraction(boolean deferredExtraction) {
            this.deferredExtraction = deferredExtraction;
            return this;
        }

        /** Node telemetry sink for object-store read metrics; defaults to {@link ExternalSourceMetrics#NOOP}. */
        public Builder externalSourceMetrics(ExternalSourceMetrics externalSourceMetrics) {
            this.externalSourceMetrics = externalSourceMetrics == null ? ExternalSourceMetrics.NOOP : externalSourceMetrics;
            return this;
        }

        /**
         * Node-level gate bounding concurrent streaming segmentators on the shared {@code esql_external_io} pool
         * (see {@link StreamingSegmentatorAdmission}). Set by {@link FileSourceFactory} from node settings; when
         * unset the builder keeps its {@link StreamingSegmentatorAdmission#unbounded()} default.
         */
        public Builder streamingSegmentatorAdmission(StreamingSegmentatorAdmission streamingSegmentatorAdmission) {
            this.streamingSegmentatorAdmission = Objects.requireNonNull(streamingSegmentatorAdmission, "admission");
            return this;
        }

        public AsyncExternalSourceOperatorFactory build() {
            return new AsyncExternalSourceOperatorFactory(
                storageProvider,
                formatReader,
                path,
                attributes,
                batchSize,
                maxBufferSize,
                rowLimit,
                executor,
                producerExecutor,
                fileList,
                schemaMap,
                partitionColumnNames,
                partitionValues,
                datasetName,
                renames,
                idPath,
                lastModifiedMillis,
                producerBlockFactory,
                sliceQueue,
                errorPolicy,
                parsingParallelism,
                maxConcurrentOpenSegments,
                maxRecordBytes,
                statsStripeSize,
                statsColumnScope,
                streamingSegmentatorAdmission,
                pushedExpressions,
                pushdownSupport,
                onClose,
                parallelism,
                deferredExtraction,
                externalSourceMetrics
            );
        }
    }

    /**
     * Installs the shared numeric TopN threshold. Must be called during planning, before the
     * first {@link #get(DriverContext)} call creates source operators from this factory.
     */
    public synchronized void setNumericThresholdSupplier(
        SharedNumericThreshold.Supplier thresholdSupplier,
        String columnName,
        ElementType elementType,
        boolean ascending,
        boolean nullsFirst
    ) {
        if (operatorRefCount.get() != 0) {
            throw new IllegalStateException("numeric threshold must be installed before source operators are created");
        }
        this.thresholdSupplier = thresholdSupplier;
        // Numeric and BytesRef thresholds are mutually exclusive (one sort key, one type); clear the
        // other so a stale supplier can never be picked up by dynamicThreshold().
        this.minCompetitiveSupplier = null;
        this.thresholdColumnName = columnName;
        this.thresholdElementType = elementType;
        this.thresholdAscending = ascending;
        this.thresholdNullsFirst = nullsFirst;
        closeDynamicThreshold();
    }

    /**
     * Installs the shared {@code BYTES_REF} competitive threshold for a single keyword/text sort
     * key, fed by the generic {@code TopNOperator}'s {@link SharedMinCompetitive} side-channel. Must
     * be called during planning, before the first {@link #get(DriverContext)} call creates source
     * operators from this factory.
     */
    public synchronized void setMinCompetitiveSupplier(
        SharedMinCompetitive.Supplier minCompetitiveSupplier,
        String columnName,
        boolean ascending,
        boolean nullsFirst
    ) {
        if (operatorRefCount.get() != 0) {
            throw new IllegalStateException("min competitive threshold must be installed before source operators are created");
        }
        this.minCompetitiveSupplier = minCompetitiveSupplier;
        // Mutually exclusive with the numeric threshold; clear it so a stale supplier can never be
        // picked up by dynamicThreshold().
        this.thresholdSupplier = null;
        this.thresholdColumnName = columnName;
        this.thresholdElementType = ElementType.BYTES_REF;
        this.thresholdAscending = ascending;
        this.thresholdNullsFirst = nullsFirst;
        closeDynamicThreshold();
    }

    @Override
    public SourceOperator get(DriverContext driverContext) {
        operatorRefCount.incrementAndGet();
        try {
            long maxBufferBytes = (long) maxBufferSize * Operator.TARGET_PAGE_SIZE;
            AsyncExternalSourceBuffer buffer = new AsyncExternalSourceBuffer(maxBufferBytes);
            driverContext.addAsyncAction();
            // Implements async STOP semantics for coordinator-only EXTERNAL plans: when the user fires
            // {@code POST /_query/async/{id}/stop}, the task's stop-hook list fans out to each driver's
            // {@link DriverContext}, which calls this hook. {@code buffer.finish(false)} flips
            // {@code noMoreInputs} on the buffer — pages already accepted into the queue stay reachable
            // to the driver loop and flow through the pipeline, while producer threads observe the flag
            // and exit instead of accepting more pages.
            //
            // {@code finish} performs a CAS on {@code noMoreInputs} and returns {@code true} only when
            // it actually made the running→finishing transition; if the producer already EOF'd (natural
            // completion) or another thread finished the buffer first, it returns {@code false}. That
            // eliminates the check-then-act race between {@code buffer.noMoreInputs()} and
            // {@code buffer.finish(false)}: STOP marking the response {@code is_partial=true} now
            // requires a genuine live cut, not merely observing "already finished".
            driverContext.addStopHook(() -> buffer.finish(false));

            if (sliceQueue != null) {
                startSliceQueueRead(buffer, driverContext);
            } else if (fileList != null && fileList.isResolved()) {
                List<String> projectedColumns = dataProjectedColumns();
                startMultiFileRead(projectedColumns, buffer, driverContext);
            } else {
                List<String> projectedColumns = dataProjectedColumns();
                StorageObject storageObject = storageProvider.newObject(path);
                // Attach the telemetry sink BEFORE any read: the first read (newStream) records on the
                // object's counters, so attaching after — as an earlier version did — loses the storage
                // request/bytes metrics for whole-file providers that finish the read at open.
                attachStorageMetrics(storageObject);
                if (formatReader.supportsNativeAsync()) {
                    startNativeAsyncRead(storageObject, projectedColumns, buffer, driverContext);
                } else {
                    startSyncWrapperRead(storageObject, projectedColumns, buffer, driverContext);
                }
            }

            return new AsyncExternalSourceOperator(buffer, externalSourceMetrics, path.scheme());
        } catch (Exception e) {
            releaseOperator();
            throw e;
        }
    }

    /**
     * Resolves (or lazily creates) the {@link SourceExtractors} registry shared between this
     * factory's source operator and a paired {@link ExternalFieldExtractOperator} for the same
     * driver. The registry is populated as files are opened and read via the registered
     * {@link ColumnExtractor#extract} contract.
     * <p>
     * Only meaningful when {@link Builder#deferredExtraction} was enabled. Calls during the
     * non-deferred mode return a fresh empty registry; callers in that mode should not depend on
     * its contents.
     */
    @Override
    public SourceExtractors sourceExtractorsFor(DriverContext driverContext) {
        return sourceExtractorsPerDriver.computeIfAbsent(driverContext, ctx -> {
            SourceExtractors registry = new SourceExtractors();
            attachOnCloseToRegistry(registry);
            return registry;
        });
    }

    /**
     * If deferred extraction is enabled and an {@link #onClose} resource is attached, hand it to
     * the registry as a trailing closeable. The registry is the natural rendezvous between the
     * source side (writes to it during the producer loop) and the
     * {@link ExternalFieldExtractOperator} side (reads from it during driver runtime, after the
     * source has already finished producing). Closing the registry then closes the resource only
     * once the last user has gone away — see {@link #deferredCloseRefCount} for the refcounting.
     */
    private void attachOnCloseToRegistry(SourceExtractors registry) {
        if (deferredExtraction == false || onClose == null) {
            return;
        }
        deferredCloseRefCount.incrementAndGet();
        // The releaser is registered LIFO via SourceExtractors#registerTrailingCloseable, so it
        // runs after every ColumnExtractor has been closed (and thus after the storage objects
        // they own have flushed any final permit releases through the budget).
        registry.registerTrailingCloseable(this::releaseDeferredCloseRef);
    }

    /**
     * Decrement the deferred-close refcount; once it hits zero the {@link #onClose} resource
     * (typically the per-query concurrency budget) is closed. Called from two places:
     * <ul>
     *   <li>{@link #releaseOperator} when the last source operator finishes — releases the
     *       factory's own ref;</li>
     *   <li>{@link SourceExtractors#close()} via the trailing closeable attached in
     *       {@link #attachOnCloseToRegistry} — releases each registry's ref.</li>
     * </ul>
     */
    private void releaseDeferredCloseRef() {
        if (deferredCloseRefCount.decrementAndGet() == 0) {
            closeQuietly(onClose);
        }
    }

    /**
     * Whether deferred field extraction is enabled on this factory. See
     * {@link Builder#deferredExtraction(boolean)} for semantics.
     */
    public boolean deferredExtractionEnabled() {
        return deferredExtraction;
    }

    /**
     * Build a {@link ColumnExtractor} from the iterator and register it with the given driver's
     * {@link SourceExtractors}. Returns the assigned extractor id, which the caller hands back to
     * the iterator via {@link ColumnExtractorProducer#setExtractorId(int)} so the iterator can
     * pre-encode every {@code _rowPosition} value it emits with that id. Caller must ensure the
     * reader implements {@link ColumnExtractorAware}; this is validated at
     * {@link Builder#build} time.
     */
    private int registerExtractorFromProducer(ColumnExtractorProducer producer, DriverContext driverContext) throws IOException {
        ColumnExtractor extractor = producer.createColumnExtractor();
        return sourceExtractorsFor(driverContext).register(extractor);
    }

    /**
     * Returns the projected-column names the format reader should produce: real data columns plus
     * the synthetic {@link ColumnExtractor#ROW_POSITION_COLUMN} when deferred extraction is on.
     * Both Hive-style partition columns and {@code _file.*} virtual columns are stripped because
     * the reader cannot materialise them — they are filled in on the producer thread by
     * {@link VirtualColumnIterator}. Note that we cannot drive this entirely from
     * {@link #queryDataSchema} because that view drops every
     * {@link org.elasticsearch.xpack.esql.core.expression.MetadataAttribute}, including
     * {@link ColumnExtractor#ROW_POSITION_COLUMN}, which the deferred-extraction handshake
     * needs to find back in the projected channel layout (see {@link #rowPositionChannelIndex}).
     */
    private List<String> dataProjectedColumns() {
        List<String> cols = new ArrayList<>(attributes.size());
        for (Attribute attr : attributes) {
            // Engine-synthesized columns ({@code _file.*} today, anything implementing
            // {@link VirtualAttribute} tomorrow) are not present in the file's schema; the
            // format reader has nothing to project for them. They reach downstream operators
            // via {@link VirtualColumnIterator}'s constant-block injection.
            if (attr instanceof VirtualAttribute) {
                continue;
            }
            if (partitionColumnNames.contains(attr.name())) {
                continue;
            }
            // Standard ES metadata names ({@code _id}, {@code _index}, ...) are not file-resident
            // columns; the producer injects them later. {@code _index}, {@code _version}, etc. enter
            // {@link VirtualColumnIterator}'s per-file constant path via the {@code partitionColumnNames}
            // union above. {@code _id} and {@code _source} are handled by separate operator wrappers.
            if (attr instanceof ExternalMetadataAttribute) {
                continue;
            }
            cols.add(attr.name());
        }
        return cols;
    }

    /**
     * Merge standard ES metadata per-file constants ({@code _index}, {@code _version}, ...) into
     * {@code basePartitionValues}. Returns {@code basePartitionValues} unchanged when no standard
     * metadata names are bound. The {@code _version} value is sourced from the {@code _file.modified}
     * entry already populated in {@code basePartitionValues} (slice-queue path); when absent
     * (single-file paths) the factory's {@link #lastModifiedMillis} is used as a fallback. When
     * neither is available {@code _version} renders as SQL {@code NULL}.
     * <p>
     * Standard metadata names win on key collision: they are dedicated (the spec defines what
     * {@code _index} means; a layout cannot redefine it), so the constants overlay last.
     * {@code HivePartitionDetector} already renames colliding partition columns to
     * {@code _partition.*} upstream, so a collision here means a non-Hive path smuggled a
     * reserved key into the partition-value map — the overlay keeps the spec honest regardless.
     * {@code _file.*} keys cannot collide with standard names (disjoint namespace) and are
     * unaffected by the overlay order.
     */
    private Map<String, Object> mergeStandardMetadata(Map<String, Object> basePartitionValues) {
        // Any new standard-metadata name must also be added to
        // {@link ExternalMetadataColumns#PER_FILE_CONSTANT_NAMES} or this gate silently skips it.
        if (standardMetadataPerFileNames.isEmpty()) {
            return basePartitionValues;
        }
        // Same key-present-vs-absent split as resolveMtimeMillis: a per-file extraction that
        // reported no mtime must yield a null _version for that file, not the factory-level
        // (first file's) mtime.
        Long version;
        if (basePartitionValues != null && basePartitionValues.containsKey(FileMetadataColumns.MODIFIED)) {
            version = basePartitionValues.get(FileMetadataColumns.MODIFIED) instanceof Long longVersion ? longVersion : null;
        } else {
            version = lastModifiedMillis;
        }
        Map<String, Object> stdConstants = ExternalMetadataColumns.extractPerFileConstants(datasetName, version);
        Map<String, Object> merged = basePartitionValues != null ? new HashMap<>(basePartitionValues) : new HashMap<>();
        merged.putAll(stdConstants);
        return merged;
    }

    /**
     * Wraps {@code pages} in a {@link VirtualColumnIterator} when this factory has any virtual
     * partition / {@code _file.*} columns. The iterator allocates the constant blocks against
     * {@link #producerBlockFactory} when set (production: the node-level root factory) and
     * falls back to the driver context's factory otherwise (test convenience). Returns
     * {@code pages} unchanged when there are no virtual columns to materialise.
     */
    private CloseableIterator<Page> wrapWithVirtualColumns(
        CloseableIterator<Page> pages,
        Map<String, Object> partitionValuesForFile,
        DriverContext driverContext
    ) {
        return wrapWithVirtualColumns(pages, partitionValuesForFile, driverContext, this.path);
    }

    /**
     * Variant that also wires the per-file {@code _id} prefix when {@code _id} is requested.
     * Callers in multi-file paths pass the file's actual {@link StoragePath} so the rendered
     * {@code _id} reflects which physical file each row came from. The prefix carries the file's
     * mtime as an identity salt, resolved the same way {@link #mergeStandardMetadata} resolves
     * {@code _version}: the per-file {@code _file.modified} value when the listing carried one,
     * else the factory-level {@link #lastModifiedMillis}, else {@code 0} (unknown).
     */
    private CloseableIterator<Page> wrapWithVirtualColumns(
        CloseableIterator<Page> pages,
        Map<String, Object> partitionValuesForFile,
        DriverContext driverContext,
        StoragePath filePath
    ) {
        if (partitionColumnNames.isEmpty()) {
            return pages;
        }
        BytesRef idPrefix = idColumnRequested ? ExternalRowIdentity.prefix(filePath, resolveMtimeMillis(partitionValuesForFile)) : null;
        return new VirtualColumnIterator(
            pages,
            attributes,
            partitionColumnNames,
            partitionValuesForFile,
            producerBlockFactory(driverContext),
            idPrefix,
            idPath
        );
    }

    /**
     * Resolves the mtime salt for the {@code _id} prefix. The per-file {@code _file.modified}
     * value wins (multi-file paths build it from the listing via
     * {@link FileMetadataColumns#extractValues}); the factory-level {@link #lastModifiedMillis}
     * covers the single-file path; {@code 0} means the storage layer reported no mtime, matching
     * the {@link org.elasticsearch.xpack.esql.datasources.spi.FileList} missing-mtime convention.
     */
    private long resolveMtimeMillis(Map<String, Object> partitionValuesForFile) {
        // Key present = a per-file extraction ran for THIS file: a Long is its mtime; null means
        // the storage layer reported none for this file — return the 0 sentinel rather than fall
        // through, or an unknown-mtime file in a multi-file glob would inherit the factory-level
        // (first file's) mtime as its _id salt. The factory fallback serves single-file paths,
        // where no per-file extraction populated the map.
        if (partitionValuesForFile != null && partitionValuesForFile.containsKey(FileMetadataColumns.MODIFIED)) {
            return partitionValuesForFile.get(FileMetadataColumns.MODIFIED) instanceof Long mtime ? mtime : 0L;
        }
        return lastModifiedMillis != null ? lastModifiedMillis : 0L;
    }

    /**
     * Resolves the {@link BlockFactory} used by producer-thread iterator wrappers (see field
     * Javadoc on {@link #producerBlockFactory}). Production wires the node-level root factory;
     * tests fall back to the driver-local factory because they typically run the producer and
     * consumer on the same thread, so the local breaker's single-thread assertion does not fire.
     */
    private BlockFactory producerBlockFactory(DriverContext driverContext) {
        return producerBlockFactory != null ? producerBlockFactory : driverContext.blockFactory();
    }

    /**
     * Index of {@link ColumnExtractor#ROW_POSITION_COLUMN} in {@code projectedColumns} (the
     * post-projection, pre-virtual-column channel layout); {@code -1} when the column is absent.
     * Used by deferred-extraction wiring to know which channel the encoder must rewrite.
     */
    private static int rowPositionChannelIndex(List<String> projectedColumns) {
        return SyntheticColumns.rowPositionIndexInNames(projectedColumns);
    }

    /**
     * Performs the deferred-extraction handshake when active and the projection contains
     * {@link ColumnExtractor#ROW_POSITION_COLUMN}: registers the iterator's matching
     * {@link ColumnExtractor} with the driver's {@link SourceExtractors} and hands the assigned
     * id back to the iterator via {@link ColumnExtractorProducer#setExtractorId(int)} so it can
     * pre-encode every {@code _rowPosition} value it emits. Returns {@code pages} unchanged in
     * every case — there is no per-page wrapper iterator on the deferred path. Off the deferred
     * path, or when the optimizer hasn't injected {@code _rowPosition}, the call is a no-op.
     * <p>
     * Historically this method wrapped {@code pages} in an {@code EncodingRowRefIterator} that
     * rebuilt the {@code _rowPosition} block on every page; pushing the encoding into the
     * iterator removes a per-page allocation and a thread-safety hazard around the local
     * circuit breaker.
     */
    private CloseableIterator<Page> wrapWithEncoderIfNeeded(
        CloseableIterator<Page> pages,
        List<String> projectedColumns,
        DriverContext driverContext
    ) throws IOException {
        if (deferredExtraction == false) {
            // No paired extract operator: no registry, no refcount, nothing to decode downstream.
            // A ColumnExtractorAware reader's iterator still ORs the installed high bits into every
            // _rowPosition value, so install zero — the unencoded form — whenever the channel is
            // projected for plain _id / _file.record_ref composition (which masks high bits anyway).
            // Gate on the READER capability, not the iterator: stats/schema wrappers implement
            // ColumnExtractorProducer as blind pass-throughs that throw when the delegate isn't one.
            if (formatReader instanceof ColumnExtractorAware
                && rowPositionChannelIndex(projectedColumns) >= 0
                && pages instanceof ColumnExtractorProducer producer) {
                producer.setExtractorId(0);
            }
            return pages;
        }
        int rpChannel = rowPositionChannelIndex(projectedColumns);
        if (rpChannel < 0) {
            // Optimizer hasn't injected _rowPosition into the projection. This can happen on
            // sub-plans the rule chose not to rewrite (e.g. no TopN above this source). Pass
            // through unchanged.
            return pages;
        }
        // Producer handshake: the iterator that emitted the synthetic _rowPosition is also the
        // authority on its addressing space (full file, range-restricted row groups, etc.). Use
        // its produced extractor so the lookup table is scoped exactly the same way as the
        // running counter — see {@link ColumnExtractor} for the contract this preserves. We
        // register the extractor first, then hand its id back to the iterator: from this point
        // on every page the iterator returns carries already-encoded values, no wrapping needed.
        if (pages instanceof ColumnExtractorProducer producer) {
            int id = registerExtractorFromProducer(producer, driverContext);
            producer.setExtractorId(id);
            return pages;
        }
        throw new IllegalStateException(
            "deferred extraction enabled but reader iterator ["
                + pages.getClass().getName()
                + "] does not implement ColumnExtractorProducer"
        );
    }

    /**
     * Applies the reader's {@link RowPositionStrategy} to {@code pages}. The strategy decides whether
     * the inner iterator already carries the {@code _rowPosition} column ({@link
     * org.elasticsearch.xpack.esql.datasources.spi.PassThroughRowPositionStrategy} — no-op), needs a
     * NULL splice ({@link org.elasticsearch.xpack.esql.datasources.spi.NullSpliceRowPositionStrategy}),
     * or some future shape. The dispatcher does not switch on reader type: it asks the reader for its
     * strategy and invokes {@code apply} polymorphically.
     */
    private static CloseableIterator<Page> applyRowPositionStrategy(
        FormatReader reader,
        CloseableIterator<Page> pages,
        List<String> projectedColumns
    ) {
        // Pre-compute the slot index once per reader.read() — strategies inspect a primitive
        // instead of walking projectedColumns per apply(). Returns -1 when _rowPosition is not
        // in the projection, which strategies short-circuit on.
        int rowPositionSlot = SyntheticColumns.rowPositionIndexInNames(projectedColumns);
        return reader.rowPositionStrategy().apply(pages, rowPositionSlot);
    }

    /**
     * Translates the unified query projection (column names in unified-schema shape, identical for every
     * file in the query) into a per-file query projection (the subset present in this file's schema,
     * ordered to match the file's natural layout).
     * <p>
     * Under UBN the unified projection may name columns that are missing from a given file. Handing
     * those names to the reader would throw "Column not found in schema"; the reader is the wrong
     * place to handle missing columns. The {@link SchemaAdaptingIterator} wrapping the reader output
     * does the null-filling via the per-file {@code ColumnMapping}. This narrowing matches the
     * adapter's input contract: the reader produces only columns that exist in the file, in the
     * file's natural order.
     * <p>
     * Under FFW and STRICT (and single-file), {@code perFileReadSchema} either equals or contains
     * every name in {@code queryProjection}, so the result equals {@code queryProjection} (modulo
     * order). When {@code perFileReadSchema} is null (no pin from the coordinator), the original
     * projection passes through unchanged.
     */
    static List<String> perFileQueryProjection(List<String> queryProjection, @Nullable List<Attribute> perFileReadSchema) {
        if (perFileReadSchema == null || perFileReadSchema.isEmpty() || queryProjection == null || queryProjection.isEmpty()) {
            return queryProjection;
        }
        Set<String> wanted = new HashSet<>(queryProjection);
        List<String> result = new ArrayList<>(Math.min(queryProjection.size(), perFileReadSchema.size()));
        for (Attribute attr : perFileReadSchema) {
            if (wanted.contains(attr.name())) {
                result.add(attr.name());
            }
        }
        // The synthetic deferred-extraction signal is never present in the file's read schema (it
        // is optimizer-inserted and resolved at read time by the format reader). Preserve it at the
        // tail of the per-file projection so the reader's _rowPosition emission path fires for
        // every file, regardless of how many of the user's data columns the file is missing.
        if (wanted.contains(ColumnExtractor.ROW_POSITION_COLUMN)) {
            // Defensive: only append once. If a future caller decides _rowPosition should also be
            // present in perFileReadSchema, the loop above would already have added it and we
            // skip the appender to keep the projection unique.
            if (result.contains(ColumnExtractor.ROW_POSITION_COLUMN) == false) {
                result.add(ColumnExtractor.ROW_POSITION_COLUMN);
            }
        }
        return result;
    }

    /**
     * Returns {@code attributes} without the synthetic deferred-extraction marker so a reader's
     * {@code resolvedAttributes} / {@code readSchema} reflect only real file columns. Returns the
     * input list unchanged when the marker is absent (deferred extraction off, or the optimizer
     * never injected the synthetic), so callers without deferred extraction pay nothing.
     * <p>
     * Package-private for unit tests.
     */
    static List<Attribute> stripRowPosition(List<Attribute> attributes) {
        if (attributes == null || attributes.isEmpty()) {
            return attributes;
        }
        int idx = -1;
        for (int i = 0; i < attributes.size(); i++) {
            if (ColumnExtractor.ROW_POSITION_COLUMN.equals(attributes.get(i).name())) {
                idx = i;
                break;
            }
        }
        if (idx < 0) {
            return attributes;
        }
        List<Attribute> result = new ArrayList<>(attributes.size() - 1);
        for (int i = 0; i < attributes.size(); i++) {
            if (i != idx) {
                result.add(attributes.get(i));
            }
        }
        return result;
    }

    private CloseableIterator<Page> adaptSchema(
        CloseableIterator<Page> pages,
        ColumnMapping mapping,
        DriverContext driverContext,
        @Nullable List<Attribute> perFileReadSchema,
        @Nullable List<String> perFileCols
    ) {
        // Empty queryDataSchema = no data columns projected (COUNT(*), _file.*-only, or a TopN with
        // all data columns deferred to _rowPosition): nothing to reshape, and the full-width mapping
        // would trip SchemaAdaptingIterator's size-vs-width guard. Treat it like identity and pass the
        // pages through (deferred extraction is handled in wrapWithEncoderIfNeeded).
        if (mapping == null || mapping.isIdentity() || queryDataSchema.isEmpty()) {
            return pages;
        }
        // The reader appends the synthetic {@link ColumnExtractor#ROW_POSITION_COLUMN} to the
        // per-file projection whenever the query projection carries it — for deferred extraction
        // AND for plain _id / _file.record_ref composition (see {@link #perFileQueryProjection}).
        // Its input slot is its position in the per-file projection: the reader emits blocks in
        // projection order, so this index addresses the reader's output page directly. Deriving
        // the slot from the deferred flag or from {@code mapping.width()} is wrong on both arms:
        // non-deferred readers also emit the channel (dropping it here starves the downstream
        // VirtualColumnIterator of a block it counts on), and width() is the OUTPUT width, which
        // diverges from the input slot whenever the file is missing query columns under UBN.
        int rowPositionInputIndex = SyntheticColumns.rowPositionIndexInNames(perFileCols);
        // Per-file source types are only needed to disambiguate LongBlock under a KEYWORD cast.
        // Every other cast path is self-contained and ignores the array, so we skip the lookup
        // for mappings that have no KEYWORD slots — i.e. virtually every file split outside the
        // UBN cross-type-drift path.
        DataType[] perFileColumnTypes = mapping.hasKeywordCast()
            ? ColumnMapping.buildPerFileColumnTypes(perFileReadSchema, perFileCols)
            : null;
        // Use the producer-thread factory: SchemaAdaptingIterator allocates null-fill / cast
        // blocks while the reader is draining pages off the generic pool, off the driver thread.
        return new SchemaAdaptingIterator(
            pages,
            queryDataSchema.attributes(),
            mapping,
            producerBlockFactory(driverContext),
            rowPositionInputIndex,
            perFileColumnTypes
        );
    }

    /**
     * Returns a format reader with an adapted pushed filter for this file, or the original reader
     * if no adaptation is needed. Adaptation is needed when the file has missing columns and
     * pushed expressions reference those columns.
     */
    private FormatReader readerForFile(FileSplit fileSplit) {
        FormatReader reader = formatReader;
        if (pushedExpressions.isEmpty() == false && pushdownSupport != null) {
            ColumnMapping mapping = fileSplit.columnMapping();
            if (mapping != null) {
                List<Expression> adapted = mapping.mapFilters(pushedExpressions, queryDataSchema);
                if (adapted != pushedExpressions) {
                    if (adapted.isEmpty()) {
                        reader = formatReader.withPushedFilter(null);
                    } else {
                        // adapted is logical (mapFilters + queryDataSchema); physicalize it so the re-minted opaque
                        // predicate references the file's physical columns, matching the plan-time mint.
                        List<Expression> physicalAdapted = PhysicalNames.translateExpressionNames(adapted, renames);
                        // Same invariant as the plan-time mint: no logical rename-source name may reach the reader's filter.
                        assert PhysicalNames.noLogicalNamesRemain(
                            physicalAdapted.stream().flatMap(e -> e.references().stream()).map(Attribute::name).toList(),
                            renames
                        ) : "logical rename-source name leaked into the re-minted pushed filter: " + physicalAdapted;
                        FilterPushdownSupport.PushdownResult result = pushdownSupport.pushFilters(physicalAdapted);
                        reader = result.hasPushedFilter()
                            ? formatReader.withPushedFilter(result.pushedFilter())
                            : formatReader.withPushedFilter(null);
                    }
                }
            }
        }
        return readerWithDynamicThreshold(reader);
    }

    @Nullable
    private DynamicThreshold dynamicThreshold() {
        DynamicThreshold threshold = cachedThreshold;
        if (threshold != null || (thresholdSupplier == null && minCompetitiveSupplier == null)) {
            return threshold;
        }
        synchronized (this) {
            threshold = cachedThreshold;
            if (threshold == null) {
                if (thresholdColumnName == null) {
                    throw new IllegalStateException("threshold descriptor is incomplete");
                }
                if (thresholdSupplier != null) {
                    if (thresholdElementType == null) {
                        throw new IllegalStateException("numeric threshold descriptor is incomplete");
                    }
                    threshold = new DynamicThreshold(
                        PhysicalNames.translate(thresholdColumnName, renames),
                        thresholdElementType,
                        thresholdAscending,
                        thresholdNullsFirst,
                        thresholdSupplier.get()
                    );
                } else if (minCompetitiveSupplier != null) {
                    threshold = new DynamicThreshold(
                        PhysicalNames.translate(thresholdColumnName, renames),
                        thresholdAscending,
                        thresholdNullsFirst,
                        minCompetitiveSupplier.get()
                    );
                }
                cachedThreshold = threshold;
            }
            return threshold;
        }
    }

    private boolean noFurtherCandidates() {
        DynamicThreshold threshold = dynamicThreshold();
        return threshold != null && threshold.noFurtherCandidates();
    }

    private FormatReader readerWithDynamicThreshold(FormatReader reader) {
        DynamicThreshold threshold = dynamicThreshold();
        if (threshold != null && reader instanceof DynamicThresholdAware aware) {
            return aware.withDynamicThreshold(threshold);
        }
        return reader;
    }

    private synchronized void closeDynamicThreshold() {
        DynamicThreshold threshold = cachedThreshold;
        cachedThreshold = null;
        if (threshold != null) {
            IOUtils.closeWhileHandlingException(threshold);
        }
    }

    private void startSliceQueueRead(AsyncExternalSourceBuffer buffer, DriverContext driverContext) {
        ActionListener<Void> completionListener = ActionListener.assertOnce(ActionListener.wrap(v -> {
            buffer.finish(false);
            driverContext.removeAsyncAction();
            releaseOperator();
        }, e -> {
            buffer.onFailure(e);
            driverContext.removeAsyncAction();
            releaseOperator();
        }));
        buffer.setSplitsTotal(sliceQueue.totalSlices());
        ProducerState state = new ProducerState(sliceQueue, null, null, buffer, driverContext, rowLimit, formatReader);
        try {
            producerExecutor.execute(ActionRunnable.wrap(completionListener, l -> runProducerLoop(state, l)));
        } catch (Exception e) {
            completionListener.onFailure(e);
        }
    }

    /**
     * Multi-file read path (legacy, non-slice-queue). Per-file filter adaptation is not applied
     * here because this path does not carry {@link FileSplit} with {@link ColumnMapping};
     * UNION_BY_NAME queries use the slice-queue path ({@link #startSliceQueueRead}) instead.
     */
    private void startMultiFileRead(List<String> projectedColumns, AsyncExternalSourceBuffer buffer, DriverContext driverContext) {
        ActionListener<Void> completionListener = ActionListener.assertOnce(ActionListener.wrap(v -> {
            buffer.finish(false);
            driverContext.removeAsyncAction();
            releaseOperator();
        }, e -> {
            buffer.onFailure(e);
            driverContext.removeAsyncAction();
            releaseOperator();
        }));
        buffer.setSplitsTotal(fileList.fileCount());
        ProducerState state = new ProducerState(null, fileList, projectedColumns, buffer, driverContext, rowLimit, formatReader);
        state.schemaInfo = schemaMap;
        try {
            producerExecutor.execute(ActionRunnable.wrap(completionListener, l -> runProducerLoop(state, l)));
        } catch (Exception e) {
            completionListener.onFailure(e);
        }
    }

    /**
     * Producer-loop state. One instance per producer path (slice-queue OR multi-file).
     * Tracks iteration position across splits/leaves/files, the currently active page iterator,
     * and the shared outputs (buffer + DriverContext). Mutated only from the producer executor.
     */
    private static final class ProducerState {
        @Nullable
        final ExternalSliceQueue queue;
        @Nullable
        final FileList fileList;
        @Nullable
        final List<String> projectedColumns;
        final AsyncExternalSourceBuffer buffer;
        final DriverContext driverContext;
        @Nullable
        final FormatReader formatReader;

        int fileIndex;
        @Nullable
        List<ExternalSplit> leaves;
        int leafIndex;
        int rowsRemaining;
        @Nullable
        CloseableIterator<Page> pages;
        @Nullable
        Map<StoragePath, SchemaReconciliation.FileSchemaInfo> schemaInfo;
        @Nullable
        StoragePath lastRangeFilePath;
        @Nullable
        Object lastFileContext;
        @Nullable
        StoragePath lastSchemaPath;
        @Nullable
        List<Attribute> lastBoundSchema;
        // Per-storage-object running tally for bytes_read deltas. Reset whenever a new
        // StorageObject is opened so deltas are attributed to a single object's lifetime.
        @Nullable
        StorageObject currentObject;
        long currentObjectBytesSnapshot;
        // 1-based index of the split / file the producer is currently working on (0 = not started).
        int currentSplitIndex;

        ProducerState(
            @Nullable ExternalSliceQueue queue,
            @Nullable FileList fileList,
            @Nullable List<String> projectedColumns,
            AsyncExternalSourceBuffer buffer,
            DriverContext driverContext,
            int rowsRemaining,
            @Nullable FormatReader formatReader
        ) {
            if ((queue == null) == (fileList == null)) {
                throw new IllegalArgumentException("ProducerState requires exactly one of queue or fileList");
            }
            this.queue = queue;
            this.fileList = fileList;
            this.projectedColumns = projectedColumns;
            this.buffer = buffer;
            this.driverContext = driverContext;
            this.rowsRemaining = rowsRemaining;
            this.formatReader = formatReader;
        }
    }

    private enum DrainResult {
        /** Hit EOF on the current iterator; caller should advance to the next unit. */
        EOF,
        /** Buffer is full; a callback is registered to resume the loop. */
        BLOCKED,
        /** Row limit exhausted or buffer finished; the whole producer is done. */
        DONE
    }

    /**
     * Single-step producer loop, split across two pools so the parser workers and the consumer that drains their
     * pages never contend for the same threads. When no unit is open it opens the next one on the read/parse
     * executor ({@code esql_external_io}) — that phase does the blocking {@code length()}/{@code computeSegments}
     * probes and dispatches the segment parser workers, all of which belong on the I/O pool — then hands off to the
     * (non-blocking) drain on the consumer executor ({@code esql_worker}). When a unit is already open it drains
     * directly on the consumer executor. Keeping the blocking open off the consumer pool and the drain off the
     * parser pool is what breaks the multi-file parallel-parse stall: a full I/O pool of blocked parser workers can
     * no longer starve the drain that must consume their pages.
     */
    private void runProducerLoop(ProducerState state, ActionListener<Void> completionListener) {
        if (state.pages == null) {
            openUnitThenDrain(state, completionListener);
        } else {
            drainCurrentUnit(state, completionListener);
        }
    }

    /**
     * Open phase (runs on the read/parse executor). Advances to the next unit — blocking {@code length()} /
     * {@code computeSegments} probes plus parser-worker dispatch — then resumes the drain on the consumer executor.
     * A {@code false} return from {@link #advanceToNextUnit} means the producer is exhausted (terminal success).
     */
    private void openUnitThenDrain(ProducerState state, ActionListener<Void> completionListener) {
        try {
            executor.execute(ActionRunnable.wrap(completionListener, l -> {
                if (advanceToNextUnit(state) == false) {
                    snapshotBytesRead(state);
                    snapshotFormatReaderStatus(state);
                    l.onResponse(null);
                    return;
                }
                // Unit opened (iterator built, parser workers dispatched on this pool); drain it on the consumer
                // pool so the parser workers cannot starve their own consumer.
                try {
                    producerExecutor.execute(() -> drainCurrentUnit(state, l));
                } catch (Exception e) {
                    clearCurrentIterator(state);
                    l.onFailure(e);
                }
            }));
        } catch (Exception e) {
            clearCurrentIterator(state);
            completionListener.onFailure(e);
        }
    }

    /**
     * Drain phase (runs on the consumer executor). Drains ready pages from the currently-open iterator; the drain
     * is non-blocking and parks via {@link #parkUntilReady} when the buffer is full or the parser workers have not
     * yet produced. On EOF it re-enters {@link #runProducerLoop}, which opens the next unit back on the read/parse
     * executor.
     */
    private void drainCurrentUnit(ProducerState state, ActionListener<Void> completionListener) {
        try {
            DrainResult result = drainHotPath(state, completionListener);
            switch (result) {
                case DONE -> {
                    // Buffer finished (externally or by row-limit exhaustion) while an iterator is still open:
                    // close it before reporting completion so no resources leak on cancellation paths.
                    snapshotBytesRead(state);
                    snapshotFormatReaderStatus(state);
                    clearCurrentIterator(state);
                    completionListener.onResponse(null);
                }
                case EOF -> {
                    // Finished consuming this unit: capture deltas, count the split as processed,
                    // and re-enter to advance to the next unit (openUnitThenDrain re-dispatches to the I/O pool).
                    snapshotBytesRead(state);
                    snapshotFormatReaderStatus(state);
                    state.buffer.incSplitsProcessed();
                    clearCurrentIterator(state);
                    state.currentObject = null;
                    state.currentObjectBytesSnapshot = 0L;
                    runProducerLoop(state, completionListener);
                }
                case BLOCKED -> {
                    // A listener has been registered on waitForSpace that will re-submit the drain.
                    snapshotBytesRead(state);
                    snapshotFormatReaderStatus(state);
                }
            }
        } catch (Exception e) {
            clearCurrentIterator(state);
            completionListener.onFailure(e);
        }
    }

    /**
     * Captures the delta in {@code StorageObject.metrics().bytesRead()} since the last snapshot
     * for the currently-active object and forwards it to the buffer. Safe no-op when no object
     * is active. Best-effort: telemetry must never break the producer lifecycle.
     */
    private static void snapshotBytesRead(ProducerState state) {
        StorageObject obj = state.currentObject;
        if (obj == null) {
            return;
        }
        try {
            StorageObjectMetrics metrics = obj.metrics();
            if (metrics == null) {
                return;
            }
            long current = metrics.bytesRead();
            long delta = current - state.currentObjectBytesSnapshot;
            if (delta > 0) {
                state.buffer.addBytesRead(delta);
                state.currentObjectBytesSnapshot = current;
            }
        } catch (Exception e) {
            // metrics() is opt-in; never let an instrumentation accessor break the producer lifecycle.
            // TRACE so on-call has a breadcrumb if telemetry counters silently flatline.
            logger.trace(() -> "telemetry: bytesRead snapshot failed for " + state.currentObject, e);
        }
    }

    /** Refreshes the buffer's view of the format reader's counter snapshot; best-effort. */
    private static void snapshotFormatReaderStatus(ProducerState state) {
        if (state.formatReader == null) {
            return;
        }
        try {
            state.buffer.recordFormatReaderStatus(state.formatReader.statusSnapshot());
        } catch (Exception e) {
            logger.trace(() -> "telemetry: format-reader statusSnapshot failed for " + state.formatReader, e);
        }
    }

    /**
     * Closes the current page iterator (if any) so the next {@link #advanceToNextUnit} call starts
     * cleanly. Safe to call multiple times.
     */
    private static void clearCurrentIterator(ProducerState state) {
        closeQuietly(state.pages);
        state.pages = null;
    }

    /**
     * Drain pages from the currently-open iterator into the buffer.
     * Runs synchronously while the buffer has space; when full, registers a callback that
     * re-submits {@link #runProducerLoop} via the executor and returns {@link DrainResult#BLOCKED}.
     */
    private DrainResult drainHotPath(ProducerState state, ActionListener<Void> completionListener) {
        CloseableIterator<Page> pages = state.pages;
        AsyncExternalSourceBuffer buffer = state.buffer;
        while (true) {
            if (noFurtherCandidates()) {
                return DrainResult.DONE;
            }
            if (buffer.noMoreInputs()) {
                return DrainResult.DONE;
            }
            if (rowLimit != FormatReader.NO_LIMIT && state.rowsRemaining <= 0) {
                return DrainResult.DONE;
            }
            // Yield on upstream-blocked (e.g. streaming-parallel iterator waiting on parser threads)
            // — symmetric to the downstream-buffer-full yield below. Without this the producer-loop
            // would spin inside {@code hasNext()} holding its executor slot while the iterator's
            // segmenter/parser sub-tasks compete for slots on the same pool: with default
            // {@code parsing_parallelism = cores} and F concurrent file readers we'd need
            // F + F + F×cores slots, exhausting the generic pool on multi-file gzip globs.
            // Synchronous iterators ({@code CloseableIterator}'s default) return an
            // immediately-completed listener and fall straight through.
            SubscribableListener<Void> ready = pages.waitForReady();
            if (ready.isDone() == false) {
                return parkUntilReady(ready, state, completionListener);
            }
            if (pages.hasNext() == false) {
                // Race: waitForReady reported done (page available or EOF), but by the time we
                // called hasNext the state advanced (e.g. another consumer drained, or POISON
                // got handled inside hasNext and the next slot is not yet populated). For
                // synchronous iterators where the default waitForReady returns immediately-done,
                // hasNext=false truly means EOF and the recheck remains done. For async iterators
                // like {@code StreamingParallelIterator}, a non-done recheck means the iterator
                // is still producing — yield and let the parser-side {@code signalReady()} wake us.
                SubscribableListener<Void> recheck = pages.waitForReady();
                if (recheck.isDone()) {
                    return DrainResult.EOF;
                }
                return parkUntilReady(recheck, state, completionListener);
            }
            SubscribableListener<Void> space = buffer.waitForSpace();
            if (space.isDone() == false) {
                return parkUntilReady(space, state, completionListener);
            }
            if (buffer.noMoreInputs()) {
                return DrainResult.DONE;
            }
            Page page = pages.next();
            int rows = page.getPositionCount();
            page.allowPassingToDifferentDriver();
            buffer.addPage(page);
            if (rowLimit != FormatReader.NO_LIMIT) {
                state.rowsRemaining -= rows;
            }
        }
    }

    /**
     * Register a single listener that resumes the producer loop when {@code signal} fires, and
     * return synchronously. {@link DrainResult#BLOCKED} is returned immediately after listener
     * registration; the producer resumes asynchronously when {@code signal} completes.
     * <p>
     * Cleanup semantics by branch:
     * <ul>
     * <li>Success branch (happy path): re-submits {@link #runProducerLoop} on {@code producerExecutor} (the
     *     consumer pool) since a park only ever happens mid-drain (a unit is open), so the resumed loop drains
     *     rather than opens; the current iterator stays open across the park. Only if
     *     {@code producerExecutor.execute()} itself throws (e.g. shutting-down pool) is the iterator cleared and
     *     the failure routed through {@code completionListener.onFailure}.</li>
     * <li>Failure branch: clears the current iterator and routes the signal's failure through
     *     {@code completionListener.onFailure}.</li>
     * </ul>
     * Both cleanup paths match what the surrounding {@link #runProducerLoop} {@code catch} block
     * does on a synchronous throw.
     * <p>
     * Collapses three structurally identical {@code addListener} blocks (page-ready, page-ready
     * recheck, buffer-space) into one site, removing copy-paste drift risk between branches.
     *
     * @param signal a not-done listener from {@code waitForReady()} or {@code waitForSpace()};
     *               callers must verify {@code signal.isDone() == false} before invoking this
     *               helper, otherwise the producer will be re-submitted immediately.
     */
    private DrainResult parkUntilReady(SubscribableListener<Void> signal, ProducerState state, ActionListener<Void> completionListener) {
        signal.addListener(ActionListener.wrap(v -> {
            try {
                producerExecutor.execute(() -> runProducerLoop(state, completionListener));
            } catch (Exception e) {
                clearCurrentIterator(state);
                completionListener.onFailure(e);
            }
        }, e -> {
            clearCurrentIterator(state);
            completionListener.onFailure(e);
        }));
        return DrainResult.BLOCKED;
    }

    /**
     * Advance the iteration position to the next unit (slice-queue leaf or multi-file file) and
     * open a fresh page iterator for it. Returns {@code false} if iteration is exhausted or the
     * buffer has been finished externally.
     */
    private boolean advanceToNextUnit(ProducerState state) throws IOException {
        while (true) {
            if (noFurtherCandidates()) {
                return false;
            }
            if (state.buffer.noMoreInputs()) {
                return false;
            }
            if (rowLimit != FormatReader.NO_LIMIT && state.rowsRemaining <= 0) {
                return false;
            }
            if (state.queue != null) {
                if (openNextSliceQueueLeaf(state)) {
                    return true;
                }
                // queue is exhausted
                if (state.leaves == null) {
                    return false;
                }
                // current split's leaves exhausted; fall through to pull the next split
                state.leaves = null;
                state.leafIndex = 0;
            } else {
                if (openNextMultiFile(state)) {
                    return true;
                }
                return false;
            }
        }
    }

    /**
     * Open the next leaf iterator in the slice-queue path. Pulls a new split from the queue when
     * the current split's leaves are exhausted. Returns {@code false} if the queue is exhausted.
     * <p>
     * When {@link #batchReadCapable} is set, delegates to {@link #openNextBatch} to claim and
     * process multiple splits at once via {@link RangeAwareFormatReader#readAll}.
     */
    private boolean openNextSliceQueueLeaf(ProducerState state) throws IOException {
        if (noFurtherCandidates()) {
            return false;
        }
        if (batchReadCapable && state.leaves == null) {
            return openNextBatch(state);
        }
        if (state.leaves == null || state.leafIndex >= state.leaves.size()) {
            ExternalSplit split = state.queue.nextSplit();
            if (split == null) {
                return false;
            }
            state.leaves = flattenToLeaves(split);
            state.leafIndex = 0;
            state.currentSplitIndex++;
            state.buffer.setCurrentSplit(state.currentSplitIndex);
        }
        ExternalSplit leaf = state.leaves.get(state.leafIndex++);
        if (leaf instanceof FileSplit == false) {
            throw new IllegalArgumentException("Unsupported split type: " + leaf.getClass().getName());
        }
        FileSplit fileSplit = (FileSplit) leaf;
        List<String> cols = dataProjectedColumns();
        // Per-file view of the projection, used identically by the reader call (range or non-range
        // branch) and by the adapter below. Sourced from the FileSplit: readSchema() is this file's
        // physical schema the coordinator inferred (null when no pin is set — single-file / legacy —
        // in which case the reader falls back to per-file inference). Under UBN the query projection
        // may include columns absent from this file; perFileQueryProjection narrows to the columns
        // actually present, and the adapter (SchemaAdaptingIterator) null-fills the rest.
        List<Attribute> perFileReadSchema = fileSplit.readSchema();
        List<String> perFileCols = perFileQueryProjection(cols, perFileReadSchema);

        CloseableIterator<Page> pages = null;
        try {
            FormatReader fileReader = readerForFile(fileSplit);
            boolean isRangeSplit = "true".equals(fileSplit.config().get(FileSplitProvider.RANGE_SPLIT_KEY));
            if (isRangeSplit && fileReader instanceof RangeAwareFormatReader rangeReader) {
                String fileLengthStr = (String) fileSplit.config().get(FileSplitProvider.FILE_LENGTH_KEY);
                StorageObject fullObj = fileLengthStr != null
                    ? storageProvider.newObject(fileSplit.path(), Long.parseLong(fileLengthStr))
                    : storageProvider.newObject(fileSplit.path());
                attachStorageMetrics(fullObj); // before any read — see note at the single-object dispatch above
                long rangeEnd = fileSplit.offset() + fileSplit.length();
                Object fileContext = fileSplit.path().equals(state.lastRangeFilePath) ? state.lastFileContext : null;
                // Pin the reader to this file's physical projection/schema — mirroring the non-range
                // branch below. The per-file ColumnMapping applied by adaptSchema is built against the
                // file's physical column order and types, so the reader must deliver its page in that
                // same shape. Feeding the query-unified projection/attributes here instead makes the
                // reader emit columns in unified order and at unified (widened) types, and ColumnMapping
                // then re-permutes/re-casts an already-adapted page — silently swapping columns whose
                // per-file order differs from the query, or failing with an "Unsupported block cast"
                // when a per-file type was widened. Fall back to the query-unified attributes only when
                // no per-file pin is present (single-file / legacy). readSchema() is already free of the
                // _rowPosition synthetic (that channel is optimizer-injected later), so it can serve as
                // the RangeReadContext resolved attributes directly.
                List<Attribute> perFileResolvedAttributes = perFileReadSchema != null && perFileReadSchema.isEmpty() == false
                    ? perFileReadSchema
                    : readerResolvedAttributes;
                RangeReadContext rangeCtx = new RangeReadContext(
                    // main pins the reader to the PER-FILE projection/attributes (ColumnMapping owns position/cast);
                    // our rename applies the last-mile logical->physical translation on top. Both are reader-facing, so
                    // translate main's per-file values; the adapter below keeps them untranslated (reconciliation stays
                    // logical). readSchema()/perFile* are in logical names — see rename-reconcile-merge-plan.
                    PhysicalNames.translateNames(perFileCols, renames),
                    batchSize,
                    fileSplit.offset(),
                    rangeEnd,
                    PhysicalNames.translateSchema(perFileResolvedAttributes, renames),
                    errorPolicy
                );
                if (fileContext != null) {
                    rangeCtx.setFileContext(fileContext);
                }
                pages = rangeReader.readRange(fullObj, rangeCtx);
                state.lastRangeFilePath = fileSplit.path();
                state.lastFileContext = rangeCtx.fileContext();
                state.currentObject = fullObj;
                state.currentObjectBytesSnapshot = readBytesOrZero(fullObj);
            } else {
                StorageObject obj = FileSplitProvider.storageObjectForSplit(storageProvider, fileSplit);
                attachStorageMetrics(obj); // before any read — see note at the single-object dispatch above
                boolean recordAlignedMacro = FileSplitProvider.isRecordAlignedMacroSplit(fileSplit);
                boolean firstSplit = fileSplit.offset() == 0 || "true".equals(fileSplit.config().get(FileSplitProvider.FIRST_SPLIT_KEY));
                if (cols.isEmpty() && recordAlignedMacro && firstSplit == false) {
                    // COUNT(*)/empty-projection path on a non-leading record-aligned macro-split:
                    // bind schema from the full file (header-bearing formats like CSV need file-leading bytes).
                    // Cache per file path to avoid redundant metadata fetches across splits of the same file.
                    List<Attribute> cachedSchema = fileSplit.path().equals(state.lastSchemaPath) ? state.lastBoundSchema : null;
                    if (cachedSchema == null) {
                        SourceMetadata meta = fileReader.metadata(storageProvider.newObject(fileSplit.path()));
                        if (meta != null && meta.schema() != null && meta.schema().isEmpty() == false) {
                            cachedSchema = meta.schema();
                        }
                    }
                    if (cachedSchema != null) {
                        fileReader = fileReader.withSchema(cachedSchema);
                        state.lastSchemaPath = fileSplit.path();
                        state.lastBoundSchema = cachedSchema;
                    }
                }
                // Compressed-offset splits (bzip2 block-aligned / zstd-indexed): splitStartByte is a
                // COMPRESSED position while text readers anchor _rowPosition in decompressed bytes —
                // composing _id from that mix yields non-split-invariant, collision-prone tokens. Take
                // the slot out of the reader's projection and null-splice it instead: null _id over
                // these layouts, same honest carve-out parquet-rs gets. Only the reader's column list is
                // narrowed (readerCols); the shared perFileCols still feeds the adapter below at full
                // width, matching the pre-hoist behaviour where the adapter recomputed the projection.
                boolean compressedOffsetSplit = "true".equals(fileSplit.config().get(FileSplitProvider.COMPRESSED_OFFSET_SPLIT_KEY));
                int compressedRowPosSlot = compressedOffsetSplit ? SyntheticColumns.rowPositionIndexInNames(perFileCols) : -1;
                List<String> readerCols = perFileCols;
                if (compressedRowPosSlot >= 0) {
                    List<String> withoutRowPosition = new ArrayList<>(perFileCols);
                    withoutRowPosition.remove(compressedRowPosSlot);
                    readerCols = withoutRowPosition;
                }
                // A record-aligned macro-split that owns the file's trailing bytes is file-final; a genuine
                // whole-file read (offset 0, not record-aligned, last split) is also file-final. Either way the
                // last split's trailing segment may close its last stripe to EOF.
                boolean splitIsFileFinal = "true".equals(fileSplit.config().get(FileSplitProvider.LAST_SPLIT_KEY))
                    || (recordAlignedMacro == false && firstSplit && fileSplit.offset() == 0);
                pages = openWithParallelism(
                    fileReader,
                    obj,
                    PhysicalNames.translateNames(readerCols, renames),
                    errorPolicy,
                    recordAlignedMacro,
                    firstSplit,
                    splitIsFileFinal,
                    // Reader + stats harvester key off this schema; hand them the PHYSICAL (translated) names so harvest
                    // keys stay physical — the declared-overlay stats boundary rekeys physical->logical, not the factory.
                    PhysicalNames.translateSchema(perFileReadSchema, renames),
                    fileSplit.offset(),
                    state.buffer.capturedSourceMetadataSink(),
                    state.buffer::recordWarning
                );
                if (pages == null) {
                    boolean lastSplit = "true".equals(fileSplit.config().get(FileSplitProvider.LAST_SPLIT_KEY));
                    FormatReadContext ctx = FormatReadContext.builder()
                        .projectedColumns(PhysicalNames.translateNames(readerCols, renames))
                        .batchSize(batchSize)
                        .rowLimit(FormatReader.NO_LIMIT)
                        .errorPolicy(errorPolicy)
                        .firstSplit(firstSplit)
                        .lastSplit(lastSplit)
                        .recordAligned(recordAlignedMacro)
                        .readSchema(PhysicalNames.translateSchema(perFileReadSchema, renames))
                        .splitStartByte(fileSplit.offset())
                        .maxRecordBytes(maxRecordBytes)
                        // Per-stripe stats addressing for record-aligned macro-splits (a large file read at
                        // parsing_parallelism<=1 is still cut into newline-aligned macro-splits, each a
                        // recordAligned chunk). The readers gate stripe harvest on chunkMode == recordAligned,
                        // so a genuine whole-file read (recordAligned=false) ignores statsStripeSize and keeps
                        // the authoritative whole-file emit; a macro-split harvests per-stripe and the
                        // coordinator's byte-range cover folds the chunks. statsBaseOffset is the split's
                        // file-global offset; statsFileFinal marks the last split (the only one that may close
                        // its trailing stripe to EOF).
                        .stats(fileSplit.offset(), statsStripeSize, splitIsFileFinal)
                        .statsColumnScope(statsColumnScope)
                        .build();
                    pages = fileReader.read(obj, ctx);
                }
                if (compressedRowPosSlot >= 0) {
                    pages = new NullSpliceRowPositionStrategy(
                        producerBlockFactory(state.driverContext),
                        "compressed-offset split has no decompressed _rowPosition anchor"
                    ).apply(pages, compressedRowPosSlot);
                } else {
                    pages = applyRowPositionStrategy(fileReader, pages, readerCols);
                }
                state.currentObject = obj;
                state.currentObjectBytesSnapshot = readBytesOrZero(obj);
                pages = StatsCapturingIterator.wrap(pages, state.buffer.capturedSourceMetadataSink());
            }
            // The adapter uses the same per-file schema and projected column order pinned above so it
            // can disambiguate LongBlock sources when stringifying under UBN; sharing the one source of
            // truth keeps the cast's source-type view consistent with what the reader was told to emit.
            CloseableIterator<Page> adapted = adaptSchema(
                pages,
                fileSplit.columnMapping(),
                state.driverContext,
                perFileReadSchema,
                perFileCols
            );
            // Deferred extraction: register one extractor per opened file split. Range-splits of
            // the same file therefore register multiple extractors; this is benign — each row's
            // encoded id maps back to the registered extractor that produced it, addressing the
            // split's owned row groups in extractor-local coordinates (see {@link ColumnExtractor}).
            CloseableIterator<Page> withEncoder = wrapWithEncoderIfNeeded(adapted, cols, state.driverContext);
            // Per-split virtual-column iterator: each slice-queue leaf has its own _file.* values
            // (different path/name/dir/size/mtime), so the wrapper is bound to *this* iterator's pages.
            state.pages = wrapWithVirtualColumns(
                withEncoder,
                mergeStandardMetadata(fileSplit.partitionValues()),
                state.driverContext,
                fileSplit.path()
            );
            return true;
        } catch (Exception e) {
            closeQuietly(pages);
            if (e instanceof IOException io) throw io;
            if (e instanceof RuntimeException re) throw re;
            throw new IOException(e);
        }
    }

    /**
     * Batch-read path: claims {@code max(1, ceil(remaining / (parallelism * 2)))} splits from the
     * queue at once and opens a single {@link RangeAwareFormatReader#readAll} iterator over all of
     * them. This allows the reader (e.g. parquet-rs) to process the files concurrently in a single
     * async call rather than paying one sequential S3 round-trip per file.
     * <p>
     * Only called when {@link #batchReadCapable} is {@code true}, which requires no partition-column
     * injection (incompatible with a unified batch iterator).
     */
    private boolean openNextBatch(ProducerState state) throws IOException {
        if (noFurtherCandidates()) {
            return false;
        }
        int remaining = state.queue.remaining();
        if (remaining == 0) {
            return false;
        }
        int claimSize = Math.max(1, Math.ceilDiv(remaining, parallelism * 2));
        List<ExternalSplit> claims = state.queue.nextSplits(claimSize);
        if (claims.isEmpty()) {
            return false;
        }

        List<RangeAwareFormatReader.SplitRef> splitRefs = new ArrayList<>(claims.size());
        for (ExternalSplit claim : claims) {
            for (ExternalSplit leaf : flattenToLeaves(claim)) {
                if (leaf instanceof FileSplit fs) {
                    String fileLengthStr = (String) fs.config().get(FileSplitProvider.FILE_LENGTH_KEY);
                    StorageObject obj = fileLengthStr != null
                        ? storageProvider.newObject(fs.path(), Long.parseLong(fileLengthStr))
                        : storageProvider.newObject(fs.path());
                    // Batch path reads several objects together — attach each before readAll() opens them.
                    attachStorageMetrics(obj);
                    splitRefs.add(new RangeAwareFormatReader.SplitRef(obj, fs.offset(), fs.length()));
                }
            }
        }

        if (splitRefs.isEmpty()) {
            return false;
        }

        // Batch-read path is gated on partitionColumnNames.isEmpty() in {@link #batchReadCapable},
        // so dataProjectedColumns() returns the full attribute list and no virtual-column wrapping
        // is needed.
        List<String> cols = dataProjectedColumns();
        RangeAwareFormatReader rangeReader = (RangeAwareFormatReader) readerWithDynamicThreshold(formatReader);
        CloseableIterator<Page> pages = null;
        try {
            pages = rangeReader.readAll(splitRefs, cols, batchSize);
            pages = applyRowPositionStrategy(rangeReader, pages, cols);
            state.pages = pages;
            return true;
        } catch (Exception e) {
            closeQuietly(pages);
            if (e instanceof IOException io) throw io;
            if (e instanceof RuntimeException re) throw re;
            throw new IOException(e);
        }
    }

    /**
     * Open the next file iterator in the multi-file path. Returns {@code false} if all files
     * have been processed.
     */
    private boolean openNextMultiFile(ProducerState state) throws IOException {
        if (noFurtherCandidates()) {
            return false;
        }
        FileList files = state.fileList;
        assert files != null;
        if (state.fileIndex >= files.fileCount()) {
            return false;
        }
        int fileIndex = state.fileIndex++;
        state.currentSplitIndex = fileIndex + 1;
        state.buffer.setCurrentSplit(state.currentSplitIndex);
        List<String> cols = state.projectedColumns;

        // Per-file partition values so _file.path/name/directory/size/modified reflect *this*
        // file rather than the factory's pre-resolution values. Merge precedence (base -> top):
        // Hive partition values, then standard ES metadata constants (dedicated names win on a
        // smuggled-key collision — same order mergeStandardMetadata applies on the slice-queue
        // and single-file paths), then _file.* values (disjoint namespace, must reflect this file).
        Map<String, Object> perFileValues = partitionValues;
        if (partitionColumnNames.isEmpty() == false) {
            perFileValues = new HashMap<>(partitionValues);
            if (standardMetadataPerFileNames.isEmpty() == false) {
                perFileValues.putAll(ExternalMetadataColumns.extractPerFileConstants(datasetName, files, fileIndex));
            }
            perFileValues.putAll(FileMetadataColumns.extractValues(files, fileIndex));
        }

        CloseableIterator<Page> pages = null;
        try {
            StorageObject obj = storageProvider.newObject(files.path(fileIndex));
            attachStorageMetrics(obj); // before any read — see note at the single-object dispatch above
            FormatReader fileReader = readerWithDynamicThreshold(formatReader);
            // Pull this file's coordinator-inferred schema from schemaInfo when available, so the
            // reader is pinned to the same inference the per-file ColumnMapping was built against.
            ColumnMapping mapping = null;
            List<Attribute> perFileReadSchema = null;
            if (state.schemaInfo != null) {
                SchemaReconciliation.FileSchemaInfo info = state.schemaInfo.get(files.path(fileIndex));
                if (info != null) {
                    mapping = info.mapping();
                    perFileReadSchema = info.fileSchema().attributes();
                }
            }
            List<String> perFileCols = perFileQueryProjection(cols, perFileReadSchema);
            pages = openWithParallelism(
                fileReader,
                obj,
                PhysicalNames.translateNames(perFileCols, renames),
                errorPolicy,
                false,
                true,
                // Whole-file read of one file in a multi-file list: the file is its own final split, so its
                // trailing segment reaches EOF and may close the last stripe.
                true,
                // Physical (translated) names so harvest keys stay physical; see the split path above.
                PhysicalNames.translateSchema(perFileReadSchema, renames),
                0L,
                state.buffer.capturedSourceMetadataSink(),
                state.buffer::recordWarning
            );
            if (pages == null) {
                int fileBudget = rowLimit == FormatReader.NO_LIMIT ? FormatReader.NO_LIMIT : state.rowsRemaining;
                FormatReadContext ctx = FormatReadContext.builder()
                    .projectedColumns(PhysicalNames.translateNames(perFileCols, renames))
                    .batchSize(batchSize)
                    .rowLimit(fileBudget)
                    .errorPolicy(errorPolicy)
                    .readSchema(PhysicalNames.translateSchema(perFileReadSchema, renames))
                    .maxRecordBytes(maxRecordBytes)
                    .statsColumnScope(statsColumnScope)
                    .build();
                pages = fileReader.read(obj, ctx);
            }
            pages = applyRowPositionStrategy(fileReader, pages, perFileCols);
            pages = StatsCapturingIterator.wrap(pages, state.buffer.capturedSourceMetadataSink());
            CloseableIterator<Page> adapted = adaptSchema(pages, mapping, state.driverContext, perFileReadSchema, perFileCols);
            CloseableIterator<Page> withEncoder = wrapWithEncoderIfNeeded(adapted, perFileCols, state.driverContext);
            // Per-file virtual-column iterator (built with FileMetadataColumns.extractValues for
            // this file) so {@code _file.*} columns carry the right values for the current file.
            state.pages = wrapWithVirtualColumns(withEncoder, perFileValues, state.driverContext, files.path(fileIndex));
            state.currentObject = obj;
            state.currentObjectBytesSnapshot = readBytesOrZero(obj);
            return true;
        } catch (Exception e) {
            closeQuietly(pages);
            if (e instanceof IOException io) throw io;
            if (e instanceof RuntimeException re) throw re;
            throw new IOException(e);
        }
    }

    /**
     * Publishes the just-opened object's read/retry events to the node {@link ExternalSourceMetrics}. The
     * sink stays attached to the object's counters for the rest of its life. Best-effort: an
     * instrumentation failure must never break the producer loop. The raw storage scheme is stored on the sink;
     * {@link ExternalSourceMetrics} folds it to one canonical token (s3/gcs/azure/http/file) on lookup, so the
     * scheme is the only, low-cardinality, dimension. This guard wraps the non-record {@code attachMetrics} /
     * {@code path()} calls, so it stays (the record methods self-guard separately).
     * <p>
     * <b>Attach ordering vs the scope invariant:</b> the sink is attached before the first READ so read requests are
     * counted, but attach ordering relative to metadata probes is NOT what keeps the read-scoped {@code storage.*}
     * registry metrics read-only. Some metadata probes actually run AFTER attach (the COUNT(*) macro-split
     * {@code fileReader.metadata()} and {@code ParallelParsingCoordinator}'s opening {@code storageObject.length()}),
     * so the real guard is profile-only routing of metadata ops, not attach ordering: on a retryable provider
     * {@code RetryableStorageObject} runs {@code length}/{@code lastModified}/{@code exists} with
     * {@code RetryPolicy.RetryTelemetry.NONE} and routes their retries through the profile-only counter, so a metadata
     * retry/error/stall never feeds the registry sink and cannot leak past {@code storage.requests.total}
     * (retries/errors/read_stall &le; requests) regardless of when the probe fires relative to this attach.
     */
    private void attachStorageMetrics(StorageObject obj) {
        try {
            obj.attachMetrics(externalSourceMetrics, obj.path().scheme());
        } catch (Exception e) {
            logger.trace(() -> "telemetry: attachMetrics failed for " + obj, e);
        }
    }

    /** Best-effort read of {@code obj.metrics().bytesRead()}; returns 0 on null/throw so test mocks don't break the producer. */
    private static long readBytesOrZero(StorageObject obj) {
        try {
            StorageObjectMetrics m = obj.metrics();
            return m == null ? 0L : m.bytesRead();
        } catch (Exception e) {
            logger.trace(() -> "telemetry: bytesRead baseline read failed for " + obj, e);
            return 0L;
        }
    }

    private void startNativeAsyncRead(
        StorageObject storageObject,
        List<String> projectedColumns,
        AsyncExternalSourceBuffer buffer,
        DriverContext driverContext
    ) {
        if (noFurtherCandidates()) {
            buffer.finish(false);
            driverContext.removeAsyncAction();
            releaseOperator();
            return;
        }
        buffer.setSplitsTotal(1);
        buffer.setCurrentSplit(1);
        FormatReadContext ctx = FormatReadContext.builder()
            .projectedColumns(PhysicalNames.translateNames(projectedColumns, renames))
            .batchSize(batchSize)
            .rowLimit(rowLimit)
            .errorPolicy(errorPolicy)
            .maxRecordBytes(maxRecordBytes)
            .statsColumnScope(statsColumnScope)
            .build();
        FormatReader reader = readerWithDynamicThreshold(formatReader);
        reader.readAsync(storageObject, ctx, executor, ActionListener.wrap(iterator -> {
            CloseableIterator<Page> wrapped = applyRowPositionStrategy(reader, iterator, projectedColumns);
            consumePagesInBackground(wrapped, buffer, driverContext, storageObject, projectedColumns);
        }, e -> {
            buffer.onFailure(e);
            driverContext.removeAsyncAction();
            releaseOperator();
        }));
    }

    private void startSyncWrapperRead(
        StorageObject storageObject,
        List<String> projectedColumns,
        AsyncExternalSourceBuffer buffer,
        DriverContext driverContext
    ) {
        if (noFurtherCandidates()) {
            buffer.finish(false);
            driverContext.removeAsyncAction();
            releaseOperator();
            return;
        }
        buffer.setSplitsTotal(1);
        buffer.setCurrentSplit(1);
        ActionListener<Void> failureListener = failureListener(buffer, driverContext);
        executor.execute(ActionRunnable.run(failureListener, () -> {
            FormatReader reader = readerWithDynamicThreshold(formatReader);
            CloseableIterator<Page> pages = openWithParallelism(
                reader,
                storageObject,
                PhysicalNames.translateNames(projectedColumns, renames),
                errorPolicy,
                false,
                true,
                // Single whole-file read: the file is its own final split, so its trailing segment reaches EOF.
                true,
                null,
                0L,
                buffer.capturedSourceMetadataSink(),
                buffer::recordWarning
            );
            if (pages == null) {
                FormatReadContext ctx = FormatReadContext.builder()
                    .projectedColumns(PhysicalNames.translateNames(projectedColumns, renames))
                    .batchSize(batchSize)
                    .rowLimit(rowLimit)
                    .errorPolicy(errorPolicy)
                    .maxRecordBytes(maxRecordBytes)
                    .statsColumnScope(statsColumnScope)
                    .build();
                pages = reader.read(storageObject, ctx);
            }
            pages = applyRowPositionStrategy(reader, pages, projectedColumns);
            pages = StatsCapturingIterator.wrap(pages, buffer.capturedSourceMetadataSink());
            // Wrap with the deferred-extraction encoder (no-op when not enabled), then with the
            // virtual-column iterator so {@code _file.*} columns flow through the producer pipeline
            // alongside the data columns.
            final CloseableIterator<Page> finalPages;
            try {
                CloseableIterator<Page> withEncoder = wrapWithEncoderIfNeeded(pages, projectedColumns, driverContext);
                finalPages = wrapWithVirtualColumns(withEncoder, mergeStandardMetadata(partitionValues), driverContext);
            } catch (Exception e) {
                closeQuietly(pages);
                throw e;
            }
            drainPagesAsync(
                finalPages,
                buffer,
                // Cold-path drain resumes on the consumer pool (esql_worker), not the read/parse pool, so the
                // drain never competes with parser workers for I/O threads. See runProducerLoop's split.
                producerExecutor,
                // Close the iterator chain and record telemetry BEFORE notifying the buffer:
                // closing publishes the finalize marker into the capture sink (via
                // StatsCapturingIterator and the parallel coordinators' finalize hook), and
                // recordSingleFileTelemetry writes the splits_processed / bytes_read /
                // format-reader status counters that the operator status snapshot reads. The
                // Driver may snapshot status() synchronously the moment buffer.finish(false)
                // flips isFinished() to true; notifying the buffer first opens a window where
                // the snapshot lacks the marker (reconciler discards the partial contributions)
                // and the telemetry counters (profile shows zeros for an operator that did
                // real work). Telemetry runs on both success and failure to match the previous
                // runAfter semantics.
                ActionListener.runAfter(ActionListener.wrap(v -> {
                    closeQuietly(finalPages);
                    recordSingleFileTelemetry(storageObject, buffer);
                    buffer.finish(false);
                }, e -> {
                    closeQuietly(finalPages);
                    recordSingleFileTelemetry(storageObject, buffer);
                    buffer.onFailure(e);
                }), () -> {
                    driverContext.removeAsyncAction();
                    releaseOperator();
                })
            );
        }));
    }

    private void consumePagesInBackground(
        CloseableIterator<Page> pages,
        AsyncExternalSourceBuffer buffer,
        DriverContext driverContext,
        StorageObject storageObject,
        List<String> projectedColumns
    ) {
        final CloseableIterator<Page> capturing = StatsCapturingIterator.wrap(pages, buffer.capturedSourceMetadataSink());
        ActionListener<Void> failureListener = ActionListener.wrap(v -> {}, e -> {
            closeQuietly(capturing);
            buffer.onFailure(e);
            driverContext.removeAsyncAction();
            releaseOperator();
        });
        executor.execute(ActionRunnable.run(failureListener, () -> {
            CloseableIterator<Page> withEncoder = wrapWithEncoderIfNeeded(capturing, projectedColumns, driverContext);
            CloseableIterator<Page> wrapped = wrapWithVirtualColumns(withEncoder, mergeStandardMetadata(partitionValues), driverContext);
            drainPagesAsync(
                wrapped,
                buffer,
                // Cold-path drain resumes on the consumer pool (esql_worker); see startSyncWrapperRead / runProducerLoop.
                producerExecutor,
                // See startSyncWrapperRead: close the iterator chain and record telemetry
                // before notifying the buffer so the finalize marker and the telemetry
                // counters reach the operator status snapshot before isFinished() flips.
                ActionListener.runAfter(ActionListener.wrap(v -> {
                    closeQuietly(wrapped);
                    recordSingleFileTelemetry(storageObject, buffer);
                    buffer.finish(false);
                }, e -> {
                    closeQuietly(wrapped);
                    recordSingleFileTelemetry(storageObject, buffer);
                    buffer.onFailure(e);
                }), () -> {
                    driverContext.removeAsyncAction();
                    releaseOperator();
                })
            );
        }));
    }

    /**
     * Records final telemetry for the single-file producer paths
     * ({@link #startNativeAsyncRead}, {@link #startSyncWrapperRead}): increments
     * splits_processed, captures the storage object's cumulative bytes_read, and
     * forwards the latest format-reader counter snapshot. Best-effort: any
     * accessor that misbehaves (e.g. returns {@code null} from a test mock) is
     * tolerated so telemetry can never short-circuit the lifecycle callbacks.
     */
    private void recordSingleFileTelemetry(StorageObject storageObject, AsyncExternalSourceBuffer buffer) {
        buffer.incSplitsProcessed();
        try {
            if (storageObject != null) {
                StorageObjectMetrics metrics = storageObject.metrics();
                if (metrics != null) {
                    long bytes = metrics.bytesRead();
                    if (bytes > 0) {
                        buffer.addBytesRead(bytes);
                    }
                }
            }
        } catch (Exception e) {
            logger.trace(() -> "telemetry: bytesRead snapshot failed for " + storageObject, e);
        }
        try {
            buffer.recordFormatReaderStatus(formatReader.statusSnapshot());
        } catch (Exception e) {
            logger.trace(() -> "telemetry: format-reader statusSnapshot failed for " + formatReader, e);
        }
    }

    private static List<ExternalSplit> flattenToLeaves(ExternalSplit split) {
        if (split instanceof CoalescedSplit coalesced == false) {
            return List.of(split);
        }
        List<ExternalSplit> leaves = new ArrayList<>();
        ArrayDeque<ExternalSplit> stack = new ArrayDeque<>();
        stack.push(split);
        while (stack.isEmpty() == false) {
            ExternalSplit current = stack.pop();
            if (current instanceof CoalescedSplit nested) {
                List<ExternalSplit> children = nested.children();
                for (int i = children.size() - 1; i >= 0; i--) {
                    stack.push(children.get(i));
                }
            } else {
                leaves.add(current);
            }
        }
        return leaves;
    }

    /**
     * Failure-only listener used by non-iterative paths ({@link #startSyncWrapperRead},
     * {@link #consumePagesInBackground}) where {@code removeAsyncAction()} lives in the
     * drain's {@code runAfter} callback. Do NOT use for the iterative slice-queue or
     * multi-file paths — those use a single {@code completionListener} instead.
     */
    private static ActionListener<Void> failureListener(AsyncExternalSourceBuffer buffer, DriverContext driverContext) {
        return ActionListener.wrap(v -> {}, e -> {
            buffer.onFailure(e);
            driverContext.removeAsyncAction();
        });
    }

    private void releaseOperator() {
        if (operatorRefCount.decrementAndGet() == 0) {
            closeDynamicThreshold();
            if (onClose != null) {
                if (deferredExtraction) {
                    // Don't close onClose here — the deferred extraction path keeps using the storage
                    // (and thus the per-query budget) after the last source operator finishes
                    // producing. Release the factory's ref instead; the budget closes once every
                    // SourceExtractors registry has also been closed.
                    releaseDeferredCloseRef();
                } else {
                    closeQuietly(onClose);
                }
            }
        }
    }

    private static void closeQuietly(Closeable closeable) {
        if (closeable != null) {
            IOUtils.closeWhileHandlingException(closeable);
        }
    }

    private static void closeQuietly(CloseableIterator<?> iterator) {
        if (iterator != null) {
            IOUtils.closeWhileHandlingException(iterator);
        }
    }

    /**
     * Resolves the effective inner reader and codec from a possibly-wrapped format reader.
     * Used by dispatch sites to determine whether streaming parallel parsing is applicable.
     */
    static SegmentableFormatReader resolveSegmentableReader(FormatReader reader) {
        if (reader instanceof SegmentableFormatReader seg) {
            return seg;
        }
        if (reader instanceof CompressionDelegatingFormatReader cdr && cdr.unwrap() instanceof SegmentableFormatReader seg) {
            return seg;
        }
        return null;
    }

    /**
     * Single source of truth for how a {@link FormatReader} should be dispatched against parsing parallelism.
     * Both {@link #openWithParallelism} and {@link #describe} resolve the same enum so the runtime path
     * and the diagnostic string cannot drift.
     */
    enum ParallelDispatchMode {
        /** Reader is a {@link SegmentableFormatReader} over an uncompressed input. */
        SEGMENTABLE_UNCOMPRESSED,
        /** Reader wraps a stream-only codec (gzip, zstd); use the streaming-parallel coordinator. */
        STREAM_ONLY_COMPRESSED,
        /** Reader wraps a splittable / indexed codec (bzip2); falls back to single-threaded reads for now. */
        SPLITTABLE_OR_INDEXED_COMPRESSED,
        /** Reader is not segmentable; parallel parsing does not apply. */
        NOT_PARALLELIZABLE
    }

    static ParallelDispatchMode resolveDispatchMode(FormatReader reader) {
        SegmentableFormatReader seg = resolveSegmentableReader(reader);
        if (seg == null) {
            return ParallelDispatchMode.NOT_PARALLELIZABLE;
        }
        if (reader instanceof CompressionDelegatingFormatReader cdr) {
            DecompressionCodec codec = cdr.codec();
            if (codec instanceof SplittableDecompressionCodec || codec instanceof IndexedDecompressionCodec) {
                return ParallelDispatchMode.SPLITTABLE_OR_INDEXED_COMPRESSED;
            }
            return ParallelDispatchMode.STREAM_ONLY_COMPRESSED;
        }
        return ParallelDispatchMode.SEGMENTABLE_UNCOMPRESSED;
    }

    CloseableIterator<Page> openWithParallelism(
        FormatReader reader,
        StorageObject obj,
        List<String> cols,
        ErrorPolicy policy,
        boolean recordAlignedMacroSplit,
        boolean splitIncludesFileLeader,
        boolean splitIsFileFinal,
        @Nullable List<Attribute> perFileReadSchema,
        long baseFileOffset,
        @Nullable ConcurrentMap<String, List<Map<String, Object>>> captureSink,
        @Nullable Consumer<String> partialResultsWarningSink
    ) throws IOException {
        if (rowLimit != FormatReader.NO_LIMIT || parsingParallelism <= 1) {
            return null;
        }
        ParallelDispatchMode mode = resolveDispatchMode(reader);
        switch (mode) {
            case NOT_PARALLELIZABLE -> {
                return null;
            }
            case SEGMENTABLE_UNCOMPRESSED -> {
                SegmentableFormatReader seg = resolveSegmentableReader(reader);
                return ParallelParsingCoordinator.parallelRead(
                    seg,
                    obj,
                    cols,
                    batchSize,
                    parsingParallelism,
                    executor,
                    policy,
                    recordAlignedMacroSplit,
                    splitIncludesFileLeader,
                    perFileReadSchema,
                    baseFileOffset,
                    maxConcurrentOpenSegments,
                    captureSink,
                    maxRecordBytes,
                    statsStripeSize,
                    statsColumnScope,
                    splitIsFileFinal,
                    externalSourceMetrics
                );
            }
            case STREAM_ONLY_COMPRESSED -> {
                // No open-segment cap here, unlike SEGMENTABLE_UNCOMPRESSED: a compressed file is read as a
                // single serial decompressing stream in bounded (~1 MiB) chunks, so it has natural
                // back-pressure and never fans out into many concurrent per-segment streams/buffers.
                CompressionDelegatingFormatReader cdr = (CompressionDelegatingFormatReader) reader;
                SegmentableFormatReader seg = resolveSegmentableReader(reader);
                DecompressionCodec codec = cdr.codec();
                InputStream raw = obj.newStream();
                InputStream decompressed;
                try {
                    decompressed = codec.decompress(raw);
                } catch (Exception e) {
                    try {
                        obj.abortStream(raw);
                    } catch (IOException abortEx) {
                        e.addSuppressed(abortEx);
                    }
                    throw e;
                }
                try {
                    // The stream-only codecs reachable here (gzip via GZIPInputStream, zstd via
                    // ZstdInputStream) follow the JDK FilterInputStream convention: closing the
                    // wrapper closes the underlying `raw`. New stream-only codecs added to this
                    // path must preserve that contract.
                    return StreamingParallelParsingCoordinator.parallelRead(
                        seg,
                        decompressed,
                        obj,
                        cols,
                        batchSize,
                        parsingParallelism,
                        executor,
                        policy,
                        perFileReadSchema,
                        baseFileOffset,
                        maxRecordBytes,
                        captureSink,
                        statsStripeSize,
                        statsColumnScope,
                        partialResultsWarningSink,
                        streamingSegmentatorAdmission
                    );
                } catch (Exception e) {
                    try {
                        obj.abortStream(raw);
                    } catch (IOException abortEx) {
                        e.addSuppressed(abortEx);
                    }
                    throw e;
                }
            }
            case SPLITTABLE_OR_INDEXED_COMPRESSED -> {
                // Splittable / indexed codecs (e.g. bzip2) need codec-aware segmenting because
                // ParallelParsingCoordinator works in raw-file byte space and would feed the inner
                // line-oriented reader compressed bytes (yielding "Unrecognized token 'BZh91A...'"
                // for bzip2). Until we wire splittable parallel decompression here, fall back to
                // the single-threaded path through the CompressionDelegatingFormatReader, which
                // wraps the StorageObject in a DecompressingStorageObject before reading.
                CompressionDelegatingFormatReader cdr = (CompressionDelegatingFormatReader) reader;
                logger.debug(
                    "falling back to single-threaded read for splittable/indexed codec [{}]: "
                        + "codec-aware parallel decompression not yet wired",
                    cdr.codec().name()
                );
                return null;
            }
            default -> throw new IllegalStateException("Unhandled dispatch mode: " + mode);
        }
    }

    @Override
    public String describe() {
        String asyncMode;
        if (formatReader instanceof RangeAwareFormatReader) {
            asyncMode = "range-split";
        } else if (parsingParallelism > 1) {
            asyncMode = switch (resolveDispatchMode(formatReader)) {
                case SEGMENTABLE_UNCOMPRESSED -> "parallel-parse(" + parsingParallelism + ")";
                case STREAM_ONLY_COMPRESSED -> "streaming-parallel-parse(" + parsingParallelism + ")";
                // Splittable / indexed compressed paths fall back to single-threaded reads
                // until codec-aware parallel decompression is wired in openWithParallelism.
                case SPLITTABLE_OR_INDEXED_COMPRESSED -> "sync-wrapper";
                case NOT_PARALLELIZABLE -> formatReader.supportsNativeAsync() ? "native-async" : "sync-wrapper";
            };
        } else if (formatReader.supportsNativeAsync()) {
            asyncMode = "native-async";
        } else {
            asyncMode = "sync-wrapper";
        }
        return "ExternalDataSourceOperator["
            + "storage="
            + storageProvider.getClass().getSimpleName()
            + ", format="
            + formatReader.formatName()
            + ", mode="
            + asyncMode
            + ", path="
            + path
            + ", batchSize="
            + batchSize
            + ", maxBufferBytes="
            + ((long) maxBufferSize * Operator.TARGET_PAGE_SIZE)
            + "]";
    }

    public StorageProvider storageProvider() {
        return storageProvider;
    }

    public FormatReader formatReader() {
        return formatReader;
    }

    public StoragePath path() {
        return path;
    }

    public List<Attribute> attributes() {
        return attributes;
    }

    public int batchSize() {
        return batchSize;
    }

    public int maxBufferSize() {
        return maxBufferSize;
    }

    public int rowLimit() {
        return rowLimit;
    }

    public Executor executor() {
        return executor;
    }

    /** Test accessor for the consumer/drain executor; equals {@link #executor()} when no distinct pool was wired. */
    Executor producerExecutor() {
        return producerExecutor;
    }

    public FileList fileList() {
        return fileList;
    }

    public Set<String> partitionColumnNames() {
        return partitionColumnNames;
    }

    public Map<String, Object> partitionValues() {
        return partitionValues;
    }

    public ExternalSliceQueue sliceQueue() {
        return sliceQueue;
    }

    public ErrorPolicy errorPolicy() {
        return errorPolicy;
    }

    public int parsingParallelism() {
        return parsingParallelism;
    }

}
