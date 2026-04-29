/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.CloseableIterator;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.datasources.spi.ErrorPolicy;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalSplit;
import org.elasticsearch.xpack.esql.datasources.spi.FileList;
import org.elasticsearch.xpack.esql.datasources.spi.FilterPushdownSupport;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReadContext;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReader;
import org.elasticsearch.xpack.esql.datasources.spi.RangeAwareFormatReader;
import org.elasticsearch.xpack.esql.datasources.spi.SegmentableFormatReader;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.datasources.spi.StorageProvider;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;

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
 * The {@code executor} passed in runs background file reads and async drain continuations: it is
 * typically the {@code generic} pool (via
 * {@link org.elasticsearch.xpack.esql.datasources.spi.SourceOperatorContext#fileReadExecutor}, set in
 * {@code LocalExecutionPlanner}) so producer continuations do not starve {@code esql_worker} drivers that
 * {@link AsyncExternalSourceBuffer#pollPage()}. The drain is fully non-blocking: it runs synchronously
 * while the buffer has space and yields the thread when full, resuming via the executor when space is freed.
 *
 * @see AsyncExternalSourceBuffer
 * @see AsyncExternalSourceOperator
 */
public class AsyncExternalSourceOperatorFactory implements SourceOperator.SourceOperatorFactory {

    private final StorageProvider storageProvider;
    private final FormatReader formatReader;
    private final StoragePath path;
    private final List<Attribute> attributes;
    private final int batchSize;
    private final int maxBufferSize;
    private final int rowLimit;
    private final Executor executor;
    private final FileList fileList;
    private final Set<String> partitionColumnNames;
    private final Map<String, Object> partitionValues;
    private final ExternalSliceQueue sliceQueue;
    private final ErrorPolicy errorPolicy;
    private final int parsingParallelism;
    private final List<Expression> pushedExpressions;
    private final FilterPushdownSupport pushdownSupport;

    private AsyncExternalSourceOperatorFactory(
        StorageProvider storageProvider,
        FormatReader formatReader,
        StoragePath path,
        List<Attribute> attributes,
        int batchSize,
        int maxBufferSize,
        int rowLimit,
        Executor executor,
        FileList fileList,
        Set<String> partitionColumnNames,
        Map<String, Object> partitionValues,
        ExternalSliceQueue sliceQueue,
        ErrorPolicy errorPolicy,
        int parsingParallelism,
        @Nullable List<Expression> pushedExpressions,
        @Nullable FilterPushdownSupport pushdownSupport
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
        this.executor = executor;
        this.batchSize = batchSize;
        this.maxBufferSize = maxBufferSize;
        this.rowLimit = rowLimit;
        this.fileList = fileList;
        this.partitionColumnNames = partitionColumnNames != null ? partitionColumnNames : Set.of();
        this.partitionValues = partitionValues != null ? partitionValues : Map.of();
        this.sliceQueue = sliceQueue;
        this.errorPolicy = errorPolicy != null ? errorPolicy : formatReader.defaultErrorPolicy();
        this.parsingParallelism = Math.max(1, parsingParallelism);
        this.pushedExpressions = pushedExpressions != null ? pushedExpressions : List.of();
        this.pushdownSupport = pushdownSupport;
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

        private int rowLimit = FormatReader.NO_LIMIT;
        private FileList fileList;
        private Set<String> partitionColumnNames;
        private Map<String, Object> partitionValues;
        private ExternalSliceQueue sliceQueue;
        private ErrorPolicy errorPolicy;
        private int parsingParallelism = 1;
        private List<Expression> pushedExpressions;
        private FilterPushdownSupport pushdownSupport;

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

        public Builder rowLimit(int rowLimit) {
            this.rowLimit = rowLimit;
            return this;
        }

        public Builder fileList(@Nullable FileList fileList) {
            this.fileList = fileList;
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

        public Builder pushedExpressions(@Nullable List<Expression> pushedExpressions) {
            this.pushedExpressions = pushedExpressions;
            return this;
        }

        public Builder pushdownSupport(@Nullable FilterPushdownSupport pushdownSupport) {
            this.pushdownSupport = pushdownSupport;
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
                fileList,
                partitionColumnNames,
                partitionValues,
                sliceQueue,
                errorPolicy,
                parsingParallelism,
                pushedExpressions,
                pushdownSupport
            );
        }
    }

    @Override
    public SourceOperator get(DriverContext driverContext) {
        long maxBufferBytes = (long) maxBufferSize * Operator.TARGET_PAGE_SIZE;
        AsyncExternalSourceBuffer buffer = new AsyncExternalSourceBuffer(maxBufferBytes);
        driverContext.addAsyncAction();

        if (sliceQueue != null) {
            startSliceQueueRead(buffer, driverContext);
        } else if (fileList != null && fileList.isResolved()) {
            VirtualColumnInjector injector = buildInjector(driverContext);
            List<String> projectedColumns = projectedColumns(injector);
            startMultiFileRead(projectedColumns, buffer, driverContext, injector);
        } else {
            VirtualColumnInjector injector = buildInjector(driverContext);
            List<String> projectedColumns = projectedColumns(injector);
            StorageObject storageObject = storageProvider.newObject(path);
            if (formatReader.supportsNativeAsync()) {
                startNativeAsyncRead(storageObject, projectedColumns, buffer, driverContext, injector);
            } else {
                startSyncWrapperRead(storageObject, projectedColumns, buffer, driverContext, injector);
            }
        }

        return new AsyncExternalSourceOperator(buffer);
    }

    private VirtualColumnInjector buildInjector(DriverContext driverContext) {
        if (partitionColumnNames.isEmpty() == false) {
            return new VirtualColumnInjector(attributes, partitionColumnNames, partitionValues, driverContext.blockFactory());
        }
        return null;
    }

    private List<String> projectedColumns(VirtualColumnInjector injector) {
        if (injector != null) {
            return injector.dataColumnNames();
        }
        List<String> cols = new ArrayList<>(attributes.size());
        for (Attribute attr : attributes) {
            cols.add(attr.name());
        }
        return cols;
    }

    private CloseableIterator<Page> adaptSchema(
        CloseableIterator<Page> pages,
        SchemaReconciliation.ColumnMapping mapping,
        DriverContext driverContext
    ) {
        if (mapping == null || mapping.isIdentity()) {
            return pages;
        }
        List<Attribute> dataColumns = attributes.subList(0, mapping.columnCount());
        return new SchemaAdaptingIterator(pages, dataColumns, mapping, driverContext.blockFactory());
    }

    /**
     * Returns a format reader with an adapted pushed filter for this file, or the original reader
     * if no adaptation is needed. Adaptation is needed when the file has missing columns and
     * pushed expressions reference those columns.
     */
    private FormatReader readerForFile(FileSplit fileSplit) {
        if (pushedExpressions.isEmpty() || pushdownSupport == null) {
            return formatReader;
        }
        SchemaReconciliation.ColumnMapping mapping = fileSplit.columnMapping();
        if (mapping == null || (mapping.hasMissingColumns() == false && mapping.hasCasts() == false)) {
            return formatReader;
        }
        Set<String> fileColumnNames = new LinkedHashSet<>();
        Map<String, DataType> fileColumnTypes = new HashMap<>();
        assert mapping.columnCount() <= attributes.size()
            : "column mapping count [" + mapping.columnCount() + "] exceeds attributes size [" + attributes.size() + "]";
        for (int i = 0; i < mapping.columnCount(); i++) {
            if (mapping.localIndex(i) != -1) {
                String name = attributes.get(i).name();
                fileColumnNames.add(name);
                DataType castTarget = mapping.cast(i);
                if (castTarget != null) {
                    DataType fileType = inferFileType(castTarget);
                    if (fileType != null) {
                        fileColumnTypes.put(name, fileType);
                    }
                }
            }
        }
        List<Expression> adapted = FilterAdaptation.adaptFilterForFile(pushedExpressions, fileColumnNames, fileColumnTypes);
        if (adapted.isEmpty()) {
            return formatReader.withPushedFilter(null);
        }
        FilterPushdownSupport.PushdownResult result = pushdownSupport.pushFilters(adapted);
        if (result.hasPushedFilter()) {
            return formatReader.withPushedFilter(result.pushedFilter());
        }
        return formatReader.withPushedFilter(null);
    }

    /**
     * Infers the file's native type from the cast target. Only returns a narrower type when
     * the adaptation is safe for integral comparisons (LONG→INTEGER).
     * DOUBLE→INTEGER narrowing is not supported because {@code Number.longValue()} truncates
     * fractional values, which changes comparison semantics (e.g., {@code col < 2.7} vs {@code col < 2}).
     */
    private static DataType inferFileType(DataType castTarget) {
        if (castTarget == DataType.LONG) {
            return DataType.INTEGER;
        }
        return null;
    }

    private void startSliceQueueRead(AsyncExternalSourceBuffer buffer, DriverContext driverContext) {
        ActionListener<Void> completionListener = ActionListener.assertOnce(ActionListener.wrap(v -> {
            buffer.finish(false);
            driverContext.removeAsyncAction();
        }, e -> {
            buffer.onFailure(e);
            driverContext.removeAsyncAction();
        }));
        ProducerState state = new ProducerState(sliceQueue, null, null, null, buffer, driverContext, rowLimit);
        try {
            executor.execute(ActionRunnable.wrap(completionListener, l -> runProducerLoop(state, l)));
        } catch (Exception e) {
            completionListener.onFailure(e);
        }
    }

    /**
     * Multi-file read path (legacy, non-slice-queue). Per-file filter adaptation is not applied
     * here because this path does not carry {@link FileSplit} with {@link SchemaReconciliation.ColumnMapping};
     * UNION_BY_NAME queries use the slice-queue path ({@link #startSliceQueueRead}) instead.
     */
    private void startMultiFileRead(
        List<String> projectedColumns,
        AsyncExternalSourceBuffer buffer,
        DriverContext driverContext,
        VirtualColumnInjector injector
    ) {
        Map<StoragePath, SchemaReconciliation.FileSchemaInfo> schemaInfo = fileList != null ? fileList.fileSchemaInfo() : null;
        ActionListener<Void> completionListener = ActionListener.assertOnce(ActionListener.wrap(v -> {
            buffer.finish(false);
            driverContext.removeAsyncAction();
        }, e -> {
            buffer.onFailure(e);
            driverContext.removeAsyncAction();
        }));
        ProducerState state = new ProducerState(null, fileList, projectedColumns, injector, buffer, driverContext, rowLimit);
        state.schemaInfo = schemaInfo;
        try {
            executor.execute(ActionRunnable.wrap(completionListener, l -> runProducerLoop(state, l)));
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
        @Nullable
        final VirtualColumnInjector multiFileInjector;
        final AsyncExternalSourceBuffer buffer;
        final DriverContext driverContext;

        int fileIndex;
        @Nullable
        List<ExternalSplit> leaves;
        int leafIndex;
        int rowsRemaining;
        @Nullable
        CloseableIterator<Page> pages;
        @Nullable
        Map<StoragePath, SchemaReconciliation.FileSchemaInfo> schemaInfo;

        ProducerState(
            @Nullable ExternalSliceQueue queue,
            @Nullable FileList fileList,
            @Nullable List<String> projectedColumns,
            @Nullable VirtualColumnInjector multiFileInjector,
            AsyncExternalSourceBuffer buffer,
            DriverContext driverContext,
            int rowsRemaining
        ) {
            if ((queue == null) == (fileList == null)) {
                throw new IllegalArgumentException("ProducerState requires exactly one of queue or fileList");
            }
            this.queue = queue;
            this.fileList = fileList;
            this.projectedColumns = projectedColumns;
            this.multiFileInjector = multiFileInjector;
            this.buffer = buffer;
            this.driverContext = driverContext;
            this.rowsRemaining = rowsRemaining;
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
     * Single-step producer loop. Each invocation either drains some pages from the current iterator,
     * opens a new iterator for the next unit, or registers a space callback and returns. The loop
     * self-resubmits on the executor to avoid running producer I/O on the Driver thread.
     */
    private void runProducerLoop(ProducerState state, ActionListener<Void> completionListener) {
        try {
            // Open an iterator for the next unit if we don't have one.
            if (state.pages == null) {
                if (advanceToNextUnit(state) == false) {
                    completionListener.onResponse(null);
                    return;
                }
            }
            DrainResult result = drainHotPath(state, completionListener);
            switch (result) {
                case DONE -> {
                    // Buffer finished (externally or by row-limit exhaustion) while an iterator is still open:
                    // close it before reporting completion so no resources leak on cancellation paths.
                    closeQuietly(state.pages);
                    state.pages = null;
                    completionListener.onResponse(null);
                }
                case EOF -> {
                    closeQuietly(state.pages);
                    state.pages = null;
                    // Re-submit to avoid unbounded recursion between units and to stay off the Driver thread.
                    executor.execute(ActionRunnable.wrap(completionListener, l -> runProducerLoop(state, l)));
                }
                case BLOCKED -> {
                    // A listener has been registered on waitForSpace that will re-submit runProducerLoop.
                }
            }
        } catch (Exception e) {
            closeQuietly(state.pages);
            state.pages = null;
            completionListener.onFailure(e);
        }
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
            if (buffer.noMoreInputs()) {
                return DrainResult.DONE;
            }
            if (rowLimit != FormatReader.NO_LIMIT && state.rowsRemaining <= 0) {
                return DrainResult.DONE;
            }
            if (pages.hasNext() == false) {
                return DrainResult.EOF;
            }
            SubscribableListener<Void> space = buffer.waitForSpace();
            if (space.isDone() == false) {
                space.addListener(ActionListener.wrap(v -> {
                    try {
                        executor.execute(() -> runProducerLoop(state, completionListener));
                    } catch (Exception e) {
                        closeQuietly(state.pages);
                        state.pages = null;
                        completionListener.onFailure(e);
                    }
                }, e -> {
                    closeQuietly(state.pages);
                    state.pages = null;
                    completionListener.onFailure(e);
                }));
                return DrainResult.BLOCKED;
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
     * Advance the iteration position to the next unit (slice-queue leaf or multi-file file) and
     * open a fresh page iterator for it. Returns {@code false} if iteration is exhausted or the
     * buffer has been finished externally.
     */
    private boolean advanceToNextUnit(ProducerState state) throws IOException {
        while (true) {
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
     */
    private boolean openNextSliceQueueLeaf(ProducerState state) throws IOException {
        if (state.leaves == null || state.leafIndex >= state.leaves.size()) {
            ExternalSplit split = state.queue.nextSplit();
            if (split == null) {
                return false;
            }
            state.leaves = flattenToLeaves(split);
            state.leafIndex = 0;
        }
        ExternalSplit leaf = state.leaves.get(state.leafIndex++);
        if (leaf instanceof FileSplit == false) {
            throw new IllegalArgumentException("Unsupported split type: " + leaf.getClass().getName());
        }
        FileSplit fileSplit = (FileSplit) leaf;
        VirtualColumnInjector injector = null;
        if (partitionColumnNames.isEmpty() == false) {
            injector = new VirtualColumnInjector(
                attributes,
                partitionColumnNames,
                fileSplit.partitionValues(),
                state.driverContext.blockFactory()
            );
        }
        List<String> cols = projectedColumns(injector);

        CloseableIterator<Page> pages = null;
        try {
            FormatReader fileReader = readerForFile(fileSplit);
            boolean isRangeSplit = "true".equals(fileSplit.config().get(FileSplitProvider.RANGE_SPLIT_KEY));
            if (isRangeSplit && fileReader instanceof RangeAwareFormatReader rangeReader) {
                String fileLengthStr = (String) fileSplit.config().get(FileSplitProvider.FILE_LENGTH_KEY);
                StorageObject fullObj = fileLengthStr != null
                    ? storageProvider.newObject(fileSplit.path(), Long.parseLong(fileLengthStr))
                    : storageProvider.newObject(fileSplit.path());
                long rangeEnd = fileSplit.offset() + fileSplit.length();
                pages = rangeReader.readRange(fullObj, cols, batchSize, fileSplit.offset(), rangeEnd, attributes, errorPolicy);
            } else {
                StorageObject obj = FileSplitProvider.storageObjectForSplit(storageProvider, fileSplit);
                boolean firstSplit = fileSplit.offset() == 0 || "true".equals(fileSplit.config().get(FileSplitProvider.FIRST_SPLIT_KEY));
                boolean lastSplit = "true".equals(fileSplit.config().get(FileSplitProvider.LAST_SPLIT_KEY));
                FormatReadContext ctx = FormatReadContext.builder()
                    .projectedColumns(cols)
                    .batchSize(batchSize)
                    .rowLimit(FormatReader.NO_LIMIT)
                    .errorPolicy(errorPolicy)
                    .firstSplit(firstSplit)
                    .lastSplit(lastSplit)
                    .build();
                pages = fileReader.read(obj, ctx);
            }
            CloseableIterator<Page> adapted = adaptSchema(pages, fileSplit.columnMapping(), state.driverContext);
            state.pages = wrapWithInjector(adapted, injector);
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
        FileList files = state.fileList;
        assert files != null;
        if (state.fileIndex >= files.fileCount()) {
            return false;
        }
        int fileIndex = state.fileIndex++;
        List<String> cols = state.projectedColumns;
        boolean useParallel = rowLimit == FormatReader.NO_LIMIT && formatReader instanceof SegmentableFormatReader;

        CloseableIterator<Page> pages = null;
        try {
            StorageObject obj = storageProvider.newObject(files.path(fileIndex));
            if (useParallel) {
                pages = ParallelParsingCoordinator.parallelRead(
                    (SegmentableFormatReader) formatReader,
                    obj,
                    cols,
                    batchSize,
                    parsingParallelism,
                    executor,
                    errorPolicy
                );
            } else {
                int fileBudget = rowLimit == FormatReader.NO_LIMIT ? FormatReader.NO_LIMIT : state.rowsRemaining;
                FormatReadContext ctx = FormatReadContext.builder()
                    .projectedColumns(cols)
                    .batchSize(batchSize)
                    .rowLimit(fileBudget)
                    .errorPolicy(errorPolicy)
                    .build();
                pages = formatReader.read(obj, ctx);
            }
            SchemaReconciliation.ColumnMapping mapping = null;
            if (state.schemaInfo != null) {
                SchemaReconciliation.FileSchemaInfo info = state.schemaInfo.get(files.path(fileIndex));
                if (info != null) {
                    mapping = info.mapping();
                }
            }
            CloseableIterator<Page> adapted = adaptSchema(pages, mapping, state.driverContext);
            state.pages = wrapWithInjector(adapted, state.multiFileInjector);
            return true;
        } catch (Exception e) {
            closeQuietly(pages);
            if (e instanceof IOException io) throw io;
            if (e instanceof RuntimeException re) throw re;
            throw new IOException(e);
        }
    }

    private void startNativeAsyncRead(
        StorageObject storageObject,
        List<String> projectedColumns,
        AsyncExternalSourceBuffer buffer,
        DriverContext driverContext,
        VirtualColumnInjector injector
    ) {
        FormatReadContext ctx = FormatReadContext.builder()
            .projectedColumns(projectedColumns)
            .batchSize(batchSize)
            .rowLimit(rowLimit)
            .errorPolicy(errorPolicy)
            .build();
        formatReader.readAsync(storageObject, ctx, executor, ActionListener.wrap(iterator -> {
            consumePagesInBackground(iterator, buffer, driverContext, injector);
        }, e -> {
            buffer.onFailure(e);
            driverContext.removeAsyncAction();
        }));
    }

    private void startSyncWrapperRead(
        StorageObject storageObject,
        List<String> projectedColumns,
        AsyncExternalSourceBuffer buffer,
        DriverContext driverContext,
        VirtualColumnInjector injector
    ) {
        ActionListener<Void> failureListener = failureListener(buffer, driverContext);
        executor.execute(ActionRunnable.run(failureListener, () -> {
            CloseableIterator<Page> pages;
            if (rowLimit == FormatReader.NO_LIMIT && formatReader instanceof SegmentableFormatReader segmentable) {
                pages = ParallelParsingCoordinator.parallelRead(
                    segmentable,
                    storageObject,
                    projectedColumns,
                    batchSize,
                    parsingParallelism,
                    executor,
                    errorPolicy
                );
            } else {
                FormatReadContext ctx = FormatReadContext.builder()
                    .projectedColumns(projectedColumns)
                    .batchSize(batchSize)
                    .rowLimit(rowLimit)
                    .errorPolicy(errorPolicy)
                    .build();
                pages = formatReader.read(storageObject, ctx);
            }
            CloseableIterator<Page> wrapped;
            try {
                wrapped = wrapWithInjector(pages, injector);
            } catch (Exception e) {
                closeQuietly(pages);
                throw e;
            }
            drainPagesAsync(
                wrapped,
                buffer,
                executor,
                ActionListener.runAfter(ActionListener.wrap(v -> buffer.finish(false), e -> buffer.onFailure(e)), () -> {
                    closeQuietly(wrapped);
                    driverContext.removeAsyncAction();
                })
            );
        }));
    }

    private void consumePagesInBackground(
        CloseableIterator<Page> pages,
        AsyncExternalSourceBuffer buffer,
        DriverContext driverContext,
        VirtualColumnInjector injector
    ) {
        ActionListener<Void> failureListener = ActionListener.wrap(v -> {}, e -> {
            closeQuietly(pages);
            buffer.onFailure(e);
            driverContext.removeAsyncAction();
        });
        executor.execute(ActionRunnable.run(failureListener, () -> {
            CloseableIterator<Page> wrapped = wrapWithInjector(pages, injector);
            drainPagesAsync(
                wrapped,
                buffer,
                executor,
                ActionListener.runAfter(ActionListener.wrap(v -> buffer.finish(false), e -> buffer.onFailure(e)), () -> {
                    closeQuietly(wrapped);
                    driverContext.removeAsyncAction();
                })
            );
        }));
    }

    private static CloseableIterator<Page> wrapWithInjector(CloseableIterator<Page> pages, VirtualColumnInjector injector) {
        if (injector != null && injector.hasPartitionColumns()) {
            return new InjectingIterator(pages, injector);
        }
        return pages;
    }

    private static class InjectingIterator implements CloseableIterator<Page> {
        private final CloseableIterator<Page> delegate;
        private final VirtualColumnInjector injector;

        InjectingIterator(CloseableIterator<Page> delegate, VirtualColumnInjector injector) {
            this.delegate = delegate;
            this.injector = injector;
        }

        @Override
        public boolean hasNext() {
            return delegate.hasNext();
        }

        @Override
        public Page next() {
            return injector.inject(delegate.next());
        }

        @Override
        public void close() throws IOException {
            delegate.close();
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

    private static void closeQuietly(CloseableIterator<?> iterator) {
        if (iterator != null) {
            try {
                iterator.close();
            } catch (Exception e) {
                // Ignore - closeExpectNoException semantics
            }
        }
    }

    @Override
    public String describe() {
        String asyncMode;
        if (formatReader instanceof RangeAwareFormatReader) {
            asyncMode = "range-split";
        } else if (formatReader instanceof SegmentableFormatReader && parsingParallelism > 1) {
            asyncMode = "parallel-parse(" + parsingParallelism + ")";
        } else if (formatReader.supportsNativeAsync()) {
            asyncMode = "native-async";
        } else {
            asyncMode = "sync-wrapper";
        }
        return "AsyncExternalSourceOperator["
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
