/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRunnable;
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
import static org.elasticsearch.xpack.esql.datasources.ExternalSourceDrainUtils.drainPagesWithBudgetAsync;

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

    public AsyncExternalSourceOperatorFactory(
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

    public AsyncExternalSourceOperatorFactory(
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
        int parsingParallelism
    ) {
        this(
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
            null,
            null
        );
    }

    public AsyncExternalSourceOperatorFactory(
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
        ErrorPolicy errorPolicy
    ) {
        this(
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
            1,
            null,
            null
        );
    }

    public AsyncExternalSourceOperatorFactory(
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
        ExternalSliceQueue sliceQueue
    ) {
        this(
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
            null,
            1,
            null,
            null
        );
    }

    public AsyncExternalSourceOperatorFactory(
        StorageProvider storageProvider,
        FormatReader formatReader,
        StoragePath path,
        List<Attribute> attributes,
        int batchSize,
        int maxBufferSize,
        Executor executor,
        FileList fileList,
        Set<String> partitionColumnNames,
        Map<String, Object> partitionValues,
        ExternalSliceQueue sliceQueue
    ) {
        this(
            storageProvider,
            formatReader,
            path,
            attributes,
            batchSize,
            maxBufferSize,
            FormatReader.NO_LIMIT,
            executor,
            fileList,
            partitionColumnNames,
            partitionValues,
            sliceQueue,
            null,
            1,
            null,
            null
        );
    }

    public AsyncExternalSourceOperatorFactory(
        StorageProvider storageProvider,
        FormatReader formatReader,
        StoragePath path,
        List<Attribute> attributes,
        int batchSize,
        int maxBufferSize,
        Executor executor,
        FileList fileList,
        Set<String> partitionColumnNames,
        Map<String, Object> partitionValues
    ) {
        this(
            storageProvider,
            formatReader,
            path,
            attributes,
            batchSize,
            maxBufferSize,
            FormatReader.NO_LIMIT,
            executor,
            fileList,
            partitionColumnNames,
            partitionValues,
            null,
            null,
            1,
            null,
            null
        );
    }

    public AsyncExternalSourceOperatorFactory(
        StorageProvider storageProvider,
        FormatReader formatReader,
        StoragePath path,
        List<Attribute> attributes,
        int batchSize,
        int maxBufferSize,
        Executor executor,
        FileList fileList
    ) {
        this(
            storageProvider,
            formatReader,
            path,
            attributes,
            batchSize,
            maxBufferSize,
            FormatReader.NO_LIMIT,
            executor,
            fileList,
            null,
            null,
            null,
            null,
            1,
            null,
            null
        );
    }

    public AsyncExternalSourceOperatorFactory(
        StorageProvider storageProvider,
        FormatReader formatReader,
        StoragePath path,
        List<Attribute> attributes,
        int batchSize,
        int maxBufferSize,
        Executor executor
    ) {
        this(
            storageProvider,
            formatReader,
            path,
            attributes,
            batchSize,
            maxBufferSize,
            FormatReader.NO_LIMIT,
            executor,
            null,
            null,
            null,
            null,
            null,
            1,
            null,
            null
        );
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
        ActionListener<Void> failureListener = failureListener(buffer, driverContext);
        executor.execute(ActionRunnable.run(failureListener, () -> processNextSplit(sliceQueue, buffer, driverContext, rowLimit)));
    }

    private void processNextSplit(
        ExternalSliceQueue queue,
        AsyncExternalSourceBuffer buffer,
        DriverContext driverContext,
        int rowsRemaining
    ) {
        if (buffer.noMoreInputs() || (rowLimit != FormatReader.NO_LIMIT && rowsRemaining <= 0)) {
            buffer.finish(false);
            driverContext.removeAsyncAction();
            return;
        }
        ExternalSplit split = queue.nextSplit();
        if (split == null) {
            buffer.finish(false);
            driverContext.removeAsyncAction();
            return;
        }
        ActionListener<Void> failureListener = failureListener(buffer, driverContext);
        List<ExternalSplit> leaves = flattenToLeaves(split);
        processNextLeaf(leaves, 0, queue, buffer, driverContext, rowsRemaining, ActionListener.wrap(remaining -> {
            executor.execute(ActionRunnable.run(failureListener, () -> processNextSplit(queue, buffer, driverContext, remaining)));
        }, failureListener::onFailure));
    }

    private void processNextLeaf(
        List<ExternalSplit> leaves,
        int leafIndex,
        ExternalSliceQueue queue,
        AsyncExternalSourceBuffer buffer,
        DriverContext driverContext,
        int rowsRemaining,
        ActionListener<Integer> splitDoneListener
    ) {
        if (leafIndex >= leaves.size() || buffer.noMoreInputs() || (rowLimit != FormatReader.NO_LIMIT && rowsRemaining <= 0)) {
            splitDoneListener.onResponse(rowsRemaining);
            return;
        }
        ExternalSplit leaf = leaves.get(leafIndex);
        if (leaf instanceof FileSplit == false) {
            splitDoneListener.onFailure(new IllegalArgumentException("Unsupported split type: " + leaf.getClass().getName()));
            return;
        }
        FileSplit fileSplit = (FileSplit) leaf;
        VirtualColumnInjector injector = null;
        if (partitionColumnNames.isEmpty() == false) {
            injector = new VirtualColumnInjector(
                attributes,
                partitionColumnNames,
                fileSplit.partitionValues(),
                driverContext.blockFactory()
            );
        }
        List<String> cols = projectedColumns(injector);

        CloseableIterator<Page> pages;
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
        } catch (Exception e) {
            splitDoneListener.onFailure(e);
            return;
        }

        CloseableIterator<Page> adaptedPages;
        CloseableIterator<Page> wrappedPages;
        try {
            adaptedPages = adaptSchema(pages, fileSplit.columnMapping(), driverContext);
            wrappedPages = wrapWithInjector(adaptedPages, injector);
        } catch (Exception e) {
            closeQuietly(pages);
            splitDoneListener.onFailure(e);
            return;
        }

        drainPagesWithBudgetAsync(
            wrappedPages,
            buffer,
            rowsRemaining,
            executor,
            ActionListener.runAfter(ActionListener.<Integer>wrap(consumed -> {
                int remaining = rowLimit != FormatReader.NO_LIMIT ? rowsRemaining - consumed : rowsRemaining;
                executor.execute(
                    ActionRunnable.wrap(
                        splitDoneListener,
                        l -> processNextLeaf(leaves, leafIndex + 1, queue, buffer, driverContext, remaining, l)
                    )
                );
            }, splitDoneListener::onFailure), () -> closeQuietly(wrappedPages))
        );
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

        ActionListener<Void> failureListener = failureListener(buffer, driverContext);
        executor.execute(
            ActionRunnable.run(
                failureListener,
                () -> processNextFile(0, projectedColumns, buffer, driverContext, injector, schemaInfo, rowLimit)
            )
        );
    }

    private void processNextFile(
        int fileIndex,
        List<String> projectedColumns,
        AsyncExternalSourceBuffer buffer,
        DriverContext driverContext,
        VirtualColumnInjector injector,
        Map<StoragePath, SchemaReconciliation.FileSchemaInfo> schemaInfo,
        int rowsRemaining
    ) {
        if (fileIndex >= fileList.fileCount() || buffer.noMoreInputs() || (rowLimit != FormatReader.NO_LIMIT && rowsRemaining <= 0)) {
            buffer.finish(false);
            driverContext.removeAsyncAction();
            return;
        }

        boolean useParallel = rowLimit == FormatReader.NO_LIMIT && formatReader instanceof SegmentableFormatReader;
        CloseableIterator<Page> pages;
        try {
            StorageObject obj = storageProvider.newObject(fileList.path(fileIndex));
            if (useParallel) {
                pages = ParallelParsingCoordinator.parallelRead(
                    (SegmentableFormatReader) formatReader,
                    obj,
                    projectedColumns,
                    batchSize,
                    parsingParallelism,
                    executor,
                    errorPolicy
                );
            } else {
                int fileBudget = rowLimit == FormatReader.NO_LIMIT ? FormatReader.NO_LIMIT : rowsRemaining;
                FormatReadContext ctx = FormatReadContext.builder()
                    .projectedColumns(projectedColumns)
                    .batchSize(batchSize)
                    .rowLimit(fileBudget)
                    .errorPolicy(errorPolicy)
                    .build();
                pages = formatReader.read(obj, ctx);
            }
        } catch (Exception e) {
            buffer.onFailure(e);
            driverContext.removeAsyncAction();
            return;
        }

        SchemaReconciliation.ColumnMapping mapping = null;
        if (schemaInfo != null) {
            SchemaReconciliation.FileSchemaInfo info = schemaInfo.get(fileList.path(fileIndex));
            if (info != null) {
                mapping = info.mapping();
            }
        }
        CloseableIterator<Page> adaptedPages;
        CloseableIterator<Page> wrappedPages;
        try {
            adaptedPages = adaptSchema(pages, mapping, driverContext);
            wrappedPages = wrapWithInjector(adaptedPages, injector);
        } catch (Exception e) {
            closeQuietly(pages);
            buffer.onFailure(e);
            driverContext.removeAsyncAction();
            return;
        }

        ActionListener<Void> failureListener = failureListener(buffer, driverContext);
        drainPagesWithBudgetAsync(
            wrappedPages,
            buffer,
            rowsRemaining,
            executor,
            ActionListener.runAfter(ActionListener.<Integer>wrap(consumed -> {
                int remaining = rowLimit != FormatReader.NO_LIMIT ? rowsRemaining - consumed : rowsRemaining;
                executor.execute(
                    ActionRunnable.run(
                        failureListener,
                        () -> processNextFile(fileIndex + 1, projectedColumns, buffer, driverContext, injector, schemaInfo, remaining)
                    )
                );
            }, failureListener::onFailure), () -> closeQuietly(wrappedPages))
        );
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
