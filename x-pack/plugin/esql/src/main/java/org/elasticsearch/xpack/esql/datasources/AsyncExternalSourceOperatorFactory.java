/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalSplit;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReader;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.datasources.spi.StorageProvider;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;

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
    private final Executor executor;
    private final FileSet fileSet;
    private final Set<String> partitionColumnNames;
    private final Map<String, Object> partitionValues;
    private final ExternalSliceQueue sliceQueue;

    public AsyncExternalSourceOperatorFactory(
        StorageProvider storageProvider,
        FormatReader formatReader,
        StoragePath path,
        List<Attribute> attributes,
        int batchSize,
        int maxBufferSize,
        Executor executor,
        FileSet fileSet,
        Set<String> partitionColumnNames,
        Map<String, Object> partitionValues,
        ExternalSliceQueue sliceQueue
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
        this.fileSet = fileSet;
        this.partitionColumnNames = partitionColumnNames != null ? partitionColumnNames : Set.of();
        this.partitionValues = partitionValues != null ? partitionValues : Map.of();
        this.sliceQueue = sliceQueue;
    }

    public AsyncExternalSourceOperatorFactory(
        StorageProvider storageProvider,
        FormatReader formatReader,
        StoragePath path,
        List<Attribute> attributes,
        int batchSize,
        int maxBufferSize,
        Executor executor,
        FileSet fileSet,
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
            executor,
            fileSet,
            partitionColumnNames,
            partitionValues,
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
        FileSet fileSet
    ) {
        this(storageProvider, formatReader, path, attributes, batchSize, maxBufferSize, executor, fileSet, null, null, null);
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
        this(storageProvider, formatReader, path, attributes, batchSize, maxBufferSize, executor, null, null, null, null);
    }

    @Override
    public SourceOperator get(DriverContext driverContext) {
        AsyncExternalSourceBuffer buffer = new AsyncExternalSourceBuffer(maxBufferSize);
        driverContext.addAsyncAction();

        if (sliceQueue != null) {
            startSliceQueueRead(buffer, driverContext);
        } else if (fileSet != null && fileSet.isResolved()) {
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

    private void startSliceQueueRead(AsyncExternalSourceBuffer buffer, DriverContext driverContext) {
        executor.execute(() -> {
            try {
                ExternalSplit split;
                while ((split = sliceQueue.nextSplit()) != null) {
                    if (buffer.noMoreInputs()) {
                        break;
                    }
                    if (split instanceof FileSplit fileSplit) {
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
                        StorageObject obj = storageProvider.newObject(fileSplit.path(), fileSplit.length());
                        try (CloseableIterator<Page> pages = formatReader.read(obj, cols, batchSize)) {
                            drainPages(pages, buffer, injector);
                        }
                    } else {
                        throw new IllegalArgumentException("Unsupported split type: " + split.getClass().getName());
                    }
                }
                buffer.finish(false);
            } catch (Exception e) {
                buffer.onFailure(e);
            } finally {
                driverContext.removeAsyncAction();
            }
        });
    }

    private void startMultiFileRead(
        List<String> projectedColumns,
        AsyncExternalSourceBuffer buffer,
        DriverContext driverContext,
        VirtualColumnInjector injector
    ) {
        executor.execute(() -> {
            try {
                for (StorageEntry entry : fileSet.files()) {
                    if (buffer.noMoreInputs()) {
                        break;
                    }
                    StorageObject obj = storageProvider.newObject(entry.path(), entry.length(), entry.lastModified());
                    try (CloseableIterator<Page> pages = formatReader.read(obj, projectedColumns, batchSize)) {
                        drainPages(pages, buffer, injector);
                    }
                }
                buffer.finish(false);
            } catch (Exception e) {
                buffer.onFailure(e);
            } finally {
                driverContext.removeAsyncAction();
            }
        });
    }

    private void startNativeAsyncRead(
        StorageObject storageObject,
        List<String> projectedColumns,
        AsyncExternalSourceBuffer buffer,
        DriverContext driverContext,
        VirtualColumnInjector injector
    ) {
        formatReader.readAsync(storageObject, projectedColumns, batchSize, executor, ActionListener.wrap(iterator -> {
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
        executor.execute(() -> {
            CloseableIterator<Page> pages = null;
            try {
                pages = formatReader.read(storageObject, projectedColumns, batchSize);
                consumePages(pages, buffer, injector);
            } catch (Exception e) {
                buffer.onFailure(e);
            } finally {
                closeQuietly(pages);
                driverContext.removeAsyncAction();
            }
        });
    }

    private void consumePagesInBackground(
        CloseableIterator<Page> pages,
        AsyncExternalSourceBuffer buffer,
        DriverContext driverContext,
        VirtualColumnInjector injector
    ) {
        executor.execute(() -> {
            try {
                consumePages(pages, buffer, injector);
            } catch (Exception e) {
                buffer.onFailure(e);
            } finally {
                closeQuietly(pages);
                driverContext.removeAsyncAction();
            }
        });
    }

    private void consumePages(CloseableIterator<Page> pages, AsyncExternalSourceBuffer buffer, VirtualColumnInjector injector) {
        drainPages(pages, buffer, injector);
        buffer.finish(false);
    }

    private void drainPages(CloseableIterator<Page> pages, AsyncExternalSourceBuffer buffer, VirtualColumnInjector injector) {
        if (injector != null && injector.hasPartitionColumns()) {
            ExternalSourceDrainUtils.drainPages(new InjectingIterator(pages, injector), buffer);
        } else {
            ExternalSourceDrainUtils.drainPages(pages, buffer);
        }
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

    /**
     * Closes a CloseableIterator quietly, ignoring any exceptions.
     */
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
        String asyncMode = formatReader.supportsNativeAsync() ? "native-async" : "sync-wrapper";
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
            + ", maxBufferSize="
            + maxBufferSize
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

    public Executor executor() {
        return executor;
    }

    public FileSet fileSet() {
        return fileSet;
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
}
