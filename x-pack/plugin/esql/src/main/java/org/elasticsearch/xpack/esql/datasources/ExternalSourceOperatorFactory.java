/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.CloseableIterator;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalSplit;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReadContext;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReader;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.datasources.spi.StorageProvider;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;

/**
 * Factory for creating source operators that read from external storage using
 * the StorageProvider and FormatReader abstractions.
 *
 * This is the generic implementation that works with any StorageProvider (HTTP, S3, local)
 * and any FormatReader (CSV, Parquet, etc.).
 *
 * The factory creates operators that:
 * <ul>
 *   <li>Use StorageProvider to access the storage object</li>
 *   <li>Use FormatReader to parse the data format</li>
 *   <li>Produce ESQL Page batches for the query pipeline</li>
 * </ul>
 */
public class ExternalSourceOperatorFactory implements SourceOperator.SourceOperatorFactory {

    private final StorageProvider storageProvider;
    private final FormatReader formatReader;
    private final StoragePath path;
    private final List<Attribute> attributes;
    private final int batchSize;
    private final int rowLimit;
    private final ExternalSliceQueue sliceQueue;

    public ExternalSourceOperatorFactory(
        StorageProvider storageProvider,
        FormatReader formatReader,
        StoragePath path,
        List<Attribute> attributes,
        int batchSize,
        int rowLimit,
        @Nullable ExternalSliceQueue sliceQueue
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
        if (batchSize <= 0) {
            throw new IllegalArgumentException("batchSize must be positive, got: " + batchSize);
        }

        this.storageProvider = storageProvider;
        this.formatReader = formatReader;
        this.path = path;
        this.attributes = attributes;
        this.batchSize = batchSize;
        this.rowLimit = rowLimit;
        this.sliceQueue = sliceQueue;
    }

    public ExternalSourceOperatorFactory(
        StorageProvider storageProvider,
        FormatReader formatReader,
        StoragePath path,
        List<Attribute> attributes,
        int batchSize
    ) {
        this(storageProvider, formatReader, path, attributes, batchSize, FormatReader.NO_LIMIT, null);
    }

    @Override
    public SourceOperator get(DriverContext driverContext) {
        List<String> projectedColumns = new ArrayList<>(attributes.size());
        for (Attribute attr : attributes) {
            projectedColumns.add(attr.name());
        }

        if (sliceQueue != null) {
            return new SliceQueueSourceOperator(
                storageProvider,
                formatReader,
                projectedColumns,
                attributes,
                batchSize,
                rowLimit,
                sliceQueue,
                driverContext.blockFactory()
            );
        }

        StorageObject storageObject = storageProvider.newObject(path);
        try {
            FormatReadContext ctx = FormatReadContext.builder()
                .projectedColumns(projectedColumns)
                .batchSize(batchSize)
                .rowLimit(rowLimit)
                .build();
            CloseableIterator<Page> pages = formatReader.read(storageObject, ctx);
            return new ExternalSourceOperator(pages, path);
        } catch (Exception e) {
            throw new ElasticsearchException("Failed to create external source operator for [" + path + "]", e);
        }
    }

    @Override
    public String describe() {
        return "ExternalSourceOperator["
            + "storage="
            + storageProvider.getClass().getSimpleName()
            + ", format="
            + formatReader.formatName()
            + ", path="
            + path
            + ", batchSize="
            + batchSize
            + "]";
    }

    private static class ExternalSourceOperator extends SourceOperator {
        private static final Logger logger = LogManager.getLogger(ExternalSourceOperator.class);

        private final CloseableIterator<Page> pages;
        private final StoragePath path;
        private boolean finished = false;

        ExternalSourceOperator(CloseableIterator<Page> pages, StoragePath path) {
            this.pages = pages;
            this.path = path;
        }

        @Override
        public Page getOutput() {
            if (finished || pages.hasNext() == false) {
                return null;
            }

            try {
                return pages.next();
            } catch (Exception e) {
                finished = true;
                throw new ElasticsearchException("Error reading from external source [" + path + "]", e);
            }
        }

        @Override
        public boolean isFinished() {
            if (finished) {
                return true;
            }

            if (pages.hasNext() == false) {
                finished = true;
                return true;
            }

            return false;
        }

        @Override
        public void finish() {
            finished = true;
        }

        @Override
        public void close() {
            try {
                pages.close();
            } catch (Exception e) {
                logger.warn("Failed to close external source pages iterator", e);
            }
        }

        @Override
        public String toString() {
            return "ExternalSourceOperator";
        }
    }

    /**
     * Synchronous source operator that claims splits from an {@link ExternalSliceQueue}
     * and reads files one at a time until the queue is exhausted.
     */
    private static class SliceQueueSourceOperator extends SourceOperator {
        private static final Logger logger = LogManager.getLogger(SliceQueueSourceOperator.class);

        private final StorageProvider storageProvider;
        private final FormatReader formatReader;
        private final List<String> projectedColumns;
        private final List<Attribute> attributes;
        private final int batchSize;
        private final int rowLimit;
        private final ExternalSliceQueue sliceQueue;
        private final BlockFactory blockFactory;
        private final ArrayDeque<ExternalSplit> pendingChildren = new ArrayDeque<>();
        private CloseableIterator<Page> currentPages;
        private StoragePath currentSplitPath;
        private boolean finished = false;

        SliceQueueSourceOperator(
            StorageProvider storageProvider,
            FormatReader formatReader,
            List<String> projectedColumns,
            List<Attribute> attributes,
            int batchSize,
            int rowLimit,
            ExternalSliceQueue sliceQueue,
            BlockFactory blockFactory
        ) {
            this.storageProvider = storageProvider;
            this.formatReader = formatReader;
            this.projectedColumns = projectedColumns;
            this.attributes = attributes;
            this.batchSize = batchSize;
            this.rowLimit = rowLimit;
            this.sliceQueue = sliceQueue;
            this.blockFactory = blockFactory;
        }

        @Override
        public Page getOutput() {
            if (finished) {
                return null;
            }
            try {
                while (true) {
                    if (currentPages != null && currentPages.hasNext()) {
                        return currentPages.next();
                    }
                    closeCurrentPages();
                    currentSplitPath = null;
                    ExternalSplit next = nextLeafSplit();
                    if (next == null) {
                        finished = true;
                        return null;
                    }
                    currentPages = openFileSplit(next);
                }
            } catch (Exception e) {
                finished = true;
                String loc = currentSplitPath != null ? currentSplitPath.toString() : "unknown";
                throw new ElasticsearchException("Error reading from external source split [" + loc + "]", e);
            }
        }

        private ExternalSplit nextLeafSplit() {
            while (true) {
                if (pendingChildren.isEmpty() == false) {
                    return pendingChildren.poll();
                }
                ExternalSplit split = sliceQueue.nextSplit();
                if (split == null) {
                    return null;
                }
                if (split instanceof CoalescedSplit coalesced) {
                    pendingChildren.addAll(coalesced.children());
                } else {
                    return split;
                }
            }
        }

        private CloseableIterator<Page> openFileSplit(ExternalSplit split) throws IOException {
            if (split instanceof FileSplit fileSplit) {
                currentSplitPath = fileSplit.path();
                StorageObject obj = FileSplitProvider.storageObjectForSplit(storageProvider, fileSplit);
                boolean firstSplit = fileSplit.offset() == 0 || "true".equals(fileSplit.config().get(FileSplitProvider.FIRST_SPLIT_KEY));
                boolean lastSplit = "true".equals(fileSplit.config().get(FileSplitProvider.LAST_SPLIT_KEY));

                SchemaReconciliation.ColumnMapping columnMapping = fileSplit.columnMapping();
                List<String> effectiveProjection = projectedColumns;
                List<Attribute> dataColumns = null;
                if (columnMapping != null && columnMapping.columnCount() < attributes.size()) {
                    dataColumns = attributes.subList(0, columnMapping.columnCount());
                    effectiveProjection = new ArrayList<>(dataColumns.size());
                    for (Attribute attr : dataColumns) {
                        effectiveProjection.add(attr.name());
                    }
                }

                FormatReadContext ctx = FormatReadContext.builder()
                    .projectedColumns(effectiveProjection)
                    .batchSize(batchSize)
                    .rowLimit(FormatReader.NO_LIMIT)
                    .firstSplit(firstSplit)
                    .lastSplit(lastSplit)
                    .build();
                CloseableIterator<Page> pages = formatReader.read(obj, ctx);

                if (columnMapping != null && columnMapping.isIdentity() == false) {
                    if (dataColumns == null) {
                        dataColumns = attributes.subList(0, columnMapping.columnCount());
                    }
                    pages = new SchemaAdaptingIterator(pages, dataColumns, columnMapping, blockFactory);
                }
                return pages;
            }
            throw new IllegalArgumentException("Unsupported split type: " + split.getClass().getName());
        }

        @Override
        public boolean isFinished() {
            return finished;
        }

        @Override
        public void finish() {
            finished = true;
        }

        @Override
        public void close() {
            closeCurrentPages();
        }

        private void closeCurrentPages() {
            if (currentPages != null) {
                try {
                    currentPages.close();
                } catch (Exception e) {
                    logger.warn("Failed to close external source pages iterator", e);
                }
                currentPages = null;
            }
        }

        @Override
        public String toString() {
            return "SliceQueueSourceOperator";
        }
    }
}
