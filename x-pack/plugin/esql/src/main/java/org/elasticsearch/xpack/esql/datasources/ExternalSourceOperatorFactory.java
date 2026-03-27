/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReader;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.datasources.spi.StorageProvider;

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

    /**
     * Creates an ExternalSourceOperatorFactory.
     *
     * @param storageProvider the storage provider for accessing the object
     * @param formatReader the format reader for parsing the data
     * @param path the path to the data object
     * @param attributes the ESQL attributes (columns to read)
     * @param batchSize the target number of rows per batch
     */
    public ExternalSourceOperatorFactory(
        StorageProvider storageProvider,
        FormatReader formatReader,
        StoragePath path,
        List<Attribute> attributes,
        int batchSize
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
    }

    @Override
    public SourceOperator get(DriverContext driverContext) {
        // Create a storage object for the path
        StorageObject storageObject = storageProvider.newObject(path);

        // Extract column names from attributes
        List<String> projectedColumns = new ArrayList<>(attributes.size());
        for (Attribute attr : attributes) {
            projectedColumns.add(attr.name());
        }

        try {
            // Open a reader for the object
            CloseableIterator<Page> pages = formatReader.read(storageObject, projectedColumns, batchSize);

            // Return a simple source operator that iterates through pages
            return new ExternalSourceOperator(pages, driverContext);
        } catch (Exception e) {
            throw new RuntimeException("Failed to create external source operator for: " + path, e);
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

    /**
     * Simple source operator that reads pages from a CloseableIterator.
     * This is a synchronous operator - for async operations, use the AsyncExternalSourceOperator.
     */
    private static class ExternalSourceOperator extends SourceOperator {
        private static final Logger logger = LogManager.getLogger(ExternalSourceOperator.class);

        private final CloseableIterator<Page> pages;
        private boolean finished = false;

        ExternalSourceOperator(CloseableIterator<Page> pages, DriverContext driverContext) {
            this.pages = pages;
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
                throw new RuntimeException("Error reading from external source", e);
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
}
