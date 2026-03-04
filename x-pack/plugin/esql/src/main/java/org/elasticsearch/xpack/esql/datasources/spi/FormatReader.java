/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.datasources.CloseableIterator;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.Executor;

/**
 * Unified interface for reading data formats.
 * <p>
 * Simple formats: implement only {@link #read} (sync) - async wrapping is automatic.
 * Async-capable formats: override {@link #readAsync} for native async behavior.
 * <p>
 * The output is ESQL's native Page format rather than Arrow to avoid
 * mandating Arrow as a dependency for all format implementations.
 * <p>
 * Implementations should provide metadata discovery via {@link #metadata(StorageObject)}
 * which returns a unified {@link SourceMetadata} containing schema and source information.
 */
public interface FormatReader extends Closeable {

    int NO_LIMIT = -1;

    /**
     * Strategy for resolving schemas across multiple files in a glob/multi-file query.
     */
    enum SchemaResolution {
        /** Use the schema from the first file; ignore differences in subsequent files. */
        FIRST_FILE_WINS,
        // TODO: implement strict schema validation across files
        STRICT,
        // TODO: implement union-by-name schema merging across files
        UNION_BY_NAME
    }

    default SchemaResolution defaultSchemaResolution() {
        return SchemaResolution.FIRST_FILE_WINS;
    }

    // === SYNC API (required - implement this for simple formats) ===

    SourceMetadata metadata(StorageObject object) throws IOException;

    default List<Attribute> schema(StorageObject object) throws IOException {
        return metadata(object).schema();
    }

    CloseableIterator<Page> read(StorageObject object, List<String> projectedColumns, int batchSize) throws IOException;

    default CloseableIterator<Page> read(StorageObject object, List<String> projectedColumns, int batchSize, int rowLimit)
        throws IOException {
        CloseableIterator<Page> iter = read(object, projectedColumns, batchSize);
        return rowLimit == NO_LIMIT ? iter : new LimitingIterator(iter, rowLimit);
    }

    default CloseableIterator<Page> readSplit(
        StorageObject object,
        List<String> projectedColumns,
        int batchSize,
        boolean skipFirstLine,
        List<Attribute> resolvedAttributes
    ) throws IOException {
        return read(object, projectedColumns, batchSize);
    }

    default CloseableIterator<Page> readSplit(
        StorageObject object,
        List<String> projectedColumns,
        int batchSize,
        boolean skipFirstLine,
        boolean lastSplit,
        List<Attribute> resolvedAttributes
    ) throws IOException {
        return readSplit(object, projectedColumns, batchSize, skipFirstLine, resolvedAttributes);
    }

    String formatName();

    List<String> fileExtensions();

    // === ASYNC API (optional - default wraps sync in executor) ===

    default void readAsync(
        StorageObject object,
        List<String> projectedColumns,
        int batchSize,
        Executor executor,
        ActionListener<CloseableIterator<Page>> listener
    ) {
        readAsync(object, projectedColumns, batchSize, NO_LIMIT, executor, listener);
    }

    default void readAsync(
        StorageObject object,
        List<String> projectedColumns,
        int batchSize,
        int rowLimit,
        Executor executor,
        ActionListener<CloseableIterator<Page>> listener
    ) {
        executor.execute(() -> {
            try {
                listener.onResponse(read(object, projectedColumns, batchSize, rowLimit));
            } catch (Exception e) {
                listener.onFailure(e);
            }
        });
    }

    default boolean supportsNativeAsync() {
        return false;
    }

    /**
     * Iterator wrapper that stops yielding pages once a cumulative row budget is exhausted.
     * Closes the delegate iterator when the budget is met or when explicitly closed.
     * When the last page would overshoot the budget, it is trimmed to the exact remaining count.
     */
    class LimitingIterator implements CloseableIterator<Page> {
        private final CloseableIterator<Page> delegate;
        private int remaining;

        LimitingIterator(CloseableIterator<Page> delegate, int rowLimit) {
            if (rowLimit <= 0) {
                throw new IllegalArgumentException("rowLimit must be positive, got: " + rowLimit);
            }
            this.delegate = delegate;
            this.remaining = rowLimit;
        }

        @Override
        public boolean hasNext() {
            if (remaining <= 0) {
                return false;
            }
            return delegate.hasNext();
        }

        @Override
        public Page next() {
            if (hasNext() == false) {
                throw new NoSuchElementException();
            }
            Page page = delegate.next();
            int rows = page.getPositionCount();
            if (rows > remaining) {
                page = truncate(page, remaining);
                remaining = 0;
            } else {
                remaining -= rows;
            }
            if (remaining <= 0) {
                try {
                    delegate.close();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
            return page;
        }

        @Override
        public void close() throws IOException {
            delegate.close();
        }

        private static Page truncate(Page page, int upTo) {
            int[] positions = new int[upTo];
            for (int i = 0; i < upTo; i++) {
                positions[i] = i;
            }
            return page.filter(false, positions);
        }
    }
}
