/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.CloseableIterator;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.core.expression.Attribute;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.Executor;

/**
 * Runtime interface for reading one data format.
 * <p>
 * Simple formats implement {@link #read(StorageObject, FormatReadContext)} synchronously;
 * async wrapping is automatic. Async-capable formats override
 * {@link #readAsync(StorageObject, FormatReadContext, Executor, ActionListener)}.
 * <p>
 * Every instance comes from {@link FormatReaderFactory#create} and is owned by that caller.
 * The caller closes it after its metadata or read operation, after closing any iterator
 * created from it. A reader releases only resources acquired by that created instance.
 */
public interface FormatReader extends Closeable {

    int NO_LIMIT = -1;

    /** Strategy for resolving schemas across multiple files in a multi-file query. */
    enum SchemaResolution {
        /** Use the schema from the first file; ignore differences in subsequent files. */
        FIRST_FILE_WINS,
        /** Require all files to share the exact same schema, modulo nullability. */
        STRICT,
        /** Merge schemas from all files by column name, with safe type widening. */
        UNION_BY_NAME;

        /**
         * Parses a case-insensitive {@code schema_resolution} value.
         *
         * @throws IllegalArgumentException if {@code value} is not a recognized strategy
         */
        public static SchemaResolution parse(String value) {
            return switch (value.toLowerCase(Locale.ROOT)) {
                case "first_file_wins" -> FIRST_FILE_WINS;
                case "strict" -> STRICT;
                case "union_by_name" -> UNION_BY_NAME;
                default -> throw new IllegalArgumentException(
                    "Unknown schema_resolution value [" + value + "]. Valid values are: first_file_wins, strict, union_by_name"
                );
            };
        }
    }

    /** Cluster-wide default schema resolution strategy when a query does not specify one. */
    SchemaResolution DEFAULT_SCHEMA_RESOLUTION = SchemaResolution.UNION_BY_NAME;

    SourceMetadata metadata(StorageObject object) throws IOException;

    /**
     * Asynchronously resolves metadata for the given storage object.
     * Formats with native asynchronous metadata access should override this method.
     */
    default void metadataAsync(StorageObject object, Executor executor, ActionListener<SourceMetadata> listener) {
        executor.execute(() -> {
            final SourceMetadata metadata;
            try {
                metadata = metadata(object);
            } catch (Exception e) {
                listener.onFailure(e);
                return;
            }
            listener.onResponse(metadata);
        });
    }

    default List<Attribute> schema(StorageObject object) throws IOException {
        return metadata(object).schema();
    }

    /** Reads the object using the supplied per-read execution context. */
    CloseableIterator<Page> read(StorageObject object, FormatReadContext context) throws IOException;

    /** Convenience overload for callers that only need projection and batch-size settings. */
    default CloseableIterator<Page> read(StorageObject object, List<String> projectedColumns, int batchSize) throws IOException {
        return read(object, FormatReadContext.of(projectedColumns, batchSize));
    }

    /**
     * Asynchronously reads the object.
     * Formats with native asynchronous reads should override this method.
     */
    default void readAsync(
        StorageObject object,
        FormatReadContext context,
        Executor executor,
        ActionListener<CloseableIterator<Page>> listener
    ) {
        executor.execute(() -> {
            final CloseableIterator<Page> iterator;
            try {
                iterator = read(object, context);
            } catch (Exception e) {
                listener.onFailure(e);
                return;
            }
            listener.onResponse(iterator);
        });
    }

    /**
     * Declares how the dispatcher supplies the {@code _rowPosition} column.
     */
    RowPositionStrategy rowPositionStrategy();

    /**
     * Per-reader counters for operator status. {@code null} when the reader does not publish any.
     */
    @Nullable
    default FormatReaderStatus statusSnapshot() {
        return null;
    }
}
