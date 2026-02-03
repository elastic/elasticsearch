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

    // === SYNC API (required - implement this for simple formats) ===

    SourceMetadata metadata(StorageObject object) throws IOException;

    default List<Attribute> schema(StorageObject object) throws IOException {
        return metadata(object).schema();
    }

    CloseableIterator<Page> read(StorageObject object, List<String> projectedColumns, int batchSize) throws IOException;

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
        executor.execute(() -> {
            try {
                listener.onResponse(read(object, projectedColumns, batchSize));
            } catch (Exception e) {
                listener.onFailure(e);
            }
        });
    }

    default boolean supportsNativeAsync() {
        return false;
    }
}
