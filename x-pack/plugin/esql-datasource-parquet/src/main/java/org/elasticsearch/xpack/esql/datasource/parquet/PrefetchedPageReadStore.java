/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.parquet;

import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.page.DictionaryPage;
import org.apache.parquet.column.page.DictionaryPageReadStore;
import org.apache.parquet.column.page.PageReadStore;

import java.util.Map;

/**
 * In-memory {@link PageReadStore} backed by per-column {@link PrefetchedPageReader}s. Replaces
 * parquet-mr's {@code ColumnChunkPageReadStore} on the optimized iterator's read path; the
 * underlying buffers are heap slices of the prefetched chunks held by the iterator and are
 * released when the iterator drops the chunks.
 */
final class PrefetchedPageReadStore implements PageReadStore, DictionaryPageReadStore {

    private final Map<ColumnDescriptor, PrefetchedPageReader> readers;
    private final long rowCount;

    PrefetchedPageReadStore(Map<ColumnDescriptor, PrefetchedPageReader> readers, long rowCount) {
        this.readers = Map.copyOf(readers);
        this.rowCount = rowCount;
    }

    @Override
    public long getRowCount() {
        return rowCount;
    }

    @Override
    public PrefetchedPageReader getPageReader(ColumnDescriptor descriptor) {
        return readers.get(descriptor);
    }

    @Override
    public DictionaryPage readDictionaryPage(ColumnDescriptor descriptor) {
        PrefetchedPageReader reader = readers.get(descriptor);
        return reader == null ? null : reader.readDictionaryPage();
    }

    @Override
    public void close() {}
}
