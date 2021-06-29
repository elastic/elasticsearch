/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.engine;

import java.io.IOException;
import java.util.function.BiConsumer;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.search.ReferenceManager;

import org.apache.lucene.search.SearcherManager;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.common.lucene.index.ElasticsearchDirectoryReader;

/**
 * Utility class to safely share {@link ElasticsearchDirectoryReader} instances across
 * multiple threads, while periodically reopening. This class ensures each
 * reader is closed only once all threads have finished using it.
 *
 * @see SearcherManager
 *
 */
@SuppressForbidden(reason = "reference counting is required here")
class ElasticsearchReaderManager extends ReferenceManager<ElasticsearchDirectoryReader> {
    private final BiConsumer<ElasticsearchDirectoryReader, ElasticsearchDirectoryReader> refreshListener;

    /**
     * Creates and returns a new ElasticsearchReaderManager from the given
     * already-opened {@link ElasticsearchDirectoryReader}, stealing
     * the incoming reference.
     *
     * @param reader            the directoryReader to use for future reopens
     * @param refreshListener   A consumer that is called every time a new reader is opened
     */
    ElasticsearchReaderManager(ElasticsearchDirectoryReader reader,
                               BiConsumer<ElasticsearchDirectoryReader, ElasticsearchDirectoryReader> refreshListener) {
        this.current = reader;
        this.refreshListener = refreshListener;
        refreshListener.accept(current, null);
    }

    @Override
    protected void decRef(ElasticsearchDirectoryReader reference) throws IOException {
        reference.decRef();
    }

    @Override
    protected ElasticsearchDirectoryReader refreshIfNeeded(ElasticsearchDirectoryReader referenceToRefresh) throws IOException {
        final ElasticsearchDirectoryReader reader = (ElasticsearchDirectoryReader) DirectoryReader.openIfChanged(referenceToRefresh);
        if (reader != null) {
            refreshListener.accept(reader, referenceToRefresh);
        }
        return reader;
    }

    @Override
    protected boolean tryIncRef(ElasticsearchDirectoryReader reference) {
        return reference.tryIncRef();
    }

    @Override
    protected int getRefCount(ElasticsearchDirectoryReader reference) {
        return reference.getRefCount();
    }
}
