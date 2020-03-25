/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.index.engine;

import java.io.IOException;
import java.util.function.BiConsumer;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.search.ReferenceManager;

import org.apache.lucene.search.SearcherManager;
import org.elasticsearch.common.SuppressForbidden;
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
