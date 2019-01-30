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

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.store.Directory;

import java.io.IOException;
import java.util.List;
import java.util.function.Function;

/**
 * NoOpEngine is an engine implementation that does nothing but the bare minimum
 * required in order to have an engine. All attempts to do something (search,
 * index, get), throw {@link UnsupportedOperationException}.
 */
public final class NoOpEngine extends ReadOnlyEngine {

    public NoOpEngine(EngineConfig config) {
        super(config, null, null, true, Function.identity());
    }

    @Override
    protected DirectoryReader open(final IndexCommit commit) throws IOException {
        final Directory directory = commit.getDirectory();
        final List<IndexCommit> indexCommits = DirectoryReader.listCommits(directory);
        final IndexCommit indexCommit = indexCommits.get(indexCommits.size() - 1);
        return new DirectoryReader(directory, new LeafReader[0]) {
            @Override
            protected DirectoryReader doOpenIfChanged() throws IOException {
                return null;
            }

            @Override
            protected DirectoryReader doOpenIfChanged(IndexCommit commit) throws IOException {
                return null;
            }

            @Override
            protected DirectoryReader doOpenIfChanged(IndexWriter writer, boolean applyAllDeletes) throws IOException {
                return null;
            }

            @Override
            public long getVersion() {
                return 0;
            }

            @Override
            public boolean isCurrent() throws IOException {
                return true;
            }

            @Override
            public IndexCommit getIndexCommit() throws IOException {
                return indexCommit;
            }

            @Override
            protected void doClose() throws IOException {
            }

            @Override
            public CacheHelper getReaderCacheHelper() {
                return null;
            }
        };
    }
}
