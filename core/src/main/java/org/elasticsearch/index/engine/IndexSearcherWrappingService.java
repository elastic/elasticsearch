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
import org.apache.lucene.index.FilterDirectoryReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.search.IndexSearcher;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.lucene.index.ElasticsearchDirectoryReader;
import org.elasticsearch.index.engine.Engine.Searcher;

import java.io.IOException;
import java.util.Set;

/**
 * Service responsible for wrapping the {@link DirectoryReader} and {@link IndexSearcher} of a {@link Searcher} via the
 * configured {@link IndexSearcherWrapper} instance. This allows custom functionally to be added the {@link Searcher}
 * before being used to do an operation (search, get, field stats etc.)
 */
// TODO: This needs extension point is a bit hacky now, because the IndexSearch from the engine can only be wrapped once,
// if we allowed the IndexSearcher to be wrapped multiple times then a custom IndexSearcherWrapper needs have good
// control over its location in the wrapping chain
public final class IndexSearcherWrappingService {

    private final IndexSearcherWrapper wrapper;

    // for unit tests:
    IndexSearcherWrappingService() {
        this.wrapper = null;
    }

    @Inject
    // Use a Set parameter here, because constructor parameter can't be optional
    // and I prefer to keep the `wrapper` field final.
    public IndexSearcherWrappingService(Set<IndexSearcherWrapper> wrappers) {
        if (wrappers.size() > 1) {
            throw new IllegalStateException("wrapping of the index searcher by more than one wrappers is forbidden, found the following wrappers [" + wrappers + "]");
        }
        if (wrappers.isEmpty()) {
            this.wrapper = null;
        } else {
            this.wrapper = wrappers.iterator().next();
        }
    }

    /**
     * If there are configured {@link IndexSearcherWrapper} instances, the {@link IndexSearcher} of the provided engine searcher
     * gets wrapped and a new {@link Engine.Searcher} instances is returned, otherwise the provided {@link Engine.Searcher} is returned.
     *
     * This is invoked each time a {@link Engine.Searcher} is requested to do an operation. (for example search)
     */
    public final Engine.Searcher wrap(EngineConfig engineConfig, final Engine.Searcher engineSearcher) throws IOException {
        final ElasticsearchDirectoryReader elasticsearchDirectoryReader = ElasticsearchDirectoryReader.getElasticsearchDirectoryReader(engineSearcher.getDirectoryReader());
        if (elasticsearchDirectoryReader == null) {
            throw new IllegalStateException("Can't wrap non elasticsearch directory reader");
        }
        if (wrapper == null) {
            return engineSearcher;
        }
        NonClosingReaderWrapper nonClosingReaderWrapper = new NonClosingReaderWrapper(engineSearcher.getDirectoryReader());
        DirectoryReader reader = wrapper.wrap(nonClosingReaderWrapper);
        if (reader != nonClosingReaderWrapper) {
            if (reader.getCoreCacheKey() != elasticsearchDirectoryReader.getCoreCacheKey()) {
                throw new IllegalStateException("wrapped directory reader doesn't delegate IndexReader#getCoreCacheKey, wrappers must override this method and delegate" +
                        " to the original readers core cache key. Wrapped readers can't be used as cache keys since their are used only per request which would lead to subtle bugs");
            }
            if (ElasticsearchDirectoryReader.getElasticsearchDirectoryReader(reader) != elasticsearchDirectoryReader) {
                // prevent that somebody wraps with a non-filter reader
                throw new IllegalStateException("wrapped directory reader hides actual ElasticsearchDirectoryReader but shouldn't");
            }
        }

        final IndexSearcher innerIndexSearcher = new IndexSearcher(reader);
        innerIndexSearcher.setQueryCache(engineConfig.getQueryCache());
        innerIndexSearcher.setQueryCachingPolicy(engineConfig.getQueryCachingPolicy());
        innerIndexSearcher.setSimilarity(engineConfig.getSimilarity());
        // TODO: Right now IndexSearcher isn't wrapper friendly, when it becomes wrapper friendly we should revise this extension point
        // For example if IndexSearcher#rewrite() is overwritten than also IndexSearcher#createNormalizedWeight needs to be overwritten
        // This needs to be fixed before we can allow the IndexSearcher from Engine to be wrapped multiple times
        final IndexSearcher indexSearcher = wrapper.wrap(engineConfig, innerIndexSearcher);
        if (reader == nonClosingReaderWrapper && indexSearcher == innerIndexSearcher) {
            return engineSearcher;
        } else {
            return new Engine.Searcher(engineSearcher.source(), indexSearcher) {
                @Override
                public void close() throws ElasticsearchException {
                    try {
                        reader().close();
                        // we close the reader to make sure wrappers can release resources if needed....
                        // our NonClosingReaderWrapper makes sure that our reader is not closed
                    } catch (IOException e) {
                        throw new ElasticsearchException("failed to close reader", e);
                    } finally {
                        engineSearcher.close();
                    }

                }
            };
        }
    }

    private static final class NonClosingReaderWrapper extends FilterDirectoryReader {

        private NonClosingReaderWrapper(DirectoryReader in) throws IOException {
            super(in, new SubReaderWrapper() {
                @Override
                public LeafReader wrap(LeafReader reader) {
                    return reader;
                }
            });
        }

        @Override
        protected DirectoryReader doWrapDirectoryReader(DirectoryReader in) throws IOException {
            return new NonClosingReaderWrapper(in);
        }

        @Override
        protected void doClose() throws IOException {
            // don't close here - mimic the MultiReader#doClose = false behavior that FilterDirectoryReader doesn't have
        }

        @Override
        public Object getCoreCacheKey() {
            return in.getCoreCacheKey();
        }
    }


}
