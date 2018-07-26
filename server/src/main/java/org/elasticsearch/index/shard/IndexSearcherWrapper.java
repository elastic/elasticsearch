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

package org.elasticsearch.index.shard;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FilterDirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.search.IndexSearcher;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.lucene.index.ElasticsearchDirectoryReader;
import org.elasticsearch.index.engine.Engine;

import java.io.IOException;

/**
 * Extension point to add custom functionality at request time to the {@link DirectoryReader}
 * and {@link IndexSearcher} managed by the {@link IndexShard}.
 */
public class IndexSearcherWrapper {

    /**
     * Wraps the given {@link DirectoryReader}. The wrapped reader can filter out document just like delete documents etc. but
     * must not change any term or document content.
     * <p>
     * NOTE: The wrapper has a per-request lifecycle, must delegate {@link IndexReader#getReaderCacheHelper()},
     * {@link LeafReader#getCoreCacheHelper()} and must be an instance of {@link FilterDirectoryReader} that
     * eventually exposes the original reader via  {@link FilterDirectoryReader#getDelegate()}.
     * The returned reader is closed once it goes out of scope.
     * </p>
     * @param reader The provided directory reader to be wrapped to add custom functionality
     * @return a new directory reader wrapping the provided directory reader or if no wrapping was performed
     *         the provided directory reader
     */
    protected DirectoryReader wrap(DirectoryReader reader) throws IOException {
        return reader;
    }

    /**
     * @param searcher      The provided index searcher to be wrapped to add custom functionality
     * @return a new index searcher wrapping the provided index searcher or if no wrapping was performed
     *         the provided index searcher
     */
    protected IndexSearcher wrap(IndexSearcher searcher) throws IOException {
        return searcher;
    }
    /**
     * If there are configured {@link IndexSearcherWrapper} instances, the {@link IndexSearcher} of the provided engine searcher
     * gets wrapped and a new {@link Engine.Searcher} instances is returned, otherwise the provided {@link Engine.Searcher} is returned.
     *
     * This is invoked each time a {@link Engine.Searcher} is requested to do an operation. (for example search)
     */
    public final Engine.Searcher wrap(Engine.Searcher engineSearcher) throws IOException {
        final ElasticsearchDirectoryReader elasticsearchDirectoryReader = ElasticsearchDirectoryReader.getElasticsearchDirectoryReader(engineSearcher.getDirectoryReader());
        if (elasticsearchDirectoryReader == null) {
            throw new IllegalStateException("Can't wrap non elasticsearch directory reader");
        }
        NonClosingReaderWrapper nonClosingReaderWrapper = new NonClosingReaderWrapper(engineSearcher.getDirectoryReader());
        DirectoryReader reader = wrap(nonClosingReaderWrapper);
        if (reader != nonClosingReaderWrapper) {
            if (reader.getReaderCacheHelper() != elasticsearchDirectoryReader.getReaderCacheHelper()) {
                throw new IllegalStateException("wrapped directory reader doesn't delegate IndexReader#getCoreCacheKey, wrappers must override this method and delegate" +
                        " to the original readers core cache key. Wrapped readers can't be used as cache keys since their are used only per request which would lead to subtle bugs");
            }
            if (ElasticsearchDirectoryReader.getElasticsearchDirectoryReader(reader) != elasticsearchDirectoryReader) {
                // prevent that somebody wraps with a non-filter reader
                throw new IllegalStateException("wrapped directory reader hides actual ElasticsearchDirectoryReader but shouldn't");
            }
        }

        final IndexSearcher origIndexSearcher = engineSearcher.searcher();
        final IndexSearcher innerIndexSearcher = new IndexSearcher(reader);
        innerIndexSearcher.setQueryCache(origIndexSearcher.getQueryCache());
        innerIndexSearcher.setQueryCachingPolicy(origIndexSearcher.getQueryCachingPolicy());
        innerIndexSearcher.setSimilarity(origIndexSearcher.getSimilarity(true));
        // TODO: Right now IndexSearcher isn't wrapper friendly, when it becomes wrapper friendly we should revise this extension point
        // For example if IndexSearcher#rewrite() is overwritten than also IndexSearcher#createNormalizedWeight needs to be overwritten
        // This needs to be fixed before we can allow the IndexSearcher from Engine to be wrapped multiple times
        final IndexSearcher indexSearcher = wrap(innerIndexSearcher);
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
        public CacheHelper getReaderCacheHelper() {
            return in.getReaderCacheHelper();
        }

    }

}
