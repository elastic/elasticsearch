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
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.IndexSearcher;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.index.engine.Engine.Searcher;
import org.elasticsearch.search.internal.ContextIndexSearcher;

import java.util.Set;

public class DefaultCreateContextIndexSearcherService implements CreateContextIndexSearcherService {

    private final IndexSearcherWrapper wrapper;

    // for unit tests:
    DefaultCreateContextIndexSearcherService() {
        this.wrapper = null;
    }

    @Inject
    // Use a Set parameter here, because constructor parameter can't be optional
    // and I prefer to keep the `wrapper` field final.
    public DefaultCreateContextIndexSearcherService(Set<IndexSearcherWrapper> wrappers) {
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
     * gets wrapped and a new {@link Searcher} instances is returned, otherwise the provided {@link Searcher} is returned.
     *
     * This is invoked each time a {@link Searcher} is requested to do an operation. (for example search)
     */
    public Searcher wrap(EngineConfig engineConfig, final Searcher originalEngineSearcher) throws EngineException {
        if (wrapper == null) {
            ContextIndexSearcher indexSearcher = createContextIndexSearcher(engineConfig, originalEngineSearcher, originalEngineSearcher.reader());
            return wrapEngineSearcher(engineConfig, indexSearcher, originalEngineSearcher);
        }

        DirectoryReader reader = wrapper.wrap((DirectoryReader) originalEngineSearcher.reader());
        ContextIndexSearcher innerIndexSearcher = createContextIndexSearcher(engineConfig, originalEngineSearcher, reader);
        // TODO: Right now IndexSearcher isn't wrapper friendly, when it becomes wrapper friendly we should revise this extension point
        // For example if IndexSearcher#rewrite() is overwritten than also IndexSearcher#createNormalizedWeight needs to be overwritten
        // This needs to be fixed before we can allow the IndexSearcher from Engine to be wrapped multiple times
        ContextIndexSearcher indexSearcher = wrapper.wrap(innerIndexSearcher, engineConfig);
        return wrapEngineSearcher(engineConfig, indexSearcher, originalEngineSearcher);
    }

    protected Searcher wrapEngineSearcher(EngineConfig engineConfig, ContextIndexSearcher indexSearcher, final Searcher originalEngineSearcher) {
        return new Searcher(originalEngineSearcher.source(), indexSearcher) {

            @Override
            public void close() throws ElasticsearchException {
                originalEngineSearcher.close();
            }
        };
    }

    private ContextIndexSearcher createContextIndexSearcher(EngineConfig engineConfig, Searcher engineSearcher, IndexReader reader) {
        return new ContextIndexSearcher(
                reader, engineSearcher.searcher().getSimilarity(true),
                engineConfig.getQueryCache(), engineConfig.getQueryCachingPolicy()
        );
    }

}
