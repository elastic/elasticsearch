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

package org.elasticsearch.test.engine;

import org.apache.lucene.search.AssertingIndexSearcher;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.SearcherManager;
import org.elasticsearch.index.engine.EngineConfig;
import org.elasticsearch.index.engine.EngineException;
import org.elasticsearch.index.engine.ShadowEngine;

import java.io.IOException;
import java.util.Map;

final class MockShadowEngine extends ShadowEngine {
    private final MockEngineSupport support;

    MockShadowEngine(EngineConfig config) {
        super(config);
        this.support = new MockEngineSupport(config);
    }

    @Override
    public void close() throws IOException {
        try {
            super.close();
        } finally {
            if (logger.isTraceEnabled()) {
                // log debug if we have pending searchers
                for (Map.Entry<AssertingSearcher, RuntimeException> entry : MockEngineSupport.INFLIGHT_ENGINE_SEARCHERS.entrySet()) {
                    logger.trace("Unreleased Searchers instance for shard [{}]", entry.getValue(), entry.getKey().shardId());
                }
            }
        }
    }

    @Override
    protected Searcher newSearcher(String source, IndexSearcher searcher, SearcherManager manager) throws EngineException {
        final AssertingIndexSearcher assertingIndexSearcher = support.newSearcher(this, source, searcher, manager);
        assertingIndexSearcher.setSimilarity(searcher.getSimilarity());
        // pass the original searcher to the super.newSearcher() method to make sure this is the searcher that will
        // be released later on. If we wrap an index reader here must not pass the wrapped version to the manager
        // on release otherwise the reader will be closed too early. - good news, stuff will fail all over the place if we don't get this right here
        return new AssertingSearcher(assertingIndexSearcher,
                super.newSearcher(source, searcher, manager), shardId, MockEngineSupport.INFLIGHT_ENGINE_SEARCHERS, logger);
    }

}
