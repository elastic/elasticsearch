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
import org.elasticsearch.index.engine.InternalEngine;

import java.io.IOException;

final class MockInternalEngine extends InternalEngine {
    private MockEngineSupport support;

    MockInternalEngine(EngineConfig config, boolean skipInitialTranslogRecovery) throws EngineException {
        super(config, skipInitialTranslogRecovery);
    }

    private synchronized MockEngineSupport support() {
        // lazy initialized since we need it already on super() ctor execution :(
        if (support == null) {
            support = new MockEngineSupport(config());
        }
        return support;
    }

    @Override
    public void close() throws IOException {
        switch(support().flushOrClose(this, MockEngineSupport.CloseAction.CLOSE)) {
            case FLUSH_AND_CLOSE:
                super.flushAndClose();
                break;
            case CLOSE:
                super.close();
                break;
        }
        logger.debug("Ongoing recoveries after engine close: " + onGoingRecoveries.get());

    }

    @Override
    public void flushAndClose() throws IOException {
        switch(support().flushOrClose(this, MockEngineSupport.CloseAction.FLUSH_AND_CLOSE)) {
            case FLUSH_AND_CLOSE:
                super.flushAndClose();
                break;
            case CLOSE:
                super.close();
                break;
        }
        logger.debug("Ongoing recoveries after engine close: " + onGoingRecoveries.get());
    }

    @Override
    protected Searcher newSearcher(String source, IndexSearcher searcher, SearcherManager manager) throws EngineException {
        final AssertingIndexSearcher assertingIndexSearcher = support().newSearcher(this, source, searcher, manager);
        assertingIndexSearcher.setSimilarity(searcher.getSimilarity());
        // pass the original searcher to the super.newSearcher() method to make sure this is the searcher that will
        // be released later on. If we wrap an index reader here must not pass the wrapped version to the manager
        // on release otherwise the reader will be closed too early. - good news, stuff will fail all over the place if we don't get this right here
        return new AssertingSearcher(assertingIndexSearcher,
                super.newSearcher(source, searcher, manager), shardId, MockEngineSupport.INFLIGHT_ENGINE_SEARCHERS, logger);
    }
}
