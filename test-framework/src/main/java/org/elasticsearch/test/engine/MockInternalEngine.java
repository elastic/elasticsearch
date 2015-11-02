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

import org.apache.lucene.index.FilterDirectoryReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.SearcherManager;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.index.engine.EngineConfig;
import org.elasticsearch.index.engine.EngineException;
import org.elasticsearch.index.engine.InternalEngine;

import java.io.IOException;

final class MockInternalEngine extends InternalEngine {
    private MockEngineSupport support;
    private final boolean randomizeFlushOnClose;
    private Class<? extends FilterDirectoryReader> wrapperClass;

    MockInternalEngine(EngineConfig config, boolean skipInitialTranslogRecovery, Class<? extends FilterDirectoryReader> wrapper) throws EngineException {
        super(config, skipInitialTranslogRecovery);
        randomizeFlushOnClose = IndexMetaData.isOnSharedFilesystem(config.getIndexSettings()) == false;
        wrapperClass = wrapper;

    }

    private synchronized MockEngineSupport support() {
        // lazy initialized since we need it already on super() ctor execution :(
        if (support == null) {
            support = new MockEngineSupport(config(), wrapperClass);
        }
        return support;
    }

    @Override
    public void close() throws IOException {
        switch (support().flushOrClose(this, MockEngineSupport.CloseAction.CLOSE)) {
            case FLUSH_AND_CLOSE:
                super.flushAndClose();
                break;
            case CLOSE:
                super.close();
                break;
        }
    }

    @Override
    public void flushAndClose() throws IOException {
        if (randomizeFlushOnClose) {
            switch (support().flushOrClose(this, MockEngineSupport.CloseAction.FLUSH_AND_CLOSE)) {
                case FLUSH_AND_CLOSE:
                    super.flushAndClose();
                    break;
                case CLOSE:
                    super.close();
                    break;
            }
        } else {
            super.flushAndClose();
        }
    }

    @Override
    protected Searcher newSearcher(String source, IndexSearcher searcher, SearcherManager manager) throws EngineException {
        final Searcher engineSearcher = super.newSearcher(source, searcher, manager);
        return support().wrapSearcher(source, engineSearcher, searcher, manager);
    }
}
