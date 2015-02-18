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

import org.apache.lucene.index.AssertingDirectoryReader;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.AssertingIndexSearcher;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.SearcherManager;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.engine.EngineConfig;
import org.elasticsearch.index.engine.EngineException;
import org.elasticsearch.index.engine.ShadowEngine;
import org.elasticsearch.test.ElasticsearchIntegrationTest;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class MockShadowEngine extends ShadowEngine {

    private final MockInternalEngine.MockContext mockContext;
    public static final ConcurrentMap<AssertingSearcher, RuntimeException> INFLIGHT_ENGINE_SEARCHERS = new ConcurrentHashMap<>();

    public MockShadowEngine(EngineConfig config) {
        super(config);
        Settings indexSettings = config.getIndexSettings();
        final long seed = indexSettings.getAsLong(ElasticsearchIntegrationTest.SETTING_INDEX_SEED, 0l);
        Random random = new Random(seed);
        final double ratio = indexSettings.getAsDouble(MockInternalEngine.WRAP_READER_RATIO, 0.0d); // DISABLED by default - AssertingDR is crazy slow
        Class<? extends AssertingDirectoryReader> wrapper = indexSettings.getAsClass(MockInternalEngine.READER_WRAPPER_TYPE, AssertingDirectoryReader.class);
        boolean wrapReader = random.nextDouble() < ratio;
        logger.trace("Using [{}] for shard [{}] seed: [{}] wrapReader: [{}]", this.getClass().getName(), shardId, seed, wrapReader);
        mockContext = new MockInternalEngine.MockContext(random, wrapReader, wrapper, indexSettings);
    }


    @Override
    public void close() throws IOException {
        try {
            super.close();
        } finally {
            if (logger.isTraceEnabled()) {
                // log debug if we have pending searchers
                for (Map.Entry<AssertingSearcher, RuntimeException> entry : INFLIGHT_ENGINE_SEARCHERS.entrySet()) {
                    logger.trace("Unreleased Searchers instance for shard [{}]", entry.getValue(), entry.getKey().shardId());
                }
            }
        }
    }

    @Override
    protected Searcher newSearcher(String source, IndexSearcher searcher, SearcherManager manager) throws EngineException {

        IndexReader reader = searcher.getIndexReader();
        IndexReader wrappedReader = reader;
        if (reader instanceof DirectoryReader && mockContext.wrapReader) {
            wrappedReader = wrapReader((DirectoryReader) reader);
        }
        // this executes basic query checks and asserts that weights are normalized only once etc.
        final AssertingIndexSearcher assertingIndexSearcher = new AssertingIndexSearcher(mockContext.random, wrappedReader);
        assertingIndexSearcher.setSimilarity(searcher.getSimilarity());
        // pass the original searcher to the super.newSearcher() method to make
        // sure this is the searcher that will be released later on. If we wrap
        // an index reader here must not pass the wrapped version to the manager
        // on release otherwise the reader will be closed too early. - good
        // news, stuff will fail all over the place if we don't get this
        // right here
        return new AssertingSearcher(assertingIndexSearcher,
                super.newSearcher(source, searcher, manager), shardId,
                INFLIGHT_ENGINE_SEARCHERS, logger);
    }

    private DirectoryReader wrapReader(DirectoryReader reader) {
        try {
            Constructor<?>[] constructors = mockContext.wrapper.getConstructors();
            Constructor<?> nonRandom = null;
            for (Constructor<?> constructor : constructors) {
                Class<?>[] parameterTypes = constructor.getParameterTypes();
                if (parameterTypes.length > 0 && parameterTypes[0] == DirectoryReader.class) {
                    if (parameterTypes.length == 1) {
                        nonRandom = constructor;
                    } else if (parameterTypes.length == 2 && parameterTypes[1] == Settings.class) {

                        return (DirectoryReader) constructor.newInstance(reader, mockContext.indexSettings);
                    }
                }
            }
            if (nonRandom != null) {
                return (DirectoryReader) nonRandom.newInstance(reader);
            }
        } catch (Exception e) {
            throw new ElasticsearchException("Can not wrap reader", e);
        }
        return reader;
    }

}
