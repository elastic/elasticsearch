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
import org.apache.lucene.index.FilterDirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.AssertingIndexSearcher;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.SearcherManager;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.EngineConfig;
import org.elasticsearch.index.engine.EngineException;
import org.elasticsearch.index.engine.InternalEngine;
import org.elasticsearch.test.ElasticsearchIntegrationTest;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Support class to build MockEngines like {@link org.elasticsearch.test.engine.MockInternalEngine} or {@link org.elasticsearch.test.engine.MockShadowEngine}
 * since they need to subclass the actual engine
 */
public final class MockEngineSupport {

    public static final String WRAP_READER_RATIO = "index.engine.mock.random.wrap_reader_ratio";
    public static final String READER_WRAPPER_TYPE = "index.engine.mock.random.wrapper";
    public static final String FLUSH_ON_CLOSE_RATIO = "index.engine.mock.flush_on_close.ratio";
    private final AtomicBoolean closing = new AtomicBoolean(false);
    private final ESLogger logger = Loggers.getLogger(Engine.class);

    public static class MockContext {
        public final Random random;
        public final boolean wrapReader;
        public final Class<? extends FilterDirectoryReader> wrapper;
        public final Settings indexSettings;
        private final double flushOnClose;

        public MockContext(Random random, boolean wrapReader, Class<? extends FilterDirectoryReader> wrapper, Settings indexSettings) {
            this.random = random;
            this.wrapReader = wrapReader;
            this.wrapper = wrapper;
            this.indexSettings = indexSettings;
            flushOnClose = indexSettings.getAsDouble(FLUSH_ON_CLOSE_RATIO, 0.5d);
        }
    }

    public static final ConcurrentMap<AssertingSearcher, RuntimeException> INFLIGHT_ENGINE_SEARCHERS = new ConcurrentHashMap<>();

    private final MockContext mockContext;

    public MockEngineSupport(EngineConfig config) {
        Settings indexSettings = config.getIndexSettings();
        final long seed = indexSettings.getAsLong(ElasticsearchIntegrationTest.SETTING_INDEX_SEED, 0l);
        Random random = new Random(seed);
        final double ratio = indexSettings.getAsDouble(WRAP_READER_RATIO, 0.0d); // DISABLED by default - AssertingDR is crazy slow
        Class<? extends AssertingDirectoryReader> wrapper = indexSettings.getAsClass(READER_WRAPPER_TYPE, AssertingDirectoryReader.class);
        boolean wrapReader = random.nextDouble() < ratio;
        if (logger.isTraceEnabled()) {
            logger.trace("Using [{}] for shard [{}] seed: [{}] wrapReader: [{}]", this.getClass().getName(), config.getShardId(), seed, wrapReader);
        }
        mockContext = new MockContext(random, wrapReader, wrapper, indexSettings);
    }

    enum CloseAction {
        FLUSH_AND_CLOSE,
        CLOSE;
    }


    /**
     * Returns the CloseAction to execute on the actual engine. Note this method changes the state on
     * the first call and treats subsequent calls as if the engine passed is already closed.
     */
    public CloseAction flushOrClose(Engine engine, CloseAction originalAction) throws IOException {
        try {
            if (closing.compareAndSet(false, true)) { // only do the random thing if we are the first call to this since super.flushOnClose() calls #close() again and then we might end up with a stackoverflow.
                if (mockContext.flushOnClose > mockContext.random.nextDouble()) {
                    return CloseAction.FLUSH_AND_CLOSE;
                } else {
                    return CloseAction.CLOSE;
                }
            } else {
                return originalAction;
            }
        } finally {
            if (logger.isTraceEnabled()) {
                // log debug if we have pending searchers
                for (Map.Entry<AssertingSearcher, RuntimeException> entry : INFLIGHT_ENGINE_SEARCHERS.entrySet()) {
                    logger.trace("Unreleased Searchers instance for shard [{}]",
                            entry.getValue(), entry.getKey().shardId());
                }
            }
        }
    }

    public AssertingIndexSearcher newSearcher(Engine engine, String source, IndexSearcher searcher, SearcherManager manager) throws EngineException {
        IndexReader reader = searcher.getIndexReader();
        IndexReader wrappedReader = reader;
        assert reader != null;
        if (reader instanceof DirectoryReader && mockContext.wrapReader) {
            wrappedReader = wrapReader((DirectoryReader) reader, engine);
        }
        // this executes basic query checks and asserts that weights are normalized only once etc.
        final AssertingIndexSearcher assertingIndexSearcher = new AssertingIndexSearcher(mockContext.random, wrappedReader);
        assertingIndexSearcher.setSimilarity(searcher.getSimilarity());
        return assertingIndexSearcher;
    }

    private DirectoryReader wrapReader(DirectoryReader reader, Engine engine) {
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

    public static abstract class DirectoryReaderWrapper extends FilterDirectoryReader {
        protected final SubReaderWrapper subReaderWrapper;

        public DirectoryReaderWrapper(DirectoryReader in, SubReaderWrapper subReaderWrapper) throws IOException {
            super(in, subReaderWrapper);
            this.subReaderWrapper = subReaderWrapper;
        }

        @Override
        public Object getCoreCacheKey() {
            return in.getCoreCacheKey();
        }

        @Override
        public Object getCombinedCoreAndDeletesKey() {
            return in.getCombinedCoreAndDeletesKey();
        }

    }

}
