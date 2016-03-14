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

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FilterDirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.AssertingIndexSearcher;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.QueryCache;
import org.apache.lucene.search.QueryCachingPolicy;
import org.apache.lucene.search.SearcherManager;
import org.apache.lucene.util.LuceneTestCase;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.EngineConfig;
import org.elasticsearch.index.engine.EngineException;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESIntegTestCase;

import java.io.Closeable;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.IdentityHashMap;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Support class to build MockEngines like {@link org.elasticsearch.test.engine.MockInternalEngine} or {@link org.elasticsearch.test.engine.MockShadowEngine}
 * since they need to subclass the actual engine
 */
public final class MockEngineSupport {

    /**
     * Allows tests to wrap an index reader randomly with a given ratio. This is disabled by default ie. <tt>0.0d</tt> since reader wrapping is insanely
     * slow if {@link org.apache.lucene.index.AssertingDirectoryReader} is used.
     */
    public static final Setting<Double> WRAP_READER_RATIO =
        Setting.doubleSetting("index.engine.mock.random.wrap_reader_ratio", 0.0d, 0.0d, Property.IndexScope);
    /**
     * Allows tests to prevent an engine from being flushed on close ie. to test translog recovery...
     */
    public static final Setting<Boolean> DISABLE_FLUSH_ON_CLOSE =
        Setting.boolSetting("index.mock.disable_flush_on_close", false, Property.IndexScope);


    private final AtomicBoolean closing = new AtomicBoolean(false);
    private final ESLogger logger = Loggers.getLogger(Engine.class);
    private final ShardId shardId;
    private final QueryCache filterCache;
    private final QueryCachingPolicy filterCachingPolicy;
    private final SearcherCloseable searcherCloseable;
    private final MockContext mockContext;
    private final boolean disableFlushOnClose;

    public boolean isFlushOnCloseDisabled() {
        return disableFlushOnClose;
    }


    public static class MockContext {
        private final Random random;
        private final boolean wrapReader;
        private final Class<? extends FilterDirectoryReader> wrapper;
        private final Settings indexSettings;

        public MockContext(Random random, boolean wrapReader, Class<? extends FilterDirectoryReader> wrapper, Settings indexSettings) {
            this.random = random;
            this.wrapReader = wrapReader;
            this.wrapper = wrapper;
            this.indexSettings = indexSettings;
        }
    }

    public MockEngineSupport(EngineConfig config, Class<? extends FilterDirectoryReader> wrapper) {
        Settings settings = config.getIndexSettings().getSettings();
        shardId = config.getShardId();
        filterCache = config.getQueryCache();
        filterCachingPolicy = config.getQueryCachingPolicy();
        final long seed =  config.getIndexSettings().getValue(ESIntegTestCase.INDEX_TEST_SEED_SETTING);
        Random random = new Random(seed);
        final double ratio = WRAP_READER_RATIO.get(settings);
        boolean wrapReader = random.nextDouble() < ratio;
        if (logger.isTraceEnabled()) {
            logger.trace("Using [{}] for shard [{}] seed: [{}] wrapReader: [{}]", this.getClass().getName(), shardId, seed, wrapReader);
        }
        mockContext = new MockContext(random, wrapReader, wrapper, settings);
        this.searcherCloseable = new SearcherCloseable();
        LuceneTestCase.closeAfterSuite(searcherCloseable); // only one suite closeable per Engine
        this.disableFlushOnClose = DISABLE_FLUSH_ON_CLOSE.get(settings);
    }

    enum CloseAction {
        FLUSH_AND_CLOSE,
        CLOSE;
    }


    /**
     * Returns the CloseAction to execute on the actual engine. Note this method changes the state on
     * the first call and treats subsequent calls as if the engine passed is already closed.
     */
    public CloseAction flushOrClose(CloseAction originalAction) throws IOException {
        if (closing.compareAndSet(false, true)) { // only do the random thing if we are the first call to this since super.flushOnClose() calls #close() again and then we might end up with a stackoverflow.
            if (mockContext.random.nextBoolean()) {
                return CloseAction.FLUSH_AND_CLOSE;
            } else {
                return CloseAction.CLOSE;
            }
        } else {
            return originalAction;
        }
    }

    public AssertingIndexSearcher newSearcher(String source, IndexSearcher searcher, SearcherManager manager) throws EngineException {
        IndexReader reader = searcher.getIndexReader();
        IndexReader wrappedReader = reader;
        assert reader != null;
        if (reader instanceof DirectoryReader && mockContext.wrapReader) {
            wrappedReader = wrapReader((DirectoryReader) reader);
        }
        // this executes basic query checks and asserts that weights are normalized only once etc.
        final AssertingIndexSearcher assertingIndexSearcher = new AssertingIndexSearcher(mockContext.random, wrappedReader);
        assertingIndexSearcher.setSimilarity(searcher.getSimilarity(true));
        assertingIndexSearcher.setQueryCache(filterCache);
        assertingIndexSearcher.setQueryCachingPolicy(filterCachingPolicy);
        return assertingIndexSearcher;
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

    }

    public Engine.Searcher wrapSearcher(String source, Engine.Searcher engineSearcher, IndexSearcher searcher, SearcherManager manager) {
        final AssertingIndexSearcher assertingIndexSearcher = newSearcher(source, searcher, manager);
        assertingIndexSearcher.setSimilarity(searcher.getSimilarity(true));
        // pass the original searcher to the super.newSearcher() method to make sure this is the searcher that will
        // be released later on. If we wrap an index reader here must not pass the wrapped version to the manager
        // on release otherwise the reader will be closed too early. - good news, stuff will fail all over the place if we don't get this right here
        AssertingSearcher assertingSearcher = new AssertingSearcher(assertingIndexSearcher, engineSearcher, shardId, logger) {
            @Override
            public void close() {
                try {
                    searcherCloseable.remove(this);
                } finally {
                    super.close();
                }
            }
        };
        searcherCloseable.add(assertingSearcher, engineSearcher.source());
        return assertingSearcher;
    }

    private static final class SearcherCloseable implements Closeable {

        private final IdentityHashMap<AssertingSearcher, RuntimeException> openSearchers = new IdentityHashMap<>();

        @Override
        public synchronized void close() throws IOException {
            if (openSearchers.isEmpty() == false) {
                AssertionError error = new AssertionError("Unreleased searchers found");
                for (RuntimeException ex : openSearchers.values()) {
                    error.addSuppressed(ex);
                }
                throw error;
            }
        }

        void add(AssertingSearcher searcher, String source) {
            final RuntimeException ex = new RuntimeException("Unreleased Searcher, source [" + source+ "]");
            synchronized (this) {
                openSearchers.put(searcher, ex);
            }
        }

        synchronized void remove(AssertingSearcher searcher) {
            openSearchers.remove(searcher);
        }
    }
}
