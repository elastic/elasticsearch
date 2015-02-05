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
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.engine.EngineConfig;
import org.elasticsearch.index.engine.EngineException;
import org.elasticsearch.index.engine.InternalEngine;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ElasticsearchIntegrationTest;

import java.lang.reflect.Constructor;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class MockInternalEngine extends InternalEngine {
    public static final String WRAP_READER_RATIO = "index.engine.mock.random.wrap_reader_ratio";
    public static final String READER_WRAPPER_TYPE = "index.engine.mock.random.wrapper";

    public static class MockContext {
        public final Random random;
        public final boolean wrapReader;
        public final Class<? extends FilterDirectoryReader> wrapper;
        public final Settings indexSettings;

        public MockContext(Random random, boolean wrapReader, Class<? extends FilterDirectoryReader> wrapper, Settings indexSettings) {
            this.random = random;
            this.wrapReader = wrapReader;
            this.wrapper = wrapper;
            this.indexSettings = indexSettings;
        }
    }

    public static final ConcurrentMap<AssertingSearcher, RuntimeException> INFLIGHT_ENGINE_SEARCHERS = new ConcurrentHashMap<>();

    private MockContext mockContext;

    public MockInternalEngine(EngineConfig config) throws EngineException {
        super(config);
        Settings indexSettings = config.getIndexSettings();
        final long seed = indexSettings.getAsLong(ElasticsearchIntegrationTest.SETTING_INDEX_SEED, 0l);
        Random random = new Random(seed);
        final double ratio = indexSettings.getAsDouble(WRAP_READER_RATIO, 0.0d); // DISABLED by default - AssertingDR is crazy slow
        Class<? extends AssertingDirectoryReader> wrapper = indexSettings.getAsClass(READER_WRAPPER_TYPE, AssertingDirectoryReader.class);
        boolean wrapReader = random.nextDouble() < ratio;
        if (logger.isTraceEnabled()) {
            logger.trace("Using [{}] for shard [{}] seed: [{}] wrapReader: [{}]", this.getClass().getName(), shardId, seed, wrapReader);
        }
        mockContext = new MockContext(random, wrapReader, wrapper, indexSettings);
    }

    @Override
    public void close() {
        try {
            super.close();
        } finally {
            if (logger.isTraceEnabled()) {
                // log debug if we have pending searchers
                for (Map.Entry<MockInternalEngine.AssertingSearcher, RuntimeException> entry : INFLIGHT_ENGINE_SEARCHERS.entrySet()) {
                    logger.trace("Unreleased Searchers instance for shard [{}]", entry.getValue(), entry.getKey().shardId);
                }
            }
        }
        logger.debug("Ongoing recoveries after engine close: " + onGoingRecoveries.get());
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
        // pass the original searcher to the super.newSearcher() method to make sure this is the searcher that will
        // be released later on. If we wrap an index reader here must not pass the wrapped version to the manager
        // on release otherwise the reader will be closed too early. - good news, stuff will fail all over the place if we don't get this right here
        return new AssertingSearcher(assertingIndexSearcher, super.newSearcher(source, searcher, manager), shardId);
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

    public final class AssertingSearcher extends Searcher {
        private final Searcher wrappedSearcher;
        private final ShardId shardId;
        private final IndexSearcher indexSearcher;
        private RuntimeException firstReleaseStack;
        private final Object lock = new Object();
        private final int initialRefCount;

        public AssertingSearcher(IndexSearcher indexSearcher, Searcher wrappedSearcher, ShardId shardId) {
            super(wrappedSearcher.source(), indexSearcher);
            // we only use the given index searcher here instead of the IS of the wrapped searcher. the IS might be a wrapped searcher
            // with a wrapped reader.
            this.wrappedSearcher = wrappedSearcher;
            this.shardId = shardId;
            initialRefCount = wrappedSearcher.reader().getRefCount();
            this.indexSearcher = indexSearcher;
            assert initialRefCount > 0 : "IndexReader#getRefCount() was [" + initialRefCount + "] expected a value > [0] - reader is already closed";
            INFLIGHT_ENGINE_SEARCHERS.put(this, new RuntimeException("Unreleased Searcher, source [" + wrappedSearcher.source() + "]"));
        }

        @Override
        public String source() {
            return wrappedSearcher.source();
        }

        @Override
        public void close() throws ElasticsearchException {
            RuntimeException remove = INFLIGHT_ENGINE_SEARCHERS.remove(this);
            synchronized (lock) {
                // make sure we only get this once and store the stack of the first caller!
                if (remove == null) {
                    assert firstReleaseStack != null;
                    AssertionError error = new AssertionError("Released Searcher more than once, source [" + wrappedSearcher.source() + "]");
                    error.initCause(firstReleaseStack);
                    throw error;
                } else {
                    assert firstReleaseStack == null;
                    firstReleaseStack = new RuntimeException("Searcher Released first here, source [" + wrappedSearcher.source() + "]");
                }
            }
            final int refCount = wrappedSearcher.reader().getRefCount();
            // this assert seems to be paranoid but given LUCENE-5362 we better add some assertions here to make sure we catch any potential
            // problems.
            assert refCount > 0 : "IndexReader#getRefCount() was [" + refCount + "] expected a value > [0] - reader is already closed. Initial refCount was: [" + initialRefCount + "]";
            try {
                wrappedSearcher.close();
            } catch (RuntimeException ex) {
                logger.debug("Failed to release searcher", ex);
                throw ex;
            }
        }

        @Override
        public IndexReader reader() {
            return indexSearcher.getIndexReader();
        }

        @Override
        public IndexSearcher searcher() {
            return indexSearcher;
        }

        public ShardId shardId() {
            return shardId;
        }
    }

    public static abstract class DirectoryReaderWrapper extends FilterDirectoryReader {
        protected final SubReaderWrapper subReaderWrapper;

        public DirectoryReaderWrapper(DirectoryReader in, SubReaderWrapper subReaderWrapper) {
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
