/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.AssertingIndexSearcher;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.SearcherManager;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.analysis.AnalysisService;
import org.elasticsearch.index.codec.CodecService;
import org.elasticsearch.index.deletionpolicy.SnapshotDeletionPolicy;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.EngineException;
import org.elasticsearch.index.engine.robin.RobinEngine;
import org.elasticsearch.index.indexing.ShardIndexingService;
import org.elasticsearch.index.merge.policy.MergePolicyProvider;
import org.elasticsearch.index.merge.scheduler.MergeSchedulerProvider;
import org.elasticsearch.index.settings.IndexSettings;
import org.elasticsearch.index.settings.IndexSettingsService;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.similarity.SimilarityService;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.indices.warmer.IndicesWarmer;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.Map.Entry;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public final class MockRobinEngine extends RobinEngine implements Engine {
    public static final ConcurrentMap<AssertingSearcher, RuntimeException> INFLIGHT_ENGINE_SEARCHERS = new ConcurrentHashMap<AssertingSearcher, RuntimeException>();
    private final Random random;

    @Inject
    public MockRobinEngine(ShardId shardId, @IndexSettings Settings indexSettings, ThreadPool threadPool,
                           IndexSettingsService indexSettingsService, ShardIndexingService indexingService, @Nullable IndicesWarmer warmer, Store store,
                           SnapshotDeletionPolicy deletionPolicy, Translog translog, MergePolicyProvider mergePolicyProvider,
                           MergeSchedulerProvider mergeScheduler, AnalysisService analysisService, SimilarityService similarityService,
                           CodecService codecService) throws EngineException {
        super(shardId, indexSettings, threadPool, indexSettingsService, indexingService, warmer, store,
                deletionPolicy, translog, mergePolicyProvider, mergeScheduler, analysisService, similarityService, codecService);
        final long seed = indexSettings.getAsLong(ElasticsearchIntegrationTest.INDEX_SEED_SETTING, 0l);
        if (logger.isTraceEnabled()) {
            logger.trace("Using [{}] for shard [{}] seed: [{}]", this.getClass().getName(), shardId, seed);
        }
        random = new Random(seed);
    }


    public void close() throws ElasticSearchException {
        try {
            super.close();
        } finally {
            if (logger.isTraceEnabled()) {
                // log debug if we have pending searchers
                for (Entry<MockRobinEngine.AssertingSearcher, RuntimeException> entry : MockRobinEngine.INFLIGHT_ENGINE_SEARCHERS.entrySet()) {
                    logger.trace("Unreleased Searchers instance for shard [{}]", entry.getValue(), entry.getKey().shardId);
                }
            }
        }
    }

    @Override
    protected Searcher newSearcher(String source, IndexSearcher searcher, SearcherManager manager) throws EngineException {
        // this executes basic query checks and asserts that weights are normalized only once etc.
        final AssertingIndexSearcher assertingIndexSearcher = new AssertingIndexSearcher(random, searcher.getTopReaderContext());
        assertingIndexSearcher.setSimilarity(searcher.getSimilarity());
        return new AssertingSearcher(super.newSearcher(source, assertingIndexSearcher, manager), shardId);
    }

    public final class AssertingSearcher implements Searcher {
        private final Searcher searcher;
        private final ShardId shardId;
        private RuntimeException firstReleaseStack;
        private final Object lock = new Object();
        private final int initialRefCount;

        public AssertingSearcher(Searcher searcher, ShardId shardId) {
            this.searcher = searcher;
            this.shardId = shardId;
            initialRefCount = searcher.reader().getRefCount();
            assert initialRefCount > 0 : "IndexReader#getRefCount() was [" + initialRefCount + "] expected a value > [0] - reader is already closed";
            INFLIGHT_ENGINE_SEARCHERS.put(this, new RuntimeException("Unreleased Searcher, source [" + searcher.source() + "]"));
        }

        @Override
        public String source() {
            return searcher.source();
        }

        @Override
        public boolean release() throws ElasticSearchException {
            RuntimeException remove = INFLIGHT_ENGINE_SEARCHERS.remove(this);
            synchronized (lock) {
                // make sure we only get this once and store the stack of the first caller!
                if (remove == null) {
                    assert firstReleaseStack != null;
                    AssertionError error = new AssertionError("Released Searcher more than once, source [" + searcher.source() + "]");
                    error.initCause(firstReleaseStack);
                    throw error;
                } else {
                    assert firstReleaseStack == null;
                    firstReleaseStack = new RuntimeException("Searcher Released first here, source [" + searcher.source() + "]");
                }
            }
            final int refCount = searcher.reader().getRefCount();
            // this assert seems to be paranoid but given LUCENE-5362 we better add some assertions here to make sure we catch any potential
            // problems.
            assert refCount > 0 : "IndexReader#getRefCount() was [" + refCount + "] expected a value > [0] - reader is already closed. Initial refCount was: [" + initialRefCount + "]";
            try {
                return searcher.release();
            } catch (RuntimeException ex) {
                logger.debug("Failed to release searcher", ex);
                throw ex;
            }
        }

        @Override
        public IndexReader reader() {
            return searcher.reader();
        }

        @Override
        public IndexSearcher searcher() {
            return searcher.searcher();
        }

        public ShardId shardId() {
            return shardId;
        }
    }
}
