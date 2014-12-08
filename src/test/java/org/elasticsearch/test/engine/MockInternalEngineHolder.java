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
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.analysis.AnalysisService;
import org.elasticsearch.index.codec.CodecService;
import org.elasticsearch.index.deletionpolicy.SnapshotDeletionPolicy;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.EngineException;
import org.elasticsearch.index.engine.internal.InternalEngine;
import org.elasticsearch.index.engine.internal.InternalEngineHolder;
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

import java.util.Random;

public final class MockInternalEngineHolder extends InternalEngineHolder implements Engine {
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

    MockContext mockContext;

    @Inject
    public MockInternalEngineHolder(ShardId shardId, @IndexSettings Settings indexSettings, ThreadPool threadPool,
                                    IndexSettingsService indexSettingsService, ShardIndexingService indexingService, @Nullable IndicesWarmer warmer, Store store,
                                    SnapshotDeletionPolicy deletionPolicy, Translog translog, MergePolicyProvider mergePolicyProvider,
                                    MergeSchedulerProvider mergeScheduler, AnalysisService analysisService, SimilarityService similarityService,
                                    CodecService codecService) throws EngineException {
        super(shardId, indexSettings, threadPool, indexSettingsService, indexingService, warmer, store, deletionPolicy, translog, mergePolicyProvider, mergeScheduler, analysisService,
                similarityService, codecService
        );
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
    protected InternalEngine createEngine() {
        return new MockInternalEngine(mockContext, shardId, logger, codecService, threadPool, indexingService,
                warmer, store, deletionPolicy, translog, mergePolicyProvider, mergeScheduler, analysisService, similarityService,
                enableGcDeletes, gcDeletesInMillis,
                indexingBufferSize, codecName, compoundOnFlush, indexConcurrency, optimizeAutoGenerateId, failEngineOnCorruption, this);
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
