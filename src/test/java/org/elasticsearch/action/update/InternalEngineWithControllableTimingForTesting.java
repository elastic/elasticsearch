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

package org.elasticsearch.action.update;

import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.analysis.AnalysisService;
import org.elasticsearch.index.codec.CodecService;
import org.elasticsearch.index.deletionpolicy.SnapshotDeletionPolicy;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.EngineException;
import org.elasticsearch.index.engine.VersionConflictEngineException;
import org.elasticsearch.index.engine.internal.InternalEngine;
import org.elasticsearch.index.indexing.ShardIndexingService;
import org.elasticsearch.index.mapper.Uid;
import org.elasticsearch.index.merge.policy.MergePolicyProvider;
import org.elasticsearch.index.merge.scheduler.MergeSchedulerProvider;
import org.elasticsearch.index.settings.IndexSettingsService;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.similarity.SimilarityService;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.indices.warmer.IndicesWarmer;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * An implementation of {@link Engine} only intended for use with {@link TransportUpdateActionTest}.
 */
public class InternalEngineWithControllableTimingForTesting extends InternalEngine implements Engine {

    /*
     * Not the best programming practice, but a simple way to make the instance accessible from test classes. The
     * "cleaner" way - making the appropriate Guice injector of the respective index available to the test class is
     * rather difficult and fragile, too. As long as tests requiring multiple instances of this class are not run in
     * parallel, everything will be fine. Currently, there is just a single test suite that uses only a single instance
     * anyway.
     */
    public static InternalEngineWithControllableTimingForTesting currentTestInstance;

    private AtomicBoolean nextGetThrowsException = new AtomicBoolean();

    private Semaphore createOperationReceived = new Semaphore(0);
    private Semaphore letCreateOperationBegin = new Semaphore(0);
    private Semaphore createOperationFinished = new Semaphore(0);
    private Semaphore letCreateOperationReturn = new Semaphore(0);

    private Semaphore indexOperationReceived = new Semaphore(0);
    private Semaphore letIndexOperationBegin = new Semaphore(0);
    private Semaphore indexOperationFinished = new Semaphore(0);
    private Semaphore letIndexOperationReturn = new Semaphore(0);

    private Semaphore deleteOperationReceived = new Semaphore(0);
    private Semaphore letDeleteOperationBegin = new Semaphore(0);
    private Semaphore deleteOperationFinished = new Semaphore(0);
    private Semaphore letDeleteOperationReturn = new Semaphore(0);

    // safety timeout so that if something goes wrong the test does not block forever
    private static final long SEMAPHORE_ACQUIRE_TIMEOUT_SECONDS = 5;

    @Inject
    public InternalEngineWithControllableTimingForTesting(ShardId shardId, Settings indexSettings, ThreadPool threadPool,
            IndexSettingsService indexSettingsService, ShardIndexingService indexingService, IndicesWarmer warmer, Store store,
            SnapshotDeletionPolicy deletionPolicy, Translog translog, MergePolicyProvider mergePolicyProvider,
            MergeSchedulerProvider mergeScheduler, AnalysisService analysisService, SimilarityService similarityService,
            CodecService codecService) throws EngineException {
        super(shardId, indexSettings, threadPool, indexSettingsService, indexingService, warmer, store, deletionPolicy, translog,
                mergePolicyProvider, mergeScheduler, analysisService, similarityService, codecService);
        // 'this' escapes from the constructor, but for the purpose of this test it is fine.
        currentTestInstance = this;
    }

    @Override
    public GetResult get(Get get) throws EngineException {
        if (nextGetThrowsException.getAndSet(false)) {
            Uid uid = Uid.createUid(get.uid().text());
            long dummyVersion = 1000L;
            throw new VersionConflictEngineException(shardId, uid.type(), uid.id(), dummyVersion, get.version());
        }
        return super.get(get);
    }

    private void acquireWithTimeout(Semaphore semaphore) {
        try {
            boolean acquired = semaphore.tryAcquire(SEMAPHORE_ACQUIRE_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            if (!acquired){
                throw new RuntimeException("(Integration test:) Cannot acquire semaphore within the specified timeout of "
                        + SEMAPHORE_ACQUIRE_TIMEOUT_SECONDS + " seconds");                
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public void create(Create create) throws EngineException {
        createOperationReceived.release();
        acquireWithTimeout(letCreateOperationBegin);
        try {
            super.create(create);
        } finally {
            createOperationFinished.release();
            acquireWithTimeout(letCreateOperationReturn);
        }
    }

    @Override
    public void index(Index index) throws EngineException {
        indexOperationReceived.release();
        acquireWithTimeout(letIndexOperationBegin);
        try {
            super.index(index);
        } finally {
            indexOperationFinished.release();
            acquireWithTimeout(letIndexOperationReturn);
        }
    }

    @Override
    public void delete(Delete delete) throws EngineException {
        deleteOperationReceived.release();
        acquireWithTimeout(letDeleteOperationBegin);
        try {
            super.delete(delete);
        } finally {
            deleteOperationFinished.release();
            acquireWithTimeout(letDeleteOperationReturn);
        }
    }

    public void letNextGetThrowException() {
        nextGetThrowsException.set(true);
    }

    public void waitUntilCreateOperationReceived() {
        acquireWithTimeout(createOperationReceived);
    }

    public void letCreateOperationBegin() {
        letCreateOperationBegin.release();
    }

    public void waitUntilCreateOperationFinished() {
        acquireWithTimeout(createOperationFinished);
    }

    public void letCreateOperationReturn() {
        letCreateOperationReturn.release();
    }

    public void waitUntilIndexOperationReceived() {
        acquireWithTimeout(indexOperationReceived);
    }

    public void letIndexOperationBegin() {
        letIndexOperationBegin.release();
    }

    public void waitUntilIndexOperationFinished() {
        acquireWithTimeout(indexOperationFinished);
    }

    public void letIndexOperationReturn() {
        letIndexOperationReturn.release();
    }

    public void waitUntilDeleteOperationReceived() {
        acquireWithTimeout(deleteOperationReceived);
    }

    public void letDeleteOperationBegin() {
        letDeleteOperationBegin.release();
    }

    public void waitUntilDeleteOperationFinished() {
        acquireWithTimeout(deleteOperationFinished);
    }

    public void letDeleteOperationReturn() {
        letDeleteOperationReturn.release();
    }

    public void resetSemaphores() {
        nextGetThrowsException = new AtomicBoolean();

        createOperationReceived = new Semaphore(0);
        letCreateOperationBegin = new Semaphore(0);
        createOperationFinished = new Semaphore(0);
        letCreateOperationReturn = new Semaphore(0);

        indexOperationReceived = new Semaphore(0);
        letIndexOperationBegin = new Semaphore(0);
        indexOperationFinished = new Semaphore(0);
        letIndexOperationReturn = new Semaphore(0);

        deleteOperationReceived = new Semaphore(0);
        letDeleteOperationBegin = new Semaphore(0);
        deleteOperationFinished = new Semaphore(0);
        letDeleteOperationReturn = new Semaphore(0);
    }
}
