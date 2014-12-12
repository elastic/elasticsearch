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
package org.elasticsearch.index.engine.internal;

import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.MergePolicy;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Preconditions;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.index.analysis.AnalysisService;
import org.elasticsearch.index.codec.CodecService;
import org.elasticsearch.index.deletionpolicy.SnapshotDeletionPolicy;
import org.elasticsearch.index.deletionpolicy.SnapshotIndexCommit;
import org.elasticsearch.index.engine.*;
import org.elasticsearch.index.indexing.ShardIndexingService;
import org.elasticsearch.index.merge.OnGoingMerge;
import org.elasticsearch.index.merge.policy.MergePolicyProvider;
import org.elasticsearch.index.merge.scheduler.MergeSchedulerProvider;
import org.elasticsearch.index.settings.IndexSettings;
import org.elasticsearch.index.settings.IndexSettingsService;
import org.elasticsearch.index.shard.AbstractIndexShardComponent;
import org.elasticsearch.index.shard.IndexShardComponent;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.similarity.SimilarityService;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.indices.warmer.IndicesWarmer;
import org.elasticsearch.indices.warmer.InternalIndicesWarmer;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class InternalEngineHolder extends AbstractIndexShardComponent implements IndexShardComponent, Engine, Engine.FailedEngineListener {


    private final FailEngineOnMergeFailure mergeSchedulerFailureListener;
    private final ApplySettings settingsListener;
    private final MergeScheduleListener mergeSchedulerListener;
    protected volatile Boolean failOnMergeFailure;
    protected volatile boolean failEngineOnCorruption;
    protected volatile ByteSizeValue indexingBufferSize;
    protected volatile int indexConcurrency;
    protected volatile boolean compoundOnFlush = true;

    protected long gcDeletesInMillis;

    protected volatile boolean enableGcDeletes = true;
    protected volatile String codecName;
    protected final boolean optimizeAutoGenerateId;

    protected final ThreadPool threadPool;

    protected final ShardIndexingService indexingService;
    protected final IndexSettingsService indexSettingsService;
    @Nullable
    protected final InternalIndicesWarmer warmer;
    protected final Store store;
    protected final SnapshotDeletionPolicy deletionPolicy;
    protected final Translog translog;
    protected final MergePolicyProvider mergePolicyProvider;
    protected final MergeSchedulerProvider mergeScheduler;
    protected final AnalysisService analysisService;
    protected final SimilarityService similarityService;
    protected final CodecService codecService;

    private final AtomicReference<InternalEngine> currentEngine = new AtomicReference<>();
    private volatile boolean closed = false;

    public static final String INDEX_INDEX_CONCURRENCY = "index.index_concurrency";
    public static final String INDEX_COMPOUND_ON_FLUSH = "index.compound_on_flush";
    public static final String INDEX_GC_DELETES = "index.gc_deletes";
    public static final String INDEX_FAIL_ON_MERGE_FAILURE = "index.fail_on_merge_failure";
    public static final String INDEX_FAIL_ON_CORRUPTION = "index.fail_on_corruption";

    public static final TimeValue DEFAULT_REFRESH_INTERVAL = new TimeValue(1, TimeUnit.SECONDS);

    private final CopyOnWriteArrayList<FailedEngineListener> failedEngineListeners = new CopyOnWriteArrayList<>();

    @Inject
    public InternalEngineHolder(ShardId shardId, @IndexSettings Settings indexSettings, ThreadPool threadPool,
                                IndexSettingsService indexSettingsService, ShardIndexingService indexingService, @Nullable IndicesWarmer warmer,
                                Store store, SnapshotDeletionPolicy deletionPolicy, Translog translog,
                                MergePolicyProvider mergePolicyProvider, MergeSchedulerProvider mergeScheduler,
                                AnalysisService analysisService, SimilarityService similarityService, CodecService codecService) throws EngineException {
        super(shardId, indexSettings);
        Preconditions.checkNotNull(store, "Store must be provided to the engine");
        Preconditions.checkNotNull(deletionPolicy, "Snapshot deletion policy must be provided to the engine");
        Preconditions.checkNotNull(translog, "Translog must be provided to the engine");

        this.gcDeletesInMillis = indexSettings.getAsTime(INDEX_GC_DELETES, TimeValue.timeValueSeconds(60)).millis();
        this.indexingBufferSize = componentSettings.getAsBytesSize("index_buffer_size", new ByteSizeValue(64, ByteSizeUnit.MB)); // not really important, as it is set by the IndexingMemory manager
        this.codecName = indexSettings.get(INDEX_CODEC, "default");

        this.threadPool = threadPool;
        this.indexSettingsService = indexSettingsService;
        this.indexingService = indexingService;
        this.warmer = (InternalIndicesWarmer) warmer;
        this.store = store;
        this.deletionPolicy = deletionPolicy;
        this.translog = translog;
        this.mergePolicyProvider = mergePolicyProvider;
        this.mergeScheduler = mergeScheduler;
        this.analysisService = analysisService;
        this.similarityService = similarityService;
        this.codecService = codecService;
        this.compoundOnFlush = indexSettings.getAsBoolean(INDEX_COMPOUND_ON_FLUSH, this.compoundOnFlush);
        this.indexConcurrency = indexSettings.getAsInt(INDEX_INDEX_CONCURRENCY, Math.max(IndexWriterConfig.DEFAULT_MAX_THREAD_STATES, (int) (EsExecutors.boundedNumberOfProcessors(indexSettings) * 0.65)));
        this.optimizeAutoGenerateId = indexSettings.getAsBoolean("index.optimize_auto_generated_id", true);

        this.failEngineOnCorruption = indexSettings.getAsBoolean(INDEX_FAIL_ON_CORRUPTION, true);
        this.failOnMergeFailure = indexSettings.getAsBoolean(INDEX_FAIL_ON_MERGE_FAILURE, true);
        this.mergeSchedulerFailureListener = new FailEngineOnMergeFailure();
        this.mergeScheduler.addFailureListener(mergeSchedulerFailureListener);
        this.mergeSchedulerListener = new MergeScheduleListener();
        this.mergeScheduler.addListener(mergeSchedulerListener);

        this.settingsListener = new ApplySettings(logger, this);
        this.indexSettingsService.addListener(this.settingsListener);
        store.incRef();
    }

    @Override
    public TimeValue defaultRefreshInterval() {
        return DEFAULT_REFRESH_INTERVAL;
    }


    public InternalEngine engineSafe() {
        InternalEngine engine = currentEngine.get();
        if (engine == null) {
            throw new EngineClosedException(shardId);
        }
        return engine;
    }

    @Override
    public void enableGcDeletes(boolean enableGcDeletes) {
        this.enableGcDeletes = enableGcDeletes;
        InternalEngine currentEngine = this.currentEngine.get();
        if (currentEngine != null) {
            currentEngine.enableGcDeletes(enableGcDeletes);
        }
    }

    @Override
    public void updateIndexingBufferSize(ByteSizeValue indexingBufferSize) {
        this.indexingBufferSize = indexingBufferSize;
        InternalEngine currentEngine = this.currentEngine.get();
        if (currentEngine != null) {
            currentEngine.updateIndexingBufferSize(indexingBufferSize);
        }
    }

    @Override
    public void addFailedEngineListener(FailedEngineListener listener) {
        failedEngineListeners.add(listener);
    }

    @Override
    public synchronized void start() throws EngineException {
        if (closed) {
            throw new EngineClosedException(shardId);
        }
        InternalEngine currentEngine = this.currentEngine.get();
        if (currentEngine != null) {
            throw new EngineAlreadyStartedException(shardId);
        }
        InternalEngine newEngine = createEngine();
        store.incRef();
        try {
            newEngine.start();
            boolean success = this.currentEngine.compareAndSet(null, newEngine);
            assert success : "engine changes should be done under a synchronize";
        } finally {
            store.decRef();
        }
    }

    @Override
    public synchronized void stop() throws EngineException {
        InternalEngine currentEngine = this.currentEngine.getAndSet(null);
        if (currentEngine != null) {
            currentEngine.close();
        }
    }

    @Override
    public synchronized void close() throws ElasticsearchException {
        if (closed == false) {
            closed = true;
            try {
                InternalEngine currentEngine = this.currentEngine.getAndSet(null);
                if (currentEngine != null) {
                    currentEngine.close();
                }
                mergeScheduler.removeFailureListener(mergeSchedulerFailureListener);
                mergeScheduler.removeListener(mergeSchedulerListener);
                indexSettingsService.removeListener(settingsListener);
            } finally {
                store.decRef();
            }
        }
    }

    protected InternalEngine createEngine() {
        return new InternalEngine(shardId, logger, codecService, threadPool, indexingService,
                warmer, store, deletionPolicy, translog, mergePolicyProvider, mergeScheduler, analysisService, similarityService,
                enableGcDeletes, gcDeletesInMillis,
                indexingBufferSize, codecName, compoundOnFlush, indexConcurrency, optimizeAutoGenerateId, failEngineOnCorruption, this);
    }

    @Override
    public void create(Create create) throws EngineException {
        engineSafe().create(create);
    }

    @Override
    public void index(Index index) throws EngineException {
        engineSafe().index(index);
    }

    @Override
    public void delete(Delete delete) throws EngineException {
        engineSafe().delete(delete);
    }

    @Override
    public void delete(DeleteByQuery delete) throws EngineException {
        engineSafe().delete(delete);
    }

    @Override
    public GetResult get(Get get) throws EngineException {
        return engineSafe().get(get);
    }

    @Override
    public Searcher acquireSearcher(String source) throws EngineException {
        return engineSafe().acquireSearcher(source);
    }

    @Override
    public SegmentsStats segmentsStats() {
        return engineSafe().segmentsStats();
    }

    @Override
    public List<Segment> segments() {
        return engineSafe().segments();
    }

    @Override
    public boolean refreshNeeded() {
        return engineSafe().refreshNeeded();
    }

    @Override
    public void refresh(String source, boolean force) throws EngineException {
        engineSafe().refresh(source, force);
    }

    @Override
    public void flush(FlushType type, boolean force, boolean waitIfOngoing) throws EngineException, FlushNotAllowedEngineException {
        engineSafe().flush(type, force, waitIfOngoing);
    }

    @Override
    public void forceMerge(boolean flush) {
        engineSafe().forceMerge(flush);
    }

    @Override
    public void forceMerge(boolean flush, int maxNumSegments, boolean onlyExpungeDeletes, boolean upgrade) throws EngineException {
        engineSafe().forceMerge(flush, maxNumSegments, onlyExpungeDeletes, upgrade);
    }

    @Override
    public SnapshotIndexCommit snapshotIndex() throws EngineException {
        return engineSafe().snapshotIndex();
    }

    @Override
    public void recover(RecoveryHandler recoveryHandler) throws EngineException {
        engineSafe().recover(recoveryHandler);
    }

    @Override
    public void failEngine(String reason, Throwable failure) {
        engineSafe().failEngine(reason, failure);
    }

    @Override
    public ShardId shardId() {
        return shardId;
    }

    @Override
    public Settings indexSettings() {
        return indexSettings;
    }


    /** return the current indexing buffer size setting * */
    public ByteSizeValue indexingBufferSize() {
        return indexingBufferSize;
    }


    // called by the current engine
    @Override
    public void onFailedEngine(ShardId shardId, String reason, @Nullable Throwable failure) {
        try {
            for (FailedEngineListener listener : failedEngineListeners) {
                try {
                    listener.onFailedEngine(shardId, reason, failure);
                } catch (Exception e) {
                    logger.warn("exception while notifying engine failure", e);
                }
            }
        } finally {
            close(); // we need to close ourself - we failed all bets are off
        }
    }

    static class ApplySettings implements IndexSettingsService.Listener {

        private final ESLogger logger;
        private final InternalEngineHolder holder;

        ApplySettings(ESLogger logger, InternalEngineHolder holder) {
            this.logger = logger;
            this.holder = holder;
        }

        @Override
        public void onRefreshSettings(Settings settings) {
            boolean change = false;
            long gcDeletesInMillis = settings.getAsTime(INDEX_GC_DELETES, TimeValue.timeValueMillis(holder.gcDeletesInMillis)).millis();
            if (gcDeletesInMillis != holder.gcDeletesInMillis) {
                logger.info("updating index.gc_deletes from [{}] to [{}]", TimeValue.timeValueMillis(holder.gcDeletesInMillis), TimeValue.timeValueMillis(gcDeletesInMillis));
                holder.gcDeletesInMillis = gcDeletesInMillis;
                change = true;
            }

            final boolean compoundOnFlush = settings.getAsBoolean(INDEX_COMPOUND_ON_FLUSH, holder.compoundOnFlush);
            if (compoundOnFlush != holder.compoundOnFlush) {
                logger.info("updating {} from [{}] to [{}]", INDEX_COMPOUND_ON_FLUSH, holder.compoundOnFlush, compoundOnFlush);
                holder.compoundOnFlush = compoundOnFlush;
                change = true;
            }

            final boolean failEngineOnCorruption = settings.getAsBoolean(INDEX_FAIL_ON_CORRUPTION, holder.failEngineOnCorruption);
            if (failEngineOnCorruption != holder.failEngineOnCorruption) {
                logger.info("updating {} from [{}] to [{}]", INDEX_FAIL_ON_CORRUPTION, holder.failEngineOnCorruption, failEngineOnCorruption);
                holder.failEngineOnCorruption = failEngineOnCorruption;
                change = true;
            }
            int indexConcurrency = settings.getAsInt(INDEX_INDEX_CONCURRENCY, holder.indexConcurrency);
            if (indexConcurrency != holder.indexConcurrency) {
                logger.info("updating index.index_concurrency from [{}] to [{}]", holder.indexConcurrency, indexConcurrency);
                holder.indexConcurrency = indexConcurrency;
                // we have to flush in this case, since it only applies on a new index writer
                change = true;
            }
            final String codecName = settings.get(INDEX_CODEC, holder.codecName);
            if (!codecName.equals(holder.codecName)) {
                logger.info("updating index.codec from [{}] to [{}]", holder.codecName, codecName);
                holder.codecName = codecName;
                // we want to flush in this case, so the new codec will be reflected right away...
                change = true;
            }
            final boolean failOnMergeFailure = settings.getAsBoolean(INDEX_FAIL_ON_MERGE_FAILURE, holder.failOnMergeFailure);
            if (failOnMergeFailure != holder.failOnMergeFailure) {
                logger.info("updating {} from [{}] to [{}]", INDEX_FAIL_ON_MERGE_FAILURE, holder.failOnMergeFailure, failOnMergeFailure);
                holder.failOnMergeFailure = failOnMergeFailure;
            }


            if (change) {
                holder.updateSettings();
            }
        }
    }

     synchronized void updateSettings() {
        // we need to make sure that we wait for the engine to be fully initialized
        // the start method sets the current engine once it's done but samples the settings
        // at construction time.
        final InternalEngine engine = currentEngine.get();
        if (engine != null) {
            engine.updateSettings(gcDeletesInMillis, compoundOnFlush, failEngineOnCorruption, indexConcurrency, codecName);
        }
    }

    class FailEngineOnMergeFailure implements MergeSchedulerProvider.FailureListener {
        @Override
        public void onFailedMerge(MergePolicy.MergeException e) {
            if (Lucene.isCorruptionException(e)) {
                if (failEngineOnCorruption) {
                    failEngine("corrupt file detected source: [merge]", e);
                } else {
                    logger.warn("corrupt file detected source: [merge] but [{}] is set to [{}]", e, INDEX_FAIL_ON_CORRUPTION, failEngineOnCorruption);
                }
            } else if (failOnMergeFailure) {
                failEngine("merge exception", e);
            }
        }
    }

    class MergeScheduleListener implements MergeSchedulerProvider.Listener {
        private final AtomicInteger numMergesInFlight = new AtomicInteger(0);
        private final AtomicBoolean isThrottling = new AtomicBoolean();

        @Override
        public synchronized void beforeMerge(OnGoingMerge merge) {
            int maxNumMerges = mergeScheduler.getMaxMerges();
            InternalEngine currentEngineImpl = currentEngine.get();
            if (numMergesInFlight.incrementAndGet() > maxNumMerges && currentEngineImpl != null) {
                if (isThrottling.getAndSet(true) == false) {
                    logger.info("now throttling indexing: numMergesInFlight={}, maxNumMerges={}", numMergesInFlight, maxNumMerges);
                    indexingService.throttlingActivated();
                    currentEngineImpl.activateThrottling();
                }
            }
        }

        @Override
        public synchronized void afterMerge(OnGoingMerge merge) {
            int maxNumMerges = mergeScheduler.getMaxMerges();
            InternalEngine currentEngineImpl = currentEngine.get();
            if (numMergesInFlight.decrementAndGet() < maxNumMerges && currentEngineImpl != null) {
                if (isThrottling.getAndSet(false)) {
                    logger.info("stop throttling indexing: numMergesInFlight={}, maxNumMerges={}", numMergesInFlight, maxNumMerges);
                    indexingService.throttlingDeactivated();
                    currentEngineImpl.deactivateThrottling();
                }
            }
        }

    }
}
