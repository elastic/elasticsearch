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
package org.elasticsearch.index.engine;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.SnapshotDeletionPolicy;
import org.apache.lucene.search.QueryCache;
import org.apache.lucene.search.QueryCachingPolicy;
import org.apache.lucene.search.similarities.Similarity;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.codec.CodecService;
import org.elasticsearch.index.shard.MergeSchedulerConfig;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.TranslogRecoveryPerformer;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.translog.TranslogConfig;
import org.elasticsearch.indices.IndexingMemoryController;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.concurrent.TimeUnit;

/*
 * Holds all the configuration that is used to create an {@link Engine}.
 * Once {@link Engine} has been created with this object, changes to this
 * object will affect the {@link Engine} instance.
 */
public final class EngineConfig {
    private final ShardId shardId;
    private final TranslogRecoveryPerformer translogRecoveryPerformer;
    private final IndexSettings indexSettings;
    private final ByteSizeValue indexingBufferSize;
    private long gcDeletesInMillis = DEFAULT_GC_DELETES.millis();
    private volatile boolean enableGcDeletes = true;
    private final TimeValue flushMergesAfter;
    private final String codecName;
    private final ThreadPool threadPool;
    private final Engine.Warmer warmer;
    private final Store store;
    private final SnapshotDeletionPolicy deletionPolicy;
    private final MergePolicy mergePolicy;
    private final MergeSchedulerConfig mergeSchedulerConfig;
    private final Analyzer analyzer;
    private final Similarity similarity;
    private final CodecService codecService;
    private final Engine.EventListener eventListener;
    private final boolean forceNewTranslog;
    private final QueryCache queryCache;
    private final QueryCachingPolicy queryCachingPolicy;

    /**
     * Index setting to enable / disable deletes garbage collection.
     * This setting is realtime updateable
     */
    public static final String INDEX_GC_DELETES_SETTING = "index.gc_deletes";

    /**
     * Index setting to change the low level lucene codec used for writing new segments.
     * This setting is <b>not</b> realtime updateable.
     */
    public static final String INDEX_CODEC_SETTING = "index.codec";

    /** if set to true the engine will start even if the translog id in the commit point can not be found */
    public static final String INDEX_FORCE_NEW_TRANSLOG = "index.engine.force_new_translog";

    public static final TimeValue DEFAULT_GC_DELETES = TimeValue.timeValueSeconds(60);

    private static final String DEFAULT_CODEC_NAME = "default";
    private TranslogConfig translogConfig;
    private boolean create = false;

    /**
     * Creates a new {@link org.elasticsearch.index.engine.EngineConfig}
     */
    public EngineConfig(ShardId shardId, ThreadPool threadPool,
                        IndexSettings indexSettings, Engine.Warmer warmer, Store store, SnapshotDeletionPolicy deletionPolicy,
                        MergePolicy mergePolicy, MergeSchedulerConfig mergeSchedulerConfig, Analyzer analyzer,
                        Similarity similarity, CodecService codecService, Engine.EventListener eventListener,
                        TranslogRecoveryPerformer translogRecoveryPerformer, QueryCache queryCache, QueryCachingPolicy queryCachingPolicy, TranslogConfig translogConfig, TimeValue flushMergesAfter) {
        this.shardId = shardId;
        final Settings settings = indexSettings.getSettings();
        this.indexSettings = indexSettings;
        this.threadPool = threadPool;
        this.warmer = warmer == null ? (a,b) -> {} : warmer;
        this.store = store;
        this.deletionPolicy = deletionPolicy;
        this.mergePolicy = mergePolicy;
        this.mergeSchedulerConfig = mergeSchedulerConfig;
        this.analyzer = analyzer;
        this.similarity = similarity;
        this.codecService = codecService;
        this.eventListener = eventListener;
        codecName = settings.get(EngineConfig.INDEX_CODEC_SETTING, EngineConfig.DEFAULT_CODEC_NAME);
        // We give IndexWriter a "huge" (256 MB) buffer, so it won't flush on its own unless the ES indexing buffer is also huge and/or
        // there are not too many shards allocated to this node.  Instead, IndexingMemoryController periodically checks
        // and refreshes the most heap-consuming shards when total indexing heap usage across all shards is too high:
        indexingBufferSize = new ByteSizeValue(256, ByteSizeUnit.MB);
        gcDeletesInMillis = settings.getAsTime(INDEX_GC_DELETES_SETTING, EngineConfig.DEFAULT_GC_DELETES).millis();
        this.translogRecoveryPerformer = translogRecoveryPerformer;
        this.forceNewTranslog = settings.getAsBoolean(INDEX_FORCE_NEW_TRANSLOG, false);
        this.queryCache = queryCache;
        this.queryCachingPolicy = queryCachingPolicy;
        this.translogConfig = translogConfig;
        this.flushMergesAfter = flushMergesAfter;
    }

    /** if true the engine will start even if the translog id in the commit point can not be found */
    public boolean forceNewTranslog() {
        return forceNewTranslog;
    }

    /**
     * Enables / disables gc deletes
     *
     * @see #isEnableGcDeletes()
     */
    public void setEnableGcDeletes(boolean enableGcDeletes) {
        this.enableGcDeletes = enableGcDeletes;
    }

    /**
     * Returns the initial index buffer size. This setting is only read on startup and otherwise controlled by {@link IndexingMemoryController}
     */
    public ByteSizeValue getIndexingBufferSize() {
        return indexingBufferSize;
    }

    /**
     * Returns the GC deletes cycle in milliseconds.
     */
    public long getGcDeletesInMillis() {
        return gcDeletesInMillis;
    }

    /**
     * Returns <code>true</code> iff delete garbage collection in the engine should be enabled. This setting is updateable
     * in realtime and forces a volatile read. Consumers can safely read this value directly go fetch it's latest value. The default is <code>true</code>
     * <p>
     *     Engine GC deletion if enabled collects deleted documents from in-memory realtime data structures after a certain amount of
     *     time ({@link #getGcDeletesInMillis()} if enabled. Before deletes are GCed they will cause re-adding the document that was deleted
     *     to fail.
     * </p>
     */
    public boolean isEnableGcDeletes() {
        return enableGcDeletes;
    }

    /**
     * Returns the {@link Codec} used in the engines {@link org.apache.lucene.index.IndexWriter}
     * <p>
     *     Note: this settings is only read on startup.
     * </p>
     */
    public Codec getCodec() {
        return codecService.codec(codecName);
    }

    /**
     * Returns a thread-pool mainly used to get estimated time stamps from {@link org.elasticsearch.threadpool.ThreadPool#estimatedTimeInMillis()} and to schedule
     * async force merge calls on the {@link org.elasticsearch.threadpool.ThreadPool.Names#FORCE_MERGE} thread-pool
     */
    public ThreadPool getThreadPool() {
        return threadPool;
    }

    /**
     * Returns an {@link org.elasticsearch.index.engine.Engine.Warmer} used to warm new searchers before they are used for searching.
     */
    public Engine.Warmer getWarmer() {
        return warmer;
    }

    /**
     * Returns the {@link org.elasticsearch.index.store.Store} instance that provides access to the {@link org.apache.lucene.store.Directory}
     * used for the engines {@link org.apache.lucene.index.IndexWriter} to write it's index files to.
     * <p>
     * Note: In order to use this instance the consumer needs to increment the stores reference before it's used the first time and hold
     * it's reference until it's not needed anymore.
     * </p>
     */
    public Store getStore() {
        return store;
    }

    /**
     * Returns a {@link SnapshotDeletionPolicy} used in the engines
     * {@link org.apache.lucene.index.IndexWriter}.
     */
    public SnapshotDeletionPolicy getDeletionPolicy() {
        return deletionPolicy;
    }

    /**
     * Returns the {@link org.apache.lucene.index.MergePolicy} for the engines {@link org.apache.lucene.index.IndexWriter}
     */
    public MergePolicy getMergePolicy() {
        return mergePolicy;
    }

    /**
     * Returns the {@link MergeSchedulerConfig}
     */
    public MergeSchedulerConfig getMergeSchedulerConfig() {
        return mergeSchedulerConfig;
    }

    /**
     * Returns a listener that should be called on engine failure
     */
    public Engine.EventListener getEventListener() {
        return eventListener;
    }

    /**
     * Returns the index settings for this index.
     */
    public IndexSettings getIndexSettings() {
        return indexSettings;
    }

    /**
     * Returns the engines shard ID
     */
    public ShardId getShardId() { return shardId; }

    /**
     * Returns the analyzer as the default analyzer in the engines {@link org.apache.lucene.index.IndexWriter}
     */
    public Analyzer getAnalyzer() {
        return analyzer;
    }

    /**
     * Returns the {@link org.apache.lucene.search.similarities.Similarity} used for indexing and searching.
     */
    public Similarity getSimilarity() {
        return similarity;
    }

    /**
     * Sets the GC deletes cycle in milliseconds.
     */
    public void setGcDeletesInMillis(long gcDeletesInMillis) {
        this.gcDeletesInMillis = gcDeletesInMillis;
    }

    /**
     * Returns the {@link org.elasticsearch.index.shard.TranslogRecoveryPerformer} for this engine. This class is used
     * to apply transaction log operations to the engine. It encapsulates all the logic to transfer the translog entry into
     * an indexing operation.
     */
    public TranslogRecoveryPerformer getTranslogRecoveryPerformer() {
        return translogRecoveryPerformer;
    }

    /**
     * Return the cache to use for queries.
     */
    public QueryCache getQueryCache() {
        return queryCache;
    }

    /**
     * Return the policy to use when caching queries.
     */
    public QueryCachingPolicy getQueryCachingPolicy() {
        return queryCachingPolicy;
    }

    /**
     * Returns the translog config for this engine
     */
    public TranslogConfig getTranslogConfig() {
        return translogConfig;
    }

    /**
     * Iff set to <code>true</code> the engine will create a new lucene index when opening the engine.
     * Otherwise the lucene index writer is opened in append mode. The default is <code>false</code>
     */
    public void setCreate(boolean create) {
        this.create = create;
    }

    /**
     * Iff <code>true</code> the engine should create a new lucene index when opening the engine.
     * Otherwise the lucene index writer should be opened in append mode. The default is <code>false</code>
     */
    public boolean isCreate() {
        return create;
    }

    /**
     * Returns a {@link TimeValue} at what time interval after the last write modification to the engine finished merges
     * should be automatically flushed. This is used to free up transient disk usage of potentially large segments that
     * are written after the engine became inactive from an indexing perspective.
     */
    public TimeValue getFlushMergesAfter() { return flushMergesAfter; }

}
