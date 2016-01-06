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

package org.elasticsearch.index.shard;

import org.apache.lucene.index.CheckIndex;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.KeepOnlyLastCommitDeletionPolicy;
import org.apache.lucene.index.SnapshotDeletionPolicy;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.QueryCachingPolicy;
import org.apache.lucene.search.UsageTrackingQueryCachingPolicy;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.util.CloseableThreadLocal;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.ThreadInterruptedException;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.admin.indices.flush.FlushRequest;
import org.elasticsearch.action.admin.indices.forcemerge.ForceMergeRequest;
import org.elasticsearch.action.admin.indices.upgrade.post.UpgradeRequest;
import org.elasticsearch.action.termvectors.TermVectorsRequest;
import org.elasticsearch.action.termvectors.TermVectorsResponse;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.common.Booleans;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.support.LoggerMessageFormat;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.metrics.MeanMetric;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.Callback;
import org.elasticsearch.common.util.concurrent.AbstractRefCounted;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.FutureUtils;
import org.elasticsearch.gateway.MetaDataStateFormat;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.NodeServicesProvider;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.cache.IndexCache;
import org.elasticsearch.index.cache.bitset.ShardBitsetFilterCache;
import org.elasticsearch.index.cache.query.QueryCacheStats;
import org.elasticsearch.index.cache.request.ShardRequestCache;
import org.elasticsearch.index.codec.CodecService;
import org.elasticsearch.index.engine.CommitStats;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.EngineClosedException;
import org.elasticsearch.index.engine.EngineConfig;
import org.elasticsearch.index.engine.EngineException;
import org.elasticsearch.index.engine.EngineFactory;
import org.elasticsearch.index.engine.InternalEngineFactory;
import org.elasticsearch.index.engine.RefreshFailedEngineException;
import org.elasticsearch.index.engine.Segment;
import org.elasticsearch.index.engine.SegmentsStats;
import org.elasticsearch.index.fielddata.FieldDataStats;
import org.elasticsearch.index.fielddata.IndexFieldDataService;
import org.elasticsearch.index.fielddata.ShardFieldData;
import org.elasticsearch.index.flush.FlushStats;
import org.elasticsearch.index.get.GetStats;
import org.elasticsearch.index.get.ShardGetService;
import org.elasticsearch.index.indexing.IndexingStats;
import org.elasticsearch.index.indexing.ShardIndexingService;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.DocumentMapperForType;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.mapper.SourceToParse;
import org.elasticsearch.index.mapper.Uid;
import org.elasticsearch.index.merge.MergeStats;
import org.elasticsearch.index.percolator.PercolateStats;
import org.elasticsearch.index.percolator.PercolatorQueriesRegistry;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.index.recovery.RecoveryStats;
import org.elasticsearch.index.refresh.RefreshStats;
import org.elasticsearch.index.search.stats.SearchStats;
import org.elasticsearch.index.search.stats.ShardSearchStats;
import org.elasticsearch.index.similarity.SimilarityService;
import org.elasticsearch.index.snapshots.IndexShardRepository;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.store.Store.MetadataSnapshot;
import org.elasticsearch.index.store.StoreFileMetaData;
import org.elasticsearch.index.store.StoreStats;
import org.elasticsearch.index.suggest.stats.ShardSuggestMetric;
import org.elasticsearch.index.suggest.stats.SuggestStats;
import org.elasticsearch.index.termvectors.TermVectorsService;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.index.translog.TranslogConfig;
import org.elasticsearch.index.translog.TranslogStats;
import org.elasticsearch.index.warmer.ShardIndexWarmerService;
import org.elasticsearch.index.warmer.WarmerStats;
import org.elasticsearch.indices.IndicesWarmer;
import org.elasticsearch.indices.cache.query.IndicesQueryCache;
import org.elasticsearch.indices.memory.IndexingMemoryController;
import org.elasticsearch.indices.recovery.RecoveryFailedException;
import org.elasticsearch.indices.recovery.RecoveryState;
import org.elasticsearch.percolator.PercolatorService;
import org.elasticsearch.search.suggest.completion.CompletionFieldStats;
import org.elasticsearch.search.suggest.completion.CompletionStats;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.io.PrintStream;
import java.nio.channels.ClosedByInterruptException;
import java.nio.charset.StandardCharsets;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;


public class IndexShard extends AbstractIndexShardComponent {

    private final ThreadPool threadPool;
    private final MapperService mapperService;
    private final IndexCache indexCache;
    private final Store store;
    private final MergeSchedulerConfig mergeSchedulerConfig;
    private final ShardIndexingService indexingService;
    private final ShardSearchStats searchService;
    private final ShardGetService getService;
    private final ShardIndexWarmerService shardWarmerService;
    private final ShardRequestCache shardQueryCache;
    private final ShardFieldData shardFieldData;
    private final PercolatorQueriesRegistry percolatorQueriesRegistry;
    private final TermVectorsService termVectorsService;
    private final IndexFieldDataService indexFieldDataService;
    private final ShardSuggestMetric shardSuggestMetric = new ShardSuggestMetric();
    private final ShardBitsetFilterCache shardBitsetFilterCache;
    private final Object mutex = new Object();
    private final String checkIndexOnStartup;
    private final CodecService codecService;
    private final IndicesWarmer warmer;
    private final SnapshotDeletionPolicy deletionPolicy;
    private final SimilarityService similarityService;
    private final EngineConfig engineConfig;
    private final TranslogConfig translogConfig;
    private final MergePolicyConfig mergePolicyConfig;
    private final IndicesQueryCache indicesQueryCache;
    private final IndexEventListener indexEventListener;
    private final IndexSettings idxSettings;
    private final NodeServicesProvider provider;

    private TimeValue refreshInterval;

    private volatile ScheduledFuture<?> refreshScheduledFuture;
    protected volatile ShardRouting shardRouting;
    protected volatile IndexShardState state;
    protected final AtomicReference<Engine> currentEngineReference = new AtomicReference<>();
    protected final EngineFactory engineFactory;

    @Nullable
    private RecoveryState recoveryState;

    private final RecoveryStats recoveryStats = new RecoveryStats();
    private final MeanMetric refreshMetric = new MeanMetric();
    private final MeanMetric flushMetric = new MeanMetric();

    private final ShardEventListener shardEventListener = new ShardEventListener();
    private volatile boolean flushOnClose = true;
    private volatile ByteSizeValue flushThresholdSize;

    /**
     * Index setting to control if a flush is executed before engine is closed
     * This setting is realtime updateable.
     */
    public static final String INDEX_FLUSH_ON_CLOSE = "index.flush_on_close";
    public static final String INDEX_TRANSLOG_FLUSH_THRESHOLD_SIZE = "index.translog.flush_threshold_size";
    /** If we see no indexing operations after this much time for a given shard, we consider that shard inactive (default: 5 minutes). */
    public static final String INDEX_SHARD_INACTIVE_TIME_SETTING = "index.shard.inactive_time";
    private static final String INDICES_INACTIVE_TIME_SETTING = "indices.memory.shard_inactive_time";

    private final ShardPath path;

    private final IndexShardOperationCounter indexShardOperationCounter;

    private final EnumSet<IndexShardState> readAllowedStates = EnumSet.of(IndexShardState.STARTED, IndexShardState.RELOCATED, IndexShardState.POST_RECOVERY);

    private final IndexSearcherWrapper searcherWrapper;
    private final TimeValue inactiveTime;

    /**
     * True if this shard is still indexing (recently) and false if we've been idle for long enough (as periodically checked by {@link
     * IndexingMemoryController}).
     */
    private final AtomicBoolean active = new AtomicBoolean();

    public IndexShard(ShardId shardId, IndexSettings indexSettings, ShardPath path, Store store, IndexCache indexCache,
                      MapperService mapperService, SimilarityService similarityService, IndexFieldDataService indexFieldDataService,
                      @Nullable EngineFactory engineFactory,
                      IndexEventListener indexEventListener, IndexSearcherWrapper indexSearcherWrapper, NodeServicesProvider provider) {
        super(shardId, indexSettings);
        final Settings settings = indexSettings.getSettings();
        this.inactiveTime = settings.getAsTime(INDEX_SHARD_INACTIVE_TIME_SETTING, settings.getAsTime(INDICES_INACTIVE_TIME_SETTING, TimeValue.timeValueMinutes(5)));
        this.idxSettings = indexSettings;
        this.codecService = new CodecService(mapperService, logger);
        this.warmer = provider.getWarmer();
        this.deletionPolicy = new SnapshotDeletionPolicy(new KeepOnlyLastCommitDeletionPolicy());
        this.similarityService = similarityService;
        Objects.requireNonNull(store, "Store must be provided to the index shard");
        this.engineFactory = engineFactory == null ? new InternalEngineFactory() : engineFactory;
        this.store = store;
        this.indexEventListener = indexEventListener;
        this.mergeSchedulerConfig = new MergeSchedulerConfig(indexSettings);
        this.threadPool = provider.getThreadPool();
        this.mapperService = mapperService;
        this.indexCache = indexCache;
        this.indexingService = new ShardIndexingService(shardId, indexSettings);
        this.getService = new ShardGetService(indexSettings, this, mapperService);
        this.termVectorsService = provider.getTermVectorsService();
        this.searchService = new ShardSearchStats(settings);
        this.shardWarmerService = new ShardIndexWarmerService(shardId, indexSettings);
        this.indicesQueryCache = provider.getIndicesQueryCache();
        this.shardQueryCache = new ShardRequestCache(shardId, indexSettings);
        this.shardFieldData = new ShardFieldData();
        this.indexFieldDataService = indexFieldDataService;
        this.shardBitsetFilterCache = new ShardBitsetFilterCache(shardId, indexSettings);
        state = IndexShardState.CREATED;
        this.refreshInterval = settings.getAsTime(INDEX_REFRESH_INTERVAL, EngineConfig.DEFAULT_REFRESH_INTERVAL);
        this.flushOnClose = settings.getAsBoolean(INDEX_FLUSH_ON_CLOSE, true);
        this.path = path;
        this.mergePolicyConfig = new MergePolicyConfig(logger, settings);
        /* create engine config */
        logger.debug("state: [CREATED]");

        this.checkIndexOnStartup = settings.get("index.shard.check_on_startup", "false");
        this.translogConfig = new TranslogConfig(shardId, shardPath().resolveTranslog(), indexSettings,
            provider.getBigArrays());
        final QueryCachingPolicy cachingPolicy;
        // the query cache is a node-level thing, however we want the most popular filters
        // to be computed on a per-shard basis
        if (settings.getAsBoolean(IndexModule.QUERY_CACHE_EVERYTHING, false)) {
            cachingPolicy = QueryCachingPolicy.ALWAYS_CACHE;
        } else {
            cachingPolicy = new UsageTrackingQueryCachingPolicy();
        }

        this.engineConfig = newEngineConfig(translogConfig, cachingPolicy);
        this.flushThresholdSize = settings.getAsBytesSize(INDEX_TRANSLOG_FLUSH_THRESHOLD_SIZE, new ByteSizeValue(512, ByteSizeUnit.MB));
        this.indexShardOperationCounter = new IndexShardOperationCounter(logger, shardId);
        this.provider = provider;
        this.searcherWrapper = indexSearcherWrapper;
        this.percolatorQueriesRegistry = new PercolatorQueriesRegistry(shardId, indexSettings, indexingService, mapperService, newQueryShardContext(), indexFieldDataService);
        if (mapperService.hasMapping(PercolatorService.TYPE_NAME)) {
            percolatorQueriesRegistry.enableRealTimePercolator();
        }


        // We start up inactive
        active.set(false);
    }

    public Store store() {
        return this.store;
    }

    public IndexSettings getIndexSettings() {
        return idxSettings;
    }

    /** returns true if this shard supports indexing (i.e., write) operations. */
    public boolean canIndex() {
        return true;
    }

    public ShardIndexingService indexingService() {
        return this.indexingService;
    }

    public ShardGetService getService() {
        return this.getService;
    }

    public ShardSuggestMetric getSuggestMetric() {
        return shardSuggestMetric;
    }

    public ShardBitsetFilterCache shardBitsetFilterCache() {
        return shardBitsetFilterCache;
    }

    public IndexFieldDataService indexFieldDataService() {
        return indexFieldDataService;
    }

    public MapperService mapperService() {
        return mapperService;
    }

    public ShardSearchStats searchService() {
        return this.searchService;
    }

    public ShardIndexWarmerService warmerService() {
        return this.shardWarmerService;
    }

    public ShardRequestCache requestCache() {
        return this.shardQueryCache;
    }

    public ShardFieldData fieldData() {
        return this.shardFieldData;
    }

    /**
     * Returns the latest cluster routing entry received with this shard. Might be null if the
     * shard was just created.
     */
    public ShardRouting routingEntry() {
        return this.shardRouting;
    }

    public QueryCachingPolicy getQueryCachingPolicy() {
        return this.engineConfig.getQueryCachingPolicy();
    }

    /**
     * Updates the shards routing entry. This mutate the shards internal state depending
     * on the changes that get introduced by the new routing value. This method will persist shard level metadata
     * unless explicitly disabled.
     */
    public void updateRoutingEntry(final ShardRouting newRouting, final boolean persistState) {
        final ShardRouting currentRouting = this.shardRouting;
        if (!newRouting.shardId().equals(shardId())) {
            throw new IllegalArgumentException("Trying to set a routing entry with shardId [" + newRouting.shardId() + "] on a shard with shardId [" + shardId() + "]");
        }
        if ((currentRouting == null || newRouting.isSameAllocation(currentRouting)) == false) {
            throw new IllegalArgumentException("Trying to set a routing entry with a different allocation. Current " + currentRouting + ", new " + newRouting);
        }
        try {
            if (currentRouting != null) {
                if (!newRouting.primary() && currentRouting.primary()) {
                    logger.warn("suspect illegal state: trying to move shard from primary mode to replica mode");
                }
                // if its the same routing except for some metadata info, return
                if (currentRouting.equalsIgnoringMetaData(newRouting)) {
                    this.shardRouting = newRouting; // might have a new version
                    return;
                }
            }

            if (state == IndexShardState.POST_RECOVERY) {
                // if the state is started or relocating (cause it might move right away from started to relocating)
                // then move to STARTED
                if (newRouting.state() == ShardRoutingState.STARTED || newRouting.state() == ShardRoutingState.RELOCATING) {
                    // we want to refresh *before* we move to internal STARTED state
                    try {
                        getEngine().refresh("cluster_state_started");
                    } catch (Throwable t) {
                        logger.debug("failed to refresh due to move to cluster wide started", t);
                    }

                    boolean movedToStarted = false;
                    synchronized (mutex) {
                        // do the check under a mutex, so we make sure to only change to STARTED if in POST_RECOVERY
                        if (state == IndexShardState.POST_RECOVERY) {
                            changeState(IndexShardState.STARTED, "global state is [" + newRouting.state() + "]");
                            movedToStarted = true;
                        } else {
                            logger.debug("state [{}] not changed, not in POST_RECOVERY, global state is [{}]", state, newRouting.state());
                        }
                    }
                    if (movedToStarted) {
                        indexEventListener.afterIndexShardStarted(this);
                    }
                }
            }
            this.shardRouting = newRouting;
            indexEventListener.shardRoutingChanged(this, currentRouting, newRouting);
        } finally {
            if (persistState) {
                persistMetadata(newRouting, currentRouting);
            }
        }
    }

    /**
     * Marks the shard as recovering based on a recovery state, fails with exception is recovering is not allowed to be set.
     */
    public IndexShardState markAsRecovering(String reason, RecoveryState recoveryState) throws IndexShardStartedException,
        IndexShardRelocatedException, IndexShardRecoveringException, IndexShardClosedException {
        synchronized (mutex) {
            if (state == IndexShardState.CLOSED) {
                throw new IndexShardClosedException(shardId);
            }
            if (state == IndexShardState.STARTED) {
                throw new IndexShardStartedException(shardId);
            }
            if (state == IndexShardState.RELOCATED) {
                throw new IndexShardRelocatedException(shardId);
            }
            if (state == IndexShardState.RECOVERING) {
                throw new IndexShardRecoveringException(shardId);
            }
            if (state == IndexShardState.POST_RECOVERY) {
                throw new IndexShardRecoveringException(shardId);
            }
            this.recoveryState = recoveryState;
            return changeState(IndexShardState.RECOVERING, reason);
        }
    }

    public IndexShard relocated(String reason) throws IndexShardNotStartedException {
        synchronized (mutex) {
            if (state != IndexShardState.STARTED) {
                throw new IndexShardNotStartedException(shardId, state);
            }
            changeState(IndexShardState.RELOCATED, reason);
        }
        return this;
    }

    public IndexShardState state() {
        return state;
    }

    /**
     * Changes the state of the current shard
     *
     * @param newState the new shard state
     * @param reason   the reason for the state change
     * @return the previous shard state
     */
    private IndexShardState changeState(IndexShardState newState, String reason) {
        logger.debug("state: [{}]->[{}], reason [{}]", state, newState, reason);
        IndexShardState previousState = state;
        state = newState;
        this.indexEventListener.indexShardStateChanged(this, previousState, newState, reason);
        return previousState;
    }

    public Engine.Index prepareIndexOnPrimary(SourceToParse source, long version, VersionType versionType) {
        try {
            if (shardRouting.primary() == false) {
                throw new IllegalIndexShardStateException(shardId, state, "shard is not a primary");
            }
            return prepareIndex(docMapper(source.type()), source, version, versionType, Engine.Operation.Origin.PRIMARY);
        } catch (Throwable t) {
            verifyNotClosed(t);
            throw t;
        }
    }

    public Engine.Index prepareIndexOnReplica(SourceToParse source, long version, VersionType versionType) {
        try {
            return prepareIndex(docMapper(source.type()), source, version, versionType, Engine.Operation.Origin.REPLICA);
        } catch (Throwable t) {
            verifyNotClosed(t);
            throw t;
        }
    }

    static Engine.Index prepareIndex(DocumentMapperForType docMapper, SourceToParse source, long version, VersionType versionType, Engine.Operation.Origin origin) {
        long startTime = System.nanoTime();
        ParsedDocument doc = docMapper.getDocumentMapper().parse(source);
        if (docMapper.getMapping() != null) {
            doc.addDynamicMappingsUpdate(docMapper.getMapping());
        }
        return new Engine.Index(docMapper.getDocumentMapper().uidMapper().term(doc.uid().stringValue()), doc, version, versionType, origin, startTime);
    }

    /**
     * Index a document and return whether it was created, as opposed to just
     * updated.
     */
    public boolean index(Engine.Index index) {
        ensureWriteAllowed(index);
        markLastWrite();
        index = indexingService.preIndex(index);
        final boolean created;
        try {
            if (logger.isTraceEnabled()) {
                logger.trace("index [{}][{}]{}", index.type(), index.id(), index.docs());
            }
            created = getEngine().index(index);
            index.endTime(System.nanoTime());
        } catch (Throwable ex) {
            indexingService.postIndex(index, ex);
            throw ex;
        }
        indexingService.postIndex(index);
        return created;
    }

    public Engine.Delete prepareDeleteOnPrimary(String type, String id, long version, VersionType versionType) {
        if (shardRouting.primary() == false) {
            throw new IllegalIndexShardStateException(shardId, state, "shard is not a primary");
        }
        final DocumentMapper documentMapper = docMapper(type).getDocumentMapper();
        return prepareDelete(type, id, documentMapper.uidMapper().term(Uid.createUid(type, id)), version, versionType, Engine.Operation.Origin.PRIMARY);
    }

    public Engine.Delete prepareDeleteOnReplica(String type, String id, long version, VersionType versionType) {
        final DocumentMapper documentMapper = docMapper(type).getDocumentMapper();
        return prepareDelete(type, id, documentMapper.uidMapper().term(Uid.createUid(type, id)), version, versionType, Engine.Operation.Origin.REPLICA);
    }

    static Engine.Delete prepareDelete(String type, String id, Term uid, long version, VersionType versionType, Engine.Operation.Origin origin) {
        long startTime = System.nanoTime();
        return new Engine.Delete(type, id, uid, version, versionType, origin, startTime, false);
    }


    public void delete(Engine.Delete delete) {
        ensureWriteAllowed(delete);
        markLastWrite();
        delete = indexingService.preDelete(delete);
        try {
            if (logger.isTraceEnabled()) {
                logger.trace("delete [{}]", delete.uid().text());
            }
            getEngine().delete(delete);
            delete.endTime(System.nanoTime());
        } catch (Throwable ex) {
            indexingService.postDelete(delete, ex);
            throw ex;
        }
        indexingService.postDelete(delete);
    }

    public Engine.GetResult get(Engine.Get get) {
        readAllowed();
        return getEngine().get(get, this::acquireSearcher);
    }

    public void refresh(String source) {
        verifyNotClosed();
        if (logger.isTraceEnabled()) {
            logger.trace("refresh with source: {}", source);
        }
        long time = System.nanoTime();
        getEngine().refresh(source);
        refreshMetric.inc(System.nanoTime() - time);
    }

    public RefreshStats refreshStats() {
        return new RefreshStats(refreshMetric.count(), TimeUnit.NANOSECONDS.toMillis(refreshMetric.sum()));
    }

    public FlushStats flushStats() {
        return new FlushStats(flushMetric.count(), TimeUnit.NANOSECONDS.toMillis(flushMetric.sum()));
    }

    public DocsStats docStats() {
        try (Engine.Searcher searcher = acquireSearcher("doc_stats")) {
            return new DocsStats(searcher.reader().numDocs(), searcher.reader().numDeletedDocs());
        }
    }

    /**
     * @return {@link CommitStats} if engine is open, otherwise null
     */
    @Nullable
    public CommitStats commitStats() {
        Engine engine = getEngineOrNull();
        return engine == null ? null : engine.commitStats();
    }

    public IndexingStats indexingStats(String... types) {
        return indexingService.stats(types);
    }

    public SearchStats searchStats(String... groups) {
        return searchService.stats(groups);
    }

    public GetStats getStats() {
        return getService.stats();
    }

    public StoreStats storeStats() {
        try {
            return store.stats();
        } catch (IOException e) {
            throw new ElasticsearchException("io exception while building 'store stats'", e);
        } catch (AlreadyClosedException ex) {
            return null; // already closed
        }
    }

    public MergeStats mergeStats() {
        final Engine engine = getEngineOrNull();
        if (engine == null) {
            return new MergeStats();
        }
        return engine.getMergeStats();
    }

    public SegmentsStats segmentStats() {
        SegmentsStats segmentsStats = getEngine().segmentsStats();
        segmentsStats.addBitsetMemoryInBytes(shardBitsetFilterCache.getMemorySizeInBytes());
        return segmentsStats;
    }

    public TermVectorsResponse getTermVectors(TermVectorsRequest request) {
        return this.termVectorsService.getTermVectors(this, request);
    }

    public WarmerStats warmerStats() {
        return shardWarmerService.stats();
    }

    public QueryCacheStats queryCacheStats() {
        return indicesQueryCache.getStats(shardId);
    }

    public FieldDataStats fieldDataStats(String... fields) {
        return shardFieldData.stats(fields);
    }

    public PercolatorQueriesRegistry percolateRegistry() {
        return percolatorQueriesRegistry;
    }

    public TranslogStats translogStats() {
        return getEngine().getTranslog().stats();
    }

    public SuggestStats suggestStats() {
        return shardSuggestMetric.stats();
    }

    public CompletionStats completionStats(String... fields) {
        CompletionStats completionStats = new CompletionStats();
        try (final Engine.Searcher currentSearcher = acquireSearcher("completion_stats")) {
            completionStats.add(CompletionFieldStats.completionStats(currentSearcher.reader(), fields));
        }
        return completionStats;
    }

    public Engine.SyncedFlushResult syncFlush(String syncId, Engine.CommitId expectedCommitId) {
        verifyStartedOrRecovering();
        logger.trace("trying to sync flush. sync id [{}]. expected commit id [{}]]", syncId, expectedCommitId);
        return getEngine().syncFlush(syncId, expectedCommitId);
    }

    public Engine.CommitId flush(FlushRequest request) throws ElasticsearchException {
        boolean waitIfOngoing = request.waitIfOngoing();
        boolean force = request.force();
        if (logger.isTraceEnabled()) {
            logger.trace("flush with {}", request);
        }
        // we allows flush while recovering, since we allow for operations to happen
        // while recovering, and we want to keep the translog at bay (up to deletes, which
        // we don't gc).
        verifyStartedOrRecovering();

        long time = System.nanoTime();
        Engine.CommitId commitId = getEngine().flush(force, waitIfOngoing);
        flushMetric.inc(System.nanoTime() - time);
        return commitId;

    }

    public void forceMerge(ForceMergeRequest forceMerge) throws IOException {
        verifyStarted();
        if (logger.isTraceEnabled()) {
            logger.trace("force merge with {}", forceMerge);
        }
        getEngine().forceMerge(forceMerge.flush(), forceMerge.maxNumSegments(),
            forceMerge.onlyExpungeDeletes(), false, false);
    }

    /**
     * Upgrades the shard to the current version of Lucene and returns the minimum segment version
     */
    public org.apache.lucene.util.Version upgrade(UpgradeRequest upgrade) throws IOException {
        verifyStarted();
        if (logger.isTraceEnabled()) {
            logger.trace("upgrade with {}", upgrade);
        }
        org.apache.lucene.util.Version previousVersion = minimumCompatibleVersion();
        // we just want to upgrade the segments, not actually forge merge to a single segment
        getEngine().forceMerge(true,  // we need to flush at the end to make sure the upgrade is durable
            Integer.MAX_VALUE, // we just want to upgrade the segments, not actually optimize to a single segment
            false, true, upgrade.upgradeOnlyAncientSegments());
        org.apache.lucene.util.Version version = minimumCompatibleVersion();
        if (logger.isTraceEnabled()) {
            logger.trace("upgraded segment {} from version {} to version {}", previousVersion, version);
        }

        return version;
    }

    public org.apache.lucene.util.Version minimumCompatibleVersion() {
        org.apache.lucene.util.Version luceneVersion = null;
        for (Segment segment : getEngine().segments(false)) {
            if (luceneVersion == null || luceneVersion.onOrAfter(segment.getVersion())) {
                luceneVersion = segment.getVersion();
            }
        }
        return luceneVersion == null ? idxSettings.getIndexVersionCreated().luceneVersion : luceneVersion;
    }

    /**
     * Creates a new {@link IndexCommit} snapshot form the currently running engine. All resources referenced by this
     * commit won't be freed until the commit / snapshot is released via {@link #releaseSnapshot(IndexCommit)}.
     *
     * @param flushFirst <code>true</code> if the index should first be flushed to disk / a low level lucene commit should be executed
     */
    public IndexCommit snapshotIndex(boolean flushFirst) throws EngineException {
        IndexShardState state = this.state; // one time volatile read
        // we allow snapshot on closed index shard, since we want to do one after we close the shard and before we close the engine
        if (state == IndexShardState.STARTED || state == IndexShardState.RELOCATED || state == IndexShardState.CLOSED) {
            return getEngine().snapshotIndex(flushFirst);
        } else {
            throw new IllegalIndexShardStateException(shardId, state, "snapshot is not allowed");
        }
    }


    /**
     * Releases a snapshot taken from {@link #snapshotIndex(boolean)} this must be called to release the resources
     * referenced by the given snapshot {@link IndexCommit}.
     */
    public void releaseSnapshot(IndexCommit snapshot) throws IOException {
        deletionPolicy.release(snapshot);
    }

    /**
     * Fails the shard and marks the shard store as corrupted if
     * <code>e</code> is caused by index corruption
     */
    public void failShard(String reason, @Nullable Throwable e) {
        // fail the engine. This will cause this shard to also be removed from the node's index service.
        getEngine().failEngine(reason, e);
    }

    public Engine.Searcher acquireSearcher(String source) {
        readAllowed();
        final Engine engine = getEngine();
        final Engine.Searcher searcher = engine.acquireSearcher(source);
        boolean success = false;
        try {
            final Engine.Searcher wrappedSearcher = searcherWrapper == null ? searcher : searcherWrapper.wrap(searcher);
            assert wrappedSearcher != null;
            success = true;
            return wrappedSearcher;
        } catch (IOException ex) {
            throw new ElasticsearchException("failed to wrap searcher", ex);
        } finally {
            if (success == false) {
                Releasables.close(success, searcher);
            }
        }
    }

    public void close(String reason, boolean flushEngine) throws IOException {
        synchronized (mutex) {
            try {
                if (state != IndexShardState.CLOSED) {
                    FutureUtils.cancel(refreshScheduledFuture);
                    refreshScheduledFuture = null;
                }
                changeState(IndexShardState.CLOSED, reason);
                indexShardOperationCounter.decRef();
            } finally {
                final Engine engine = this.currentEngineReference.getAndSet(null);
                try {
                    if (engine != null && flushEngine && this.flushOnClose) {
                        engine.flushAndClose();
                    }
                } finally { // playing safe here and close the engine even if the above succeeds - close can be called multiple times
                    IOUtils.close(engine, percolatorQueriesRegistry, queryShardContextCache);
                }
            }
        }
    }


    public IndexShard postRecovery(String reason) throws IndexShardStartedException, IndexShardRelocatedException, IndexShardClosedException {
        if (mapperService.hasMapping(PercolatorService.TYPE_NAME)) {
            refresh("percolator_load_queries");
            try (Engine.Searcher searcher = getEngine().acquireSearcher("percolator_load_queries")) {
                this.percolatorQueriesRegistry.loadQueries(searcher.reader());
            }
        }
        synchronized (mutex) {
            if (state == IndexShardState.CLOSED) {
                throw new IndexShardClosedException(shardId);
            }
            if (state == IndexShardState.STARTED) {
                throw new IndexShardStartedException(shardId);
            }
            if (state == IndexShardState.RELOCATED) {
                throw new IndexShardRelocatedException(shardId);
            }
            recoveryState.setStage(RecoveryState.Stage.DONE);
            changeState(IndexShardState.POST_RECOVERY, reason);
        }
        return this;
    }

    /**
     * called before starting to copy index files over
     */
    public void prepareForIndexRecovery() {
        if (state != IndexShardState.RECOVERING) {
            throw new IndexShardNotRecoveringException(shardId, state);
        }
        recoveryState.setStage(RecoveryState.Stage.INDEX);
        assert currentEngineReference.get() == null;
    }

    /**
     * Applies all operations in the iterable to the current engine and returns the number of operations applied.
     * This operation will stop applying operations once an operation failed to apply.
     * Note: This method is typically used in peer recovery to replay remote transaction log entries.
     */
    public int performBatchRecovery(Iterable<Translog.Operation> operations) {
        if (state != IndexShardState.RECOVERING) {
            throw new IndexShardNotRecoveringException(shardId, state);
        }
        return engineConfig.getTranslogRecoveryPerformer().performBatchRecovery(getEngine(), operations);
    }

    /**
     * After the store has been recovered, we need to start the engine in order to apply operations
     */
    public void performTranslogRecovery(boolean indexExists) {
        internalPerformTranslogRecovery(false, indexExists);
        assert recoveryState.getStage() == RecoveryState.Stage.TRANSLOG : "TRANSLOG stage expected but was: " + recoveryState.getStage();
    }

    private void internalPerformTranslogRecovery(boolean skipTranslogRecovery, boolean indexExists) {
        if (state != IndexShardState.RECOVERING) {
            throw new IndexShardNotRecoveringException(shardId, state);
        }
        recoveryState.setStage(RecoveryState.Stage.VERIFY_INDEX);
        // also check here, before we apply the translog
        if (Booleans.parseBoolean(checkIndexOnStartup, false)) {
            try {
                checkIndex();
            } catch (IOException ex) {
                throw new RecoveryFailedException(recoveryState, "check index failed", ex);
            }
        }
        recoveryState.setStage(RecoveryState.Stage.TRANSLOG);
        // we disable deletes since we allow for operations to be executed against the shard while recovering
        // but we need to make sure we don't loose deletes until we are done recovering
        engineConfig.setEnableGcDeletes(false);
        engineConfig.setCreate(indexExists == false);
        createNewEngine(skipTranslogRecovery, engineConfig);

    }

    /**
     * After the store has been recovered, we need to start the engine. This method starts a new engine but skips
     * the replay of the transaction log which is required in cases where we restore a previous index or recover from
     * a remote peer.
     */
    public void skipTranslogRecovery() throws IOException {
        assert getEngineOrNull() == null : "engine was already created";
        internalPerformTranslogRecovery(true, true);
        assert recoveryState.getTranslog().recoveredOperations() == 0;
    }

    /**
     * called if recovery has to be restarted after network error / delay **
     */
    public void performRecoveryRestart() throws IOException {
        synchronized (mutex) {
            if (state != IndexShardState.RECOVERING) {
                throw new IndexShardNotRecoveringException(shardId, state);
            }
            final Engine engine = this.currentEngineReference.getAndSet(null);
            IOUtils.close(engine);
            recoveryState().setStage(RecoveryState.Stage.INIT);
        }
    }

    /**
     * returns stats about ongoing recoveries, both source and target
     */
    public RecoveryStats recoveryStats() {
        return recoveryStats;
    }

    /**
     * Returns the current {@link RecoveryState} if this shard is recovering or has been recovering.
     * Returns null if the recovery has not yet started or shard was not recovered (created via an API).
     */
    public RecoveryState recoveryState() {
        return this.recoveryState;
    }

    /**
     * perform the last stages of recovery once all translog operations are done.
     * note that you should still call {@link #postRecovery(String)}.
     */
    public void finalizeRecovery() {
        recoveryState().setStage(RecoveryState.Stage.FINALIZE);
        getEngine().refresh("recovery_finalization");
        startScheduledTasksIfNeeded();
        engineConfig.setEnableGcDeletes(true);
    }

    /**
     * Returns <tt>true</tt> if this shard can ignore a recovery attempt made to it (since the already doing/done it)
     */
    public boolean ignoreRecoveryAttempt() {
        IndexShardState state = state(); // one time volatile read
        return state == IndexShardState.POST_RECOVERY || state == IndexShardState.RECOVERING || state == IndexShardState.STARTED ||
            state == IndexShardState.RELOCATED || state == IndexShardState.CLOSED;
    }

    public void readAllowed() throws IllegalIndexShardStateException {
        IndexShardState state = this.state; // one time volatile read
        if (readAllowedStates.contains(state) == false) {
            throw new IllegalIndexShardStateException(shardId, state, "operations only allowed when shard state is one of " + readAllowedStates.toString());
        }
    }

    /** Records timestamp of the last write operation, possibly switching {@code active} to true if we were inactive. */
    private void markLastWrite() {
        if (active.getAndSet(true) == false) {
            indexEventListener.onShardActive(this);
        }
    }

    private void ensureWriteAllowed(Engine.Operation op) throws IllegalIndexShardStateException {
        Engine.Operation.Origin origin = op.origin();
        IndexShardState state = this.state; // one time volatile read

        if (origin == Engine.Operation.Origin.PRIMARY) {
            // for primaries, we only allow to write when actually started (so the cluster has decided we started)
            // otherwise, we need to retry, we also want to still allow to index if we are relocated in case it fails
            if (state != IndexShardState.STARTED && state != IndexShardState.RELOCATED) {
                throw new IllegalIndexShardStateException(shardId, state, "operation only allowed when started/recovering, origin [" + origin + "]");
            }
        } else {
            // for replicas, we allow to write also while recovering, since we index also during recovery to replicas
            // and rely on version checks to make sure its consistent
            if (state != IndexShardState.STARTED && state != IndexShardState.RELOCATED && state != IndexShardState.RECOVERING && state != IndexShardState.POST_RECOVERY) {
                throw new IllegalIndexShardStateException(shardId, state, "operation only allowed when started/recovering, origin [" + origin + "]");
            }
        }
    }

    protected final void verifyStartedOrRecovering() throws IllegalIndexShardStateException {
        IndexShardState state = this.state; // one time volatile read
        if (state != IndexShardState.STARTED && state != IndexShardState.RECOVERING && state != IndexShardState.POST_RECOVERY) {
            throw new IllegalIndexShardStateException(shardId, state, "operation only allowed when started/recovering");
        }
    }

    private void verifyNotClosed() throws IllegalIndexShardStateException {
        verifyNotClosed(null);
    }

    private void verifyNotClosed(Throwable suppressed) throws IllegalIndexShardStateException {
        IndexShardState state = this.state; // one time volatile read
        if (state == IndexShardState.CLOSED) {
            final IllegalIndexShardStateException exc = new IllegalIndexShardStateException(shardId, state, "operation only allowed when not closed");
            if (suppressed != null) {
                exc.addSuppressed(suppressed);
            }
            throw exc;
        }
    }

    protected final void verifyStarted() throws IllegalIndexShardStateException {
        IndexShardState state = this.state; // one time volatile read
        if (state != IndexShardState.STARTED) {
            throw new IndexShardNotStartedException(shardId, state);
        }
    }

    private void startScheduledTasksIfNeeded() {
        if (refreshInterval.millis() > 0) {
            refreshScheduledFuture = threadPool.schedule(refreshInterval, ThreadPool.Names.SAME, new EngineRefresher());
            logger.debug("scheduling refresher every {}", refreshInterval);
        } else {
            logger.debug("scheduled refresher disabled");
        }
    }

    public static final String INDEX_REFRESH_INTERVAL = "index.refresh_interval";

    public void addShardFailureCallback(Callback<ShardFailure> onShardFailure) {
        this.shardEventListener.delegates.add(onShardFailure);
    }

    /**
     * Change the indexing and translog buffer sizes.  If {@code IndexWriter} is currently using more than
     * the new buffering indexing size then we do a refresh to free up the heap.
     */
    public void updateBufferSize(ByteSizeValue shardIndexingBufferSize) {

        final EngineConfig config = engineConfig;
        final ByteSizeValue preValue = config.getIndexingBufferSize();

        config.setIndexingBufferSize(shardIndexingBufferSize);

        Engine engine = getEngineOrNull();
        if (engine == null) {
            logger.debug("updateBufferSize: engine is closed; skipping");
            return;
        }

        // update engine if it is already started.
        if (preValue.bytes() != shardIndexingBufferSize.bytes()) {
            // so we push changes these changes down to IndexWriter:
            engine.onSettingsChanged();

            long iwBytesUsed = engine.indexWriterRAMBytesUsed();

            String message = LoggerMessageFormat.format("updating index_buffer_size from [{}] to [{}]; IndexWriter now using [{}] bytes",
                preValue, shardIndexingBufferSize, iwBytesUsed);

            if (iwBytesUsed > shardIndexingBufferSize.bytes()) {
                // our allowed buffer was changed to less than we are currently using; we ask IW to refresh
                // so it clears its buffers (otherwise it won't clear until the next indexing/delete op)
                logger.debug(message + "; now refresh to clear IndexWriter memory");

                // TODO: should IW have an API to move segments to disk, but not refresh?  Its flush method is protected...
                try {
                    refresh("update index buffer");
                } catch (Throwable e) {
                    logger.warn("failed to refresh after decreasing index buffer", e);
                }
            } else {
                logger.debug(message);
            }
        }
    }

    /**
     * Called by {@link IndexingMemoryController} to check whether more than {@code inactiveTimeNS} has passed since the last
     * indexing operation, and become inactive (reducing indexing and translog buffers to tiny values) if so.  This returns true
     * if the shard is inactive.
     */
    public boolean checkIdle() {
        return checkIdle(inactiveTime.nanos());
    }

    final boolean checkIdle(long inactiveTimeNS) { // pkg private for testing
        Engine engineOrNull = getEngineOrNull();
        if (engineOrNull != null && System.nanoTime() - engineOrNull.getLastWriteNanos() >= inactiveTimeNS) {
            boolean wasActive = active.getAndSet(false);
            if (wasActive) {
                updateBufferSize(IndexingMemoryController.INACTIVE_SHARD_INDEXING_BUFFER);
                logger.debug("marking shard as inactive (inactive_time=[{}]) indexing wise", inactiveTime);
                indexEventListener.onShardInactive(this);
            }
        }

        return active.get() == false;
    }

    /**
     * Returns {@code true} if this shard is active (has seen indexing ops in the last {@link
     * IndexShard#INDEX_SHARD_INACTIVE_TIME_SETTING} (default 5 minutes), else {@code false}.
     */
    public boolean getActive() {
        return active.get();
    }

    public final boolean isFlushOnClose() {
        return flushOnClose;
    }

    /**
     * Deletes the shards metadata state. This method can only be executed if the shard is not active.
     *
     * @throws IOException if the delete fails
     */
    public void deleteShardState() throws IOException {
        if (this.routingEntry() != null && this.routingEntry().active()) {
            throw new IllegalStateException("Can't delete shard state on an active shard");
        }
        MetaDataStateFormat.deleteMetaState(shardPath().getDataPath());
    }

    public ShardPath shardPath() {
        return path;
    }

    public boolean recoverFromStore(DiscoveryNode localNode) {
        // we are the first primary, recover from the gateway
        // if its post api allocation, the index should exists
        assert shardRouting.primary() : "recover from store only makes sense if the shard is a primary shard";
        boolean shouldExist = shardRouting.allocatedPostIndexCreate(idxSettings.getIndexMetaData());

        StoreRecovery storeRecovery = new StoreRecovery(shardId, logger);
        return storeRecovery.recoverFromStore(this, shouldExist, localNode);
    }

    public boolean restoreFromRepository(IndexShardRepository repository, DiscoveryNode localNode) {
        assert shardRouting.primary() : "recover from store only makes sense if the shard is a primary shard";
        StoreRecovery storeRecovery = new StoreRecovery(shardId, logger);
        return storeRecovery.recoverFromRepository(this, repository, localNode);
    }

    /**
     * Returns <code>true</code> iff this shard needs to be flushed due to too many translog operation or a too large transaction log.
     * Otherwise <code>false</code>.
     */
    boolean shouldFlush() {
        Engine engine = getEngineOrNull();
        if (engine != null) {
            try {
                Translog translog = engine.getTranslog();
                return translog.sizeInBytes() > flushThresholdSize.bytes();
            } catch (AlreadyClosedException | EngineClosedException ex) {
                // that's fine we are already close - no need to flush
            }
        }
        return false;
    }

    public void onRefreshSettings(Settings settings) {
        boolean change = false;
        synchronized (mutex) {
            if (state() == IndexShardState.CLOSED) { // no need to update anything if we are closed
                return;
            }
            ByteSizeValue flushThresholdSize = settings.getAsBytesSize(INDEX_TRANSLOG_FLUSH_THRESHOLD_SIZE, this.flushThresholdSize);
            if (!flushThresholdSize.equals(this.flushThresholdSize)) {
                logger.info("updating flush_threshold_size from [{}] to [{}]", this.flushThresholdSize, flushThresholdSize);
                this.flushThresholdSize = flushThresholdSize;
            }

            final EngineConfig config = engineConfig;
            final boolean flushOnClose = settings.getAsBoolean(INDEX_FLUSH_ON_CLOSE, this.flushOnClose);
            if (flushOnClose != this.flushOnClose) {
                logger.info("updating {} from [{}] to [{}]", INDEX_FLUSH_ON_CLOSE, this.flushOnClose, flushOnClose);
                this.flushOnClose = flushOnClose;
            }

            TimeValue refreshInterval = settings.getAsTime(INDEX_REFRESH_INTERVAL, this.refreshInterval);
            if (!refreshInterval.equals(this.refreshInterval)) {
                logger.info("updating refresh_interval from [{}] to [{}]", this.refreshInterval, refreshInterval);
                if (refreshScheduledFuture != null) {
                    // NOTE: we pass false here so we do NOT attempt Thread.interrupt if EngineRefresher.run is currently running.  This is
                    // very important, because doing so can cause files to suddenly be closed if they were doing IO when the interrupt
                    // hit.  See https://issues.apache.org/jira/browse/LUCENE-2239
                    FutureUtils.cancel(refreshScheduledFuture);
                    refreshScheduledFuture = null;
                }
                this.refreshInterval = refreshInterval;
                if (refreshInterval.millis() > 0) {
                    refreshScheduledFuture = threadPool.schedule(refreshInterval, ThreadPool.Names.SAME, new EngineRefresher());
                }
            }

            long gcDeletesInMillis = settings.getAsTime(EngineConfig.INDEX_GC_DELETES_SETTING, TimeValue.timeValueMillis(config.getGcDeletesInMillis())).millis();
            if (gcDeletesInMillis != config.getGcDeletesInMillis()) {
                logger.info("updating {} from [{}] to [{}]", EngineConfig.INDEX_GC_DELETES_SETTING, TimeValue.timeValueMillis(config.getGcDeletesInMillis()), TimeValue.timeValueMillis(gcDeletesInMillis));
                config.setGcDeletesInMillis(gcDeletesInMillis);
                change = true;
            }

            final String versionMapSize = settings.get(EngineConfig.INDEX_VERSION_MAP_SIZE, config.getVersionMapSizeSetting());
            if (config.getVersionMapSizeSetting().equals(versionMapSize) == false) {
                config.setVersionMapSizeSetting(versionMapSize);
            }

            final int maxThreadCount = settings.getAsInt(MergeSchedulerConfig.MAX_THREAD_COUNT, mergeSchedulerConfig.getMaxThreadCount());
            if (maxThreadCount != mergeSchedulerConfig.getMaxThreadCount()) {
                logger.info("updating [{}] from [{}] to [{}]", MergeSchedulerConfig.MAX_THREAD_COUNT, mergeSchedulerConfig.getMaxMergeCount(), maxThreadCount);
                mergeSchedulerConfig.setMaxThreadCount(maxThreadCount);
                change = true;
            }

            final int maxMergeCount = settings.getAsInt(MergeSchedulerConfig.MAX_MERGE_COUNT, mergeSchedulerConfig.getMaxMergeCount());
            if (maxMergeCount != mergeSchedulerConfig.getMaxMergeCount()) {
                logger.info("updating [{}] from [{}] to [{}]", MergeSchedulerConfig.MAX_MERGE_COUNT, mergeSchedulerConfig.getMaxMergeCount(), maxMergeCount);
                mergeSchedulerConfig.setMaxMergeCount(maxMergeCount);
                change = true;
            }

            final boolean autoThrottle = settings.getAsBoolean(MergeSchedulerConfig.AUTO_THROTTLE, mergeSchedulerConfig.isAutoThrottle());
            if (autoThrottle != mergeSchedulerConfig.isAutoThrottle()) {
                logger.info("updating [{}] from [{}] to [{}]", MergeSchedulerConfig.AUTO_THROTTLE, mergeSchedulerConfig.isAutoThrottle(), autoThrottle);
                mergeSchedulerConfig.setAutoThrottle(autoThrottle);
                change = true;
            }
        }
        mergePolicyConfig.onRefreshSettings(settings);
        searchService.onRefreshSettings(settings);
        indexingService.onRefreshSettings(settings);
        if (change) {
            getEngine().onSettingsChanged();
        }
    }

    public Translog.View acquireTranslogView() {
        Engine engine = getEngine();
        assert engine.getTranslog() != null : "translog must not be null";
        return engine.getTranslog().newView();
    }

    public List<Segment> segments(boolean verbose) {
        return getEngine().segments(verbose);
    }

    public void flushAndCloseEngine() throws IOException {
        getEngine().flushAndClose();
    }

    public Translog getTranslog() {
        return getEngine().getTranslog();
    }

    public PercolateStats percolateStats() {
        return percolatorQueriesRegistry.stats();
    }

    public IndexEventListener getIndexEventListener() {
        return indexEventListener;
    }

    public TimeValue getInactiveTime() {
        return inactiveTime;
    }

    class EngineRefresher implements Runnable {
        @Override
        public void run() {
            // we check before if a refresh is needed, if not, we reschedule, otherwise, we fork, refresh, and then reschedule
            if (!getEngine().refreshNeeded()) {
                reschedule();
                return;
            }
            threadPool.executor(ThreadPool.Names.REFRESH).execute(new Runnable() {
                @Override
                public void run() {
                    try {
                        if (getEngine().refreshNeeded()) {
                            refresh("schedule");
                        }
                    } catch (EngineClosedException e) {
                        // we are being closed, ignore
                    } catch (RefreshFailedEngineException e) {
                        if (e.getCause() instanceof InterruptedException) {
                            // ignore, we are being shutdown
                        } else if (e.getCause() instanceof ClosedByInterruptException) {
                            // ignore, we are being shutdown
                        } else if (e.getCause() instanceof ThreadInterruptedException) {
                            // ignore, we are being shutdown
                        } else {
                            if (state != IndexShardState.CLOSED) {
                                logger.warn("Failed to perform scheduled engine refresh", e);
                            }
                        }
                    } catch (Exception e) {
                        if (state != IndexShardState.CLOSED) {
                            logger.warn("Failed to perform scheduled engine refresh", e);
                        }
                    }

                    reschedule();
                }
            });
        }

        /**
         * Schedules another (future) refresh, if refresh_interval is still enabled.
         */
        private void reschedule() {
            synchronized (mutex) {
                if (state != IndexShardState.CLOSED && refreshInterval.millis() > 0) {
                    refreshScheduledFuture = threadPool.schedule(refreshInterval, ThreadPool.Names.SAME, this);
                }
            }
        }
    }

    private void checkIndex() throws IOException {
        if (store.tryIncRef()) {
            try {
                doCheckIndex();
            } finally {
                store.decRef();
            }
        }
    }

    private void doCheckIndex() throws IOException {
        long timeNS = System.nanoTime();
        if (!Lucene.indexExists(store.directory())) {
            return;
        }
        BytesStreamOutput os = new BytesStreamOutput();
        PrintStream out = new PrintStream(os, false, StandardCharsets.UTF_8.name());

        if ("checksum".equalsIgnoreCase(checkIndexOnStartup)) {
            // physical verification only: verify all checksums for the latest commit
            IOException corrupt = null;
            MetadataSnapshot metadata = store.getMetadata();
            for (Map.Entry<String, StoreFileMetaData> entry : metadata.asMap().entrySet()) {
                try {
                    Store.checkIntegrity(entry.getValue(), store.directory());
                    out.println("checksum passed: " + entry.getKey());
                } catch (IOException exc) {
                    out.println("checksum failed: " + entry.getKey());
                    exc.printStackTrace(out);
                    corrupt = exc;
                }
            }
            out.flush();
            if (corrupt != null) {
                logger.warn("check index [failure]\n{}", new String(os.bytes().toBytes(), StandardCharsets.UTF_8));
                throw corrupt;
            }
        } else {
            // full checkindex
            try (CheckIndex checkIndex = new CheckIndex(store.directory())) {
                checkIndex.setInfoStream(out);
                CheckIndex.Status status = checkIndex.checkIndex();
                out.flush();

                if (!status.clean) {
                    if (state == IndexShardState.CLOSED) {
                        // ignore if closed....
                        return;
                    }
                    logger.warn("check index [failure]\n{}", new String(os.bytes().toBytes(), StandardCharsets.UTF_8));
                    if ("fix".equalsIgnoreCase(checkIndexOnStartup)) {
                        if (logger.isDebugEnabled()) {
                            logger.debug("fixing index, writing new segments file ...");
                        }
                        checkIndex.exorciseIndex(status);
                        if (logger.isDebugEnabled()) {
                            logger.debug("index fixed, wrote new segments file \"{}\"", status.segmentsFileName);
                        }
                    } else {
                        // only throw a failure if we are not going to fix the index
                        throw new IllegalStateException("index check failure but can't fix it");
                    }
                }
            }
        }

        if (logger.isDebugEnabled()) {
            logger.debug("check index [success]\n{}", new String(os.bytes().toBytes(), StandardCharsets.UTF_8));
        }

        recoveryState.getVerifyIndex().checkIndexTime(Math.max(0, TimeValue.nsecToMSec(System.nanoTime() - timeNS)));
    }

    Engine getEngine() {
        Engine engine = getEngineOrNull();
        if (engine == null) {
            throw new EngineClosedException(shardId);
        }
        return engine;
    }

    /**
     * NOTE: returns null if engine is not yet started (e.g. recovery phase 1, copying over index files, is still running), or if engine is
     * closed.
     */
    protected Engine getEngineOrNull() {
        return this.currentEngineReference.get();
    }

    class ShardEventListener implements Engine.EventListener {
        private final CopyOnWriteArrayList<Callback<ShardFailure>> delegates = new CopyOnWriteArrayList<>();

        // called by the current engine
        @Override
        public void onFailedEngine(String reason, @Nullable Throwable failure) {
            final ShardFailure shardFailure = new ShardFailure(shardRouting, reason, failure, getIndexUUID());
            for (Callback<ShardFailure> listener : delegates) {
                try {
                    listener.handle(shardFailure);
                } catch (Exception e) {
                    logger.warn("exception while notifying engine failure", e);
                }
            }
        }
    }

    private void createNewEngine(boolean skipTranslogRecovery, EngineConfig config) {
        synchronized (mutex) {
            if (state == IndexShardState.CLOSED) {
                throw new EngineClosedException(shardId);
            }
            assert this.currentEngineReference.get() == null;
            this.currentEngineReference.set(newEngine(skipTranslogRecovery, config));
        }
    }

    protected Engine newEngine(boolean skipTranslogRecovery, EngineConfig config) {
        return engineFactory.newReadWriteEngine(config, skipTranslogRecovery);
    }

    /**
     * Returns <code>true</code> iff this shard allows primary promotion, otherwise <code>false</code>
     */
    public boolean allowsPrimaryPromotion() {
        return true;
    }

    // pkg private for testing
    void persistMetadata(ShardRouting newRouting, ShardRouting currentRouting) {
        assert newRouting != null : "newRouting must not be null";
        if (newRouting.active()) {
            try {
                final String writeReason;
                if (currentRouting == null) {
                    writeReason = "freshly started, version [" + newRouting.version() + "]";
                } else if (currentRouting.version() < newRouting.version()) {
                    writeReason = "version changed from [" + currentRouting.version() + "] to [" + newRouting.version() + "]";
                } else if (currentRouting.equals(newRouting) == false) {
                    writeReason = "routing changed from " + currentRouting + " to " + newRouting;
                } else {
                    logger.trace("skip writing shard state, has been written before; previous version:  [" +
                        currentRouting.version() + "] current version [" + newRouting.version() + "]");
                    assert currentRouting.version() <= newRouting.version() : "version should not go backwards for shardID: " + shardId +
                        " previous version:  [" + currentRouting.version() + "] current version [" + newRouting.version() + "]";
                    return;
                }
                final ShardStateMetaData newShardStateMetadata = new ShardStateMetaData(newRouting.version(), newRouting.primary(), getIndexUUID(), newRouting.allocationId());
                logger.trace("{} writing shard state, reason [{}]", shardId, writeReason);
                ShardStateMetaData.FORMAT.write(newShardStateMetadata, newShardStateMetadata.version, shardPath().getShardStatePath());
            } catch (IOException e) { // this is how we used to handle it.... :(
                logger.warn("failed to write shard state", e);
                // we failed to write the shard state, we will try and write
                // it next time...
            }
        }
    }

    private String getIndexUUID() {
        return indexSettings.getUUID();
    }

    private DocumentMapperForType docMapper(String type) {
        return mapperService.documentMapperWithAutoCreate(type);
    }

    private final EngineConfig newEngineConfig(TranslogConfig translogConfig, QueryCachingPolicy cachingPolicy) {
        final TranslogRecoveryPerformer translogRecoveryPerformer = new TranslogRecoveryPerformer(shardId, mapperService, logger) {
            @Override
            protected void operationProcessed() {
                assert recoveryState != null;
                recoveryState.getTranslog().incrementRecoveredOperations();
            }
        };
        final Engine.Warmer engineWarmer = (searcher, toLevel) -> warmer.warm(searcher, this, idxSettings, toLevel);
        return new EngineConfig(shardId,
            threadPool, indexingService, indexSettings, engineWarmer, store, deletionPolicy, mergePolicyConfig.getMergePolicy(), mergeSchedulerConfig,
            mapperService.indexAnalyzer(), similarityService.similarity(mapperService), codecService, shardEventListener, translogRecoveryPerformer, indexCache.query(), cachingPolicy, translogConfig, inactiveTime);
    }

    private static class IndexShardOperationCounter extends AbstractRefCounted {
        final private ESLogger logger;
        private final ShardId shardId;

        public IndexShardOperationCounter(ESLogger logger, ShardId shardId) {
            super("index-shard-operations-counter");
            this.logger = logger;
            this.shardId = shardId;
        }

        @Override
        protected void closeInternal() {
            logger.debug("operations counter reached 0, will not accept any further writes");
        }

        @Override
        protected void alreadyClosed() {
            throw new IndexShardClosedException(shardId, "could not increment operation counter. shard is closed.");
        }
    }

    public void incrementOperationCounter() {
        indexShardOperationCounter.incRef();
    }

    public void decrementOperationCounter() {
        indexShardOperationCounter.decRef();
    }

    public int getOperationsCount() {
        return Math.max(0, indexShardOperationCounter.refCount() - 1); // refCount is incremented on creation and decremented on close
    }

    /**
     * Syncs the given location with the underlying storage unless already synced.
     */
    public void sync(Translog.Location location) {
        try {
            final Engine engine = getEngine();
            engine.getTranslog().ensureSynced(location);
        } catch (EngineClosedException ex) {
            // that's fine since we already synced everything on engine close - this also is conform with the methods documentation
        } catch (IOException ex) { // if this fails we are in deep shit - fail the request
            logger.debug("failed to sync translog", ex);
            throw new ElasticsearchException("failed to sync translog", ex);
        }
    }

    /**
     * Returns the current translog durability mode
     */
    public Translog.Durability getTranslogDurability() {
        return indexSettings.getTranslogDurability();
    }

    private final AtomicBoolean asyncFlushRunning = new AtomicBoolean();

    /**
     * Schedules a flush if needed but won't schedule more than one flush concurrently. The flush will be executed on the
     * Flush thread-pool asynchronously.
     *
     * @return <code>true</code> if a new flush is scheduled otherwise <code>false</code>.
     */
    public boolean maybeFlush() {
        if (shouldFlush()) {
            if (asyncFlushRunning.compareAndSet(false, true)) { // we can't use a lock here since we "release" in a different thread
                if (shouldFlush() == false) {
                    // we have to check again since otherwise there is a race when a thread passes
                    // the first shouldFlush() check next to another thread which flushes fast enough
                    // to finish before the current thread could flip the asyncFlushRunning flag.
                    // in that situation we have an extra unexpected flush.
                    asyncFlushRunning.compareAndSet(true, false);
                } else {
                    logger.debug("submitting async flush request");
                    final AbstractRunnable abstractRunnable = new AbstractRunnable() {
                        @Override
                        public void onFailure(Throwable t) {
                            if (state != IndexShardState.CLOSED) {
                                logger.warn("failed to flush index", t);
                            }
                        }

                        @Override
                        protected void doRun() throws Exception {
                            flush(new FlushRequest());
                        }

                        @Override
                        public void onAfter() {
                            asyncFlushRunning.compareAndSet(true, false);
                            maybeFlush(); // fire a flush up again if we have filled up the limits such that shouldFlush() returns true
                        }
                    };
                    threadPool.executor(ThreadPool.Names.FLUSH).execute(abstractRunnable);
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Simple struct encapsulating a shard failure
     *
     * @see IndexShard#addShardFailureCallback(Callback)
     */
    public static final class ShardFailure {
        public final ShardRouting routing;
        public final String reason;
        @Nullable
        public final Throwable cause;
        public final String indexUUID;

        public ShardFailure(ShardRouting routing, String reason, @Nullable Throwable cause, String indexUUID) {
            this.routing = routing;
            this.reason = reason;
            this.cause = cause;
            this.indexUUID = indexUUID;
        }
    }

    private CloseableThreadLocal<QueryShardContext> queryShardContextCache = new CloseableThreadLocal<QueryShardContext>() {
        // TODO We should get rid of this threadlocal but I think it should be a sep change
        @Override
        protected QueryShardContext initialValue() {
            return newQueryShardContext();
        }
    };

    private QueryShardContext newQueryShardContext() {
        return new QueryShardContext(idxSettings, provider.getClient(), indexCache.bitsetFilterCache(), indexFieldDataService, mapperService, similarityService, provider.getScriptService(), provider.getIndicesQueriesRegistry());
    }

    /**
     * Returns a threadlocal QueryShardContext for this shard.
     */
    public QueryShardContext getQueryShardContext() {
        return queryShardContextCache.get();
    }

    EngineFactory getEngineFactory() {
        return engineFactory;
    }

}
