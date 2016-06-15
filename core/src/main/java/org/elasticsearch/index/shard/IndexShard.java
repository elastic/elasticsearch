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

import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.index.CheckIndex;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.KeepOnlyLastCommitDeletionPolicy;
import org.apache.lucene.index.SnapshotDeletionPolicy;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryCachingPolicy;
import org.apache.lucene.search.UsageTrackingQueryCachingPolicy;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.ThreadInterruptedException;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.admin.indices.flush.FlushRequest;
import org.elasticsearch.action.admin.indices.forcemerge.ForceMergeRequest;
import org.elasticsearch.action.admin.indices.upgrade.post.UpgradeRequest;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.common.Booleans;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.LoggerMessageFormat;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.metrics.MeanMetric;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.Callback;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.SuspendableRefContainer;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.cache.IndexCache;
import org.elasticsearch.index.cache.bitset.ShardBitsetFilterCache;
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
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.DocumentMapperForType;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.mapper.SourceToParse;
import org.elasticsearch.index.mapper.Uid;
import org.elasticsearch.index.merge.MergeStats;
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
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.index.translog.TranslogConfig;
import org.elasticsearch.index.translog.TranslogStats;
import org.elasticsearch.index.warmer.ShardIndexWarmerService;
import org.elasticsearch.index.warmer.WarmerStats;
import org.elasticsearch.indices.IndexingMemoryController;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.cluster.IndicesClusterStateService;
import org.elasticsearch.indices.recovery.RecoveryFailedException;
import org.elasticsearch.indices.recovery.RecoveryState;
import org.elasticsearch.indices.recovery.RecoveryTargetService;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.search.suggest.completion.CompletionFieldStats;
import org.elasticsearch.search.suggest.completion.CompletionStats;
import org.elasticsearch.search.suggest.completion2x.Completion090PostingsFormat;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.io.PrintStream;
import java.nio.channels.ClosedByInterruptException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

public class IndexShard extends AbstractIndexShardComponent implements IndicesClusterStateService.Shard {

    private final ThreadPool threadPool;
    private final MapperService mapperService;
    private final IndexCache indexCache;
    private final Store store;
    private final InternalIndexingStats internalIndexingStats;
    private final ShardSearchStats searchStats = new ShardSearchStats();
    private final ShardGetService getService;
    private final ShardIndexWarmerService shardWarmerService;
    private final ShardRequestCache requestCacheStats;
    private final ShardFieldData shardFieldData;
    private final IndexFieldDataService indexFieldDataService;
    private final ShardBitsetFilterCache shardBitsetFilterCache;
    private final Object mutex = new Object();
    private final String checkIndexOnStartup;
    private final CodecService codecService;
    private final Engine.Warmer warmer;
    private final SnapshotDeletionPolicy deletionPolicy;
    private final SimilarityService similarityService;
    private final TranslogConfig translogConfig;
    private final IndexEventListener indexEventListener;
    private final QueryCachingPolicy cachingPolicy;


    /**
     * How many bytes we are currently moving to disk, via either IndexWriter.flush or refresh.  IndexingMemoryController polls this
     * across all shards to decide if throttling is necessary because moving bytes to disk is falling behind vs incoming documents
     * being indexed/deleted.
     */
    private final AtomicLong writingBytes = new AtomicLong();
    private final SearchOperationListener searchOperationListener;

    protected volatile ShardRouting shardRouting;
    protected volatile IndexShardState state;
    protected volatile long primaryTerm;
    protected final AtomicReference<Engine> currentEngineReference = new AtomicReference<>();
    protected final EngineFactory engineFactory;

    private final IndexingOperationListener indexingOperationListeners;

    @Nullable
    private RecoveryState recoveryState;

    private final RecoveryStats recoveryStats = new RecoveryStats();
    private final MeanMetric refreshMetric = new MeanMetric();
    private final MeanMetric flushMetric = new MeanMetric();

    private final ShardEventListener shardEventListener = new ShardEventListener();

    private final ShardPath path;

    private final SuspendableRefContainer suspendableRefContainer;

    private static final EnumSet<IndexShardState> readAllowedStates = EnumSet.of(IndexShardState.STARTED, IndexShardState.RELOCATED, IndexShardState.POST_RECOVERY);
    // for primaries, we only allow to write when actually started (so the cluster has decided we started)
    // in case we have a relocation of a primary, we also allow to write after phase 2 completed, where the shard may be
    // in state RECOVERING or POST_RECOVERY. After a primary has been marked as RELOCATED, we only allow writes to the relocation target
    // which can be either in POST_RECOVERY or already STARTED (this prevents writing concurrently to two primaries).
    public static final EnumSet<IndexShardState> writeAllowedStatesForPrimary = EnumSet.of(IndexShardState.RECOVERING, IndexShardState.POST_RECOVERY, IndexShardState.STARTED);
    // replication is also allowed while recovering, since we index also during recovery to replicas and rely on version checks to make sure its consistent
    // a relocated shard can also be target of a replication if the relocation target has not been marked as active yet and is syncing it's changes back to the relocation source
    private static final EnumSet<IndexShardState> writeAllowedStatesForReplica = EnumSet.of(IndexShardState.RECOVERING, IndexShardState.POST_RECOVERY, IndexShardState.STARTED, IndexShardState.RELOCATED);

    private final IndexSearcherWrapper searcherWrapper;
    /**
     * True if this shard is still indexing (recently) and false if we've been idle for long enough (as periodically checked by {@link
     * IndexingMemoryController}).
     */
    private final AtomicBoolean active = new AtomicBoolean();
    /**
     * Allows for the registration of listeners that are called when a change becomes visible for search. This is nullable because
     * {@linkplain ShadowIndexShard} doesn't support this.
     */
    @Nullable
    private final RefreshListeners refreshListeners;

    public IndexShard(ShardRouting shardRouting, IndexSettings indexSettings, ShardPath path, Store store, IndexCache indexCache,
                      MapperService mapperService, SimilarityService similarityService, IndexFieldDataService indexFieldDataService,
                      @Nullable EngineFactory engineFactory,
                      IndexEventListener indexEventListener, IndexSearcherWrapper indexSearcherWrapper, ThreadPool threadPool, BigArrays bigArrays,
                      Engine.Warmer warmer, List<SearchOperationListener> searchOperationListener, List<IndexingOperationListener> listeners) throws IOException {
        super(shardRouting.shardId(), indexSettings);
        assert shardRouting.initializing();
        this.shardRouting = shardRouting;
        final Settings settings = indexSettings.getSettings();
        this.codecService = new CodecService(mapperService, logger);
        this.warmer = warmer;
        this.deletionPolicy = new SnapshotDeletionPolicy(new KeepOnlyLastCommitDeletionPolicy());
        this.similarityService = similarityService;
        Objects.requireNonNull(store, "Store must be provided to the index shard");
        this.engineFactory = engineFactory == null ? new InternalEngineFactory() : engineFactory;
        this.store = store;
        this.indexEventListener = indexEventListener;
        this.threadPool = threadPool;
        this.mapperService = mapperService;
        this.indexCache = indexCache;
        this.internalIndexingStats = new InternalIndexingStats();
        final List<IndexingOperationListener> listenersList = new ArrayList<>(listeners);
        listenersList.add(internalIndexingStats);
        this.indexingOperationListeners = new IndexingOperationListener.CompositeListener(listenersList, logger);
        final List<SearchOperationListener> searchListenersList = new ArrayList<>(searchOperationListener);
        searchListenersList.add(searchStats);
        this.searchOperationListener = new SearchOperationListener.CompositeListener(searchListenersList, logger);
        this.getService = new ShardGetService(indexSettings, this, mapperService);
        this.shardWarmerService = new ShardIndexWarmerService(shardId, indexSettings);
        this.requestCacheStats = new ShardRequestCache();
        this.shardFieldData = new ShardFieldData();
        this.indexFieldDataService = indexFieldDataService;
        this.shardBitsetFilterCache = new ShardBitsetFilterCache(shardId, indexSettings);
        state = IndexShardState.CREATED;
        this.path = path;
        /* create engine config */
        logger.debug("state: [CREATED]");

        this.checkIndexOnStartup = indexSettings.getValue(IndexSettings.INDEX_CHECK_ON_STARTUP);
        this.translogConfig = new TranslogConfig(shardId, shardPath().resolveTranslog(), indexSettings,
            bigArrays);
        // the query cache is a node-level thing, however we want the most popular filters
        // to be computed on a per-shard basis
        if (IndexModule.INDEX_QUERY_CACHE_EVERYTHING_SETTING.get(settings)) {
            cachingPolicy = QueryCachingPolicy.ALWAYS_CACHE;
        } else {
            cachingPolicy = new UsageTrackingQueryCachingPolicy();
        }
        suspendableRefContainer = new SuspendableRefContainer();
        searcherWrapper = indexSearcherWrapper;
        primaryTerm = indexSettings.getIndexMetaData().primaryTerm(shardId.id());
        refreshListeners = buildRefreshListeners();
        persistMetadata(shardRouting, null);
    }

    public Store store() {
        return this.store;
    }

    /**
     * returns true if this shard supports indexing (i.e., write) operations.
     */
    public boolean canIndex() {
        return true;
    }

    public ShardGetService getService() {
        return this.getService;
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

    public SearchOperationListener getSearchOperationListener() {
        return this.searchOperationListener;
    }

    public ShardIndexWarmerService warmerService() {
        return this.shardWarmerService;
    }

    public ShardRequestCache requestCache() {
        return this.requestCacheStats;
    }

    public ShardFieldData fieldData() {
        return this.shardFieldData;
    }


    /**
     * Returns the primary term the index shard is on. See {@link org.elasticsearch.cluster.metadata.IndexMetaData#primaryTerm(int)}
     */
    public long getPrimaryTerm() {
        return this.primaryTerm;
    }

    /**
     * notifies the shard of an increase in the primary term
     */
    public void updatePrimaryTerm(final long newTerm) {
        synchronized (mutex) {
            if (newTerm != primaryTerm) {
                assert shardRouting.primary() == false : "a primary shard should never update it's term. shard: " + shardRouting
                    + " current term [" + primaryTerm + "] new term [" + newTerm + "]";
                assert newTerm > primaryTerm : "primary terms can only go up. current [" + primaryTerm + "], new [" + newTerm + "]";
                primaryTerm = newTerm;
            }
        }


    }

    /**
     * Returns the latest cluster routing entry received with this shard.
     */
    @Override
    public ShardRouting routingEntry() {
        return this.shardRouting;
    }

    public QueryCachingPolicy getQueryCachingPolicy() {
        return cachingPolicy;
    }

    /**
     * Updates the shards routing entry. This mutate the shards internal state depending
     * on the changes that get introduced by the new routing value. This method will persist shard level metadata.
     *
     * @throws IndexShardRelocatedException if shard is marked as relocated and relocation aborted
     * @throws IOException                  if shard state could not be persisted
     */
    public void updateRoutingEntry(final ShardRouting newRouting) throws IOException {
        final ShardRouting currentRouting = this.shardRouting;
        if (!newRouting.shardId().equals(shardId())) {
            throw new IllegalArgumentException("Trying to set a routing entry with shardId " + newRouting.shardId() + " on a shard with shardId " + shardId() + "");
        }
        if ((currentRouting == null || newRouting.isSameAllocation(currentRouting)) == false) {
            throw new IllegalArgumentException("Trying to set a routing entry with a different allocation. Current " + currentRouting + ", new " + newRouting);
        }
        if (currentRouting != null) {
            if (!newRouting.primary() && currentRouting.primary()) {
                logger.warn("suspect illegal state: trying to move shard from primary mode to replica mode");
            }
            // if its the same routing, return
            if (currentRouting.equals(newRouting)) {
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

        if (state == IndexShardState.RELOCATED &&
            (newRouting.relocating() == false || newRouting.equalsIgnoringMetaData(currentRouting) == false)) {
            // if the shard is marked as RELOCATED we have to fail when any changes in shard routing occur (e.g. due to recovery
            // failure / cancellation). The reason is that at the moment we cannot safely move back to STARTED without risking two
            // active primaries.
            throw new IndexShardRelocatedException(shardId(), "Shard is marked as relocated, cannot safely move to state " + newRouting.state());
        }
        this.shardRouting = newRouting;
        indexEventListener.shardRoutingChanged(this, currentRouting, newRouting);
        persistMetadata(newRouting, currentRouting);
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
        try (Releasable block = suspendableRefContainer.blockAcquisition()) {
            // no shard operation locks are being held here, move state from started to relocated
            synchronized (mutex) {
                if (state != IndexShardState.STARTED) {
                    throw new IndexShardNotStartedException(shardId, state);
                }
                changeState(IndexShardState.RELOCATED, reason);
            }
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
            verifyPrimary();
            return prepareIndex(docMapper(source.type()), source, version, versionType, Engine.Operation.Origin.PRIMARY);
        } catch (Throwable t) {
            verifyNotClosed(t);
            throw t;
        }
    }

    public Engine.Index prepareIndexOnReplica(SourceToParse source, long version, VersionType versionType) {
        try {
            verifyReplicationTarget();
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
        MappedFieldType uidFieldType = docMapper.getDocumentMapper().uidMapper().fieldType();
        Query uidQuery = uidFieldType.termQuery(doc.uid(), null);
        Term uid = MappedFieldType.extractTerm(uidQuery);
        return new Engine.Index(uid, doc, version, versionType, origin, startTime);
    }

    /**
     * Index a document and return whether it was created, as opposed to just
     * updated.
     */
    public boolean index(Engine.Index index) {
        ensureWriteAllowed(index);
        Engine engine = getEngine();
        return index(engine, index);
    }

    private boolean index(Engine engine, Engine.Index index) {
        active.set(true);
        index = indexingOperationListeners.preIndex(index);
        final boolean created;
        try {
            if (logger.isTraceEnabled()) {
                logger.trace("index [{}][{}]{}", index.type(), index.id(), index.docs());
            }
            created = engine.index(index);
            index.endTime(System.nanoTime());
        } catch (Throwable ex) {
            indexingOperationListeners.postIndex(index, ex);
            throw ex;
        }

        indexingOperationListeners.postIndex(index, created);

        return created;
    }

    public Engine.Delete prepareDeleteOnPrimary(String type, String id, long version, VersionType versionType) {
        verifyPrimary();
        final DocumentMapper documentMapper = docMapper(type).getDocumentMapper();
        final MappedFieldType uidFieldType = documentMapper.uidMapper().fieldType();
        final Query uidQuery = uidFieldType.termQuery(Uid.createUid(type, id), null);
        final Term uid = MappedFieldType.extractTerm(uidQuery);
        return prepareDelete(type, id, uid, version, versionType, Engine.Operation.Origin.PRIMARY);
    }

    public Engine.Delete prepareDeleteOnReplica(String type, String id, long version, VersionType versionType) {
        final DocumentMapper documentMapper = docMapper(type).getDocumentMapper();
        final MappedFieldType uidFieldType = documentMapper.uidMapper().fieldType();
        final Query uidQuery = uidFieldType.termQuery(Uid.createUid(type, id), null);
        final Term uid = MappedFieldType.extractTerm(uidQuery);
        return prepareDelete(type, id, uid, version, versionType, Engine.Operation.Origin.REPLICA);
    }

    static Engine.Delete prepareDelete(String type, String id, Term uid, long version, VersionType versionType, Engine.Operation.Origin origin) {
        long startTime = System.nanoTime();
        return new Engine.Delete(type, id, uid, version, versionType, origin, startTime, false);
    }

    public void delete(Engine.Delete delete) {
        ensureWriteAllowed(delete);
        Engine engine = getEngine();
        delete(engine, delete);
    }

    private void delete(Engine engine, Engine.Delete delete) {
        active.set(true);
        delete = indexingOperationListeners.preDelete(delete);
        try {
            if (logger.isTraceEnabled()) {
                logger.trace("delete [{}]", delete.uid().text());
            }
            engine.delete(delete);
            delete.endTime(System.nanoTime());
        } catch (Throwable ex) {
            indexingOperationListeners.postDelete(delete, ex);
            throw ex;
        }

        indexingOperationListeners.postDelete(delete);
    }

    public Engine.GetResult get(Engine.Get get) {
        readAllowed();
        return getEngine().get(get, this::acquireSearcher);
    }

    /**
     * Writes all indexing changes to disk and opens a new searcher reflecting all changes.  This can throw {@link EngineClosedException}.
     */
    public void refresh(String source) {
        verifyNotClosed();

        if (canIndex()) {
            long bytes = getEngine().getIndexBufferRAMBytesUsed();
            writingBytes.addAndGet(bytes);
            try {
                if (logger.isTraceEnabled()) {
                    logger.trace("refresh with source [{}] indexBufferRAMBytesUsed [{}]", source, new ByteSizeValue(bytes));
                }
                long time = System.nanoTime();
                getEngine().refresh(source);
                refreshMetric.inc(System.nanoTime() - time);
            } finally {
                if (logger.isTraceEnabled()) {
                    logger.trace("remove [{}] writing bytes for shard [{}]", new ByteSizeValue(bytes), shardId());
                }
                writingBytes.addAndGet(-bytes);
            }
        } else {
            if (logger.isTraceEnabled()) {
                logger.trace("refresh with source [{}]", source);
            }
            long time = System.nanoTime();
            getEngine().refresh(source);
            refreshMetric.inc(System.nanoTime() - time);
        }
    }

    /**
     * Returns how many bytes we are currently moving from heap to disk
     */
    public long getWritingBytes() {
        return writingBytes.get();
    }

    public RefreshStats refreshStats() {
        return new RefreshStats(refreshMetric.count(), TimeUnit.NANOSECONDS.toMillis(refreshMetric.sum()));
    }

    public FlushStats flushStats() {
        return new FlushStats(flushMetric.count(), TimeUnit.NANOSECONDS.toMillis(flushMetric.sum()));
    }

    public DocsStats docStats() {
        readAllowed();
        final Engine engine = getEngine();
        return engine.getDocStats();
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
        Engine engine = getEngineOrNull();
        final boolean throttled;
        final long throttleTimeInMillis;
        if (engine == null) {
            throttled = false;
            throttleTimeInMillis = 0;
        } else {
            throttled = engine.isThrottled();
            throttleTimeInMillis = engine.getIndexThrottleTimeInMillis();
        }
        return internalIndexingStats.stats(throttled, throttleTimeInMillis, types);
    }

    public SearchStats searchStats(String... groups) {
        return searchStats.stats(groups);
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

    public SegmentsStats segmentStats(boolean includeSegmentFileSizes) {
        SegmentsStats segmentsStats = getEngine().segmentsStats(includeSegmentFileSizes);
        segmentsStats.addBitsetMemoryInBytes(shardBitsetFilterCache.getMemorySizeInBytes());
        return segmentsStats;
    }

    public WarmerStats warmerStats() {
        return shardWarmerService.stats();
    }

    public FieldDataStats fieldDataStats(String... fields) {
        return shardFieldData.stats(fields);
    }

    public TranslogStats translogStats() {
        return getEngine().getTranslog().stats();
    }

    public CompletionStats completionStats(String... fields) {
        CompletionStats completionStats = new CompletionStats();
        try (final Engine.Searcher currentSearcher = acquireSearcher("completion_stats")) {
            completionStats.add(CompletionFieldStats.completionStats(currentSearcher.reader(), fields));
            // Necessary for 2.x shards:
            Completion090PostingsFormat postingsFormat = ((Completion090PostingsFormat)
                PostingsFormat.forName(Completion090PostingsFormat.CODEC_NAME));
            completionStats.add(postingsFormat.completionStats(currentSearcher.reader(), fields));
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
            logger.trace("upgraded segments for {} from version {} to version {}", shardId, previousVersion, version);
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
        return luceneVersion == null ? indexSettings.getIndexVersionCreated().luceneVersion : luceneVersion;
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
                changeState(IndexShardState.CLOSED, reason);
            } finally {
                final Engine engine = this.currentEngineReference.getAndSet(null);
                try {
                    if (engine != null && flushEngine) {
                        engine.flushAndClose();
                    }
                } finally { // playing safe here and close the engine even if the above succeeds - close can be called multiple times
                    IOUtils.close(engine);
                }
            }
        }
    }

    public IndexShard postRecovery(String reason) throws IndexShardStartedException, IndexShardRelocatedException, IndexShardClosedException {
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
        // We set active because we are now writing operations to the engine; this way, if we go idle after some time and become inactive,
        // we still invoke any onShardInactive listeners ... we won't sync'd flush in this case because we only do that on primary and this
        // is a replica
        active.set(true);
        Engine engine = getEngine();
        return engine.config().getTranslogRecoveryPerformer().performBatchRecovery(engine, operations);
    }

    /**
     * After the store has been recovered, we need to start the engine in order to apply operations
     */
    public void performTranslogRecovery(boolean indexExists) throws IOException {
        if (indexExists == false) {
            // note: these are set when recovering from the translog
            final RecoveryState.Translog translogStats = recoveryState().getTranslog();
            translogStats.totalOperations(0);
            translogStats.totalOperationsOnStart(0);
        }
        internalPerformTranslogRecovery(false, indexExists);
        assert recoveryState.getStage() == RecoveryState.Stage.TRANSLOG : "TRANSLOG stage expected but was: " + recoveryState.getStage();
    }

    private void internalPerformTranslogRecovery(boolean skipTranslogRecovery, boolean indexExists) throws IOException {
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
        final EngineConfig.OpenMode openMode;
        /* by default we recover and index and replay the translog but if the index
         * doesn't exist we create everything from the scratch. Yet, if the index
         * doesn't exist we don't need to worry about the skipTranslogRecovery since
         * there is no translog on a non-existing index.
         * The skipTranslogRecovery invariant is used if we do remote recovery since
         * there the translog isn't local but on the remote host, hence we can skip it.
         */
        if (indexExists == false) {
            openMode = EngineConfig.OpenMode.CREATE_INDEX_AND_TRANSLOG;
        } else if (skipTranslogRecovery) {
            openMode = EngineConfig.OpenMode.OPEN_INDEX_CREATE_TRANSLOG;
        } else {
            openMode = EngineConfig.OpenMode.OPEN_INDEX_AND_TRANSLOG;
        }
        final EngineConfig config = newEngineConfig(openMode);
        // we disable deletes since we allow for operations to be executed against the shard while recovering
        // but we need to make sure we don't loose deletes until we are done recovering
        config.setEnableGcDeletes(false);
        Engine newEngine = createNewEngine(config);
        verifyNotClosed();
        if (openMode == EngineConfig.OpenMode.OPEN_INDEX_AND_TRANSLOG) {
            // We set active because we are now writing operations to the engine; this way, if we go idle after some time and become inactive,
            // we still give sync'd flush a chance to run:
            active.set(true);
            newEngine.recoverFromTranslog();
        }

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
        Engine engine = getEngine();
        engine.refresh("recovery_finalization");
        engine.config().setEnableGcDeletes(true);
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

    private void ensureWriteAllowed(Engine.Operation op) throws IllegalIndexShardStateException {
        Engine.Operation.Origin origin = op.origin();
        IndexShardState state = this.state; // one time volatile read

        if (origin == Engine.Operation.Origin.PRIMARY) {
            if (writeAllowedStatesForPrimary.contains(state) == false) {
                throw new IllegalIndexShardStateException(shardId, state, "operation only allowed when shard state is one of " + writeAllowedStatesForPrimary + ", origin [" + origin + "]");
            }
        } else if (origin.isRecovery()) {
            if (state != IndexShardState.RECOVERING) {
                throw new IllegalIndexShardStateException(shardId, state, "operation only allowed when recovering, origin [" + origin + "]");
            }
        } else {
            assert origin == Engine.Operation.Origin.REPLICA;
            if (writeAllowedStatesForReplica.contains(state) == false) {
                throw new IllegalIndexShardStateException(shardId, state, "operation only allowed when shard state is one of " + writeAllowedStatesForReplica + ", origin [" + origin + "]");
            }
        }
    }

    private void verifyPrimary() {
        if (shardRouting.primary() == false) {
            throw new IllegalStateException("shard is not a primary " + shardRouting);
        }
    }

    private void verifyReplicationTarget() {
        final IndexShardState state = state();
        if (shardRouting.primary() && shardRouting.active() && state != IndexShardState.RELOCATED) {
            // must use exception that is not ignored by replication logic. See TransportActions.isShardNotAvailableException
            throw new IllegalStateException("active primary shard cannot be a replication target before " +
                " relocation hand off " + shardRouting + ", state is [" + state + "]");
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
            final IllegalIndexShardStateException exc = new IndexShardClosedException(shardId, "operation only allowed when not closed");
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

    /**
     * Returns number of heap bytes used by the indexing buffer for this shard, or 0 if the shard is closed
     */
    public long getIndexBufferRAMBytesUsed() {
        Engine engine = getEngineOrNull();
        if (engine == null) {
            return 0;
        }
        try {
            return engine.getIndexBufferRAMBytesUsed();
        } catch (AlreadyClosedException ex) {
            return 0;
        }
    }

    public void addShardFailureCallback(Callback<ShardFailure> onShardFailure) {
        this.shardEventListener.delegates.add(onShardFailure);
    }

    /**
     * Called by {@link IndexingMemoryController} to check whether more than {@code inactiveTimeNS} has passed since the last
     * indexing operation, and notify listeners that we are now inactive so e.g. sync'd flush can happen.
     */
    public void checkIdle(long inactiveTimeNS) {
        Engine engineOrNull = getEngineOrNull();
        if (engineOrNull != null && System.nanoTime() - engineOrNull.getLastWriteNanos() >= inactiveTimeNS) {
            boolean wasActive = active.getAndSet(false);
            if (wasActive) {
                logger.debug("shard is now inactive");
                indexEventListener.onShardInactive(this);
            }
        }
    }

    public boolean isActive() {
        return active.get();
    }

    public ShardPath shardPath() {
        return path;
    }

    public boolean recoverFromLocalShards(BiConsumer<String, MappingMetaData> mappingUpdateConsumer, List<IndexShard> localShards) throws IOException {
        final List<LocalShardSnapshot> snapshots = new ArrayList<>();
        try {
            for (IndexShard shard : localShards) {
                snapshots.add(new LocalShardSnapshot(shard));
            }

            // we are the first primary, recover from the gateway
            // if its post api allocation, the index should exists
            assert shardRouting.primary() : "recover from local shards only makes sense if the shard is a primary shard";
            StoreRecovery storeRecovery = new StoreRecovery(shardId, logger);
            return storeRecovery.recoverFromLocalShards(mappingUpdateConsumer, this, snapshots);
        } finally {
            IOUtils.close(snapshots);
        }
    }

    public boolean recoverFromStore() {
        // we are the first primary, recover from the gateway
        // if its post api allocation, the index should exists
        assert shardRouting.primary() : "recover from store only makes sense if the shard is a primary shard";
        boolean shouldExist = shardRouting.allocatedPostIndexCreate(indexSettings.getIndexMetaData());

        StoreRecovery storeRecovery = new StoreRecovery(shardId, logger);
        return storeRecovery.recoverFromStore(this, shouldExist);
    }

    public boolean restoreFromRepository(IndexShardRepository repository) {
        assert shardRouting.primary() : "recover from store only makes sense if the shard is a primary shard";
        StoreRecovery storeRecovery = new StoreRecovery(shardId, logger);
        return storeRecovery.recoverFromRepository(this, repository);
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
                return translog.sizeInBytes() > indexSettings.getFlushThresholdSize().bytes();
            } catch (AlreadyClosedException | EngineClosedException ex) {
                // that's fine we are already close - no need to flush
            }
        }
        return false;
    }

    public void onSettingsChanged() {
        Engine engineOrNull = getEngineOrNull();
        if (engineOrNull != null) {
            engineOrNull.onSettingsChanged();
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

    public IndexEventListener getIndexEventListener() {
        return indexEventListener;
    }

    public void activateThrottling() {
        try {
            getEngine().activateThrottling();
        } catch (EngineClosedException ex) {
            // ignore
        }
    }

    public void deactivateThrottling() {
        try {
            getEngine().deactivateThrottling();
        } catch (EngineClosedException ex) {
            // ignore
        }
    }

    private void handleRefreshException(Exception e) {
        if (e instanceof EngineClosedException) {
            // ignore
        } else if (e instanceof RefreshFailedEngineException) {
            RefreshFailedEngineException rfee = (RefreshFailedEngineException) e;
            if (rfee.getCause() instanceof InterruptedException) {
                // ignore, we are being shutdown
            } else if (rfee.getCause() instanceof ClosedByInterruptException) {
                // ignore, we are being shutdown
            } else if (rfee.getCause() instanceof ThreadInterruptedException) {
                // ignore, we are being shutdown
            } else {
                if (state != IndexShardState.CLOSED) {
                    logger.warn("Failed to perform engine refresh", e);
                }
            }
        } else {
            if (state != IndexShardState.CLOSED) {
                logger.warn("Failed to perform engine refresh", e);
            }
        }
    }

    /**
     * Called when our shard is using too much heap and should move buffered indexed/deleted documents to disk.
     */
    public void writeIndexingBuffer() {
        if (canIndex() == false) {
            throw new UnsupportedOperationException();
        }
        try {
            Engine engine = getEngine();
            long bytes = engine.getIndexBufferRAMBytesUsed();

            // NOTE: this can be an overestimate by up to 20%, if engine uses IW.flush not refresh, because version map
            // memory is low enough, but this is fine because after the writes finish, IMC will poll again and see that
            // there's still up to the 20% being used and continue writing if necessary:
            logger.debug("add [{}] writing bytes for shard [{}]", new ByteSizeValue(bytes), shardId());
            writingBytes.addAndGet(bytes);
            try {
                engine.writeIndexingBuffer();
            } finally {
                writingBytes.addAndGet(-bytes);
                logger.debug("remove [{}] writing bytes for shard [{}]", new ByteSizeValue(bytes), shardId());
            }
        } catch (Exception e) {
            handleRefreshException(e);
        }
    }

    /**
     * Should be called for each no-op update operation to increment relevant statistics.
     *
     * @param type the doc type of the update
     */
    public void noopUpdate(String type) {
        internalIndexingStats.noopUpdate(type);
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

        if ("checksum".equals(checkIndexOnStartup)) {
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
                    if ("fix".equals(checkIndexOnStartup)) {
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

    public void startRecovery(RecoveryState recoveryState, RecoveryTargetService recoveryTargetService,
                              RecoveryTargetService.RecoveryListener recoveryListener, RepositoriesService repositoriesService,
                              BiConsumer<String, MappingMetaData> mappingUpdateConsumer,
                              IndicesService indicesService) {
        switch (recoveryState.getType()) {
            case PRIMARY_RELOCATION:
            case REPLICA:
                try {
                    markAsRecovering("from " + recoveryState.getSourceNode(), recoveryState);
                    recoveryTargetService.startRecovery(this, recoveryState.getType(), recoveryState.getSourceNode(), recoveryListener);
                } catch (Throwable e) {
                    failShard("corrupted preexisting index", e);
                    recoveryListener.onRecoveryFailure(recoveryState, new RecoveryFailedException(recoveryState, null, e), true);
                }
                break;
            case STORE:
                markAsRecovering("from store", recoveryState); // mark the shard as recovering on the cluster state thread
                threadPool.generic().execute(() -> {
                    try {
                        if (recoverFromStore()) {
                            recoveryListener.onRecoveryDone(recoveryState);
                        }
                    } catch (Throwable t) {
                        recoveryListener.onRecoveryFailure(recoveryState, new RecoveryFailedException(recoveryState, null, t), true);
                    }
                });
                break;
            case LOCAL_SHARDS:
                final IndexMetaData indexMetaData = indexSettings().getIndexMetaData();
                final Index mergeSourceIndex = indexMetaData.getMergeSourceIndex();
                final List<IndexShard> startedShards = new ArrayList<>();
                final IndexService sourceIndexService = indicesService.indexService(mergeSourceIndex);
                final int numShards = sourceIndexService != null ? sourceIndexService.getIndexSettings().getNumberOfShards() : -1;
                if (sourceIndexService != null) {
                    for (IndexShard shard : sourceIndexService) {
                        if (shard.state() == IndexShardState.STARTED) {
                            startedShards.add(shard);
                        }
                    }
                }
                if (numShards == startedShards.size()) {
                    markAsRecovering("from local shards", recoveryState); // mark the shard as recovering on the cluster state thread
                    threadPool.generic().execute(() -> {
                        try {
                            final Set<ShardId> shards = IndexMetaData.selectShrinkShards(shardId().id(), sourceIndexService.getMetaData(),
                                +                                indexMetaData.getNumberOfShards());
                            if (recoverFromLocalShards(mappingUpdateConsumer, startedShards.stream()
                                .filter((s) -> shards.contains(s.shardId())).collect(Collectors.toList()))) {
                                recoveryListener.onRecoveryDone(recoveryState);
                            }
                        } catch (Throwable t) {
                            recoveryListener.onRecoveryFailure(recoveryState,
                                new RecoveryFailedException(recoveryState, null, t), true);
                        }
                    });
                } else {
                    final Throwable t;
                    if (numShards == -1) {
                        t = new IndexNotFoundException(mergeSourceIndex);
                    } else {
                        t = new IllegalStateException("not all shards from index " + mergeSourceIndex
                            + " are started yet, expected " + numShards + " found " + startedShards.size() + " can't recover shard "
                            + shardId());
                    }
                    recoveryListener.onRecoveryFailure(recoveryState, new RecoveryFailedException(recoveryState, null, t), true);
                }
                break;
            case SNAPSHOT:
                markAsRecovering("from snapshot", recoveryState); // mark the shard as recovering on the cluster state thread
                threadPool.generic().execute(() -> {
                    try {
                        final IndexShardRepository indexShardRepository = repositoriesService.indexShardRepository(
                            recoveryState.getRestoreSource().snapshot().getRepository());
                        if (restoreFromRepository(indexShardRepository)) {
                            recoveryListener.onRecoveryDone(recoveryState);
                        }
                    } catch (Throwable first) {
                        recoveryListener.onRecoveryFailure(recoveryState, new RecoveryFailedException(recoveryState, null, first), true);
                    }
                });
                break;
            default:
                throw new IllegalArgumentException("Unknown recovery type " + recoveryState.getType());
        }
    }

    class ShardEventListener implements Engine.EventListener {
        private final CopyOnWriteArrayList<Callback<ShardFailure>> delegates = new CopyOnWriteArrayList<>();

        // called by the current engine
        @Override
        public void onFailedEngine(String reason, @Nullable Throwable failure) {
            final ShardFailure shardFailure = new ShardFailure(shardRouting, reason, failure);
            for (Callback<ShardFailure> listener : delegates) {
                try {
                    listener.handle(shardFailure);
                } catch (Exception e) {
                    logger.warn("exception while notifying engine failure", e);
                }
            }
        }
    }

    private Engine createNewEngine(EngineConfig config) {
        synchronized (mutex) {
            if (state == IndexShardState.CLOSED) {
                throw new EngineClosedException(shardId);
            }
            assert this.currentEngineReference.get() == null;
            this.currentEngineReference.set(newEngine(config));

        }

        // time elapses after the engine is created above (pulling the config settings) until we set the engine reference, during which
        // settings changes could possibly have happened, so here we forcefully push any config changes to the new engine:
        Engine engine = getEngineOrNull();

        // engine could perhaps be null if we were e.g. concurrently closed:
        if (engine != null) {
            engine.onSettingsChanged();
        }
        return engine;
    }

    protected Engine newEngine(EngineConfig config) {
        return engineFactory.newReadWriteEngine(config);
    }

    // pkg private for testing
    void persistMetadata(ShardRouting newRouting, @Nullable ShardRouting currentRouting) throws IOException {
        assert newRouting != null : "newRouting must not be null";

        // only persist metadata if routing information that is persisted in shard state metadata actually changed
        if (currentRouting == null
            || currentRouting.primary() != newRouting.primary()
            || currentRouting.allocationId().equals(newRouting.allocationId()) == false) {
            assert currentRouting == null || currentRouting.isSameAllocation(newRouting);
            final String writeReason;
            if (currentRouting == null) {
                writeReason = "initial state with allocation id [" + newRouting.allocationId() + "]";
            } else {
                writeReason = "routing changed from " + currentRouting + " to " + newRouting;
            }
            logger.trace("{} writing shard state, reason [{}]", shardId, writeReason);
            final ShardStateMetaData newShardStateMetadata = new ShardStateMetaData(newRouting.primary(), getIndexUUID(), newRouting.allocationId());
            ShardStateMetaData.FORMAT.write(newShardStateMetadata, newShardStateMetadata.legacyVersion, shardPath().getShardStatePath());
        } else {
            logger.trace("{} skip writing shard state, has been written before", shardId);
        }
    }

    private String getIndexUUID() {
        return indexSettings.getUUID();
    }

    private DocumentMapperForType docMapper(String type) {
        return mapperService.documentMapperWithAutoCreate(type);
    }

    private final EngineConfig newEngineConfig(EngineConfig.OpenMode openMode) {
        final IndexShardRecoveryPerformer translogRecoveryPerformer = new IndexShardRecoveryPerformer(shardId, mapperService, logger);
        return new EngineConfig(openMode, shardId,
            threadPool, indexSettings, warmer, store, deletionPolicy, indexSettings.getMergePolicy(),
            mapperService.indexAnalyzer(), similarityService.similarity(mapperService), codecService, shardEventListener, translogRecoveryPerformer, indexCache.query(), cachingPolicy, translogConfig,
            IndexingMemoryController.SHARD_INACTIVE_TIME_SETTING.get(indexSettings.getSettings()), refreshListeners);
    }

    public Releasable acquirePrimaryOperationLock() {
        verifyNotClosed();
        verifyPrimary();
        return suspendableRefContainer.acquireUninterruptibly();
    }

    /**
     * acquires operation log. If the given primary term is lower then the one in {@link #shardRouting}
     * an {@link IllegalArgumentException} is thrown.
     */
    public Releasable acquireReplicaOperationLock(long opPrimaryTerm) {
        verifyNotClosed();
        verifyReplicationTarget();
        if (primaryTerm > opPrimaryTerm) {
            // must use exception that is not ignored by replication logic. See TransportActions.isShardNotAvailableException
            throw new IllegalArgumentException(LoggerMessageFormat.format("{} operation term [{}] is too old (current [{}])",
                shardId, opPrimaryTerm, primaryTerm));
        }
        return suspendableRefContainer.acquireUninterruptibly();
    }

    public int getActiveOperationsCount() {
        return suspendableRefContainer.activeRefs(); // refCount is incremented on creation and decremented on close
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
     * Build {@linkplain RefreshListeners} for this shard. Protected so {@linkplain ShadowIndexShard} can override it to return null.
     */
    protected RefreshListeners buildRefreshListeners() {
        return new RefreshListeners(
                indexSettings::getMaxRefreshListeners,
                () -> refresh("too_many_listeners"),
                threadPool.executor(ThreadPool.Names.LISTENER)::execute,
                logger);
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

        public ShardFailure(ShardRouting routing, String reason, @Nullable Throwable cause) {
            this.routing = routing;
            this.reason = reason;
            this.cause = cause;
        }
    }

    EngineFactory getEngineFactory() {
        return engineFactory;
    }

    /**
     * Returns <code>true</code> iff one or more changes to the engine are not visible to via the current searcher *or* there are pending
     * refresh listeners.
     * Otherwise <code>false</code>.
     *
     * @throws EngineClosedException  if the engine is already closed
     * @throws AlreadyClosedException if the internal indexwriter in the engine is already closed
     */
    public boolean isRefreshNeeded() {
        return getEngine().refreshNeeded() || (refreshListeners != null && refreshListeners.refreshNeeded());
    }

    /**
     * Add a listener for refreshes.
     *
     * @param location the location to listen for
     * @param listener for the refresh. Called with true if registering the listener ran it out of slots and forced a refresh. Called with
     *        false otherwise.
     */
    public void addRefreshListener(Translog.Location location, Consumer<Boolean> listener) {
        refreshListeners.addOrNotify(location, listener);
    }

    private class IndexShardRecoveryPerformer extends TranslogRecoveryPerformer {

        protected IndexShardRecoveryPerformer(ShardId shardId, MapperService mapperService, ESLogger logger) {
            super(shardId, mapperService, logger);
        }

        @Override
        protected void operationProcessed() {
            assert recoveryState != null;
            recoveryState.getTranslog().incrementRecoveredOperations();
        }

        @Override
        public int recoveryFromSnapshot(Engine engine, Translog.Snapshot snapshot) throws IOException {
            assert recoveryState != null;
            RecoveryState.Translog translogStats = recoveryState.getTranslog();
            translogStats.totalOperations(snapshot.totalOperations());
            translogStats.totalOperationsOnStart(snapshot.totalOperations());
            return super.recoveryFromSnapshot(engine, snapshot);
        }

        @Override
        protected void index(Engine engine, Engine.Index engineIndex) {
            IndexShard.this.index(engine, engineIndex);
        }

        @Override
        protected void delete(Engine engine, Engine.Delete engineDelete) {
            IndexShard.this.delete(engine, engineDelete);
        }
    }
}
