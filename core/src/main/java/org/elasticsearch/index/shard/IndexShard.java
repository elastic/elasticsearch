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

import com.carrotsearch.hppc.ObjectLongMap;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.lucene.index.CheckIndex;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.QueryCachingPolicy;
import org.apache.lucene.search.ReferenceManager;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.UsageTrackingQueryCachingPolicy;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.ThreadInterruptedException;
import org.elasticsearch.Assertions;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.flush.FlushRequest;
import org.elasticsearch.action.admin.indices.forcemerge.ForceMergeRequest;
import org.elasticsearch.action.admin.indices.upgrade.post.UpgradeRequest;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.RecoverySource.SnapshotRecoverySource;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.Booleans;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.metrics.MeanMetric;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.AsyncIOProcessor;
import org.elasticsearch.common.xcontent.XContentFactory;
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
import org.elasticsearch.index.engine.EngineConfig;
import org.elasticsearch.index.engine.EngineException;
import org.elasticsearch.index.engine.EngineFactory;
import org.elasticsearch.index.engine.InternalEngine;
import org.elasticsearch.index.engine.InternalEngineFactory;
import org.elasticsearch.index.engine.RefreshFailedEngineException;
import org.elasticsearch.index.engine.Segment;
import org.elasticsearch.index.engine.SegmentsStats;
import org.elasticsearch.index.fielddata.FieldDataStats;
import org.elasticsearch.index.fielddata.ShardFieldData;
import org.elasticsearch.index.flush.FlushStats;
import org.elasticsearch.index.get.GetStats;
import org.elasticsearch.index.get.ShardGetService;
import org.elasticsearch.index.mapper.DocumentMapperForType;
import org.elasticsearch.index.mapper.IdFieldMapper;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.Mapping;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.mapper.SourceToParse;
import org.elasticsearch.index.mapper.Uid;
import org.elasticsearch.index.mapper.UidFieldMapper;
import org.elasticsearch.index.merge.MergeStats;
import org.elasticsearch.index.recovery.RecoveryStats;
import org.elasticsearch.index.refresh.RefreshStats;
import org.elasticsearch.index.search.stats.SearchStats;
import org.elasticsearch.index.search.stats.ShardSearchStats;
import org.elasticsearch.index.seqno.GlobalCheckpointTracker;
import org.elasticsearch.index.seqno.SeqNoStats;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.seqno.SequenceNumbersService;
import org.elasticsearch.index.shard.PrimaryReplicaSyncer.ResyncTask;
import org.elasticsearch.index.similarity.SimilarityService;
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
import org.elasticsearch.indices.TypeMissingException;
import org.elasticsearch.indices.cluster.IndicesClusterStateService;
import org.elasticsearch.indices.recovery.PeerRecoveryTargetService;
import org.elasticsearch.indices.recovery.RecoveryFailedException;
import org.elasticsearch.indices.recovery.RecoveryState;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.repositories.Repository;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.suggest.completion.CompletionFieldStats;
import org.elasticsearch.search.suggest.completion.CompletionStats;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.Closeable;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.channels.ClosedByInterruptException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.elasticsearch.index.mapper.SourceToParse.source;

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
    private final ShardBitsetFilterCache shardBitsetFilterCache;
    private final Object mutex = new Object();
    private final String checkIndexOnStartup;
    private final CodecService codecService;
    private final Engine.Warmer warmer;
    private final SimilarityService similarityService;
    private final TranslogConfig translogConfig;
    private final IndexEventListener indexEventListener;
    private final QueryCachingPolicy cachingPolicy;
    private final Supplier<Sort> indexSortSupplier;

    private final SearchOperationListener searchOperationListener;

    protected volatile ShardRouting shardRouting;
    protected volatile IndexShardState state;
    protected volatile long primaryTerm;
    protected final AtomicReference<Engine> currentEngineReference = new AtomicReference<>();
    protected final EngineFactory engineFactory;

    private final IndexingOperationListener indexingOperationListeners;
    private final Runnable globalCheckpointSyncer;

    Runnable getGlobalCheckpointSyncer() {
        return globalCheckpointSyncer;
    }

    @Nullable
    private RecoveryState recoveryState;

    private final RecoveryStats recoveryStats = new RecoveryStats();
    private final MeanMetric refreshMetric = new MeanMetric();
    private final MeanMetric flushMetric = new MeanMetric();

    private final ShardEventListener shardEventListener = new ShardEventListener();

    private final ShardPath path;

    private final IndexShardOperationPermits indexShardOperationPermits;

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
     * Allows for the registration of listeners that are called when a change becomes visible for search.
     */
    private final RefreshListeners refreshListeners;

    public IndexShard(
            ShardRouting shardRouting,
            IndexSettings indexSettings,
            ShardPath path,
            Store store,
            Supplier<Sort> indexSortSupplier,
            IndexCache indexCache,
            MapperService mapperService,
            SimilarityService similarityService,
            @Nullable EngineFactory engineFactory,
            IndexEventListener indexEventListener,
            IndexSearcherWrapper indexSearcherWrapper,
            ThreadPool threadPool,
            BigArrays bigArrays,
            Engine.Warmer warmer,
            List<SearchOperationListener> searchOperationListener,
            List<IndexingOperationListener> listeners,
            Runnable globalCheckpointSyncer) throws IOException {
        super(shardRouting.shardId(), indexSettings);
        assert shardRouting.initializing();
        this.shardRouting = shardRouting;
        final Settings settings = indexSettings.getSettings();
        this.codecService = new CodecService(mapperService, logger);
        this.warmer = warmer;
        this.similarityService = similarityService;
        Objects.requireNonNull(store, "Store must be provided to the index shard");
        this.engineFactory = engineFactory == null ? new InternalEngineFactory() : engineFactory;
        this.store = store;
        this.indexSortSupplier = indexSortSupplier;
        this.indexEventListener = indexEventListener;
        this.threadPool = threadPool;
        this.mapperService = mapperService;
        this.indexCache = indexCache;
        this.internalIndexingStats = new InternalIndexingStats();
        final List<IndexingOperationListener> listenersList = new ArrayList<>(listeners);
        listenersList.add(internalIndexingStats);
        this.indexingOperationListeners = new IndexingOperationListener.CompositeListener(listenersList, logger);
        this.globalCheckpointSyncer = globalCheckpointSyncer;
        final List<SearchOperationListener> searchListenersList = new ArrayList<>(searchOperationListener);
        searchListenersList.add(searchStats);
        this.searchOperationListener = new SearchOperationListener.CompositeListener(searchListenersList, logger);
        this.getService = new ShardGetService(indexSettings, this, mapperService);
        this.shardWarmerService = new ShardIndexWarmerService(shardId, indexSettings);
        this.requestCacheStats = new ShardRequestCache();
        this.shardFieldData = new ShardFieldData();
        this.shardBitsetFilterCache = new ShardBitsetFilterCache(shardId, indexSettings);
        state = IndexShardState.CREATED;
        this.path = path;
        /* create engine config */
        logger.debug("state: [CREATED]");

        this.checkIndexOnStartup = indexSettings.getValue(IndexSettings.INDEX_CHECK_ON_STARTUP);
        this.translogConfig = new TranslogConfig(shardId, shardPath().resolveTranslog(), indexSettings, bigArrays);
        // the query cache is a node-level thing, however we want the most popular filters
        // to be computed on a per-shard basis
        if (IndexModule.INDEX_QUERY_CACHE_EVERYTHING_SETTING.get(settings)) {
            cachingPolicy = QueryCachingPolicy.ALWAYS_CACHE;
        } else {
            cachingPolicy = new UsageTrackingQueryCachingPolicy();
        }
        indexShardOperationPermits = new IndexShardOperationPermits(shardId, logger, threadPool);
        searcherWrapper = indexSearcherWrapper;
        primaryTerm = indexSettings.getIndexMetaData().primaryTerm(shardId.id());
        refreshListeners = buildRefreshListeners();
        persistMetadata(path, indexSettings, shardRouting, null, logger);
    }

    public ThreadPool getThreadPool() {
        return this.threadPool;
    }

    public Store store() {
        return this.store;
    }

    /**
     * Return the sort order of this index, or null if the index has no sort.
     */
    public Sort getIndexSort() {
        return indexSortSupplier.get();
    }

    public ShardGetService getService() {
        return this.getService;
    }

    public ShardBitsetFilterCache shardBitsetFilterCache() {
        return shardBitsetFilterCache;
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
     * Returns the latest cluster routing entry received with this shard.
     */
    @Override
    public ShardRouting routingEntry() {
        return this.shardRouting;
    }

    public QueryCachingPolicy getQueryCachingPolicy() {
        return cachingPolicy;
    }


    @Override
    public void updateShardState(final ShardRouting newRouting,
                                 final long newPrimaryTerm,
                                 final BiConsumer<IndexShard, ActionListener<ResyncTask>> primaryReplicaSyncer,
                                 final long applyingClusterStateVersion,
                                 final Set<String> inSyncAllocationIds,
                                 final IndexShardRoutingTable routingTable,
                                 final Set<String> pre60AllocationIds) throws IOException {
        final ShardRouting currentRouting;
        synchronized (mutex) {
            currentRouting = this.shardRouting;

            if (!newRouting.shardId().equals(shardId())) {
                throw new IllegalArgumentException("Trying to set a routing entry with shardId " + newRouting.shardId() + " on a shard with shardId " + shardId());
            }
            if ((currentRouting == null || newRouting.isSameAllocation(currentRouting)) == false) {
                throw new IllegalArgumentException("Trying to set a routing entry with a different allocation. Current " + currentRouting + ", new " + newRouting);
            }
            if (currentRouting != null && currentRouting.primary() && newRouting.primary() == false) {
                throw new IllegalArgumentException("illegal state: trying to move shard from primary mode to replica mode. Current "
                    + currentRouting + ", new " + newRouting);
            }

            if (newRouting.primary()) {
                final Engine engine = getEngineOrNull();
                if (engine != null) {
                    engine.seqNoService().updateAllocationIdsFromMaster(applyingClusterStateVersion, inSyncAllocationIds, routingTable, pre60AllocationIds);
                }
            }

            if (state == IndexShardState.POST_RECOVERY && newRouting.active()) {
                assert currentRouting.active() == false : "we are in POST_RECOVERY, but our shard routing is active " + currentRouting;
                // we want to refresh *before* we move to internal STARTED state
                try {
                    getEngine().refresh("cluster_state_started");
                } catch (Exception e) {
                    logger.debug("failed to refresh due to move to cluster wide started", e);
                }

                if (newRouting.primary()) {
                    final DiscoveryNode recoverySourceNode = recoveryState.getSourceNode();
                    final Engine engine = getEngine();
                    if (currentRouting.isRelocationTarget() == false || recoverySourceNode.getVersion().before(Version.V_6_0_0_alpha1)) {
                        // there was no primary context hand-off in < 6.0.0, need to manually activate the shard
                        engine.seqNoService().activatePrimaryMode(getEngine().seqNoService().getLocalCheckpoint());
                    }
                    if (currentRouting.isRelocationTarget() == true && recoverySourceNode.getVersion().before(Version.V_6_0_0_alpha1)) {
                        // Flush the translog as it may contain operations with no sequence numbers. We want to make sure those
                        // operations will never be replayed as part of peer recovery to avoid an arbitrary mixture of operations with seq#
                        // (due to active indexing) and operations without a seq# coming from the translog. We therefore flush
                        // to create a lucene commit point to an empty translog file.
                        engine.flush(false, true);
                    }
                }

                changeState(IndexShardState.STARTED, "global state is [" + newRouting.state() + "]");
            } else if (state == IndexShardState.RELOCATED &&
                (newRouting.relocating() == false || newRouting.equalsIgnoringMetaData(currentRouting) == false)) {
                // if the shard is marked as RELOCATED we have to fail when any changes in shard routing occur (e.g. due to recovery
                // failure / cancellation). The reason is that at the moment we cannot safely move back to STARTED without risking two
                // active primaries.
                throw new IndexShardRelocatedException(shardId(), "Shard is marked as relocated, cannot safely move to state " + newRouting.state());
            }
            assert newRouting.active() == false || state == IndexShardState.STARTED || state == IndexShardState.RELOCATED ||
                state == IndexShardState.CLOSED :
                "routing is active, but local shard state isn't. routing: " + newRouting + ", local state: " + state;
            persistMetadata(path, indexSettings, newRouting, currentRouting, logger);
            final CountDownLatch shardStateUpdated = new CountDownLatch(1);

            if (newRouting.primary()) {
                if (newPrimaryTerm != primaryTerm) {
                    assert currentRouting.primary() == false : "term is only increased as part of primary promotion";
                    /* Note that due to cluster state batching an initializing primary shard term can failed and re-assigned
                     * in one state causing it's term to be incremented. Note that if both current shard state and new
                     * shard state are initializing, we could replace the current shard and reinitialize it. It is however
                     * possible that this shard is being started. This can happen if:
                     * 1) Shard is post recovery and sends shard started to the master
                     * 2) Node gets disconnected and rejoins
                     * 3) Master assigns the shard back to the node
                     * 4) Master processes the shard started and starts the shard
                     * 5) The node process the cluster state where the shard is both started and primary term is incremented.
                     *
                     * We could fail the shard in that case, but this will cause it to be removed from the insync allocations list
                     * potentially preventing re-allocation.
                     */
                    assert newRouting.initializing() == false :
                        "a started primary shard should never update its term; "
                            + "shard " + newRouting + ", "
                            + "current term [" + primaryTerm + "], "
                            + "new term [" + newPrimaryTerm + "]";
                    assert newPrimaryTerm > primaryTerm :
                        "primary terms can only go up; current term [" + primaryTerm + "], new term [" + newPrimaryTerm + "]";
                    /*
                     * Before this call returns, we are guaranteed that all future operations are delayed and so this happens before we
                     * increment the primary term. The latch is needed to ensure that we do not unblock operations before the primary term is
                     * incremented.
                     */
                    // to prevent primary relocation handoff while resync is not completed
                    boolean resyncStarted = primaryReplicaResyncInProgress.compareAndSet(false, true);
                    if (resyncStarted == false) {
                        throw new IllegalStateException("cannot start resync while it's already in progress");
                    }
                    indexShardOperationPermits.asyncBlockOperations(
                        30,
                        TimeUnit.MINUTES,
                        () -> {
                            shardStateUpdated.await();
                            try {
                                /*
                                 * If this shard was serving as a replica shard when another shard was promoted to primary then the state of
                                 * its local checkpoint tracker was reset during the primary term transition. In particular, the local
                                 * checkpoint on this shard was thrown back to the global checkpoint and the state of the local checkpoint
                                 * tracker above the local checkpoint was destroyed. If the other shard that was promoted to primary
                                 * subsequently fails before the primary/replica re-sync completes successfully and we are now being
                                 * promoted, the local checkpoint tracker here could be left in a state where it would re-issue sequence
                                 * numbers. To ensure that this is not the case, we restore the state of the local checkpoint tracker by
                                 * replaying the translog and marking any operations there are completed.
                                 */
                                final Engine engine = getEngine();
                                engine.restoreLocalCheckpointFromTranslog();
                                if (indexSettings.getIndexVersionCreated().onOrBefore(Version.V_6_0_0_alpha1)) {
                                    // an index that was created before sequence numbers were introduced may contain operations in its
                                    // translog that do not have a sequence numbers. We want to make sure those operations will never
                                    // be replayed as part of peer recovery to avoid an arbitrary mixture of operations with seq# (due
                                    // to active indexing) and operations without a seq# coming from the translog. We therefore flush
                                    // to create a lucene commit point to an empty translog file.
                                    engine.flush(false, true);
                                }
                                /* Rolling the translog generation is not strictly needed here (as we will never have collisions between
                                 * sequence numbers in a translog generation in a new primary as it takes the last known sequence number
                                 * as a starting point), but it simplifies reasoning about the relationship between primary terms and
                                 * translog generations.
                                 */
                                engine.rollTranslogGeneration();
                                engine.fillSeqNoGaps(newPrimaryTerm);
                                engine.seqNoService().updateLocalCheckpointForShard(currentRouting.allocationId().getId(),
                                    getEngine().seqNoService().getLocalCheckpoint());
                                primaryReplicaSyncer.accept(this, new ActionListener<ResyncTask>() {
                                    @Override
                                    public void onResponse(ResyncTask resyncTask) {
                                        logger.info("primary-replica resync completed with {} operations",
                                            resyncTask.getResyncedOperations());
                                        boolean resyncCompleted = primaryReplicaResyncInProgress.compareAndSet(true, false);
                                        assert resyncCompleted : "primary-replica resync finished but was not started";
                                    }

                                    @Override
                                    public void onFailure(Exception e) {
                                        boolean resyncCompleted = primaryReplicaResyncInProgress.compareAndSet(true, false);
                                        assert resyncCompleted : "primary-replica resync finished but was not started";
                                        if (state == IndexShardState.CLOSED) {
                                            // ignore, shutting down
                                        } else {
                                            failShard("exception during primary-replica resync", e);
                                        }
                                    }
                                });
                            } catch (final AlreadyClosedException e) {
                                // okay, the index was deleted
                            }
                        },
                        e -> failShard("exception during primary term transition", e));
                    getEngine().seqNoService().activatePrimaryMode(getEngine().seqNoService().getLocalCheckpoint());
                    primaryTerm = newPrimaryTerm;
                }
            }
            // set this last, once we finished updating all internal state.
            this.shardRouting = newRouting;
            shardStateUpdated.countDown();
        }
        if (currentRouting != null && currentRouting.active() == false && newRouting.active()) {
            indexEventListener.afterIndexShardStarted(this);
        }
        if (newRouting.equals(currentRouting) == false) {
            indexEventListener.shardRoutingChanged(this, currentRouting, newRouting);
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

    private final AtomicBoolean primaryReplicaResyncInProgress = new AtomicBoolean();

    /**
     * Completes the relocation. Operations are blocked and current operations are drained before changing state to relocated. The provided
     * {@link Runnable} is executed after all operations are successfully blocked.
     *
     * @param reason    the reason for the relocation
     * @param consumer a {@link Runnable} that is executed after operations are blocked
     * @throws IllegalIndexShardStateException if the shard is not relocating due to concurrent cancellation
     * @throws InterruptedException            if blocking operations is interrupted
     */
    public void relocated(
            final String reason, final Consumer<GlobalCheckpointTracker.PrimaryContext> consumer) throws IllegalIndexShardStateException, InterruptedException {
        assert shardRouting.primary() : "only primaries can be marked as relocated: " + shardRouting;
        try {
            indexShardOperationPermits.blockOperations(30, TimeUnit.MINUTES, () -> {
                // no shard operation permits are being held here, move state from started to relocated
                assert indexShardOperationPermits.getActiveOperationsCount() == 0 :
                        "in-flight operations in progress while moving shard state to relocated";
                /*
                 * We should not invoke the runnable under the mutex as the expected implementation is to handoff the primary context via a
                 * network operation. Doing this under the mutex can implicitly block the cluster state update thread on network operations.
                 */
                verifyRelocatingState();
                final GlobalCheckpointTracker.PrimaryContext primaryContext = getEngine().seqNoService().startRelocationHandoff();
                try {
                    consumer.accept(primaryContext);
                    synchronized (mutex) {
                        verifyRelocatingState();
                        changeState(IndexShardState.RELOCATED, reason);
                    }
                    getEngine().seqNoService().completeRelocationHandoff();
                } catch (final Exception e) {
                    try {
                        getEngine().seqNoService().abortRelocationHandoff();
                    } catch (final Exception inner) {
                        e.addSuppressed(inner);
                    }
                    throw e;
                }
            });
        } catch (TimeoutException e) {
            logger.warn("timed out waiting for relocation hand-off to complete");
            // This is really bad as ongoing replication operations are preventing this shard from completing relocation hand-off.
            // Fail primary relocation source and target shards.
            failShard("timed out waiting for relocation hand-off to complete", null);
            throw new IndexShardClosedException(shardId(), "timed out waiting for relocation hand-off to complete");
        }
    }

    private void verifyRelocatingState() {
        if (state != IndexShardState.STARTED) {
            throw new IndexShardNotStartedException(shardId, state);
        }
        /*
         * If the master cancelled recovery, the target will be removed and the recovery will be cancelled. However, it is still possible
         * that we concurrently end up here and therefore have to protect that we do not mark the shard as relocated when its shard routing
         * says otherwise.
         */

        if (shardRouting.relocating() == false) {
            throw new IllegalIndexShardStateException(shardId, IndexShardState.STARTED,
                ": shard is no longer relocating " + shardRouting);
        }

        if (primaryReplicaResyncInProgress.get()) {
            throw new IllegalIndexShardStateException(shardId, IndexShardState.STARTED,
                ": primary relocation is forbidden while primary-replica resync is in progress " + shardRouting);
        }
    }

    @Override
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
        assert Thread.holdsLock(mutex);
        logger.debug("state: [{}]->[{}], reason [{}]", state, newState, reason);
        IndexShardState previousState = state;
        state = newState;
        this.indexEventListener.indexShardStateChanged(this, previousState, newState, reason);
        return previousState;
    }

    public Engine.IndexResult applyIndexOperationOnPrimary(long version, VersionType versionType, SourceToParse sourceToParse,
                                                           long autoGeneratedTimestamp, boolean isRetry,
                                                           Consumer<Mapping> onMappingUpdate) throws IOException {
        return applyIndexOperation(SequenceNumbers.UNASSIGNED_SEQ_NO, primaryTerm, version, versionType, autoGeneratedTimestamp,
            isRetry, Engine.Operation.Origin.PRIMARY, sourceToParse, onMappingUpdate);
    }

    public Engine.IndexResult applyIndexOperationOnReplica(long seqNo, long version, VersionType versionType,
                                                           long autoGeneratedTimeStamp, boolean isRetry, SourceToParse sourceToParse,
                                                           Consumer<Mapping> onMappingUpdate) throws IOException {
        return applyIndexOperation(seqNo, primaryTerm, version, versionType, autoGeneratedTimeStamp, isRetry,
            Engine.Operation.Origin.REPLICA, sourceToParse, onMappingUpdate);
    }

    private Engine.IndexResult applyIndexOperation(long seqNo, long opPrimaryTerm, long version, VersionType versionType,
                                                   long autoGeneratedTimeStamp, boolean isRetry, Engine.Operation.Origin origin,
                                                   SourceToParse sourceToParse, Consumer<Mapping> onMappingUpdate) throws IOException {
        assert opPrimaryTerm <= this.primaryTerm : "op term [ " + opPrimaryTerm + " ] > shard term [" + this.primaryTerm + "]";
        assert versionType.validateVersionForWrites(version);
        ensureWriteAllowed(origin);
        Engine.Index operation;
        try {
            operation = prepareIndex(docMapper(sourceToParse.type()), indexSettings.getIndexVersionCreated(), sourceToParse, seqNo,
                    opPrimaryTerm, version, versionType, origin,
                autoGeneratedTimeStamp, isRetry);
            Mapping update = operation.parsedDoc().dynamicMappingsUpdate();
            if (update != null) {
                // wrap this in the outer catch block, as the master might also throw a MapperParsingException when updating the mapping
                onMappingUpdate.accept(update);
            }
        } catch (MapperParsingException | IllegalArgumentException | TypeMissingException e) {
            return new Engine.IndexResult(e, version, seqNo);
        } catch (Exception e) {
            verifyNotClosed(e);
            throw e;
        }

        return index(getEngine(), operation);
    }

    public static Engine.Index prepareIndex(DocumentMapperForType docMapper, Version indexCreatedVersion, SourceToParse source, long seqNo,
            long primaryTerm, long version, VersionType versionType, Engine.Operation.Origin origin, long autoGeneratedIdTimestamp,
            boolean isRetry) {
        long startTime = System.nanoTime();
        ParsedDocument doc = docMapper.getDocumentMapper().parse(source);
        if (docMapper.getMapping() != null) {
            doc.addDynamicMappingsUpdate(docMapper.getMapping());
        }
        Term uid;
        if (indexCreatedVersion.onOrAfter(Version.V_6_0_0_beta1)) {
            uid = new Term(IdFieldMapper.NAME, Uid.encodeId(doc.id()));
        } else if (docMapper.getDocumentMapper().idFieldMapper().fieldType().indexOptions() != IndexOptions.NONE) {
            uid = new Term(IdFieldMapper.NAME, doc.id());
        } else {
            uid = new Term(UidFieldMapper.NAME, Uid.createUidAsBytes(doc.type(), doc.id()));
        }
        return new Engine.Index(uid, doc, seqNo, primaryTerm, version, versionType, origin, startTime, autoGeneratedIdTimestamp, isRetry);
    }

    private Engine.IndexResult index(Engine engine, Engine.Index index) throws IOException {
        active.set(true);
        final Engine.IndexResult result;
        index = indexingOperationListeners.preIndex(shardId, index);
        try {
            if (logger.isTraceEnabled()) {
                // don't use index.source().utf8ToString() here source might not be valid UTF-8
                logger.trace("index [{}][{}] (seq# [{}])",  index.type(), index.id(), index.seqNo());
            }
            result = engine.index(index);
        } catch (Exception e) {
            indexingOperationListeners.postIndex(shardId, index, e);
            throw e;
        }
        indexingOperationListeners.postIndex(shardId, index, result);
        return result;
    }

    public Engine.NoOpResult markSeqNoAsNoop(long seqNo, String reason) throws IOException {
        return markSeqNoAsNoop(seqNo, primaryTerm, reason, Engine.Operation.Origin.REPLICA);
    }

    private Engine.NoOpResult markSeqNoAsNoop(long seqNo, long opPrimaryTerm, String reason,
                                              Engine.Operation.Origin origin) throws IOException {
        assert opPrimaryTerm <= this.primaryTerm : "op term [ " + opPrimaryTerm + " ] > shard term [" + this.primaryTerm + "]";
        long startTime = System.nanoTime();
        ensureWriteAllowed(origin);
        final Engine.NoOp noOp = new Engine.NoOp(seqNo, opPrimaryTerm, origin, startTime, reason);
        return noOp(getEngine(), noOp);
    }

    private Engine.NoOpResult noOp(Engine engine, Engine.NoOp noOp) {
        active.set(true);
        if (logger.isTraceEnabled()) {
            logger.trace("noop (seq# [{}])", noOp.seqNo());
        }
        return engine.noOp(noOp);
    }

    public Engine.DeleteResult applyDeleteOperationOnPrimary(long version, String type, String id, VersionType versionType,
                                                             Consumer<Mapping> onMappingUpdate) throws IOException {
        return applyDeleteOperation(SequenceNumbers.UNASSIGNED_SEQ_NO, primaryTerm, version, type, id, versionType,
            Engine.Operation.Origin.PRIMARY, onMappingUpdate);
    }

    public Engine.DeleteResult applyDeleteOperationOnReplica(long seqNo, long version, String type, String id,
                                                             VersionType versionType,
                                                             Consumer<Mapping> onMappingUpdate) throws IOException {
        return applyDeleteOperation(seqNo, primaryTerm, version, type, id, versionType, Engine.Operation.Origin.REPLICA, onMappingUpdate);
    }

    private Engine.DeleteResult applyDeleteOperation(long seqNo, long opPrimaryTerm, long version, String type, String id,
                                                     VersionType versionType, Engine.Operation.Origin origin,
                                                     Consumer<Mapping> onMappingUpdate) throws IOException {
        assert opPrimaryTerm <= this.primaryTerm : "op term [ " + opPrimaryTerm + " ] > shard term [" + this.primaryTerm + "]";
        assert versionType.validateVersionForWrites(version);
        ensureWriteAllowed(origin);
        if (indexSettings().isSingleType()) {
            // When there is a single type, the unique identifier is only composed of the _id,
            // so there is no way to differenciate foo#1 from bar#1. This is especially an issue
            // if a user first deletes foo#1 and then indexes bar#1: since we do not encode the
            // _type in the uid it might look like we are reindexing the same document, which
            // would fail if bar#1 is indexed with a lower version than foo#1 was deleted with.
            // In order to work around this issue, we make deletions create types. This way, we
            // fail if index and delete operations do not use the same type.
            try {
                Mapping update = docMapper(type).getMapping();
                if (update != null) {
                    onMappingUpdate.accept(update);
                }
            } catch (MapperParsingException | IllegalArgumentException | TypeMissingException e) {
                return new Engine.DeleteResult(e, version, seqNo, false);
            }
        }
        final Term uid = extractUidForDelete(type, id);
        final Engine.Delete delete = prepareDelete(type, id, uid, seqNo, opPrimaryTerm, version,
            versionType, origin);
        return delete(getEngine(), delete);
    }

    private static Engine.Delete prepareDelete(String type, String id, Term uid, long seqNo, long primaryTerm, long version,
                                               VersionType versionType, Engine.Operation.Origin origin) {
        long startTime = System.nanoTime();
        return new Engine.Delete(type, id, uid, seqNo, primaryTerm, version, versionType, origin, startTime);
    }

    private Term extractUidForDelete(String type, String id) {
        if (indexSettings.getIndexVersionCreated().onOrAfter(Version.V_6_0_0_beta1)) {
            assert indexSettings.isSingleType();
            // This is only correct because we create types dynamically on delete operations
            // otherwise this could match the same _id from a different type
            BytesRef idBytes = Uid.encodeId(id);
            return new Term(IdFieldMapper.NAME, idBytes);
        } else if (indexSettings.isSingleType()) {
            // This is only correct because we create types dynamically on delete operations
            // otherwise this could match the same _id from a different type
            return new Term(IdFieldMapper.NAME, id);
        } else {
            return new Term(UidFieldMapper.NAME, Uid.createUidAsBytes(type, id));
        }
    }

    private Engine.DeleteResult delete(Engine engine, Engine.Delete delete) throws IOException {
        active.set(true);
        final Engine.DeleteResult result;
        delete = indexingOperationListeners.preDelete(shardId, delete);
        try {
            if (logger.isTraceEnabled()) {
                logger.trace("delete [{}] (seq no [{}])", delete.uid().text(), delete.seqNo());
            }
            result = engine.delete(delete);
        } catch (Exception e) {
            indexingOperationListeners.postDelete(shardId, delete, e);
            throw e;
        }
        indexingOperationListeners.postDelete(shardId, delete, result);
        return result;
    }

    public Engine.GetResult get(Engine.Get get) {
        readAllowed();
        return getEngine().get(get, this::acquireSearcher);
    }

    /**
     * Writes all indexing changes to disk and opens a new searcher reflecting all changes.  This can throw {@link AlreadyClosedException}.
     */
    public void refresh(String source) {
        verifyNotClosed();
        if (logger.isTraceEnabled()) {
            logger.trace("refresh with source [{}]", source);
        }
        getEngine().refresh(source);
    }

    /**
     * Returns how many bytes we are currently moving from heap to disk
     */
    public long getWritingBytes() {
        Engine engine = getEngineOrNull();
        if (engine == null) {
            return 0;
        }
        return engine.getWritingBytes();
    }

    public RefreshStats refreshStats() {
        int listeners = refreshListeners.pendingCount();
        return new RefreshStats(refreshMetric.count(), TimeUnit.NANOSECONDS.toMillis(refreshMetric.sum()), listeners);
    }

    public FlushStats flushStats() {
        return new FlushStats(flushMetric.count(), TimeUnit.NANOSECONDS.toMillis(flushMetric.sum()));
    }

    public DocsStats docStats() {
        // we calculate the doc stats based on the internal reader that is more up-to-date and not subject
        // to external refreshes. For instance we don't refresh an external reader if we flush and indices with
        // index.refresh_interval=-1 won't see any doc stats updates at all. This change will give more accurate statistics
        // when indexing but not refreshing in general. Yet, if a refresh happens the internal reader is refresh as well so we are
        // safe here.
        long numDocs = 0;
        long numDeletedDocs = 0;
        long sizeInBytes = 0;
        try (Engine.Searcher searcher = acquireSearcher("docStats", Engine.SearcherScope.INTERNAL)) {
            for (LeafReaderContext reader : searcher.reader().leaves()) {
                // we go on the segment level here to get accurate numbers
                final SegmentReader segmentReader = Lucene.segmentReader(reader.reader());
                SegmentCommitInfo info = segmentReader.getSegmentInfo();
                numDocs += reader.reader().numDocs();
                numDeletedDocs += reader.reader().numDeletedDocs();
                try {
                    sizeInBytes += info.sizeInBytes();
                } catch (IOException e) {
                    logger.trace((org.apache.logging.log4j.util.Supplier<?>)
                        () -> new ParameterizedMessage("failed to get size for [{}]", info.info.name), e);
                }
            }
        }
        return new DocsStats(numDocs, numDeletedDocs, sizeInBytes);
    }

    /**
     * @return {@link CommitStats} if engine is open, otherwise null
     */
    @Nullable
    public CommitStats commitStats() {
        Engine engine = getEngineOrNull();
        return engine == null ? null : engine.commitStats();
    }

    /**
     * @return {@link SeqNoStats} if engine is open, otherwise null
     */
    @Nullable
    public SeqNoStats seqNoStats() {
        Engine engine = getEngineOrNull();
        return engine == null ? null : engine.seqNoService().stats();
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
        try (Engine.Searcher currentSearcher = acquireSearcher("completion_stats")) {
            completionStats.add(CompletionFieldStats.completionStats(currentSearcher.reader(), fields));
        }
        return completionStats;
    }

    public Engine.SyncedFlushResult syncFlush(String syncId, Engine.CommitId expectedCommitId) {
        verifyNotClosed();
        logger.trace("trying to sync flush. sync id [{}]. expected commit id [{}]]", syncId, expectedCommitId);
        Engine engine = getEngine();
        if (engine.isRecovering()) {
            throw new IllegalIndexShardStateException(shardId(), state, "syncFlush is only allowed if the engine is not recovery" +
                " from translog");
        }
        return engine.syncFlush(syncId, expectedCommitId);
    }

    /**
     * Executes the given flush request against the engine.
     *
     * @param request the flush request
     * @return the commit ID
     */
    public Engine.CommitId flush(FlushRequest request) {
        final boolean waitIfOngoing = request.waitIfOngoing();
        final boolean force = request.force();
        logger.trace("flush with {}", request);
        /*
         * We allow flushes while recovery since we allow operations to happen while recovering and we want to keep the translog under
         * control (up to deletes, which we do not GC). Yet, we do not use flush internally to clear deletes and flush the index writer
         * since we use Engine#writeIndexingBuffer for this now.
         */
        verifyNotClosed();
        final Engine engine = getEngine();
        if (engine.isRecovering()) {
            throw new IllegalIndexShardStateException(
                    shardId(),
                    state,
                    "flush is only allowed if the engine is not recovery from translog");
        }
        final long time = System.nanoTime();
        final Engine.CommitId commitId = engine.flush(force, waitIfOngoing);
        engine.refresh("flush"); // TODO this is technically wrong we should remove this in 7.0
        flushMetric.inc(System.nanoTime() - time);
        return commitId;
    }

    /**
     * checks and removes translog files that no longer need to be retained. See
     * {@link org.elasticsearch.index.translog.TranslogDeletionPolicy} for details
     */
    public void trimTranslog() {
        verifyNotClosed();
        final Engine engine = getEngine();
        engine.trimTranslog();
    }

    /**
     * Rolls the tranlog generation and cleans unneeded.
     */
    private void rollTranslogGeneration() {
        final Engine engine = getEngine();
        engine.rollTranslogGeneration();
    }

    public void forceMerge(ForceMergeRequest forceMerge) throws IOException {
        verifyActive();
        if (logger.isTraceEnabled()) {
            logger.trace("force merge with {}", forceMerge);
        }
        Engine engine = getEngine();
        engine.forceMerge(forceMerge.flush(), forceMerge.maxNumSegments(),
            forceMerge.onlyExpungeDeletes(), false, false);
        if (forceMerge.flush()) {
            engine.refresh("force_merge"); // TODO this is technically wrong we should remove this in 7.0
        }
    }

    /**
     * Upgrades the shard to the current version of Lucene and returns the minimum segment version
     */
    public org.apache.lucene.util.Version upgrade(UpgradeRequest upgrade) throws IOException {
        verifyActive();
        if (logger.isTraceEnabled()) {
            logger.trace("upgrade with {}", upgrade);
        }
        org.apache.lucene.util.Version previousVersion = minimumCompatibleVersion();
        // we just want to upgrade the segments, not actually forge merge to a single segment
        final Engine engine = getEngine();
        engine.forceMerge(true,  // we need to flush at the end to make sure the upgrade is durable
            Integer.MAX_VALUE, // we just want to upgrade the segments, not actually optimize to a single segment
            false, true, upgrade.upgradeOnlyAncientSegments());
        engine.refresh("upgrade"); // TODO this is technically wrong we should remove this in 7.0

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
     * commit won't be freed until the commit / snapshot is closed.
     *
     * @param flushFirst <code>true</code> if the index should first be flushed to disk / a low level lucene commit should be executed
     */
    public Engine.IndexCommitRef acquireIndexCommit(boolean flushFirst) throws EngineException {
        IndexShardState state = this.state; // one time volatile read
        // we allow snapshot on closed index shard, since we want to do one after we close the shard and before we close the engine
        if (state == IndexShardState.STARTED || state == IndexShardState.RELOCATED || state == IndexShardState.CLOSED) {
            return getEngine().acquireIndexCommit(flushFirst);
        } else {
            throw new IllegalIndexShardStateException(shardId, state, "snapshot is not allowed");
        }
    }


    /**
     * gets a {@link Store.MetadataSnapshot} for the current directory. This method is safe to call in all lifecycle of the index shard,
     * without having to worry about the current state of the engine and concurrent flushes.
     *
     * @throws org.apache.lucene.index.IndexNotFoundException     if no index is found in the current directory
     * @throws org.apache.lucene.index.CorruptIndexException      if the lucene index is corrupted. This can be caused by a checksum
     *                                                            mismatch or an unexpected exception when opening the index reading the
     *                                                            segments file.
     * @throws org.apache.lucene.index.IndexFormatTooOldException if the lucene index is too old to be opened.
     * @throws org.apache.lucene.index.IndexFormatTooNewException if the lucene index is too new to be opened.
     * @throws java.io.FileNotFoundException                      if one or more files referenced by a commit are not present.
     * @throws java.nio.file.NoSuchFileException                  if one or more files referenced by a commit are not present.
     */
    public Store.MetadataSnapshot snapshotStoreMetadata() throws IOException {
        Engine.IndexCommitRef indexCommit = null;
        store.incRef();
        try {
            Engine engine;
            synchronized (mutex) {
                // if the engine is not running, we can access the store directly, but we need to make sure no one starts
                // the engine on us. If the engine is running, we can get a snapshot via the deletion policy which is initialized.
                // That can be done out of mutex, since the engine can be closed half way.
                engine = getEngineOrNull();
                if (engine == null) {
                    return store.getMetadata(null, true);
                }
            }
            indexCommit = engine.acquireIndexCommit(false);
            return store.getMetadata(indexCommit.getIndexCommit());
        } finally {
            store.decRef();
            IOUtils.close(indexCommit);
        }
    }

    /**
     * Fails the shard and marks the shard store as corrupted if
     * <code>e</code> is caused by index corruption
     */
    public void failShard(String reason, @Nullable Exception e) {
        // fail the engine. This will cause this shard to also be removed from the node's index service.
        getEngine().failEngine(reason, e);
    }
    public Engine.Searcher acquireSearcher(String source) {
        return acquireSearcher(source, Engine.SearcherScope.EXTERNAL);
    }

    private Engine.Searcher acquireSearcher(String source, Engine.SearcherScope scope) {
        readAllowed();
        final Engine engine = getEngine();
        final Engine.Searcher searcher = engine.acquireSearcher(source, scope);
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
                } finally {
                    // playing safe here and close the engine even if the above succeeds - close can be called multiple times
                    // Also closing refreshListeners to prevent us from accumulating any more listeners
                    IOUtils.close(engine, refreshListeners);
                    indexShardOperationPermits.close();
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
            // we need to refresh again to expose all operations that were index until now. Otherwise
            // we may not expose operations that were indexed with a refresh listener that was immediately
            // responded to in addRefreshListener.
            getEngine().refresh("post_recovery");
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

    public Engine.Result applyTranslogOperation(Translog.Operation operation, Engine.Operation.Origin origin,
                                                Consumer<Mapping> onMappingUpdate) throws IOException {
        final Engine.Result result;
        switch (operation.opType()) {
            case INDEX:
                final Translog.Index index = (Translog.Index) operation;
                // we set canHaveDuplicates to true all the time such that we de-optimze the translog case and ensure that all
                // autoGeneratedID docs that are coming from the primary are updated correctly.
                result = applyIndexOperation(index.seqNo(), index.primaryTerm(), index.version(),
                    index.versionType().versionTypeForReplicationAndRecovery(), index.getAutoGeneratedIdTimestamp(), true, origin,
                    source(shardId.getIndexName(), index.type(), index.id(), index.source(), XContentFactory.xContentType(index.source()))
                        .routing(index.routing()).parent(index.parent()), onMappingUpdate);
                break;
            case DELETE:
                final Translog.Delete delete = (Translog.Delete) operation;
                result = applyDeleteOperation(delete.seqNo(), delete.primaryTerm(), delete.version(), delete.type(), delete.id(),
                    delete.versionType().versionTypeForReplicationAndRecovery(), origin, onMappingUpdate);
                break;
            case NO_OP:
                final Translog.NoOp noOp = (Translog.NoOp) operation;
                result = markSeqNoAsNoop(noOp.seqNo(), noOp.primaryTerm(), noOp.reason(), origin);
                break;
            default:
                throw new IllegalStateException("No operation defined for [" + operation + "]");
        }
        return result;
    }

    // package-private for testing
    int runTranslogRecovery(Engine engine, Translog.Snapshot snapshot) throws IOException {
        recoveryState.getTranslog().totalOperations(snapshot.totalOperations());
        recoveryState.getTranslog().totalOperationsOnStart(snapshot.totalOperations());
        int opsRecovered = 0;
        Translog.Operation operation;
        while ((operation = snapshot.next()) != null) {
            try {
                logger.trace("[translog] recover op {}", operation);
                Engine.Result result = applyTranslogOperation(operation, Engine.Operation.Origin.LOCAL_TRANSLOG_RECOVERY, update -> {
                    throw new IllegalArgumentException("unexpected mapping update: " + update);
                });
                ExceptionsHelper.reThrowIfNotNull(result.getFailure());
                opsRecovered++;
                recoveryState.getTranslog().incrementRecoveredOperations();
            } catch (Exception e) {
                if (ExceptionsHelper.status(e) == RestStatus.BAD_REQUEST) {
                    // mainly for MapperParsingException and Failure to detect xcontent
                    logger.info("ignoring recovery of a corrupt translog entry", e);
                } else {
                    throw e;
                }
            }
        }
        return opsRecovered;
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
        if (Booleans.isTrue(checkIndexOnStartup)) {
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

        assert indexExists == false || assertMaxUnsafeAutoIdInCommit();

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
        assertSequenceNumbersInCommit();
    }

    private boolean assertSequenceNumbersInCommit() throws IOException {
        final Map<String, String> userData = SegmentInfos.readLatestCommit(store.directory()).getUserData();
        assert userData.containsKey(SequenceNumbers.LOCAL_CHECKPOINT_KEY) : "commit point doesn't contains a local checkpoint";
        assert userData.containsKey(SequenceNumbers.MAX_SEQ_NO) : "commit point doesn't contains a maximum sequence number";
        assert userData.containsKey(Engine.HISTORY_UUID_KEY) : "commit point doesn't contains a history uuid";
        assert userData.get(Engine.HISTORY_UUID_KEY).equals(getHistoryUUID()) : "commit point history uuid ["
            + userData.get(Engine.HISTORY_UUID_KEY) + "] is different than engine [" + getHistoryUUID() + "]";
        return true;
    }

    private boolean assertMaxUnsafeAutoIdInCommit() throws IOException {
        final Map<String, String> userData = SegmentInfos.readLatestCommit(store.directory()).getUserData();
        if (indexSettings.getIndexVersionCreated().onOrAfter(Version.V_5_5_0) &&
            // TODO: LOCAL_SHARDS need to transfer this information
            recoveryState().getRecoverySource().getType() != RecoverySource.Type.LOCAL_SHARDS) {
            // as of 5.5.0, the engine stores the maxUnsafeAutoIdTimestamp in the commit point.
            // This should have baked into the commit by the primary we recover from, regardless of the index age.
            assert userData.containsKey(InternalEngine.MAX_UNSAFE_AUTO_ID_TIMESTAMP_COMMIT_ID) :
                "opening index which was created post 5.5.0 but " + InternalEngine.MAX_UNSAFE_AUTO_ID_TIMESTAMP_COMMIT_ID
                    + " is not found in commit";
        }
        return true;
    }

    protected void onNewEngine(Engine newEngine) {
        refreshListeners.setTranslog(newEngine.getTranslog());
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
            assert refreshListeners.pendingCount() == 0 : "we can't restart with pending listeners";
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
    @Override
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

    /** returns true if the {@link IndexShardState} allows reading */
    public boolean isReadAllowed() {
        return readAllowedStates.contains(state);
    }

    private void ensureWriteAllowed(Engine.Operation.Origin origin) throws IllegalIndexShardStateException {
        IndexShardState state = this.state; // one time volatile read

        if (origin == Engine.Operation.Origin.PRIMARY) {
            verifyPrimary();
            if (writeAllowedStatesForPrimary.contains(state) == false) {
                throw new IllegalIndexShardStateException(shardId, state, "operation only allowed when shard state is one of " + writeAllowedStatesForPrimary + ", origin [" + origin + "]");
            }
        } else if (origin.isRecovery()) {
            if (state != IndexShardState.RECOVERING) {
                throw new IllegalIndexShardStateException(shardId, state, "operation only allowed when recovering, origin [" + origin + "]");
            }
        } else {
            assert origin == Engine.Operation.Origin.REPLICA;
            verifyReplicationTarget();
            if (writeAllowedStatesForReplica.contains(state) == false) {
                throw new IllegalIndexShardStateException(shardId, state, "operation only allowed when shard state is one of " + writeAllowedStatesForReplica + ", origin [" + origin + "]");
            }
        }
    }

    private void verifyPrimary() {
        if (shardRouting.primary() == false) {
            throw new IllegalStateException("shard " + shardRouting + " is not a primary");
        }
    }

    private void verifyReplicationTarget() {
        final IndexShardState state = state();
        if (shardRouting.primary() && shardRouting.active() && state != IndexShardState.RELOCATED) {
            // must use exception that is not ignored by replication logic. See TransportActions.isShardNotAvailableException
            throw new IllegalStateException("active primary shard " + shardRouting + " cannot be a replication target before " +
                "relocation hand off, state is [" + state + "]");
        }
    }

    private void verifyNotClosed() throws IllegalIndexShardStateException {
        verifyNotClosed(null);
    }

    private void verifyNotClosed(Exception suppressed) throws IllegalIndexShardStateException {
        IndexShardState state = this.state; // one time volatile read
        if (state == IndexShardState.CLOSED) {
            final IllegalIndexShardStateException exc = new IndexShardClosedException(shardId, "operation only allowed when not closed");
            if (suppressed != null) {
                exc.addSuppressed(suppressed);
            }
            throw exc;
        }
    }

    protected final void verifyActive() throws IllegalIndexShardStateException {
        IndexShardState state = this.state; // one time volatile read
        if (state != IndexShardState.STARTED && state != IndexShardState.RELOCATED) {
            throw new IllegalIndexShardStateException(shardId, state, "operation only allowed when shard is active");
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

    public void addShardFailureCallback(Consumer<ShardFailure> onShardFailure) {
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
                try {
                    indexEventListener.onShardInactive(this);
                } catch (Exception e) {
                    logger.warn("failed to notify index event listener", e);
                }
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
        assert shardRouting.primary() : "recover from local shards only makes sense if the shard is a primary shard";
        assert recoveryState.getRecoverySource().getType() == RecoverySource.Type.LOCAL_SHARDS : "invalid recovery type: " + recoveryState.getRecoverySource();
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
        assert shardRouting.initializing() : "can only start recovery on initializing shard";
        StoreRecovery storeRecovery = new StoreRecovery(shardId, logger);
        return storeRecovery.recoverFromStore(this);
    }

    public boolean restoreFromRepository(Repository repository) {
        assert shardRouting.primary() : "recover from store only makes sense if the shard is a primary shard";
        assert recoveryState.getRecoverySource().getType() == RecoverySource.Type.SNAPSHOT : "invalid recovery type: " + recoveryState.getRecoverySource();
        StoreRecovery storeRecovery = new StoreRecovery(shardId, logger);
        return storeRecovery.recoverFromRepository(this, repository);
    }

    /**
     * Tests whether or not the translog should be flushed. This test is based on the current size of the translog comparted to the
     * configured flush threshold size.
     *
     * @return {@code true} if the translog should be flushed
     */
    boolean shouldFlush() {
        final Engine engine = getEngineOrNull();
        if (engine != null) {
            try {
                final Translog translog = engine.getTranslog();
                return translog.shouldFlush();
            } catch (final AlreadyClosedException e) {
                // we are already closed, no need to flush or roll
            }
        }
        return false;
    }

    /**
     * Tests whether or not the translog generation should be rolled to a new generation. This test is based on the size of the current
     * generation compared to the configured generation threshold size.
     *
     * @return {@code true} if the current generation should be rolled to a new generation
     */
    boolean shouldRollTranslogGeneration() {
        final Engine engine = getEngineOrNull();
        if (engine != null) {
            try {
                final Translog translog = engine.getTranslog();
                return translog.shouldRollGeneration();
            } catch (final AlreadyClosedException e) {
                // we are already closed, no need to flush or roll
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

    public Closeable acquireTranslogRetentionLock() {
        Engine engine = getEngine();
        return engine.getTranslog().acquireRetentionLock();
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

    public String getHistoryUUID() {
        return getEngine().getHistoryUUID();
    }

    public IndexEventListener getIndexEventListener() {
        return indexEventListener;
    }

    public void activateThrottling() {
        try {
            getEngine().activateThrottling();
        } catch (AlreadyClosedException ex) {
            // ignore
        }
    }

    public void deactivateThrottling() {
        try {
            getEngine().deactivateThrottling();
        } catch (AlreadyClosedException ex) {
            // ignore
        }
    }

    private void handleRefreshException(Exception e) {
        if (e instanceof AlreadyClosedException) {
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
        try {
            Engine engine = getEngine();
            engine.writeIndexingBuffer();
        } catch (Exception e) {
            handleRefreshException(e);
        }
    }

    /**
     * Notifies the service to update the local checkpoint for the shard with the provided allocation ID. See
     * {@link org.elasticsearch.index.seqno.GlobalCheckpointTracker#updateLocalCheckpoint(String, long)} for
     * details.
     *
     * @param allocationId the allocation ID of the shard to update the local checkpoint for
     * @param checkpoint   the local checkpoint for the shard
     */
    public void updateLocalCheckpointForShard(final String allocationId, final long checkpoint) {
        verifyPrimary();
        verifyNotClosed();
        getEngine().seqNoService().updateLocalCheckpointForShard(allocationId, checkpoint);
    }

    /**
     * Update the local knowledge of the global checkpoint for the specified allocation ID.
     *
     * @param allocationId     the allocation ID to update the global checkpoint for
     * @param globalCheckpoint the global checkpoint
     */
    public void updateGlobalCheckpointForShard(final String allocationId, final long globalCheckpoint) {
        verifyPrimary();
        verifyNotClosed();
        getEngine().seqNoService().updateGlobalCheckpointForShard(allocationId, globalCheckpoint);
    }

    /**
     * Waits for all operations up to the provided sequence number to complete.
     *
     * @param seqNo the sequence number that the checkpoint must advance to before this method returns
     * @throws InterruptedException if the thread was interrupted while blocking on the condition
     */
    public void waitForOpsToComplete(final long seqNo) throws InterruptedException {
        getEngine().seqNoService().waitForOpsToComplete(seqNo);
    }

    /**
     * Called when the recovery process for a shard is ready to open the engine on the target shard.
     * See {@link GlobalCheckpointTracker#initiateTracking(String)} for details.
     *
     * @param allocationId  the allocation ID of the shard for which recovery was initiated
     */
    public void initiateTracking(final String allocationId) {
        verifyPrimary();
        getEngine().seqNoService().initiateTracking(allocationId);
    }

    /**
     * Marks the shard with the provided allocation ID as in-sync with the primary shard. See
     * {@link org.elasticsearch.index.seqno.GlobalCheckpointTracker#markAllocationIdAsInSync(String, long)}
     * for additional details.
     *
     * @param allocationId    the allocation ID of the shard to mark as in-sync
     * @param localCheckpoint the current local checkpoint on the shard
     */
    public void markAllocationIdAsInSync(final String allocationId, final long localCheckpoint) throws InterruptedException {
        verifyPrimary();
        getEngine().seqNoService().markAllocationIdAsInSync(allocationId, localCheckpoint);
    }

    /**
     * Returns the local checkpoint for the shard.
     *
     * @return the local checkpoint
     */
    public long getLocalCheckpoint() {
        return getEngine().seqNoService().getLocalCheckpoint();
    }

    /**
     * Returns the global checkpoint for the shard.
     *
     * @return the global checkpoint
     */
    public long getGlobalCheckpoint() {
        return getEngine().seqNoService().getGlobalCheckpoint();
    }

    /**
     * Get the local knowledge of the global checkpoints for all in-sync allocation IDs.
     *
     * @return a map from allocation ID to the local knowledge of the global checkpoint for that allocation ID
     */
    public ObjectLongMap<String> getInSyncGlobalCheckpoints() {
        verifyPrimary();
        verifyNotClosed();
        return getEngine().seqNoService().getInSyncGlobalCheckpoints();
    }

    /**
     * Syncs the global checkpoint to the replicas if the global checkpoint on at least one replica is behind the global checkpoint on the
     * primary.
     */
    public void maybeSyncGlobalCheckpoint(final String reason) {
        verifyPrimary();
        verifyNotClosed();
        if (state == IndexShardState.RELOCATED) {
            return;
        }
        // only sync if there are not operations in flight
        final SeqNoStats stats = getEngine().seqNoService().stats();
        if (stats.getMaxSeqNo() == stats.getGlobalCheckpoint()) {
            final ObjectLongMap<String> globalCheckpoints = getInSyncGlobalCheckpoints();
            final String allocationId = routingEntry().allocationId().getId();
            assert globalCheckpoints.containsKey(allocationId);
            final long globalCheckpoint = globalCheckpoints.get(allocationId);
            final boolean syncNeeded =
                    StreamSupport
                            .stream(globalCheckpoints.values().spliterator(), false)
                            .anyMatch(v -> v.value < globalCheckpoint);
            // only sync if there is a shard lagging the primary
            if (syncNeeded) {
                logger.trace("syncing global checkpoint for [{}]", reason);
                globalCheckpointSyncer.run();
            }
        }
    }

    /**
     * Returns the current replication group for the shard.
     *
     * @return the replication group
     */
    public ReplicationGroup getReplicationGroup() {
        verifyPrimary();
        verifyNotClosed();
        return getEngine().seqNoService().getReplicationGroup();
    }

    /**
     * Updates the global checkpoint on a replica shard after it has been updated by the primary.
     *
     * @param globalCheckpoint the global checkpoint
     * @param reason           the reason the global checkpoint was updated
     */
    public void updateGlobalCheckpointOnReplica(final long globalCheckpoint, final String reason) {
        verifyReplicationTarget();
        final SequenceNumbersService seqNoService = getEngine().seqNoService();
        final long localCheckpoint = seqNoService.getLocalCheckpoint();
        if (globalCheckpoint > localCheckpoint) {
            /*
             * This can happen during recovery when the shard has started its engine but recovery is not finalized and is receiving global
             * checkpoint updates. However, since this shard is not yet contributing to calculating the global checkpoint, it can be the
             * case that the global checkpoint update from the primary is ahead of the local checkpoint on this shard. In this case, we
             * ignore the global checkpoint update. This can happen if we are in the translog stage of recovery. Prior to this, the engine
             * is not opened and this shard will not receive global checkpoint updates, and after this the shard will be contributing to
             * calculations of the global checkpoint. However, we can not assert that we are in the translog stage of recovery here as
             * while the global checkpoint update may have emanated from the primary when we were in that state, we could subsequently move
             * to recovery finalization, or even finished recovery before the update arrives here.
             */
            assert state() != IndexShardState.POST_RECOVERY && state() != IndexShardState.STARTED && state() != IndexShardState.RELOCATED :
                "supposedly in-sync shard copy received a global checkpoint [" + globalCheckpoint + "] " +
                    "that is higher than its local checkpoint [" + localCheckpoint + "]";
            return;
        }
        seqNoService.updateGlobalCheckpointOnReplica(globalCheckpoint, reason);
    }

    /**
     * Updates the known allocation IDs and the local checkpoints for the corresponding allocations from a primary relocation source.
     *
     * @param primaryContext the sequence number context
     */
    public void activateWithPrimaryContext(final GlobalCheckpointTracker.PrimaryContext primaryContext) {
        verifyPrimary();
        assert shardRouting.isRelocationTarget() : "only relocation target can update allocation IDs from primary context: " + shardRouting;
        assert primaryContext.getCheckpointStates().containsKey(routingEntry().allocationId().getId()) &&
            getEngine().seqNoService().getLocalCheckpoint() ==
                primaryContext.getCheckpointStates().get(routingEntry().allocationId().getId()).getLocalCheckpoint();
        getEngine().seqNoService().activateWithPrimaryContext(primaryContext);
    }

    /**
     * Check if there are any recoveries pending in-sync.
     *
     * @return {@code true} if there is at least one shard pending in-sync, otherwise false
     */
    public boolean pendingInSync() {
        verifyPrimary();
        return getEngine().seqNoService().pendingInSync();
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
            MetadataSnapshot metadata = snapshotStoreMetadata();
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
                logger.warn("check index [failure]\n{}", os.bytes().utf8ToString());
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
                    logger.warn("check index [failure]\n{}", os.bytes().utf8ToString());
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
            logger.debug("check index [success]\n{}", os.bytes().utf8ToString());
        }

        recoveryState.getVerifyIndex().checkIndexTime(Math.max(0, TimeValue.nsecToMSec(System.nanoTime() - timeNS)));
    }

    Engine getEngine() {
        Engine engine = getEngineOrNull();
        if (engine == null) {
            throw new AlreadyClosedException("engine is closed");
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

    public void startRecovery(RecoveryState recoveryState, PeerRecoveryTargetService recoveryTargetService,
                              PeerRecoveryTargetService.RecoveryListener recoveryListener, RepositoriesService repositoriesService,
                              BiConsumer<String, MappingMetaData> mappingUpdateConsumer,
                              IndicesService indicesService) {
        // TODO: Create a proper object to encapsulate the recovery context
        // all of the current methods here follow a pattern of:
        // resolve context which isn't really dependent on the local shards and then async
        // call some external method with this pointer.
        // with a proper recovery context object we can simply change this to:
        // startRecovery(RecoveryState recoveryState, ShardRecoverySource source ) {
        //     markAsRecovery("from " + source.getShortDescription(), recoveryState);
        //     threadPool.generic().execute()  {
        //           onFailure () { listener.failure() };
        //           doRun() {
        //                if (source.recover(this)) {
        //                  recoveryListener.onRecoveryDone(recoveryState);
        //                }
        //           }
        //     }}
        // }
        assert recoveryState.getRecoverySource().equals(shardRouting.recoverySource());
        switch (recoveryState.getRecoverySource().getType()) {
            case EMPTY_STORE:
            case EXISTING_STORE:
                markAsRecovering("from store", recoveryState); // mark the shard as recovering on the cluster state thread
                threadPool.generic().execute(() -> {
                    try {
                        if (recoverFromStore()) {
                            recoveryListener.onRecoveryDone(recoveryState);
                        }
                    } catch (Exception e) {
                        recoveryListener.onRecoveryFailure(recoveryState, new RecoveryFailedException(recoveryState, null, e), true);
                    }
                });
                break;
            case PEER:
                try {
                    markAsRecovering("from " + recoveryState.getSourceNode(), recoveryState);
                    recoveryTargetService.startRecovery(this, recoveryState.getSourceNode(), recoveryListener);
                } catch (Exception e) {
                    failShard("corrupted preexisting index", e);
                    recoveryListener.onRecoveryFailure(recoveryState, new RecoveryFailedException(recoveryState, null, e), true);
                }
                break;
            case SNAPSHOT:
                markAsRecovering("from snapshot", recoveryState); // mark the shard as recovering on the cluster state thread
                SnapshotRecoverySource recoverySource = (SnapshotRecoverySource) recoveryState.getRecoverySource();
                threadPool.generic().execute(() -> {
                    try {
                        final Repository repository = repositoriesService.repository(recoverySource.snapshot().getRepository());
                        if (restoreFromRepository(repository)) {
                            recoveryListener.onRecoveryDone(recoveryState);
                        }
                    } catch (Exception e) {
                        recoveryListener.onRecoveryFailure(recoveryState, new RecoveryFailedException(recoveryState, null, e), true);
                    }
                });
                break;
            case LOCAL_SHARDS:
                final IndexMetaData indexMetaData = indexSettings().getIndexMetaData();
                final Index resizeSourceIndex = indexMetaData.getResizeSourceIndex();
                final List<IndexShard> startedShards = new ArrayList<>();
                final IndexService sourceIndexService = indicesService.indexService(resizeSourceIndex);
                final Set<ShardId> requiredShards;
                final int numShards;
                if (sourceIndexService != null) {
                    requiredShards = IndexMetaData.selectRecoverFromShards(shardId().id(),
                        sourceIndexService.getMetaData(), indexMetaData.getNumberOfShards());
                    for (IndexShard shard : sourceIndexService) {
                        if (shard.state() == IndexShardState.STARTED && requiredShards.contains(shard.shardId())) {
                            startedShards.add(shard);
                        }
                    }
                    numShards = requiredShards.size();
                } else {
                    numShards = -1;
                    requiredShards = Collections.emptySet();
                }

                if (numShards == startedShards.size()) {
                    assert requiredShards.isEmpty() == false;
                    markAsRecovering("from local shards", recoveryState); // mark the shard as recovering on the cluster state thread
                    threadPool.generic().execute(() -> {
                        try {
                            if (recoverFromLocalShards(mappingUpdateConsumer, startedShards.stream()
                                .filter((s) -> requiredShards.contains(s.shardId())).collect(Collectors.toList()))) {
                                recoveryListener.onRecoveryDone(recoveryState);
                            }
                        } catch (Exception e) {
                            recoveryListener.onRecoveryFailure(recoveryState,
                                new RecoveryFailedException(recoveryState, null, e), true);
                        }
                    });
                } else {
                    final RuntimeException e;
                    if (numShards == -1) {
                        e = new IndexNotFoundException(resizeSourceIndex);
                    } else {
                        e = new IllegalStateException("not all required shards of index " + resizeSourceIndex
                            + " are started yet, expected " + numShards + " found " + startedShards.size() + " can't recover shard "
                            + shardId());
                    }
                    throw e;
                }
                break;
            default:
                throw new IllegalArgumentException("Unknown recovery source " + recoveryState.getRecoverySource());
        }
    }

    class ShardEventListener implements Engine.EventListener {
        private final CopyOnWriteArrayList<Consumer<ShardFailure>> delegates = new CopyOnWriteArrayList<>();

        // called by the current engine
        @Override
        public void onFailedEngine(String reason, @Nullable Exception failure) {
            final ShardFailure shardFailure = new ShardFailure(shardRouting, reason, failure);
            for (Consumer<ShardFailure> listener : delegates) {
                try {
                    listener.accept(shardFailure);
                } catch (Exception inner) {
                    inner.addSuppressed(failure);
                    logger.warn("exception while notifying engine failure", inner);
                }
            }
        }
    }

    private Engine createNewEngine(EngineConfig config) {
        synchronized (mutex) {
            if (state == IndexShardState.CLOSED) {
                throw new AlreadyClosedException(shardId + " can't create engine - shard is closed");
            }
            assert this.currentEngineReference.get() == null;
            Engine engine = newEngine(config);
            onNewEngine(engine); // call this before we pass the memory barrier otherwise actions that happen
            // inside the callback are not visible. This one enforces happens-before
            this.currentEngineReference.set(engine);
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

    private static void persistMetadata(
            final ShardPath shardPath,
            final IndexSettings indexSettings,
            final ShardRouting newRouting,
            final @Nullable ShardRouting currentRouting,
            final Logger logger) throws IOException {
        assert newRouting != null : "newRouting must not be null";

        // only persist metadata if routing information that is persisted in shard state metadata actually changed
        final ShardId shardId = newRouting.shardId();
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
            final ShardStateMetaData newShardStateMetadata =
                    new ShardStateMetaData(newRouting.primary(), indexSettings.getUUID(), newRouting.allocationId());
            ShardStateMetaData.FORMAT.write(newShardStateMetadata, shardPath.getShardStatePath());
        } else {
            logger.trace("{} skip writing shard state, has been written before", shardId);
        }
    }

    private DocumentMapperForType docMapper(String type) {
        return mapperService.documentMapperWithAutoCreate(type);
    }

    private EngineConfig newEngineConfig(EngineConfig.OpenMode openMode) {
        Sort indexSort = indexSortSupplier.get();
        final boolean forceNewHistoryUUID;
        switch (shardRouting.recoverySource().getType()) {
            case EXISTING_STORE:
            case PEER:
                forceNewHistoryUUID = false;
                break;
            case EMPTY_STORE:
            case SNAPSHOT:
            case LOCAL_SHARDS:
                forceNewHistoryUUID = true;
                break;
            default:
                throw new AssertionError("unknown recovery type: [" + shardRouting.recoverySource().getType() + "]");
        }
        return new EngineConfig(openMode, shardId, shardRouting.allocationId().getId(),
            threadPool, indexSettings, warmer, store, indexSettings.getMergePolicy(),
            mapperService.indexAnalyzer(), similarityService.similarity(mapperService), codecService, shardEventListener,
            indexCache.query(), cachingPolicy, forceNewHistoryUUID, translogConfig,
            IndexingMemoryController.SHARD_INACTIVE_TIME_SETTING.get(indexSettings.getSettings()),
            Collections.singletonList(refreshListeners),
            Collections.singletonList(new RefreshMetricUpdater(refreshMetric)),
            indexSort, this::runTranslogRecovery);
    }

    /**
     * Acquire a primary operation permit whenever the shard is ready for indexing. If a permit is directly available, the provided
     * ActionListener will be called on the calling thread. During relocation hand-off, permit acquisition can be delayed. The provided
     * ActionListener will then be called using the provided executor.
     */
    public void acquirePrimaryOperationPermit(ActionListener<Releasable> onPermitAcquired, String executorOnDelay) {
        verifyNotClosed();
        verifyPrimary();

        indexShardOperationPermits.acquire(onPermitAcquired, executorOnDelay, false);
    }

    private final Object primaryTermMutex = new Object();

    /**
     * Acquire a replica operation permit whenever the shard is ready for indexing (see
     * {@link #acquirePrimaryOperationPermit(ActionListener, String)}). If the given primary term is lower than then one in
     * {@link #shardRouting}, the {@link ActionListener#onFailure(Exception)} method of the provided listener is invoked with an
     * {@link IllegalStateException}. If permit acquisition is delayed, the listener will be invoked on the executor with the specified
     * name.
     *
     * @param operationPrimaryTerm the operation primary term
     * @param globalCheckpoint     the global checkpoint associated with the request
     * @param onPermitAcquired     the listener for permit acquisition
     * @param executorOnDelay      the name of the executor to invoke the listener on if permit acquisition is delayed
     */
    public void acquireReplicaOperationPermit(final long operationPrimaryTerm, final long globalCheckpoint,
                                              final ActionListener<Releasable> onPermitAcquired, final String executorOnDelay) {
        verifyNotClosed();
        verifyReplicationTarget();
        final boolean globalCheckpointUpdated;
        if (operationPrimaryTerm > primaryTerm) {
            synchronized (primaryTermMutex) {
                if (operationPrimaryTerm > primaryTerm) {
                    IndexShardState shardState = state();
                    // only roll translog and update primary term if shard has made it past recovery
                    // Having a new primary term here means that the old primary failed and that there is a new primary, which again
                    // means that the master will fail this shard as all initializing shards are failed when a primary is selected
                    // We abort early here to prevent an ongoing recovery from the failed primary to mess with the global / local checkpoint
                    if (shardState != IndexShardState.POST_RECOVERY &&
                        shardState != IndexShardState.STARTED &&
                        shardState != IndexShardState.RELOCATED) {
                        throw new IndexShardNotStartedException(shardId, shardState);
                    }
                    try {
                        indexShardOperationPermits.blockOperations(30, TimeUnit.MINUTES, () -> {
                            assert operationPrimaryTerm > primaryTerm :
                                "shard term already update.  op term [" + operationPrimaryTerm + "], shardTerm [" + primaryTerm + "]";
                            primaryTerm = operationPrimaryTerm;
                            updateGlobalCheckpointOnReplica(globalCheckpoint, "primary term transition");
                            final long currentGlobalCheckpoint = getGlobalCheckpoint();
                            final long localCheckpoint;
                            if (currentGlobalCheckpoint == SequenceNumbers.UNASSIGNED_SEQ_NO) {
                                localCheckpoint = SequenceNumbers.NO_OPS_PERFORMED;
                            } else {
                                localCheckpoint = currentGlobalCheckpoint;
                            }
                            logger.trace(
                                    "detected new primary with primary term [{}], resetting local checkpoint from [{}] to [{}]",
                                    operationPrimaryTerm,
                                    getLocalCheckpoint(),
                                    localCheckpoint);
                            getEngine().seqNoService().resetLocalCheckpoint(localCheckpoint);
                            getEngine().getTranslog().rollGeneration();
                        });
                        globalCheckpointUpdated = true;
                    } catch (final Exception e) {
                        onPermitAcquired.onFailure(e);
                        return;
                    }
                } else {
                    globalCheckpointUpdated = false;
                }
            }
        } else {
            globalCheckpointUpdated = false;
        }

        assert operationPrimaryTerm <= primaryTerm
                : "operation primary term [" + operationPrimaryTerm + "] should be at most [" + primaryTerm + "]";
        indexShardOperationPermits.acquire(
                new ActionListener<Releasable>() {
                    @Override
                    public void onResponse(final Releasable releasable) {
                        if (operationPrimaryTerm < primaryTerm) {
                            releasable.close();
                            final String message = String.format(
                                    Locale.ROOT,
                                    "%s operation primary term [%d] is too old (current [%d])",
                                    shardId,
                                    operationPrimaryTerm,
                                    primaryTerm);
                            onPermitAcquired.onFailure(new IllegalStateException(message));
                        } else {
                            if (globalCheckpointUpdated == false) {
                                try {
                                    updateGlobalCheckpointOnReplica(globalCheckpoint, "operation");
                                } catch (Exception e) {
                                    releasable.close();
                                    onPermitAcquired.onFailure(e);
                                    return;
                                }
                            }
                            onPermitAcquired.onResponse(releasable);
                        }
                    }

                    @Override
                    public void onFailure(final Exception e) {
                        onPermitAcquired.onFailure(e);
                    }
                },
                executorOnDelay,
                true);
    }

    public int getActiveOperationsCount() {
        return indexShardOperationPermits.getActiveOperationsCount(); // refCount is incremented on successful acquire and decremented on close
    }

    private final AsyncIOProcessor<Translog.Location> translogSyncProcessor = new AsyncIOProcessor<Translog.Location>(logger, 1024) {
        @Override
        protected void write(List<Tuple<Translog.Location, Consumer<Exception>>> candidates) throws IOException {
            try {
                final Engine engine = getEngine();
                engine.getTranslog().ensureSynced(candidates.stream().map(Tuple::v1));
            } catch (AlreadyClosedException ex) {
                // that's fine since we already synced everything on engine close - this also is conform with the methods
                // documentation
            } catch (IOException ex) { // if this fails we are in deep shit - fail the request
                logger.debug("failed to sync translog", ex);
                throw ex;
            }
        }
    };

    /**
     * Syncs the given location with the underlying storage unless already synced. This method might return immediately without
     * actually fsyncing the location until the sync listener is called. Yet, unless there is already another thread fsyncing
     * the transaction log the caller thread will be hijacked to run the fsync for all pending fsync operations.
     * This method allows indexing threads to continue indexing without blocking on fsync calls. We ensure that there is only
     * one thread blocking on the sync an all others can continue indexing.
     * NOTE: if the syncListener throws an exception when it's processed the exception will only be logged. Users should make sure that the
     * listener handles all exception cases internally.
     */
    public final void sync(Translog.Location location, Consumer<Exception> syncListener) {
        verifyNotClosed();
        translogSyncProcessor.put(location, syncListener);
    }

    public final void sync() throws IOException {
        verifyNotClosed();
        getEngine().getTranslog().sync();
    }

    /**
     * Returns the current translog durability mode
     */
    public Translog.Durability getTranslogDurability() {
        return indexSettings.getTranslogDurability();
    }

    // we can not protect with a lock since we "release" on a different thread
    private final AtomicBoolean flushOrRollRunning = new AtomicBoolean();

    /**
     * Schedules a flush or translog generation roll if needed but will not schedule more than one concurrently. The operation will be
     * executed asynchronously on the flush thread pool.
     */
    public void afterWriteOperation() {
        if (shouldFlush() || shouldRollTranslogGeneration()) {
            if (flushOrRollRunning.compareAndSet(false, true)) {
                /*
                 * We have to check again since otherwise there is a race when a thread passes the first check next to another thread which
                 * performs the operation quickly enough to  finish before the current thread could flip the flag. In that situation, we
                 * have an extra operation.
                 *
                 * Additionally, a flush implicitly executes a translog generation roll so if we execute a flush then we do not need to
                 * check if we should roll the translog generation.
                 */
                if (shouldFlush()) {
                    logger.debug("submitting async flush request");
                    final AbstractRunnable flush = new AbstractRunnable() {
                        @Override
                        public void onFailure(final Exception e) {
                            if (state != IndexShardState.CLOSED) {
                                logger.warn("failed to flush index", e);
                            }
                        }

                        @Override
                        protected void doRun() throws IOException {
                            flush(new FlushRequest());
                        }

                        @Override
                        public void onAfter() {
                            flushOrRollRunning.compareAndSet(true, false);
                            afterWriteOperation();
                        }
                    };
                    threadPool.executor(ThreadPool.Names.FLUSH).execute(flush);
                } else if (shouldRollTranslogGeneration()) {
                    logger.debug("submitting async roll translog generation request");
                    final AbstractRunnable roll = new AbstractRunnable() {
                        @Override
                        public void onFailure(final Exception e) {
                            if (state != IndexShardState.CLOSED) {
                                logger.warn("failed to roll translog generation", e);
                            }
                        }

                        @Override
                        protected void doRun() throws Exception {
                            rollTranslogGeneration();
                        }

                        @Override
                        public void onAfter() {
                            flushOrRollRunning.compareAndSet(true, false);
                            afterWriteOperation();
                        }
                    };
                    threadPool.executor(ThreadPool.Names.FLUSH).execute(roll);
                } else {
                    flushOrRollRunning.compareAndSet(true, false);
                }
            }
        }
    }

    /**
     * Build {@linkplain RefreshListeners} for this shard.
     */
    private RefreshListeners buildRefreshListeners() {
        return new RefreshListeners(
            indexSettings::getMaxRefreshListeners,
            () -> refresh("too_many_listeners"),
            threadPool.executor(ThreadPool.Names.LISTENER)::execute,
            logger, threadPool.getThreadContext());
    }

    /**
     * Simple struct encapsulating a shard failure
     *
     * @see IndexShard#addShardFailureCallback(Consumer)
     */
    public static final class ShardFailure {
        public final ShardRouting routing;
        public final String reason;
        @Nullable
        public final Exception cause;

        public ShardFailure(ShardRouting routing, String reason, @Nullable Exception cause) {
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
     * @throws AlreadyClosedException if the engine or internal indexwriter in the engine is already closed
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
        final boolean readAllowed;
        if (isReadAllowed()) {
            readAllowed = true;
        } else {
            // check again under mutex. this is important to create a happens before relationship
            // between the switch to POST_RECOVERY + associated refresh. Otherwise we may respond
            // to a listener before a refresh actually happened that contained that operation.
            synchronized (mutex) {
                readAllowed = isReadAllowed();
            }
        }
        if (readAllowed) {
            refreshListeners.addOrNotify(location, listener);
        } else {
            // we're not yet ready fo ready for reads, just ignore refresh cycles
            listener.accept(false);
        }
    }

    private static class RefreshMetricUpdater implements ReferenceManager.RefreshListener {

        private final MeanMetric refreshMetric;
        private long currentRefreshStartTime;
        private Thread callingThread = null;

        private RefreshMetricUpdater(MeanMetric refreshMetric) {
            this.refreshMetric = refreshMetric;
        }

        @Override
        public void beforeRefresh() throws IOException {
            if (Assertions.ENABLED) {
                assert callingThread == null : "beforeRefresh was called by " + callingThread.getName() +
                    " without a corresponding call to afterRefresh";
                callingThread = Thread.currentThread();
            }
            currentRefreshStartTime = System.nanoTime();
        }

        @Override
        public void afterRefresh(boolean didRefresh) throws IOException {
            if (Assertions.ENABLED) {
                assert callingThread != null : "afterRefresh called but not beforeRefresh";
                assert callingThread == Thread.currentThread() : "beforeRefreshed called by a different thread. current ["
                    + Thread.currentThread().getName() + "], thread that called beforeRefresh [" + callingThread.getName() + "]";
                callingThread = null;
            }
            refreshMetric.inc(System.nanoTime() - currentRefreshStartTime);
        }
    }
}
