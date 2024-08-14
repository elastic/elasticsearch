/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.shard;

import org.apache.logging.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.DelegatingAnalyzerWrapper;
import org.apache.lucene.index.CheckIndex;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.FilterDirectoryReader;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.search.QueryCachingPolicy;
import org.apache.lucene.search.ReferenceManager;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.UsageTrackingQueryCachingPolicy;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.util.SetOnce;
import org.apache.lucene.util.ThreadInterruptedException;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.action.admin.indices.flush.FlushRequest;
import org.elasticsearch.action.admin.indices.forcemerge.ForceMergeRequest;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.action.support.replication.PendingReplicationActions;
import org.elasticsearch.action.support.replication.ReplicationResponse;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.MappingMetadata;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.RecoverySource.SnapshotRecoverySource;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.decider.DiskThresholdDecider;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.lucene.index.ElasticsearchDirectoryReader;
import org.elasticsearch.common.metrics.CounterMetric;
import org.elasticsearch.common.metrics.MeanMetric;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Assertions;
import org.elasticsearch.core.Booleans;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.core.CheckedFunction;
import org.elasticsearch.core.CheckedRunnable;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.gateway.WriteStateException;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.bulk.stats.BulkOperationListener;
import org.elasticsearch.index.bulk.stats.BulkStats;
import org.elasticsearch.index.bulk.stats.ShardBulkStats;
import org.elasticsearch.index.cache.IndexCache;
import org.elasticsearch.index.cache.bitset.ShardBitsetFilterCache;
import org.elasticsearch.index.cache.query.TrivialQueryCachingPolicy;
import org.elasticsearch.index.cache.request.ShardRequestCache;
import org.elasticsearch.index.codec.CodecService;
import org.elasticsearch.index.engine.CommitStats;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.Engine.GetResult;
import org.elasticsearch.index.engine.EngineConfig;
import org.elasticsearch.index.engine.EngineException;
import org.elasticsearch.index.engine.EngineFactory;
import org.elasticsearch.index.engine.ReadOnlyEngine;
import org.elasticsearch.index.engine.RefreshFailedEngineException;
import org.elasticsearch.index.engine.SafeCommitInfo;
import org.elasticsearch.index.engine.Segment;
import org.elasticsearch.index.engine.SegmentsStats;
import org.elasticsearch.index.fielddata.FieldDataStats;
import org.elasticsearch.index.fielddata.ShardFieldData;
import org.elasticsearch.index.flush.FlushStats;
import org.elasticsearch.index.get.GetStats;
import org.elasticsearch.index.get.ShardGetService;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperMetrics;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.Mapping;
import org.elasticsearch.index.mapper.MappingLookup;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.mapper.SourceToParse;
import org.elasticsearch.index.mapper.Uid;
import org.elasticsearch.index.merge.MergeStats;
import org.elasticsearch.index.recovery.RecoveryStats;
import org.elasticsearch.index.refresh.RefreshStats;
import org.elasticsearch.index.search.stats.FieldUsageStats;
import org.elasticsearch.index.search.stats.SearchStats;
import org.elasticsearch.index.search.stats.ShardFieldUsageTracker;
import org.elasticsearch.index.search.stats.ShardSearchStats;
import org.elasticsearch.index.seqno.ReplicationTracker;
import org.elasticsearch.index.seqno.RetentionLease;
import org.elasticsearch.index.seqno.RetentionLeaseStats;
import org.elasticsearch.index.seqno.RetentionLeaseSyncer;
import org.elasticsearch.index.seqno.RetentionLeases;
import org.elasticsearch.index.seqno.SeqNoStats;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.shard.PrimaryReplicaSyncer.ResyncTask;
import org.elasticsearch.index.similarity.SimilarityService;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.store.Store.MetadataSnapshot;
import org.elasticsearch.index.store.StoreFileMetadata;
import org.elasticsearch.index.store.StoreStats;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.index.translog.TranslogConfig;
import org.elasticsearch.index.translog.TranslogStats;
import org.elasticsearch.index.warmer.ShardIndexWarmerService;
import org.elasticsearch.index.warmer.WarmerStats;
import org.elasticsearch.indices.IndexingMemoryController;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.indices.cluster.IndicesClusterStateService;
import org.elasticsearch.indices.recovery.PeerRecoveryTargetService;
import org.elasticsearch.indices.recovery.RecoveryFailedException;
import org.elasticsearch.indices.recovery.RecoverySettings;
import org.elasticsearch.indices.recovery.RecoveryState;
import org.elasticsearch.indices.recovery.RecoveryTarget;
import org.elasticsearch.plugins.IndexStorePlugin;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.repositories.Repository;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.internal.FieldUsageTrackingDirectoryReader;
import org.elasticsearch.search.suggest.completion.CompletionStats;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.Closeable;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.channels.ClosedByInterruptException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.EnumSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.LongSupplier;
import java.util.function.LongUnaryOperator;
import java.util.function.Supplier;

import static org.elasticsearch.cluster.metadata.DataStream.TIMESERIES_LEAF_READERS_SORTER;
import static org.elasticsearch.core.Strings.format;
import static org.elasticsearch.index.seqno.RetentionLeaseActions.RETAIN_ALL;
import static org.elasticsearch.index.seqno.SequenceNumbers.UNASSIGNED_SEQ_NO;

public class IndexShard extends AbstractIndexShardComponent implements IndicesClusterStateService.Shard {

    private final ThreadPool threadPool;
    private final MapperService mapperService;
    private final IndexCache indexCache;
    private final Store store;
    private final InternalIndexingStats internalIndexingStats;
    private final ShardSearchStats searchStats = new ShardSearchStats();
    private final ShardFieldUsageTracker fieldUsageTracker;
    private final String shardUuid = UUIDs.randomBase64UUID();
    private final long shardCreationTime;
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
    // Package visible for testing
    final CircuitBreakerService circuitBreakerService;

    private final SearchOperationListener searchOperationListener;

    private final ShardBulkStats bulkOperationListener;
    private final GlobalCheckpointListeners globalCheckpointListeners;
    private final PendingReplicationActions pendingReplicationActions;
    private final ReplicationTracker replicationTracker;
    private final IndexStorePlugin.SnapshotCommitSupplier snapshotCommitSupplier;
    private final Engine.IndexCommitListener indexCommitListener;
    private FieldInfos fieldInfos;
    private volatile ShardFieldStats shardFieldStats;

    // sys prop to disable the field has value feature, defaults to true (enabled) if set to false (disabled) the
    // field caps always returns empty fields ignoring the value of the query param `field_caps_empty_fields_filter`.
    private final boolean enableFieldHasValue = Booleans.parseBoolean(
        System.getProperty("es.field_caps_empty_fields_filter", Boolean.TRUE.toString())
    );

    protected volatile ShardRouting shardRouting;
    protected volatile IndexShardState state;
    // ensure happens-before relation between addRefreshListener() and postRecovery()
    private volatile SubscribableListener<Void> postRecoveryComplete;
    private volatile long pendingPrimaryTerm; // see JavaDocs for getPendingPrimaryTerm
    private final Object engineMutex = new Object(); // lock ordering: engineMutex -> mutex
    private final AtomicReference<Engine> currentEngineReference = new AtomicReference<>();
    final EngineFactory engineFactory;

    private final IndexingOperationListener indexingOperationListeners;
    private final GlobalCheckpointSyncer globalCheckpointSyncer;

    private final RetentionLeaseSyncer retentionLeaseSyncer;

    @Nullable
    private volatile RecoveryState recoveryState;

    private final RecoveryStats recoveryStats = new RecoveryStats();
    private final MeanMetric refreshMetric = new MeanMetric();
    private final MeanMetric externalRefreshMetric = new MeanMetric();
    private final MeanMetric flushMetric = new MeanMetric();
    private final CounterMetric periodicFlushMetric = new CounterMetric();

    private final ShardEventListener shardEventListener = new ShardEventListener();

    private final ShardPath path;

    private final IndexShardOperationPermits indexShardOperationPermits;

    private static final EnumSet<IndexShardState> readAllowedStates = EnumSet.of(IndexShardState.STARTED, IndexShardState.POST_RECOVERY);
    // for primaries, we only allow to write when actually started (so the cluster has decided we started)
    // in case we have a relocation of a primary, we also allow to write after phase 2 completed, where the shard may be
    // in state RECOVERING or POST_RECOVERY.
    // for replicas, replication is also allowed while recovering, since we index also during recovery to replicas and rely on
    // version checks to make sure its consistent a relocated shard can also be target of a replication if the relocation target has not
    // been marked as active yet and is syncing it's changes back to the relocation source
    private static final EnumSet<IndexShardState> writeAllowedStates = EnumSet.of(
        IndexShardState.RECOVERING,
        IndexShardState.POST_RECOVERY,
        IndexShardState.STARTED
    );

    private final CheckedFunction<DirectoryReader, DirectoryReader, IOException> readerWrapper;

    /**
     * True if this shard is still indexing (recently) and false if we've been idle for long enough (as periodically checked by {@link
     * IndexingMemoryController}).
     */
    private final AtomicBoolean active = new AtomicBoolean();
    /**
     * Allows for the registration of listeners that are called when a change becomes visible for search.
     */
    private final RefreshListeners refreshListeners;

    private final AtomicLong lastSearcherAccess = new AtomicLong();
    private final AtomicReference<Translog.Location> pendingRefreshLocation = new AtomicReference<>();
    private final RefreshPendingLocationListener refreshPendingLocationListener;
    private final RefreshFieldHasValueListener refreshFieldHasValueListener;
    private volatile boolean useRetentionLeasesInPeerRecovery;
    private final LongSupplier relativeTimeInNanosSupplier;
    private volatile long startedRelativeTimeInNanos;
    private volatile long indexingTimeBeforeShardStartedInNanos;
    private final SubscribableListener<Void> waitForEngineOrClosedShardListeners = new SubscribableListener<>();

    // the translog keeps track of the GCP, but unpromotable shards have no translog so we need to track the GCP here instead
    private volatile long globalCheckPointIfUnpromotable;

    @SuppressWarnings("this-escape")
    public IndexShard(
        final ShardRouting shardRouting,
        final IndexSettings indexSettings,
        final ShardPath path,
        final Store store,
        final Supplier<Sort> indexSortSupplier,
        final IndexCache indexCache,
        final MapperService mapperService,
        final SimilarityService similarityService,
        final EngineFactory engineFactory,
        final IndexEventListener indexEventListener,
        final CheckedFunction<DirectoryReader, DirectoryReader, IOException> indexReaderWrapper,
        final ThreadPool threadPool,
        final BigArrays bigArrays,
        final Engine.Warmer warmer,
        final List<SearchOperationListener> searchOperationListener,
        final List<IndexingOperationListener> listeners,
        final GlobalCheckpointSyncer globalCheckpointSyncer,
        final RetentionLeaseSyncer retentionLeaseSyncer,
        final CircuitBreakerService circuitBreakerService,
        final IndexStorePlugin.SnapshotCommitSupplier snapshotCommitSupplier,
        final LongSupplier relativeTimeInNanosSupplier,
        final Engine.IndexCommitListener indexCommitListener,
        final MapperMetrics mapperMetrics
    ) throws IOException {
        super(shardRouting.shardId(), indexSettings);
        assert shardRouting.initializing();
        this.shardRouting = shardRouting;
        final Settings settings = indexSettings.getSettings();
        this.codecService = new CodecService(mapperService, bigArrays);
        this.warmer = warmer;
        this.similarityService = similarityService;
        Objects.requireNonNull(store, "Store must be provided to the index shard");
        this.engineFactory = Objects.requireNonNull(engineFactory);
        this.snapshotCommitSupplier = Objects.requireNonNull(snapshotCommitSupplier);
        this.store = store;
        this.indexSortSupplier = indexSortSupplier;
        this.indexEventListener = indexEventListener;
        this.threadPool = threadPool;
        this.mapperService = mapperService;
        this.indexCache = indexCache;
        this.internalIndexingStats = new InternalIndexingStats();
        this.indexingOperationListeners = new IndexingOperationListener.CompositeListener(
            CollectionUtils.appendToCopyNoNullElements(listeners, internalIndexingStats),
            logger
        );
        this.bulkOperationListener = new ShardBulkStats();
        this.globalCheckpointSyncer = globalCheckpointSyncer;
        this.retentionLeaseSyncer = Objects.requireNonNull(retentionLeaseSyncer);
        this.searchOperationListener = new SearchOperationListener.CompositeListener(
            CollectionUtils.appendToCopyNoNullElements(searchOperationListener, searchStats),
            logger
        );
        this.getService = new ShardGetService(indexSettings, this, mapperService, mapperMetrics);
        this.shardWarmerService = new ShardIndexWarmerService(shardId, indexSettings);
        this.requestCacheStats = new ShardRequestCache();
        this.shardFieldData = new ShardFieldData();
        this.shardBitsetFilterCache = new ShardBitsetFilterCache(shardId, indexSettings);
        state = IndexShardState.CREATED;
        this.path = path;
        this.circuitBreakerService = circuitBreakerService;
        /* create engine config */
        logger.debug("state: [CREATED]");

        this.checkIndexOnStartup = indexSettings.getValue(IndexSettings.INDEX_CHECK_ON_STARTUP);
        this.translogConfig = new TranslogConfig(shardId, shardPath().resolveTranslog(), indexSettings, bigArrays);
        final String aId = shardRouting.allocationId().getId();
        final long primaryTerm = indexSettings.getIndexMetadata().primaryTerm(shardId.id());
        this.pendingPrimaryTerm = primaryTerm;
        this.globalCheckpointListeners = new GlobalCheckpointListeners(shardId, threadPool.scheduler(), logger);
        this.pendingReplicationActions = new PendingReplicationActions(shardId, threadPool);
        this.replicationTracker = new ReplicationTracker(
            shardId,
            aId,
            indexSettings,
            primaryTerm,
            UNASSIGNED_SEQ_NO,
            globalCheckpointListeners::globalCheckpointUpdated,
            threadPool::absoluteTimeInMillis,
            (retentionLeases, listener) -> retentionLeaseSyncer.sync(shardId, aId, getPendingPrimaryTerm(), retentionLeases, listener),
            this::getSafeCommitInfo,
            pendingReplicationActions
        );
        fieldUsageTracker = new ShardFieldUsageTracker();
        shardCreationTime = threadPool.absoluteTimeInMillis();

        // the query cache is a node-level thing, however we want the most popular filters
        // to be computed on a per-shard basis
        if (IndexModule.INDEX_QUERY_CACHE_EVERYTHING_SETTING.get(settings)) {
            cachingPolicy = TrivialQueryCachingPolicy.ALWAYS;
        } else {
            cachingPolicy = new UsageTrackingQueryCachingPolicy();
        }
        indexShardOperationPermits = new IndexShardOperationPermits(shardId, threadPool);
        readerWrapper = indexReaderWrapper;
        refreshListeners = new RefreshListeners(
            indexSettings::getMaxRefreshListeners,
            () -> refresh("too_many_listeners"),
            logger,
            threadPool.getThreadContext(),
            externalRefreshMetric
        );
        lastSearcherAccess.set(threadPool.relativeTimeInMillis());
        persistMetadata(path, indexSettings, shardRouting, null, logger);
        this.useRetentionLeasesInPeerRecovery = replicationTracker.hasAllPeerRecoveryRetentionLeases();
        this.refreshPendingLocationListener = new RefreshPendingLocationListener();
        this.refreshFieldHasValueListener = new RefreshFieldHasValueListener();
        this.relativeTimeInNanosSupplier = relativeTimeInNanosSupplier;
        this.indexCommitListener = indexCommitListener;
        this.fieldInfos = FieldInfos.EMPTY;
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

    public BulkOperationListener getBulkOperationListener() {
        return this.bulkOperationListener;
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

    public boolean isSystem() {
        return indexSettings.getIndexMetadata().isSystem();
    }

    /**
     * USE THIS METHOD WITH CARE!
     * Returns the primary term the index shard is supposed to be on. In case of primary promotion or when a replica learns about
     * a new term due to a new primary, the term that's exposed here will not be the term that the shard internally uses to assign
     * to operations. The shard will auto-correct its internal operation term, but this might take time.
     * See {@link org.elasticsearch.cluster.metadata.IndexMetadata#primaryTerm(int)}
     */
    public long getPendingPrimaryTerm() {
        return this.pendingPrimaryTerm;
    }

    /** Returns the primary term that is currently being used to assign to operations */
    public long getOperationPrimaryTerm() {
        return replicationTracker.getOperationPrimaryTerm();
    }

    /**
     * Returns a unique UUID that identifies this IndexShard instance
     */
    public String getShardUuid() {
        return shardUuid;
    }

    /**
     * Returns the timestamp at which this IndexShard instance was created
     */
    public long getShardCreationTime() {
        return shardCreationTime;
    }

    /**
     * Returns the latest cluster routing entry received with this shard.
     */
    @Override
    public ShardRouting routingEntry() {
        return this.shardRouting;
    }

    @Override
    public void updateShardState(
        final ShardRouting newRouting,
        final long newPrimaryTerm,
        final BiConsumer<IndexShard, ActionListener<ResyncTask>> primaryReplicaSyncer,
        final long applyingClusterStateVersion,
        final Set<String> inSyncAllocationIds,
        final IndexShardRoutingTable routingTable
    ) throws IOException {
        final ShardRouting currentRouting;
        synchronized (mutex) {
            currentRouting = this.shardRouting;
            assert currentRouting != null;

            if (newRouting.shardId().equals(shardId()) == false) {
                throw new IllegalArgumentException(
                    "Trying to set a routing entry with shardId " + newRouting.shardId() + " on a shard with shardId " + shardId()
                );
            }
            if (newRouting.isSameAllocation(currentRouting) == false) {
                throw new IllegalArgumentException(
                    "Trying to set a routing entry with a different allocation. Current " + currentRouting + ", new " + newRouting
                );
            }
            if (currentRouting.primary() && newRouting.primary() == false) {
                throw new IllegalArgumentException(
                    "illegal state: trying to move shard from primary mode to replica mode. Current "
                        + currentRouting
                        + ", new "
                        + newRouting
                );
            }

            if (newRouting.primary()) {
                replicationTracker.updateFromMaster(applyingClusterStateVersion, inSyncAllocationIds, routingTable);
            }

            if (state == IndexShardState.POST_RECOVERY && newRouting.active()) {
                assert currentRouting.active() == false : "we are in POST_RECOVERY, but our shard routing is active " + currentRouting;
                assert currentRouting.isRelocationTarget() == false
                    || currentRouting.primary() == false
                    || replicationTracker.isPrimaryMode()
                    : "a primary relocation is completed by the master, but primary mode is not active " + currentRouting;

                changeState(IndexShardState.STARTED, "global state is [" + newRouting.state() + "]");
                startedRelativeTimeInNanos = getRelativeTimeInNanos();
                indexingTimeBeforeShardStartedInNanos = internalIndexingStats.totalIndexingTimeInNanos();
            } else if (currentRouting.primary()
                && currentRouting.relocating()
                && replicationTracker.isRelocated()
                && (newRouting.relocating() == false || newRouting.equalsIgnoringMetadata(currentRouting) == false)) {
                    // if the shard is not in primary mode anymore (after primary relocation) we have to fail when any changes in shard
                    // routing occur (e.g. due to recovery failure / cancellation). The reason is that at the moment we cannot safely
                    // reactivate primary mode without risking two active primaries.
                    throw new IndexShardRelocatedException(
                        shardId(),
                        "Shard is marked as relocated, cannot safely move to state " + newRouting.state()
                    );
                }

            if (newRouting.active() && state != IndexShardState.STARTED && state != IndexShardState.CLOSED) {
                // If cluster.no_master_block: all then we remove all shards locally whenever there's no master, but there might still be
                // a shard-started message in flight. When the new master is elected we start to recover our shards again and the stale
                // shard-started message could arrive and move this shard to STARTED in the cluster state too soon. This is pretty rare so
                // we fix it by just failing the shard and starting the recovery again.
                //
                // NB this can only happen on replicas - if it happened to a primary then we'd move to a new primary term and ignore the
                // stale shard-started message.
                assert newRouting.primary() == false
                    : "primary routing is active, but local shard state isn't. routing: " + newRouting + ", local state: " + state;
                throw new IllegalIndexShardStateException(shardId, state, "master processed stale shard-started event, failing shard");
            }

            persistMetadata(path, indexSettings, newRouting, currentRouting, logger);
            final CountDownLatch shardStateUpdated = new CountDownLatch(1);

            if (newRouting.primary()) {
                if (newPrimaryTerm == pendingPrimaryTerm) {
                    if (currentRouting.initializing() && currentRouting.isRelocationTarget() == false && newRouting.active()) {
                        // the master started a recovering primary, activate primary mode.
                        replicationTracker.activatePrimaryMode(getLocalCheckpoint());
                        ensurePeerRecoveryRetentionLeasesExist();
                    }
                } else {
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
                    assert newRouting.initializing() == false
                        : "a started primary shard should never update its term; "
                            + "shard "
                            + newRouting
                            + ", "
                            + "current term ["
                            + pendingPrimaryTerm
                            + "], "
                            + "new term ["
                            + newPrimaryTerm
                            + "]";
                    assert newPrimaryTerm > pendingPrimaryTerm
                        : "primary terms can only go up; current term [" + pendingPrimaryTerm + "], new term [" + newPrimaryTerm + "]";
                    /*
                     * Before this call returns, we are guaranteed that all future operations are delayed and so this happens before we
                     * increment the primary term. The latch is needed to ensure that we do not unblock operations before the primary
                     * term is incremented.
                     */
                    // to prevent primary relocation handoff while resync is not completed
                    boolean resyncStarted = primaryReplicaResyncInProgress.compareAndSet(false, true);
                    if (resyncStarted == false) {
                        throw new IllegalStateException("cannot start resync while it's already in progress");
                    }
                    bumpPrimaryTerm(newPrimaryTerm, () -> {
                        shardStateUpdated.await();
                        assert pendingPrimaryTerm == newPrimaryTerm
                            : "shard term changed on primary. expected ["
                                + newPrimaryTerm
                                + "] but was ["
                                + pendingPrimaryTerm
                                + "]"
                                + ", current routing: "
                                + currentRouting
                                + ", new routing: "
                                + newRouting;
                        assert getOperationPrimaryTerm() == newPrimaryTerm;
                        try {
                            replicationTracker.activatePrimaryMode(getLocalCheckpoint());
                            ensurePeerRecoveryRetentionLeasesExist();
                            /*
                             * If this shard was serving as a replica shard when another shard was promoted to primary then
                             * its Lucene index was reset during the primary term transition. In particular, the Lucene index
                             * on this shard was reset to the global checkpoint and the operations above the local checkpoint
                             * were reverted. If the other shard that was promoted to primary subsequently fails before the
                             * primary/replica re-sync completes successfully and we are now being promoted, we have to restore
                             * the reverted operations on this shard by replaying the translog to avoid losing acknowledged writes.
                             */
                            final Engine engine = getEngine();
                            engine.restoreLocalHistoryFromTranslog(
                                (resettingEngine, snapshot) -> runTranslogRecovery(
                                    resettingEngine,
                                    snapshot,
                                    Engine.Operation.Origin.LOCAL_RESET,
                                    () -> {}
                                )
                            );
                            /* Rolling the translog generation is not strictly needed here (as we will never have collisions between
                             * sequence numbers in a translog generation in a new primary as it takes the last known sequence number
                             * as a starting point), but it simplifies reasoning about the relationship between primary terms and
                             * translog generations.
                             */
                            engine.rollTranslogGeneration();
                            engine.fillSeqNoGaps(newPrimaryTerm);
                            replicationTracker.updateLocalCheckpoint(currentRouting.allocationId().getId(), getLocalCheckpoint());
                            primaryReplicaSyncer.accept(this, new ActionListener<ResyncTask>() {
                                @Override
                                public void onResponse(ResyncTask resyncTask) {
                                    logger.info("primary-replica resync completed with {} operations", resyncTask.getResyncedOperations());
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
                                        try {
                                            failShard("exception during primary-replica resync", e);
                                        } catch (AlreadyClosedException ace) {
                                            // okay, the index was deleted
                                        }
                                    }
                                }
                            });
                        } catch (final AlreadyClosedException e) {
                            // okay, the index was deleted
                        }
                    }, null);
                }
            }
            // set this last, once we finished updating all internal state.
            this.shardRouting = newRouting;

            assert this.shardRouting.primary() == false || this.shardRouting.started() == false || // note that we use started and not
                                                                                                   // active to avoid relocating shards
                this.indexShardOperationPermits.isBlocked() || // if permits are blocked, we are still transitioning
                this.replicationTracker.isPrimaryMode()
                : "a started primary with non-pending operation term must be in primary mode " + this.shardRouting;
            shardStateUpdated.countDown();
        }
        if (currentRouting.active() == false && newRouting.active()) {
            indexEventListener.afterIndexShardStarted(this);
        }
        if (newRouting.equals(currentRouting) == false) {
            indexEventListener.shardRoutingChanged(this, currentRouting, newRouting);
        }

        if (indexSettings.isSoftDeleteEnabled() && useRetentionLeasesInPeerRecovery == false) {
            final RetentionLeases retentionLeases = replicationTracker.getRetentionLeases();
            boolean allShardsUseRetentionLeases = true;
            for (int copy = 0; copy < routingTable.size(); copy++) {
                ShardRouting shardRouting = routingTable.shard(copy);
                if (shardRouting.isPromotableToPrimary()) {
                    if (shardRouting.assignedToNode() == false
                        || retentionLeases.contains(ReplicationTracker.getPeerRecoveryRetentionLeaseId(shardRouting)) == false) {
                        allShardsUseRetentionLeases = false;
                        break;
                    }
                    if (this.shardRouting.relocating()) {
                        ShardRouting shardRoutingReloc = this.shardRouting.getTargetRelocatingShard();
                        if (shardRoutingReloc.assignedToNode() == false
                            || retentionLeases.contains(ReplicationTracker.getPeerRecoveryRetentionLeaseId(shardRoutingReloc)) == false) {
                            allShardsUseRetentionLeases = false;
                            break;
                        }
                    }
                }
            }
            useRetentionLeasesInPeerRecovery = allShardsUseRetentionLeases;
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
     * {@link BiConsumer} is executed after all operations are successfully blocked.
     *
     * @param consumer a {@link BiConsumer} that is executed after operations are blocked and that consumes the primary context as well as
     *                 a listener to resolve once it finished
     * @param listener listener to resolve once this method actions including executing {@code consumer} in the non-failure case complete
     * @throws IllegalIndexShardStateException if the shard is not relocating due to concurrent cancellation
     * @throws IllegalStateException           if the relocation target is no longer part of the replication group
     */
    public void relocated(
        final String targetNodeId,
        final String targetAllocationId,
        final BiConsumer<ReplicationTracker.PrimaryContext, ActionListener<Void>> consumer,
        final ActionListener<Void> listener
    ) throws IllegalIndexShardStateException, IllegalStateException {
        assert shardRouting.primary() : "only primaries can be marked as relocated: " + shardRouting;
        try (Releasable forceRefreshes = refreshListeners.forceRefreshes()) {
            indexShardOperationPermits.blockOperations(new ActionListener<>() {
                @Override
                public void onResponse(Releasable releasable) {
                    boolean success = false;
                    try {
                        forceRefreshes.close();
                        // no shard operation permits are being held here, move state from started to relocated
                        assert indexShardOperationPermits.getActiveOperationsCount() == OPERATIONS_BLOCKED
                            : "in-flight operations in progress while moving shard state to relocated";
                        /*
                         * We should not invoke the runnable under the mutex as the expected implementation is to handoff the primary
                         * context via a network operation. Doing this under the mutex can implicitly block the cluster state update thread
                         * on network operations.
                         */
                        verifyRelocatingState(targetNodeId);
                        final ReplicationTracker.PrimaryContext primaryContext = replicationTracker.startRelocationHandoff(
                            targetAllocationId
                        );
                        // make sure we release all permits before we resolve the final listener
                        final ActionListener<Void> wrappedInnerListener = ActionListener.runBefore(
                            listener,
                            Releasables.releaseOnce(releasable)::close
                        );
                        final ActionListener<Void> wrappedListener = new ActionListener<>() {
                            @Override
                            public void onResponse(Void unused) {
                                try {
                                    // make changes to primaryMode and relocated flag only under mutex
                                    synchronized (mutex) {
                                        verifyRelocatingState(targetNodeId);
                                        replicationTracker.completeRelocationHandoff();
                                    }
                                    wrappedInnerListener.onResponse(null);
                                } catch (Exception e) {
                                    onFailure(e);
                                }
                            }

                            @Override
                            public void onFailure(Exception e) {
                                try {
                                    replicationTracker.abortRelocationHandoff();
                                } catch (final Exception inner) {
                                    e.addSuppressed(inner);
                                }
                                wrappedInnerListener.onFailure(e);
                            }
                        };
                        try {
                            consumer.accept(primaryContext, wrappedListener);
                        } catch (final Exception e) {
                            wrappedListener.onFailure(e);
                        }
                        success = true;
                    } catch (Exception e) {
                        listener.onFailure(e);
                    } finally {
                        if (success == false) {
                            releasable.close();
                        }
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    if (e instanceof TimeoutException) {
                        logger.warn("timed out waiting for relocation hand-off to complete");
                        // This is really bad as ongoing replication operations are preventing this shard from completing relocation
                        // hand-off.
                        // Fail primary relocation source and target shards.
                        failShard("timed out waiting for relocation hand-off to complete", null);
                        listener.onFailure(
                            new IndexShardClosedException(shardId(), "timed out waiting for relocation hand-off to complete")
                        );
                    } else {
                        listener.onFailure(e);
                    }
                }
            }, 30L, TimeUnit.MINUTES, EsExecutors.DIRECT_EXECUTOR_SERVICE); // Wait on current thread because this execution is wrapped by
                                                                            // CancellableThreads and we want to be able to interrupt it
        }
    }

    private void verifyRelocatingState(String targetNodeId) {
        if (state != IndexShardState.STARTED) {
            throw new IndexShardNotStartedException(shardId, state);
        }
        /*
         * If the master cancelled recovery, the target will be removed and the recovery will be cancelled. However, it is still possible
         * that we concurrently end up here and therefore have to protect that we do not mark the shard as relocated when its shard routing
         * says otherwise.
         */

        if (shardRouting.relocating() == false) {
            throw new IllegalIndexShardStateException(shardId, IndexShardState.STARTED, ": shard is no longer relocating " + shardRouting);
        }

        if (Objects.equals(targetNodeId, shardRouting.relocatingNodeId()) == false) {
            throw new IllegalIndexShardStateException(
                shardId,
                IndexShardState.STARTED,
                ": shard is no longer relocating to node [" + targetNodeId + "]: " + shardRouting
            );
        }

        if (primaryReplicaResyncInProgress.get()) {
            throw new IllegalIndexShardStateException(
                shardId,
                IndexShardState.STARTED,
                ": primary relocation is forbidden while primary-replica resync is in progress " + shardRouting
            );
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

    public Engine.IndexResult applyIndexOperationOnPrimary(
        long version,
        VersionType versionType,
        SourceToParse sourceToParse,
        long ifSeqNo,
        long ifPrimaryTerm,
        long autoGeneratedTimestamp,
        boolean isRetry
    ) throws IOException {
        assert versionType.validateVersionForWrites(version);
        return applyIndexOperation(
            getEngine(),
            UNASSIGNED_SEQ_NO,
            getOperationPrimaryTerm(),
            version,
            versionType,
            ifSeqNo,
            ifPrimaryTerm,
            autoGeneratedTimestamp,
            isRetry,
            Engine.Operation.Origin.PRIMARY,
            sourceToParse
        );
    }

    public Engine.IndexResult applyIndexOperationOnReplica(
        long seqNo,
        long opPrimaryTerm,
        long version,
        long autoGeneratedTimeStamp,
        boolean isRetry,
        SourceToParse sourceToParse
    ) throws IOException {
        return applyIndexOperation(
            getEngine(),
            seqNo,
            opPrimaryTerm,
            version,
            null,
            UNASSIGNED_SEQ_NO,
            0,
            autoGeneratedTimeStamp,
            isRetry,
            Engine.Operation.Origin.REPLICA,
            sourceToParse
        );
    }

    private Engine.IndexResult applyIndexOperation(
        Engine engine,
        long seqNo,
        long opPrimaryTerm,
        long version,
        @Nullable VersionType versionType,
        long ifSeqNo,
        long ifPrimaryTerm,
        long autoGeneratedTimeStamp,
        boolean isRetry,
        Engine.Operation.Origin origin,
        SourceToParse sourceToParse
    ) throws IOException {
        assert opPrimaryTerm <= getOperationPrimaryTerm()
            : "op term [ " + opPrimaryTerm + " ] > shard term [" + getOperationPrimaryTerm() + "]";
        ensureWriteAllowed(origin);
        Engine.Index operation;
        try {
            operation = prepareIndex(
                mapperService,
                sourceToParse,
                seqNo,
                opPrimaryTerm,
                version,
                versionType,
                origin,
                autoGeneratedTimeStamp,
                isRetry,
                ifSeqNo,
                ifPrimaryTerm,
                getRelativeTimeInNanos()
            );
            Mapping update = operation.parsedDoc().dynamicMappingsUpdate();
            if (update != null) {
                return new Engine.IndexResult(update, operation.parsedDoc().id());
            }
        } catch (Exception e) {
            // We treat any exception during parsing and or mapping update as a document level failure
            // with the exception side effects of closing the shard. Since we don't have the shard, we
            // can not raise an exception that may block any replication of previous operations to the
            // replicas
            verifyNotClosed(e);
            return new Engine.IndexResult(e, version, opPrimaryTerm, seqNo, sourceToParse.id());
        }
        return index(engine, operation);
    }

    public void setFieldInfos(FieldInfos fieldInfos) {
        this.fieldInfos = fieldInfos;
    }

    public FieldInfos getFieldInfos() {
        return fieldInfos;
    }

    public static Engine.Index prepareIndex(
        MapperService mapperService,
        SourceToParse source,
        long seqNo,
        long primaryTerm,
        long version,
        VersionType versionType,
        Engine.Operation.Origin origin,
        long autoGeneratedIdTimestamp,
        boolean isRetry,
        long ifSeqNo,
        long ifPrimaryTerm,
        long startTimeInNanos
    ) {
        assert source.dynamicTemplates().isEmpty() || origin == Engine.Operation.Origin.PRIMARY
            : "dynamic_templates parameter can only be associated with primary operations";
        DocumentMapper documentMapper = mapperService.documentMapper();
        Mapping mapping = null;
        if (documentMapper == null) {
            documentMapper = DocumentMapper.createEmpty(mapperService);
            mapping = documentMapper.mapping();
        }
        ParsedDocument doc = documentMapper.parse(source);
        if (mapping != null) {
            // If we are indexing but there is no mapping we create one. This is to ensure that whenever at least a document is indexed
            // some mappings do exist. It covers for the case of indexing an empty doc (`{}`).
            // TODO this can be removed if we eagerly create mappings as soon as a new index is created, regardless of
            // whether mappings were provided or not.
            doc.addDynamicMappingsUpdate(mapping);
        }
        return new Engine.Index(
            Uid.encodeId(doc.id()),
            doc,
            seqNo,
            primaryTerm,
            version,
            versionType,
            origin,
            startTimeInNanos,
            autoGeneratedIdTimestamp,
            isRetry,
            ifSeqNo,
            ifPrimaryTerm
        );
    }

    private Engine.IndexResult index(Engine engine, Engine.Index index) throws IOException {
        try {
            final Engine.IndexResult result;
            final Engine.Index preIndex = indexingOperationListeners.preIndex(shardId, index);
            try {
                if (logger.isTraceEnabled()) {
                    // don't use index.source().utf8ToString() here source might not be valid UTF-8
                    logger.trace(
                        "index [{}] seq# [{}] allocation-id [{}] primaryTerm [{}] operationPrimaryTerm [{}] origin [{}]",
                        preIndex.id(),
                        preIndex.seqNo(),
                        routingEntry().allocationId(),
                        preIndex.primaryTerm(),
                        getOperationPrimaryTerm(),
                        preIndex.origin()
                    );
                }
                result = engine.index(preIndex);
                if (logger.isTraceEnabled()) {
                    logger.trace(
                        "index-done [{}] seq# [{}] allocation-id [{}] primaryTerm [{}] operationPrimaryTerm [{}] origin [{}] "
                            + "result-seq# [{}] result-term [{}] failure [{}]",
                        preIndex.id(),
                        preIndex.seqNo(),
                        routingEntry().allocationId(),
                        preIndex.primaryTerm(),
                        getOperationPrimaryTerm(),
                        preIndex.origin(),
                        result.getSeqNo(),
                        result.getTerm(),
                        result.getFailure()
                    );
                }
            } catch (Exception e) {
                if (logger.isTraceEnabled()) {
                    logger.trace(
                        () -> format(
                            "index-fail [%s] seq# [%s] allocation-id [%s] primaryTerm [%s] operationPrimaryTerm [%s] origin [%s]",
                            preIndex.id(),
                            preIndex.seqNo(),
                            routingEntry().allocationId(),
                            preIndex.primaryTerm(),
                            getOperationPrimaryTerm(),
                            preIndex.origin()
                        ),
                        e
                    );
                }
                indexingOperationListeners.postIndex(shardId, preIndex, e);
                throw e;
            }
            indexingOperationListeners.postIndex(shardId, preIndex, result);
            return result;
        } finally {
            active.set(true);
        }
    }

    public Engine.NoOpResult markSeqNoAsNoop(long seqNo, long opPrimaryTerm, String reason) throws IOException {
        return markSeqNoAsNoop(getEngine(), seqNo, opPrimaryTerm, reason, Engine.Operation.Origin.REPLICA);
    }

    private Engine.NoOpResult markSeqNoAsNoop(Engine engine, long seqNo, long opPrimaryTerm, String reason, Engine.Operation.Origin origin)
        throws IOException {
        assert opPrimaryTerm <= getOperationPrimaryTerm()
            : "op term [ " + opPrimaryTerm + " ] > shard term [" + getOperationPrimaryTerm() + "]";
        long startTime = System.nanoTime();
        ensureWriteAllowed(origin);
        final Engine.NoOp noOp = new Engine.NoOp(seqNo, opPrimaryTerm, origin, startTime, reason);
        return noOp(engine, noOp);
    }

    private Engine.NoOpResult noOp(Engine engine, Engine.NoOp noOp) throws IOException {
        try {
            if (logger.isTraceEnabled()) {
                logger.trace("noop (seq# [{}])", noOp.seqNo());
            }
            return engine.noOp(noOp);
        } finally {
            active.set(true);
        }
    }

    public Engine.IndexResult getFailedIndexResult(Exception e, long version, String id) {
        return new Engine.IndexResult(e, version, id);
    }

    public Engine.DeleteResult getFailedDeleteResult(Exception e, long version, String id) {
        return new Engine.DeleteResult(e, version, getOperationPrimaryTerm(), id);
    }

    public Engine.DeleteResult applyDeleteOperationOnPrimary(
        long version,
        String id,
        VersionType versionType,
        long ifSeqNo,
        long ifPrimaryTerm
    ) throws IOException {
        assert versionType.validateVersionForWrites(version);
        return applyDeleteOperation(
            getEngine(),
            UNASSIGNED_SEQ_NO,
            getOperationPrimaryTerm(),
            version,
            id,
            versionType,
            ifSeqNo,
            ifPrimaryTerm,
            Engine.Operation.Origin.PRIMARY
        );
    }

    public Engine.DeleteResult applyDeleteOperationOnReplica(long seqNo, long opPrimaryTerm, long version, String id) throws IOException {
        return applyDeleteOperation(
            getEngine(),
            seqNo,
            opPrimaryTerm,
            version,
            id,
            null,
            UNASSIGNED_SEQ_NO,
            0,
            Engine.Operation.Origin.REPLICA
        );
    }

    private Engine.DeleteResult applyDeleteOperation(
        Engine engine,
        long seqNo,
        long opPrimaryTerm,
        long version,
        String id,
        @Nullable VersionType versionType,
        long ifSeqNo,
        long ifPrimaryTerm,
        Engine.Operation.Origin origin
    ) throws IOException {
        assert opPrimaryTerm <= getOperationPrimaryTerm()
            : "op term [ " + opPrimaryTerm + " ] > shard term [" + getOperationPrimaryTerm() + "]";
        ensureWriteAllowed(origin);
        try {
            Engine.Delete delete = indexingOperationListeners.preDelete(
                shardId,
                prepareDelete(id, seqNo, opPrimaryTerm, version, versionType, origin, ifSeqNo, ifPrimaryTerm)
            );
            final Engine.DeleteResult result;
            try {
                if (logger.isTraceEnabled()) {
                    logger.trace("delete [{}] (seq no [{}])", delete.uid(), delete.seqNo());
                }
                result = engine.delete(delete);
            } catch (Exception e) {
                indexingOperationListeners.postDelete(shardId, delete, e);
                throw e;
            }
            indexingOperationListeners.postDelete(shardId, delete, result);
            return result;
        } finally {
            active.set(true);
        }
    }

    public static Engine.Delete prepareDelete(
        String id,
        long seqNo,
        long primaryTerm,
        long version,
        VersionType versionType,
        Engine.Operation.Origin origin,
        long ifSeqNo,
        long ifPrimaryTerm
    ) {
        long startTime = System.nanoTime();
        return new Engine.Delete(id, Uid.encodeId(id), seqNo, primaryTerm, version, versionType, origin, startTime, ifSeqNo, ifPrimaryTerm);
    }

    public Engine.GetResult get(Engine.Get get) {
        return innerGet(get, false, this::wrapSearcher);
    }

    /**
     * Invokes the consumer with a {@link MultiEngineGet} that can perform multiple engine gets without wrapping searchers multiple times.
     * Callers must not pass the provided {@link MultiEngineGet} to other threads.
     */
    public void mget(Consumer<MultiEngineGet> mgetter) {
        final MultiEngineGet mget = new MultiEngineGet(this::wrapSearcher) {
            @Override
            public GetResult get(Engine.Get get) {
                return innerGet(get, false, this::wrapSearchSearchWithCache);
            }
        };
        try {
            mgetter.accept(mget);
        } finally {
            mget.releaseCachedSearcher();
        }
    }

    public Engine.GetResult getFromTranslog(Engine.Get get) {
        assert get.realtime();
        return innerGet(get, true, this::wrapSearcher);
    }

    private Engine.GetResult innerGet(Engine.Get get, boolean translogOnly, Function<Engine.Searcher, Engine.Searcher> searcherWrapper) {
        readAllowed();
        MappingLookup mappingLookup = mapperService.mappingLookup();
        if (mappingLookup.hasMappings() == false) {
            return GetResult.NOT_EXISTS;
        }
        if (indexSettings.getIndexVersionCreated().isLegacyIndexVersion()) {
            throw new IllegalStateException("get operations not allowed on a legacy index");
        }
        if (translogOnly) {
            return getEngine().getFromTranslog(get, mappingLookup, mapperService.documentParser(), searcherWrapper);
        }
        return getEngine().get(get, mappingLookup, mapperService.documentParser(), searcherWrapper);
    }

    /**
     * Writes all indexing changes to disk and opens a new searcher reflecting all changes.  This can throw {@link AlreadyClosedException}.
     */
    public Engine.RefreshResult refresh(String source) {
        verifyNotClosed();
        logger.trace("refresh with source [{}]", source);
        return getEngine().refresh(source);
    }

    public void externalRefresh(String source, ActionListener<Engine.RefreshResult> listener) {
        verifyNotClosed();
        getEngine().externalRefresh(source, listener);
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
        return new RefreshStats(
            refreshMetric.count(),
            TimeUnit.NANOSECONDS.toMillis(refreshMetric.sum()),
            externalRefreshMetric.count(),
            TimeUnit.NANOSECONDS.toMillis(externalRefreshMetric.sum()),
            listeners
        );
    }

    public FlushStats flushStats() {
        return new FlushStats(
            flushMetric.count(),
            periodicFlushMetric.count(),
            TimeUnit.NANOSECONDS.toMillis(flushMetric.sum()),
            getEngineOrNull() != null ? getEngineOrNull().getTotalFlushTimeExcludingWaitingOnLockInMillis() : 0L
        );
    }

    public DocsStats docStats() {
        readAllowed();
        return getEngine().docStats();
    }

    /**
     * @return {@link CommitStats}
     * @throws AlreadyClosedException if shard is closed
     */
    public CommitStats commitStats() {
        return getEngine().commitStats();
    }

    /**
     * @return {@link SeqNoStats}
     * @throws AlreadyClosedException if shard is closed
     */
    public SeqNoStats seqNoStats() {
        return getEngine().getSeqNoStats(replicationTracker.getGlobalCheckpoint());
    }

    public IndexingStats indexingStats() {
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

        return internalIndexingStats.stats(
            throttled,
            throttleTimeInMillis,
            indexingTimeBeforeShardStartedInNanos,
            getRelativeTimeInNanos() - startedRelativeTimeInNanos
        );
    }

    public SearchStats searchStats(String... groups) {
        return searchStats.stats(groups);
    }

    public FieldUsageStats fieldUsageStats(String... fields) {
        return fieldUsageTracker.stats(fields);
    }

    public GetStats getStats() {
        return getService.stats();
    }

    public StoreStats storeStats() {
        try {
            final RecoveryState recoveryState = this.recoveryState;
            if (DiskThresholdDecider.SETTING_IGNORE_DISK_WATERMARKS.get(indexSettings.getSettings())) {
                // if this shard has no disk footprint then its local size is reported as 0
                return store.stats(0, size -> 0);
            } else {
                final long bytesStillToRecover = recoveryState == null ? -1L : recoveryState.getIndex().bytesStillToRecover();
                final long reservedBytes = bytesStillToRecover == -1 ? StoreStats.UNKNOWN_RESERVED_BYTES : bytesStillToRecover;
                return store.stats(reservedBytes, LongUnaryOperator.identity());
            }
        } catch (IOException e) {
            failShard("Failing shard because of exception during storeStats", e);
            throw new ElasticsearchException("io exception while building 'store stats'", e);
        }
    }

    public MergeStats mergeStats() {
        final Engine engine = getEngineOrNull();
        if (engine == null) {
            return new MergeStats();
        }
        return engine.getMergeStats();
    }

    public SegmentsStats segmentStats(boolean includeSegmentFileSizes, boolean includeUnloadedSegments) {
        SegmentsStats segmentsStats = getEngine().segmentsStats(includeSegmentFileSizes, includeUnloadedSegments);
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
        return getEngine().getTranslogStats();
    }

    public CompletionStats completionStats(String... fields) {
        readAllowed();
        return getEngine().completionStats(fields);
    }

    public DenseVectorStats denseVectorStats() {
        readAllowed();
        MappingLookup mappingLookup = mapperService != null ? mapperService.mappingLookup() : null;
        return getEngine().denseVectorStats(mappingLookup);
    }

    public SparseVectorStats sparseVectorStats() {
        readAllowed();
        MappingLookup mappingLookup = mapperService != null ? mapperService.mappingLookup() : null;
        return getEngine().sparseVectorStats(mappingLookup);
    }

    public BulkStats bulkStats() {
        return bulkOperationListener.stats();
    }

    /**
     * Executes the given flush request against the engine.
     *
     * @param request the flush request
     * @return <code>false</code> if <code>waitIfOngoing==false</code> and an ongoing request is detected, else <code>true</code>.
     *         If <code>false</code> is returned, no flush happened.
     */
    public boolean flush(FlushRequest request) {
        PlainActionFuture<Boolean> future = new PlainActionFuture<>();
        flush(request, future);
        return future.actionGet();
    }

    /**
     * Executes the given flush request against the engine.
     *
     * @param request the flush request
     * @param listener to notify after full durability has been achieved.
     *                 <code>false</code> if <code>waitIfOngoing==false</code>
     *                 and an ongoing request is detected, else <code>true</code>.
     *                 If <code>false</code> is returned, no flush happened.
     */
    public void flush(FlushRequest request, ActionListener<Boolean> listener) {
        final boolean waitIfOngoing = request.waitIfOngoing();
        final boolean force = request.force();
        logger.trace("flush with {}", request);
        ActionListener.run(listener, l -> {
            /*
             * We allow flushes while recovery since we allow operations to happen while recovering and we want to keep the translog under
             * control (up to deletes, which we do not GC). Yet, we do not use flush internally to clear deletes and flush the index writer
             * since we use Engine#writeIndexingBuffer for this now.
             */
            verifyNotClosed();
            final long startTime = System.nanoTime();
            getEngine().flush(
                force,
                waitIfOngoing,
                ActionListener.runBefore(l.map(Engine.FlushResult::flushPerformed), () -> flushMetric.inc(System.nanoTime() - startTime))
            );
        });
    }

    /**
     * checks and removes translog files that no longer need to be retained. See
     * {@link org.elasticsearch.index.translog.TranslogDeletionPolicy} for details
     */
    public void trimTranslog() {
        verifyNotClosed();
        final Engine engine = getEngine();
        engine.trimUnreferencedTranslogFiles();
    }

    /**
     * Rolls the tranlog generation and cleans unneeded.
     */
    public void rollTranslogGeneration() {
        final Engine engine = getEngine();
        engine.rollTranslogGeneration();
    }

    public void forceMerge(ForceMergeRequest forceMerge) throws IOException {
        IndexShardState state = this.state; // one time volatile read
        if (state != IndexShardState.STARTED) {
            throw new IllegalIndexShardStateException(shardId, state, "operation only allowed when shard is active");
        }
        logger.trace("force merge with {}", forceMerge);
        Engine engine = getEngine();
        engine.forceMerge(forceMerge.flush(), forceMerge.maxNumSegments(), forceMerge.onlyExpungeDeletes(), forceMerge.forceMergeUUID());
    }

    /**
     * Creates a new {@link IndexCommit} snapshot from the currently running engine. All resources referenced by this
     * commit won't be freed until the commit / snapshot is closed.
     *
     * @param flushFirst <code>true</code> if the index should first be flushed to disk / a low level lucene commit should be executed
     */
    public Engine.IndexCommitRef acquireLastIndexCommit(boolean flushFirst) throws EngineException {
        final IndexShardState state = this.state; // one time volatile read
        // we allow snapshot on closed index shard, since we want to do one after we close the shard and before we close the engine
        if (state == IndexShardState.STARTED || state == IndexShardState.CLOSED) {
            return getEngine().acquireLastIndexCommit(flushFirst);
        } else {
            throw new IllegalIndexShardStateException(shardId, state, "snapshot is not allowed");
        }
    }

    /**
     * Acquires the {@link IndexCommit} which should be included in a snapshot.
     */
    public Engine.IndexCommitRef acquireIndexCommitForSnapshot() throws EngineException {
        final IndexShardState state = this.state; // one time volatile read
        if (state == IndexShardState.STARTED) {
            // unlike acquireLastIndexCommit(), there's no need to acquire a snapshot on a shard that is shutting down
            return getEngine().acquireIndexCommitForSnapshot();
        } else {
            throw new IllegalIndexShardStateException(shardId, state, "snapshot is not allowed");
        }
    }

    /**
     * Snapshots the most recent safe index commit from the currently running engine.
     * All index files referenced by this index commit won't be freed until the commit/snapshot is closed.
     */
    public Engine.IndexCommitRef acquireSafeIndexCommit() throws EngineException {
        final IndexShardState state = this.state; // one time volatile read
        // we allow snapshot on closed index shard, since we want to do one after we close the shard and before we close the engine
        if (state == IndexShardState.STARTED || state == IndexShardState.CLOSED) {
            return getEngine().acquireSafeIndexCommit();
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
        assert Thread.holdsLock(mutex) == false : "snapshotting store metadata under mutex";
        Engine.IndexCommitRef indexCommit = null;
        store.incRef();
        try {
            synchronized (engineMutex) {
                // if the engine is not running, we can access the store directly, but we need to make sure no one starts
                // the engine on us. If the engine is running, we can get a snapshot via the deletion policy of the engine.
                final Engine engine = getEngineOrNull();
                if (engine != null) {
                    indexCommit = engine.acquireLastIndexCommit(false);
                }
                if (indexCommit == null) {
                    return store.getMetadata(null, true);
                }
            }
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

    /**
     * Acquires a point-in-time reader that can be used to create {@link Engine.Searcher}s on demand.
     */
    public Engine.SearcherSupplier acquireSearcherSupplier() {
        return acquireSearcherSupplier(Engine.SearcherScope.EXTERNAL);
    }

    /**
     * Acquires a point-in-time reader that can be used to create {@link Engine.Searcher}s on demand.
     */
    public Engine.SearcherSupplier acquireSearcherSupplier(Engine.SearcherScope scope) {
        readAllowed();
        markSearcherAccessed();
        final Engine engine = getEngine();
        return engine.acquireSearcherSupplier(this::wrapSearcher, scope);
    }

    public Engine.Searcher acquireSearcher(String source) {
        readAllowed();
        markSearcherAccessed();
        final Engine engine = getEngine();
        return engine.acquireSearcher(source, Engine.SearcherScope.EXTERNAL, this::wrapSearcher);
    }

    private void markSearcherAccessed() {
        lastSearcherAccess.lazySet(threadPool.relativeTimeInMillis());
    }

    private Engine.Searcher wrapSearcher(Engine.Searcher searcher) {
        assert ElasticsearchDirectoryReader.unwrap(searcher.getDirectoryReader()) != null
            : "DirectoryReader must be an instance or ElasticsearchDirectoryReader";
        boolean success = false;
        try {
            final Engine.Searcher newSearcher = wrapSearcher(searcher, fieldUsageTracker.createSession(), readerWrapper);
            assert newSearcher != null;
            success = true;
            return newSearcher;
        } catch (IOException ex) {
            throw new ElasticsearchException("failed to wrap searcher", ex);
        } finally {
            if (success == false) {
                Releasables.closeWhileHandlingException(searcher);
            }
        }
    }

    static Engine.Searcher wrapSearcher(
        Engine.Searcher engineSearcher,
        ShardFieldUsageTracker.FieldUsageStatsTrackingSession fieldUsageStatsTrackingSession,
        @Nullable CheckedFunction<DirectoryReader, DirectoryReader, IOException> readerWrapper
    ) throws IOException {
        final ElasticsearchDirectoryReader elasticsearchDirectoryReader = ElasticsearchDirectoryReader.getElasticsearchDirectoryReader(
            engineSearcher.getDirectoryReader()
        );
        if (elasticsearchDirectoryReader == null) {
            throw new IllegalStateException("Can't wrap non elasticsearch directory reader");
        }
        if (readerWrapper == null) {
            readerWrapper = r -> r;
        }
        NonClosingReaderWrapper nonClosingReaderWrapper = new NonClosingReaderWrapper(engineSearcher.getDirectoryReader());
        // first apply field usage stats wrapping before applying other wrappers so that it can track the effects of these wrappers
        DirectoryReader reader = readerWrapper.apply(
            new FieldUsageTrackingDirectoryReader(nonClosingReaderWrapper, fieldUsageStatsTrackingSession)
        );
        if (reader.getReaderCacheHelper() != elasticsearchDirectoryReader.getReaderCacheHelper()) {
            throw new IllegalStateException(
                "wrapped directory reader doesn't delegate IndexReader#getCoreCacheKey,"
                    + " wrappers must override this method and delegate to the original readers core cache key. Wrapped readers can't be "
                    + "used as cache keys since their are used only per request which would lead to subtle bugs"
            );
        }
        if (ElasticsearchDirectoryReader.getElasticsearchDirectoryReader(reader) != elasticsearchDirectoryReader) {
            // prevent that somebody wraps with a non-filter reader
            throw new IllegalStateException("wrapped directory reader hides actual ElasticsearchDirectoryReader but shouldn't");
        }

        // we close the reader to make sure wrappers can release resources if needed....
        // our NonClosingReaderWrapper makes sure that our reader is not closed
        return new Engine.Searcher(
            engineSearcher.source(),
            reader,
            engineSearcher.getSimilarity(),
            engineSearcher.getQueryCache(),
            engineSearcher.getQueryCachingPolicy(),
            () -> IOUtils.close(
                reader, // this will close the wrappers excluding the NonClosingReaderWrapper
                engineSearcher, // this will run the closeable on the wrapped engine reader
                fieldUsageStatsTrackingSession
            )
        ); // completes stats recording
    }

    public void setGlobalCheckpointIfUnpromotable(long globalCheckpoint) {
        assert shardRouting.isPromotableToPrimary() == false : "must only call this on unpromotable shards";
        globalCheckPointIfUnpromotable = globalCheckpoint;
    }

    private static final class NonClosingReaderWrapper extends FilterDirectoryReader {

        private NonClosingReaderWrapper(DirectoryReader in) throws IOException {
            super(in, new SubReaderWrapper() {
                @Override
                public LeafReader wrap(LeafReader reader) {
                    return reader;
                }
            });
        }

        @Override
        protected DirectoryReader doWrapDirectoryReader(DirectoryReader in) throws IOException {
            return new NonClosingReaderWrapper(in);
        }

        @Override
        protected void doClose() {
            // don't close here - mimic the MultiReader#doClose = false behavior that FilterDirectoryReader doesn't have
        }

        @Override
        public CacheHelper getReaderCacheHelper() {
            return in.getReaderCacheHelper();
        }

    }

    public void close(String reason, boolean flushEngine, Executor closeExecutor, ActionListener<Void> closeListener) throws IOException {
        synchronized (engineMutex) {
            try {
                synchronized (mutex) {
                    changeState(IndexShardState.CLOSED, reason);
                }
                checkAndCallWaitForEngineOrClosedShardListeners();
            } finally {
                final Engine engine = this.currentEngineReference.getAndSet(null);
                closeExecutor.execute(ActionRunnable.run(closeListener, new CheckedRunnable<>() {
                    @Override
                    public void run() throws Exception {
                        try {
                            if (engine != null && flushEngine) {
                                engine.flushAndClose();
                            }
                        } finally {
                            // playing safe here and close the engine even if the above succeeds - close can be called multiple times
                            // Also closing refreshListeners to prevent us from accumulating any more listeners
                            IOUtils.close(
                                engine,
                                globalCheckpointListeners,
                                refreshListeners,
                                pendingReplicationActions,
                                indexShardOperationPermits
                            );
                        }
                    }

                    @Override
                    public String toString() {
                        return "IndexShard#close[" + shardId + "]";
                    }
                }));
            }
        }
    }

    public void preRecovery(ActionListener<Void> listener) {
        final IndexShardState currentState = this.state; // single volatile read
        if (currentState == IndexShardState.CLOSED) {
            throw new IndexShardNotRecoveringException(shardId, currentState);
        }
        assert currentState == IndexShardState.RECOVERING : "expected a recovering shard " + shardId + " but got " + currentState;
        indexEventListener.beforeIndexShardRecovery(this, indexSettings, listener);
    }

    public void postRecovery(String reason, ActionListener<Void> listener) throws IndexShardStartedException, IndexShardRelocatedException,
        IndexShardClosedException {
        assert postRecoveryComplete == null;
        SubscribableListener<Void> subscribableListener = new SubscribableListener<>();
        postRecoveryComplete = subscribableListener;
        final ActionListener<Void> finalListener = ActionListener.runBefore(listener, () -> subscribableListener.onResponse(null));
        try {
            getEngine().refresh("post_recovery");
            // we need to refresh again to expose all operations that were index until now. Otherwise
            // we may not expose operations that were indexed with a refresh listener that was immediately
            // responded to in addRefreshListener. The refresh must happen under the same mutex used in addRefreshListener
            synchronized (mutex) {
                if (state == IndexShardState.CLOSED) {
                    throw new IndexShardClosedException(shardId);
                }
                if (state == IndexShardState.STARTED) {
                    throw new IndexShardStartedException(shardId);
                }
                recoveryState.setStage(RecoveryState.Stage.DONE);
                changeState(IndexShardState.POST_RECOVERY, reason);
            }
            indexEventListener.afterIndexShardRecovery(this, finalListener);
        } catch (Exception e) {
            finalListener.onFailure(e);
        }
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
     * A best-effort attempt to bring up this shard to the global checkpoint using the local translog before performing a peer recovery.
     *
     * @param recoveryStartingSeqNoListener a listener to be completed with the sequence number from which an operation-based peer recovery
     *                                      can start. This is the first operation after the local checkpoint of the safe commit if exists.
     */
    public void recoverLocallyUpToGlobalCheckpoint(ActionListener<Long> recoveryStartingSeqNoListener) {
        assert Thread.holdsLock(mutex) == false : "must not hold the mutex here";
        if (state != IndexShardState.RECOVERING) {
            recoveryStartingSeqNoListener.onFailure(new IndexShardNotRecoveringException(shardId, state));
            return;
        }
        try {
            recoveryState.validateCurrentStage(RecoveryState.Stage.INDEX);
        } catch (Exception e) {
            recoveryStartingSeqNoListener.onFailure(e);
            return;
        }
        assert routingEntry().recoverySource().getType() == RecoverySource.Type.PEER : "not a peer recovery [" + routingEntry() + "]";
        try {
            final var translogUUID = store.readLastCommittedSegmentsInfo().getUserData().get(Translog.TRANSLOG_UUID_KEY);
            final var globalCheckpoint = Translog.readGlobalCheckpoint(translogConfig.getTranslogPath(), translogUUID);
            final var safeCommit = store.findSafeIndexCommit(globalCheckpoint);
            ActionListener.run(recoveryStartingSeqNoListener.delegateResponse((l, e) -> {
                logger.debug(() -> format("failed to recover shard locally up to global checkpoint %s", globalCheckpoint), e);
                l.onResponse(UNASSIGNED_SEQ_NO);
            }), l -> doLocalRecovery(globalCheckpoint, safeCommit, l));
        } catch (org.apache.lucene.index.IndexNotFoundException e) {
            logger.trace("skip local recovery as no index commit found");
            recoveryStartingSeqNoListener.onResponse(UNASSIGNED_SEQ_NO);
        } catch (Exception e) {
            logger.debug("skip local recovery as failed to find the safe commit", e);
            recoveryStartingSeqNoListener.onResponse(UNASSIGNED_SEQ_NO);
        }
    }

    private void doLocalRecovery(
        long globalCheckpoint,
        @SuppressWarnings("OptionalUsedAsFieldOrParameterType") Optional<SequenceNumbers.CommitInfo> safeCommit,
        ActionListener<Long> recoveryStartingSeqNoListener
    ) {
        maybeCheckIndex(); // check index here and won't do it again if ops-based recovery occurs
        recoveryState.setLocalTranslogStage();
        if (safeCommit.isPresent() == false) {
            logger.trace("skip local recovery as no safe commit found");
            recoveryStartingSeqNoListener.onResponse(UNASSIGNED_SEQ_NO);
            return;
        }

        assert safeCommit.get().localCheckpoint() <= globalCheckpoint : safeCommit.get().localCheckpoint() + " > " + globalCheckpoint;
        if (safeCommit.get().localCheckpoint() == globalCheckpoint) {
            logger.trace(
                "skip local recovery as the safe commit is up to date; safe commit {} global checkpoint {}",
                safeCommit.get(),
                globalCheckpoint
            );
            recoveryState.getTranslog().totalLocal(0);
            recoveryStartingSeqNoListener.onResponse(globalCheckpoint + 1);
            return;
        }

        if (indexSettings.getIndexMetadata().getState() == IndexMetadata.State.CLOSE
            || IndexMetadata.INDEX_BLOCKS_WRITE_SETTING.get(indexSettings.getSettings())) {
            logger.trace(
                "skip local recovery as the index was closed or not allowed to write; safe commit {} global checkpoint {}",
                safeCommit.get(),
                globalCheckpoint
            );
            recoveryState.getTranslog().totalLocal(0);
            recoveryStartingSeqNoListener.onResponse(safeCommit.get().localCheckpoint() + 1);
            return;
        }

        SubscribableListener
            // First, start a temporary engine, recover the local translog up to the given checkpoint, and then close the engine again.
            .<Void>newForked(l -> ActionListener.runWithResource(ActionListener.assertOnce(l), () -> () -> {
                assert Thread.holdsLock(mutex) == false : "must not hold the mutex here";
                synchronized (engineMutex) {
                    IOUtils.close(currentEngineReference.getAndSet(null));
                }
            }, (recoveryCompleteListener, ignoredRef) -> {
                assert Thread.holdsLock(mutex) == false : "must not hold the mutex here";
                final Engine.TranslogRecoveryRunner translogRecoveryRunner = (engine, snapshot) -> {
                    recoveryState.getTranslog().totalLocal(snapshot.totalOperations());
                    final int recoveredOps = runTranslogRecovery(
                        engine,
                        snapshot,
                        Engine.Operation.Origin.LOCAL_TRANSLOG_RECOVERY,
                        recoveryState.getTranslog()::incrementRecoveredOperations
                    );
                    recoveryState.getTranslog().totalLocal(recoveredOps); // adjust the total local to reflect the actual count
                    return recoveredOps;
                };
                innerOpenEngineAndTranslog(() -> globalCheckpoint);
                getEngine().recoverFromTranslog(translogRecoveryRunner, globalCheckpoint, recoveryCompleteListener.map(v -> {
                    logger.trace("shard locally recovered up to {}", getEngine().getSeqNoStats(globalCheckpoint));
                    return v;
                }));
            }))
            // If the recovery replayed any operations then it will have created a new safe commit for the specified global checkpoint,
            // which we can use for the rest of the recovery, so now we load the safe commit and use its local checkpoint as the recovery
            // starting point.
            .andThenApply(ignored -> {
                assert Thread.holdsLock(mutex) == false : "must not hold the mutex here";
                try {
                    // we need to find the safe commit again as we should have created a new one during the local recovery
                    final Optional<SequenceNumbers.CommitInfo> newSafeCommit = store.findSafeIndexCommit(globalCheckpoint);
                    assert newSafeCommit.isPresent() : "no safe commit found after local recovery";
                    return newSafeCommit.get().localCheckpoint() + 1;
                } catch (Exception e) {
                    logger.debug(
                        () -> format(
                            "failed to find the safe commit after recovering shard locally up to global checkpoint %s",
                            globalCheckpoint
                        ),
                        e
                    );
                    return UNASSIGNED_SEQ_NO;
                }
            })

            .addListener(recoveryStartingSeqNoListener);
    }

    public void trimOperationOfPreviousPrimaryTerms(long aboveSeqNo) {
        getEngine().trimOperationsFromTranslog(getOperationPrimaryTerm(), aboveSeqNo);
    }

    /**
     * Returns the maximum auto_id_timestamp of all append-only requests have been processed by this shard or the auto_id_timestamp received
     * from the primary via {@link #updateMaxUnsafeAutoIdTimestamp(long)} at the beginning of a peer-recovery or a primary-replica resync.
     *
     * @see #updateMaxUnsafeAutoIdTimestamp(long)
     */
    public long getMaxSeenAutoIdTimestamp() {
        return getEngine().getMaxSeenAutoIdTimestamp();
    }

    /**
     * Since operations stored in soft-deletes do not have max_auto_id_timestamp, the primary has to propagate its max_auto_id_timestamp
     * (via {@link #getMaxSeenAutoIdTimestamp()} of all processed append-only requests to replicas at the beginning of a peer-recovery
     * or a primary-replica resync to force a replica to disable optimization for all append-only requests which are replicated via
     * replication while its retry variants are replicated via recovery without auto_id_timestamp.
     * <p>
     * Without this force-update, a replica can generate duplicate documents (for the same id) if it first receives
     * a retry append-only (without timestamp) via recovery, then an original append-only (with timestamp) via replication.
     */
    public void updateMaxUnsafeAutoIdTimestamp(long maxSeenAutoIdTimestampFromPrimary) {
        getEngine().updateMaxUnsafeAutoIdTimestamp(maxSeenAutoIdTimestampFromPrimary);
    }

    public Engine.Result applyTranslogOperation(Translog.Operation operation, Engine.Operation.Origin origin) throws IOException {
        return applyTranslogOperation(getEngine(), operation, origin);
    }

    private Engine.Result applyTranslogOperation(Engine engine, Translog.Operation operation, Engine.Operation.Origin origin)
        throws IOException {
        // If a translog op is replayed on the primary (eg. ccr), we need to use external instead of null for its version type.
        final VersionType versionType = (origin == Engine.Operation.Origin.PRIMARY) ? VersionType.EXTERNAL : null;
        final Engine.Result result;
        switch (operation.opType()) {
            case INDEX -> {
                final Translog.Index index = (Translog.Index) operation;
                // we set canHaveDuplicates to true all the time such that we de-optimze the translog case and ensure that all
                // autoGeneratedID docs that are coming from the primary are updated correctly.
                result = applyIndexOperation(
                    engine,
                    index.seqNo(),
                    index.primaryTerm(),
                    index.version(),
                    versionType,
                    UNASSIGNED_SEQ_NO,
                    0,
                    index.getAutoGeneratedIdTimestamp(),
                    true,
                    origin,
                    new SourceToParse(index.id(), index.source(), XContentHelper.xContentType(index.source()), index.routing())
                );
            }
            case DELETE -> {
                final Translog.Delete delete = (Translog.Delete) operation;
                result = applyDeleteOperation(
                    engine,
                    delete.seqNo(),
                    delete.primaryTerm(),
                    delete.version(),
                    delete.id(),
                    versionType,
                    UNASSIGNED_SEQ_NO,
                    0,
                    origin
                );
            }
            case NO_OP -> {
                final Translog.NoOp noOp = (Translog.NoOp) operation;
                result = markSeqNoAsNoop(engine, noOp.seqNo(), noOp.primaryTerm(), noOp.reason(), origin);
            }
            default -> throw new IllegalStateException("No operation defined for [" + operation + "]");
        }
        return result;
    }

    /**
     * Replays translog operations from the provided translog {@code snapshot} to the current engine using the given {@code origin}.
     * The callback {@code onOperationRecovered} is notified after each translog operation is replayed successfully.
     */
    int runTranslogRecovery(Engine engine, Translog.Snapshot snapshot, Engine.Operation.Origin origin, Runnable onOperationRecovered)
        throws IOException {
        int opsRecovered = 0;
        Translog.Operation operation;
        while ((operation = snapshot.next()) != null) {
            try {
                logger.trace("[translog] recover op {}", operation);
                Engine.Result result = applyTranslogOperation(engine, operation, origin);
                switch (result.getResultType()) {
                    case FAILURE:
                        throw result.getFailure();
                    case MAPPING_UPDATE_REQUIRED:
                        throw new IllegalArgumentException("unexpected mapping update: " + result.getRequiredMappingUpdate());
                    case SUCCESS:
                        break;
                    default:
                        throw new AssertionError("Unknown result type [" + result.getResultType() + "]");
                }

                opsRecovered++;
                onOperationRecovered.run();
            } catch (Exception e) {
                // TODO: Don't enable this leniency unless users explicitly opt-in
                if (origin == Engine.Operation.Origin.LOCAL_TRANSLOG_RECOVERY && ExceptionsHelper.status(e) == RestStatus.BAD_REQUEST) {
                    // mainly for MapperParsingException and Failure to detect xcontent
                    logger.info("ignoring recovery of a corrupt translog entry", e);
                } else {
                    throw ExceptionsHelper.convertToRuntime(e);
                }
            }
        }
        return opsRecovered;
    }

    private void loadGlobalCheckpointToReplicationTracker() throws IOException {
        if (shardRouting.isPromotableToPrimary()) {
            // we have to set it before we open an engine and recover from the translog because
            // acquiring a snapshot from the translog causes a sync which causes the global checkpoint to be pulled in,
            // and an engine can be forced to close in ctor which also causes the global checkpoint to be pulled in.
            final String translogUUID = store.readLastCommittedSegmentsInfo().getUserData().get(Translog.TRANSLOG_UUID_KEY);
            final long globalCheckpoint = Translog.readGlobalCheckpoint(translogConfig.getTranslogPath(), translogUUID);
            replicationTracker.updateGlobalCheckpointOnReplica(globalCheckpoint, "read from translog checkpoint");
        } else {
            replicationTracker.updateGlobalCheckpointOnReplica(globalCheckPointIfUnpromotable, "from CleanFilesRequest");
        }
    }

    /**
     * opens the engine on top of the existing lucene engine and translog.
     * Operations from the translog will be replayed to bring lucene up to date.
     **/
    public void openEngineAndRecoverFromTranslog(ActionListener<Void> listener) {
        try {
            recoveryState.validateCurrentStage(RecoveryState.Stage.INDEX);
            maybeCheckIndex();
            recoveryState.setLocalTranslogStage();
            final RecoveryState.Translog translogRecoveryStats = recoveryState.getTranslog();
            final Engine.TranslogRecoveryRunner translogRecoveryRunner = (engine, snapshot) -> {
                translogRecoveryStats.totalOperations(snapshot.totalOperations());
                translogRecoveryStats.totalOperationsOnStart(snapshot.totalOperations());
                return runTranslogRecovery(
                    engine,
                    snapshot,
                    Engine.Operation.Origin.LOCAL_TRANSLOG_RECOVERY,
                    translogRecoveryStats::incrementRecoveredOperations
                );
            };
            loadGlobalCheckpointToReplicationTracker();
            innerOpenEngineAndTranslog(replicationTracker);
            getEngine().recoverFromTranslog(translogRecoveryRunner, Long.MAX_VALUE, listener);
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    /**
     * Opens the engine on top of the existing lucene engine and translog.
     * The translog is kept but its operations won't be replayed.
     */
    public void openEngineAndSkipTranslogRecovery() throws IOException {
        assert routingEntry().recoverySource().getType() == RecoverySource.Type.PEER : "not a peer recovery [" + routingEntry() + "]";
        recoveryState.validateCurrentStage(RecoveryState.Stage.TRANSLOG);
        loadGlobalCheckpointToReplicationTracker();
        innerOpenEngineAndTranslog(replicationTracker);
        getEngine().skipTranslogRecovery();
    }

    private void innerOpenEngineAndTranslog(LongSupplier globalCheckpointSupplier) throws IOException {
        assert Thread.holdsLock(mutex) == false : "opening engine under mutex";
        if (state != IndexShardState.RECOVERING) {
            throw new IndexShardNotRecoveringException(shardId, state);
        }
        final EngineConfig config = newEngineConfig(globalCheckpointSupplier);

        // we disable deletes since we allow for operations to be executed against the shard while recovering
        // but we need to make sure we don't loose deletes until we are done recovering
        config.setEnableGcDeletes(false);
        updateRetentionLeasesOnReplica(loadRetentionLeases());
        assert recoveryState.getRecoverySource().expectEmptyRetentionLeases() == false || getRetentionLeases().leases().isEmpty()
            : "expected empty set of retention leases with recovery source ["
                + recoveryState.getRecoverySource()
                + "] but got "
                + getRetentionLeases();
        synchronized (engineMutex) {
            assert currentEngineReference.get() == null : "engine is running";
            verifyNotClosed();
            // we must create a new engine under mutex (see IndexShard#snapshotStoreMetadata).
            final Engine newEngine = createEngine(config);
            onNewEngine(newEngine);
            currentEngineReference.set(newEngine);
            // We set active because we are now writing operations to the engine; this way,
            // we can flush if we go idle after some time and become inactive.
            active.set(true);
        }
        // time elapses after the engine is created above (pulling the config settings) until we set the engine reference, during
        // which settings changes could possibly have happened, so here we forcefully push any config changes to the new engine.
        onSettingsChanged();
        assert assertSequenceNumbersInCommit();
        recoveryState.validateCurrentStage(RecoveryState.Stage.TRANSLOG);
        checkAndCallWaitForEngineOrClosedShardListeners();
    }

    // awful hack to work around problem in CloseFollowerIndexIT
    static boolean suppressCreateEngineErrors;

    private Engine createEngine(EngineConfig config) {
        if (suppressCreateEngineErrors) {
            try {
                return engineFactory.newReadWriteEngine(config);
            } catch (Error e) {
                ExceptionsHelper.maybeDieOnAnotherThread(e);
                throw new RuntimeException("rethrowing suppressed error", e);
            }
        } else {
            return engineFactory.newReadWriteEngine(config);
        }
    }

    private boolean assertSequenceNumbersInCommit() throws IOException {
        final SegmentInfos segmentCommitInfos = SegmentInfos.readLatestCommit(store.directory());
        final Map<String, String> userData = segmentCommitInfos.getUserData();
        assert userData.containsKey(SequenceNumbers.LOCAL_CHECKPOINT_KEY) : "commit point doesn't contains a local checkpoint";
        assert userData.containsKey(SequenceNumbers.MAX_SEQ_NO) : "commit point doesn't contains a maximum sequence number";
        assert userData.containsKey(Engine.HISTORY_UUID_KEY) : "commit point doesn't contains a history uuid";
        assert userData.get(Engine.HISTORY_UUID_KEY).equals(getHistoryUUID())
            : "commit point history uuid ["
                + userData.get(Engine.HISTORY_UUID_KEY)
                + "] is different than engine ["
                + getHistoryUUID()
                + "]";
        assert userData.containsKey(Engine.MAX_UNSAFE_AUTO_ID_TIMESTAMP_COMMIT_ID)
            : "opening index which was created post 5.5.0 but " + Engine.MAX_UNSAFE_AUTO_ID_TIMESTAMP_COMMIT_ID + " is not found in commit";
        final org.apache.lucene.util.Version commitLuceneVersion = segmentCommitInfos.getCommitLuceneVersion();
        // This relies in the previous minor having another lucene version
        assert commitLuceneVersion.onOrAfter(RecoverySettings.SEQ_NO_SNAPSHOT_RECOVERIES_SUPPORTED_VERSION.luceneVersion()) == false
            || userData.containsKey(Engine.ES_VERSION)
                && Engine.readIndexVersion(userData.get(Engine.ES_VERSION)).onOrBefore(IndexVersion.current())
            : "commit point has an invalid ES_VERSION value. commit point lucene version ["
                + commitLuceneVersion
                + "],"
                + " ES_VERSION ["
                + userData.get(Engine.ES_VERSION)
                + "]";
        return true;
    }

    private void onNewEngine(Engine newEngine) {
        assert Thread.holdsLock(engineMutex);
        refreshListeners.setCurrentRefreshLocationSupplier(newEngine::getTranslogLastWriteLocation);
        refreshListeners.setCurrentProcessedCheckpointSupplier(newEngine::getProcessedLocalCheckpoint);
        refreshListeners.setMaxIssuedSeqNoSupplier(newEngine::getMaxSeqNo);
    }

    /**
     * called if recovery has to be restarted after network error / delay **
     */
    public void performRecoveryRestart() throws IOException {
        assert Thread.holdsLock(mutex) == false : "restart recovery under mutex";
        synchronized (engineMutex) {
            assert refreshListeners.pendingCount() == 0 : "we can't restart with pending listeners";
            IOUtils.close(currentEngineReference.getAndSet(null));
            resetRecoveryStage();
        }
    }

    /**
     * If a file-based recovery occurs, a recovery target calls this method to reset the recovery stage.
     */
    public void resetRecoveryStage() {
        assert routingEntry().recoverySource().getType() == RecoverySource.Type.PEER : "not a peer recovery [" + routingEntry() + "]";
        assert currentEngineReference.get() == null;
        if (state != IndexShardState.RECOVERING) {
            throw new IndexShardNotRecoveringException(shardId, state);
        }
        synchronized (mutex) {
            this.recoveryState = recoveryState().reset();
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

    @Override
    public ShardLongFieldRange getTimestampRange() {
        return determineShardLongFieldRange(DataStream.TIMESTAMP_FIELD_NAME);
    }

    @Override
    public ShardLongFieldRange getEventIngestedRange() {
        return determineShardLongFieldRange(IndexMetadata.EVENT_INGESTED_FIELD_NAME);
    }

    private ShardLongFieldRange determineShardLongFieldRange(String fieldName) {
        if (mapperService() == null) {
            return ShardLongFieldRange.UNKNOWN; // no mapper service, no idea if the field even exists
        }
        final MappedFieldType mappedFieldType = mapperService().fieldType(fieldName);
        if (mappedFieldType instanceof DateFieldMapper.DateFieldType == false) {
            return ShardLongFieldRange.UNKNOWN; // field missing or not a date
        }
        if (mappedFieldType.isIndexed() == false) {
            return ShardLongFieldRange.UNKNOWN; // range information missing
        }

        final ShardLongFieldRange rawTimestampFieldRange;
        try {
            rawTimestampFieldRange = getEngine().getRawFieldRange(fieldName);
            assert rawTimestampFieldRange != null;
        } catch (IOException | AlreadyClosedException e) {
            logger.debug("exception obtaining range for field " + fieldName, e);
            return ShardLongFieldRange.UNKNOWN;
        }
        if (rawTimestampFieldRange == ShardLongFieldRange.UNKNOWN) {
            return ShardLongFieldRange.UNKNOWN;
        }
        if (rawTimestampFieldRange == ShardLongFieldRange.EMPTY) {
            return ShardLongFieldRange.EMPTY;
        }

        return ShardLongFieldRange.of(rawTimestampFieldRange.getMin(), rawTimestampFieldRange.getMax());
    }

    /**
     * perform the last stages of recovery once all translog operations are done.
     * note that you should still call {@link #postRecovery(String, ActionListener)}.
     */
    public void finalizeRecovery() {
        recoveryState().setStage(RecoveryState.Stage.FINALIZE);
        Engine engine = getEngine();
        engine.refresh("recovery_finalization");
        engine.config().setEnableGcDeletes(true);
    }

    /**
     * Returns {@code true} if this shard can ignore a recovery attempt made to it (since the already doing/done it)
     */
    public boolean ignoreRecoveryAttempt() {
        IndexShardState state = state(); // one time volatile read
        return state == IndexShardState.POST_RECOVERY
            || state == IndexShardState.RECOVERING
            || state == IndexShardState.STARTED
            || state == IndexShardState.CLOSED;
    }

    public void readAllowed() throws IllegalIndexShardStateException {
        IndexShardState state = this.state; // one time volatile read
        if (readAllowedStates.contains(state) == false) {
            throw new IllegalIndexShardStateException(
                shardId,
                state,
                "operations only allowed when shard state is one of " + readAllowedStates.toString()
            );
        }
    }

    /** returns true if the {@link IndexShardState} allows reading */
    public boolean isReadAllowed() {
        return readAllowedStates.contains(state);
    }

    private void ensureWriteAllowed(Engine.Operation.Origin origin) throws IllegalIndexShardStateException {
        IndexShardState state = this.state; // one time volatile read

        if (origin.isRecovery()) {
            if (state != IndexShardState.RECOVERING) {
                throw new IllegalIndexShardStateException(
                    shardId,
                    state,
                    "operation only allowed when recovering, origin [" + origin + "]"
                );
            }
        } else {
            assert assertWriteOriginInvariants(origin);
            if (writeAllowedStates.contains(state) == false) {
                throw new IllegalIndexShardStateException(
                    shardId,
                    state,
                    "operation only allowed when shard state is one of " + writeAllowedStates + ", origin [" + origin + "]"
                );
            }
        }
    }

    private boolean assertWriteOriginInvariants(Engine.Operation.Origin origin) {
        switch (origin) {
            case PRIMARY -> {
                assertPrimaryMode();
                assertExpectedStateForPrimaryIndexing(state);
            }
            case REPLICA -> {
                assert assertReplicationTarget();
            }
            case LOCAL_RESET -> {
                final var activeOperationsCount = getActiveOperationsCount();
                assert activeOperationsCount == OPERATIONS_BLOCKED
                    : "locally resetting without blocking operations, active operations [" + activeOperationsCount + "]";
            }
            default -> {
                assert false : "unexpected origin: " + origin;
            }
        }
        return true;
    }

    private void assertExpectedStateForPrimaryIndexing(IndexShardState state) {
        // We do not do indexing into primaries that have not reached state STARTED since:
        // * TransportReplicationAction.ReroutePhase only allows to index into active primaries.
        // * A relocation will retry the reroute phase.
        // * Allocation ids protect against spurious requests towards old allocations.
        // * We apply the cluster state on IndexShard instances before making it available for routing
        assert state == IndexShardState.STARTED || state == IndexShardState.CLOSED : "primary indexing unexpected in state [" + state + "]";
    }

    private boolean assertPrimaryMode() {
        assert shardRouting.primary() && replicationTracker.isPrimaryMode()
            : "shard " + shardRouting + " is not a primary shard in primary mode";
        return true;
    }

    private boolean assertReplicationTarget() {
        assert replicationTracker.isPrimaryMode() == false : "shard " + shardRouting + " in primary mode cannot be a replication target";
        return true;
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
     * indexing operation, so we can flush the index.
     */
    public void flushOnIdle(long inactiveTimeNS) {
        Engine engineOrNull = getEngineOrNull();
        if (engineOrNull != null && System.nanoTime() - engineOrNull.getLastWriteNanos() >= inactiveTimeNS) {
            boolean wasActive = active.getAndSet(false);
            if (wasActive) {
                logger.debug("flushing shard on inactive");
                threadPool.executor(ThreadPool.Names.FLUSH)
                    .execute(() -> flush(new FlushRequest().waitIfOngoing(false).force(false), new ActionListener<>() {
                        @Override
                        public void onResponse(Boolean flushed) {
                            if (flushed == false) {
                                // In case an ongoing flush was detected, revert active flag so that a next flushOnIdle request
                                // will retry (#87888)
                                active.set(true);
                            }
                            periodicFlushMetric.inc();
                        }

                        @Override
                        public void onFailure(Exception e) {
                            if (state != IndexShardState.CLOSED) {
                                active.set(true);
                                logger.warn("failed to flush shard on inactive", e);
                            }
                        }
                    }));
            }
        }
    }

    public boolean isActive() {
        return active.get();
    }

    public ShardPath shardPath() {
        return path;
    }

    void recoverFromLocalShards(
        BiConsumer<MappingMetadata, ActionListener<Void>> mappingUpdateConsumer,
        List<IndexShard> localShards,
        ActionListener<Boolean> listener
    ) throws IOException {
        assert shardRouting.primary() : "recover from local shards only makes sense if the shard is a primary shard";
        assert recoveryState.getRecoverySource().getType() == RecoverySource.Type.LOCAL_SHARDS
            : "invalid recovery type: " + recoveryState.getRecoverySource();
        final List<LocalShardSnapshot> snapshots = new ArrayList<>();
        final ActionListener<Boolean> recoveryListener = ActionListener.runBefore(listener, () -> IOUtils.close(snapshots));
        boolean success = false;
        try {
            for (IndexShard shard : localShards) {
                snapshots.add(new LocalShardSnapshot(shard));
            }
            // we are the first primary, recover from the gateway
            // if its post api allocation, the index should exists
            assert shardRouting.primary() : "recover from local shards only makes sense if the shard is a primary shard";
            StoreRecovery storeRecovery = new StoreRecovery(shardId, logger);
            storeRecovery.recoverFromLocalShards(mappingUpdateConsumer, this, snapshots, recoveryListener);
            success = true;
        } finally {
            if (success == false) {
                IOUtils.close(snapshots);
            }
        }
    }

    public void recoverFromStore(ActionListener<Boolean> listener) {
        // we are the first primary, recover from the gateway
        // if its post api allocation, the index should exists
        assert shardRouting.primary() : "recover from store only makes sense if the shard is a primary shard";
        assert shardRouting.initializing() : "can only start recovery on initializing shard";
        StoreRecovery storeRecovery = new StoreRecovery(shardId, logger);
        storeRecovery.recoverFromStore(this, listener);
    }

    public void restoreFromRepository(Repository repository, ActionListener<Boolean> listener) {
        try {
            assert shardRouting.primary() : "recover from store only makes sense if the shard is a primary shard";
            assert recoveryState.getRecoverySource().getType() == RecoverySource.Type.SNAPSHOT
                : "invalid recovery type: " + recoveryState.getRecoverySource();
            StoreRecovery storeRecovery = new StoreRecovery(shardId, logger);
            storeRecovery.recoverFromRepository(this, repository, listener);
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    /**
     * Tests whether or not the engine should be flushed periodically.
     * This test is based on the current size of the translog compared to the configured flush threshold size.
     *
     * @return {@code true} if the engine should be flushed
     */
    boolean shouldPeriodicallyFlush() {
        final Engine engine = getEngineOrNull();
        if (engine != null) {
            try {
                return engine.shouldPeriodicallyFlush();
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
                return engine.shouldRollTranslogGeneration();
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

    /**
     * Acquires a lock on the translog files and Lucene soft-deleted documents to prevent them from being trimmed
     */
    public Closeable acquireHistoryRetentionLock() {
        return getEngine().acquireHistoryRetentionLock();
    }

    /**
     * Checks if we have a completed history of operations since the given starting seqno (inclusive).
     * This method should be called after acquiring the retention lock; See {@link #acquireHistoryRetentionLock()}
     */
    public boolean hasCompleteHistoryOperations(String reason, long startingSeqNo) {
        return getEngine().hasCompleteOperationHistory(reason, startingSeqNo);
    }

    /**
     * Gets the minimum retained sequence number for this engine.
     *
     * @return the minimum retained sequence number
     */
    public long getMinRetainedSeqNo() {
        return getEngine().getMinRetainedSeqNo();
    }

    /**
     * Counts the number of operations in the range of the given sequence numbers.
     *
     * @param source    the source of the request
     * @param fromSeqNo the start sequence number (inclusive)
     * @param toSeqNo   the end sequence number (inclusive)
     * @see #newChangesSnapshot(String, long, long, boolean, boolean, boolean)
     */
    public int countChanges(String source, long fromSeqNo, long toSeqNo) throws IOException {
        return getEngine().countChanges(source, fromSeqNo, toSeqNo);
    }

    /**
     * Creates a new changes snapshot for reading operations whose seq_no are between {@code fromSeqNo}(inclusive)
     * and {@code toSeqNo}(inclusive). The caller has to close the returned snapshot after finishing the reading.
     *
     * @param source            the source of the request
     * @param fromSeqNo         the from seq_no (inclusive) to read
     * @param toSeqNo           the to seq_no (inclusive) to read
     * @param requiredFullRange if {@code true} then {@link Translog.Snapshot#next()} will throw {@link IllegalStateException}
     *                          if any operation between {@code fromSeqNo} and {@code toSeqNo} is missing.
     *                          This parameter should be only enabled when the entire requesting range is below the global checkpoint.
     * @param singleConsumer    true if the snapshot is accessed by only the thread that creates the snapshot. In this case, the
     *                          snapshot can enable some optimizations to improve the performance.
     * @param accessStats       true if the stats of the snapshot is accessed via {@link Translog.Snapshot#totalOperations()}
     */
    public Translog.Snapshot newChangesSnapshot(
        String source,
        long fromSeqNo,
        long toSeqNo,
        boolean requiredFullRange,
        boolean singleConsumer,
        boolean accessStats
    ) throws IOException {
        return getEngine().newChangesSnapshot(source, fromSeqNo, toSeqNo, requiredFullRange, singleConsumer, accessStats);
    }

    public List<Segment> segments() {
        return getEngine().segments();
    }

    public List<Segment> segments(boolean includeVectorFormatsInfo) {
        return getEngine().segments(includeVectorFormatsInfo);
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
        } else if (e instanceof RefreshFailedEngineException rfee) {
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
     * {@link ReplicationTracker#updateLocalCheckpoint(String, long)} for
     * details.
     *
     * @param allocationId the allocation ID of the shard to update the local checkpoint for
     * @param checkpoint   the local checkpoint for the shard
     */
    public void updateLocalCheckpointForShard(final String allocationId, final long checkpoint) {
        assert assertPrimaryMode();
        verifyNotClosed();
        replicationTracker.updateLocalCheckpoint(allocationId, checkpoint);
    }

    /**
     * Update the local knowledge of the persisted global checkpoint for the specified allocation ID.
     *
     * @param allocationId     the allocation ID to update the global checkpoint for
     * @param globalCheckpoint the global checkpoint
     */
    public void updateGlobalCheckpointForShard(final String allocationId, final long globalCheckpoint) {
        assert assertPrimaryMode();
        verifyNotClosed();
        replicationTracker.updateGlobalCheckpointForShard(allocationId, globalCheckpoint);
    }

    /**
     * Add a global checkpoint listener. If the global checkpoint is equal to or above the global checkpoint the listener is waiting for,
     * then the listener will be notified immediately via an executor (so possibly not on the current thread). If the specified timeout
     * elapses before the listener is notified, the listener will be notified with an {@link TimeoutException}. A caller may pass null to
     * specify no timeout.
     *
     * @param waitingForGlobalCheckpoint the global checkpoint the listener is waiting for
     * @param listener                   the listener
     * @param timeout                    the timeout
     */
    public void addGlobalCheckpointListener(
        final long waitingForGlobalCheckpoint,
        final GlobalCheckpointListeners.GlobalCheckpointListener listener,
        final TimeValue timeout
    ) {
        this.globalCheckpointListeners.add(waitingForGlobalCheckpoint, listener, timeout);
    }

    private void ensureSoftDeletesEnabled() {
        if (indexSettings.isSoftDeleteEnabled() == false) {
            String message = "retention leases requires soft deletes but "
                + indexSettings.getIndex()
                + " does not have soft deletes enabled";
            assert false : message;
            throw new IllegalStateException(message);
        }
    }

    /**
     * Get all retention leases tracked on this shard.
     *
     * @return the retention leases
     */
    public RetentionLeases getRetentionLeases() {
        return getRetentionLeases(false);
    }

    /**
     * If the expire leases parameter is false, gets all retention leases tracked on this shard and otherwise first calculates
     * expiration of existing retention leases, and then gets all non-expired retention leases tracked on this shard. Note that only the
     * primary shard calculates which leases are expired, and if any have expired, syncs the retention leases to any replicas. If the
     * expire leases parameter is true, this replication tracker must be in primary mode.
     *
     * @return the non-expired retention leases
     */
    public RetentionLeases getRetentionLeases(final boolean expireLeases) {
        assert expireLeases == false || assertPrimaryMode();
        verifyNotClosed();
        return replicationTracker.getRetentionLeases(expireLeases);
    }

    public RetentionLeaseStats getRetentionLeaseStats() {
        verifyNotClosed();
        return new RetentionLeaseStats(getRetentionLeases());
    }

    /**
     * Adds a new retention lease.
     *
     * @param id                      the identifier of the retention lease
     * @param retainingSequenceNumber the retaining sequence number
     * @param source                  the source of the retention lease
     * @param listener                the callback when the retention lease is successfully added and synced to replicas
     * @return the new retention lease
     * @throws IllegalArgumentException if the specified retention lease already exists
     */
    public RetentionLease addRetentionLease(
        final String id,
        final long retainingSequenceNumber,
        final String source,
        final ActionListener<ReplicationResponse> listener
    ) {
        Objects.requireNonNull(listener);
        assert assertPrimaryMode();
        verifyNotClosed();
        ensureSoftDeletesEnabled();
        try (Closeable ignore = acquireHistoryRetentionLock()) {
            final long actualRetainingSequenceNumber = retainingSequenceNumber == RETAIN_ALL
                ? getMinRetainedSeqNo()
                : retainingSequenceNumber;
            return replicationTracker.addRetentionLease(id, actualRetainingSequenceNumber, source, listener);
        } catch (final IOException e) {
            throw new AssertionError(e);
        }
    }

    /**
     * Renews an existing retention lease.
     *
     * @param id                      the identifier of the retention lease
     * @param retainingSequenceNumber the retaining sequence number
     * @param source                  the source of the retention lease
     * @return the renewed retention lease
     * @throws IllegalArgumentException if the specified retention lease does not exist
     */
    public RetentionLease renewRetentionLease(final String id, final long retainingSequenceNumber, final String source) {
        assert assertPrimaryMode();
        verifyNotClosed();
        ensureSoftDeletesEnabled();
        try (Closeable ignore = acquireHistoryRetentionLock()) {
            final long actualRetainingSequenceNumber = retainingSequenceNumber == RETAIN_ALL
                ? getMinRetainedSeqNo()
                : retainingSequenceNumber;
            return replicationTracker.renewRetentionLease(id, actualRetainingSequenceNumber, source);
        } catch (final IOException e) {
            throw new AssertionError(e);
        }
    }

    /**
     * Removes an existing retention lease.
     *
     * @param id       the identifier of the retention lease
     * @param listener the callback when the retention lease is successfully removed and synced to replicas
     */
    public void removeRetentionLease(final String id, final ActionListener<ReplicationResponse> listener) {
        Objects.requireNonNull(listener);
        assert assertPrimaryMode();
        verifyNotClosed();
        ensureSoftDeletesEnabled();
        replicationTracker.removeRetentionLease(id, listener);
    }

    /**
     * Updates retention leases on a replica.
     *
     * @param retentionLeases the retention leases
     */
    public void updateRetentionLeasesOnReplica(final RetentionLeases retentionLeases) {
        assert assertReplicationTarget();
        verifyNotClosed();
        replicationTracker.updateRetentionLeasesOnReplica(retentionLeases);
    }

    /**
     * Loads the latest retention leases from their dedicated state file.
     *
     * @return the retention leases
     * @throws IOException if an I/O exception occurs reading the retention leases
     */
    public RetentionLeases loadRetentionLeases() throws IOException {
        verifyNotClosed();
        return replicationTracker.loadRetentionLeases(path.getShardStatePath());
    }

    /**
     * Persists the current retention leases to their dedicated state file.
     *
     * @throws WriteStateException if an exception occurs writing the state file
     */
    public void persistRetentionLeases() throws WriteStateException {
        verifyNotClosed();
        replicationTracker.persistRetentionLeases(path.getShardStatePath());
    }

    public boolean assertRetentionLeasesPersisted() throws IOException {
        return replicationTracker.assertRetentionLeasesPersisted(path.getShardStatePath());
    }

    /**
     * Syncs the current retention leases to all replicas.
     */
    public void syncRetentionLeases() {
        assert assertPrimaryMode();
        verifyNotClosed();
        replicationTracker.renewPeerRecoveryRetentionLeases();
        final RetentionLeases retentionLeases = getRetentionLeases(true);
        logger.trace("background syncing retention leases [{}] after expiration check", retentionLeases);
        retentionLeaseSyncer.backgroundSync(shardId, shardRouting.allocationId().getId(), getPendingPrimaryTerm(), retentionLeases);
    }

    /**
     * Called when the recovery process for a shard has opened the engine on the target shard. Ensures that the right data structures
     * have been set up locally to track local checkpoint information for the shard and that the shard is added to the replication group.
     *
     * @param allocationId  the allocation ID of the shard for which recovery was initiated
     */
    public void initiateTracking(final String allocationId) {
        assert assertPrimaryMode();
        replicationTracker.initiateTracking(allocationId);
    }

    /**
     * Marks the shard with the provided allocation ID as in-sync with the primary shard. See
     * {@link ReplicationTracker#markAllocationIdAsInSync(String, long)}
     * for additional details.
     *
     * @param allocationId    the allocation ID of the shard to mark as in-sync
     * @param localCheckpoint the current local checkpoint on the shard
     */
    public void markAllocationIdAsInSync(final String allocationId, final long localCheckpoint) throws InterruptedException {
        assert assertPrimaryMode();
        replicationTracker.markAllocationIdAsInSync(allocationId, localCheckpoint);
    }

    /**
     * Returns the persisted local checkpoint for the shard.
     *
     * @return the local checkpoint
     */
    public long getLocalCheckpoint() {
        return getEngine().getPersistedLocalCheckpoint();
    }

    /**
     * Returns the global checkpoint for the shard.
     *
     * @return the global checkpoint
     */
    public long getLastKnownGlobalCheckpoint() {
        return replicationTracker.getGlobalCheckpoint();
    }

    /**
     * Returns the latest global checkpoint value that has been persisted in the underlying storage (i.e. translog's checkpoint)
     */
    public long getLastSyncedGlobalCheckpoint() {
        return getEngine().getLastSyncedGlobalCheckpoint();
    }

    /**
     * Get the local knowledge of the global checkpoints for all in-sync allocation IDs.
     *
     * @return a map from allocation ID to the local knowledge of the global checkpoint for that allocation ID
     */
    public Map<String, Long> getInSyncGlobalCheckpoints() {
        assert assertPrimaryMode();
        verifyNotClosed();
        return replicationTracker.getInSyncGlobalCheckpoints();
    }

    /**
     * Syncs the global checkpoint to the replicas if the global checkpoint on at least one replica is behind the global checkpoint on the
     * primary.
     */
    public void maybeSyncGlobalCheckpoint(final String reason) {
        verifyNotClosed();
        assert shardRouting.primary() : "only call maybeSyncGlobalCheckpoint on primary shard";
        if (replicationTracker.isPrimaryMode() == false) {
            return;
        }
        assert assertPrimaryMode();
        // only sync if there are no operations in flight, or when using async durability
        final SeqNoStats stats = getEngine().getSeqNoStats(replicationTracker.getGlobalCheckpoint());
        final boolean asyncDurability = indexSettings().getTranslogDurability() == Translog.Durability.ASYNC;
        if (stats.getMaxSeqNo() == stats.getGlobalCheckpoint() || asyncDurability) {
            final var trackedGlobalCheckpointsNeedSync = replicationTracker.trackedGlobalCheckpointsNeedSync();
            // async durability means that the local checkpoint might lag (as it is only advanced on fsync)
            // periodically ask for the newest local checkpoint by syncing the global checkpoint, so that ultimately the global
            // checkpoint can be synced. Also take into account that a shard might be pending sync, which means that it isn't
            // in the in-sync set just yet but might be blocked on waiting for its persisted local checkpoint to catch up to
            // the global checkpoint.
            final boolean syncNeeded = (asyncDurability
                && (stats.getGlobalCheckpoint() < stats.getMaxSeqNo() || replicationTracker.pendingInSync()))
                || trackedGlobalCheckpointsNeedSync;
            // only sync if index is not closed and there is a shard lagging the primary
            if (syncNeeded && indexSettings.getIndexMetadata().getState() == IndexMetadata.State.OPEN) {
                syncGlobalCheckpoints(reason);
            }
        }
    }

    private void syncGlobalCheckpoints(String reason) {
        logger.trace("syncing global checkpoint for [{}]", reason);
        globalCheckpointSyncer.syncGlobalCheckpoints(shardId);
    }

    // exposed for tests
    GlobalCheckpointSyncer getGlobalCheckpointSyncer() {
        return globalCheckpointSyncer;
    }

    /**
     * Returns the current replication group for the shard.
     *
     * @return the replication group
     */
    public ReplicationGroup getReplicationGroup() {
        assert assertPrimaryMode();
        verifyNotClosed();
        ReplicationGroup replicationGroup = replicationTracker.getReplicationGroup();
        // PendingReplicationActions is dependent on ReplicationGroup. Every time we expose ReplicationGroup,
        // ensure PendingReplicationActions is updated with the newest version to prevent races.
        pendingReplicationActions.accept(replicationGroup);
        return replicationGroup;
    }

    /**
     * Returns the pending replication actions for the shard.
     *
     * @return the pending replication actions
     */
    public PendingReplicationActions getPendingReplicationActions() {
        assert assertPrimaryMode();
        verifyNotClosed();
        return pendingReplicationActions;
    }

    /**
     * Updates the global checkpoint on a replica shard after it has been updated by the primary.
     *
     * @param globalCheckpoint the global checkpoint
     * @param reason           the reason the global checkpoint was updated
     */
    public void updateGlobalCheckpointOnReplica(final long globalCheckpoint, final String reason) {
        assert assertReplicationTarget();
        final long localCheckpoint = getLocalCheckpoint();
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
            assert state() != IndexShardState.POST_RECOVERY && state() != IndexShardState.STARTED
                : "supposedly in-sync shard copy received a global checkpoint ["
                    + globalCheckpoint
                    + "] "
                    + "that is higher than its local checkpoint ["
                    + localCheckpoint
                    + "]";
            return;
        }
        replicationTracker.updateGlobalCheckpointOnReplica(globalCheckpoint, reason);
    }

    private final AtomicInteger outstandingCleanFilesConditions = new AtomicInteger(0);

    private final Deque<Runnable> afterCleanFilesActions = new LinkedList<>();

    /**
     * Creates a {@link Runnable} that must be executed before the clean files step in peer recovery can complete.
     *
     * @return runnable that must be executed during the clean files step in peer recovery
     */
    public Runnable addCleanFilesDependency() {
        logger.trace("adding clean files dependency for [{}]", shardRouting);
        outstandingCleanFilesConditions.incrementAndGet();
        return () -> {
            if (outstandingCleanFilesConditions.decrementAndGet() == 0) {
                runAfterCleanFilesActions();
            }
        };
    }

    /**
     * Execute a {@link Runnable} on the generic pool once all dependencies added via {@link #addCleanFilesDependency()} have finished.
     * If there are no dependencies to wait for then the {@code Runnable} will be executed on the calling thread.
     */
    public void afterCleanFiles(Runnable runnable) {
        if (outstandingCleanFilesConditions.get() == 0) {
            runnable.run();
        } else {
            synchronized (afterCleanFilesActions) {
                afterCleanFilesActions.add(runnable);
            }
            if (outstandingCleanFilesConditions.get() == 0) {
                runAfterCleanFilesActions();
            }
        }
    }

    // for tests
    public int outstandingCleanFilesConditions() {
        return outstandingCleanFilesConditions.get();
    }

    private void runAfterCleanFilesActions() {
        synchronized (afterCleanFilesActions) {
            final Executor executor = threadPool.generic();
            Runnable afterCleanFilesAction;
            while ((afterCleanFilesAction = afterCleanFilesActions.poll()) != null) {
                executor.execute(afterCleanFilesAction);
            }
        }
    }

    /**
     * Updates the known allocation IDs and the local checkpoints for the corresponding allocations from a primary relocation source.
     *
     * @param primaryContext the sequence number context
     */
    public void activateWithPrimaryContext(final ReplicationTracker.PrimaryContext primaryContext) {
        assert shardRouting.primary() && shardRouting.isRelocationTarget()
            : "only primary relocation target can update allocation IDs from primary context: " + shardRouting;
        assert primaryContext.getCheckpointStates().containsKey(routingEntry().allocationId().getId())
            : "primary context [" + primaryContext + "] does not contain relocation target [" + routingEntry() + "]";
        assert getLocalCheckpoint() == primaryContext.getCheckpointStates().get(routingEntry().allocationId().getId()).getLocalCheckpoint()
            || indexSettings().getTranslogDurability() == Translog.Durability.ASYNC
            : "local checkpoint [" + getLocalCheckpoint() + "] does not match checkpoint from primary context [" + primaryContext + "]";
        synchronized (mutex) {
            replicationTracker.activateWithPrimaryContext(primaryContext); // make changes to primaryMode flag only under mutex
        }
        ensurePeerRecoveryRetentionLeasesExist();
    }

    private void ensurePeerRecoveryRetentionLeasesExist() {
        threadPool.generic()
            .execute(
                () -> replicationTracker.createMissingPeerRecoveryRetentionLeases(
                    ActionListener.wrap(
                        r -> logger.trace("created missing peer recovery retention leases"),
                        e -> logger.debug("failed creating missing peer recovery retention leases", e)
                    )
                )
            );
    }

    /**
     * Check if there are any recoveries pending in-sync.
     *
     * @return {@code true} if there is at least one shard pending in-sync, otherwise false
     */
    public boolean pendingInSync() {
        assert assertPrimaryMode();
        return replicationTracker.pendingInSync();
    }

    /**
     * Should be called for each no-op update operation to increment relevant statistics.
     */
    public void noopUpdate() {
        internalIndexingStats.noopUpdate();
    }

    public void maybeCheckIndex() {
        recoveryState.setStage(RecoveryState.Stage.VERIFY_INDEX);
        if (Booleans.isTrue(checkIndexOnStartup) || "checksum".equals(checkIndexOnStartup)) {
            logger.warn(
                "performing expensive diagnostic checks during shard startup [{}={}]; "
                    + "these checks should only be enabled temporarily, you must remove this index setting as soon as possible",
                IndexSettings.INDEX_CHECK_ON_STARTUP.getKey(),
                checkIndexOnStartup
            );
            try {
                checkIndex();
            } catch (IOException ex) {
                throw new RecoveryFailedException(recoveryState, "check index failed", ex);
            }
        }
    }

    void checkIndex() throws IOException {
        if (store.tryIncRef()) {
            try {
                doCheckIndex();
            } catch (IOException e) {
                store.markStoreCorrupted(e);
                throw e;
            } finally {
                store.decRef();
            }
        }
    }

    private void doCheckIndex() throws IOException {
        long timeNS = System.nanoTime();
        if (Lucene.indexExists(store.directory()) == false) {
            return;
        }

        if ("checksum".equals(checkIndexOnStartup)) {
            // physical verification only: verify all checksums for the latest commit
            IOException corrupt = null;
            final MetadataSnapshot metadata;
            try {
                metadata = snapshotStoreMetadata();
            } catch (IOException e) {
                logger.warn("check index [failure]", e);
                throw e;
            }
            final List<String> checkedFiles = new ArrayList<>(metadata.size());
            for (Map.Entry<String, StoreFileMetadata> entry : metadata.fileMetadataMap().entrySet()) {
                try {
                    Store.checkIntegrity(entry.getValue(), store.directory());
                    if (corrupt == null) {
                        checkedFiles.add(entry.getKey());
                    } else {
                        logger.info("check index [ok]: checksum check passed on [{}]", entry.getKey());
                    }
                } catch (IOException ioException) {
                    for (final String checkedFile : checkedFiles) {
                        logger.info("check index [ok]: checksum check passed on [{}]", checkedFile);
                    }
                    checkedFiles.clear();
                    logger.warn(() -> "check index [failure]: checksum failed on [" + entry.getKey() + "]", ioException);
                    corrupt = ioException;
                }
            }
            if (corrupt != null) {
                throw corrupt;
            }
            if (logger.isDebugEnabled()) {
                for (final String checkedFile : checkedFiles) {
                    logger.debug("check index [ok]: checksum check passed on [{}]", checkedFile);
                }
            }
        } else {
            // full checkindex
            final BytesStreamOutput os = new BytesStreamOutput();
            final PrintStream out = new PrintStream(os, false, StandardCharsets.UTF_8.name());
            final CheckIndex.Status status = store.checkIndex(out);
            out.flush();
            if (status.clean == false) {
                if (state == IndexShardState.CLOSED) {
                    // ignore if closed....
                    return;
                }
                logger.warn("check index [failure]");
                // report details in a separate message, it might contain control characters which mess up detection of the failure message
                logger.warn("{}", os.bytes().utf8ToString());
                throw new IOException("index check failure");
            }

            if (logger.isDebugEnabled()) {
                logger.debug("check index [success]\n{}", os.bytes().utf8ToString());
            }
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
    public Engine getEngineOrNull() {
        return this.currentEngineReference.get();
    }

    public void startRecovery(
        RecoveryState recoveryState,
        PeerRecoveryTargetService recoveryTargetService,
        PeerRecoveryTargetService.RecoveryListener recoveryListener,
        RepositoriesService repositoriesService,
        BiConsumer<MappingMetadata, ActionListener<Void>> mappingUpdateConsumer,
        IndicesService indicesService,
        long clusterStateVersion
    ) {
        // TODO: Create a proper object to encapsulate the recovery context
        // all of the current methods here follow a pattern of:
        // resolve context which isn't really dependent on the local shards and then async
        // call some external method with this pointer.
        // with a proper recovery context object we can simply change this to:
        // startRecovery(RecoveryState recoveryState, ShardRecoverySource source ) {
        // markAsRecovery("from " + source.getShortDescription(), recoveryState);
        // threadPool.generic().execute() {
        // onFailure () { listener.failure() };
        // doRun() {
        // if (source.recover(this)) {
        // recoveryListener.onRecoveryDone(recoveryState);
        // }
        // }
        // }}
        // }
        assert recoveryState.getRecoverySource().equals(shardRouting.recoverySource());
        switch (recoveryState.getRecoverySource().getType()) {
            case EMPTY_STORE, EXISTING_STORE -> executeRecovery("from store", recoveryState, recoveryListener, this::recoverFromStore);
            case PEER -> {
                try {
                    markAsRecovering("from " + recoveryState.getSourceNode(), recoveryState);
                    recoveryTargetService.startRecovery(this, recoveryState.getSourceNode(), clusterStateVersion, recoveryListener);
                } catch (Exception e) {
                    failShard("corrupted preexisting index", e);
                    recoveryListener.onRecoveryFailure(new RecoveryFailedException(recoveryState, null, e), true);
                }
            }
            case SNAPSHOT -> {
                final String repo = ((SnapshotRecoverySource) recoveryState.getRecoverySource()).snapshot().getRepository();
                executeRecovery(
                    "from snapshot",
                    recoveryState,
                    recoveryListener,
                    l -> restoreFromRepository(repositoriesService.repository(repo), l)
                );
            }
            case LOCAL_SHARDS -> {
                final IndexMetadata indexMetadata = indexSettings().getIndexMetadata();
                final Index resizeSourceIndex = indexMetadata.getResizeSourceIndex();
                final List<IndexShard> startedShards = new ArrayList<>();
                final IndexService sourceIndexService = indicesService.indexService(resizeSourceIndex);
                final Set<ShardId> requiredShards;
                final int numShards;
                if (sourceIndexService != null) {
                    requiredShards = IndexMetadata.selectRecoverFromShards(
                        shardId().id(),
                        sourceIndexService.getMetadata(),
                        indexMetadata.getNumberOfShards()
                    );
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
                    executeRecovery(
                        "from local shards",
                        recoveryState,
                        recoveryListener,
                        l -> recoverFromLocalShards(
                            mappingUpdateConsumer,
                            startedShards.stream().filter((s) -> requiredShards.contains(s.shardId())).toList(),
                            l
                        )
                    );
                } else {
                    final RuntimeException e;
                    if (numShards == -1) {
                        e = new IndexNotFoundException(resizeSourceIndex);
                    } else {
                        e = new IllegalStateException(
                            "not all required shards of index "
                                + resizeSourceIndex
                                + " are started yet, expected "
                                + numShards
                                + " found "
                                + startedShards.size()
                                + " can't recover shard "
                                + shardId()
                        );
                    }
                    throw e;
                }
            }
            default -> throw new IllegalArgumentException("Unknown recovery source " + recoveryState.getRecoverySource());
        }
    }

    private void executeRecovery(
        String reason,
        RecoveryState recoveryState,
        PeerRecoveryTargetService.RecoveryListener recoveryListener,
        CheckedConsumer<ActionListener<Boolean>, Exception> action
    ) {
        markAsRecovering(reason, recoveryState); // mark the shard as recovering on the cluster state thread
        threadPool.generic().execute(ActionRunnable.wrap(ActionListener.wrap(r -> {
            if (r) {
                recoveryListener.onRecoveryDone(recoveryState, getTimestampRange(), getEventIngestedRange());
            }
        }, e -> recoveryListener.onRecoveryFailure(new RecoveryFailedException(recoveryState, null, e), true)), action));
    }

    /**
     * Returns whether the shard is a relocated primary, i.e. not in charge anymore of replicating changes (see {@link ReplicationTracker}).
     */
    public boolean isRelocatedPrimary() {
        assert shardRouting.primary() : "only call isRelocatedPrimary on primary shard";
        return replicationTracker.isRelocated();
    }

    public RetentionLease addPeerRecoveryRetentionLease(
        String nodeId,
        long globalCheckpoint,
        ActionListener<ReplicationResponse> listener
    ) {
        assert assertPrimaryMode();
        // only needed for BWC reasons involving rolling upgrades from versions that do not support PRRLs:
        assert indexSettings.getIndexVersionCreated().before(IndexVersions.V_7_4_0) || indexSettings.isSoftDeleteEnabled() == false;
        return replicationTracker.addPeerRecoveryRetentionLease(nodeId, globalCheckpoint, listener);
    }

    public RetentionLease cloneLocalPeerRecoveryRetentionLease(String nodeId, ActionListener<ReplicationResponse> listener) {
        assert assertPrimaryMode();
        return replicationTracker.cloneLocalPeerRecoveryRetentionLease(nodeId, listener);
    }

    public void removePeerRecoveryRetentionLease(String nodeId, ActionListener<ReplicationResponse> listener) {
        assert assertPrimaryMode();
        replicationTracker.removePeerRecoveryRetentionLease(nodeId, listener);
    }

    /**
     * Returns a list of retention leases for peer recovery installed in this shard copy.
     */
    public List<RetentionLease> getPeerRecoveryRetentionLeases() {
        return replicationTracker.getPeerRecoveryRetentionLeases();
    }

    public boolean useRetentionLeasesInPeerRecovery() {
        return useRetentionLeasesInPeerRecovery;
    }

    private SafeCommitInfo getSafeCommitInfo() {
        final Engine engine = getEngineOrNull();
        return engine == null ? SafeCommitInfo.EMPTY : engine.getSafeCommitInfo();
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

    private static void persistMetadata(
        final ShardPath shardPath,
        final IndexSettings indexSettings,
        final ShardRouting newRouting,
        final @Nullable ShardRouting currentRouting,
        final Logger logger
    ) throws IOException {
        assert newRouting != null : "newRouting must not be null";

        // only persist metadata if routing information that is persisted in shard state metadata actually changed
        final ShardId shardId = newRouting.shardId();
        if (currentRouting == null
            || currentRouting.primary() != newRouting.primary()
            || currentRouting.allocationId().equals(newRouting.allocationId()) == false) {
            assert currentRouting == null || currentRouting.isSameAllocation(newRouting);
            if (logger.isTraceEnabled()) {
                final String writeReason;
                if (currentRouting == null) {
                    writeReason = "initial state with allocation id [" + newRouting.allocationId() + "]";
                } else {
                    writeReason = "routing changed from " + currentRouting + " to " + newRouting;
                }
                logger.trace("{} writing shard state, reason [{}]", shardId, writeReason);
            }
            final ShardStateMetadata newShardStateMetadata = new ShardStateMetadata(
                newRouting.primary(),
                indexSettings.getUUID(),
                newRouting.allocationId()
            );
            ShardStateMetadata.FORMAT.writeAndCleanup(newShardStateMetadata, shardPath.getShardStatePath());
        } else {
            logger.trace("{} skip writing shard state, has been written before", shardId);
        }
    }

    public static Analyzer buildIndexAnalyzer(MapperService mapperService) {
        if (mapperService == null) {
            return null;
        }
        return new DelegatingAnalyzerWrapper(Analyzer.PER_FIELD_REUSE_STRATEGY) {
            @Override
            protected Analyzer getWrappedAnalyzer(String fieldName) {
                return mapperService.indexAnalyzer(
                    fieldName,
                    f -> { throw new IllegalArgumentException("Field [" + f + "] has no associated analyzer"); }
                );
            }
        };
    }

    private EngineConfig newEngineConfig(LongSupplier globalCheckpointSupplier) {
        final Sort indexSort = indexSortSupplier.get();
        final Engine.Warmer warmer = reader -> {
            assert Thread.holdsLock(mutex) == false : "warming engine under mutex";
            assert reader != null;
            if (this.warmer != null) {
                this.warmer.warm(reader);
            }
        };
        final boolean isTimeBasedIndex = mapperService == null ? false : mapperService.mappingLookup().hasTimestampField();
        return new EngineConfig(
            shardId,
            threadPool,
            indexSettings,
            warmer,
            store,
            indexSettings.getMergePolicy(isTimeBasedIndex),
            buildIndexAnalyzer(mapperService),
            similarityService.similarity(mapperService == null ? null : mapperService::fieldType),
            codecService,
            shardEventListener,
            indexCache != null ? indexCache.query() : null,
            cachingPolicy,
            translogConfig,
            IndexingMemoryController.SHARD_INACTIVE_TIME_SETTING.get(indexSettings.getSettings()),
            List.of(refreshListeners, refreshPendingLocationListener, refreshFieldHasValueListener),
            List.of(new RefreshMetricUpdater(refreshMetric), new RefreshShardFieldStatsListener()),
            indexSort,
            circuitBreakerService,
            globalCheckpointSupplier,
            replicationTracker::getRetentionLeases,
            this::getOperationPrimaryTerm,
            snapshotCommitSupplier,
            isTimeBasedIndex ? TIMESERIES_LEAF_READERS_SORTER : null,
            relativeTimeInNanosSupplier,
            indexCommitListener,
            routingEntry().isPromotableToPrimary(),
            mapperService()
        );
    }

    /**
     * Acquire a primary operation permit whenever the shard is ready for indexing. If a permit is directly available, the provided
     * ActionListener will be called on the calling thread. During relocation hand-off, permit acquisition can be delayed. The provided
     * ActionListener will then be called using the provided executor.
     *
     */
    public void acquirePrimaryOperationPermit(ActionListener<Releasable> onPermitAcquired, Executor executorOnDelay) {
        acquirePrimaryOperationPermit(onPermitAcquired, executorOnDelay, false);
    }

    public void acquirePrimaryOperationPermit(
        ActionListener<Releasable> onPermitAcquired,
        Executor executorOnDelay,
        boolean forceExecution
    ) {
        verifyNotClosed();
        assert shardRouting.primary() : "acquirePrimaryOperationPermit should only be called on primary shard: " + shardRouting;
        indexShardOperationPermits.acquire(wrapPrimaryOperationPermitListener(onPermitAcquired), executorOnDelay, forceExecution);
    }

    public boolean isPrimaryMode() {
        assert indexShardOperationPermits.getActiveOperationsCount() != 0 : "must hold permit to check primary mode";
        return replicationTracker.isPrimaryMode();
    }

    /**
     * Acquire all primary operation permits. Once all permits are acquired, the provided ActionListener is called.
     * It is the responsibility of the caller to close the {@link Releasable}.
     */
    public void acquireAllPrimaryOperationsPermits(final ActionListener<Releasable> onPermitAcquired, final TimeValue timeout) {
        verifyNotClosed();
        assert shardRouting.primary() : "acquireAllPrimaryOperationsPermits should only be called on primary shard: " + shardRouting;

        asyncBlockOperations(wrapPrimaryOperationPermitListener(onPermitAcquired), timeout.duration(), timeout.timeUnit());
    }

    /**
     * Wraps the action to run on a primary after acquiring permit. This wrapping is used to check if the shard is in primary mode before
     * executing the action.
     *
     * @param listener the listener to wrap
     * @return the wrapped listener
     */
    private ActionListener<Releasable> wrapPrimaryOperationPermitListener(final ActionListener<Releasable> listener) {
        return listener.delegateFailure((l, r) -> {
            if (isPrimaryMode()) {
                l.onResponse(r);
            } else {
                r.close();
                l.onFailure(new ShardNotInPrimaryModeException(shardId, state));
            }
        });
    }

    private void asyncBlockOperations(ActionListener<Releasable> onPermitAcquired, long timeout, TimeUnit timeUnit) {
        final Releasable forceRefreshes = refreshListeners.forceRefreshes();
        final ActionListener<Releasable> wrappedListener = ActionListener.wrap(r -> {
            forceRefreshes.close();
            onPermitAcquired.onResponse(r);
        }, e -> {
            forceRefreshes.close();
            onPermitAcquired.onFailure(e);
        });
        try {
            indexShardOperationPermits.blockOperations(wrappedListener, timeout, timeUnit, threadPool.generic());
        } catch (Exception e) {
            forceRefreshes.close();
            throw e;
        }
    }

    /**
     * Runs the specified runnable under a permit and otherwise calling back the specified failure callback. This method is really a
     * convenience for {@link #acquirePrimaryOperationPermit(ActionListener, Executor)} where the listener equates to
     * try-with-resources closing the releasable after executing the runnable on successfully acquiring the permit, an otherwise calling
     * back the failure callback.
     *
     * @param runnable the runnable to execute under permit
     * @param onFailure the callback on failure
     * @param executorOnDelay the executor to execute the runnable on if permit acquisition is blocked
     */
    public void runUnderPrimaryPermit(final Runnable runnable, final Consumer<Exception> onFailure, final Executor executorOnDelay) {
        verifyNotClosed();
        assert shardRouting.primary() : "runUnderPrimaryPermit should only be called on primary shard but was " + shardRouting;
        final ActionListener<Releasable> onPermitAcquired = ActionListener.wrap(releasable -> {
            try (Releasable ignore = releasable) {
                runnable.run();
            }
        }, onFailure);
        acquirePrimaryOperationPermit(onPermitAcquired, executorOnDelay);
    }

    private <E extends Exception> void bumpPrimaryTerm(
        final long newPrimaryTerm,
        final CheckedRunnable<E> onBlocked,
        @Nullable ActionListener<Releasable> combineWithAction
    ) {
        assert Thread.holdsLock(mutex);
        assert newPrimaryTerm > pendingPrimaryTerm || (newPrimaryTerm >= pendingPrimaryTerm && combineWithAction != null);
        assert getOperationPrimaryTerm() <= pendingPrimaryTerm;
        final CountDownLatch termUpdated = new CountDownLatch(1);
        asyncBlockOperations(new ActionListener<Releasable>() {
            @Override
            public void onFailure(final Exception e) {
                try {
                    innerFail(e);
                } finally {
                    if (combineWithAction != null) {
                        combineWithAction.onFailure(e);
                    }
                }
            }

            private void innerFail(final Exception e) {
                try {
                    failShard("exception during primary term transition", e);
                } catch (AlreadyClosedException ace) {
                    // ignore, shard is already closed
                }
            }

            @Override
            public void onResponse(final Releasable releasable) {
                final Releasable releaseOnce = Releasables.releaseOnce(releasable);
                try {
                    assert getOperationPrimaryTerm() <= pendingPrimaryTerm;
                    termUpdated.await();
                    // indexShardOperationPermits doesn't guarantee that async submissions are executed
                    // in the order submitted. We need to guard against another term bump
                    if (getOperationPrimaryTerm() < newPrimaryTerm) {
                        replicationTracker.setOperationPrimaryTerm(newPrimaryTerm);
                        onBlocked.run();
                    }
                } catch (final Exception e) {
                    if (combineWithAction == null) {
                        // otherwise leave it to combineWithAction to release the permit
                        releaseOnce.close();
                    }
                    innerFail(e);
                } finally {
                    if (combineWithAction != null) {
                        combineWithAction.onResponse(releasable);
                    } else {
                        releaseOnce.close();
                    }
                }
            }
        }, 30, TimeUnit.MINUTES);
        pendingPrimaryTerm = newPrimaryTerm;
        termUpdated.countDown();
    }

    /**
     * Acquire a replica operation permit whenever the shard is ready for indexing (see
     * {@link #acquirePrimaryOperationPermit(ActionListener, Executor)}). If the given primary term is lower than then one in
     * {@link #shardRouting}, the {@link ActionListener#onFailure(Exception)} method of the provided listener is invoked with an
     * {@link IllegalStateException}. If permit acquisition is delayed, the listener will be invoked on the executor with the specified
     * name.
     *
     * @param opPrimaryTerm              the operation primary term
     * @param globalCheckpoint           the global checkpoint associated with the request
     * @param maxSeqNoOfUpdatesOrDeletes the max seq_no of updates (index operations overwrite Lucene) or deletes captured on the primary
     *                                   after this replication request was executed on it (see {@link #getMaxSeqNoOfUpdatesOrDeletes()}
     * @param onPermitAcquired           the listener for permit acquisition
     * @param executorOnDelay            the name of the executor to invoke the listener on if permit acquisition is delayed
     */
    public void acquireReplicaOperationPermit(
        final long opPrimaryTerm,
        final long globalCheckpoint,
        final long maxSeqNoOfUpdatesOrDeletes,
        final ActionListener<Releasable> onPermitAcquired,
        final Executor executorOnDelay
    ) {
        innerAcquireReplicaOperationPermit(
            opPrimaryTerm,
            globalCheckpoint,
            maxSeqNoOfUpdatesOrDeletes,
            onPermitAcquired,
            false,
            (listener) -> indexShardOperationPermits.acquire(listener, executorOnDelay, true)
        );
    }

    /**
     * Acquire all replica operation permits whenever the shard is ready for indexing (see
     * {@link #acquireAllPrimaryOperationsPermits(ActionListener, TimeValue)}. If the given primary term is lower than then one in
     * {@link #shardRouting}, the {@link ActionListener#onFailure(Exception)} method of the provided listener is invoked with an
     * {@link IllegalStateException}.
     *
     * @param opPrimaryTerm              the operation primary term
     * @param globalCheckpoint           the global checkpoint associated with the request
     * @param maxSeqNoOfUpdatesOrDeletes the max seq_no of updates (index operations overwrite Lucene) or deletes captured on the primary
     *                                   after this replication request was executed on it (see {@link #getMaxSeqNoOfUpdatesOrDeletes()}
     * @param onPermitAcquired           the listener for permit acquisition
     * @param timeout                    the maximum time to wait for the in-flight operations block
     */
    public void acquireAllReplicaOperationsPermits(
        final long opPrimaryTerm,
        final long globalCheckpoint,
        final long maxSeqNoOfUpdatesOrDeletes,
        final ActionListener<Releasable> onPermitAcquired,
        final TimeValue timeout
    ) {
        innerAcquireReplicaOperationPermit(
            opPrimaryTerm,
            globalCheckpoint,
            maxSeqNoOfUpdatesOrDeletes,
            onPermitAcquired,
            true,
            listener -> asyncBlockOperations(listener, timeout.duration(), timeout.timeUnit())
        );
    }

    private void innerAcquireReplicaOperationPermit(
        final long opPrimaryTerm,
        final long globalCheckpoint,
        final long maxSeqNoOfUpdatesOrDeletes,
        final ActionListener<Releasable> onPermitAcquired,
        final boolean allowCombineOperationWithPrimaryTermUpdate,
        final Consumer<ActionListener<Releasable>> operationExecutor
    ) {
        verifyNotClosed();

        // This listener is used for the execution of the operation. If the operation requires all the permits for its
        // execution and the primary term must be updated first, we can combine the operation execution with the
        // primary term update. Since indexShardOperationPermits doesn't guarantee that async submissions are executed
        // in the order submitted, combining both operations ensure that the term is updated before the operation is
        // executed. It also has the side effect of acquiring all the permits one time instead of two.
        final ActionListener<Releasable> operationListener = onPermitAcquired.delegateFailure((delegatedListener, releasable) -> {
            if (opPrimaryTerm < getOperationPrimaryTerm()) {
                releasable.close();
                final String message = String.format(
                    Locale.ROOT,
                    "%s operation primary term [%d] is too old (current [%d])",
                    shardId,
                    opPrimaryTerm,
                    getOperationPrimaryTerm()
                );
                delegatedListener.onFailure(new IllegalStateException(message));
            } else {
                assert assertReplicationTarget();
                try {
                    updateGlobalCheckpointOnReplica(globalCheckpoint, "operation");
                    advanceMaxSeqNoOfUpdatesOrDeletes(maxSeqNoOfUpdatesOrDeletes);
                } catch (Exception e) {
                    releasable.close();
                    delegatedListener.onFailure(e);
                    return;
                }
                delegatedListener.onResponse(releasable);
            }
        });

        if (requirePrimaryTermUpdate(opPrimaryTerm, allowCombineOperationWithPrimaryTermUpdate)) {
            synchronized (mutex) {
                if (requirePrimaryTermUpdate(opPrimaryTerm, allowCombineOperationWithPrimaryTermUpdate)) {
                    final IndexShardState shardState = state();
                    // only roll translog and update primary term if shard has made it past recovery
                    // Having a new primary term here means that the old primary failed and that there is a new primary, which again
                    // means that the master will fail this shard as all initializing shards are failed when a primary is selected
                    // We abort early here to prevent an ongoing recovery from the failed primary to mess with the global / local checkpoint
                    if (shardState != IndexShardState.POST_RECOVERY && shardState != IndexShardState.STARTED) {
                        throw new IndexShardNotStartedException(shardId, shardState);
                    }

                    bumpPrimaryTerm(opPrimaryTerm, () -> {
                        updateGlobalCheckpointOnReplica(globalCheckpoint, "primary term transition");
                        final long currentGlobalCheckpoint = getLastKnownGlobalCheckpoint();
                        final long maxSeqNo = seqNoStats().getMaxSeqNo();
                        logger.info(
                            "detected new primary with primary term [{}], global checkpoint [{}], max_seq_no [{}]",
                            opPrimaryTerm,
                            currentGlobalCheckpoint,
                            maxSeqNo
                        );
                        if (currentGlobalCheckpoint < maxSeqNo) {
                            resetEngineToGlobalCheckpoint();
                        } else {
                            getEngine().rollTranslogGeneration();
                        }
                    }, allowCombineOperationWithPrimaryTermUpdate ? operationListener : null);

                    if (allowCombineOperationWithPrimaryTermUpdate) {
                        logger.debug("operation execution has been combined with primary term update");
                        return;
                    }
                }
            }
        }
        assert opPrimaryTerm <= pendingPrimaryTerm
            : "operation primary term [" + opPrimaryTerm + "] should be at most [" + pendingPrimaryTerm + "]";
        operationExecutor.accept(operationListener);
    }

    private boolean requirePrimaryTermUpdate(final long opPrimaryTerm, final boolean allPermits) {
        return (opPrimaryTerm > pendingPrimaryTerm) || (allPermits && opPrimaryTerm > getOperationPrimaryTerm());
    }

    public static final int OPERATIONS_BLOCKED = -1;

    /**
     * Obtain the active operation count, or {@link IndexShard#OPERATIONS_BLOCKED} if all permits are held (even if there are
     * outstanding operations in flight).
     *
     * @return the active operation count, or {@link IndexShard#OPERATIONS_BLOCKED} when all permits are held.
     */
    public int getActiveOperationsCount() {
        return indexShardOperationPermits.getActiveOperationsCount();
    }

    /**
     * Syncs the given location with the underlying storage, unless already synced, as part of a write operation.
     * <p>
     * This method might return immediately without actually fsyncing the location until the sync listener is called. Yet, unless there is
     * already another thread fsyncing the transaction log the caller thread will be hijacked to run the fsync for all pending fsync
     * operations.
     * <p>
     * This method allows indexing threads to continue indexing without blocking on fsync calls. We ensure that there is only one thread
     * blocking on the sync an all others can continue indexing.
     * <p>
     * NOTE: if the syncListener throws an exception when it's processed the exception will only be logged. Users should make sure that the
     * listener handles all exception cases internally.
     */
    public final void syncAfterWrite(Translog.Location location, Consumer<Exception> syncListener) {
        // TODO AwaitsFix https://github.com/elastic/elasticsearch/issues/97183
        // assert indexShardOperationPermits.getActiveOperationsCount() != 0;
        verifyNotClosed();
        getEngine().asyncEnsureTranslogSynced(location, syncListener);
    }

    /**
     * This method provides the same behavior as #sync but for persisting the global checkpoint. It will initiate a sync
     * if the request global checkpoint is greater than the currently persisted global checkpoint. However, same as #sync it
     * will not ensure that the request global checkpoint is available to be synced. It is the caller's duty to only call this
     * method with a valid processed global checkpoint that is available to sync.
     */
    public void syncGlobalCheckpoint(long globalCheckpoint, Consumer<Exception> syncListener) {
        verifyNotClosed();
        getEngine().asyncEnsureGlobalCheckpointSynced(globalCheckpoint, syncListener);
    }

    public void sync() throws IOException {
        verifyNotClosed();
        getEngine().syncTranslog();
    }

    /**
     * Checks if the underlying storage sync is required.
     */
    public boolean isSyncNeeded() {
        return getEngine().isTranslogSyncNeeded();
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
        if (shouldPeriodicallyFlush() || shouldRollTranslogGeneration()) {
            if (flushOrRollRunning.compareAndSet(false, true)) {
                /*
                 * We have to check again since otherwise there is a race when a thread passes the first check next to another thread which
                 * performs the operation quickly enough to  finish before the current thread could flip the flag. In that situation, we
                 * have an extra operation.
                 *
                 * Additionally, a flush implicitly executes a translog generation roll so if we execute a flush then we do not need to
                 * check if we should roll the translog generation.
                 */
                if (shouldPeriodicallyFlush()) {
                    logger.debug("submitting async flush request");
                    threadPool.executor(ThreadPool.Names.FLUSH).execute(() -> {
                        flush(new FlushRequest(), new ActionListener<>() {
                            @Override
                            public void onResponse(Boolean flushed) {
                                periodicFlushMetric.inc();
                            }

                            @Override
                            public void onFailure(Exception e) {
                                if (state != IndexShardState.CLOSED) {
                                    logger.warn("failed to flush index", e);
                                }
                            }
                        });
                        flushOrRollRunning.compareAndSet(true, false);
                        afterWriteOperation();
                    });
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
                        protected void doRun() {
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
     * Simple struct encapsulating a shard failure
     *
     * @see IndexShard#addShardFailureCallback(Consumer)
     */
    public record ShardFailure(ShardRouting routing, String reason, @Nullable Exception cause) {}

    EngineFactory getEngineFactory() {
        return engineFactory;
    }

    // for tests
    ReplicationTracker getReplicationTracker() {
        return replicationTracker;
    }

    /**
     * Executes a scheduled refresh if necessary. Completes the listener with true if a refresh was performed otherwise false.
     */
    public void scheduledRefresh(ActionListener<Boolean> listener) {
        ActionListener.run(listener, l -> {
            verifyNotClosed();
            boolean listenerNeedsRefresh = refreshListeners.refreshNeeded();
            final Engine engine = getEngine();
            if (isReadAllowed() && (listenerNeedsRefresh || engine.refreshNeeded())) {
                if (listenerNeedsRefresh == false // if we have a listener that is waiting for a refresh we need to force it
                    && engine.allowSearchIdleOptimization()
                    && isSearchIdle()
                    && indexSettings.isExplicitRefresh() == false
                    && active.get()) { // it must be active otherwise we might not free up segment memory once the shard became inactive
                    // lets skip this refresh since we are search idle and
                    // don't necessarily need to refresh. the next searcher access will register a refreshListener and that will
                    // cause the next schedule to refresh.
                    logger.trace("scheduledRefresh: search-idle, skipping refresh");
                    engine.maybePruneDeletes(); // try to prune the deletes in the engine if we accumulated some
                    setRefreshPending(engine);
                    l.onResponse(false);
                    return;
                } else {
                    logger.trace("scheduledRefresh: refresh with source [schedule]");
                    engine.maybeRefresh("schedule", l.map(Engine.RefreshResult::refreshed));
                    return;
                }
            }
            logger.trace("scheduledRefresh: no refresh needed");
            engine.maybePruneDeletes(); // try to prune the deletes in the engine if we accumulated some
            l.onResponse(false);
        });
    }

    /**
     * Returns true if this shards is search idle
     */
    public final boolean isSearchIdle() {
        return (threadPool.relativeTimeInMillis() - lastSearcherAccess.get()) >= indexSettings.getSearchIdleAfter().getMillis();
    }

    public long searchIdleTime() {
        return threadPool.relativeTimeInMillis() - lastSearcherAccess.get();
    }

    /**
     * Returns the last timestamp the searcher was accessed. This is a relative timestamp in milliseconds.
     */
    final long getLastSearcherAccess() {
        return lastSearcherAccess.get();
    }

    /**
     * Returns true if this shard has some scheduled refresh that is pending because of search-idle.
     */
    public final boolean hasRefreshPending() {
        return pendingRefreshLocation.get() != null;
    }

    private void setRefreshPending(Engine engine) {
        final Translog.Location lastWriteLocation = engine.getTranslogLastWriteLocation();
        pendingRefreshLocation.updateAndGet(curr -> {
            if (curr == null || curr.compareTo(lastWriteLocation) <= 0) {
                return lastWriteLocation;
            } else {
                return curr;
            }
        });
    }

    private class RefreshPendingLocationListener implements ReferenceManager.RefreshListener {
        Translog.Location lastWriteLocation;

        @Override
        public void beforeRefresh() {
            try {
                lastWriteLocation = getEngine().getTranslogLastWriteLocation();
            } catch (AlreadyClosedException exc) {
                // shard is closed - no location is fine
                lastWriteLocation = null;
            }
        }

        @Override
        public void afterRefresh(boolean didRefresh) {
            if (didRefresh && lastWriteLocation != null) {
                pendingRefreshLocation.updateAndGet(pendingLocation -> {
                    if (pendingLocation == null || pendingLocation.compareTo(lastWriteLocation) <= 0) {
                        return null;
                    } else {
                        return pendingLocation;
                    }
                });
            }
        }
    }

    private class RefreshFieldHasValueListener implements ReferenceManager.RefreshListener {
        @Override
        public void beforeRefresh() {}

        @Override
        public void afterRefresh(boolean didRefresh) {
            if (enableFieldHasValue) {
                try (Engine.Searcher hasValueSearcher = getEngine().acquireSearcher("field_has_value")) {
                    setFieldInfos(FieldInfos.getMergedFieldInfos(hasValueSearcher.getIndexReader()));
                } catch (AlreadyClosedException ignore) {
                    // engine is closed - no updated FieldInfos is fine
                }
            }
        }
    }

    /**
     * Returns the shard-level field stats, which includes the number of segments in the latest NRT reader of this shard
     * and the total number of fields across those segments.
     */
    public ShardFieldStats getShardFieldStats() {
        return shardFieldStats;
    }

    private class RefreshShardFieldStatsListener implements ReferenceManager.RefreshListener {
        @Override
        public void beforeRefresh() {

        }

        @Override
        public void afterRefresh(boolean didRefresh) {
            if (shardFieldStats == null || didRefresh) {
                try (var searcher = getEngine().acquireSearcher("shard_field_stats", Engine.SearcherScope.INTERNAL)) {
                    int numSegments = 0;
                    int totalFields = 0;
                    for (LeafReaderContext leaf : searcher.getLeafContexts()) {
                        numSegments++;
                        totalFields += leaf.reader().getFieldInfos().size();
                    }
                    shardFieldStats = new ShardFieldStats(numSegments, totalFields);
                } catch (AlreadyClosedException ignored) {

                }
            }
        }
    }

    /**
     * Ensures this shard is search active before invoking the provided listener.
     * <p>
     * This is achieved by registering a refresh listener and invoking the provided listener from the refresh listener once the shard is
     * active again and all pending refresh translog location has been refreshed. A refresh may be executed to avoid waiting for
     * {@link #scheduledRefresh(ActionListener)} to be invoked. If there is no pending refresh location registered the provided listener
     * will be invoked immediately.
     *
     * @param listener the listener to invoke once the pending refresh location is visible. The listener will be called with
     *                 <code>true</code> if the listener was registered to wait for a refresh.
     */
    public final void ensureShardSearchActive(Consumer<Boolean> listener) {
        markSearcherAccessed(); // move the shard into non-search idle
        final Translog.Location location = pendingRefreshLocation.get();
        if (location != null) {
            addRefreshListener(location, (result) -> {
                pendingRefreshLocation.compareAndSet(location, null);
                listener.accept(true);
            });
            // trigger a refresh to avoid waiting for scheduledRefresh(...) to be invoked from index level refresh scheduler.
            // (The if statement should avoid doing an additional refresh if scheduled refresh was invoked between getting
            // the current refresh location and adding a refresh listener.)
            if (location == pendingRefreshLocation.get()) {
                // This method may be called from many different threads including transport_worker threads and
                // a refresh can be a costly operation, so we should fork to a refresh thread to be safe:
                threadPool.executor(ThreadPool.Names.REFRESH).execute(() -> {
                    if (location == pendingRefreshLocation.get()) {
                        getEngine().maybeRefresh("ensure-shard-search-active", new PlainActionFuture<>());
                    }
                });
            }
        } else {
            listener.accept(false);
        }
    }

    /**
     * Add a listener for refreshes.
     *
     * @param location the location to listen for
     * @param listener for the refresh. Called with true if registering the listener ran it out of slots and forced a refresh. Called with
     *        false otherwise.
     */
    public void addRefreshListener(Translog.Location location, Consumer<Boolean> listener) {
        SubscribableListener<Void> subscribableListener = postRecoveryComplete;
        if (postRecoveryComplete != null) {
            subscribableListener.addListener(new ActionListener<>() {
                @Override
                public void onResponse(Void unused) {
                    if (isReadAllowed()) {
                        refreshListeners.addOrNotify(location, listener);
                    } else {
                        listener.accept(false);
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    listener.accept(false);
                }
            }, EsExecutors.DIRECT_EXECUTOR_SERVICE, threadPool.getThreadContext());
        } else {
            // we're not yet ready for reads, just ignore refresh cycles
            listener.accept(false);
        }
    }

    /**
     * Add a listener for refreshes.
     *
     * @param checkpoint the seqNo checkpoint to listen for
     * @param allowUnIssuedSequenceNumber whether to allow waiting for checkpoints larger than the processed local checkpoint
     * @param listener for the refresh.
     */
    public void addRefreshListener(long checkpoint, boolean allowUnIssuedSequenceNumber, ActionListener<Void> listener) {
        SubscribableListener<Void> subscribableListener = postRecoveryComplete;
        if (subscribableListener != null) {
            subscribableListener.addListener(new ActionListener<>() {
                @Override
                public void onResponse(Void unused) {
                    if (isReadAllowed()) {
                        refreshListeners.addOrNotify(checkpoint, allowUnIssuedSequenceNumber, listener);
                    } else {
                        listener.onFailure(new IllegalIndexShardStateException(shardId, state, "Read not allowed on IndexShard"));
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    listener.onFailure(e);
                }
            }, EsExecutors.DIRECT_EXECUTOR_SERVICE, threadPool.getThreadContext());
        } else {
            listener.onFailure(new IllegalIndexShardStateException(shardId, state, "Read not allowed on IndexShard"));
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
        public void beforeRefresh() {
            if (Assertions.ENABLED) {
                assert callingThread == null
                    : "beforeRefresh was called by " + callingThread.getName() + " without a corresponding call to afterRefresh";
                callingThread = Thread.currentThread();
            }
            currentRefreshStartTime = System.nanoTime();
        }

        @Override
        public void afterRefresh(boolean didRefresh) {
            if (Assertions.ENABLED) {
                assert callingThread != null : "afterRefresh called but not beforeRefresh";
                assert callingThread == Thread.currentThread()
                    : "beforeRefreshed called by a different thread. current ["
                        + Thread.currentThread().getName()
                        + "], thread that called beforeRefresh ["
                        + callingThread.getName()
                        + "]";
                callingThread = null;
            }
            refreshMetric.inc(System.nanoTime() - currentRefreshStartTime);
        }
    }

    /**
     * Rollback the current engine to the safe commit, then replay local translog up to the global checkpoint.
     */
    void resetEngineToGlobalCheckpoint() throws IOException {
        assert Thread.holdsLock(mutex) == false : "resetting engine under mutex";
        assert getActiveOperationsCount() == OPERATIONS_BLOCKED
            : "resetting engine without blocking operations; active operations are [" + getActiveOperationsCount() + ']';
        sync(); // persist the global checkpoint to disk
        final SeqNoStats seqNoStats = seqNoStats();
        final TranslogStats translogStats = translogStats();
        // flush to make sure the latest commit, which will be opened by the read-only engine, includes all operations.
        flush(new FlushRequest().waitIfOngoing(true));

        SetOnce<Engine> newEngineReference = new SetOnce<>();
        final long globalCheckpoint = getLastKnownGlobalCheckpoint();
        assert globalCheckpoint == getLastSyncedGlobalCheckpoint();
        synchronized (engineMutex) {
            verifyNotClosed();
            // we must create both new read-only engine and new read-write engine under engineMutex to ensure snapshotStoreMetadata,
            // acquireXXXCommit and close works.
            final Engine readOnlyEngine = new ReadOnlyEngine(
                newEngineConfig(replicationTracker),
                seqNoStats,
                translogStats,
                false,
                Function.identity(),
                true,
                false
            ) {
                @Override
                public IndexCommitRef acquireLastIndexCommit(boolean flushFirst) {
                    synchronized (engineMutex) {
                        if (newEngineReference.get() == null) {
                            throw new AlreadyClosedException("engine was closed");
                        }
                        // ignore flushFirst since we flushed above and we do not want to interfere with ongoing translog replay
                        return newEngineReference.get().acquireLastIndexCommit(false);
                    }
                }

                @Override
                public IndexCommitRef acquireSafeIndexCommit() {
                    synchronized (engineMutex) {
                        if (newEngineReference.get() == null) {
                            throw new AlreadyClosedException("engine was closed");
                        }
                        return newEngineReference.get().acquireSafeIndexCommit();
                    }
                }

                @Override
                public void close() throws IOException {
                    Engine newEngine;
                    synchronized (engineMutex) {
                        newEngine = newEngineReference.get();
                        if (newEngine == currentEngineReference.get()) {
                            // we successfully installed the new engine so do not close it.
                            newEngine = null;
                        }
                    }
                    IOUtils.close(super::close, newEngine);
                }
            };
            IOUtils.close(currentEngineReference.getAndSet(readOnlyEngine));
            newEngineReference.set(engineFactory.newReadWriteEngine(newEngineConfig(replicationTracker)));
            onNewEngine(newEngineReference.get());
        }
        final Engine.TranslogRecoveryRunner translogRunner = (engine, snapshot) -> runTranslogRecovery(
            engine,
            snapshot,
            Engine.Operation.Origin.LOCAL_RESET,
            () -> {
                // TODO: add a dedicate recovery stats for the reset translog
            }
        );
        newEngineReference.get().recoverFromTranslog(translogRunner, globalCheckpoint);
        newEngineReference.get().refresh("reset_engine");
        synchronized (engineMutex) {
            verifyNotClosed();
            IOUtils.close(currentEngineReference.getAndSet(newEngineReference.get()));
            // We set active because we are now writing operations to the engine; this way,
            // if we go idle after some time and become inactive, we still give sync'd flush a chance to run.
            active.set(true);
        }
        // time elapses after the engine is created above (pulling the config settings) until we set the engine reference, during
        // which settings changes could possibly have happened, so here we forcefully push any config changes to the new engine.
        onSettingsChanged();
    }

    /**
     * Returns the maximum sequence number of either update or delete operations have been processed in this shard
     * or the sequence number from {@link #advanceMaxSeqNoOfUpdatesOrDeletes(long)}. An index request is considered
     * as an update operation if it overwrites the existing documents in Lucene index with the same document id.
     * <p>
     * The primary captures this value after executes a replication request, then transfers it to a replica before
     * executing that replication request on a replica.
     */
    public long getMaxSeqNoOfUpdatesOrDeletes() {
        return getEngine().getMaxSeqNoOfUpdatesOrDeletes();
    }

    /**
     * A replica calls this method to advance the max_seq_no_of_updates marker of its engine to at least the max_seq_no_of_updates
     * value (piggybacked in a replication request) that it receives from its primary before executing that replication request.
     * The receiving value is at least as high as the max_seq_no_of_updates on the primary was when any of the operations of that
     * replication request were processed on it.
     * <p>
     * A replica shard also calls this method to bootstrap the max_seq_no_of_updates marker with the value that it received from
     * the primary in peer-recovery, before it replays remote translog operations from the primary. The receiving value is at least
     * as high as the max_seq_no_of_updates on the primary was when any of these operations were processed on it.
     * <p>
     * These transfers guarantee that every index/delete operation when executing on a replica engine will observe this marker a value
     * which is at least the value of the max_seq_no_of_updates marker on the primary after that operation was executed on the primary.
     *
     * @see #acquireReplicaOperationPermit(long, long, long, ActionListener, Executor)
     * @see RecoveryTarget#indexTranslogOperations(List, int, long, long, RetentionLeases, long, ActionListener)
     */
    public void advanceMaxSeqNoOfUpdatesOrDeletes(long seqNo) {
        getEngine().advanceMaxSeqNoOfUpdatesOrDeletes(seqNo);
    }

    /**
     * Performs the pre-closing checks on the {@link IndexShard}.
     *
     * @throws IllegalStateException if the sanity checks failed
     */
    public void verifyShardBeforeIndexClosing() throws IllegalStateException {
        getEngine().verifyEngineBeforeIndexClosing();
    }

    RetentionLeaseSyncer getRetentionLeaseSyncer() {
        return retentionLeaseSyncer;
    }

    public long getRelativeTimeInNanos() {
        return relativeTimeInNanosSupplier.getAsLong();
    }

    @Override
    public String toString() {
        return "IndexShard(shardRouting=" + shardRouting + ")";
    }

    /**
     * @deprecated use {@link #waitForPrimaryTermAndGeneration(long, long, ActionListener)} instead.
     */
    @Deprecated
    public void waitForSegmentGeneration(long segmentGeneration, ActionListener<Long> listener) {
        waitForPrimaryTermAndGeneration(getOperationPrimaryTerm(), segmentGeneration, listener);
    }

    private void checkAndCallWaitForEngineOrClosedShardListeners() {
        if (getEngineOrNull() != null || state == IndexShardState.CLOSED) {
            waitForEngineOrClosedShardListeners.onResponse(null);
        }
    }

    /**
     * Registers a listener for an event when the shard opens the engine or is the shard is closed
     */
    public void waitForEngineOrClosedShard(ActionListener<Void> listener) {
        waitForEngineOrClosedShardListeners.addListener(listener);
    }

    /**
     * Registers a listener for an event when the shard advances to the provided primary term and segment generation
     */
    public void waitForPrimaryTermAndGeneration(long primaryTerm, long segmentGeneration, ActionListener<Long> listener) {
        waitForEngineOrClosedShard(
            listener.delegateFailureAndWrap(
                (l, ignored) -> getEngine().addPrimaryTermAndGenerationListener(primaryTerm, segmentGeneration, l)
            )
        );
    }

}
