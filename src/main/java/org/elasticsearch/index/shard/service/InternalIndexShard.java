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

package org.elasticsearch.index.shard.service;

import org.apache.lucene.index.CheckIndex;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.FilteredQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.ThreadInterruptedException;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.ElasticSearchIllegalArgumentException;
import org.elasticsearch.ElasticSearchIllegalStateException;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.BytesHolder;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.FastByteArrayOutputStream;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.metrics.MeanMetric;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.aliases.IndexAliasesService;
import org.elasticsearch.index.cache.IndexCache;
import org.elasticsearch.index.engine.*;
import org.elasticsearch.index.flush.FlushStats;
import org.elasticsearch.index.get.GetStats;
import org.elasticsearch.index.get.ShardGetService;
import org.elasticsearch.index.indexing.IndexingStats;
import org.elasticsearch.index.indexing.ShardIndexingService;
import org.elasticsearch.index.mapper.*;
import org.elasticsearch.index.merge.MergeStats;
import org.elasticsearch.index.merge.scheduler.MergeSchedulerProvider;
import org.elasticsearch.index.query.IndexQueryParserService;
import org.elasticsearch.index.query.QueryParseContext;
import org.elasticsearch.index.refresh.RefreshStats;
import org.elasticsearch.index.search.nested.IncludeAllChildrenQuery;
import org.elasticsearch.index.search.nested.NonNestedDocsFilter;
import org.elasticsearch.index.search.stats.SearchStats;
import org.elasticsearch.index.search.stats.ShardSearchService;
import org.elasticsearch.index.settings.IndexSettings;
import org.elasticsearch.index.settings.IndexSettingsService;
import org.elasticsearch.index.shard.*;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.store.StoreStats;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.indices.IndicesLifecycle;
import org.elasticsearch.indices.InternalIndicesLifecycle;
import org.elasticsearch.indices.recovery.RecoveryStatus;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.io.PrintStream;
import java.nio.channels.ClosedByInterruptException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.index.mapper.SourceToParse.source;

/**
 *
 */
public class InternalIndexShard extends AbstractIndexShardComponent implements IndexShard {

    private final ThreadPool threadPool;

    private final IndexSettingsService indexSettingsService;

    private final MapperService mapperService;

    private final IndexQueryParserService queryParserService;

    private final IndexCache indexCache;

    private final InternalIndicesLifecycle indicesLifecycle;

    private final Store store;

    private final MergeSchedulerProvider mergeScheduler;

    private final Engine engine;

    private final Translog translog;

    private final IndexAliasesService indexAliasesService;

    private final ShardIndexingService indexingService;

    private final ShardSearchService searchService;

    private final ShardGetService getService;

    private final Object mutex = new Object();


    private final boolean checkIndexOnStartup;

    private long checkIndexTook = 0;

    private volatile IndexShardState state;

    private TimeValue refreshInterval;
    private final TimeValue mergeInterval;

    private volatile ScheduledFuture refreshScheduledFuture;

    private volatile ScheduledFuture mergeScheduleFuture;

    private volatile ShardRouting shardRouting;

    private RecoveryStatus peerRecoveryStatus;

    private ApplyRefreshSettings applyRefreshSettings = new ApplyRefreshSettings();

    private final MeanMetric refreshMetric = new MeanMetric();
    private final MeanMetric flushMetric = new MeanMetric();

    @Inject
    public InternalIndexShard(ShardId shardId, @IndexSettings Settings indexSettings, IndexSettingsService indexSettingsService, IndicesLifecycle indicesLifecycle, Store store, Engine engine, MergeSchedulerProvider mergeScheduler, Translog translog,
                              ThreadPool threadPool, MapperService mapperService, IndexQueryParserService queryParserService, IndexCache indexCache, IndexAliasesService indexAliasesService, ShardIndexingService indexingService, ShardGetService getService, ShardSearchService searchService) {
        super(shardId, indexSettings);
        this.indicesLifecycle = (InternalIndicesLifecycle) indicesLifecycle;
        this.indexSettingsService = indexSettingsService;
        this.store = store;
        this.engine = engine;
        this.mergeScheduler = mergeScheduler;
        this.translog = translog;
        this.threadPool = threadPool;
        this.mapperService = mapperService;
        this.queryParserService = queryParserService;
        this.indexCache = indexCache;
        this.indexAliasesService = indexAliasesService;
        this.indexingService = indexingService;
        this.getService = getService.setIndexShard(this);
        this.searchService = searchService;
        state = IndexShardState.CREATED;

        this.refreshInterval = indexSettings.getAsTime("engine.robin.refresh_interval", indexSettings.getAsTime("index.refresh_interval", engine.defaultRefreshInterval()));
        this.mergeInterval = indexSettings.getAsTime("index.merge.async_interval", TimeValue.timeValueSeconds(1));

        indexSettingsService.addListener(applyRefreshSettings);

        logger.debug("state: [CREATED]");

        this.checkIndexOnStartup = indexSettings.getAsBoolean("index.shard.check_on_startup", false);
    }

    public MergeSchedulerProvider mergeScheduler() {
        return this.mergeScheduler;
    }

    public Store store() {
        return this.store;
    }

    public Engine engine() {
        return engine;
    }

    public Translog translog() {
        return translog;
    }

    public ShardIndexingService indexingService() {
        return this.indexingService;
    }

    @Override
    public ShardGetService getService() {
        return this.getService;
    }

    @Override
    public ShardSearchService searchService() {
        return this.searchService;
    }

    @Override
    public ShardRouting routingEntry() {
        return this.shardRouting;
    }

    public InternalIndexShard routingEntry(ShardRouting shardRouting) {
        ShardRouting currentRouting = this.shardRouting;
        if (!shardRouting.shardId().equals(shardId())) {
            throw new ElasticSearchIllegalArgumentException("Trying to set a routing entry with shardId [" + shardRouting.shardId() + "] on a shard with shardId [" + shardId() + "]");
        }
        if (currentRouting != null) {
            if (!shardRouting.primary() && currentRouting.primary()) {
                logger.warn("suspect illegal state: trying to move shard from primary mode to backup mode");
            }
            // if its the same routing, return
            if (currentRouting.equals(shardRouting)) {
                return this;
            }
        }
        this.shardRouting = shardRouting;
        indicesLifecycle.shardRoutingChanged(this, currentRouting, shardRouting);
        return this;
    }

    /**
     * Marks the shard as recovering, fails with exception is recovering is not allowed to be set.
     */
    public IndexShardState recovering(String reason) throws IndexShardStartedException,
            IndexShardRelocatedException, IndexShardRecoveringException, IndexShardClosedException {
        synchronized (mutex) {
            IndexShardState returnValue = state;
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
            logger.debug("state: [{}]->[{}], reason [{}]", state, IndexShardState.RECOVERING, reason);
            state = IndexShardState.RECOVERING;
            return returnValue;
        }
    }

    public InternalIndexShard relocated(String reason) throws IndexShardNotStartedException {
        synchronized (mutex) {
            if (state != IndexShardState.STARTED) {
                throw new IndexShardNotStartedException(shardId, state);
            }
            logger.debug("state: [{}]->[{}], reason [{}]", state, IndexShardState.RELOCATED, reason);
            state = IndexShardState.RELOCATED;
        }
        return this;
    }

    public InternalIndexShard start(String reason) throws IndexShardStartedException, IndexShardRelocatedException, IndexShardClosedException {
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
            if (checkIndexOnStartup) {
                checkIndex(true);
            }
            engine.start();
            startScheduledTasksIfNeeded();
            logger.debug("state: [{}]->[{}], reason [{}]", state, IndexShardState.STARTED, reason);
            state = IndexShardState.STARTED;
        }
        indicesLifecycle.afterIndexShardStarted(this);
        return this;
    }

    @Override
    public IndexShardState state() {
        return state;
    }

    @Override
    public Engine.Create prepareCreate(SourceToParse source) throws ElasticSearchException {
        long startTime = System.nanoTime();
        DocumentMapper docMapper = mapperService.documentMapperWithAutoCreate(source.type());
        ParsedDocument doc = docMapper.parse(source);
        return new Engine.Create(docMapper, docMapper.uidMapper().term(doc.uid()), doc).startTime(startTime);
    }

    @Override
    public ParsedDocument create(Engine.Create create) throws ElasticSearchException {
        writeAllowed();
        create = indexingService.preCreate(create);
        if (logger.isTraceEnabled()) {
            logger.trace("index {}", create.docs());
        }
        engine.create(create);
        create.endTime(System.nanoTime());
        indexingService.postCreate(create);
        return create.parsedDoc();
    }

    @Override
    public Engine.Index prepareIndex(SourceToParse source) throws ElasticSearchException {
        long startTime = System.nanoTime();
        DocumentMapper docMapper = mapperService.documentMapperWithAutoCreate(source.type());
        ParsedDocument doc = docMapper.parse(source);
        return new Engine.Index(docMapper, docMapper.uidMapper().term(doc.uid()), doc).startTime(startTime);
    }

    @Override
    public ParsedDocument index(Engine.Index index) throws ElasticSearchException {
        writeAllowed();
        index = indexingService.preIndex(index);
        try {
            if (logger.isTraceEnabled()) {
                logger.trace("index {}", index.docs());
            }
            engine.index(index);
            index.endTime(System.nanoTime());
        } catch (RuntimeException ex) {
            indexingService.failedIndex(index);
            throw ex;
        }
        indexingService.postIndex(index);
        return index.parsedDoc();
    }

    @Override
    public Engine.Delete prepareDelete(String type, String id, long version) throws ElasticSearchException {
        long startTime = System.nanoTime();
        DocumentMapper docMapper = mapperService.documentMapperWithAutoCreate(type);
        return new Engine.Delete(type, id, docMapper.uidMapper().term(type, id)).version(version).startTime(startTime);
    }

    @Override
    public void delete(Engine.Delete delete) throws ElasticSearchException {
        writeAllowed();
        delete = indexingService.preDelete(delete);
        try {
            if (logger.isTraceEnabled()) {
                logger.trace("delete [{}]", delete.uid().text());
            }
            engine.delete(delete);
            delete.endTime(System.nanoTime());
        } catch (RuntimeException ex) {
            indexingService.failedDelete(delete);
            throw ex;
        }
        indexingService.postDelete(delete);
    }

    @Override
    public Engine.DeleteByQuery prepareDeleteByQuery(BytesHolder querySource, @Nullable String[] filteringAliases, String... types) throws ElasticSearchException {
        long startTime = System.nanoTime();
        if (types == null) {
            types = Strings.EMPTY_ARRAY;
        }
        Query query = queryParserService.parse(querySource.bytes(), querySource.offset(), querySource.length()).query();
        query = filterQueryIfNeeded(query, types);

        Filter aliasFilter = indexAliasesService.aliasFilter(filteringAliases);

        return new Engine.DeleteByQuery(query, querySource, filteringAliases, aliasFilter, types).startTime(startTime);
    }

    @Override
    public void deleteByQuery(Engine.DeleteByQuery deleteByQuery) throws ElasticSearchException {
        writeAllowed();
        if (mapperService.hasNested()) {
            // we need to wrap it to delete nested docs as well...
            IncludeAllChildrenQuery nestedQuery = new IncludeAllChildrenQuery(deleteByQuery.query(), indexCache.filter().cache(NonNestedDocsFilter.INSTANCE));
            deleteByQuery = new Engine.DeleteByQuery(nestedQuery, deleteByQuery.source(), deleteByQuery.filteringAliases(), deleteByQuery.aliasFilter(), deleteByQuery.types());
        }
        if (logger.isTraceEnabled()) {
            logger.trace("delete_by_query [{}]", deleteByQuery.query());
        }
        deleteByQuery = indexingService.preDeleteByQuery(deleteByQuery);
        engine.delete(deleteByQuery);
        deleteByQuery.endTime(System.nanoTime());
        indexingService.postDeleteByQuery(deleteByQuery);
    }

    @Override
    public Engine.GetResult get(Engine.Get get) throws ElasticSearchException {
        readAllowed();
        return engine.get(get);
    }

    @Override
    public long count(float minScore, byte[] querySource, @Nullable String[] filteringAliases, String... types) throws ElasticSearchException {
        return count(minScore, querySource, 0, querySource.length, filteringAliases, types);
    }

    @Override
    public long count(float minScore, byte[] querySource, int querySourceOffset, int querySourceLength,
                      @Nullable String[] filteringAliases, String... types) throws ElasticSearchException {
        readAllowed();
        Query query;
        if (querySourceLength == 0) {
            query = Queries.MATCH_ALL_QUERY;
        } else {
            try {
                QueryParseContext.setTypes(types);
                query = queryParserService.parse(querySource, querySourceOffset, querySourceLength).query();
            } finally {
                QueryParseContext.removeTypes();
            }
        }
        // wrap it in filter, cache it, and constant score it
        // Don't cache it, since it might be very different queries each time...
//        query = new ConstantScoreQuery(filterCache.cache(new QueryWrapperFilter(query)));
        query = filterQueryIfNeeded(query, types);
        Filter aliasFilter = indexAliasesService.aliasFilter(filteringAliases);
        Engine.Searcher searcher = engine.searcher();
        try {
            long count = Lucene.count(searcher.searcher(), query, aliasFilter, minScore);
            if (logger.isTraceEnabled()) {
                logger.trace("count of [{}] is [{}]", query, count);
            }
            return count;
        } catch (IOException e) {
            throw new ElasticSearchException("Failed to count query [" + query + "]", e);
        } finally {
            searcher.release();
        }
    }

    @Override
    public void refresh(Engine.Refresh refresh) throws ElasticSearchException {
        verifyStarted();
        if (logger.isTraceEnabled()) {
            logger.trace("refresh with {}", refresh);
        }
        long time = System.nanoTime();
        engine.refresh(refresh);
        refreshMetric.inc(System.nanoTime() - time);
    }

    @Override
    public RefreshStats refreshStats() {
        return new RefreshStats(refreshMetric.count(), TimeUnit.NANOSECONDS.toMillis(refreshMetric.sum()));
    }

    @Override
    public FlushStats flushStats() {
        return new FlushStats(flushMetric.count(), TimeUnit.NANOSECONDS.toMillis(flushMetric.sum()));
    }

    @Override
    public DocsStats docStats() {
        Engine.Searcher searcher = null;
        try {
            searcher = engine.searcher();
            return new DocsStats(searcher.reader().numDocs(), searcher.reader().numDeletedDocs());
        } catch (Exception e) {
            return new DocsStats();
        } finally {
            if (searcher != null) {
                searcher.release();
            }
        }
    }

    @Override
    public IndexingStats indexingStats(String... types) {
        return indexingService.stats(types);
    }

    @Override
    public SearchStats searchStats(String... groups) {
        return searchService.stats(groups);
    }

    @Override
    public GetStats getStats() {
        return getService.stats();
    }

    @Override
    public StoreStats storeStats() {
        try {
            return store.stats();
        } catch (IOException e) {
            return new StoreStats();
        }
    }

    @Override
    public MergeStats mergeStats() {
        return mergeScheduler.stats();
    }

    @Override
    public void flush(Engine.Flush flush) throws ElasticSearchException {
        // we allows flush while recovering, since we allow for operations to happen
        // while recovering, and we want to keep the translog at bay (up to deletes, which
        // we don't gc).
        verifyStartedOrRecovering();
        if (logger.isTraceEnabled()) {
            logger.trace("flush with {}", flush);
        }
        long time = System.nanoTime();
        engine.flush(flush);
        flushMetric.inc(System.nanoTime() - time);
    }

    @Override
    public void optimize(Engine.Optimize optimize) throws ElasticSearchException {
        verifyStarted();
        if (logger.isTraceEnabled()) {
            logger.trace("optimize with {}", optimize);
        }
        engine.optimize(optimize);
    }

    @Override
    public <T> T snapshot(Engine.SnapshotHandler<T> snapshotHandler) throws EngineException {
        IndexShardState state = this.state; // one time volatile read
        // we allow snapshot on closed index shard, since we want to do one after we close the shard and before we close the engine
        if (state != IndexShardState.STARTED && state != IndexShardState.RELOCATED && state != IndexShardState.CLOSED) {
            throw new IllegalIndexShardStateException(shardId, state, "snapshot is not allowed");
        }
        return engine.snapshot(snapshotHandler);
    }

    @Override
    public void recover(Engine.RecoveryHandler recoveryHandler) throws EngineException {
        verifyStarted();
        engine.recover(recoveryHandler);
    }

    @Override
    public Engine.Searcher searcher() {
        readAllowed();
        return engine.searcher();
    }

    public void close(String reason) {
        synchronized (mutex) {
            indexSettingsService.removeListener(applyRefreshSettings);
            if (state != IndexShardState.CLOSED) {
                if (refreshScheduledFuture != null) {
                    refreshScheduledFuture.cancel(true);
                    refreshScheduledFuture = null;
                }
                if (mergeScheduleFuture != null) {
                    mergeScheduleFuture.cancel(true);
                    mergeScheduleFuture = null;
                }
            }
            logger.debug("state: [{}]->[{}], reason [{}]", state, IndexShardState.CLOSED, reason);
            state = IndexShardState.CLOSED;
        }
    }

    public long checkIndexTook() {
        return this.checkIndexTook;
    }

    /**
     * After the store has been recovered, we need to start the engine in order to apply operations
     */
    public void performRecoveryPrepareForTranslog() throws ElasticSearchException {
        if (state != IndexShardState.RECOVERING) {
            throw new IndexShardNotRecoveringException(shardId, state);
        }
        // also check here, before we apply the translog
        if (checkIndexOnStartup) {
            checkIndex(true);
        }
        // we disable deletes since we allow for operations to be executed against the shard while recovering
        // but we need to make sure we don't loose deletes until we are done recovering
        engine.enableGcDeletes(false);
        engine.start();
    }

    /**
     * The peer recovery status if this shard recovered from a peer shard.
     */
    public RecoveryStatus peerRecoveryStatus() {
        return this.peerRecoveryStatus;
    }

    public void performRecoveryFinalization(boolean withFlush, RecoveryStatus peerRecoveryStatus) throws ElasticSearchException {
        performRecoveryFinalization(withFlush);
        this.peerRecoveryStatus = peerRecoveryStatus;
    }

    public void performRecoveryFinalization(boolean withFlush) throws ElasticSearchException {
        if (withFlush) {
            engine.flush(new Engine.Flush());
        }
        // clear unreferenced files
        translog.clearUnreferenced();
        engine.refresh(new Engine.Refresh(true));
        synchronized (mutex) {
            logger.debug("state: [{}]->[{}], reason [post recovery]", state, IndexShardState.STARTED);
            state = IndexShardState.STARTED;
        }
        startScheduledTasksIfNeeded();
        indicesLifecycle.afterIndexShardStarted(this);
        engine.enableGcDeletes(true);
    }

    public void performRecoveryOperation(Translog.Operation operation) throws ElasticSearchException {
        if (state != IndexShardState.RECOVERING) {
            throw new IndexShardNotRecoveringException(shardId, state);
        }
        try {
            switch (operation.opType()) {
                case CREATE:
                    Translog.Create create = (Translog.Create) operation;
                    engine.create(prepareCreate(source(create.source().bytes(), create.source().offset(), create.source().length()).type(create.type()).id(create.id())
                            .routing(create.routing()).parent(create.parent()).timestamp(create.timestamp()).ttl(create.ttl())).version(create.version())
                            .origin(Engine.Operation.Origin.RECOVERY));
                    break;
                case SAVE:
                    Translog.Index index = (Translog.Index) operation;
                    engine.index(prepareIndex(source(index.source().bytes(), index.source().offset(), index.source().length()).type(index.type()).id(index.id())
                            .routing(index.routing()).parent(index.parent()).timestamp(index.timestamp()).ttl(index.ttl())).version(index.version())
                            .origin(Engine.Operation.Origin.RECOVERY));
                    break;
                case DELETE:
                    Translog.Delete delete = (Translog.Delete) operation;
                    Uid uid = Uid.createUid(delete.uid().text());
                    engine.delete(new Engine.Delete(uid.type(), uid.id(), delete.uid()).version(delete.version())
                            .origin(Engine.Operation.Origin.RECOVERY));
                    break;
                case DELETE_BY_QUERY:
                    Translog.DeleteByQuery deleteByQuery = (Translog.DeleteByQuery) operation;
                    engine.delete(prepareDeleteByQuery(deleteByQuery.source(), deleteByQuery.filteringAliases(), deleteByQuery.types()));
                    break;
                default:
                    throw new ElasticSearchIllegalStateException("No operation defined for [" + operation + "]");
            }
        } catch (ElasticSearchException e) {
            boolean hasIgnoreOnRecoveryException = false;
            ElasticSearchException current = e;
            while (true) {
                if (current instanceof IgnoreOnRecoveryEngineException) {
                    hasIgnoreOnRecoveryException = true;
                    break;
                }
                if (current.getCause() instanceof ElasticSearchException) {
                    current = (ElasticSearchException) current.getCause();
                } else {
                    break;
                }
            }
            if (!hasIgnoreOnRecoveryException) {
                throw e;
            }
        }
    }

    /**
     * Returns <tt>true</tt> if this shard can ignore a recovery attempt made to it (since the already doing/done it)
     */
    public boolean ignoreRecoveryAttempt() {
        IndexShardState state = state(); // one time volatile read
        return state == IndexShardState.RECOVERING || state == IndexShardState.STARTED ||
                state == IndexShardState.RELOCATED || state == IndexShardState.CLOSED;
    }

    public void readAllowed() throws IllegalIndexShardStateException {
        IndexShardState state = this.state; // one time volatile read
        if (state != IndexShardState.STARTED && state != IndexShardState.RELOCATED) {
            throw new IllegalIndexShardStateException(shardId, state, "Read operations only allowed when started/relocated");
        }
    }

    private void writeAllowed() throws IllegalIndexShardStateException {
        verifyStartedOrRecovering();
    }

    private void verifyStartedOrRecovering() throws IllegalIndexShardStateException {
        IndexShardState state = this.state; // one time volatile read
        if (state != IndexShardState.STARTED && state != IndexShardState.RECOVERING) {
            throw new IllegalIndexShardStateException(shardId, state, "write operation only allowed when started/recovering");
        }
    }

    private void verifyStarted() throws IllegalIndexShardStateException {
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
        // since we can do async merging, it will not be called explicitly when indexing (adding / deleting docs), and only when flushing
        // so, make sure we periodically call it, this need to be a small enough value so mergine will actually
        // happen and reduce the number of segments
        if (mergeInterval.millis() > 0) {
            mergeScheduleFuture = threadPool.schedule(mergeInterval, ThreadPool.Names.SAME, new EngineMerger());
            logger.debug("scheduling optimizer / merger every {}", mergeInterval);
        } else {
            logger.debug("scheduled optimizer / merger disabled");
        }
    }

    private Query filterQueryIfNeeded(Query query, String[] types) {
        Filter searchFilter = mapperService.searchFilter(types);
        if (searchFilter != null) {
            query = new FilteredQuery(query, indexCache.filter().cache(searchFilter));
        }
        return query;
    }

    static {
        IndexMetaData.addDynamicSettings("index.refresh_interval");
    }

    private class ApplyRefreshSettings implements IndexSettingsService.Listener {
        @Override
        public void onRefreshSettings(Settings settings) {
            synchronized (mutex) {
                if (state == IndexShardState.CLOSED) {
                    return;
                }
                TimeValue refreshInterval = settings.getAsTime("engine.robin.refresh_interval", settings.getAsTime("index.refresh_interval", InternalIndexShard.this.refreshInterval));
                if (!refreshInterval.equals(InternalIndexShard.this.refreshInterval)) {
                    logger.info("updating refresh_interval from [{}] to [{}]", InternalIndexShard.this.refreshInterval, refreshInterval);
                    if (refreshScheduledFuture != null) {
                        refreshScheduledFuture.cancel(false);
                        refreshScheduledFuture = null;
                    }
                    InternalIndexShard.this.refreshInterval = refreshInterval;
                    if (refreshInterval.millis() > 0) {
                        refreshScheduledFuture = threadPool.schedule(refreshInterval, ThreadPool.Names.SAME, new EngineRefresher());
                    }
                }
            }
        }
    }

    private class EngineRefresher implements Runnable {
        @Override
        public void run() {
            // we check before if a refresh is needed, if not, we reschedule, otherwise, we fork, refresh, and then reschedule
            if (!engine().refreshNeeded()) {
                synchronized (mutex) {
                    if (state != IndexShardState.CLOSED) {
                        refreshScheduledFuture = threadPool.schedule(refreshInterval, ThreadPool.Names.SAME, this);
                    }
                }
                return;
            }
            threadPool.executor(ThreadPool.Names.REFRESH).execute(new Runnable() {
                @Override
                public void run() {
                    try {
                        if (engine.refreshNeeded()) {
                            refresh(new Engine.Refresh(false));
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
                    synchronized (mutex) {
                        if (state != IndexShardState.CLOSED) {
                            refreshScheduledFuture = threadPool.schedule(refreshInterval, ThreadPool.Names.SAME, EngineRefresher.this);
                        }
                    }
                }
            });
        }
    }

    private class EngineMerger implements Runnable {
        @Override
        public void run() {
            if (!engine().possibleMergeNeeded()) {
                synchronized (mutex) {
                    if (state != IndexShardState.CLOSED) {
                        mergeScheduleFuture = threadPool.schedule(mergeInterval, ThreadPool.Names.SAME, this);
                    }
                }
                return;
            }
            threadPool.executor(ThreadPool.Names.MERGE).execute(new Runnable() {
                @Override
                public void run() {
                    try {
                        engine.maybeMerge();
                    } catch (EngineClosedException e) {
                        // we are being closed, ignore
                    } catch (OptimizeFailedEngineException e) {
                        if (e.getCause() instanceof EngineClosedException) {
                            // ignore, we are being shutdown
                        } else if (e.getCause() instanceof InterruptedException) {
                            // ignore, we are being shutdown
                        } else if (e.getCause() instanceof ClosedByInterruptException) {
                            // ignore, we are being shutdown
                        } else if (e.getCause() instanceof ThreadInterruptedException) {
                            // ignore, we are being shutdown
                        } else {
                            if (state != IndexShardState.CLOSED) {
                                logger.warn("Failed to perform scheduled engine optimize/merge", e);
                            }
                        }
                    } catch (Exception e) {
                        if (state != IndexShardState.CLOSED) {
                            logger.warn("Failed to perform scheduled engine optimize/merge", e);
                        }
                    }
                    synchronized (mutex) {
                        if (state != IndexShardState.CLOSED) {
                            mergeScheduleFuture = threadPool.schedule(mergeInterval, ThreadPool.Names.SAME, EngineMerger.this);
                        }
                    }
                }
            });
        }
    }

    private void checkIndex(boolean throwException) throws IndexShardException {
        try {
            checkIndexTook = 0;
            long time = System.currentTimeMillis();
            if (!IndexReader.indexExists(store.directory())) {
                return;
            }
            CheckIndex checkIndex = new CheckIndex(store.directory());
            FastByteArrayOutputStream os = new FastByteArrayOutputStream();
            PrintStream out = new PrintStream(os);
            checkIndex.setInfoStream(out);
            out.flush();
            CheckIndex.Status status = checkIndex.checkIndex();
            if (!status.clean) {
                if (state == IndexShardState.CLOSED) {
                    // ignore if closed....
                    return;
                }
                logger.warn("check index [failure]\n{}", new String(os.underlyingBytes(), 0, os.size()));
                if (throwException) {
                    throw new IndexShardException(shardId, "index check failure");
                }
            } else {
                if (logger.isDebugEnabled()) {
                    logger.debug("check index [success]\n{}", new String(os.underlyingBytes(), 0, os.size()));
                }
            }
            checkIndexTook = System.currentTimeMillis() - time;
        } catch (Exception e) {
            logger.warn("failed to check index", e);
        }
    }
}