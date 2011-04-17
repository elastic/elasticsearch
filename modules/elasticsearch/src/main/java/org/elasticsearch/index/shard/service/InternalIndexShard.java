/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
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

import org.apache.lucene.document.Document;
import org.apache.lucene.index.CheckIndex;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.FilteredQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.ThreadInterruptedException;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.ElasticSearchIllegalArgumentException;
import org.elasticsearch.ElasticSearchIllegalStateException;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.FastByteArrayOutputStream;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.ThreadSafe;
import org.elasticsearch.index.cache.IndexCache;
import org.elasticsearch.index.engine.*;
import org.elasticsearch.index.mapper.*;
import org.elasticsearch.index.merge.scheduler.MergeSchedulerProvider;
import org.elasticsearch.index.query.IndexQueryParser;
import org.elasticsearch.index.query.IndexQueryParserMissingException;
import org.elasticsearch.index.query.IndexQueryParserService;
import org.elasticsearch.index.refresh.RefreshStats;
import org.elasticsearch.index.settings.IndexSettings;
import org.elasticsearch.index.settings.IndexSettingsService;
import org.elasticsearch.index.shard.*;
import org.elasticsearch.index.shard.recovery.RecoveryStatus;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.indices.IndicesLifecycle;
import org.elasticsearch.indices.InternalIndicesLifecycle;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.io.PrintStream;
import java.nio.channels.ClosedByInterruptException;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicLong;

import static org.elasticsearch.index.mapper.SourceToParse.*;

/**
 * @author kimchy (shay.banon)
 */
@ThreadSafe
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

    private final Object mutex = new Object();


    private final boolean checkIndex;

    private volatile IndexShardState state;

    private TimeValue refreshInterval;
    private final TimeValue mergeInterval;

    private volatile ScheduledFuture refreshScheduledFuture;

    private volatile ScheduledFuture mergeScheduleFuture;

    private volatile ShardRouting shardRouting;

    private RecoveryStatus peerRecoveryStatus;

    private CopyOnWriteArrayList<OperationListener> listeners = null;

    private ApplyRefreshSettings applyRefreshSettings = new ApplyRefreshSettings();

    private final AtomicLong totalRefresh = new AtomicLong();
    private final AtomicLong totalRefreshTime = new AtomicLong();

    @Inject public InternalIndexShard(ShardId shardId, @IndexSettings Settings indexSettings, IndexSettingsService indexSettingsService, IndicesLifecycle indicesLifecycle, Store store, Engine engine, MergeSchedulerProvider mergeScheduler, Translog translog,
                                      ThreadPool threadPool, MapperService mapperService, IndexQueryParserService queryParserService, IndexCache indexCache) {
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
        state = IndexShardState.CREATED;

        this.refreshInterval = indexSettings.getAsTime("engine.robin.refresh_interval", indexSettings.getAsTime("index.refresh_interval", engine.defaultRefreshInterval()));
        this.mergeInterval = indexSettings.getAsTime("index.merge.async_interval", TimeValue.timeValueSeconds(1));

        indexSettingsService.addListener(applyRefreshSettings);

        logger.debug("state: [CREATED]");

        this.checkIndex = indexSettings.getAsBoolean("index.shard.check_index", false);
    }

    @Override public synchronized void addListener(OperationListener listener) {
        if (listeners == null) {
            listeners = new CopyOnWriteArrayList<OperationListener>();
        }
        listeners.add(listener);
    }

    @Override public synchronized void removeListener(OperationListener listener) {
        if (listeners == null) {
            return;
        }
        listeners.remove(listener);
        if (listeners.isEmpty()) {
            listeners = null;
        }
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

    @Override public ShardRouting routingEntry() {
        return this.shardRouting;
    }

    public InternalIndexShard routingEntry(ShardRouting shardRouting) {
        if (!shardRouting.shardId().equals(shardId())) {
            throw new ElasticSearchIllegalArgumentException("Trying to set a routing entry with shardId [" + shardRouting.shardId() + "] on a shard with shardId [" + shardId() + "]");
        }
        if (this.shardRouting != null) {
            if (!shardRouting.primary() && this.shardRouting.primary()) {
                logger.warn("suspect illegal state: trying to move shard from primary mode to backup mode");
            }
        }
        this.shardRouting = shardRouting;
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
            if (checkIndex) {
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

    @Override public IndexShardState state() {
        return state;
    }

    @Override public Engine.Create prepareCreate(SourceToParse source) throws ElasticSearchException {
        DocumentMapper docMapper = mapperService.documentMapperWithAutoCreate(source.type());
        ParsedDocument doc = docMapper.parse(source);
        return new Engine.Create(docMapper, docMapper.uidMapper().term(doc.uid()), doc);
    }

    @Override public ParsedDocument create(Engine.Create create) throws ElasticSearchException {
        writeAllowed();
        if (listeners != null) {
            for (OperationListener listener : listeners) {
                create = listener.beforeCreate(create);
            }
        }
        if (logger.isTraceEnabled()) {
            logger.trace("index {}", create.doc());
        }
        engine.create(create);
        return create.parsedDoc();
    }

    @Override public Engine.Index prepareIndex(SourceToParse source) throws ElasticSearchException {
        DocumentMapper docMapper = mapperService.documentMapperWithAutoCreate(source.type());
        ParsedDocument doc = docMapper.parse(source);
        return new Engine.Index(docMapper, docMapper.uidMapper().term(doc.uid()), doc);
    }

    @Override public ParsedDocument index(Engine.Index index) throws ElasticSearchException {
        writeAllowed();
        if (listeners != null) {
            for (OperationListener listener : listeners) {
                index = listener.beforeIndex(index);
            }
        }
        if (logger.isTraceEnabled()) {
            logger.trace("index {}", index.doc());
        }
        engine.index(index);
        return index.parsedDoc();
    }

    @Override public Engine.Delete prepareDelete(String type, String id, long version) throws ElasticSearchException {
        DocumentMapper docMapper = mapperService.documentMapperWithAutoCreate(type);
        return new Engine.Delete(type, id, docMapper.uidMapper().term(type, id)).version(version);
    }

    @Override public void delete(Engine.Delete delete) throws ElasticSearchException {
        writeAllowed();
        if (listeners != null) {
            for (OperationListener listener : listeners) {
                delete = listener.beforeDelete(delete);
            }
        }
        if (logger.isTraceEnabled()) {
            logger.trace("delete [{}]", delete.uid().text());
        }
        engine.delete(delete);
    }

    @Override public void deleteByQuery(byte[] querySource, @Nullable String queryParserName, String... types) throws ElasticSearchException {
        writeAllowed();
        if (types == null) {
            types = Strings.EMPTY_ARRAY;
        }
        innerDeleteByQuery(querySource, queryParserName, types);
    }

    private void innerDeleteByQuery(byte[] querySource, String queryParserName, String... types) {
        IndexQueryParser queryParser = queryParserService.defaultIndexQueryParser();
        if (queryParserName != null) {
            queryParser = queryParserService.indexQueryParser(queryParserName);
            if (queryParser == null) {
                throw new IndexQueryParserMissingException(queryParserName);
            }
        }
        Query query = queryParser.parse(querySource).query();
        query = filterByTypesIfNeeded(query, types);

        if (logger.isTraceEnabled()) {
            logger.trace("delete_by_query [{}]", query);
        }

        engine.delete(new Engine.DeleteByQuery(query, querySource, queryParserName, types));
    }

    @Override public byte[] get(String type, String id) throws ElasticSearchException {
        readAllowed();
        DocumentMapper docMapper = mapperService.documentMapperWithAutoCreate(type);
        Engine.Searcher searcher = engine.searcher();
        try {
            int docId = Lucene.docId(searcher.reader(), docMapper.uidMapper().term(type, id));
            if (docId == Lucene.NO_DOC) {
                if (logger.isTraceEnabled()) {
                    logger.trace("get for [{}#{}] returned no result", type, id);
                }
                return null;
            }
            Document doc = searcher.reader().document(docId, docMapper.sourceMapper().fieldSelector());
            if (logger.isTraceEnabled()) {
                logger.trace("get for [{}#{}] returned [{}]", type, id, doc);
            }
            return docMapper.sourceMapper().value(doc);
        } catch (IOException e) {
            throw new ElasticSearchException("Failed to get type [" + type + "] and id [" + id + "]", e);
        } finally {
            searcher.release();
        }
    }

    @Override public long count(float minScore, byte[] querySource, @Nullable String queryParserName, String... types) throws ElasticSearchException {
        return count(minScore, querySource, 0, querySource.length, queryParserName, types);
    }

    @Override public long count(float minScore, byte[] querySource, int querySourceOffset, int querySourceLength,
                                @Nullable String queryParserName, String... types) throws ElasticSearchException {
        readAllowed();
        IndexQueryParser queryParser = queryParserService.defaultIndexQueryParser();
        if (queryParserName != null) {
            queryParser = queryParserService.indexQueryParser(queryParserName);
            if (queryParser == null) {
                throw new IndexQueryParserMissingException(queryParserName);
            }
        }
        Query query = queryParser.parse(querySource, querySourceOffset, querySourceLength).query();
        // wrap it in filter, cache it, and constant score it
        // Don't cache it, since it might be very different queries each time...
//        query = new ConstantScoreQuery(filterCache.cache(new QueryWrapperFilter(query)));
        query = filterByTypesIfNeeded(query, types);
        Engine.Searcher searcher = engine.searcher();
        try {
            long count = Lucene.count(searcher.searcher(), query, minScore);
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

    @Override public void refresh(Engine.Refresh refresh) throws ElasticSearchException {
        writeAllowed();
        if (logger.isTraceEnabled()) {
            logger.trace("refresh with {}", refresh);
        }
        long time = System.currentTimeMillis();
        engine.refresh(refresh);
        totalRefresh.incrementAndGet();
        totalRefreshTime.addAndGet(System.currentTimeMillis() - time);
    }

    @Override public RefreshStats refreshStats() {
        return new RefreshStats(totalRefresh.get(), totalRefreshTime.get());
    }

    @Override public void flush(Engine.Flush flush) throws ElasticSearchException {
        writeAllowed();
        if (logger.isTraceEnabled()) {
            logger.trace("flush with {}", flush);
        }
        engine.flush(flush);
    }

    @Override public void optimize(Engine.Optimize optimize) throws ElasticSearchException {
        writeAllowed();
        if (logger.isTraceEnabled()) {
            logger.trace("optimize with {}", optimize);
        }
        engine.optimize(optimize);
    }

    @Override public <T> T snapshot(Engine.SnapshotHandler<T> snapshotHandler) throws EngineException {
        IndexShardState state = this.state; // one time volatile read
        // we allow snapshot on closed index shard, since we want to do one after we close the shard and before we close the engine
        if (state != IndexShardState.STARTED && state != IndexShardState.RELOCATED && state != IndexShardState.CLOSED) {
            throw new IllegalIndexShardStateException(shardId, state, "snapshot is not allowed");
        }
        return engine.snapshot(snapshotHandler);
    }

    @Override public void recover(Engine.RecoveryHandler recoveryHandler) throws EngineException {
        writeAllowed();
        engine.recover(recoveryHandler);
    }

    @Override public Engine.Searcher searcher() {
        readAllowed();
        return engine.searcher();
    }

    public void close(String reason) {
        synchronized (mutex) {
            if (listeners != null) {
                listeners.clear();
            }
            listeners = null;
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

    /**
     * After the store has been recovered, we need to start the engine in order to apply operations
     */
    public void performRecoveryPrepareForTranslog() throws ElasticSearchException {
        if (state != IndexShardState.RECOVERING) {
            throw new IndexShardNotRecoveringException(shardId, state);
        }
        // also check here, before we apply the translog
        if (checkIndex) {
            checkIndex(true);
        }
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
        synchronized (mutex) {
            logger.debug("state: [{}]->[{}], reason [post recovery]", state, IndexShardState.STARTED);
            state = IndexShardState.STARTED;
        }
        startScheduledTasksIfNeeded();
        engine.refresh(new Engine.Refresh(true));

        // clear unreferenced files
        translog.clearUnreferenced();
        indicesLifecycle.afterIndexShardStarted(this);
    }

    public void performRecoveryOperation(Translog.Operation operation) throws ElasticSearchException {
        if (state != IndexShardState.RECOVERING) {
            throw new IndexShardNotRecoveringException(shardId, state);
        }
        switch (operation.opType()) {
            case CREATE:
                Translog.Create create = (Translog.Create) operation;
                engine.create(prepareCreate(source(create.source()).type(create.type()).id(create.id())
                        .routing(create.routing()).parent(create.parent())).version(create.version())
                        .origin(Engine.Operation.Origin.RECOVERY));
                break;
            case SAVE:
                Translog.Index index = (Translog.Index) operation;
                engine.index(prepareIndex(source(index.source()).type(index.type()).id(index.id())
                        .routing(index.routing()).parent(index.parent())).version(index.version())
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
                innerDeleteByQuery(deleteByQuery.source(), deleteByQuery.queryParserName(), deleteByQuery.types());
                break;
            default:
                throw new ElasticSearchIllegalStateException("No operation defined for [" + operation + "]");
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

    public void writeAllowed() throws IllegalIndexShardStateException {
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

    private Query filterByTypesIfNeeded(Query query, String[] types) {
        if (types != null && types.length > 0) {
            query = new FilteredQuery(query, indexCache.filter().cache(mapperService.typesFilter(types)));
        }
        return query;
    }

    private class ApplyRefreshSettings implements IndexSettingsService.Listener {
        @Override public void onRefreshSettings(Settings settings) {
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
        @Override public void run() {
            // we check before if a refresh is needed, if not, we reschedule, otherwise, we fork, refresh, and then reschedule
            if (!engine().refreshNeeded()) {
                synchronized (mutex) {
                    if (state != IndexShardState.CLOSED) {
                        refreshScheduledFuture = threadPool.schedule(refreshInterval, ThreadPool.Names.SAME, this);
                    }
                }
                return;
            }
            threadPool.cached().execute(new Runnable() {
                @Override public void run() {
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
                            logger.warn("Failed to perform scheduled engine refresh", e);
                        }
                    } catch (Exception e) {
                        logger.warn("Failed to perform scheduled engine refresh", e);
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
        @Override public void run() {
            if (!engine().possibleMergeNeeded()) {
                synchronized (mutex) {
                    if (state != IndexShardState.CLOSED) {
                        mergeScheduleFuture = threadPool.schedule(mergeInterval, ThreadPool.Names.SAME, this);
                    }
                }
                return;
            }
            threadPool.executor(ThreadPool.Names.MERGE).execute(new Runnable() {
                @Override public void run() {
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
                            logger.warn("Failed to perform scheduled engine optimize/merge", e);
                        }
                    } catch (Exception e) {
                        logger.warn("Failed to perform scheduled engine optimize/merge", e);
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
                logger.warn("check index [failure]\n{}", new String(os.unsafeByteArray(), 0, os.size()));
                if (throwException) {
                    throw new IndexShardException(shardId, "index check failure");
                }
            } else {
                if (logger.isDebugEnabled()) {
                    logger.debug("check index [success]\n{}", new String(os.unsafeByteArray(), 0, os.size()));
                }
            }
        } catch (Exception e) {
            logger.warn("failed to check index", e);
        }
    }
}