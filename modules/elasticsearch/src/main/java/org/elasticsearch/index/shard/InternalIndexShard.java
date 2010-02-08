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

package org.elasticsearch.index.shard;

import com.google.inject.Inject;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.*;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.ElasticSearchIllegalArgumentException;
import org.elasticsearch.ElasticSearchIllegalStateException;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.index.cache.filter.FilterCache;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.EngineException;
import org.elasticsearch.index.engine.ScheduledRefreshableEngine;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.DocumentMapperNotFoundException;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.query.IndexQueryParser;
import org.elasticsearch.index.query.IndexQueryParserMissingException;
import org.elasticsearch.index.query.IndexQueryParserService;
import org.elasticsearch.index.settings.IndexSettings;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.indices.TypeMissingException;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.util.Nullable;
import org.elasticsearch.util.SizeValue;
import org.elasticsearch.util.Strings;
import org.elasticsearch.util.TimeValue;
import org.elasticsearch.util.concurrent.ThreadSafe;
import org.elasticsearch.util.lucene.Lucene;
import org.elasticsearch.util.lucene.search.TermFilter;
import org.elasticsearch.util.settings.Settings;

import java.io.IOException;
import java.util.concurrent.ScheduledFuture;

/**
 * @author kimchy (Shay Banon)
 */
@IndexShardLifecycle
@ThreadSafe
public class InternalIndexShard extends AbstractIndexShardComponent implements IndexShard {

    private final ThreadPool threadPool;

    private final MapperService mapperService;

    private final IndexQueryParserService queryParserService;

    private final FilterCache filterCache;

    private final Store store;

    private final Engine engine;

    private final Translog translog;

    private final Object mutex = new Object();

    private volatile IndexShardState state;

    private ScheduledFuture refreshScheduledFuture;

    private volatile ShardRouting shardRouting;

    @Inject public InternalIndexShard(ShardId shardId, @IndexSettings Settings indexSettings, Store store, Engine engine, Translog translog,
                                      ThreadPool threadPool, MapperService mapperService, IndexQueryParserService queryParserService, FilterCache filterCache) {
        super(shardId, indexSettings);
        this.store = store;
        this.engine = engine;
        this.translog = translog;
        this.threadPool = threadPool;
        this.mapperService = mapperService;
        this.queryParserService = queryParserService;
        this.filterCache = filterCache;
        state = IndexShardState.CREATED;
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

    public ShardRouting routingEntry() {
        return this.shardRouting;
    }

    public InternalIndexShard routingEntry(ShardRouting shardRouting) {
        if (!shardRouting.shardId().equals(shardId())) {
            throw new ElasticSearchIllegalArgumentException("Trying to set a routing entry with shardId [" + shardRouting.shardId() + "] on a shard with shardId [" + shardId() + "]");
        }
        if (this.shardRouting != null) {
            if (!shardRouting.primary() && this.shardRouting.primary()) {
                logger.warn("Suspect illegal state: Trying to move shard from primary mode to backup mode");
            }
        }
        this.shardRouting = shardRouting;
        return this;
    }

    public IndexShardState recovering() throws IndexShardStartedException,
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
            state = IndexShardState.RECOVERING;
            return returnValue;
        }
    }

    public InternalIndexShard restoreRecoveryState(IndexShardState stateToRestore) {
        synchronized (mutex) {
            if (this.state != IndexShardState.RECOVERING) {
                throw new IndexShardNotRecoveringException(shardId, state);
            }
            this.state = stateToRestore;
        }
        return this;
    }

    public InternalIndexShard relocated() throws IndexShardNotStartedException {
        synchronized (mutex) {
            if (state != IndexShardState.STARTED) {
                throw new IndexShardNotStartedException(shardId, state);
            }
            state = IndexShardState.RELOCATED;
        }
        return this;
    }

    public InternalIndexShard start() throws IndexShardStartedException, IndexShardRelocatedException, IndexShardClosedException {
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
            engine.start();
            scheduleRefresherIfNeeded();
            state = IndexShardState.STARTED;
        }
        return this;
    }

    public IndexShardState state() {
        return state;
    }

    /**
     * Returns the estimated flushable memory size. Returns <tt>null</tt> if not available.
     */
    public SizeValue estimateFlushableMemorySize() throws ElasticSearchException {
        writeAllowed();
        return engine.estimateFlushableMemorySize();
    }

    public void create(String type, String id, String source) throws ElasticSearchException {
        writeAllowed();
        innerCreate(type, id, source);
    }

    private void innerCreate(String type, String id, String source) {
        DocumentMapper docMapper = mapperService.type(type);
        if (docMapper == null) {
            throw new DocumentMapperNotFoundException("No mapper found for type [" + type + "]");
        }
        ParsedDocument doc = docMapper.parse(type, id, source);
        if (logger.isTraceEnabled()) {
            logger.trace("Indexing {}", doc);
        }
        engine.create(new Engine.Create(doc.doc(), docMapper.mappers().indexAnalyzer(), docMapper.type(), doc.id(), doc.source()));
    }

    public void index(String type, String id, String source) throws ElasticSearchException {
        writeAllowed();
        innerIndex(type, id, source);
    }

    private void innerIndex(String type, String id, String source) {
        DocumentMapper docMapper = mapperService.type(type);
        if (docMapper == null) {
            throw new DocumentMapperNotFoundException("No mapper found for type [" + type + "]");
        }
        ParsedDocument doc = docMapper.parse(type, id, source);
        if (logger.isTraceEnabled()) {
            logger.trace("Indexing {}", doc);
        }
        engine.index(new Engine.Index(docMapper.uidMapper().term(doc.uid()), doc.doc(), docMapper.mappers().indexAnalyzer(), docMapper.type(), doc.id(), doc.source()));
    }

    public void delete(String type, String id) {
        writeAllowed();
        DocumentMapper docMapper = mapperService.type(type);
        if (docMapper == null) {
            throw new DocumentMapperNotFoundException("No mapper found for type [" + type + "]");
        }
        innerDelete(docMapper.uidMapper().term(type, id));
    }

    public void delete(Term uid) {
        writeAllowed();
        innerDelete(uid);
    }

    private void innerDelete(Term uid) {
        if (logger.isTraceEnabled()) {
            logger.trace("Deleting [{}]", uid.text());
        }
        engine.delete(new Engine.Delete(uid));
    }

    public void deleteByQuery(String querySource, @Nullable String queryParserName, String... types) throws ElasticSearchException {
        writeAllowed();
        if (types == null) {
            types = Strings.EMPTY_ARRAY;
        }
        innerDeleteByQuery(querySource, queryParserName, types);
    }

    private void innerDeleteByQuery(String querySource, String queryParserName, String... types) {
        IndexQueryParser queryParser = queryParserService.defaultIndexQueryParser();
        if (queryParserName != null) {
            queryParser = queryParserService.indexQueryParser(queryParserName);
            if (queryParser == null) {
                throw new IndexQueryParserMissingException(queryParserName);
            }
        }
        Query query = queryParser.parse(querySource);
        query = filterByTypesIfNeeded(query, types);

        if (logger.isTraceEnabled()) {
            logger.trace("Deleting By Query [{}]", query);
        }

        engine.delete(new Engine.DeleteByQuery(query, querySource, queryParserName, types));
    }

    public String get(String type, String id) throws ElasticSearchException {
        readAllowed();
        DocumentMapper docMapper = mapperService.type(type);
        if (docMapper == null) {
            throw new DocumentMapperNotFoundException("No mapper found for type [" + type + "]");
        }
        Engine.Searcher searcher = engine.searcher();
        try {
            int docId = Lucene.docId(searcher.reader(), docMapper.uidMapper().term(type, id));
            if (docId == Lucene.NO_DOC) {
                if (logger.isTraceEnabled()) {
                    logger.trace("Get for [{}#{}] returned no result", type, id);
                }
                return null;
            }
            Document doc = searcher.reader().document(docId, docMapper.sourceMapper().fieldSelector());
            if (logger.isTraceEnabled()) {
                logger.trace("Get for [{}#{}] returned [{}]", new Object[]{type, id, doc});
            }
            return docMapper.sourceMapper().value(doc);
        } catch (IOException e) {
            throw new ElasticSearchException("Failed to get type [" + type + "] and id [" + id + "]", e);
        } finally {
            searcher.release();
        }
    }

    public long count(float minScore, String querySource, @Nullable String queryParserName, String... types) throws ElasticSearchException {
        readAllowed();
        IndexQueryParser queryParser = queryParserService.defaultIndexQueryParser();
        if (queryParserName != null) {
            queryParser = queryParserService.indexQueryParser(queryParserName);
            if (queryParser == null) {
                throw new IndexQueryParserMissingException(queryParserName);
            }
        }
        Query query = queryParser.parse(querySource);
        query = filterByTypesIfNeeded(query, types);
        Engine.Searcher searcher = engine.searcher();
        try {
            long count = Lucene.count(searcher.searcher(), query, minScore);
            if (logger.isTraceEnabled()) {
                logger.trace("Count of [{}] is [{}]", query, count);
            }
            return count;
        } catch (IOException e) {
            throw new ElasticSearchException("Failed to count query [" + query + "]", e);
        } finally {
            searcher.release();
        }
    }

    public void refresh(boolean waitForOperations) throws ElasticSearchException {
        writeAllowed();
        if (logger.isTraceEnabled()) {
            logger.trace("Refresh, waitForOperations[{}]", waitForOperations);
        }
        engine.refresh(waitForOperations);
    }

    public void flush() throws ElasticSearchException {
        writeAllowed();
        if (logger.isTraceEnabled()) {
            logger.trace("Flush");
        }
        engine.flush();
    }

    public void snapshot(Engine.SnapshotHandler snapshotHandler) throws EngineException {
        readAllowed();
        engine.snapshot(snapshotHandler);
    }

    public void recover(Engine.RecoveryHandler recoveryHandler) throws EngineException {
        writeAllowed();
        engine.recover(recoveryHandler);
    }

    public Engine.Searcher searcher() {
        readAllowed();
        return engine.searcher();
    }

    public void close() {
        synchronized (mutex) {
            if (state != IndexShardState.CLOSED) {
                if (refreshScheduledFuture != null) {
                    refreshScheduledFuture.cancel(true);
                    refreshScheduledFuture = null;
                }
            }
            state = IndexShardState.CLOSED;
        }
    }

    public void performRecovery(Iterable<Translog.Operation> operations) throws ElasticSearchException {
        if (state != IndexShardState.RECOVERING) {
            throw new IndexShardNotRecoveringException(shardId, state);
        }
        engine.start();
        applyTranslogOperations(operations);
        synchronized (mutex) {
            state = IndexShardState.STARTED;
        }
        scheduleRefresherIfNeeded();
    }

    public void performRecovery(Translog.Snapshot snapshot, boolean phase3) throws ElasticSearchException {
        if (state != IndexShardState.RECOVERING) {
            throw new IndexShardNotRecoveringException(shardId, state);
        }
        if (!phase3) {
            // start the engine, but the shard is not started yet...
            engine.start();
        }
        applyTranslogOperations(snapshot);
        if (phase3) {
            synchronized (mutex) {
                state = IndexShardState.STARTED;
            }
            scheduleRefresherIfNeeded();
        }
    }

    private void applyTranslogOperations(Iterable<Translog.Operation> snapshot) {
        for (Translog.Operation operation : snapshot) {
            switch (operation.opType()) {
                case CREATE:
                    Translog.Create create = (Translog.Create) operation;
                    innerCreate(create.type(), create.id(), create.source());
                    break;
                case SAVE:
                    Translog.Index index = (Translog.Index) operation;
                    innerIndex(index.type(), index.id(), index.source());
                    break;
                case DELETE:
                    Translog.Delete delete = (Translog.Delete) operation;
                    innerDelete(delete.uid());
                    break;
                case DELETE_BY_QUERY:
                    Translog.DeleteByQuery deleteByQuery = (Translog.DeleteByQuery) operation;
                    innerDeleteByQuery(deleteByQuery.source(), deleteByQuery.queryParserName(), deleteByQuery.types());
                    break;
                default:
                    throw new ElasticSearchIllegalStateException("No operation defined for [" + operation + "]");
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

    public void writeAllowed() throws IllegalIndexShardStateException {
        IndexShardState state = this.state; // one time volatile read
        if (state != IndexShardState.STARTED) {
            throw new IndexShardNotStartedException(shardId, state);
        }
    }

    private void scheduleRefresherIfNeeded() {
        if (engine instanceof ScheduledRefreshableEngine) {
            TimeValue refreshInterval = ((ScheduledRefreshableEngine) engine).refreshInterval();
            if (refreshInterval.millis() > 0) {
                refreshScheduledFuture = threadPool.scheduleWithFixedDelay(new EngineRefresher(), refreshInterval);
                logger.debug("Scheduling refresher every {}", refreshInterval);
            }
        }
    }

    private Query filterByTypesIfNeeded(Query query, String[] types) {
        if (types != null && types.length > 0) {
            if (types.length == 1) {
                String type = types[0];
                DocumentMapper docMapper = mapperService.documentMapper(type);
                if (docMapper == null) {
                    throw new TypeMissingException(shardId.index(), type);
                }
                Filter typeFilter = new TermFilter(docMapper.typeMapper().term(docMapper.type()));
                typeFilter = filterCache.cache(typeFilter);
                query = new FilteredQuery(query, typeFilter);
            } else {
                BooleanFilter booleanFilter = new BooleanFilter();
                for (String type : types) {
                    DocumentMapper docMapper = mapperService.documentMapper(type);
                    if (docMapper == null) {
                        throw new TypeMissingException(shardId.index(), type);
                    }
                    Filter typeFilter = new TermFilter(docMapper.typeMapper().term(docMapper.type()));
                    typeFilter = filterCache.cache(typeFilter);
                    booleanFilter.add(new FilterClause(typeFilter, BooleanClause.Occur.SHOULD));
                }
                query = new FilteredQuery(query, booleanFilter);
            }
        }
        return query;
    }

    private class EngineRefresher implements Runnable {
        @Override public void run() {
            try {
                engine.refresh(false);
            } catch (Exception e) {
                logger.warn("Failed to perform scheduled engine refresh", e);
            }
        }
    }
}