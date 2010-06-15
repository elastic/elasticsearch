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
import org.apache.lucene.index.Term;
import org.apache.lucene.search.*;
import org.apache.lucene.util.ThreadInterruptedException;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.ElasticSearchIllegalArgumentException;
import org.elasticsearch.ElasticSearchIllegalStateException;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.lucene.search.TermFilter;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadSafe;
import org.elasticsearch.index.cache.IndexCache;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.EngineException;
import org.elasticsearch.index.engine.RefreshFailedEngineException;
import org.elasticsearch.index.engine.ScheduledRefreshableEngine;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.DocumentMapperNotFoundException;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.query.IndexQueryParser;
import org.elasticsearch.index.query.IndexQueryParserMissingException;
import org.elasticsearch.index.query.IndexQueryParserService;
import org.elasticsearch.index.settings.IndexSettings;
import org.elasticsearch.index.shard.*;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.indices.TypeMissingException;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.util.SizeValue;
import org.elasticsearch.util.Strings;
import org.elasticsearch.util.TimeValue;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.channels.ClosedByInterruptException;
import java.util.concurrent.ScheduledFuture;

/**
 * @author kimchy (shay.banon)
 */
@ThreadSafe
public class InternalIndexShard extends AbstractIndexShardComponent implements IndexShard {

    private final ThreadPool threadPool;

    private final MapperService mapperService;

    private final IndexQueryParserService queryParserService;

    private final IndexCache indexCache;

    private final Store store;

    private final Engine engine;

    private final Translog translog;

    private final Object mutex = new Object();

    private volatile IndexShardState state;

    private ScheduledFuture refreshScheduledFuture;

    private volatile ShardRouting shardRouting;

    @Inject public InternalIndexShard(ShardId shardId, @IndexSettings Settings indexSettings, Store store, Engine engine, Translog translog,
                                      ThreadPool threadPool, MapperService mapperService, IndexQueryParserService queryParserService, IndexCache indexCache) {
        super(shardId, indexSettings);
        this.store = store;
        this.engine = engine;
        this.translog = translog;
        this.threadPool = threadPool;
        this.mapperService = mapperService;
        this.queryParserService = queryParserService;
        this.indexCache = indexCache;
        state = IndexShardState.CREATED;
        logger.debug("Moved to state [CREATED]");
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
            logger.debug("Moved to state [RECOVERING]");
            return returnValue;
        }
    }

    public InternalIndexShard restoreRecoveryState(IndexShardState stateToRestore) {
        synchronized (mutex) {
            if (this.state != IndexShardState.RECOVERING) {
                throw new IndexShardNotRecoveringException(shardId, state);
            }
            logger.debug("Restored to state [{}] from state [{}]", stateToRestore, state);
            this.state = stateToRestore;
        }
        return this;
    }

    public InternalIndexShard relocated() throws IndexShardNotStartedException {
        synchronized (mutex) {
            if (state != IndexShardState.STARTED) {
                throw new IndexShardNotStartedException(shardId, state);
            }
            logger.debug("Moved to state [RELOCATED]");
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
            logger.debug("Moved to state [STARTED]");
            state = IndexShardState.STARTED;
        }
        return this;
    }

    @Override public IndexShardState state() {
        return state;
    }

    /**
     * Returns the estimated flushable memory size. Returns <tt>null</tt> if not available.
     */
    @Override public SizeValue estimateFlushableMemorySize() throws ElasticSearchException {
        writeAllowed();
        return engine.estimateFlushableMemorySize();
    }

    @Override public ParsedDocument create(String type, String id, byte[] source) throws ElasticSearchException {
        writeAllowed();
        return innerCreate(type, id, source);
    }

    private ParsedDocument innerCreate(String type, String id, byte[] source) {
        DocumentMapper docMapper = mapperService.type(type);
        if (docMapper == null) {
            throw new DocumentMapperNotFoundException("No mapper found for type [" + type + "]");
        }
        ParsedDocument doc = docMapper.parse(type, id, source);
        if (logger.isTraceEnabled()) {
            logger.trace("Indexing {}", doc);
        }
        engine.create(new Engine.Create(doc.doc(), docMapper.mappers().indexAnalyzer(), docMapper.type(), doc.id(), doc.source()));
        return doc;
    }

    @Override public ParsedDocument index(String type, String id, byte[] source) throws ElasticSearchException {
        writeAllowed();
        return innerIndex(type, id, source);
    }

    private ParsedDocument innerIndex(String type, String id, byte[] source) {
        DocumentMapper docMapper = mapperService.type(type);
        if (docMapper == null) {
            throw new DocumentMapperNotFoundException("No mapper found for type [" + type + "]");
        }
        ParsedDocument doc = docMapper.parse(type, id, source);
        if (logger.isTraceEnabled()) {
            logger.trace("Indexing {}", doc);
        }
        engine.index(new Engine.Index(docMapper.uidMapper().term(doc.uid()), doc.doc(), docMapper.mappers().indexAnalyzer(), docMapper.type(), doc.id(), doc.source()));
        return doc;
    }

    @Override public void delete(String type, String id) {
        writeAllowed();
        DocumentMapper docMapper = mapperService.type(type);
        if (docMapper == null) {
            throw new DocumentMapperNotFoundException("No mapper found for type [" + type + "]");
        }
        innerDelete(docMapper.uidMapper().term(type, id));
    }

    @Override public void delete(Term uid) {
        writeAllowed();
        innerDelete(uid);
    }

    private void innerDelete(Term uid) {
        if (logger.isTraceEnabled()) {
            logger.trace("Deleting [{}]", uid.text());
        }
        engine.delete(new Engine.Delete(uid));
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
        Query query = queryParser.parse(querySource);
        query = filterByTypesIfNeeded(query, types);

        if (logger.isTraceEnabled()) {
            logger.trace("Deleting By Query [{}]", query);
        }

        engine.delete(new Engine.DeleteByQuery(query, querySource, queryParserName, types));
    }

    @Override public byte[] get(String type, String id) throws ElasticSearchException {
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
                logger.trace("Get for [{}#{}] returned [{}]", type, id, doc);
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
        Query query = queryParser.parse(querySource);
        // wrap it in filter, cache it, and constant score it
        // Don't cache it, since it might be very different queries each time...
//        query = new ConstantScoreQuery(filterCache.cache(new QueryWrapperFilter(query)));
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

    @Override public void refresh(Engine.Refresh refresh) throws ElasticSearchException {
        writeAllowed();
        if (logger.isTraceEnabled()) {
            logger.trace("Refresh with {}", refresh);
        }
        engine.refresh(refresh);
    }

    @Override public void flush(Engine.Flush flush) throws ElasticSearchException {
        writeAllowed();
        if (logger.isTraceEnabled()) {
            logger.trace("Flush with {}", flush);
        }
        engine.flush(flush);
    }

    @Override public void optimize(Engine.Optimize optimize) throws ElasticSearchException {
        writeAllowed();
        if (logger.isTraceEnabled()) {
            logger.trace("Optimize with {}", optimize);
        }
        engine.optimize(optimize);
    }

    @Override public <T> T snapshot(Engine.SnapshotHandler<T> snapshotHandler) throws EngineException {
        readAllowed();
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

    @Override public void close() {
        synchronized (mutex) {
            if (state != IndexShardState.CLOSED) {
                if (refreshScheduledFuture != null) {
                    refreshScheduledFuture.cancel(true);
                    refreshScheduledFuture = null;
                }
            }
            logger.debug("Moved to state [CLOSED]");
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
            logger.debug("Moved to state [STARTED] post recovery (from gateway)");
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
                logger.debug("Moved to state [STARTED] post recovery (from another shard)");
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
                typeFilter = indexCache.filter().cache(typeFilter);
                query = new FilteredQuery(query, typeFilter);
            } else {
                BooleanFilter booleanFilter = new BooleanFilter();
                for (String type : types) {
                    DocumentMapper docMapper = mapperService.documentMapper(type);
                    if (docMapper == null) {
                        throw new TypeMissingException(shardId.index(), type);
                    }
                    Filter typeFilter = new TermFilter(docMapper.typeMapper().term(docMapper.type()));
                    typeFilter = indexCache.filter().cache(typeFilter);
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
                engine.refresh(new Engine.Refresh(false));
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
        }
    }

    // I wrote all this code, and now there is no need for it since dynamic mappings are autoamtically
    // broadcast to all the cluster when updated, so we won't be in a state when the mappings are not up to
    // date, in any case, lets leave it here for now

//    /**
//     * The mapping sniffer reads docs from the index and introduces them into the mapping service. This is
//     * because of dynamic fields and we want to reintroduce them.
//     *
//     * <p>Note, this is done on the shard level, we might have other dynamic fields in other shards, but
//     * this will be taken care off in another component.
//     */
//    private class ShardMappingSniffer implements Runnable {
//        @Override public void run() {
//            engine.refresh(new Engine.Refresh(true));
//
//            TermEnum termEnum = null;
//            Engine.Searcher searcher = searcher();
//            try {
//                List<String> typeNames = newArrayList();
//                termEnum = searcher.reader().terms(new Term(TypeFieldMapper.NAME, ""));
//                while (true) {
//                    Term term = termEnum.term();
//                    if (term == null) {
//                        break;
//                    }
//                    if (!term.field().equals(TypeFieldMapper.NAME)) {
//                        break;
//                    }
//                    typeNames.add(term.text());
//                    termEnum.next();
//                }
//
//                logger.debug("Sniffing mapping for [{}]", typeNames);
//
//                for (final String type : typeNames) {
//                    threadPool.execute(new Runnable() {
//                        @Override public void run() {
//                            Engine.Searcher searcher = searcher();
//                            try {
//                                Query query = new ConstantScoreQuery(filterCache.cache(new TermFilter(new Term(TypeFieldMapper.NAME, type))));
//                                long typeCount = Lucene.count(searcher().searcher(), query, -1);
//
//                                int marker = (int) (typeCount / mappingSnifferDocs);
//                                if (marker == 0) {
//                                    marker = 1;
//                                }
//                                final int fMarker = marker;
//                                searcher.searcher().search(query, new Collector() {
//
//                                    private final FieldSelector fieldSelector = new UidAndSourceFieldSelector();
//                                    private int counter = 0;
//                                    private IndexReader reader;
//
//                                    @Override public void setScorer(Scorer scorer) throws IOException {
//                                    }
//
//                                    @Override public void collect(int doc) throws IOException {
//                                        if (state == IndexShardState.CLOSED) {
//                                            throw new IOException("CLOSED");
//                                        }
//                                        if (++counter == fMarker) {
//                                            counter = 0;
//
//                                            Document document = reader.document(doc, fieldSelector);
//                                            Uid uid = Uid.createUid(document.get(UidFieldMapper.NAME));
//                                            String source = document.get(SourceFieldMapper.NAME);
//
//                                            mapperService.type(uid.type()).parse(uid.type(), uid.id(), source);
//                                        }
//                                    }
//
//                                    @Override public void setNextReader(IndexReader reader, int docBase) throws IOException {
//                                        this.reader = reader;
//                                    }
//
//                                    @Override public boolean acceptsDocsOutOfOrder() {
//                                        return true;
//                                    }
//                                });
//                            } catch (IOException e) {
//                                if (e.getMessage().equals("CLOSED")) {
//                                    // ignore, we got closed
//                                } else {
//                                    logger.warn("Failed to sniff mapping for type [" + type + "]", e);
//                                }
//                            } finally {
//                                searcher.release();
//                            }
//                        }
//                    });
//                }
//            } catch (IOException e) {
//                if (e.getMessage().equals("CLOSED")) {
//                    // ignore, we got closed
//                } else {
//                    logger.warn("Failed to sniff mapping", e);
//                }
//            } finally {
//                if (termEnum != null) {
//                    try {
//                        termEnum.close();
//                    } catch (IOException e) {
//                        // ignore
//                    }
//                }
//                searcher.release();
//            }
//        }
//    }
}