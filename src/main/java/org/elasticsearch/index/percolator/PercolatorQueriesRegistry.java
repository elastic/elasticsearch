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

package org.elasticsearch.index.percolator;

import org.apache.lucene.index.Term;
import org.apache.lucene.queries.TermFilter;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.lucene.HashedBytesRef;
import org.elasticsearch.common.lucene.search.XConstantScoreQuery;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.cache.IndexCache;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.fielddata.IndexFieldDataService;
import org.elasticsearch.index.indexing.IndexingOperationListener;
import org.elasticsearch.index.indexing.ShardIndexingService;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.DocumentTypeListener;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.internal.TypeFieldMapper;
import org.elasticsearch.index.percolator.stats.ShardPercolateService;
import org.elasticsearch.index.query.IndexQueryParserService;
import org.elasticsearch.index.query.QueryParseContext;
import org.elasticsearch.index.settings.IndexSettings;
import org.elasticsearch.index.shard.AbstractIndexShardComponent;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.service.IndexShard;
import org.elasticsearch.indices.IndicesLifecycle;
import org.elasticsearch.percolator.PercolatorService;

import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Each shard will have a percolator registry even if there isn't a {@link PercolatorService#TYPE_NAME} document type in the index.
 * For shards with indices that have no {@link PercolatorService#TYPE_NAME} document type, this will hold no percolate queries.
 * <p/>
 * Once a document type has been created, the real-time percolator will start to listen to write events and update the
 * this registry with queries in real time.
 */
public class PercolatorQueriesRegistry extends AbstractIndexShardComponent {

    // This is a shard level service, but these below are index level service:
    private final IndexQueryParserService queryParserService;
    private final MapperService mapperService;
    private final IndicesLifecycle indicesLifecycle;
    private final IndexCache indexCache;
    private final IndexFieldDataService indexFieldDataService;

    private final ShardIndexingService indexingService;
    private final ShardPercolateService shardPercolateService;

    private final ConcurrentMap<HashedBytesRef, Query> percolateQueries = ConcurrentCollections.newConcurrentMapWithAggressiveConcurrency();
    private final ShardLifecycleListener shardLifecycleListener = new ShardLifecycleListener();
    private final RealTimePercolatorOperationListener realTimePercolatorOperationListener = new RealTimePercolatorOperationListener();
    private final PercolateTypeListener percolateTypeListener = new PercolateTypeListener();
    private final AtomicBoolean realTimePercolatorEnabled = new AtomicBoolean(false);

    @Inject
    public PercolatorQueriesRegistry(ShardId shardId, @IndexSettings Settings indexSettings, IndexQueryParserService queryParserService,
                                     ShardIndexingService indexingService, IndicesLifecycle indicesLifecycle, MapperService mapperService,
                                     IndexCache indexCache, IndexFieldDataService indexFieldDataService, ShardPercolateService shardPercolateService) {
        super(shardId, indexSettings);
        this.queryParserService = queryParserService;
        this.mapperService = mapperService;
        this.indicesLifecycle = indicesLifecycle;
        this.indexingService = indexingService;
        this.indexCache = indexCache;
        this.indexFieldDataService = indexFieldDataService;
        this.shardPercolateService = shardPercolateService;

        indicesLifecycle.addListener(shardLifecycleListener);
        mapperService.addTypeListener(percolateTypeListener);
    }

    public ConcurrentMap<HashedBytesRef, Query> percolateQueries() {
        return percolateQueries;
    }

    public void close() {
        mapperService.removeTypeListener(percolateTypeListener);
        indicesLifecycle.removeListener(shardLifecycleListener);
        indexingService.removeListener(realTimePercolatorOperationListener);
        clear();
    }

    public void clear() {
        percolateQueries.clear();
    }

    void enableRealTimePercolator() {
        if (realTimePercolatorEnabled.compareAndSet(false, true)) {
            indexingService.addListener(realTimePercolatorOperationListener);
        }
    }

    void disableRealTimePercolator() {
        if (realTimePercolatorEnabled.compareAndSet(true, false)) {
            indexingService.removeListener(realTimePercolatorOperationListener);
        }
    }

    public void addPercolateQuery(String idAsString, BytesReference source) {
        Query newquery = parsePercolatorDocument(idAsString, source);
        HashedBytesRef id = new HashedBytesRef(new BytesRef(idAsString));
        Query previousQuery = percolateQueries.put(id, newquery);
        shardPercolateService.addedQuery(id, previousQuery, newquery);
    }

    public void removePercolateQuery(String idAsString) {
        HashedBytesRef id = new HashedBytesRef(idAsString);
        Query query = percolateQueries.remove(id);
        if (query != null) {
            shardPercolateService.removedQuery(id, query);
        }
    }

    Query parsePercolatorDocument(String id, BytesReference source) {
        String type = null;
        BytesReference querySource = null;

        XContentParser parser = null;
        try {
            parser = XContentHelper.createParser(source);
            String currentFieldName = null;
            XContentParser.Token token = parser.nextToken(); // move the START_OBJECT
            if (token != XContentParser.Token.START_OBJECT) {
                throw new ElasticsearchException("failed to parse query [" + id + "], not starting with OBJECT");
            }
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if (token == XContentParser.Token.START_OBJECT) {
                    if ("query".equals(currentFieldName)) {
                        if (type != null) {
                            return parseQuery(type, null, parser);
                        } else {
                            XContentBuilder builder = XContentFactory.contentBuilder(parser.contentType());
                            builder.copyCurrentStructure(parser);
                            querySource = builder.bytes();
                            builder.close();
                        }
                    } else {
                        parser.skipChildren();
                    }
                } else if (token == XContentParser.Token.START_ARRAY) {
                    parser.skipChildren();
                } else if (token.isValue()) {
                    if ("type".equals(currentFieldName)) {
                        type = parser.text();
                    }
                }
            }
            return parseQuery(type, querySource, null);
        } catch (Exception e) {
            throw new PercolatorException(shardId().index(), "failed to parse query [" + id + "]", e);
        } finally {
            if (parser != null) {
                parser.close();
            }
        }
    }

    private Query parseQuery(String type, BytesReference querySource, XContentParser parser) {
        if (type == null) {
            if (parser != null) {
                return queryParserService.parse(parser).query();
            } else {
                return queryParserService.parse(querySource).query();
            }
        }

        String[] previousTypes = QueryParseContext.setTypesWithPrevious(new String[]{type});
        try {
            if (parser != null) {
                return queryParserService.parse(parser).query();
            } else {
                return queryParserService.parse(querySource).query();
            }
        } finally {
            QueryParseContext.setTypes(previousTypes);
        }
    }

    private class PercolateTypeListener implements DocumentTypeListener {

        @Override
        public void beforeCreate(DocumentMapper mapper) {
            if (PercolatorService.TYPE_NAME.equals(mapper.type())) {
                enableRealTimePercolator();
            }
        }

        @Override
        public void afterRemove(DocumentMapper mapper) {
            if (PercolatorService.TYPE_NAME.equals(mapper.type())) {
                disableRealTimePercolator();
                clear();
            }
        }

    }

    private class ShardLifecycleListener extends IndicesLifecycle.Listener {

        @Override
        public void afterIndexShardCreated(IndexShard indexShard) {
            if (hasPercolatorType(indexShard)) {
                enableRealTimePercolator();
            }
        }

        @Override
        public void afterIndexShardPostRecovery(IndexShard indexShard) {
            if (hasPercolatorType(indexShard)) {
                // percolator index has started, fetch what we can from it and initialize the indices
                // we have
                logger.debug("loading percolator queries for index [{}] and shard[{}]...", shardId.index(), shardId.id());
                loadQueries(indexShard);
                logger.trace("done loading percolator queries for index [{}] and shard[{}]", shardId.index(), shardId.id());
            }
        }

        private boolean hasPercolatorType(IndexShard indexShard) {
            ShardId otherShardId = indexShard.shardId();
            return shardId.equals(otherShardId) && mapperService.hasMapping(PercolatorService.TYPE_NAME);
        }

        private void loadQueries(IndexShard shard) {
            try {
                shard.refresh(new Engine.Refresh("percolator_load_queries").force(true));
                // Maybe add a mode load? This isn't really a write. We need write b/c state=post_recovery
                Engine.Searcher searcher = shard.acquireSearcher("percolator_load_queries", IndexShard.Mode.WRITE);
                try {
                    Query query = new XConstantScoreQuery(
                            indexCache.filter().cache(
                                    new TermFilter(new Term(TypeFieldMapper.NAME, PercolatorService.TYPE_NAME))
                            )
                    );
                    QueriesLoaderCollector queryCollector = new QueriesLoaderCollector(PercolatorQueriesRegistry.this, logger, mapperService, indexFieldDataService);
                    searcher.searcher().search(query, queryCollector);
                    Map<HashedBytesRef, Query> queries = queryCollector.queries();
                    for (Map.Entry<HashedBytesRef, Query> entry : queries.entrySet()) {
                        Query previousQuery = percolateQueries.put(entry.getKey(), entry.getValue());
                        shardPercolateService.addedQuery(entry.getKey(), previousQuery, entry.getValue());
                    }
                } finally {
                    searcher.close();
                }
            } catch (Exception e) {
                throw new PercolatorException(shardId.index(), "failed to load queries from percolator index", e);
            }
        }

    }

    private class RealTimePercolatorOperationListener extends IndexingOperationListener {

        @Override
        public Engine.Create preCreate(Engine.Create create) {
            // validate the query here, before we index
            if (PercolatorService.TYPE_NAME.equals(create.type())) {
                parsePercolatorDocument(create.id(), create.source());
            }
            return create;
        }

        @Override
        public void postCreateUnderLock(Engine.Create create) {
            // add the query under a doc lock
            if (PercolatorService.TYPE_NAME.equals(create.type())) {
                addPercolateQuery(create.id(), create.source());
            }
        }

        @Override
        public Engine.Index preIndex(Engine.Index index) {
            // validate the query here, before we index
            if (PercolatorService.TYPE_NAME.equals(index.type())) {
                parsePercolatorDocument(index.id(), index.source());
            }
            return index;
        }

        @Override
        public void postIndexUnderLock(Engine.Index index) {
            // add the query under a doc lock
            if (PercolatorService.TYPE_NAME.equals(index.type())) {
                addPercolateQuery(index.id(), index.source());
            }
        }

        @Override
        public void postDeleteUnderLock(Engine.Delete delete) {
            // remove the query under a lock
            if (PercolatorService.TYPE_NAME.equals(delete.type())) {
                removePercolateQuery(delete.id());
            }
        }

        // Updating the live percolate queries for a delete by query is tricky with the current way delete by queries
        // are handled. It is only possible if we put a big lock around the post delete by query hook...

        // If we implement delete by query, that just runs a query and generates delete operations in a bulk, then
        // updating the live percolator is automatically supported for delete by query.
//        @Override
//        public void postDeleteByQuery(Engine.DeleteByQuery deleteByQuery) {
//        }
    }

}
