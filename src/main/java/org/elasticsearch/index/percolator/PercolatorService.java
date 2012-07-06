/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.index.percolator;

import com.google.common.collect.Maps;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Fieldable;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.*;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.lucene.search.TermFilter;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.AbstractIndexComponent;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.indexing.IndexingOperationListener;
import org.elasticsearch.index.mapper.Uid;
import org.elasticsearch.index.mapper.internal.SourceFieldMapper;
import org.elasticsearch.index.mapper.internal.TypeFieldMapper;
import org.elasticsearch.index.mapper.internal.UidFieldMapper;
import org.elasticsearch.index.mapper.selector.UidAndSourceFieldSelector;
import org.elasticsearch.index.service.IndexService;
import org.elasticsearch.index.settings.IndexSettings;
import org.elasticsearch.index.shard.IndexShardState;
import org.elasticsearch.index.shard.service.IndexShard;
import org.elasticsearch.indices.IndicesLifecycle;
import org.elasticsearch.indices.IndicesService;

import java.io.IOException;
import java.util.Map;

/**
 *
 */
public class PercolatorService extends AbstractIndexComponent {

    public static final String INDEX_NAME = "_percolator";

    private final IndicesService indicesService;

    private final PercolatorExecutor percolator;

    private final ShardLifecycleListener shardLifecycleListener;

    private final RealTimePercolatorOperationListener realTimePercolatorOperationListener = new RealTimePercolatorOperationListener();

    private final Object mutex = new Object();

    private boolean initialQueriesFetchDone = false;

    @Inject
    public PercolatorService(Index index, @IndexSettings Settings indexSettings, IndicesService indicesService,
                             PercolatorExecutor percolator) {
        super(index, indexSettings);
        this.indicesService = indicesService;
        this.percolator = percolator;
        this.shardLifecycleListener = new ShardLifecycleListener();
        this.indicesService.indicesLifecycle().addListener(shardLifecycleListener);
        this.percolator.setIndicesService(indicesService);

        // if percolator is already allocated, make sure to register real time percolation
        if (percolatorAllocated()) {
            IndexService percolatorIndexService = percolatorIndexService();
            if (percolatorIndexService != null) {
                for (IndexShard indexShard : percolatorIndexService) {
                    try {
                        indexShard.indexingService().addListener(realTimePercolatorOperationListener);
                    } catch (Exception e) {
                        // ignore
                    }
                }
            }
        }
    }

    public void close() {
        this.indicesService.indicesLifecycle().removeListener(shardLifecycleListener);

        // clean up any index that has registered real time updated from the percolator shards allocated on this node
        IndexService percolatorIndexService = percolatorIndexService();
        if (percolatorIndexService != null) {
            for (IndexShard indexShard : percolatorIndexService) {
                try {
                    indexShard.indexingService().removeListener(realTimePercolatorOperationListener);
                } catch (Exception e) {
                    // ignore
                }
            }
        }
    }

    public PercolatorExecutor.Response percolate(PercolatorExecutor.SourceRequest request) throws PercolatorException {
        return percolator.percolate(request);
    }

    public PercolatorExecutor.Response percolate(PercolatorExecutor.DocAndSourceQueryRequest request) throws PercolatorException {
        return percolator.percolate(request);
    }

    private void loadQueries(String indexName) {
        IndexService indexService = percolatorIndexService();
        IndexShard shard = indexService.shard(0);
        shard.refresh(new Engine.Refresh(true));
        Engine.Searcher searcher = shard.searcher();
        try {
            // create a query to fetch all queries that are registered under the index name (which is the type
            // in the percolator).
            Query query = new DeletionAwareConstantScoreQuery(indexQueriesFilter(indexName));
            QueriesLoaderCollector queries = new QueriesLoaderCollector();
            searcher.searcher().search(query, queries);
            percolator.addQueries(queries.queries());
        } catch (IOException e) {
            throw new PercolatorException(index, "failed to load queries from percolator index");
        } finally {
            searcher.release();
        }
    }

    private Filter indexQueriesFilter(String indexName) {
        return percolatorIndexService().cache().filter().cache(new TermFilter(TypeFieldMapper.TERM_FACTORY.createTerm(indexName)));
    }

    private boolean percolatorAllocated() {
        if (!indicesService.hasIndex(INDEX_NAME)) {
            return false;
        }
        if (percolatorIndexService().numberOfShards() == 0) {
            return false;
        }
        if (percolatorIndexService().shard(0).state() != IndexShardState.STARTED) {
            return false;
        }
        return true;
    }

    private IndexService percolatorIndexService() {
        return indicesService.indexService(INDEX_NAME);
    }

    class QueriesLoaderCollector extends Collector {

        private IndexReader reader;

        private Map<String, Query> queries = Maps.newHashMap();

        public Map<String, Query> queries() {
            return this.queries;
        }

        @Override
        public void setScorer(Scorer scorer) throws IOException {
        }

        @Override
        public void collect(int doc) throws IOException {
            // the _source is the query
            Document document = reader.document(doc, new UidAndSourceFieldSelector());
            String id = Uid.createUid(document.get(UidFieldMapper.NAME)).id();
            try {
                Fieldable sourceField = document.getFieldable(SourceFieldMapper.NAME);
                queries.put(id, percolator.parseQuery(id, new BytesArray(sourceField.getBinaryValue(), sourceField.getBinaryOffset(), sourceField.getBinaryLength())));
            } catch (Exception e) {
                logger.warn("failed to add query [{}]", e, id);
            }
        }

        @Override
        public void setNextReader(IndexReader reader, int docBase) throws IOException {
            this.reader = reader;
        }

        @Override
        public boolean acceptsDocsOutOfOrder() {
            return true;
        }
    }

    class ShardLifecycleListener extends IndicesLifecycle.Listener {

        @Override
        public void afterIndexShardCreated(IndexShard indexShard) {
            // add a listener that will update based on changes done to the _percolate index
            // the relevant indices with loaded queries
            if (indexShard.shardId().index().name().equals(INDEX_NAME)) {
                indexShard.indexingService().addListener(realTimePercolatorOperationListener);
            }
        }

        @Override
        public void afterIndexShardStarted(IndexShard indexShard) {
            if (indexShard.shardId().index().name().equals(INDEX_NAME)) {
                // percolator index has started, fetch what we can from it and initialize the indices
                // we have
                synchronized (mutex) {
                    if (initialQueriesFetchDone) {
                        return;
                    }
                    // we load the queries for all existing indices
                    for (IndexService indexService : indicesService) {
                        // only load queries for "this" index percolator service
                        if (indexService.index().equals(index())) {
                            logger.debug("loading percolator queries for index [{}]...", indexService.index().name());
                            loadQueries(indexService.index().name());
                            logger.trace("done loading percolator queries for index [{}]", indexService.index().name());
                        }
                    }
                    initialQueriesFetchDone = true;
                }
            }
            if (!indexShard.shardId().index().equals(index())) {
                // not our index, bail
                return;
            }
            if (!percolatorAllocated()) {
                return;
            }
            // we are only interested when the first shard on this node has been created for an index
            // when it does, fetch the relevant queries if not fetched already
            IndexService indexService = indicesService.indexService(indexShard.shardId().index().name());
            if (indexService == null) {
                return;
            }
            if (indexService.numberOfShards() != 1) {
                return;
            }
            synchronized (mutex) {
                if (initialQueriesFetchDone) {
                    return;
                }
                // we load queries for this index
                logger.debug("loading percolator queries for index [{}]...", indexService.index().name());
                loadQueries(index.name());
                logger.trace("done loading percolator queries for index [{}]", indexService.index().name());
                initialQueriesFetchDone = true;
            }
        }
    }

    class RealTimePercolatorOperationListener extends IndexingOperationListener {

        @Override
        public Engine.Create preCreate(Engine.Create create) {
            // validate the query here, before we index
            if (create.type().equals(index().name())) {
                percolator.parseQuery(create.id(), create.source());
            }
            return create;
        }

        @Override
        public void postCreateUnderLock(Engine.Create create) {
            // add the query under a doc lock
            if (create.type().equals(index().name())) {
                percolator.addQuery(create.id(), create.source());
            }
        }

        @Override
        public Engine.Index preIndex(Engine.Index index) {
            // validate the query here, before we index
            if (index.type().equals(index().name())) {
                percolator.parseQuery(index.id(), index.source());
            }
            return index;
        }

        @Override
        public void postIndexUnderLock(Engine.Index index) {
            // add the query under a doc lock
            if (index.type().equals(index().name())) {
                percolator.addQuery(index.id(), index.source());
            }
        }

        @Override
        public void postDeleteUnderLock(Engine.Delete delete) {
            // remove the query under a lock
            if (delete.type().equals(index().name())) {
                percolator.removeQuery(delete.id());
            }
        }
    }
}
