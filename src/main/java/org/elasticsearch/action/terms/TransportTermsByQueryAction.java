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

package org.elasticsearch.action.terms;

import com.google.common.collect.ImmutableMap;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.search.*;
import org.apache.lucene.util.FixedBitSet;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ShardOperationFailedException;
import org.elasticsearch.action.support.DefaultShardOperationFailedException;
import org.elasticsearch.action.support.broadcast.BroadcastShardOperationFailedException;
import org.elasticsearch.action.support.broadcast.TransportBroadcastOperationAction;
import org.elasticsearch.cache.recycler.CacheRecycler;
import org.elasticsearch.cache.recycler.PageCacheRecycler;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.routing.GroupShardsIterator;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.lucene.search.XConstantScoreQuery;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.query.ParsedQuery;
import org.elasticsearch.index.query.QueryParseContext;
import org.elasticsearch.index.service.IndexService;
import org.elasticsearch.index.shard.service.IndexShard;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.SearchContextException;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.internal.DefaultSearchContext;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.internal.ShardSearchRequest;
import org.elasticsearch.search.query.QueryPhaseExecutionException;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReferenceArray;

import static com.google.common.collect.Lists.newArrayList;

/**
 * The terms by query transport operation
 */
public class TransportTermsByQueryAction extends TransportBroadcastOperationAction<TermsByQueryRequest, TermsByQueryResponse, ShardTermsByQueryRequest, ShardTermsByQueryResponse> {

    private final IndicesService indicesService;
    private final ScriptService scriptService;
    private final CacheRecycler cacheRecycler;
    private final PageCacheRecycler pageCacheRecycler;
    private final BigArrays bigArrays;

    /**
     * Constructor
     */
    @Inject
    public TransportTermsByQueryAction(Settings settings, ThreadPool threadPool, ClusterService clusterService,
                                       TransportService transportService, IndicesService indicesService,
                                       ScriptService scriptService, CacheRecycler cacheRecycler,
                                       PageCacheRecycler pageCacheRecycler, BigArrays bigArrays) {
        super(settings, threadPool, clusterService, transportService);
        this.indicesService = indicesService;
        this.scriptService = scriptService;
        this.cacheRecycler = cacheRecycler;
        this.pageCacheRecycler = pageCacheRecycler;
        this.bigArrays = bigArrays;
    }

    /**
     * Executes the actions.
     */
    @Override
    protected void doExecute(TermsByQueryRequest request, ActionListener<TermsByQueryResponse> listener) {
        request.nowInMillis(System.currentTimeMillis()); // set time to be used in scripts
        super.doExecute(request, listener);
    }

    /**
     * The threadpool this request will execute against
     */
    @Override
    protected String executor() {
        return ThreadPool.Names.SEARCH;
    }

    /**
     * The action name
     */
    @Override
    protected String transportAction() {
        return TermsByQueryAction.NAME;
    }

    /**
     * Creates a new {@link TermsByQueryRequest}
     */
    @Override
    protected TermsByQueryRequest newRequest() {
        return new TermsByQueryRequest();
    }

    /**
     * Creates a new {@link ShardTermsByQueryRequest}
     */
    @Override
    protected ShardTermsByQueryRequest newShardRequest() {
        return new ShardTermsByQueryRequest();
    }

    /**
     * Creates a new {@link ShardTermsByQueryRequest}
     */
    @Override
    protected ShardTermsByQueryRequest newShardRequest(ShardRouting shard, TermsByQueryRequest request) {
        String[] filteringAliases = clusterService.state().metaData().filteringAliases(shard.index(), request.indices());
        return new ShardTermsByQueryRequest(shard.index(), shard.id(), filteringAliases, request);
    }

    /**
     * Creates a new {@link ShardTermsByQueryResponse}
     */
    @Override
    protected ShardTermsByQueryResponse newShardResponse() {
        return new ShardTermsByQueryResponse();
    }

    /**
     * The shards this request will execute against.
     */
    @Override
    protected GroupShardsIterator shards(ClusterState clusterState, TermsByQueryRequest request, String[] concreteIndices) {
        Map<String, Set<String>> routingMap = clusterState.metaData().resolveSearchRouting(request.routing(), request.indices());
        return clusterService.operationRouting().searchShards(clusterState,
                request.indices(),
                concreteIndices,
                routingMap,
                request.preference());
    }

    @Override
    protected ClusterBlockException checkGlobalBlock(ClusterState state, TermsByQueryRequest request) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.READ);
    }

    @Override
    protected ClusterBlockException checkRequestBlock(ClusterState state, TermsByQueryRequest request, String[] concreteIndices) {
        return state.blocks().indicesBlockedException(ClusterBlockLevel.READ, concreteIndices);
    }

    /**
     * Merges the individual shard responses and returns the final {@link TermsByQueryResponse}.
     */
    @Override
    protected TermsByQueryResponse newResponse(TermsByQueryRequest request, AtomicReferenceArray shardsResponses, ClusterState clusterState) {
        int successfulShards = 0;
        int failedShards = 0;
        int numTerms = 0;
        ResponseTerms responseTerms = null;
        ResponseTerms[] responses = new ResponseTerms[shardsResponses.length()];
        List<ShardOperationFailedException> shardFailures = null;

        // we check each shard response
        for (int i = 0; i < shardsResponses.length(); i++) {
            Object shardResponse = shardsResponses.get(i);
            if (shardResponse == null) {
                failedShards++;
            } else if (shardResponse instanceof BroadcastShardOperationFailedException) {
                failedShards++;
                if (shardFailures == null) {
                    shardFailures = newArrayList();
                }
                logger.info("shard operation failed", (BroadcastShardOperationFailedException) shardResponse);
                shardFailures.add(new DefaultShardOperationFailedException((BroadcastShardOperationFailedException) shardResponse));
            } else {
                // on successful shard response, just add to the array or responses so we can process them below
                // we calculate the total number of terms gathered across each shard so we can use it during
                // initialization of the final ResponseTerms below (to avoid rehashing during merging)
                ShardTermsByQueryResponse shardResp = ((ShardTermsByQueryResponse) shardResponse);
                ResponseTerms response = shardResp.getResponseTerms();
                responses[i] = response;
                numTerms += response.size();
                successfulShards++;
            }
        }

        // merge the responses
        for (int i = 0; i < responses.length; i++) {
            ResponseTerms response = responses[i];
            if (response == null) {
                continue;
            }

            // responseTerms is responsible for the merge, use first non-null response
            // set size to avoid rehashing on certain implementations.
            if (responseTerms == null) {
                responseTerms = ResponseTerms.get(response.getType(), numTerms);
            }

            responseTerms.merge(response);
        }

        return new TermsByQueryResponse(responseTerms, shardsResponses.length(), successfulShards, failedShards, shardFailures);
    }

    /**
     * The operation that executes the query and generates a {@link ShardTermsByQueryResponse} for each shard.
     */
    @Override
    protected ShardTermsByQueryResponse shardOperation(ShardTermsByQueryRequest shardRequest) throws ElasticsearchException {
        IndexService indexService = indicesService.indexServiceSafe(shardRequest.index());
        IndexShard indexShard = indexService.shardSafe(shardRequest.shardId());
        TermsByQueryRequest request = shardRequest.request();

        SearchShardTarget shardTarget =
                new SearchShardTarget(clusterService.localNode().id(), shardRequest.index(), shardRequest.shardId());

        ShardSearchRequest shardSearchRequest = new ShardSearchRequest()
                .types(request.types())
                .filteringAliases(shardRequest.filteringAliases())
                .nowInMillis(request.nowInMillis());

        SearchContext context = new DefaultSearchContext(0, shardSearchRequest, shardTarget,
                indexShard.acquireSearcher("termsByQuery"), indexService, indexShard, scriptService, cacheRecycler,
                pageCacheRecycler, bigArrays);

        try {
            SearchContext.setCurrent(context);
            FieldMapper fieldMapper = context.smartNameFieldMapper(request.field());
            if (fieldMapper == null) {
                throw new SearchContextException(context, "field not found");
            }

            IndexFieldData indexFieldData = context.fieldData().getForField(fieldMapper);

            if (request.minScore() != null) {
                context.minimumScore(request.minScore());
            }

            BytesReference filterSource = request.filterSource();
            if (filterSource != null && filterSource.length() > 0) {
                XContentParser filterParser = null;
                try {
                    filterParser = XContentFactory.xContent(filterSource).createParser(filterSource);
                    QueryParseContext.setTypes(request.types());
                    Filter filter = indexService.queryParserService().parseInnerFilter(filterParser).filter();
                    context.parsedQuery(new ParsedQuery(new XConstantScoreQuery(filter), ImmutableMap.<String, Filter>of()));
                } finally {
                    QueryParseContext.removeTypes();
                    if (filterParser != null) {
                        filterParser.close();
                    }
                }
            }

            context.preProcess();

            // execute the search only gathering the hit count and bitset for each segment
            HitSetCollector termCollector = new HitSetCollector(context.searcher().getTopReaderContext().leaves().size());
            Query query = context.query();
            if (!(query instanceof ConstantScoreQuery)) {
                query = new ConstantScoreQuery(query);
            }

            context.searcher().search(query, termCollector);

            // gather the terms reading the values from the field data cache
            // the number of terms will be less than or equal to the total hits from the collector
            ResponseTerms responseTerms = ResponseTerms.get(termCollector, indexFieldData, request);
            responseTerms.process(context.searcher().getTopReaderContext().leaves());

            return new ShardTermsByQueryResponse(shardRequest.index(), shardRequest.shardId(), responseTerms);
        } catch (Throwable e) {
            logger.info("error executing shard operation", e);
            throw new QueryPhaseExecutionException(context, "failed to execute termsByQuery", e);
        } finally {
            // this will also release the index searcher
            context.clearAndRelease();
            SearchContext.removeCurrent();
        }
    }

    /*
     * Collector that tracks the total number of hits and the BitSet for each segment.
     */
    protected class HitSetCollector extends Collector {
        private final FixedBitSet[] fixedBitSets;
        private FixedBitSet current;
        private int hits;

        public HitSetCollector(int numSegments) {
            this.fixedBitSets = new FixedBitSet[numSegments];
        }

        @Override
        public void collect(int doc) throws IOException {
            current.set(doc);
            hits = hits + 1;
        }

        @Override
        public void setNextReader(AtomicReaderContext context) throws IOException {
            current = new FixedBitSet(context.reader().maxDoc());
            fixedBitSets[context.ord] = current;
        }

        @Override
        public void setScorer(Scorer scorer) throws IOException {
        }

        @Override
        public boolean acceptsDocsOutOfOrder() {
            return true;
        }

        /**
         * The total hits
         */
        public int getHits() {
            return hits;
        }

        /**
         * The BitSets for each segment
         */
        public FixedBitSet[] getFixedSets() {
            return fixedBitSets;
        }
    }

}
