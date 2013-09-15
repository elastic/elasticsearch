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

package org.elasticsearch.action.explain;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.Explanation;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.single.shard.TransportShardSingleOperationAction;
import org.elasticsearch.cache.recycler.CacheRecycler;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.routing.ShardIterator;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.get.GetResult;
import org.elasticsearch.index.mapper.Uid;
import org.elasticsearch.index.mapper.internal.UidFieldMapper;
import org.elasticsearch.index.query.ParsedQuery;
import org.elasticsearch.index.service.IndexService;
import org.elasticsearch.index.shard.service.IndexShard;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.internal.ShardSearchRequest;
import org.elasticsearch.search.rescore.RescoreSearchContext;
import org.elasticsearch.search.rescore.Rescorer;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;

/**
 * Explain transport action. Computes the explain on the targeted shard.
 */
// TODO: AggregatedDfs. Currently the idf can be different then when executing a normal search with explain.
public class TransportExplainAction extends TransportShardSingleOperationAction<ExplainRequest, ExplainResponse> {

    private final IndicesService indicesService;

    private final ScriptService scriptService;

    private final CacheRecycler cacheRecycler;

    @Inject
    public TransportExplainAction(Settings settings, ThreadPool threadPool, ClusterService clusterService,
                                  TransportService transportService, IndicesService indicesService,
                                  ScriptService scriptService, CacheRecycler cacheRecycler) {
        super(settings, threadPool, clusterService, transportService);
        this.indicesService = indicesService;
        this.scriptService = scriptService;
        this.cacheRecycler = cacheRecycler;
    }

    @Override
    protected void doExecute(ExplainRequest request, ActionListener<ExplainResponse> listener) {
        request.nowInMillis = System.currentTimeMillis();
        super.doExecute(request, listener);
    }

    protected String transportAction() {
        return ExplainAction.NAME;
    }

    protected String executor() {
        return ThreadPool.Names.GET; // Or use Names.SEARCH?
    }

    @Override
    protected void resolveRequest(ClusterState state, ExplainRequest request) {
        String concreteIndex = state.metaData().concreteIndex(request.index());
        request.filteringAlias(state.metaData().filteringAliases(concreteIndex, request.index()));
        request.index(state.metaData().concreteIndex(request.index()));
    }

    protected ExplainResponse shardOperation(ExplainRequest request, int shardId) throws ElasticSearchException {
        IndexService indexService = indicesService.indexService(request.index());
        IndexShard indexShard = indexService.shardSafe(shardId);
        Term uidTerm = new Term(UidFieldMapper.NAME, Uid.createUidAsBytes(request.type(), request.id()));
        Engine.GetResult result = indexShard.get(new Engine.Get(false, uidTerm));
        if (!result.exists()) {
            return new ExplainResponse(false);
        }

        SearchContext context = new SearchContext(
                0,
                new ShardSearchRequest().types(new String[]{request.type()})
                        .filteringAliases(request.filteringAlias())
                        .nowInMillis(request.nowInMillis),
                null, result.searcher(), indexService, indexShard,
                scriptService, cacheRecycler
        );
        SearchContext.setCurrent(context);

        try {
            context.parsedQuery(parseQuery(request, indexService));
            context.preProcess();
            int topLevelDocId = result.docIdAndVersion().docId + result.docIdAndVersion().reader.docBase;
            Explanation explanation;
            if (context.rescore() != null) {
                RescoreSearchContext ctx = context.rescore();
                Rescorer rescorer = ctx.rescorer();
                explanation = rescorer.explain(topLevelDocId, context, ctx);
            } else {
                explanation = context.searcher().explain(context.query(), topLevelDocId);
            }
            if (request.fields() != null) {
                if (request.fields().length == 1 && "_source".equals(request.fields()[0])) {
                    request.fields(null); // Load the _source field
                }
                // Advantage is that we're not opening a second searcher to retrieve the _source. Also
                // because we are working in the same searcher in engineGetResult we can be sure that a
                // doc isn't deleted between the initial get and this call.
                GetResult getResult = indexShard.getService().get(result, request.id(), request.type(), request.fields());
                return new ExplainResponse(true, explanation, getResult);
            } else {
                return new ExplainResponse(true, explanation);
            }
        } catch (IOException e) {
            throw new ElasticSearchException("Could not explain", e);
        } finally {
            context.release();
            SearchContext.removeCurrent();
        }
    }

    private ParsedQuery parseQuery(ExplainRequest request, IndexService indexService) {
        try {
            XContentParser parser = XContentHelper.createParser(request.source());
            for (XContentParser.Token token = parser.nextToken(); token != XContentParser.Token.END_OBJECT; token = parser.nextToken()) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    String fieldName = parser.currentName();
                    if ("query".equals(fieldName)) {
                        return indexService.queryParserService().parse(parser);
                    } else if ("query_binary".equals(fieldName)) {
                        byte[] querySource = parser.binaryValue();
                        XContentParser qSourceParser = XContentFactory.xContent(querySource).createParser(querySource);
                        return indexService.queryParserService().parse(qSourceParser);
                    }
                }
            }
        } catch (Exception e) {
            throw new ElasticSearchException("Couldn't parse query from source.", e);
        }

        throw new ElasticSearchException("No query specified");
    }

    protected ExplainRequest newRequest() {
        return new ExplainRequest();
    }

    protected ExplainResponse newResponse() {
        return new ExplainResponse();
    }

    protected ClusterBlockException checkGlobalBlock(ClusterState state, ExplainRequest request) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.READ);
    }

    protected ClusterBlockException checkRequestBlock(ClusterState state, ExplainRequest request) {
        return state.blocks().indexBlockedException(ClusterBlockLevel.READ, request.index());
    }

    protected ShardIterator shards(ClusterState state, ExplainRequest request) throws ElasticSearchException {
        return clusterService.operationRouting().getShards(
                clusterService.state(), request.index(), request.type(), request.id(), request.routing(), request.preference()
        );
    }
}
