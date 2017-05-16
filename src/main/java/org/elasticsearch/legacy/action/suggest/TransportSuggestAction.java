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

package org.elasticsearch.legacy.action.suggest;

import org.elasticsearch.legacy.ElasticsearchException;
import org.elasticsearch.legacy.ElasticsearchIllegalArgumentException;
import org.elasticsearch.legacy.action.ShardOperationFailedException;
import org.elasticsearch.legacy.action.support.DefaultShardOperationFailedException;
import org.elasticsearch.legacy.action.support.broadcast.BroadcastShardOperationFailedException;
import org.elasticsearch.legacy.action.support.broadcast.TransportBroadcastOperationAction;
import org.elasticsearch.legacy.cluster.ClusterService;
import org.elasticsearch.legacy.cluster.ClusterState;
import org.elasticsearch.legacy.cluster.block.ClusterBlockException;
import org.elasticsearch.legacy.cluster.block.ClusterBlockLevel;
import org.elasticsearch.legacy.cluster.routing.GroupShardsIterator;
import org.elasticsearch.legacy.cluster.routing.ShardRouting;
import org.elasticsearch.legacy.common.bytes.BytesReference;
import org.elasticsearch.legacy.common.inject.Inject;
import org.elasticsearch.legacy.common.settings.Settings;
import org.elasticsearch.legacy.common.xcontent.XContentFactory;
import org.elasticsearch.legacy.common.xcontent.XContentParser;
import org.elasticsearch.legacy.index.engine.Engine;
import org.elasticsearch.legacy.index.service.IndexService;
import org.elasticsearch.legacy.index.shard.service.IndexShard;
import org.elasticsearch.legacy.index.suggest.stats.ShardSuggestService;
import org.elasticsearch.legacy.indices.IndicesService;
import org.elasticsearch.legacy.search.suggest.Suggest;
import org.elasticsearch.legacy.search.suggest.SuggestPhase;
import org.elasticsearch.legacy.search.suggest.SuggestionSearchContext;
import org.elasticsearch.legacy.threadpool.ThreadPool;
import org.elasticsearch.legacy.transport.TransportService;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReferenceArray;

import static com.google.common.collect.Lists.newArrayList;

/**
 * Defines the transport of a suggestion request across the cluster
 */
public class TransportSuggestAction extends TransportBroadcastOperationAction<SuggestRequest, SuggestResponse, ShardSuggestRequest, ShardSuggestResponse> {

    private final IndicesService indicesService;

    private final SuggestPhase suggestPhase;

    @Inject
    public TransportSuggestAction(Settings settings, ThreadPool threadPool, ClusterService clusterService, TransportService transportService,
                                  IndicesService indicesService, SuggestPhase suggestPhase) {
        super(settings, SuggestAction.NAME, threadPool, clusterService, transportService);
        this.indicesService = indicesService;
        this.suggestPhase = suggestPhase;
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.SUGGEST;
    }

    @Override
    protected SuggestRequest newRequest() {
        return new SuggestRequest();
    }

    @Override
    protected ShardSuggestRequest newShardRequest() {
        return new ShardSuggestRequest();
    }

    @Override
    protected ShardSuggestRequest newShardRequest(int numShards, ShardRouting shard, SuggestRequest request) {
        return new ShardSuggestRequest(shard.index(), shard.id(), request);
    }

    @Override
    protected ShardSuggestResponse newShardResponse() {
        return new ShardSuggestResponse();
    }

    @Override
    protected GroupShardsIterator shards(ClusterState clusterState, SuggestRequest request, String[] concreteIndices) {
        Map<String, Set<String>> routingMap = clusterState.metaData().resolveSearchRouting(request.routing(), request.indices());
        return clusterService.operationRouting().searchShards(clusterState, request.indices(), concreteIndices, routingMap, request.preference());
    }

    @Override
    protected ClusterBlockException checkGlobalBlock(ClusterState state, SuggestRequest request) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.READ);
    }

    @Override
    protected ClusterBlockException checkRequestBlock(ClusterState state, SuggestRequest countRequest, String[] concreteIndices) {
        return state.blocks().indicesBlockedException(ClusterBlockLevel.READ, concreteIndices);
    }

    @Override
    protected SuggestResponse newResponse(SuggestRequest request, AtomicReferenceArray shardsResponses, ClusterState clusterState) {
        int successfulShards = 0;
        int failedShards = 0;

        final Map<String, List<Suggest.Suggestion>> groupedSuggestions = new HashMap<>();

        List<ShardOperationFailedException> shardFailures = null;
        for (int i = 0; i < shardsResponses.length(); i++) {
            Object shardResponse = shardsResponses.get(i);
            if (shardResponse == null) {
                // simply ignore non active shards
            } else if (shardResponse instanceof BroadcastShardOperationFailedException) {
                failedShards++;
                if (shardFailures == null) {
                    shardFailures = newArrayList();
                }
                shardFailures.add(new DefaultShardOperationFailedException((BroadcastShardOperationFailedException) shardResponse));
            } else {
                Suggest suggest = ((ShardSuggestResponse) shardResponse).getSuggest();
                Suggest.group(groupedSuggestions, suggest);
                successfulShards++;
            }
        }

        return new SuggestResponse(new Suggest(Suggest.reduce(groupedSuggestions)), shardsResponses.length(), successfulShards, failedShards, shardFailures);
    }

    @Override
    protected ShardSuggestResponse shardOperation(ShardSuggestRequest request) throws ElasticsearchException {
        IndexService indexService = indicesService.indexServiceSafe(request.index());
        IndexShard indexShard = indexService.shardSafe(request.shardId());
        final Engine.Searcher searcher = indexShard.acquireSearcher("suggest");
        ShardSuggestService shardSuggestService = indexShard.shardSuggestService();
        shardSuggestService.preSuggest();
        long startTime = System.nanoTime();
        XContentParser parser = null;
        try {
            BytesReference suggest = request.suggest();
            if (suggest != null && suggest.length() > 0) {
                parser = XContentFactory.xContent(suggest).createParser(suggest);
                if (parser.nextToken() != XContentParser.Token.START_OBJECT) {
                    throw new ElasticsearchIllegalArgumentException("suggest content missing");
                }
                final SuggestionSearchContext context = suggestPhase.parseElement().parseInternal(parser, indexService.mapperService(), request.index(), request.shardId());
                final Suggest result = suggestPhase.execute(context, searcher.reader());
                return new ShardSuggestResponse(request.index(), request.shardId(), result);
            }
            return new ShardSuggestResponse(request.index(), request.shardId(), new Suggest());
        } catch (Throwable ex) {
            throw new ElasticsearchException("failed to execute suggest", ex);
        } finally {
            searcher.close();
            if (parser != null) {
                parser.close();
            }
            shardSuggestService.postSuggest(System.nanoTime() - startTime);
        }
    }
}
