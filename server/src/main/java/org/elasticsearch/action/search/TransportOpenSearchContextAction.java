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

package org.elasticsearch.action.search;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.routing.GroupShardsIterator;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.AtomicArray;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.SearchPhaseResult;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.internal.AliasFilter;
import org.elasticsearch.search.internal.InternalSearchResponse;
import org.elasticsearch.search.internal.ShardSearchContextId;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.function.BiFunction;

public class TransportOpenSearchContextAction extends HandledTransportAction<OpenSearchContextRequest, OpenSearchContextResponse> {
    public static final String NAME = "indices:data/read/open_search_context";
    public static final ActionType<OpenSearchContextResponse> INSTANCE = new ActionType<>(NAME, OpenSearchContextResponse::new);

    private final TransportSearchAction transportSearchAction;
    private final SearchTransportService searchTransportService;

    @Inject
    public TransportOpenSearchContextAction(TransportService transportService, SearchTransportService searchTransportService,
                                            ActionFilters actionFilters, TransportSearchAction transportSearchAction) {
        super(NAME, transportService, actionFilters, OpenSearchContextRequest::new);
        this.transportSearchAction = transportSearchAction;
        this.searchTransportService = searchTransportService;
    }

    @Override
    protected void doExecute(Task task, OpenSearchContextRequest request, ActionListener<OpenSearchContextResponse> listener) {
        final TransportSearchAction.SearchAsyncActionProvider actionProvider = new TransportSearchAction.SearchAsyncActionProvider() {
            @Override
            public AbstractSearchAsyncAction<? extends SearchPhaseResult> asyncSearchAction(
                SearchTask task, SearchRequest searchRequest, GroupShardsIterator<SearchShardIterator> shardIterators,
                TransportSearchAction.SearchTimeProvider timeProvider, BiFunction<String, String, Transport.Connection> connectionLookup,
                ClusterState clusterState, Map<String, AliasFilter> aliasFilter, Map<String, Float> concreteIndexBoosts,
                Map<String, Set<String>> indexRoutings, ActionListener<SearchResponse> listener, boolean preFilter,
                ThreadPool threadPool, SearchResponse.Clusters clusters) {
                final Executor executor = threadPool.executor(ThreadPool.Names.SEARCH);
                return new OpenReaderSearchPhase(request, logger, searchTransportService, connectionLookup,
                    aliasFilter, concreteIndexBoosts, indexRoutings, executor, searchRequest, listener, shardIterators,
                    timeProvider, clusterState, task, clusters);
            }
        };
        final SearchRequest searchRequest = new SearchRequest()
            .indices(request.indices())
            .indicesOptions(request.indicesOptions())
            .preference(request.preference())
            .routing(request.routing())
            .allowPartialSearchResults(false);
        transportSearchAction.executeRequest(task, searchRequest, actionProvider,
            ActionListener.map(listener, r -> new OpenSearchContextResponse(r.searchContextId())));
    }

    static final class ShardOpenReaderRequest extends TransportRequest implements IndicesRequest {
        final ShardId shardId;
        final OriginalIndices originalIndices;
        final TimeValue keepAlive;

        ShardOpenReaderRequest(ShardId shardId, OriginalIndices originalIndices, TimeValue keepAlive) {
            this.shardId = shardId;
            this.originalIndices = originalIndices;
            this.keepAlive = keepAlive;
        }

        ShardOpenReaderRequest(StreamInput in) throws IOException {
            super(in);
            shardId = new ShardId(in);
            originalIndices = OriginalIndices.readOriginalIndices(in);
            keepAlive = in.readTimeValue();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            shardId.writeTo(out);
            OriginalIndices.writeOriginalIndices(originalIndices, out);
            out.writeTimeValue(keepAlive);
        }

        public ShardId getShardId() {
            return shardId;
        }

        @Override
        public String[] indices() {
            return originalIndices.indices();
        }

        @Override
        public IndicesOptions indicesOptions() {
            return originalIndices.indicesOptions();
        }
    }

    static final class ShardOpenReaderResponse extends SearchPhaseResult {
        ShardOpenReaderResponse(ShardSearchContextId contextId) {
            this.contextId = contextId;
        }

        ShardOpenReaderResponse(StreamInput in) throws IOException {
            super(in);
            contextId = new ShardSearchContextId(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            contextId.writeTo(out);
        }
    }

    static final class OpenReaderSearchPhase extends AbstractSearchAsyncAction<SearchPhaseResult> {
        final OpenSearchContextRequest request;

        OpenReaderSearchPhase(OpenSearchContextRequest request, Logger logger, SearchTransportService searchTransportService,
                              BiFunction<String, String, Transport.Connection> nodeIdToConnection,
                              Map<String, AliasFilter> aliasFilter, Map<String, Float> concreteIndexBoosts,
                              Map<String, Set<String>> indexRoutings, Executor executor, SearchRequest searchRequest,
                              ActionListener<SearchResponse> listener, GroupShardsIterator<SearchShardIterator> shardsIts,
                              TransportSearchAction.SearchTimeProvider timeProvider, ClusterState clusterState,
                              SearchTask task, SearchResponse.Clusters clusters) {
            super("open_search_context", logger, searchTransportService, nodeIdToConnection, aliasFilter, concreteIndexBoosts,
                indexRoutings, executor, searchRequest, listener, shardsIts, timeProvider, clusterState, task,
                new ArraySearchPhaseResults<>(shardsIts.size()), shardsIts.size(), clusters);
            this.request = request;
        }

        @Override
        protected void executePhaseOnShard(SearchShardIterator shardIt, ShardRouting shard,
                                           SearchActionListener<SearchPhaseResult> listener) {
            final Transport.Connection connection = getConnection(shardIt.getClusterAlias(), shard.currentNodeId());
            final SearchShardTarget searchShardTarget = shardIt.newSearchShardTarget(shard.currentNodeId());
            final ShardOpenReaderRequest shardRequest = new ShardOpenReaderRequest(searchShardTarget.getShardId(),
                searchShardTarget.getOriginalIndices(), request.keepAlive());
            getSearchTransport().sendOpenShardReaderContext(connection, getTask(), shardRequest, ActionListener.map(listener, r -> r));
        }

        @Override
        protected SearchPhase getNextPhase(SearchPhaseResults<SearchPhaseResult> results, SearchPhaseContext context) {
            return new SearchPhase(getName()) {
                @Override
                public void run() {
                    final AtomicArray<SearchPhaseResult> atomicArray = results.getAtomicArray();
                    sendSearchResponse(InternalSearchResponse.empty(), atomicArray);
                }
            };
        }

        @Override
        boolean includeSearchContextInResponse() {
            return true;
        }
    }
}
