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
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.routing.GroupShardsIterator;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.AtomicArray;
import org.elasticsearch.search.SearchPhaseResult;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.internal.AliasFilter;
import org.elasticsearch.search.internal.InternalSearchResponse;
import org.elasticsearch.search.internal.SearchContextId;
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

public class TransportOpenReaderAction extends HandledTransportAction<OpenReaderRequest, OpenReaderResponse> {
    public static final String NAME = "indices:data/read/open_reader";
    public static final ActionType<OpenReaderResponse> INSTANCE = new ActionType<>(NAME, OpenReaderResponse::new);

    private final TransportSearchAction transportSearchAction;
    private final SearchTransportService searchTransportService;

    @Inject
    public TransportOpenReaderAction(TransportService transportService, SearchTransportService searchTransportService,
                                     ActionFilters actionFilters, TransportSearchAction transportSearchAction) {
        super(NAME, transportService, actionFilters, OpenReaderRequest::new);
        this.transportSearchAction = transportSearchAction;
        this.searchTransportService = searchTransportService;
    }

    @Override
    protected void doExecute(Task task, OpenReaderRequest openReaderRequest, ActionListener<OpenReaderResponse> listener) {
        final TransportSearchAction.SearchAsyncActionProvider actionProvider = new TransportSearchAction.SearchAsyncActionProvider() {
            @Override
            public AbstractSearchAsyncAction<? extends SearchPhaseResult> asyncSearchAction(
                SearchTask task, SearchRequest searchRequest, GroupShardsIterator<SearchShardIterator> shardIterators,
                TransportSearchAction.SearchTimeProvider timeProvider, BiFunction<String, String, Transport.Connection> connectionLookup,
                ClusterState clusterState, Map<String, AliasFilter> aliasFilter, Map<String, Float> concreteIndexBoosts,
                Map<String, Set<String>> indexRoutings, ActionListener<SearchResponse> listener, boolean preFilter,
                ThreadPool threadPool, SearchResponse.Clusters clusters) {
                final Executor executor = threadPool.executor(ThreadPool.Names.SEARCH);
                return new OpenReaderSearchPhase(openReaderRequest, logger, searchTransportService, connectionLookup,
                    aliasFilter, concreteIndexBoosts, indexRoutings, executor, searchRequest, listener, shardIterators,
                    timeProvider, clusterState, task, clusters);
            }
        };
        final SearchRequest searchRequest = new SearchRequest()
            .indices(openReaderRequest.indices())
            .indicesOptions(openReaderRequest.indicesOptions())
            .preference(openReaderRequest.preference())
            .routing(openReaderRequest.routing())
            .allowPartialSearchResults(false);
        transportSearchAction.executeRequest(task, searchRequest, actionProvider,
            ActionListener.map(listener, r -> new OpenReaderResponse(r.getReaderId())));
    }

    static final class ShardOpenReaderRequest extends TransportRequest {
        final SearchShardTarget searchShardTarget;
        final TimeValue keepAlive;

        ShardOpenReaderRequest(SearchShardTarget searchShardTarget, TimeValue keepAlive) {
            this.searchShardTarget = searchShardTarget;
            this.keepAlive = keepAlive;
        }

        ShardOpenReaderRequest(StreamInput in) throws IOException {
            super(in);
            searchShardTarget = new SearchShardTarget(in);
            keepAlive = in.readTimeValue();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            searchShardTarget.writeTo(out);
            out.writeTimeValue(keepAlive);
        }
    }

    static final class ShardOpenReaderResponse extends SearchPhaseResult {
        ShardOpenReaderResponse(SearchContextId contextId, SearchShardTarget searchShardTarget) {
            this.contextId = contextId;
            setSearchShardTarget(searchShardTarget);
        }

        ShardOpenReaderResponse(StreamInput in) throws IOException {
            super(in);
            contextId = new SearchContextId(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            contextId.writeTo(out);
        }
    }

    static final class OpenReaderSearchPhase extends AbstractSearchAsyncAction<SearchPhaseResult> {
        final OpenReaderRequest openReaderRequest;

        OpenReaderSearchPhase(OpenReaderRequest openReaderRequest, Logger logger, SearchTransportService searchTransportService,
                              BiFunction<String, String, Transport.Connection> nodeIdToConnection,
                              Map<String, AliasFilter> aliasFilter, Map<String, Float> concreteIndexBoosts,
                              Map<String, Set<String>> indexRoutings, Executor executor, SearchRequest request,
                              ActionListener<SearchResponse> listener, GroupShardsIterator<SearchShardIterator> shardsIts,
                              TransportSearchAction.SearchTimeProvider timeProvider, ClusterState clusterState,
                              SearchTask task, SearchResponse.Clusters clusters) {
            super("open_reader", logger, searchTransportService, nodeIdToConnection, aliasFilter, concreteIndexBoosts, indexRoutings,
                executor, request, listener, shardsIts, timeProvider, clusterState, task,
                new ArraySearchPhaseResults<>(shardsIts.size()), shardsIts.size(), clusters);
            this.openReaderRequest = openReaderRequest;
        }

        @Override
        protected void executePhaseOnShard(SearchShardIterator shardIt, ShardRouting shard,
                                           SearchActionListener<SearchPhaseResult> listener) {
            final Transport.Connection connection = getConnection(shardIt.getClusterAlias(), shard.currentNodeId());
            final SearchShardTarget searchShardTarget = shardIt.newSearchShardTarget(shard.currentNodeId());
            final ShardOpenReaderRequest shardRequest = new ShardOpenReaderRequest(searchShardTarget, openReaderRequest.keepAlive());
            getSearchTransport().sendShardOpenReader(connection, getTask(), shardRequest, ActionListener.map(listener, r -> r));
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
        boolean includeReaderIdInResponse() {
            return true;
        }
    }
}
