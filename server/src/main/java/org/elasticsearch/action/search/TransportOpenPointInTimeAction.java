/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.search;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.ChannelActionListener;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.routing.GroupShardsIterator;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchPhaseResult;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.internal.AliasFilter;
import org.elasticsearch.search.internal.ShardSearchContextId;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportActionProxy;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportRequestHandler;
import org.elasticsearch.transport.TransportResponseHandler;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.function.BiFunction;

import static org.elasticsearch.core.Strings.format;

public class TransportOpenPointInTimeAction extends HandledTransportAction<OpenPointInTimeRequest, OpenPointInTimeResponse> {

    private static final Logger logger = LogManager.getLogger(TransportOpenPointInTimeAction.class);

    public static final String OPEN_SHARD_READER_CONTEXT_NAME = "indices:data/read/open_reader_context";
    public static final ActionType<OpenPointInTimeResponse> TYPE = new ActionType<>("indices:data/read/open_point_in_time");

    private final TransportSearchAction transportSearchAction;
    private final SearchTransportService searchTransportService;
    private final NamedWriteableRegistry namedWriteableRegistry;
    private final TransportService transportService;
    private final SearchService searchService;
    private final ClusterService clusterService;

    @Inject
    public TransportOpenPointInTimeAction(
        TransportService transportService,
        SearchService searchService,
        ActionFilters actionFilters,
        TransportSearchAction transportSearchAction,
        SearchTransportService searchTransportService,
        NamedWriteableRegistry namedWriteableRegistry,
        ClusterService clusterService
    ) {
        super(TYPE.name(), transportService, actionFilters, OpenPointInTimeRequest::new, EsExecutors.DIRECT_EXECUTOR_SERVICE);
        this.transportService = transportService;
        this.transportSearchAction = transportSearchAction;
        this.searchService = searchService;
        this.searchTransportService = searchTransportService;
        this.namedWriteableRegistry = namedWriteableRegistry;
        this.clusterService = clusterService;
        transportService.registerRequestHandler(
            OPEN_SHARD_READER_CONTEXT_NAME,
            EsExecutors.DIRECT_EXECUTOR_SERVICE,
            ShardOpenReaderRequest::new,
            new ShardOpenReaderRequestHandler()
        );
        TransportActionProxy.registerProxyAction(
            transportService,
            OPEN_SHARD_READER_CONTEXT_NAME,
            false,
            TransportOpenPointInTimeAction.ShardOpenReaderResponse::new
        );
    }

    @Override
    protected void doExecute(Task task, OpenPointInTimeRequest request, ActionListener<OpenPointInTimeResponse> listener) {
        final ClusterState clusterState = clusterService.state();
        // Check if all the nodes in this cluster know about the service
        if (request.allowPartialSearchResults() && clusterState.getMinTransportVersion().before(TransportVersions.V_8_16_0)) {
            listener.onFailure(
                new ElasticsearchStatusException(
                    format(
                        "The [allow_partial_search_results] parameter cannot be used while the cluster is still upgrading. "
                            + "Please wait until the upgrade is fully completed and try again."
                    ),
                    RestStatus.BAD_REQUEST
                )
            );
            return;
        }
        final SearchRequest searchRequest = new SearchRequest().indices(request.indices())
            .indicesOptions(request.indicesOptions())
            .preference(request.preference())
            .routing(request.routing())
            .allowPartialSearchResults(request.allowPartialSearchResults())
            .source(new SearchSourceBuilder().query(request.indexFilter()));
        searchRequest.setMaxConcurrentShardRequests(request.maxConcurrentShardRequests());
        searchRequest.setCcsMinimizeRoundtrips(false);
        transportSearchAction.executeRequest((SearchTask) task, searchRequest, listener.map(r -> {
            assert r.pointInTimeId() != null : r;
            return new OpenPointInTimeResponse(
                r.pointInTimeId(),
                r.getTotalShards(),
                r.getSuccessfulShards(),
                r.getFailedShards(),
                r.getSkippedShards()
            );
        }), searchListener -> new OpenPointInTimePhase(request, searchListener));
    }

    private final class OpenPointInTimePhase implements TransportSearchAction.SearchPhaseProvider {
        private final OpenPointInTimeRequest pitRequest;
        private final ActionListener<SearchResponse> listener;

        OpenPointInTimePhase(OpenPointInTimeRequest pitRequest, ActionListener<SearchResponse> listener) {
            this.pitRequest = pitRequest;
            this.listener = listener;
        }

        @Override
        public void runNewSearchPhase(
            SearchTask task,
            SearchRequest searchRequest,
            Executor executor,
            GroupShardsIterator<SearchShardIterator> shardIterators,
            TransportSearchAction.SearchTimeProvider timeProvider,
            BiFunction<String, String, Transport.Connection> connectionLookup,
            ClusterState clusterState,
            Map<String, AliasFilter> aliasFilter,
            Map<String, Float> concreteIndexBoosts,
            boolean preFilter,
            ThreadPool threadPool,
            SearchResponse.Clusters clusters
        ) {
            // Note: remote shards are prefiltered via can match as part of search shards. They don't need additional pre-filtering and
            // that is signaled to the local can match through the SearchShardIterator#prefiltered flag. Local shards do need to go
            // through the local can match phase.
            if (SearchService.canRewriteToMatchNone(searchRequest.source())) {
                new CanMatchPreFilterSearchPhase(
                    logger,
                    searchTransportService,
                    connectionLookup,
                    aliasFilter,
                    concreteIndexBoosts,
                    threadPool.executor(ThreadPool.Names.SEARCH_COORDINATION),
                    searchRequest,
                    shardIterators,
                    timeProvider,
                    task,
                    false,
                    searchService.getCoordinatorRewriteContextProvider(timeProvider::absoluteStartMillis),
                    listener.delegateFailureAndWrap(
                        (searchResponseActionListener, searchShardIterators) -> runOpenPointInTimePhase(
                            task,
                            searchRequest,
                            executor,
                            searchShardIterators,
                            timeProvider,
                            connectionLookup,
                            clusterState,
                            aliasFilter,
                            concreteIndexBoosts,
                            clusters
                        )
                    )
                ).start();
            } else {
                runOpenPointInTimePhase(
                    task,
                    searchRequest,
                    executor,
                    shardIterators,
                    timeProvider,
                    connectionLookup,
                    clusterState,
                    aliasFilter,
                    concreteIndexBoosts,
                    clusters
                );
            }
        }

        void runOpenPointInTimePhase(
            SearchTask task,
            SearchRequest searchRequest,
            Executor executor,
            GroupShardsIterator<SearchShardIterator> shardIterators,
            TransportSearchAction.SearchTimeProvider timeProvider,
            BiFunction<String, String, Transport.Connection> connectionLookup,
            ClusterState clusterState,
            Map<String, AliasFilter> aliasFilter,
            Map<String, Float> concreteIndexBoosts,
            SearchResponse.Clusters clusters
        ) {
            assert searchRequest.getMaxConcurrentShardRequests() == pitRequest.maxConcurrentShardRequests()
                : searchRequest.getMaxConcurrentShardRequests() + " != " + pitRequest.maxConcurrentShardRequests();
            new AbstractSearchAsyncAction<>(
                actionName,
                logger,
                namedWriteableRegistry,
                searchTransportService,
                connectionLookup,
                aliasFilter,
                concreteIndexBoosts,
                executor,
                searchRequest,
                listener,
                shardIterators,
                timeProvider,
                clusterState,
                task,
                new ArraySearchPhaseResults<>(shardIterators.size()),
                searchRequest.getMaxConcurrentShardRequests(),
                clusters
            ) {
                protected String missingShardsErrorMessage(StringBuilder missingShards) {
                    return "[open_point_in_time] action requires all shards to be available. Missing shards: ["
                        + missingShards
                        + "].  Consider using `allow_partial_search_results` setting to bypass this error.";
                }

                @Override
                protected void executePhaseOnShard(
                    SearchShardIterator shardIt,
                    Transport.Connection connection,
                    SearchActionListener<SearchPhaseResult> phaseListener
                ) {
                    transportService.sendChildRequest(
                        connection,
                        OPEN_SHARD_READER_CONTEXT_NAME,
                        new ShardOpenReaderRequest(shardIt.shardId(), shardIt.getOriginalIndices(), pitRequest.keepAlive()),
                        task,
                        new ActionListenerResponseHandler<>(
                            phaseListener,
                            ShardOpenReaderResponse::new,
                            TransportResponseHandler.TRANSPORT_WORKER
                        )
                    );
                }

                @Override
                protected SearchPhase getNextPhase() {
                    return new SearchPhase(getName()) {
                        @Override
                        public void run() {
                            sendSearchResponse(SearchResponseSections.EMPTY_WITH_TOTAL_HITS, results.getAtomicArray());
                        }
                    };
                }

                @Override
                boolean buildPointInTimeFromSearchResults() {
                    return true;
                }
            }.start();
        }
    }

    private static final class ShardOpenReaderRequest extends TransportRequest implements IndicesRequest {
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

    private static final class ShardOpenReaderResponse extends SearchPhaseResult {
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

    private class ShardOpenReaderRequestHandler implements TransportRequestHandler<ShardOpenReaderRequest> {
        @Override
        public void messageReceived(ShardOpenReaderRequest request, TransportChannel channel, Task task) {
            searchService.openReaderContext(
                request.getShardId(),
                request.keepAlive,
                new ChannelActionListener<>(channel).map(ShardOpenReaderResponse::new)
            );
        }
    }
}
