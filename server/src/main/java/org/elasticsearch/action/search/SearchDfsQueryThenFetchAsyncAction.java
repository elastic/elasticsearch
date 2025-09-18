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
import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.SearchPhaseResult;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.dfs.DfsSearchResult;
import org.elasticsearch.search.internal.AliasFilter;
import org.elasticsearch.transport.Transport;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Executor;
import java.util.function.BiFunction;

final class SearchDfsQueryThenFetchAsyncAction extends AbstractSearchAsyncAction<DfsSearchResult> {

    private final SearchPhaseResults<SearchPhaseResult> queryPhaseResultConsumer;
    private final SearchProgressListener progressListener;
    private final Client client;
    private final TransportVersion minTransportVersion;

    SearchDfsQueryThenFetchAsyncAction(
        Logger logger,
        NamedWriteableRegistry namedWriteableRegistry,
        SearchTransportService searchTransportService,
        BiFunction<String, String, Transport.Connection> nodeIdToConnection,
        Map<String, AliasFilter> aliasFilter,
        Map<String, Float> concreteIndexBoosts,
        Executor executor,
        SearchPhaseResults<SearchPhaseResult> queryPhaseResultConsumer,
        SearchRequest request,
        ActionListener<SearchResponse> listener,
        List<SearchShardIterator> shardsIts,
        TransportSearchAction.SearchTimeProvider timeProvider,
        ClusterState clusterState,
        SearchTask task,
        SearchResponse.Clusters clusters,
        Client client
    ) {
        super(
            "dfs",
            logger,
            namedWriteableRegistry,
            searchTransportService,
            nodeIdToConnection,
            aliasFilter,
            concreteIndexBoosts,
            executor,
            request,
            listener,
            shardsIts,
            timeProvider,
            clusterState,
            task,
            new ArraySearchPhaseResults<>(shardsIts.size()),
            request.getMaxConcurrentShardRequests(),
            clusters
        );
        this.queryPhaseResultConsumer = queryPhaseResultConsumer;
        addReleasable(queryPhaseResultConsumer);
        this.progressListener = task.getProgressListener();
        // don't build the SearchShard list (can be expensive) if the SearchProgressListener won't use it
        if (progressListener != SearchProgressListener.NOOP) {
            notifyListShards(progressListener, clusters, request, shardsIts);
        }
        this.client = client;
        this.minTransportVersion = clusterState.getMinTransportVersion();
    }

    @Override
    protected void executePhaseOnShard(
        final SearchShardIterator shardIt,
        final Transport.Connection connection,
        final SearchActionListener<DfsSearchResult> listener
    ) {
        getSearchTransport().sendExecuteDfs(connection, buildShardSearchRequest(shardIt, listener.requestIndex), getTask(), listener);
    }

    @Override
    protected SearchPhase getNextPhase() {
        return new DfsQueryPhase(queryPhaseResultConsumer, client, this);
    }

    @Override
    protected void onShardGroupFailure(int shardIndex, SearchShardTarget shardTarget, Exception exc) {
        progressListener.notifyQueryFailure(shardIndex, shardTarget, exc);
    }

    private static final Logger logger = LogManager.getLogger(SearchQueryThenFetchAsyncAction.class);

    // TODO share this with change in SearchQueryThenFetchAsyncAction
    protected BytesReference buildSearchContextId(ShardSearchFailure[] failures) {
        SearchSourceBuilder source = request.source();
        // only (re-)build a search context id if we have a point in time
        if (source != null && source.pointInTimeBuilder() != null && source.pointInTimeBuilder().singleSession() == false) {
            boolean idChanged = false;
            BytesReference currentId = source.pointInTimeBuilder().getEncodedId();
            // we want to change node ids in the PIT id if any shards and its PIT context have moved
            SearchContextId originalSearchContextId = source.pointInTimeBuilder().getSearchContextId(getNamedWriteableRegistry());
            Map<ShardId, SearchContextIdForNode> shardMapCopy = new HashMap<>(originalSearchContextId.shards());
            for (SearchPhaseResult result : results.getAtomicArray().asList()) {
                SearchShardTarget searchShardTarget = result.getSearchShardTarget();
                ShardId shardId = searchShardTarget.getShardId();
                SearchContextIdForNode original = shardMapCopy.get(shardId);
                if (original != null
                    && Objects.equals(original.getClusterAlias(), searchShardTarget.getClusterAlias())
                    && Objects.equals(original.getSearchContextId(), result.getContextId())) {
                    // result shard and context id match the original one, check if the node is different and replace if so
                    String originalNode = original.getNode();
                    if (originalNode != null && originalNode.equals(searchShardTarget.getNodeId()) == false) {
                        shardMapCopy.put(
                            shardId,
                            new SearchContextIdForNode(
                                original.getClusterAlias(),
                                searchShardTarget.getNodeId(),
                                original.getSearchContextId()
                            )
                        );
                        idChanged = true;
                    }
                }
            }
            if (idChanged) {
                BytesReference newId = SearchContextId.encode(
                    shardMapCopy,
                    originalSearchContextId.aliasFilter(),
                    minTransportVersion,
                    failures
                );
                logger.debug("Changing PIT id to [{}]", newId);
                return newId;
            } else {
                logger.debug("Keeping original PIT id");
                return currentId;
            }
        } else {
            return super.buildSearchContextId(failures);
        }
    }
}
