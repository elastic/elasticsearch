/*
 * ELASTICSEARCH CONFIDENTIAL
 * __________________
 *
 * Copyright Elasticsearch B.V. All rights reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Elasticsearch B.V. and its suppliers, if any.
 * The intellectual and technical concepts contained herein
 * are proprietary to Elasticsearch B.V. and its suppliers and
 * may be covered by U.S. and Foreign Patents, patents in
 * process, and are protected by trade secret or copyright
 * law.  Dissemination of this information or reproduction of
 * this material is strictly forbidden unless prior written
 * permission is obtained from Elasticsearch B.V.
 */

package co.elastic.elasticsearch.stateless.action;

import co.elastic.elasticsearch.stateless.Stateless;
import co.elastic.elasticsearch.stateless.commits.ClosedShardService;
import co.elastic.elasticsearch.stateless.engine.PrimaryTermAndGeneration;
import co.elastic.elasticsearch.stateless.engine.SearchEngine;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.action.support.nodes.TransportNodesAction;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.common.Strings.format;

/**
 * Broadcasts a {@link FetchShardCommitsInUseAction.Request} to a set of search nodes, asks each node what commits are still in use for a
 * particular shard, returning a {@link FetchShardCommitsInUseAction.Response} to the caller containing all the responses.
 * <p>
 * See {@link #nodeOperationAsync} for details about the receiving search node logic.
 */
public class TransportFetchShardCommitsInUseAction extends TransportNodesAction<
    FetchShardCommitsInUseAction.Request,
    FetchShardCommitsInUseAction.Response,
    FetchShardCommitsInUseAction.NodeRequest,
    FetchShardCommitsInUseAction.NodeResponse,
    Void> {
    private static final Logger logger = LogManager.getLogger(TransportFetchShardCommitsInUseAction.class);

    public static final String NAME = "internal:admin/" + Stateless.NAME + "/search/fetch/commits";
    public static final ActionType<FetchShardCommitsInUseAction.Response> TYPE = new ActionType<>(NAME);

    protected final ClusterService clusterService;
    private final ClosedShardService closedShardService;
    private final IndicesService indicesService;

    @Inject
    public TransportFetchShardCommitsInUseAction(
        ThreadPool threadPool,
        ClusterService clusterService,
        TransportService transportService,
        ClosedShardService closedShardService,
        IndicesService indicesService,
        ActionFilters actionFilters
    ) {
        super(NAME, clusterService, transportService, actionFilters, FetchShardCommitsInUseAction.NodeRequest::new, threadPool.generic());
        this.clusterService = clusterService;
        this.closedShardService = closedShardService;
        this.indicesService = indicesService;
    }

    @Override
    protected FetchShardCommitsInUseAction.Response newResponse(
        FetchShardCommitsInUseAction.Request request,
        List<FetchShardCommitsInUseAction.NodeResponse> nodeResponses,
        List<FailedNodeException> nodeFailures
    ) {
        return new FetchShardCommitsInUseAction.Response(clusterService.getClusterName(), nodeResponses, nodeFailures);
    }

    @Override
    protected FetchShardCommitsInUseAction.NodeRequest newNodeRequest(FetchShardCommitsInUseAction.Request request) {
        return new FetchShardCommitsInUseAction.NodeRequest(request);
    }

    @Override
    protected FetchShardCommitsInUseAction.NodeResponse newNodeResponse(StreamInput in, DiscoveryNode node) throws IOException {
        return new FetchShardCommitsInUseAction.NodeResponse(in);
    }

    private void completeListenerWithClosedShardService(
        ActionListener<FetchShardCommitsInUseAction.NodeResponse> listener,
        ShardId shardId
    ) {
        ActionListener.completeWith(
            listener,
            () -> new FetchShardCommitsInUseAction.NodeResponse(
                clusterService.localNode(),
                getPrimaryTermAndGenerationsFromClosedShardService(shardId)
            )
        );
    }

    private Set<PrimaryTermAndGeneration> getPrimaryTermAndGenerationsFromClosedShardService(ShardId shardId) {
        return closedShardService.getPrimaryTermAndGenerations(shardId);
    }

    /**
     * Looks up what shard commits are in active use by search operations on a node receiving the request.
     */
    @Override
    protected void nodeOperationAsync(
        FetchShardCommitsInUseAction.NodeRequest request,
        Task task,
        ActionListener<FetchShardCommitsInUseAction.NodeResponse> listener
    ) {
        logger.trace(() -> format("received fetch-shard-commits-in-use request [%s]", request));

        // If the search shard or index is gone, check the ClosedShardService to see if active reader state was carried over on shard
        // close.
        IndexService indexService = indicesService.indexService(request.getShardId().getIndex());
        if (indexService == null) {
            completeListenerWithClosedShardService(listener, request.getShardId());
            return;
        }
        IndexShard shard = indexService.getShardOrNull(request.getShardId().id());
        if (shard == null) {
            completeListenerWithClosedShardService(listener, request.getShardId());
            return;
        }

        // Since there's an IndexShard, try to access the SearchEngine to find what commits are in use.
        SubscribableListener
            // Wait to ensure the shard is no longer in the process of starting up: when this step completes the shard either now
            // has an engine, or it closed without ever creating an engine.
            .newForked(shard::waitForEngineOrClosedShard)

            .<FetchShardCommitsInUseAction.NodeResponse>andThen((subscribedListener, ignored) -> {
                if (shard.indexSettings().getIndexMetadata().getState() == IndexMetadata.State.CLOSE) {
                    completeListenerWithClosedShardService(subscribedListener, request.getShardId());
                    return;
                }

                final Engine engineOrNull = shard.getEngineOrNull();
                if (engineOrNull == null) {
                    completeListenerWithClosedShardService(subscribedListener, request.getShardId());
                    return;
                }

                if (engineOrNull instanceof SearchEngine searchEngine) {
                    ActionListener.completeWith(
                        subscribedListener,
                        () -> new FetchShardCommitsInUseAction.NodeResponse(
                            clusterService.localNode(),
                            // Even if the index is officially closed by the time we fetch the commit generations, the SearchEngine will
                            // remain active and its list of `openReaders` will remain valid. Search operations retain a reference to the
                            // SearchEngine and only remove active reader tracking when done. The ClosedShardService acts as a concurrent
                            // intermediary only in cases where the SearchEngine becomes inaccessible after the IndexShard destructs.
                            searchEngine.getAcquiredPrimaryTermAndGenerations()
                        )
                    );
                } else {
                    final var exception = new ElasticsearchException("expecting SearchEngine but got " + engineOrNull);
                    logger.error("unexpected", exception);
                    assert false : exception;
                    throw exception;
                }
            })

            // Send the response back to the caller.
            .addListener(listener);
    }

    @Override
    protected FetchShardCommitsInUseAction.NodeResponse nodeOperation(FetchShardCommitsInUseAction.NodeRequest request, Task task) {
        assert false : "this should never be called because nodeOperationAsync is overridden";
        return null;
    }
}
