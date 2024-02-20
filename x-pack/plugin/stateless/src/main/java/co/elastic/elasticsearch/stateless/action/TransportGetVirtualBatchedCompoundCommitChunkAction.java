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
import co.elastic.elasticsearch.stateless.engine.IndexEngine;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchTimeoutException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.ChannelActionListener;
import org.elasticsearch.action.support.RetryableAction;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateObserver;
import org.elasticsearch.cluster.action.shard.ShardStateAction;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.ByteArray;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.Strings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardNotStartedException;
import org.elasticsearch.index.shard.IndexShardState;
import org.elasticsearch.indices.IndexClosedException;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.node.NodeClosedException;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.ConnectTransportException;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportRequestHandler;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;

public class TransportGetVirtualBatchedCompoundCommitChunkAction extends TransportAction<
    GetVirtualBatchedCompoundCommitChunkRequest,
    GetVirtualBatchedCompoundCommitChunkResponse> {
    private static final Logger logger = LogManager.getLogger(TransportGetVirtualBatchedCompoundCommitChunkAction.class);
    public static final String NAME = "internal:admin/" + Stateless.NAME + "/vbcc/get/chunk";
    public static final ActionType<GetVirtualBatchedCompoundCommitChunkResponse> TYPE = new ActionType<>(NAME);

    protected final String transportPrimaryAction;
    private final BigArrays bigArrays;
    private final IndicesService indicesService;
    private final TransportService transportService;
    private final ClusterService clusterService;
    private final ShardStateAction shardStateAction;

    @Inject
    public TransportGetVirtualBatchedCompoundCommitChunkAction(
        ActionFilters actionFilters,
        BigArrays bigArrays,
        TransportService transportService,
        IndicesService indicesService,
        ClusterService clusterService,
        ShardStateAction shardStateAction
    ) {
        super(NAME, actionFilters, transportService.getTaskManager());
        this.bigArrays = bigArrays;
        this.indicesService = indicesService;
        this.transportService = transportService;
        this.clusterService = clusterService;
        this.shardStateAction = shardStateAction;
        this.transportPrimaryAction = actionName + "[p]";

        transportService.registerRequestHandler(
            transportPrimaryAction,
            transportService.getThreadPool().executor(Stateless.GET_VIRTUAL_BATCHED_COMPOUND_COMMIT_CHUNK_THREAD_POOL),
            (inputStream) -> new GetVirtualBatchedCompoundCommitChunkRequest(inputStream),
            new TransportRequestHandler<GetVirtualBatchedCompoundCommitChunkRequest>() {
                @Override
                public void messageReceived(GetVirtualBatchedCompoundCommitChunkRequest request, TransportChannel channel, Task task)
                    throws Exception {
                    final ActionListener<GetVirtualBatchedCompoundCommitChunkResponse> listener = new ChannelActionListener<>(channel);
                    ActionListener.run(listener, (l) -> primaryShardOperation(task, request, l));
                }
            }
        );
    }

    @Override
    protected void doExecute(
        Task task,
        GetVirtualBatchedCompoundCommitChunkRequest request,
        ActionListener<GetVirtualBatchedCompoundCommitChunkResponse> listener
    ) {
        assert transportService.getLocalNode().hasRole(DiscoveryNodeRole.SEARCH_ROLE.roleName()) : "not a search node";
        ActionListener.run(listener, l -> {
            if (task != null) {
                request.setParentTask(clusterService.localNode().getId(), task.getId());
            }

            final RetryableAction<GetVirtualBatchedCompoundCommitChunkResponse> retryableAction = new RetryableAction<>(
                logger,
                transportService.getThreadPool(),
                TimeValue.timeValueMillis(1),
                TimeValue.timeValueSeconds(5),
                TimeValue.timeValueSeconds(60),
                l,
                EsExecutors.DIRECT_EXECUTOR_SERVICE
            ) {
                @Override
                public void tryAction(ActionListener<GetVirtualBatchedCompoundCommitChunkResponse> listener) {
                    sendRequestToPrimaryNodeWhenReady(clusterService.state(), request, listener);
                }

                @Override
                public boolean shouldRetry(Exception e) {
                    // TODO handle more errors and exceptions (either before or after sending the request to the indexing node), e.g.,:
                    // * IndexClosedException or see if shard was unassigned (which also affects the search node) --> fail the request.
                    // * FileNotFound -> assertion failure, fail shard in production (with proper error logging).
                    // * if index is deleted at any point (e.g., also in the ClusterStateObserver) -> fail the request.
                    return ExceptionsHelper.unwrap(
                        e,
                        ConnectTransportException.class,
                        CircuitBreakingException.class,
                        NodeClosedException.class,
                        IndexShardNotStartedException.class
                    ) != null;
                }
            };
            retryableAction.run();
        });
    }

    /**
     * Send the request to the ready primary node if found, otherwise observe the cluster state and send the request to the primary node
     * when it is found and is ready.
     */
    private void sendRequestToPrimaryNodeWhenReady(
        ClusterState clusterState,
        GetVirtualBatchedCompoundCommitChunkRequest request,
        ActionListener<GetVirtualBatchedCompoundCommitChunkResponse> listener
    ) {
        final DiscoveryNode readyPrimaryDiscoveryNode = getReadyPrimaryNodeFromClusterState(clusterState, request);
        if (readyPrimaryDiscoveryNode != null) {
            sendRequestToPrimaryNode(readyPrimaryDiscoveryNode, request, listener);
        } else {
            TimeValue timeout = TimeValue.timeValueSeconds(60);
            ClusterStateObserver observer = new ClusterStateObserver(
                clusterState,
                clusterService,
                timeout,
                logger,
                transportService.getThreadPool().getThreadContext()
            );
            observer.waitForNextChange(new ClusterStateObserver.Listener() {
                @Override
                public void onNewClusterState(ClusterState state) {
                    ActionListener.run(listener, l -> {
                        final DiscoveryNode readyPrimaryDiscoveryNode = getReadyPrimaryNodeFromClusterState(clusterState, request);
                        sendRequestToPrimaryNode(readyPrimaryDiscoveryNode, request, listener);
                    });
                }

                @Override
                public void onClusterServiceClose() {
                    listener.onFailure(new NodeClosedException(clusterService.localNode()));
                }

                @Override
                public void onTimeout(TimeValue timeout) {
                    listener.onFailure(
                        new ElasticsearchTimeoutException(
                            Strings.format(
                                "Timed out while waiting for primary shard to become available " + "[timeout=%s, shard=%s].",
                                timeout,
                                request.getShardId()
                            )
                        )
                    );
                }
            }, newState -> getReadyPrimaryNodeFromClusterState(newState, request) != null);
        }
    }

    /**
     * Gets the {@link DiscoveryNode} of the primary shard from the cluster state if shard has started, or null otherwise.
     */
    private DiscoveryNode getReadyPrimaryNodeFromClusterState(
        ClusterState clusterState,
        GetVirtualBatchedCompoundCommitChunkRequest request
    ) {
        ShardRouting primaryShardRouting = clusterState.routingTable().shardRoutingTable(request.getShardId()).primaryShard();
        if (primaryShardRouting != null && primaryShardRouting.started()) {
            String primaryNode = primaryShardRouting.currentNodeId();
            final DiscoveryNode primaryDiscoveryNode = clusterState.nodes().get(primaryNode);
            assert primaryDiscoveryNode != null;
            return primaryDiscoveryNode;
        }
        return null;
    }

    private void sendRequestToPrimaryNode(
        DiscoveryNode primaryNode,
        GetVirtualBatchedCompoundCommitChunkRequest request,
        ActionListener<GetVirtualBatchedCompoundCommitChunkResponse> listener
    ) {
        transportService.sendRequest(
            primaryNode,
            transportPrimaryAction,
            request,
            TransportRequestOptions.EMPTY,
            new ActionListenerResponseHandler<>(
                listener,
                (inputStream) -> new GetVirtualBatchedCompoundCommitChunkResponse(inputStream),
                transportService.getThreadPool().executor(Stateless.SHARD_READ_THREAD_POOL)
            )
        );
    }

    private void primaryShardOperation(
        Task task,
        GetVirtualBatchedCompoundCommitChunkRequest request,
        ActionListener<GetVirtualBatchedCompoundCommitChunkResponse> listener
    ) throws IOException {
        assert transportService.getLocalNode().hasRole(DiscoveryNodeRole.INDEX_ROLE.roleName()) : "not an indexing node";
        assert ThreadPool.assertCurrentThreadPool(Stateless.GET_VIRTUAL_BATCHED_COMPOUND_COMMIT_CHUNK_THREAD_POOL);
        Index index = request.getShardId().getIndex();
        final IndexShard shard = indicesService.indexServiceSafe(index).getShard(request.getShardId().id());
        assert shard.routingEntry().primary() : shard + " not primary on node " + transportService.getLocalNode();

        if (shard.indexSettings().getIndexMetadata().getState() == IndexMetadata.State.CLOSE) {
            throw new IndexClosedException(request.getShardId().getIndex());
        }
        if (shard.state() != IndexShardState.STARTED) {
            throw new IndexShardNotStartedException(shard.shardId(), shard.state()); // trigger retry logic on search node
        }
        if (request.getPrimaryTerm() != shard.getOperationPrimaryTerm()) {
            final var exception = new ElasticsearchException(
                "primary term mismatch [request=" + request.getPrimaryTerm() + ", shard=" + shard.getOperationPrimaryTerm() + "]"
            );
            throw exception;
        }
        final Engine engine = shard.getEngineOrNull();
        assert engine != null : "engine not started";
        if (engine instanceof IndexEngine == false) {
            final var exception = new ElasticsearchException("expecting IndexEngine but got " + engine);
            logger.error("unexpected", exception);
            assert false : exception;
            throw exception;
        }
        IndexEngine indexEngine = (IndexEngine) engine;

        // TODO: should we limit the amount we have outstanding to some number, like 5% of heap or so?
        // TODO: could the request length be much bigger than the actual length? If yes, we could allocate a smaller amount here.
        ByteArray array = bigArrays.newByteArray(request.getLength(), false);
        BytesReference bytesReference = BytesReference.fromByteArray(array, request.getLength());
        try (ReleasableBytesReference reference = new ReleasableBytesReference(bytesReference, array)) {
            indexEngine.readVirtualBatchedCompoundCommitChunk(request, reference);
            ActionListener.respondAndRelease(listener, new GetVirtualBatchedCompoundCommitChunkResponse(reference));
        }
    }
}
