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
 *
 * This file was contributed to by generative AI
 */

package co.elastic.elasticsearch.stateless.action;

import co.elastic.elasticsearch.stateless.Stateless;
import co.elastic.elasticsearch.stateless.engine.IndexEngine;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.store.AlreadyClosedException;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.ChannelActionListener;
import org.elasticsearch.action.support.RetryableAction;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.ReleasableBytesStreamOutput;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardNotFoundException;
import org.elasticsearch.indices.IndexClosedException;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.node.NodeClosedException;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.ConnectTransportException;
import org.elasticsearch.transport.TcpTransport;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportResponseHandler;
import org.elasticsearch.transport.TransportService;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.NoSuchFileException;

public class TransportGetVirtualBatchedCompoundCommitChunkAction extends TransportAction<
    GetVirtualBatchedCompoundCommitChunkRequest,
    GetVirtualBatchedCompoundCommitChunkResponse> {
    public static final String NAME = "internal:admin/" + Stateless.NAME + "/vbcc/get/chunk";
    public static final ActionType<GetVirtualBatchedCompoundCommitChunkResponse> TYPE = new ActionType<>(NAME);
    private static final Logger logger = LogManager.getLogger(TransportGetVirtualBatchedCompoundCommitChunkAction.class);
    protected final String transportPrimaryAction;
    private final BigArrays bigArrays;
    private final IndicesService indicesService;
    private final TransportService transportService;
    private final ClusterService clusterService;

    @Inject
    public TransportGetVirtualBatchedCompoundCommitChunkAction(
        ActionFilters actionFilters,
        BigArrays bigArrays,
        TransportService transportService,
        IndicesService indicesService,
        ClusterService clusterService
    ) {
        super(NAME, actionFilters, transportService.getTaskManager());
        this.bigArrays = bigArrays;
        this.indicesService = indicesService;
        this.transportService = transportService;
        this.clusterService = clusterService;
        this.transportPrimaryAction = actionName + "[p]";

        transportService.registerRequestHandler(
            transportPrimaryAction,
            transportService.getThreadPool().executor(Stateless.GET_VIRTUAL_BATCHED_COMPOUND_COMMIT_CHUNK_THREAD_POOL),
            GetVirtualBatchedCompoundCommitChunkRequest::new,
            (request, channel, task) -> {
                final ActionListener<GetVirtualBatchedCompoundCommitChunkResponse> listener = new ChannelActionListener<>(channel);
                ActionListener.run(listener, (l) -> primaryShardOperation(task, request, l));
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
                TimeValue.MAX_VALUE,
                l,
                EsExecutors.DIRECT_EXECUTOR_SERVICE
            ) {
                @Override
                public void tryAction(ActionListener<GetVirtualBatchedCompoundCommitChunkResponse> listener) {
                    sendRequestToPrimaryNode(findPrimaryNode(clusterService.state(), request), request, listener);
                }

                @Override
                public boolean shouldRetry(Exception e) {
                    // The search shard may get concurrently closed during recovery. The initial closing shard is done on the
                    // cluster applier thread which needs to obtain the engineMutex. But opening the engine (as part of the recovery)
                    // holds the engineMutex which prevents the indexShard from being closed and in turn cluster state update.
                    // Hence, we skip retry if the search index/shard is already removed since it cannot be successful.
                    // It also blocks the applier thread and leads to the node lagging on cluster state update.
                    final IndexService indexService = indicesService.indexService(request.getShardId().getIndex());
                    final boolean shouldRetry = indexService != null && indexService.hasShard(request.getShardId().id());

                    return shouldRetry
                        && ExceptionsHelper.unwrap(e, ConnectTransportException.class, CircuitBreakingException.class) != null;
                }
            };
            retryableAction.run();
        });
    }

    private DiscoveryNode findPrimaryNode(ClusterState clusterState, GetVirtualBatchedCompoundCommitChunkRequest request) {
        if (request.getPreferredNodeId() != null && clusterState.nodes().nodeExists(request.getPreferredNodeId())) {
            return clusterState.nodes().get(request.getPreferredNodeId());
        } else {
            throw new ResourceNotFoundException("Unable to find node " + request.getPreferredNodeId() + " in the cluster state");
        }
    }

    private void sendRequestToPrimaryNode(
        DiscoveryNode discoveryNode,
        GetVirtualBatchedCompoundCommitChunkRequest request,
        ActionListener<GetVirtualBatchedCompoundCommitChunkResponse> listener
    ) {
        assert discoveryNode != null : request;
        transportService.sendRequest(
            discoveryNode,
            transportPrimaryAction,
            request,
            TransportRequestOptions.EMPTY,
            new ActionListenerResponseHandler<>(listener.delegateResponse((l, e) -> {
                var cause = ExceptionsHelper.unwrapCause(e);
                // We don't want to retry on this exception, but still want to convert it to recoverable ResourceNotFoundException
                if (cause instanceof NodeClosedException) {
                    l.onFailure(new ResourceNotFoundException("Unable to get virtual batched compound commit chunk", e));
                } else {
                    l.onFailure(e);
                }
            }).delegateFailure((l, r) -> {
                assert ThreadPool.assertCurrentThreadPool(TcpTransport.TRANSPORT_WORKER_THREAD_NAME_PREFIX);
                listener.onResponse(r);
            }), GetVirtualBatchedCompoundCommitChunkResponse::new, TransportResponseHandler.TRANSPORT_WORKER)
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
        if (request.getPrimaryTerm() != shard.getOperationPrimaryTerm()) {
            // The primary term of the shard has changed since the request was sent. Send exception to signify the blob has been uploaded.
            final var exception = new ResourceNotFoundException(
                "primary term mismatch [request=" + request.getPrimaryTerm() + ", shard=" + shard.getOperationPrimaryTerm() + "]"
            );
            throw exception;
        }
        final Engine engine = shard.getEngineOrNull();
        if (engine == null) {
            throw new ShardNotFoundException(shard.shardId(), "engine not started");
        }
        if (engine instanceof IndexEngine == false) {
            final var exception = new ElasticsearchException("expecting IndexEngine but got " + engine);
            logger.error("unexpected", exception);
            assert false : exception;
            throw exception;
        }
        IndexEngine indexEngine = (IndexEngine) engine;

        try {
            // TODO: should we limit the amount we have outstanding to some number, like 5% of heap or so? By outstanding we mean the amount
            // of bytes we have allocated but not released yet. Since the release happens async after sending over the wire, we could
            // exhaust the heap here and limiting that would be good. It could be blocking, though an async mechanism could be preferable.

            ReleasableBytesStreamOutput output = new ReleasableBytesStreamOutput(request.getLength(), bigArrays);
            try {
                indexEngine.readVirtualBatchedCompoundCommitChunk(request, output);
                // Transfer responsibility of releasing the output bytes to a ReleasableBytesReference for the response.
                var transfer = new ReleasableBytesReference(output.bytes(), output);
                output = null;
                ActionListener.respondAndRelease(listener, new GetVirtualBatchedCompoundCommitChunkResponse(transfer));
            } catch (AlreadyClosedException e) {
                throw new ShardNotFoundException(shard.shardId(), "Engine already closed", e);
            } finally {
                Releasables.close(output);
            }
        } catch (Exception e) {
            if (ExceptionsHelper.unwrap(e, FileNotFoundException.class, NoSuchFileException.class) != null) {
                shard.failShard("failed to get a virtual batched compound commit chunk", e);
                listener.onFailure(e);
            } else {
                throw e;
            }
        }
    }
}
