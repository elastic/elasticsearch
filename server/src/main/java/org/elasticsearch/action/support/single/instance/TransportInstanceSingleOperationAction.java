/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.support.single.instance;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.action.UnavailableShardsException;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.ChannelActionListener;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ProjectState;
import org.elasticsearch.cluster.ProjectStateObserver;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.routing.ShardIterator;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.node.NodeClosedException;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.ConnectTransportException;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportResponseHandler;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.concurrent.Executor;

import static org.elasticsearch.cluster.metadata.IndexNameExpressionResolver.EXCLUDED_DATA_STREAMS_KEY;

public abstract class TransportInstanceSingleOperationAction<
    Request extends InstanceShardOperationRequest<Request>,
    Response extends ActionResponse> extends HandledTransportAction<Request, Response> {

    protected final ThreadPool threadPool;
    protected final ClusterService clusterService;
    private final ProjectResolver projectResolver;
    protected final TransportService transportService;
    protected final IndexNameExpressionResolver indexNameExpressionResolver;

    final String shardActionName;

    @SuppressWarnings("this-escape")
    protected TransportInstanceSingleOperationAction(
        String actionName,
        ThreadPool threadPool,
        ClusterService clusterService,
        ProjectResolver projectResolver,
        TransportService transportService,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        Writeable.Reader<Request> request
    ) {
        super(actionName, transportService, actionFilters, request, EsExecutors.DIRECT_EXECUTOR_SERVICE);
        this.threadPool = threadPool;
        this.clusterService = clusterService;
        this.projectResolver = projectResolver;
        this.transportService = transportService;
        this.indexNameExpressionResolver = indexNameExpressionResolver;
        this.shardActionName = actionName + "[s]";
        transportService.registerRequestHandler(shardActionName, EsExecutors.DIRECT_EXECUTOR_SERVICE, request, this::handleShardRequest);
    }

    @Override
    protected void doExecute(Task task, Request request, ActionListener<Response> listener) {
        new AsyncSingleAction(request, listener).start();
    }

    protected ProjectState getProjectState() {
        return projectResolver.getProjectState(clusterService.state());
    }

    protected abstract Executor executor(ShardId shardId);

    protected abstract void shardOperation(Request request, ActionListener<Response> listener);

    protected abstract Response newResponse(StreamInput in) throws IOException;

    protected static ClusterBlockException checkGlobalBlock(ProjectState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.WRITE);
    }

    protected ClusterBlockException checkRequestBlock(ProjectState state, Request request) {
        return state.blocks().indexBlockedException(state.projectId(), ClusterBlockLevel.WRITE, request.concreteIndex());
    }

    /**
     * Resolves the request. Throws an exception if the request cannot be resolved.
     */
    protected abstract void resolveRequest(ProjectState state, Request request);

    protected boolean retryOnFailure(Exception e) {
        return false;
    }

    protected static TransportRequestOptions transportOptions() {
        return TransportRequestOptions.EMPTY;
    }

    /**
     * Should return an iterator with a single shard!
     */
    protected abstract ShardIterator shards(ProjectState projectState, Request request);

    class AsyncSingleAction {

        private final ActionListener<Response> listener;
        private final Request request;
        private volatile ProjectStateObserver observer;
        private ShardIterator shardIt;

        AsyncSingleAction(Request request, ActionListener<Response> listener) {
            this.request = request;
            this.listener = listener;
        }

        public void start() {
            final ProjectState state = getProjectState();
            this.observer = new ProjectStateObserver(state, clusterService, request.timeout(), logger, threadPool.getThreadContext());
            doStart(state);
        }

        protected void doStart(ProjectState projectState) {
            try {
                ClusterBlockException blockException = checkGlobalBlock(projectState);
                if (blockException != null) {
                    if (blockException.retryable()) {
                        retry(blockException);
                        return;
                    } else {
                        throw blockException;
                    }
                }
                try {
                    request.concreteIndex(indexNameExpressionResolver.concreteWriteIndex(projectState.metadata(), request).getName());
                } catch (IndexNotFoundException e) {
                    if (request.includeDataStreams() == false && e.getMetadataKeys().contains(EXCLUDED_DATA_STREAMS_KEY)) {
                        throw new IllegalArgumentException("only write ops with an op_type of create are allowed in data streams");
                    } else {
                        throw e;
                    }
                }
                resolveRequest(projectState, request);
                blockException = checkRequestBlock(projectState, request);
                if (blockException != null) {
                    if (blockException.retryable()) {
                        retry(blockException);
                        return;
                    } else {
                        throw blockException;
                    }
                }
                shardIt = shards(projectState, request);
            } catch (Exception e) {
                listener.onFailure(e);
                return;
            }

            // no shardIt, might be in the case between index gateway recovery and shardIt initialization
            if (shardIt.size() == 0) {
                retry(null);
                return;
            }

            // this transport only make sense with an iterator that returns a single shard routing (like primary)
            assert shardIt.size() == 1;

            ShardRouting shard = shardIt.nextOrNull();
            assert shard != null;

            if (shard.active() == false) {
                retry(null);
                return;
            }

            request.shardId = shardIt.shardId();
            DiscoveryNode node = projectState.cluster().nodes().get(shard.currentNodeId());
            transportService.sendRequest(
                node,
                shardActionName,
                request,
                transportOptions(),
                new ActionListenerResponseHandler<>(
                    listener,
                    TransportInstanceSingleOperationAction.this::newResponse,
                    TransportResponseHandler.TRANSPORT_WORKER
                ) {
                    @Override
                    public void handleException(TransportException exp) {
                        final Throwable cause = exp.unwrapCause();
                        // if we got disconnected from the node, or the node / shard is not in the right state (being closed)
                        if (cause instanceof ConnectTransportException || cause instanceof NodeClosedException || retryOnFailure(exp)) {
                            retry((Exception) cause);
                        } else {
                            listener.onFailure(exp);
                        }
                    }
                }
            );
        }

        void retry(@Nullable final Exception failure) {
            if (observer.isTimedOut()) {
                // we running as a last attempt after a timeout has happened. don't retry
                Exception listenFailure = failure;
                if (listenFailure == null) {
                    if (shardIt == null) {
                        listenFailure = new UnavailableShardsException(
                            request.concreteIndex(),
                            -1,
                            "Timeout waiting for [{}], request: {}",
                            request.timeout(),
                            actionName
                        );
                    } else {
                        listenFailure = new UnavailableShardsException(
                            shardIt.shardId(),
                            "[{}] shardIt, [{}] active : Timeout waiting for [{}], request: {}",
                            shardIt.size(),
                            shardIt.sizeActive(),
                            request.timeout(),
                            actionName
                        );
                    }
                }
                listener.onFailure(listenFailure);
                return;
            }

            observer.waitForNextChange(new ProjectStateObserver.Listener() {
                @Override
                public void onProjectStateChange(ProjectState projectState) {
                    doStart(projectState);
                }

                @Override
                public void onProjectMissing(ProjectId projectId, ClusterState clusterState) {
                    listener.onFailure(
                        new ResourceNotFoundException(
                            "project ["
                                + projectId
                                + "] does not exist in cluster state ["
                                + clusterState.stateUUID()
                                + "] version ["
                                + clusterState.version()
                                + "]"
                        )
                    );
                }

                @Override
                public void onClusterServiceClose() {
                    listener.onFailure(new NodeClosedException(clusterService.localNode()));
                }

                @Override
                public void onTimeout(TimeValue timeout) {
                    // just to be on the safe side, see if we can start it now?
                    observer.observeLastAppliedState(this);
                }
            }, request.timeout());
        }
    }

    private void handleShardRequest(Request request, TransportChannel channel, Task task) {
        executor(request.shardId).execute(
            ActionRunnable.wrap(new ChannelActionListener<Response>(channel), l -> shardOperation(request, l))
        );
    }
}
