/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.health.node.action;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateObserver;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.health.node.selection.HealthNode;
import org.elasticsearch.node.NodeClosedException;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.ConnectTransportException;
import org.elasticsearch.transport.RemoteTransportException;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportService;

import java.util.function.Predicate;

import static org.elasticsearch.core.Strings.format;

/**
 * A base class for operations that need to be performed on the health node.
 */
public abstract class TransportHealthNodeAction<Request extends HealthNodeRequest<Request>, Response extends ActionResponse> extends
    HandledTransportAction<Request, Response> {

    private static final Logger logger = LogManager.getLogger(TransportHealthNodeAction.class);

    protected final TransportService transportService;
    protected final ClusterService clusterService;
    protected final ThreadPool threadPool;
    protected final String executor;

    private final Writeable.Reader<Response> responseReader;

    protected TransportHealthNodeAction(
        String actionName,
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        Writeable.Reader<Request> request,
        Writeable.Reader<Response> response,
        String executor
    ) {
        super(actionName, true, transportService, actionFilters, request);
        this.transportService = transportService;
        this.clusterService = clusterService;
        this.threadPool = threadPool;
        this.executor = executor;
        this.responseReader = response;
    }

    protected abstract void healthOperation(Task task, Request request, ClusterState state, ActionListener<Response> listener)
        throws Exception;

    private void executeHealthOperation(Task task, Request request, ClusterState state, ActionListener<Response> listener)
        throws Exception {
        if ((task instanceof CancellableTask t) && t.isCancelled()) {
            throw new TaskCancelledException("Task was cancelled");
        }

        healthOperation(task, request, state, listener);
    }

    @Override
    protected void doExecute(Task task, final Request request, ActionListener<Response> listener) {
        ClusterState state = clusterService.state();
        logger.trace("starting to process request [{}] with cluster state version [{}]", request, state.version());
        if (task != null) {
            request.setParentTask(clusterService.localNode().getId(), task.getId());
        }
        new AsyncSingleAction(task, request, listener).doStart(state);
    }

    class AsyncSingleAction {

        private final ActionListener<Response> listener;
        private final Request request;
        private ClusterStateObserver observer;
        private final long startTime;
        private final Task task;

        AsyncSingleAction(Task task, Request request, ActionListener<Response> listener) {
            this.task = task;
            this.request = request;
            this.listener = listener;
            this.startTime = threadPool.relativeTimeInMillis();
        }

        protected void doStart(ClusterState clusterState) {
            if (isTaskCancelled()) {
                listener.onFailure(new TaskCancelledException("Task was cancelled"));
                return;
            }
            try {
                DiscoveryNode healthNode = HealthNode.findHealthNode(clusterState);
                DiscoveryNode localNode = clusterState.nodes().getLocalNode();
                if (healthNode == null) {
                    logger.debug("no known health node, scheduling a retry");
                    retryOnHealthNodeChange(clusterState, null);
                } else if (localNode.getId().equals(healthNode.getId())) {
                    ActionListener<Response> delegate = listener.delegateResponse((delegatedListener, t) -> {
                        if (t instanceof NotHealthNodeException) {
                            logger.debug(() -> format("health node stepped down before action [%s], scheduling a retry", actionName), t);
                            retryOnHealthNodeChange(clusterState, t);
                        } else {
                            logger.debug(() -> format("unexpected exception during health node request"), t);
                            delegatedListener.onFailure(t);
                        }
                    });
                    threadPool.executor(executor)
                        .execute(ActionRunnable.wrap(delegate, l -> executeHealthOperation(task, request, clusterState, l)));
                } else {
                    logger.trace("forwarding request [{}] to health node [{}]", actionName, healthNode);
                    transportService.sendRequest(
                        healthNode,
                        actionName,
                        request,
                        new ActionListenerResponseHandler<>(listener, responseReader) {
                            @Override
                            public void handleException(final TransportException exception) {
                                Throwable cause = exception.unwrapCause();
                                if (cause instanceof ConnectTransportException
                                    || (exception instanceof RemoteTransportException && cause instanceof NodeClosedException)) {
                                    // we want to retry here in case a new health node has been selected
                                    logger.debug(
                                        () -> format(
                                            "connection exception while trying to forward request with action name [%s] to "
                                                + "health node [%s], scheduling a retry.",
                                            actionName,
                                            healthNode
                                        ),
                                        exception
                                    );
                                    retryOnHealthNodeChange(clusterState, cause);
                                } else {
                                    logger.trace(
                                        () -> format("failure when forwarding request [%s] to health node [%s]", actionName, healthNode),
                                        exception
                                    );
                                    listener.onFailure(exception);
                                }
                            }
                        }
                    );
                }
            } catch (Exception e) {
                logger.trace(() -> format("Failed to route/execute health node action %s", actionName), e);
                listener.onFailure(e);
            }
        }

        private void retryOnHealthNodeChange(ClusterState state, Throwable failure) {
            retry(state, failure, createNewHealthNodePredicate(state));
        }

        private void retry(ClusterState state, final Throwable failure, final Predicate<ClusterState> statePredicate) {
            if (observer == null) {
                final long remainingTimeoutMS = request.healthNodeTimeout().millis() - (threadPool.relativeTimeInMillis() - startTime);
                if (remainingTimeoutMS <= 0) {
                    logger.debug(() -> format("timed out before retrying [%s] after failure", actionName), failure);
                    listener.onFailure(new HealthNodeNotDiscoveredException(failure));
                    return;
                }
                this.observer = new ClusterStateObserver(
                    state,
                    clusterService,
                    TimeValue.timeValueMillis(remainingTimeoutMS),
                    logger,
                    threadPool.getThreadContext()
                );
            }
            observer.waitForNextChange(new ClusterStateObserver.Listener() {
                @Override
                public void onNewClusterState(ClusterState state) {
                    logger.trace("retrying with cluster state version [{}]", state.version());
                    doStart(state);
                }

                @Override
                public void onClusterServiceClose() {
                    listener.onFailure(new NodeClosedException(clusterService.localNode()));
                }

                @Override
                public void onTimeout(TimeValue timeout) {
                    logger.debug(() -> format("timed out while retrying [%s] after failure (timeout [%s])", actionName, timeout), failure);
                    listener.onFailure(new HealthNodeNotDiscoveredException(failure));
                }
            }, clusterState -> isTaskCancelled() || statePredicate.test(clusterState));
        }

        private boolean isTaskCancelled() {
            return (task instanceof CancellableTask t) && t.isCancelled();
        }
    }

    // visible for testing
    static Predicate<ClusterState> createNewHealthNodePredicate(ClusterState currentState) {
        final long currentVersion = currentState.version();
        final DiscoveryNode currentHealthNode = HealthNode.findHealthNode(currentState);
        final String currentHealthNodeId = currentHealthNode == null ? null : currentHealthNode.getId();
        return newState -> {
            final DiscoveryNode newHealthNode = HealthNode.findHealthNode(newState);
            final boolean accept;
            if (newHealthNode == null) {
                accept = false;
            } else if (newHealthNode.getId().equals(currentHealthNodeId) == false) {
                accept = true;
            } else {
                accept = newState.version() > currentVersion;
            }
            return accept;
        };
    }
}
