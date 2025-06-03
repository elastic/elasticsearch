/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.support.master;

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
import org.elasticsearch.cluster.block.ClusterBlock;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.service.MasterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.FixForMultiProject;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.discovery.MasterNotDiscoveredException;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.node.NodeClosedException;
import org.elasticsearch.reservedstate.ActionWithReservedState;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.ConnectTransportException;
import org.elasticsearch.transport.RemoteTransportException;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportService;

import java.util.Optional;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Predicate;

import static org.elasticsearch.core.Strings.format;

/**
 * A base class for operations that needs to be performed on the master node.
 */
public abstract class TransportMasterNodeAction<Request extends MasterNodeRequest<Request>, Response extends ActionResponse> extends
    HandledTransportAction<Request, Response>
    implements
        ActionWithReservedState<Request> {

    private static final Logger logger = LogManager.getLogger(TransportMasterNodeAction.class);

    protected final ThreadPool threadPool;
    protected final TransportService transportService;
    protected final ClusterService clusterService;

    private final Writeable.Reader<Response> responseReader;

    protected final Executor executor;

    protected TransportMasterNodeAction(
        String actionName,
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        Writeable.Reader<Request> request,
        Writeable.Reader<Response> response,
        Executor executor
    ) {
        this(actionName, true, transportService, clusterService, threadPool, actionFilters, request, response, executor);
    }

    protected TransportMasterNodeAction(
        String actionName,
        boolean canTripCircuitBreaker,
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        Writeable.Reader<Request> request,
        Writeable.Reader<Response> response,
        Executor executor
    ) {
        super(actionName, canTripCircuitBreaker, transportService, actionFilters, request, EsExecutors.DIRECT_EXECUTOR_SERVICE);
        this.transportService = transportService;
        this.clusterService = clusterService;
        this.threadPool = threadPool;
        this.executor = executor;
        this.responseReader = response;
    }

    protected abstract void masterOperation(Task task, Request request, ClusterState state, ActionListener<Response> listener)
        throws Exception;

    private void executeMasterOperation(Task task, Request request, ClusterState state, ActionListener<Response> listener)
        throws Exception {
        if (task instanceof CancellableTask cancellableTask && cancellableTask.isCancelled()) {
            throw new TaskCancelledException("Task was cancelled");
        }
        masterOperation(task, request, state, listener);
    }

    protected boolean localExecute(Request request) {
        return false;
    }

    protected abstract ClusterBlockException checkBlock(Request request, ClusterState state);

    private ClusterBlockException checkBlockIfStateRecovered(Request request, ClusterState state) {
        try {
            return checkBlock(request, state);
        } catch (IndexNotFoundException e) {
            if (state.blocks().hasGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK)) {
                // no index metadata is exposed yet, but checkBlock depends on an index, so keep trying until the cluster forms
                assert GatewayService.STATE_NOT_RECOVERED_BLOCK.contains(ClusterBlockLevel.METADATA_READ);
                assert state.blocks().global(ClusterBlockLevel.METADATA_READ).stream().allMatch(ClusterBlock::retryable);
                return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
            } else {
                throw e;
            }
        }
    }

    @FixForMultiProject // this is overridden for project-specific reserved metadata checks. A common subclass needs to exist for this.
    protected void validateForReservedState(Request request, ClusterState state) {
        Optional<String> handlerName = reservedStateHandlerName();
        assert handlerName.isPresent();

        validateForReservedState(
            state.metadata().reservedStateMetadata().values(),
            handlerName.get(),
            modifiedKeys(request),
            request.toString()
        );
    }

    // package private for testing
    boolean supportsReservedState() {
        return reservedStateHandlerName().isPresent();
    }

    @Override
    protected void doExecute(Task task, final Request request, ActionListener<Response> listener) {
        ClusterState state = clusterService.state();
        if (supportsReservedState()) {
            validateForReservedState(request, state);
        }
        logger.trace("starting processing request [{}] with cluster state version [{}]", request, state.version());
        if (task != null) {
            request.setParentTask(clusterService.localNode().getId(), task.getId());
        }
        request.mustIncRef();
        new AsyncSingleAction(task, request, ActionListener.runBefore(listener, request::decRef)).doStart(state);
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
            final long currentStateVersion = clusterState.version();
            try {
                final DiscoveryNodes nodes = clusterState.nodes();
                if (nodes.isLocalNodeElectedMaster() || localExecute(request)) {
                    // check for block, if blocked, retry, else, execute locally
                    final ClusterBlockException blockException = checkBlockIfStateRecovered(request, clusterState);
                    if (blockException != null) {
                        if (blockException.retryable() == false) {
                            logger.trace("can't execute due to a non-retryable cluster block", blockException);
                            listener.onFailure(blockException);
                        } else {
                            logger.debug("can't execute due to a cluster block, retrying", blockException);
                            retry(currentStateVersion, blockException, newState -> {
                                try {
                                    ClusterBlockException newException = checkBlockIfStateRecovered(request, newState);
                                    return (newException == null || newException.retryable() == false);
                                } catch (Exception e) {
                                    // accept state as block will be rechecked by doStart() and listener.onFailure() then called
                                    logger.debug("exception occurred during cluster block checking, accepting state", e);
                                    return true;
                                }
                            });
                        }
                    } else {
                        ActionListener<Response> delegate = listener.delegateResponse((delegatedListener, t) -> {
                            if (MasterService.isPublishFailureException(t)) {
                                logger.debug(
                                    () -> format(
                                        "master could not publish cluster state or "
                                            + "stepped down before publishing action [%s], scheduling a retry",
                                        actionName
                                    ),
                                    t
                                );
                                retryOnNextState(currentStateVersion, t);
                            } else {
                                logger.debug("unexpected exception during publication", t);
                                delegatedListener.onFailure(t);
                            }
                        });
                        executor.execute(ActionRunnable.wrap(delegate, l -> executeMasterOperation(task, request, clusterState, l)));
                    }
                } else {
                    if (nodes.getMasterNode() == null) {
                        logger.debug("no known master node, scheduling a retry");
                        retryOnNextState(currentStateVersion, null);
                    } else if (clusterState.term() < request.masterTerm()) {
                        logger.debug(
                            "request routed to master in term [{}] but local term is [{}], waiting for local term bump",
                            request.masterTerm(),
                            clusterState.term()
                        );
                        retry(currentStateVersion, null, cs -> request.masterTerm() <= cs.term());
                    } else {
                        DiscoveryNode masterNode = nodes.getMasterNode();
                        logger.trace("forwarding request [{}] to master [{}]", actionName, masterNode);
                        transportService.sendRequest(
                            masterNode,
                            actionName,
                            new TermOverridingMasterNodeRequest(request, clusterState.term()),
                            new ActionListenerResponseHandler<>(listener, responseReader, executor) {
                                @Override
                                public void handleException(final TransportException exp) {
                                    Throwable cause = exp.unwrapCause();
                                    if (cause instanceof ConnectTransportException
                                        || (exp instanceof RemoteTransportException && cause instanceof NodeClosedException)) {
                                        // we want to retry here a bit to see if a new master is elected
                                        logger.debug(
                                            "connection exception while trying to forward request with action name [{}] to "
                                                + "master node [{}], scheduling a retry. Error: [{}]",
                                            actionName,
                                            nodes.getMasterNode(),
                                            exp.getDetailedMessage()
                                        );
                                        retryOnNextState(currentStateVersion, cause);
                                    } else {
                                        logger.trace(
                                            () -> format("failure when forwarding request [%s] to master [%s]", actionName, masterNode),
                                            exp
                                        );
                                        listener.onFailure(exp);
                                    }
                                }
                            }
                        );
                    }
                }
            } catch (Exception e) {
                logger.trace("top-level failure", e);
                listener.onFailure(e);
            }
        }

        private void retryOnNextState(long currentStateVersion, Throwable failure) {
            retry(currentStateVersion, failure, ClusterStateObserver.NON_NULL_MASTER_PREDICATE);
        }

        private void retry(long currentStateVersion, final Throwable failure, final Predicate<ClusterState> statePredicate) {
            if (observer == null) {
                final TimeValue timeout;
                if (request.masterNodeTimeout().millis() < 0) {
                    timeout = null;
                } else {
                    final long remainingTimeoutMS = request.masterNodeTimeout().millis() - (threadPool.relativeTimeInMillis() - startTime);
                    if (remainingTimeoutMS <= 0) {
                        logger.debug(() -> "timed out before retrying [" + actionName + "] after failure", failure);
                        listener.onFailure(new MasterNotDiscoveredException(failure));
                        return;
                    }
                    timeout = TimeValue.timeValueMillis(remainingTimeoutMS);
                }
                this.observer = new ClusterStateObserver(
                    currentStateVersion,
                    clusterService.getClusterApplierService(),
                    timeout,
                    logger,
                    threadPool.getThreadContext()
                );
            }
            // We track whether we already notified the listener or started executing the action, to avoid invoking the listener twice.
            // Because of that second part, we can not use ActionListener#notifyOnce.
            final var waitComplete = new AtomicBoolean(false);
            if (task instanceof CancellableTask cancellableTask) {
                cancellableTask.addListener(() -> {
                    if (waitComplete.compareAndSet(false, true) == false) {
                        return;
                    }
                    listener.onFailure(new TaskCancelledException("Task was cancelled"));
                    logger.trace("task [{}] was cancelled, notifying listener", task.getId());
                });
            }
            observer.waitForNextChange(new ClusterStateObserver.Listener() {
                @Override
                public void onNewClusterState(ClusterState state) {
                    if (waitComplete.compareAndSet(false, true) == false) {
                        return;
                    }
                    logger.trace("retrying with cluster state version [{}]", state.version());
                    doStart(state);
                }

                @Override
                public void onClusterServiceClose() {
                    if (waitComplete.compareAndSet(false, true) == false) {
                        return;
                    }
                    listener.onFailure(new NodeClosedException(clusterService.localNode()));
                }

                @Override
                public void onTimeout(TimeValue timeout) {
                    if (waitComplete.compareAndSet(false, true) == false) {
                        return;
                    }
                    logger.debug(() -> format("timed out while retrying [%s] after failure (timeout [%s])", actionName, timeout), failure);
                    listener.onFailure(new MasterNotDiscoveredException(failure));
                }

                @Override
                public String toString() {
                    return Strings.format(
                        "listener for [%s] retrying after cluster state version [%d]",
                        AsyncSingleAction.this,
                        currentStateVersion
                    );
                }
            }, clusterState -> isTaskCancelled() || statePredicate.test(clusterState));
        }

        private boolean isTaskCancelled() {
            return task instanceof CancellableTask cancellableTask && cancellableTask.isCancelled();
        }

        @Override
        public String toString() {
            return Strings.format("execution of [%s]", task);
        }
    }
}
