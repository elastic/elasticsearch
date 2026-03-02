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
import org.elasticsearch.core.Predicates;
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
import java.util.function.Predicate;

import static org.elasticsearch.core.Strings.format;

/**
 * A base class for operations that need to be performed on the master node.
 *
 * <p>Many cluster operations (e.g. creating indices, managing snapshots) result in
 * {@link org.elasticsearch.cluster.ClusterState} updates and must therefore run on the elected master
 * node. This class provides a common framework that handles routing requests to the current master,
 * retrying when the master changes, and checking for cluster blocks.
 *
 * <h2>Subclasses responsibility</h2>
 *
 * <p>Subclasses must implement two abstract methods:
 * <ul>
 *   <li>{@link #masterOperation} – the actual operation to perform on the master. This typically
 *       involves submitting a cluster state update task to the
 *       {@link org.elasticsearch.cluster.service.MasterService}.</li>
 *   <li>{@link #checkBlock} – returns a
 *       {@link org.elasticsearch.cluster.block.ClusterBlockException} if the action should be blocked
 *       for the current cluster state (e.g. an index write block during a close operation), or
 *       {@code null} if the action can proceed.</li>
 * </ul>
 *
 * <h2>Request Types</h2>
 *
 * <p> The {@link MasterNodeRequest} is the request type this class is parameterized on. It carries two
 * fields relevant to master routing:
 * <ul>
 *   <li>{@code masterNodeTimeout} – how long the request will wait when no master is available or the
 *       current master is unreachable. For REST-originated requests this is set via the
 *       {@code master_timeout} query parameter; internally-generated requests typically use
 *       {@code INFINITE_MASTER_NODE_TIMEOUT} to wait indefinitely.</li>
 *   <li>{@code masterTerm} – the term of the cluster state used to route this request, which prevents
 *       routing loops. When a node receives a forwarded request whose {@code masterTerm} exceeds its
 *       own cluster state term, it waits for its local term to catch up before proceeding rather than
 *       forwarding the request again.</li>
 * </ul>
 *
 * <p>The {@link #localExecute} method can be overridden to allow the action to run on the local node
 * rather than being forwarded to the master (see {@link TransportMasterNodeReadAction} as an example).
 * This is typically used for read-only operations (e.g. cluster health or cluster state queries) where
 * a potentially stale local view is acceptable.
 *
 * <h2>Execution Flow</h2>
 *
 * <p>Execution is delegated to an {@code AsyncSingleAction}, which handles routing, async execution,
 * and result listener registration.
 *
 * <p>If the local node is the master (or {@link #localExecute} returns {@code true}):
 * <ol>
 *   <li>Cluster blocks are checked via {@link #checkBlock}. A non-retryable block fails the request
 *       immediately. A retryable block causes a retry until the block clears.</li>
 *   <li>If there are no blocks, {@link #masterOperation} is invoked on the configured executor.</li>
 *   <li>If {@code masterOperation} fails with a publish failure
 *       ({@link org.elasticsearch.cluster.NotMasterException} or
 *       {@link org.elasticsearch.cluster.coordination.FailedToCommitClusterStateException}), the
 *       action retries on the next master.</li>
 * </ol>
 *
 * <p>If the local node is not the master:
 * <ol>
 *   <li>If no master is known, the action retries until a master appears.</li>
 *   <li>If the local cluster state term is lower than the request's {@code masterTerm}, the action
 *       waits for the local term to catch up to avoid routing loops.</li>
 *   <li>Otherwise, the request is forwarded to the known master via the transport layer. If the
 *       connection fails ({@link org.elasticsearch.transport.ConnectTransportException}) or the target
 *       node is closing ({@link org.elasticsearch.node.NodeClosedException}), the action retries on
 *       the next available master.</li>
 * </ol>
 *
 * <h2>Retry Mechanism</h2>
 *
 * <p>All retries are driven by a {@link org.elasticsearch.cluster.ClusterStateObserver}, which watches
 * for cluster state changes satisfying a given predicate. The observer uses the remaining
 * {@code masterNodeTimeout} as its deadline. If no suitable state appears before the timeout expires,
 * the request fails with a {@link org.elasticsearch.discovery.MasterNotDiscoveredException}. For
 * {@link org.elasticsearch.tasks.CancellableTask}s, a cancellation listener is also registered so
 * that the retry aborts immediately if the task is canceled. Each retry re-enters {@code doStart}
 * with the new cluster state, re-evaluating the routing decision from scratch.
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
            request::toString
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
            final var waitComplete = Predicates.once();
            if (task instanceof CancellableTask cancellableTask) {
                cancellableTask.addListener(() -> {
                    if (waitComplete.getAsBoolean() == false) {
                        return;
                    }
                    listener.onFailure(new TaskCancelledException("Task was cancelled"));
                    logger.trace("task [{}] was cancelled, notifying listener", task.getId());
                });
            }
            observer.waitForNextChange(new ClusterStateObserver.Listener() {
                @Override
                public void onNewClusterState(ClusterState state) {
                    if (waitComplete.getAsBoolean() == false) {
                        return;
                    }
                    logger.trace("retrying with cluster state version [{}]", state.version());
                    doStart(state);
                }

                @Override
                public void onClusterServiceClose() {
                    if (waitComplete.getAsBoolean() == false) {
                        return;
                    }
                    listener.onFailure(new NodeClosedException(clusterService.localNode()));
                }

                @Override
                public void onTimeout(TimeValue timeout) {
                    if (waitComplete.getAsBoolean() == false) {
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
