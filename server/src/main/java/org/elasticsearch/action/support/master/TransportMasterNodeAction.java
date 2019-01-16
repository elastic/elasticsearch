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

package org.elasticsearch.action.support.master;

import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateObserver;
import org.elasticsearch.cluster.MasterNodeChangePredicate;
import org.elasticsearch.cluster.NotMasterException;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.cluster.coordination.FailedToCommitClusterStateException;
import org.elasticsearch.discovery.MasterNotDiscoveredException;
import org.elasticsearch.node.NodeClosedException;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.ConnectTransportException;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.function.Predicate;
import java.util.function.Supplier;

/**
 * A base class for operations that needs to be performed on the master node.
 */
public abstract class TransportMasterNodeAction<Request extends MasterNodeRequest<Request>, Response extends ActionResponse>
    extends HandledTransportAction<Request, Response> {

    protected final ThreadPool threadPool;
    protected final TransportService transportService;
    protected final ClusterService clusterService;
    protected final IndexNameExpressionResolver indexNameExpressionResolver;

    private final String executor;

    protected TransportMasterNodeAction(String actionName, TransportService transportService,
                                        ClusterService clusterService, ThreadPool threadPool, ActionFilters actionFilters,
                                        IndexNameExpressionResolver indexNameExpressionResolver, Supplier<Request> request) {
        this(actionName, true, transportService, clusterService, threadPool, actionFilters, indexNameExpressionResolver, request);
    }

    protected TransportMasterNodeAction(String actionName, TransportService transportService,
                                        ClusterService clusterService, ThreadPool threadPool, ActionFilters actionFilters,
                                        Writeable.Reader<Request> request, IndexNameExpressionResolver indexNameExpressionResolver) {
        this(actionName, true, transportService, clusterService, threadPool, actionFilters, request, indexNameExpressionResolver);
    }

    protected TransportMasterNodeAction(String actionName, boolean canTripCircuitBreaker,
                                        TransportService transportService, ClusterService clusterService, ThreadPool threadPool,
                                        ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver,
                                        Supplier<Request> request) {
        super(actionName, canTripCircuitBreaker, transportService, actionFilters, request);
        this.transportService = transportService;
        this.clusterService = clusterService;
        this.threadPool = threadPool;
        this.indexNameExpressionResolver = indexNameExpressionResolver;
        this.executor = executor();
    }

    protected TransportMasterNodeAction(String actionName, boolean canTripCircuitBreaker,
                                        TransportService transportService, ClusterService clusterService, ThreadPool threadPool,
                                        ActionFilters actionFilters, Writeable.Reader<Request> request,
                                        IndexNameExpressionResolver indexNameExpressionResolver) {
        super(actionName, canTripCircuitBreaker, transportService, actionFilters, request);
        this.transportService = transportService;
        this.clusterService = clusterService;
        this.threadPool = threadPool;
        this.indexNameExpressionResolver = indexNameExpressionResolver;
        this.executor = executor();
    }

    protected abstract String executor();

    /**
     * @deprecated new implementors should override {@link #read(StreamInput)} and use the
     *             {@link Writeable.Reader} interface.
     * @return a new response instance. Typically this is used for serialization using the
     *         {@link Streamable#readFrom(StreamInput)} method.
     */
    @Deprecated
    protected abstract Response newResponse();

    protected Response read(StreamInput in) throws IOException {
        Response response = newResponse();
        response.readFrom(in);
        return response;
    }

    protected abstract void masterOperation(Request request, ClusterState state, ActionListener<Response> listener) throws Exception;

    /**
     * Override this operation if access to the task parameter is needed
     */
    protected void masterOperation(Task task, Request request, ClusterState state, ActionListener<Response> listener) throws Exception {
        masterOperation(request, state, listener);
    }

    protected boolean localExecute(Request request) {
        return false;
    }

    protected abstract ClusterBlockException checkBlock(Request request, ClusterState state);

    @Override
    protected void doExecute(Task task, final Request request, ActionListener<Response> listener) {
        new AsyncSingleAction(task, request, listener).start();
    }

    class AsyncSingleAction {

        private final ActionListener<Response> listener;
        private final Request request;
        private volatile ClusterStateObserver observer;
        private final Task task;

        AsyncSingleAction(Task task, Request request, ActionListener<Response> listener) {
            this.task = task;
            this.request = request;
            if (task != null) {
                request.setParentTask(clusterService.localNode().getId(), task.getId());
            }
            this.listener = listener;
        }

        public void start() {
            ClusterState state = clusterService.state();
            this.observer
                = new ClusterStateObserver(state, clusterService, request.masterNodeTimeout(), logger, threadPool.getThreadContext());
            doStart(state);
        }

        protected void doStart(ClusterState clusterState) {
            try {
                final Predicate<ClusterState> masterChangePredicate = MasterNodeChangePredicate.build(clusterState);
                final DiscoveryNodes nodes = clusterState.nodes();
                if (nodes.isLocalNodeElectedMaster() || localExecute(request)) {
                    // check for block, if blocked, retry, else, execute locally
                    final ClusterBlockException blockException = checkBlock(request, clusterState);
                    if (blockException != null) {
                        if (!blockException.retryable()) {
                            listener.onFailure(blockException);
                        } else {
                            logger.trace("can't execute due to a cluster block, retrying", blockException);
                            retry(blockException, newState -> {
                                try {
                                    ClusterBlockException newException = checkBlock(request, newState);
                                    return (newException == null || !newException.retryable());
                                } catch (Exception e) {
                                    // accept state as block will be rechecked by doStart() and listener.onFailure() then called
                                    logger.trace("exception occurred during cluster block checking, accepting state", e);
                                    return true;
                                }
                            });
                        }
                    } else {
                        ActionListener<Response> delegate = new ActionListener<Response>() {
                            @Override
                            public void onResponse(Response response) {
                                listener.onResponse(response);
                            }

                            @Override
                            public void onFailure(Exception t) {
                                if (t instanceof FailedToCommitClusterStateException || t instanceof NotMasterException) {
                                    logger.debug(() -> new ParameterizedMessage("master could not publish cluster state or " +
                                        "stepped down before publishing action [{}], scheduling a retry", actionName), t);
                                    retry(t, masterChangePredicate);
                                } else {
                                    listener.onFailure(t);
                                }
                            }
                        };
                        threadPool.executor(executor).execute(new ActionRunnable<Response>(delegate) {
                            @Override
                            protected void doRun() throws Exception {
                                masterOperation(task, request, clusterState, delegate);
                            }
                        });
                    }
                } else {
                    if (nodes.getMasterNode() == null) {
                        logger.debug("no known master node, scheduling a retry");
                        retry(null, masterChangePredicate);
                    } else {
                        DiscoveryNode masterNode = nodes.getMasterNode();
                        final String actionName = getMasterActionName(masterNode);
                        transportService.sendRequest(masterNode, actionName, request,
                            new ActionListenerResponseHandler<Response>(listener, TransportMasterNodeAction.this::read) {
                                @Override
                                public void handleException(final TransportException exp) {
                                    Throwable cause = exp.unwrapCause();
                                    if (cause instanceof ConnectTransportException) {
                                        // we want to retry here a bit to see if a new master is elected
                                        logger.debug("connection exception while trying to forward request with action name [{}] to " +
                                                "master node [{}], scheduling a retry. Error: [{}]",
                                            actionName, nodes.getMasterNode(), exp.getDetailedMessage());
                                        retry(cause, masterChangePredicate);
                                    } else {
                                        listener.onFailure(exp);
                                    }
                                }
                        });
                    }
                }
            } catch (Exception e) {
                listener.onFailure(e);
            }
        }

        private void retry(final Throwable failure, final Predicate<ClusterState> statePredicate) {
            observer.waitForNextChange(
                new ClusterStateObserver.Listener() {
                    @Override
                    public void onNewClusterState(ClusterState state) {
                        doStart(state);
                    }

                    @Override
                    public void onClusterServiceClose() {
                        listener.onFailure(new NodeClosedException(clusterService.localNode()));
                    }

                    @Override
                    public void onTimeout(TimeValue timeout) {
                        logger.debug(() -> new ParameterizedMessage("timed out while retrying [{}] after failure (timeout [{}])",
                            actionName, timeout), failure);
                        listener.onFailure(new MasterNotDiscoveredException(failure));
                    }
                }, statePredicate
            );
        }
    }

    /**
     * Allows to conditionally return a different master node action name in the case an action gets renamed.
     * This mainly for backwards compatibility should be used rarely
     */
    protected String getMasterActionName(DiscoveryNode node) {
        return actionName;
    }
}
