/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.support.local;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchTimeoutException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateObserver;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.node.NodeClosedException;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskManager;

import java.util.concurrent.Executor;

import static org.elasticsearch.common.Strings.format;

/**
 * Analogue of {@link org.elasticsearch.action.support.master.TransportMasterNodeReadAction} except that it runs on the local node rather
 * than delegating to the master.
 */
public abstract class TransportLocalClusterStateAction<Request extends LocalClusterStateRequest, Response extends ActionResponse> extends
    TransportAction<Request, Response> {

    private static final Logger logger = LogManager.getLogger(TransportLocalClusterStateAction.class);

    protected final ClusterService clusterService;
    protected final Executor executor;

    protected TransportLocalClusterStateAction(
        String actionName,
        ActionFilters actionFilters,
        TaskManager taskManager,
        ClusterService clusterService,
        Executor executor
    ) {
        // TODO replace DIRECT_EXECUTOR_SERVICE when removing workaround for https://github.com/elastic/elasticsearch/issues/97916
        super(actionName, actionFilters, taskManager, EsExecutors.DIRECT_EXECUTOR_SERVICE);
        this.clusterService = clusterService;
        this.executor = executor;
    }

    protected abstract ClusterBlockException checkBlock(Request request, ClusterState state);

    protected abstract void localClusterStateOperation(Task task, Request request, ClusterState state, ActionListener<Response> listener)
        throws Exception;

    @Override
    protected final void doExecute(Task task, Request request, ActionListener<Response> listener) {
        final var state = clusterService.state();
        final var clusterBlockException = checkBlock(request, state);
        if (clusterBlockException != null) {
            if (clusterBlockException.retryable() == false) {
                listener.onFailure(clusterBlockException);
            } else {
                waitForClusterUnblock(task, request, listener, state, clusterBlockException);
            }
        } else {
            innerDoExecute(task, request, listener, state);
        }
    }

    private void innerDoExecute(Task task, Request request, ActionListener<Response> listener, ClusterState state) {
        if (task instanceof CancellableTask cancellableTask && cancellableTask.notifyIfCancelled(listener)) {
            return;
        }
        // Workaround for https://github.com/elastic/elasticsearch/issues/97916 - TODO remove this when we can
        executor.execute(ActionRunnable.wrap(listener, l -> localClusterStateOperation(task, request, state, l)));
    }

    private void waitForClusterUnblock(
        Task task,
        Request request,
        ActionListener<Response> listener,
        ClusterState initialState,
        ClusterBlockException exception
    ) {
        var observer = new ClusterStateObserver(
            initialState,
            clusterService,
            request.masterTimeout(),
            logger,
            clusterService.threadPool().getThreadContext()
        );
        observer.waitForNextChange(new ClusterStateObserver.Listener() {
            @Override
            public void onNewClusterState(ClusterState state) {
                logger.trace("retrying with cluster state version [{}]", state.version());
                innerDoExecute(task, request, listener, state);
            }

            @Override
            public void onClusterServiceClose() {
                listener.onFailure(new NodeClosedException(clusterService.localNode()));
            }

            @Override
            public void onTimeout(TimeValue timeout) {
                logger.debug(
                    () -> format("timed out while waiting for cluster to unblock in [%s] (timeout [%s])", actionName, timeout),
                    exception
                );
                listener.onFailure(new ElasticsearchTimeoutException("timed out while waiting for cluster to unblock", exception));
            }
        }, clusterState -> isTaskCancelled(task) || checkBlock(request, clusterState) == null);
    }

    private boolean isTaskCancelled(Task task) {
        return task instanceof CancellableTask cancellableTask && cancellableTask.isCancelled();
    }
}
