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
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.health.node.selection.HealthNode;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportService;

import static org.elasticsearch.core.Strings.format;

/**
 * A base class for operations that need to be performed on the health node.
 */
public abstract class TransportHealthNodeAction<Request extends HealthNodeRequest, Response extends ActionResponse> extends
    HandledTransportAction<Request, Response> {

    private static final Logger logger = LogManager.getLogger(TransportHealthNodeAction.class);

    /**
     * This is the amount of time given as the timeout for transport requests to the health node. The default is fairly low because these
     * actions are expected to be lightweight and time-sensitive.
     */
    public static final Setting<TimeValue> HEALTH_NODE_TRANSPORT_ACTION_TIMEOUT = Setting.timeSetting(
        "health_node.transport_action_timeout",
        TimeValue.timeValueSeconds(5),
        TimeValue.timeValueMillis(1),
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    protected final TransportService transportService;
    protected final ClusterService clusterService;
    protected final ThreadPool threadPool;
    protected final String executor;
    private TimeValue healthNodeTransportActionTimeout;

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
        this.healthNodeTransportActionTimeout = HEALTH_NODE_TRANSPORT_ACTION_TIMEOUT.get(clusterService.getSettings());
        clusterService.getClusterSettings()
            .addSettingsUpdateConsumer(
                HEALTH_NODE_TRANSPORT_ACTION_TIMEOUT,
                newTimeout -> this.healthNodeTransportActionTimeout = newTimeout
            );
    }

    protected abstract void healthOperation(Task task, Request request, ClusterState state, ActionListener<Response> listener)
        throws Exception;

    @Override
    protected void doExecute(Task task, final Request request, ActionListener<Response> listener) {
        ClusterState state = clusterService.state();
        logger.trace("starting to process request [{}] with cluster state version [{}]", request, state.version());
        if (isTaskCancelled(task)) {
            listener.onFailure(new TaskCancelledException("Task was cancelled"));
            return;
        }
        try {
            ClusterState clusterState = clusterService.state();
            DiscoveryNode healthNode = HealthNode.findHealthNode(clusterState);
            DiscoveryNode localNode = clusterState.nodes().getLocalNode();
            if (healthNode == null) {
                listener.onFailure(new HealthNodeNotDiscoveredException());
            } else if (localNode.getId().equals(healthNode.getId())) {
                threadPool.executor(executor).execute(() -> {
                    try {
                        if (isTaskCancelled(task)) {
                            listener.onFailure(new TaskCancelledException("Task was cancelled"));
                        } else {
                            healthOperation(task, request, clusterState, listener);
                        }
                    } catch (Exception e) {
                        listener.onFailure(e);
                    }
                });
            } else {
                logger.trace("forwarding request [{}] to health node [{}]", actionName, healthNode);
                ActionListenerResponseHandler<Response> handler = new ActionListenerResponseHandler<>(listener, responseReader) {
                    @Override
                    public void handleException(final TransportException exception) {
                        logger.trace(
                            () -> format("failure when forwarding request [%s] to health node [%s]", actionName, healthNode),
                            exception
                        );
                        listener.onFailure(exception);
                    }
                };
                if (task != null) {
                    transportService.sendChildRequest(
                        healthNode,
                        actionName,
                        request,
                        task,
                        TransportRequestOptions.timeout(healthNodeTransportActionTimeout),
                        handler
                    );
                } else {
                    transportService.sendRequest(healthNode, actionName, request, handler);
                }
            }
        } catch (Exception e) {
            logger.trace(() -> format("Failed to route/execute health node action %s", actionName), e);
            listener.onFailure(e);
        }
    }

    private static boolean isTaskCancelled(Task task) {
        return (task instanceof CancellableTask t) && t.isCancelled();
    }
}
