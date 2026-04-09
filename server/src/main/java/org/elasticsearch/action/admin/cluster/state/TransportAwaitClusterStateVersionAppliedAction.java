/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.cluster.state;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.action.support.nodes.BaseNodeResponse;
import org.elasticsearch.action.support.nodes.TransportNodesAction;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.TimeoutClusterStateListener;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.node.NodeClosedException;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.AbstractTransportRequest;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

/**
 * An action that waits for a given cluster state version to be applied on provided set of nodes in the cluster.
 */
public class TransportAwaitClusterStateVersionAppliedAction extends TransportNodesAction<
    AwaitClusterStateVersionAppliedRequest,
    AwaitClusterStateVersionAppliedResponse,
    TransportAwaitClusterStateVersionAppliedAction.NodeRequest,
    TransportAwaitClusterStateVersionAppliedAction.NodeResponse,
    Void> {
    public static final ActionType<AwaitClusterStateVersionAppliedResponse> TYPE = new ActionType<>(
        "internal:cluster/nodes/state/await_version"
    );

    private static final Logger logger = LogManager.getLogger(TransportAwaitClusterStateVersionAppliedAction.class);

    private final ThreadPool threadPool;

    @Inject
    public TransportAwaitClusterStateVersionAppliedAction(
        ClusterService clusterService,
        TransportService transportService,
        ActionFilters actionFilters,
        ThreadPool threadPool
    ) {
        super(
            TYPE.name(),
            clusterService,
            transportService,
            actionFilters,
            NodeRequest::new,
            threadPool.executor(ThreadPool.Names.GENERIC)
        );
        this.threadPool = threadPool;
    }

    @Override
    protected AwaitClusterStateVersionAppliedResponse newResponse(
        AwaitClusterStateVersionAppliedRequest request,
        List<NodeResponse> nodeResponses,
        List<FailedNodeException> failures
    ) {
        return new AwaitClusterStateVersionAppliedResponse(clusterService.getClusterName(), nodeResponses, failures);
    }

    @Override
    protected NodeRequest newNodeRequest(AwaitClusterStateVersionAppliedRequest request) {
        return new NodeRequest(request.clusterStateVersion(), request.nodeTimeout());
    }

    @Override
    protected NodeResponse newNodeResponse(StreamInput in, DiscoveryNode node) throws IOException {
        return new NodeResponse(in, node);
    }

    @Override
    protected NodeResponse nodeOperation(NodeRequest request, Task task) {
        /// We are using [#nodeOperationAsync].
        logger.error("expected nodeOperationAsync");
        assert false : "nodeOperationAsync";
        throw new UnsupportedOperationException();
    }

    private class VersionAppliedListener implements TimeoutClusterStateListener {

        private final long clusterStateVersion;
        private final Consumer<Runnable> cancelSubscriber;
        private final ActionListener<Void> listener;

        VersionAppliedListener(long clusterStateVersion, Consumer<Runnable> cancelSubscriber, ActionListener<Void> listener) {
            this.clusterStateVersion = clusterStateVersion;
            this.cancelSubscriber = cancelSubscriber;
            this.listener = listener;
        }

        @Override
        public void postAdded() {
            if (clusterService.state().version() >= clusterStateVersion) {
                removeListener();
                listener.onResponse(null);
            } else {
                cancelSubscriber.accept(VersionAppliedListener.this::removeListener);
            }
        }

        private void removeListener() {
            clusterService.getClusterApplierService().removeTimeoutListener(VersionAppliedListener.this);
        }

        @Override
        public void onClose() {
            removeListener();
            listener.onFailure(new NodeClosedException(clusterService.localNode()));
        }

        @Override
        public void onTimeout(TimeValue timeout) {
            logger.error("no timeout configured");
            assert false : "no timeout configured";
        }

        @Override
        public void clusterChanged(ClusterChangedEvent event) {
            if (event.state().version() >= clusterStateVersion) {
                removeListener();
                listener.onResponse(null);
            }
        }
    }

    @Override
    protected void nodeOperationAsync(NodeRequest request, Task task, ActionListener<NodeResponse> listener) {
        final var onceListener = new SubscribableListener<Void>();
        onceListener.addListener(listener.map(ignored -> new NodeResponse(clusterService.localNode())));

        if (request.timeout != TimeValue.MINUS_ONE) {
            onceListener.addTimeout(request.timeout, threadPool, EsExecutors.DIRECT_EXECUTOR_SERVICE);
        }

        final var cancellableTask = (CancellableTask) task;
        cancellableTask.addListener(() -> onceListener.onFailure(new TaskCancelledException(cancellableTask.getReasonCancelled())));

        clusterService.getClusterApplierService()
            .addTimeoutListener(
                null,
                new VersionAppliedListener(
                    request.clusterStateVersion,
                    r -> onceListener.addListener(ActionListener.running(r)),
                    onceListener
                )
            );
    }

    public static class NodeRequest extends AbstractTransportRequest {
        private final long clusterStateVersion;
        private final TimeValue timeout;

        NodeRequest(StreamInput in) throws IOException {
            super(in);
            this.clusterStateVersion = in.readLong();
            this.timeout = in.readTimeValue();
        }

        NodeRequest(long clusterStateVersion, TimeValue timeout) {
            this.clusterStateVersion = clusterStateVersion;
            this.timeout = timeout;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeLong(clusterStateVersion);
            out.writeTimeValue(timeout);
        }

        @Override
        public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
            return new CancellableTask(id, type, action, "", parentTaskId, headers) {
                @Override
                public String getDescription() {
                    return Strings.format("waiting for cluster state version=%s to be applied", clusterStateVersion);
                }
            };
        }
    }

    public static class NodeResponse extends BaseNodeResponse {
        NodeResponse(StreamInput in, DiscoveryNode node) throws IOException {
            super(in, node);
        }

        NodeResponse(DiscoveryNode node) {
            super(node);
        }
    }
}
