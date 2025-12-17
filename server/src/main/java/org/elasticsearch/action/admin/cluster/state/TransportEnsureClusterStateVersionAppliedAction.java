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
import org.elasticsearch.ElasticsearchTimeoutException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.nodes.BaseNodeResponse;
import org.elasticsearch.action.support.nodes.TransportNodesAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateObserver;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.node.NodeClosedException;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.AbstractTransportRequest;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

/**
 * An action that waits for a given cluster state version to be applied on provided set of nodes in the cluster.
 */
public class TransportEnsureClusterStateVersionAppliedAction extends TransportNodesAction<
    EnsureClusterStateVersionAppliedRequest,
    EnsureClusterStateVersionAppliedResponse,
    TransportEnsureClusterStateVersionAppliedAction.NodeRequest,
    TransportEnsureClusterStateVersionAppliedAction.NodeResponse,
    Void> {
    public static final ActionType<EnsureClusterStateVersionAppliedResponse> TYPE = new ActionType<>(
        "internal:cluster/nodes/state/wait_for_version"
    );

    private static final Logger logger = LogManager.getLogger(TransportEnsureClusterStateVersionAppliedAction.class);

    private final ThreadPool threadPool;

    @Inject
    public TransportEnsureClusterStateVersionAppliedAction(
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
    protected EnsureClusterStateVersionAppliedResponse newResponse(
        EnsureClusterStateVersionAppliedRequest request,
        List<NodeResponse> nodeResponses,
        List<FailedNodeException> failures
    ) {
        return new EnsureClusterStateVersionAppliedResponse(clusterService.getClusterName(), nodeResponses, failures);
    }

    @Override
    protected NodeRequest newNodeRequest(EnsureClusterStateVersionAppliedRequest request) {
        return new NodeRequest(request.clusterStateVersion());
    }

    @Override
    protected NodeResponse newNodeResponse(StreamInput in, DiscoveryNode node) throws IOException {
        return new NodeResponse(in, node);
    }

    @Override
    protected NodeResponse nodeOperation(NodeRequest request, Task task) {
        // We are using nodeOperationAsync(...).
        throw new UnsupportedOperationException();
    }

    @Override
    protected void nodeOperationAsync(NodeRequest request, Task task, ActionListener<NodeResponse> listener) {
        var cancellableTask = (CancellableTask) task;

        Predicate<ClusterState> predicate = (ClusterState state) -> cancellableTask.isCancelled()
            || state.version() == request.clusterStateVersion;

        var clusterStateListener = new ClusterStateObserver.Listener() {
            @Override
            public void onNewClusterState(ClusterState state) {
                if (cancellableTask.notifyIfCancelled(listener)) {
                    return;
                }

                listener.onResponse(new NodeResponse(clusterService.localNode()));
            }

            @Override
            public void onClusterServiceClose() {
                listener.onFailure(new NodeClosedException(clusterService.localNode()));
            }

            @Override
            public void onTimeout(TimeValue timeout) {
                // TODO ??
                listener.onFailure(new ElasticsearchTimeoutException(""));
            }
        };

        ClusterStateObserver.waitForState(clusterService, threadPool.getThreadContext(), clusterStateListener, predicate, null, logger);
    }

    public static class NodeRequest extends AbstractTransportRequest {
        private final long clusterStateVersion;

        public NodeRequest(StreamInput in) throws IOException {
            super(in);
            this.clusterStateVersion = in.readVLong();
        }

        public NodeRequest(long clusterStateVersion) {
            this.clusterStateVersion = clusterStateVersion;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeVLong(clusterStateVersion);
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
        public NodeResponse(StreamInput in, DiscoveryNode node) throws IOException {
            super(in, node);
        }

        public NodeResponse(StreamInput in) throws IOException {
            super(in);
        }

        public NodeResponse(DiscoveryNode node) {
            super(node);
        }
    }
}
