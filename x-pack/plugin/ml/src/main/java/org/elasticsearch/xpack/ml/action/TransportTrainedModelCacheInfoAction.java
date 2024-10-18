/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.action;

import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.nodes.TransportNodesAction;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.UpdateForV9;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.ml.action.TrainedModelCacheInfoAction;
import org.elasticsearch.xpack.core.ml.action.TrainedModelCacheInfoAction.Response.CacheInfo;
import org.elasticsearch.xpack.ml.inference.loadingservice.ModelLoadingService;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class TransportTrainedModelCacheInfoAction extends TransportNodesAction<
    TrainedModelCacheInfoAction.Request,
    TrainedModelCacheInfoAction.Response,
    TransportTrainedModelCacheInfoAction.NodeModelCacheInfoRequest,
    CacheInfo,
    Void> {

    private final ModelLoadingService modelLoadingService;

    @Inject
    public TransportTrainedModelCacheInfoAction(
        ThreadPool threadPool,
        ClusterService clusterService,
        TransportService transportService,
        ActionFilters actionFilters,
        ModelLoadingService modelLoadingService
    ) {
        super(
            TrainedModelCacheInfoAction.NAME,
            clusterService,
            transportService,
            actionFilters,
            NodeModelCacheInfoRequest::new,
            threadPool.executor(ThreadPool.Names.MANAGEMENT)
        );
        this.modelLoadingService = modelLoadingService;
    }

    @Override
    protected TrainedModelCacheInfoAction.Response newResponse(
        TrainedModelCacheInfoAction.Request request,
        List<CacheInfo> responses,
        List<FailedNodeException> failures
    ) {
        return new TrainedModelCacheInfoAction.Response(clusterService.getClusterName(), responses, failures);
    }

    @Override
    protected NodeModelCacheInfoRequest newNodeRequest(TrainedModelCacheInfoAction.Request request) {
        return new NodeModelCacheInfoRequest();
    }

    @Override
    protected CacheInfo newNodeResponse(StreamInput in, DiscoveryNode node) throws IOException {
        return new CacheInfo(in);
    }

    @Override
    protected CacheInfo nodeOperation(NodeModelCacheInfoRequest nodeModelCacheInfoRequest, Task task) {
        assert task instanceof CancellableTask;
        return new CacheInfo(
            transportService.getLocalNode(),
            modelLoadingService.getMaxCacheSize(),
            modelLoadingService.getCurrentCacheSize()
        );
    }

    @UpdateForV9(owner = UpdateForV9.Owner.MACHINE_LEARNING) // this can be replaced with TransportRequest.Empty in v9
    public static class NodeModelCacheInfoRequest extends TransportRequest {

        NodeModelCacheInfoRequest() {}

        public NodeModelCacheInfoRequest(StreamInput in) throws IOException {
            super(in);
        }

        @Override
        public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
            return new CancellableTask(id, type, action, "", parentTaskId, headers);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
        }
    }
}
