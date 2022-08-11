/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.health.node;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.health.node.action.TransportHealthNodeAction;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;

public class UpdateHealthInfoCacheAction extends ActionType<UpdateHealthInfoCacheAction.Response> {

    public static class Request extends ActionRequest {
        private final String nodeId;
        private final DiskHealthInfo diskHealthInfo;

        public Request(String nodeId, DiskHealthInfo diskHealthInfo) {
            this.nodeId = nodeId;
            this.diskHealthInfo = diskHealthInfo;
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            this.nodeId = in.readString();
            this.diskHealthInfo = new DiskHealthInfo(in);
        }

        public String getNodeId() {
            return nodeId;
        }

        public DiskHealthInfo getDiskHealthInfo() {
            return diskHealthInfo;
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }
    }

    public static class Response extends ActionResponse {
        public Response(StreamInput input) {

        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {

        }
    }

    public static final UpdateHealthInfoCacheAction INSTANCE = new UpdateHealthInfoCacheAction();
    public static final String NAME = "cluster:update/node/health/info";

    private UpdateHealthInfoCacheAction() {
        super(NAME, UpdateHealthInfoCacheAction.Response::new);
    }

    public static class TransportAction extends TransportHealthNodeAction<Request, Response> {
        private final HealthInfoCache nodeHealthOverview;

        @Inject
        public TransportAction(
            String actionName,
            TransportService transportService,
            ClusterService clusterService,
            ThreadPool threadPool,
            ActionFilters actionFilters,
            HealthInfoCache nodeHealthOverview
        ) {
            super(
                actionName,
                transportService,
                clusterService,
                threadPool,
                actionFilters,
                UpdateHealthInfoCacheAction.Request::new,
                UpdateHealthInfoCacheAction.Response::new,
                ThreadPool.Names.MANAGEMENT
            );
            this.nodeHealthOverview = nodeHealthOverview;
        }

        @Override
        protected void healthOperation(Task task, Request request, ClusterState clusterState, ActionListener<Response> listener) {
            nodeHealthOverview.updateNodeHealth(request.getNodeId(), request.getDiskHealthInfo());
            listener.onResponse(null);
        }
    }
}
