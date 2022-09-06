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
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
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
import java.util.Objects;

/**
 * This action allows a node to send their health info to the selected health node.
 * When the health node receives the health info, it will update the internal cache
 * regarding this node.
 */
public class UpdateHealthInfoCacheAction extends ActionType<AcknowledgedResponse> {

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

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(nodeId);
            diskHealthInfo.writeTo(out);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Request request = (Request) o;
            return Objects.equals(nodeId, request.nodeId) && Objects.equals(diskHealthInfo, request.diskHealthInfo);
        }

        @Override
        public int hashCode() {
            return Objects.hash(nodeId, diskHealthInfo);
        }
    }

    public static final UpdateHealthInfoCacheAction INSTANCE = new UpdateHealthInfoCacheAction();
    public static final String NAME = "cluster:monitor/update/health/info";

    private UpdateHealthInfoCacheAction() {
        super(NAME, AcknowledgedResponse::readFrom);
    }

    public static class TransportAction extends TransportHealthNodeAction<Request, AcknowledgedResponse> {
        private final HealthInfoCache nodeHealthOverview;

        @Inject
        public TransportAction(
            TransportService transportService,
            ClusterService clusterService,
            ThreadPool threadPool,
            ActionFilters actionFilters,
            HealthInfoCache nodeHealthOverview
        ) {
            super(
                UpdateHealthInfoCacheAction.NAME,
                transportService,
                clusterService,
                threadPool,
                actionFilters,
                UpdateHealthInfoCacheAction.Request::new,
                AcknowledgedResponse::readFrom,
                ThreadPool.Names.MANAGEMENT
            );
            this.nodeHealthOverview = nodeHealthOverview;
        }

        @Override
        protected void healthOperation(
            Task task,
            Request request,
            ClusterState clusterState,
            ActionListener<AcknowledgedResponse> listener
        ) {
            nodeHealthOverview.updateNodeHealth(request.getNodeId(), request.getDiskHealthInfo());
            listener.onResponse(AcknowledgedResponse.of(true));
        }
    }
}
