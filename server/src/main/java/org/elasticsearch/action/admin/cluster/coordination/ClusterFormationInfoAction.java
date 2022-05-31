/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.coordination;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.cluster.coordination.ClusterFormationFailureHelper;
import org.elasticsearch.cluster.coordination.Coordinator;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.Objects;

public class ClusterFormationInfoAction extends ActionType<ClusterFormationInfoAction.Response> {

    public static final ClusterFormationInfoAction INSTANCE = new ClusterFormationInfoAction();
    public static final String NAME = "cluster:internal/formation/info";

    private ClusterFormationInfoAction() {
        super(NAME, ClusterFormationInfoAction.Response::new);
    }

    public static class Request extends ActionRequest {

        public Request() {}

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        public Request(StreamInput in) throws IOException {
            super(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
        }

        @Override
        public boolean equals(Object o) {
            // There are no parameters, so all instances of this class are equal
            if (this == o) return true;
            return o != null && getClass() == o.getClass();
        }

        @Override
        public int hashCode() {
            // There are no parameters, so all instances of this class are equal
            return 1;
        }

    }

    public static class Response extends ActionResponse {

        private final String nodeId;
        private final ClusterFormationFailureHelper.ClusterFormationState clusterFormationState;

        public Response(StreamInput in) throws IOException {
            super(in);
            nodeId = in.readString();
            clusterFormationState = new ClusterFormationFailureHelper.ClusterFormationState(in);
        }

        public Response(DiscoveryNode node, ClusterFormationFailureHelper.ClusterFormationState clusterFormationState) {
            this.nodeId = node.getId();
            this.clusterFormationState = clusterFormationState;
        }

        public ClusterFormationFailureHelper.ClusterFormationState getClusterFormationState() {
            return clusterFormationState;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(nodeId);
            clusterFormationState.writeTo(out);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ClusterFormationInfoAction.Response response = (ClusterFormationInfoAction.Response) o;
            return nodeId.equals(response.nodeId) && clusterFormationState.equals(response.clusterFormationState);
        }

        @Override
        public int hashCode() {
            return Objects.hash(nodeId, clusterFormationState);
        }
    }

    /**
     * This transport action fetches the MasterHistory from a remote node.
     */
    public static class TransportAction extends HandledTransportAction<
        ClusterFormationInfoAction.Request,
        ClusterFormationInfoAction.Response> {
        private final ClusterService clusterService;
        private final Coordinator coordinator;

        @Inject
        public TransportAction(
            ClusterService clusterService,
            TransportService transportService,
            ActionFilters actionFilters,
            Coordinator coordinator
        ) {
            super(ClusterFormationInfoAction.NAME, transportService, actionFilters, ClusterFormationInfoAction.Request::new);
            this.clusterService = clusterService;
            this.coordinator = coordinator;
        }

        @Override
        protected void doExecute(
            Task task,
            ClusterFormationInfoAction.Request request,
            ActionListener<ClusterFormationInfoAction.Response> listener
        ) {
            listener.onResponse(
                new ClusterFormationInfoAction.Response(clusterService.localNode(), coordinator.getClusterFormationState())
            );
        }
    }

}
