/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.MasterNodeRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.core.ml.inference.assignment.RoutingInfoUpdate;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;

import java.io.IOException;
import java.util.Objects;

public class UpdateTrainedModelAssignmentRoutingInfoAction extends ActionType<AcknowledgedResponse> {
    public static final UpdateTrainedModelAssignmentRoutingInfoAction INSTANCE = new UpdateTrainedModelAssignmentRoutingInfoAction();
    public static final String NAME = "cluster:internal/xpack/ml/model_allocation/update";

    private UpdateTrainedModelAssignmentRoutingInfoAction() {
        super(NAME, AcknowledgedResponse::readFrom);
    }

    public static class Request extends MasterNodeRequest<Request> {
        private final String nodeId;
        private final String deploymentId;
        private final RoutingInfoUpdate update;

        public Request(String nodeId, String deploymentId, RoutingInfoUpdate update) {
            this.nodeId = ExceptionsHelper.requireNonNull(nodeId, "node_id");
            this.deploymentId = ExceptionsHelper.requireNonNull(deploymentId, "deployment_id");
            this.update = ExceptionsHelper.requireNonNull(update, "update");
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            this.nodeId = in.readString();
            this.deploymentId = in.readString();
            this.update = new RoutingInfoUpdate(in);
        }

        public String getNodeId() {
            return nodeId;
        }

        public String getDeploymentId() {
            return deploymentId;
        }

        public RoutingInfoUpdate getUpdate() {
            return update;
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(nodeId);
            out.writeString(deploymentId);
            update.writeTo(out);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Request request = (Request) o;
            return Objects.equals(nodeId, request.nodeId)
                && Objects.equals(deploymentId, request.deploymentId)
                && Objects.equals(update, request.update);
        }

        @Override
        public int hashCode() {
            return Objects.hash(nodeId, deploymentId, update);
        }

        @Override
        public String toString() {
            return "Request{" + "nodeId='" + nodeId + '\'' + ", deploymentId='" + deploymentId + '\'' + ", update=" + update + '}';
        }
    }

}
