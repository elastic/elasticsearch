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
import org.elasticsearch.xpack.core.ml.inference.assignment.RoutingInfo;
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
        private final String modelId;
        private final RoutingInfo routingInfo;

        public Request(String nodeId, String modelId, RoutingInfo routingInfo) {
            this.nodeId = ExceptionsHelper.requireNonNull(nodeId, "node_id");
            this.modelId = ExceptionsHelper.requireNonNull(modelId, "model_id");
            this.routingInfo = ExceptionsHelper.requireNonNull(routingInfo, "routing_info");
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            this.nodeId = in.readString();
            this.modelId = in.readString();
            this.routingInfo = new RoutingInfo(in);
        }

        public String getNodeId() {
            return nodeId;
        }

        public String getModelId() {
            return modelId;
        }

        public RoutingInfo getRoutingInfo() {
            return routingInfo;
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(nodeId);
            out.writeString(modelId);
            routingInfo.writeTo(out);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Request request = (Request) o;
            return Objects.equals(nodeId, request.nodeId)
                && Objects.equals(modelId, request.modelId)
                && Objects.equals(routingInfo, request.routingInfo);
        }

        @Override
        public int hashCode() {
            return Objects.hash(nodeId, modelId, routingInfo);
        }

        @Override
        public String toString() {
            return "Request{" + "nodeId='" + nodeId + '\'' + ", modelId='" + modelId + '\'' + ", routingInfo=" + routingInfo + '}';
        }
    }

}
