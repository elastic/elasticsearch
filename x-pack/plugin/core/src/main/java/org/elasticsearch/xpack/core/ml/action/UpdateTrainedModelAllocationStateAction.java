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
import org.elasticsearch.xpack.core.ml.inference.allocation.RoutingStateAndReason;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;

import java.io.IOException;
import java.util.Objects;

public class UpdateTrainedModelAllocationStateAction extends ActionType<AcknowledgedResponse> {
    public static final UpdateTrainedModelAllocationStateAction INSTANCE = new UpdateTrainedModelAllocationStateAction();
    public static final String NAME = "cluster:internal/xpack/ml/model_allocation/update";

    private UpdateTrainedModelAllocationStateAction() {
        super(NAME, AcknowledgedResponse::readFrom);
    }

    public static class Request extends MasterNodeRequest<Request> {
        private final String nodeId;
        private final String modelId;
        private final RoutingStateAndReason routingState;

        public Request(String nodeId, String modelId, RoutingStateAndReason routingState) {
            this.nodeId = ExceptionsHelper.requireNonNull(nodeId, "node_id");
            this.modelId = ExceptionsHelper.requireNonNull(modelId, "model_id");
            this.routingState = ExceptionsHelper.requireNonNull(routingState, "routing_state");
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            this.nodeId = in.readString();
            this.modelId = in.readString();
            this.routingState = new RoutingStateAndReason(in);
        }

        public String getNodeId() {
            return nodeId;
        }

        public String getModelId() {
            return modelId;
        }

        public RoutingStateAndReason getRoutingState() {
            return routingState;
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
            routingState.writeTo(out);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Request request = (Request) o;
            return Objects.equals(nodeId, request.nodeId)
                && Objects.equals(modelId, request.modelId)
                && Objects.equals(routingState, request.routingState);
        }

        @Override
        public int hashCode() {
            return Objects.hash(nodeId, modelId, routingState);
        }

        @Override
        public String toString() {
            return "Request{" + "nodeId='" + nodeId + '\'' + ", modelId='" + modelId + '\'' + ", routingState=" + routingState + '}';
        }
    }

}
