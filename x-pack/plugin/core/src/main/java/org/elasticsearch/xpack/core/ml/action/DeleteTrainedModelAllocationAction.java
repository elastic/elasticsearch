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
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;

import java.io.IOException;
import java.util.Objects;

public class DeleteTrainedModelAllocationAction extends ActionType<AcknowledgedResponse> {
    public static final DeleteTrainedModelAllocationAction INSTANCE = new DeleteTrainedModelAllocationAction();
    public static final String NAME = "cluster:internal/xpack/ml/model_allocation/delete";

    private DeleteTrainedModelAllocationAction() {
        super(NAME, AcknowledgedResponse::readFrom);
    }

    public static class Request extends MasterNodeRequest<Request> {
        private final String modelId;

        public Request(String modelId) {
            this.modelId = ExceptionsHelper.requireNonNull(modelId, "model_id");
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            this.modelId = in.readString();
        }

        public String getModelId() {
            return modelId;
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(modelId);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Request request = (Request) o;
            return Objects.equals(modelId, request.modelId);
        }

        @Override
        public int hashCode() {
            return Objects.hash(modelId);
        }
    }

}
