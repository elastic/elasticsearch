/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelConfig;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;

import java.io.IOException;
import java.util.Objects;

public class DeleteTrainedModelAliasAction extends ActionType<AcknowledgedResponse> {

    public static final DeleteTrainedModelAliasAction INSTANCE = new DeleteTrainedModelAliasAction();
    public static final String NAME = "cluster:admin/xpack/ml/inference/model_aliases/delete";

    private DeleteTrainedModelAliasAction() {
        super(NAME, AcknowledgedResponse::readFrom);
    }

    public static class Request extends AcknowledgedRequest<Request> {

        public static final String MODEL_ALIAS = "model_alias";

        private final String modelAlias;
        private final String modelId;

        public Request(String modelAlias, String modelId) {
            this.modelAlias = ExceptionsHelper.requireNonNull(modelAlias, MODEL_ALIAS);
            this.modelId = ExceptionsHelper.requireNonNull(modelId, TrainedModelConfig.MODEL_ID);
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            this.modelAlias = in.readString();
            this.modelId = in.readString();
        }

        public String getModelAlias() {
            return modelAlias;
        }

        public String getModelId() {
            return modelId;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(modelAlias);
            out.writeString(modelId);
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Request request = (Request) o;
            return Objects.equals(modelAlias, request.modelAlias) && Objects.equals(modelId, request.modelId);
        }

        @Override
        public int hashCode() {
            return Objects.hash(modelAlias, modelId);
        }

    }
}
