/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelConfig;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;

import java.io.IOException;
import java.util.Objects;

public class DeleteTrainedModelAction extends ActionType<AcknowledgedResponse> {

    public static final DeleteTrainedModelAction INSTANCE = new DeleteTrainedModelAction();
    public static final String NAME = "cluster:admin/xpack/ml/inference/delete";

    private DeleteTrainedModelAction() {
        super(NAME, AcknowledgedResponse::readFrom);
    }

    public static class Request extends AcknowledgedRequest<Request> implements ToXContentFragment {

        public static final ParseField FORCE = new ParseField("force");

        private String id;
        private boolean force;

        public Request(StreamInput in) throws IOException {
            super(in);
            id = in.readString();
            if (in.getTransportVersion().onOrAfter(TransportVersion.V_8_1_0)) {
                force = in.readBoolean();
            } else {
                force = false;
            }
        }

        public Request(String id) {
            this.id = ExceptionsHelper.requireNonNull(id, TrainedModelConfig.MODEL_ID);
        }

        public String getId() {
            return id;
        }

        public boolean isForce() {
            return force;
        }

        public void setForce(boolean force) {
            this.force = force;
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.field(TrainedModelConfig.MODEL_ID.getPreferredName(), id);
            builder.field(FORCE.getPreferredName(), force);
            return builder;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            DeleteTrainedModelAction.Request request = (DeleteTrainedModelAction.Request) o;
            return Objects.equals(id, request.id) && force == request.force;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(id);
            if (out.getTransportVersion().onOrAfter(TransportVersion.V_8_1_0)) {
                out.writeBoolean(force);
            }
        }

        @Override
        public int hashCode() {
            return Objects.hash(id, force);
        }
    }

}
