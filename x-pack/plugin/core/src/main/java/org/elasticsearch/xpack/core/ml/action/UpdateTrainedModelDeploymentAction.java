/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.TransportVersions;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.inference.assignment.AutoscalingSettings;
import org.elasticsearch.xpack.core.ml.job.messages.Messages;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.xpack.core.ml.action.StartTrainedModelDeploymentAction.Request.AUTOSCALING_SETTINGS;
import static org.elasticsearch.xpack.core.ml.action.StartTrainedModelDeploymentAction.Request.MODEL_ID;
import static org.elasticsearch.xpack.core.ml.action.StartTrainedModelDeploymentAction.Request.NUMBER_OF_ALLOCATIONS;

public class UpdateTrainedModelDeploymentAction extends ActionType<CreateTrainedModelAssignmentAction.Response> {

    public static final UpdateTrainedModelDeploymentAction INSTANCE = new UpdateTrainedModelDeploymentAction();
    public static final String NAME = "cluster:admin/xpack/ml/trained_models/deployment/update";

    public UpdateTrainedModelDeploymentAction() {
        super(NAME);
    }

    public static class Request extends AcknowledgedRequest<Request> implements ToXContentObject {

        public static final ObjectParser<Request, Void> PARSER = new ObjectParser<>(NAME, Request::new);

        public static final ParseField TIMEOUT = new ParseField("timeout");

        static {
            PARSER.declareString(Request::setDeploymentId, MODEL_ID);
            PARSER.declareInt(Request::setNumberOfAllocations, NUMBER_OF_ALLOCATIONS);
            PARSER.declareObjectOrNull(
                Request::setAutoscalingSettings,
                (p, c) -> AutoscalingSettings.PARSER.parse(p, c).build(),
                AutoscalingSettings.RESET_PLACEHOLDER,
                AUTOSCALING_SETTINGS
            );
            PARSER.declareString((r, val) -> r.ackTimeout(TimeValue.parseTimeValue(val, TIMEOUT.getPreferredName())), TIMEOUT);
        }

        public static Request parseRequest(String deploymentId, XContentParser parser) {
            Request request = PARSER.apply(parser, null);
            if (request.getDeploymentId() == null) {
                request.setDeploymentId(deploymentId);
            } else if (Strings.isNullOrEmpty(deploymentId) == false && deploymentId.equals(request.getDeploymentId()) == false) {
                throw ExceptionsHelper.badRequestException(
                    Messages.getMessage(Messages.INCONSISTENT_ID, MODEL_ID, request.getDeploymentId(), deploymentId)
                );
            }
            return request;
        }

        private String deploymentId;
        private Integer numberOfAllocations;
        private AutoscalingSettings autoscalingSettings;

        private Request() {
            super(TRAPPY_IMPLICIT_DEFAULT_MASTER_NODE_TIMEOUT, DEFAULT_ACK_TIMEOUT);
        }

        public Request(String deploymentId) {
            super(TRAPPY_IMPLICIT_DEFAULT_MASTER_NODE_TIMEOUT, DEFAULT_ACK_TIMEOUT);
            setDeploymentId(deploymentId);
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            deploymentId = in.readString();
            if (in.getTransportVersion().before(TransportVersions.INFERENCE_AUTOSCALING)) {
                numberOfAllocations = in.readVInt();
            } else {
                numberOfAllocations = in.readOptionalVInt();
            }
            if (in.getTransportVersion().onOrAfter(TransportVersions.INFERENCE_AUTOSCALING)) {
                autoscalingSettings = in.readOptionalWriteable(AutoscalingSettings::new);
            }
        }

        public final void setDeploymentId(String deploymentId) {
            this.deploymentId = ExceptionsHelper.requireNonNull(deploymentId, MODEL_ID);
        }

        public String getDeploymentId() {
            return deploymentId;
        }

        public void setNumberOfAllocations(Integer numberOfAllocations) {
            this.numberOfAllocations = numberOfAllocations;
        }

        public Integer getNumberOfAllocations() {
            return numberOfAllocations;
        }

        public void setAutoscalingSettings(AutoscalingSettings autoscalingSettings) {
            this.autoscalingSettings = autoscalingSettings;
        }

        public AutoscalingSettings getAutoscalingSettings() {
            return autoscalingSettings;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(deploymentId);
            if (out.getTransportVersion().before(TransportVersions.INFERENCE_AUTOSCALING)) {
                out.writeVInt(numberOfAllocations);
            } else {
                out.writeOptionalVInt(numberOfAllocations);
            }
            if (out.getTransportVersion().onOrAfter(TransportVersions.INFERENCE_AUTOSCALING)) {
                out.writeOptionalWriteable(autoscalingSettings);
            }
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(MODEL_ID.getPreferredName(), deploymentId);
            if (numberOfAllocations != null) {
                builder.field(NUMBER_OF_ALLOCATIONS.getPreferredName(), numberOfAllocations);
            }
            if (autoscalingSettings != null) {
                builder.field(AUTOSCALING_SETTINGS.getPreferredName(), autoscalingSettings);
            }
            builder.endObject();
            return builder;
        }

        @Override
        public ActionRequestValidationException validate() {
            ActionRequestValidationException validationException = new ActionRequestValidationException();
            if (numberOfAllocations != null && numberOfAllocations < 1) {
                validationException.addValidationError("[" + NUMBER_OF_ALLOCATIONS + "] must be a positive integer");
            }
            ActionRequestValidationException autoscaleException = autoscalingSettings == null ? null : autoscalingSettings.validate();
            if (autoscaleException != null) {
                validationException.addValidationErrors(autoscaleException.validationErrors());
            }
            return validationException.validationErrors().isEmpty() ? null : validationException;
        }

        @Override
        public int hashCode() {
            return Objects.hash(deploymentId, numberOfAllocations, autoscalingSettings);
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null || obj.getClass() != getClass()) {
                return false;
            }
            Request other = (Request) obj;
            return Objects.equals(deploymentId, other.deploymentId)
                && Objects.equals(numberOfAllocations, other.numberOfAllocations)
                && Objects.equals(autoscalingSettings, other.autoscalingSettings);
        }

        @Override
        public String toString() {
            return Strings.toString(this);
        }
    }
}
