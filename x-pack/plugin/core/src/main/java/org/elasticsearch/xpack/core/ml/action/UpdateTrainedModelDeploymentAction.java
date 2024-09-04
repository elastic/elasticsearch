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
import org.elasticsearch.xpack.core.ml.inference.assignment.AdaptiveAllocationsFeatureFlag;
import org.elasticsearch.xpack.core.ml.inference.assignment.AdaptiveAllocationsSettings;
import org.elasticsearch.xpack.core.ml.job.messages.Messages;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.xpack.core.ml.action.StartTrainedModelDeploymentAction.Request.ADAPTIVE_ALLOCATIONS;
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
            if (AdaptiveAllocationsFeatureFlag.isEnabled()) {
                PARSER.declareObjectOrNull(
                    Request::setAdaptiveAllocationsSettings,
                    (p, c) -> AdaptiveAllocationsSettings.PARSER.parse(p, c).build(),
                    AdaptiveAllocationsSettings.RESET_PLACEHOLDER,
                    ADAPTIVE_ALLOCATIONS
                );
            }
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
        private AdaptiveAllocationsSettings adaptiveAllocationsSettings;
        private boolean isInternal;

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
            if (in.getTransportVersion().before(TransportVersions.INFERENCE_ADAPTIVE_ALLOCATIONS)) {
                numberOfAllocations = in.readVInt();
                adaptiveAllocationsSettings = null;
                isInternal = false;
            } else {
                numberOfAllocations = in.readOptionalVInt();
                adaptiveAllocationsSettings = in.readOptionalWriteable(AdaptiveAllocationsSettings::new);
                isInternal = in.readBoolean();
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

        public void setAdaptiveAllocationsSettings(AdaptiveAllocationsSettings adaptiveAllocationsSettings) {
            this.adaptiveAllocationsSettings = adaptiveAllocationsSettings;
        }

        public boolean isInternal() {
            return isInternal;
        }

        public void setIsInternal(boolean isInternal) {
            this.isInternal = isInternal;
        }

        public AdaptiveAllocationsSettings getAdaptiveAllocationsSettings() {
            return adaptiveAllocationsSettings;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(deploymentId);
            if (out.getTransportVersion().before(TransportVersions.INFERENCE_ADAPTIVE_ALLOCATIONS)) {
                out.writeVInt(numberOfAllocations);
            } else {
                out.writeOptionalVInt(numberOfAllocations);
                out.writeOptionalWriteable(adaptiveAllocationsSettings);
                out.writeBoolean(isInternal);
            }
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(MODEL_ID.getPreferredName(), deploymentId);
            if (numberOfAllocations != null) {
                builder.field(NUMBER_OF_ALLOCATIONS.getPreferredName(), numberOfAllocations);
            }
            if (adaptiveAllocationsSettings != null) {
                builder.field(ADAPTIVE_ALLOCATIONS.getPreferredName(), adaptiveAllocationsSettings);
            }
            builder.endObject();
            return builder;
        }

        @Override
        public ActionRequestValidationException validate() {
            ActionRequestValidationException validationException = new ActionRequestValidationException();
            if (numberOfAllocations != null) {
                if (numberOfAllocations < 1) {
                    validationException.addValidationError("[" + NUMBER_OF_ALLOCATIONS + "] must be a positive integer");
                }
                if (isInternal == false
                    && adaptiveAllocationsSettings != null
                    && adaptiveAllocationsSettings.getEnabled() == Boolean.TRUE) {
                    validationException.addValidationError(
                        "[" + NUMBER_OF_ALLOCATIONS + "] cannot be set if adaptive allocations is enabled"
                    );
                }
            }
            ActionRequestValidationException autoscaleException = adaptiveAllocationsSettings == null
                ? null
                : adaptiveAllocationsSettings.validate();
            if (autoscaleException != null) {
                validationException.addValidationErrors(autoscaleException.validationErrors());
            }
            return validationException.validationErrors().isEmpty() ? null : validationException;
        }

        @Override
        public int hashCode() {
            return Objects.hash(deploymentId, numberOfAllocations, adaptiveAllocationsSettings, isInternal);
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
                && Objects.equals(adaptiveAllocationsSettings, other.adaptiveAllocationsSettings)
                && isInternal == other.isInternal;
        }

        @Override
        public String toString() {
            return Strings.toString(this);
        }
    }
}
