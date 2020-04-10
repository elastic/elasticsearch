/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.transform.action;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.core.common.validation.SourceDestValidator;
import org.elasticsearch.xpack.core.transform.TransformField;
import org.elasticsearch.xpack.core.transform.transforms.TransformConfig;
import org.elasticsearch.xpack.core.transform.transforms.TransformConfigUpdate;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.action.ValidateActions.addValidationError;

public class UpdateTransformAction extends ActionType<UpdateTransformAction.Response> {

    public static final UpdateTransformAction INSTANCE = new UpdateTransformAction();
    public static final String NAME = "cluster:admin/transform/update";

    private static final TimeValue MIN_FREQUENCY = TimeValue.timeValueSeconds(1);
    private static final TimeValue MAX_FREQUENCY = TimeValue.timeValueHours(1);

    private UpdateTransformAction() {
        super(NAME, Response::new);
    }

    public static class Request extends AcknowledgedRequest<Request> {

        private final TransformConfigUpdate update;
        private final String id;
        private final boolean deferValidation;

        public Request(TransformConfigUpdate update, String id, boolean deferValidation) {
            this.update = update;
            this.id = id;
            this.deferValidation = deferValidation;
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            this.update = new TransformConfigUpdate(in);
            this.id = in.readString();
            this.deferValidation = in.readBoolean();
        }

        public static Request fromXContent(final XContentParser parser, final String id, final boolean deferValidation) {
            return new Request(TransformConfigUpdate.fromXContent(parser), id, deferValidation);
        }

        /**
         * More complex validations with how {@link TransformConfig#getDestination()} and
         * {@link TransformConfig#getSource()} relate are done in the update transport handler.
         */
        @Override
        public ActionRequestValidationException validate() {
            ActionRequestValidationException validationException = null;

            if (update.getDestination() != null && update.getDestination().getIndex() != null) {

                validationException = SourceDestValidator.validateRequest(validationException, update.getDestination().getIndex());
            }

            TimeValue frequency = update.getFrequency();
            if (frequency != null) {
                if (frequency.compareTo(MIN_FREQUENCY) < 0) {
                    validationException = addValidationError(
                        "minimum permitted [" + TransformField.FREQUENCY + "] is [" + MIN_FREQUENCY.getStringRep() + "]",
                        validationException
                    );
                } else if (frequency.compareTo(MAX_FREQUENCY) > 0) {
                    validationException = addValidationError(
                        "highest permitted [" + TransformField.FREQUENCY + "] is [" + MAX_FREQUENCY.getStringRep() + "]",
                        validationException
                    );
                }
            }

            return validationException;
        }

        public String getId() {
            return id;
        }

        public boolean isDeferValidation() {
            return deferValidation;
        }

        public TransformConfigUpdate getUpdate() {
            return update;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            this.update.writeTo(out);
            out.writeString(id);
            out.writeBoolean(deferValidation);
        }

        @Override
        public int hashCode() {
            return Objects.hash(update, id, deferValidation);
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            Request other = (Request) obj;
            return Objects.equals(update, other.update) && this.deferValidation == other.deferValidation && this.id.equals(other.id);
        }
    }

    public static class Response extends ActionResponse implements ToXContentObject {

        private final TransformConfig config;

        public Response(TransformConfig config) {
            this.config = config;
        }

        public Response(StreamInput in) throws IOException {
            this.config = new TransformConfig(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            this.config.writeTo(out);
        }

        @Override
        public int hashCode() {
            return config.hashCode();
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            Response other = (Response) obj;
            return Objects.equals(config, other.config);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            return config.toXContent(builder, params);
        }
    }
}
