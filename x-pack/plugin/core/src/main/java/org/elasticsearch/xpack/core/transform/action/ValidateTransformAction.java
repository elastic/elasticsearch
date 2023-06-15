/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.transform.action;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xpack.core.common.validation.SourceDestValidator;
import org.elasticsearch.xpack.core.transform.transforms.TransformConfig;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

public class ValidateTransformAction extends ActionType<ValidateTransformAction.Response> {

    public static final ValidateTransformAction INSTANCE = new ValidateTransformAction();
    public static final String NAME = "cluster:admin/transform/validate";

    private ValidateTransformAction() {
        super(NAME, ValidateTransformAction.Response::new);
    }

    public static class Request extends AcknowledgedRequest<Request> {

        private final TransformConfig config;
        private final boolean deferValidation;

        public Request(TransformConfig config, boolean deferValidation, TimeValue timeout) {
            super(timeout);
            this.config = config;
            this.deferValidation = deferValidation;
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            this.config = new TransformConfig(in);
            this.deferValidation = in.readBoolean();
        }

        @Override
        public ActionRequestValidationException validate() {
            ActionRequestValidationException validationException = null;

            validationException = config.validate(validationException);
            validationException = SourceDestValidator.validateRequest(
                validationException,
                config.getDestination() != null ? config.getDestination().getIndex() : null
            );

            return validationException;
        }

        public TransformConfig getConfig() {
            return config;
        }

        public boolean isDeferValidation() {
            return deferValidation;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            this.config.writeTo(out);
            out.writeBoolean(this.deferValidation);
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == this) {
                return true;
            }
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            Request that = (Request) obj;

            // the base class does not implement equals, therefore we need to check timeout ourselves
            return Objects.equals(config, that.config) && deferValidation == that.deferValidation && timeout().equals(that.timeout());
        }

        @Override
        public int hashCode() {
            // the base class does not implement hashCode, therefore we need to hash timeout ourselves
            return Objects.hash(timeout(), config, deferValidation);
        }
    }

    public static class Response extends ActionResponse {

        private final Map<String, String> destIndexMappings;

        public Response(Map<String, String> destIndexMappings) {
            this.destIndexMappings = destIndexMappings;
        }

        public Response(StreamInput in) throws IOException {
            this.destIndexMappings = in.readMap(StreamInput::readString);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeMap(destIndexMappings, StreamOutput::writeString, StreamOutput::writeString);
        }

        public Map<String, String> getDestIndexMappings() {
            return destIndexMappings;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == this) {
                return true;
            }
            if (obj == null || obj.getClass() != getClass()) {
                return false;
            }
            Response that = (Response) obj;
            return Objects.equals(this.destIndexMappings, that.destIndexMappings);
        }

        @Override
        public int hashCode() {
            return Objects.hash(destIndexMappings);
        }
    }
}
