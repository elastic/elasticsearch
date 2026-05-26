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
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.xpack.core.common.validation.SourceDestValidator;
import org.elasticsearch.xpack.core.security.cloud.CloudCredential;
import org.elasticsearch.xpack.core.transform.transforms.TransformConfig;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.core.transform.transforms.TransformConfig.TRANSFORM_CLOUD_TOKEN;

public class ValidateTransformAction extends ActionType<ValidateTransformAction.Response> {

    public static final ValidateTransformAction INSTANCE = new ValidateTransformAction();
    public static final String NAME = "cluster:admin/transform/validate";

    private ValidateTransformAction() {
        super(NAME);
    }

    public static class Request extends AcknowledgedRequest<Request> implements Releasable {

        private final TransformConfig config;
        private final boolean deferValidation;
        // Caller's UIAM cloud credential carried on the request payload so it survives the
        // system-origin context stash performed by ClientHelper.executeAsyncWithOrigin. The
        // receiver re-injects it into its local thread context before invoking user-data ops.
        @Nullable
        private final CloudCredential cloudCredential;

        public Request(TransformConfig config, boolean deferValidation, TimeValue timeout) {
            this(config, deferValidation, timeout, null);
        }

        public Request(TransformConfig config, boolean deferValidation, TimeValue timeout, @Nullable CloudCredential cloudCredential) {
            super(TRAPPY_IMPLICIT_DEFAULT_MASTER_NODE_TIMEOUT, timeout);
            this.config = config;
            this.deferValidation = deferValidation;
            this.cloudCredential = cloudCredential;
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            this.config = new TransformConfig(in);
            this.deferValidation = in.readBoolean();
            if (in.getTransportVersion().supports(TRANSFORM_CLOUD_TOKEN)) {
                this.cloudCredential = in.readOptionalWriteable(CloudCredential::new);
            } else {
                this.cloudCredential = null;
            }
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

        @Nullable
        public CloudCredential cloudCredential() {
            return cloudCredential;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            this.config.writeTo(out);
            out.writeBoolean(this.deferValidation);
            if (out.getTransportVersion().supports(TRANSFORM_CLOUD_TOKEN)) {
                out.writeOptionalWriteable(this.cloudCredential);
            }
        }

        @Override
        public void close() {
            IOUtils.closeWhileHandlingException(cloudCredential);
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

            // the base class does not implement equals, therefore we need to check timeout ourselves.
            // cloudCredential is intentionally excluded: it's a request-scoped secret carrier, not part
            // of the logical request identity, and its SecureString does not implement value equality.
            return Objects.equals(config, that.config) && deferValidation == that.deferValidation && ackTimeout().equals(that.ackTimeout());
        }

        @Override
        public int hashCode() {
            // the base class does not implement hashCode, therefore we need to hash timeout ourselves
            return Objects.hash(ackTimeout(), config, deferValidation);
        }

        @Override
        public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
            return new CancellableTask(id, type, action, getDescription(), parentTaskId, headers);
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
            out.writeMap(destIndexMappings, StreamOutput::writeString);
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
