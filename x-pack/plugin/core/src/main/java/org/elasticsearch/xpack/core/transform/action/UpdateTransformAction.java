/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.transform.action;

import org.elasticsearch.TransportVersions;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.tasks.BaseTasksRequest;
import org.elasticsearch.action.support.tasks.BaseTasksResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.common.validation.SourceDestValidator;
import org.elasticsearch.xpack.core.transform.TransformField;
import org.elasticsearch.xpack.core.transform.transforms.AuthorizationState;
import org.elasticsearch.xpack.core.transform.transforms.TransformConfig;
import org.elasticsearch.xpack.core.transform.transforms.TransformConfigUpdate;

import java.io.IOException;
import java.util.Collections;
import java.util.Objects;

import static org.elasticsearch.action.ValidateActions.addValidationError;

public class UpdateTransformAction extends ActionType<UpdateTransformAction.Response> {

    public static final UpdateTransformAction INSTANCE = new UpdateTransformAction();
    public static final String NAME = "cluster:admin/transform/update";

    private static final TimeValue MIN_FREQUENCY = TimeValue.timeValueSeconds(1);
    private static final TimeValue MAX_FREQUENCY = TimeValue.timeValueHours(1);

    private UpdateTransformAction() {
        super(NAME);
    }

    public static final class Request extends BaseTasksRequest<Request> {

        private final TransformConfigUpdate update;
        private final String id;
        private final boolean deferValidation;
        private TransformConfig config;
        private AuthorizationState authState;

        public Request(TransformConfigUpdate update, String id, boolean deferValidation, TimeValue timeout) {
            this.update = update;
            this.id = id;
            this.deferValidation = deferValidation;
            this.setTimeout(timeout);
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            this.update = new TransformConfigUpdate(in);
            this.id = in.readString();
            this.deferValidation = in.readBoolean();
            if (in.readBoolean()) {
                this.config = new TransformConfig(in);
            }
            if (in.getTransportVersion().onOrAfter(TransportVersions.V_8_8_0)) {
                if (in.readBoolean()) {
                    this.authState = new AuthorizationState(in);
                }
            }
        }

        public static Request fromXContent(
            final XContentParser parser,
            final String id,
            final boolean deferValidation,
            final TimeValue timeout
        ) {
            return new Request(TransformConfigUpdate.fromXContent(parser), id, deferValidation, timeout);
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

        public TransformConfig getConfig() {
            return config;
        }

        public void setConfig(TransformConfig config) {
            this.config = config;
        }

        public AuthorizationState getAuthState() {
            return authState;
        }

        public void setAuthState(AuthorizationState authState) {
            this.authState = authState;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            update.writeTo(out);
            out.writeString(id);
            out.writeBoolean(deferValidation);
            if (config == null) {
                out.writeBoolean(false);
            } else {
                out.writeBoolean(true);
                config.writeTo(out);
            }
            if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_8_0)) {
                if (authState == null) {
                    out.writeBoolean(false);
                } else {
                    out.writeBoolean(true);
                    authState.writeTo(out);
                }
            }
        }

        @Override
        public int hashCode() {
            // the base class does not implement hashCode, therefore we need to hash timeout ourselves
            return Objects.hash(getTimeout(), update, id, deferValidation, config, authState);
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

            // the base class does not implement equals, therefore we need to check timeout ourselves
            return Objects.equals(update, other.update)
                && this.deferValidation == other.deferValidation
                && this.id.equals(other.id)
                && Objects.equals(config, other.config)
                && Objects.equals(authState, other.authState)
                && getTimeout().equals(other.getTimeout());
        }

        @Override
        public boolean match(Task task) {
            if (task.getDescription().startsWith(TransformField.PERSISTENT_TASK_DESCRIPTION_PREFIX)) {
                String taskId = task.getDescription().substring(TransformField.PERSISTENT_TASK_DESCRIPTION_PREFIX.length());
                return taskId.equals(this.id);
            }
            return false;
        }
    }

    public static class Response extends BaseTasksResponse implements ToXContentObject {

        private final TransformConfig config;

        public Response(TransformConfig config) {
            // ignore failures
            super(Collections.emptyList(), Collections.emptyList());
            this.config = config;
        }

        public Response(StreamInput in) throws IOException {
            super(in);
            this.config = new TransformConfig(in);
        }

        public TransformConfig getConfig() {
            return config;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            config.writeTo(out);
        }

        @Override
        public int hashCode() {
            return Objects.hash(super.hashCode(), config);
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
            return Objects.equals(config, other.config) && super.equals(obj);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            super.toXContentCommon(builder, params);
            return config.toXContent(builder, params);
        }

    }
}
