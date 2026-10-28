/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.connector.syncjob.action;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.application.connector.Connector;
import org.elasticsearch.xpack.application.connector.syncjob.ConnectorSyncJob;
import org.elasticsearch.xpack.application.connector.syncjob.ConnectorSyncJobTriggerMethod;
import org.elasticsearch.xpack.application.connector.syncjob.ConnectorSyncJobType;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.action.ValidateActions.addValidationError;
import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

public class PostConnectorSyncJobAction {

    public static final String NAME = "cluster:admin/xpack/connector/sync_job/post";
    public static final ActionType<PostConnectorSyncJobAction.Response> INSTANCE = new ActionType<>(NAME);

    private PostConnectorSyncJobAction() {/* no instances */}

    public static class Request extends ConnectorSyncJobActionRequest implements ToXContentObject {
        public static final String EMPTY_CONNECTOR_ID_ERROR_MESSAGE = "[id] of the connector cannot be null or empty";
        private final String id;
        private final ConnectorSyncJobType jobType;
        private final ConnectorSyncJobTriggerMethod triggerMethod;

        public Request(String id, ConnectorSyncJobType jobType, ConnectorSyncJobTriggerMethod triggerMethod) {
            this.id = id;
            this.jobType = jobType;
            this.triggerMethod = triggerMethod;
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            this.id = in.readString();
            this.jobType = in.readOptionalEnum(ConnectorSyncJobType.class);
            this.triggerMethod = in.readOptionalEnum(ConnectorSyncJobTriggerMethod.class);
        }

        private static final ConstructingObjectParser<Request, Void> PARSER = new ConstructingObjectParser<>(
            "connector_sync_job_post_request",
            false,
            ((args) -> {
                String connectorId = (String) args[0];
                String syncJobTypeString = (String) args[1];
                String triggerMethodString = (String) args[2];

                boolean syncJobTypeSpecified = syncJobTypeString != null;
                boolean triggerMethodSpecified = triggerMethodString != null;

                return new Request(
                    connectorId,
                    syncJobTypeSpecified ? ConnectorSyncJobType.fromString(syncJobTypeString) : null,
                    triggerMethodSpecified ? ConnectorSyncJobTriggerMethod.fromString(triggerMethodString) : null
                );
            })
        );

        static {
            PARSER.declareString(constructorArg(), ConnectorSyncJob.ID_FIELD);
            PARSER.declareString(optionalConstructorArg(), ConnectorSyncJob.JOB_TYPE_FIELD);
            PARSER.declareString(optionalConstructorArg(), ConnectorSyncJob.TRIGGER_METHOD_FIELD);
        }

        public String getId() {
            return id;
        }

        public ConnectorSyncJobType getJobType() {
            return jobType;
        }

        public ConnectorSyncJobTriggerMethod getTriggerMethod() {
            return triggerMethod;
        }

        public static Request fromXContent(XContentParser parser) throws IOException {
            return PARSER.parse(parser, null);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            {
                builder.field(Connector.ID_FIELD.getPreferredName(), id);
                builder.field(ConnectorSyncJob.JOB_TYPE_FIELD.getPreferredName(), jobType);
                builder.field(ConnectorSyncJob.TRIGGER_METHOD_FIELD.getPreferredName(), triggerMethod);
            }
            builder.endObject();
            return builder;
        }

        @Override
        public ActionRequestValidationException validate() {
            ActionRequestValidationException validationException = null;

            if (Strings.isNullOrEmpty(getId())) {
                validationException = addValidationError(EMPTY_CONNECTOR_ID_ERROR_MESSAGE, validationException);
            }

            return validationException;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(id);
            out.writeOptionalEnum(jobType);
            out.writeOptionalEnum(triggerMethod);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Request request = (Request) o;
            return Objects.equals(id, request.id) && jobType == request.jobType && triggerMethod == request.triggerMethod;
        }

        @Override
        public int hashCode() {
            return Objects.hash(id, jobType, triggerMethod);
        }
    }

    public static class Response extends ActionResponse implements ToXContentObject {

        private final String id;

        public Response(StreamInput in) throws IOException {
            this.id = in.readString();
        }

        public Response(String id) {
            this.id = id;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(id);
        }

        public String getId() {
            return id;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(ConnectorSyncJob.ID_FIELD.getPreferredName(), id);
            builder.endObject();
            return builder;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Response response = (Response) o;
            return Objects.equals(id, response.id);
        }

        @Override
        public int hashCode() {
            return Objects.hash(id);
        }
    }
}
