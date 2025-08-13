/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.connector.action;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.application.connector.Connector;
import org.elasticsearch.xpack.application.connector.ConnectorIngestPipeline;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.action.ValidateActions.addValidationError;
import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;

public class UpdateConnectorPipelineAction {

    public static final String NAME = "cluster:admin/xpack/connector/update_pipeline";
    public static final ActionType<ConnectorUpdateActionResponse> INSTANCE = new ActionType<>(NAME);

    private UpdateConnectorPipelineAction() {/* no instances */}

    public static class Request extends ConnectorActionRequest implements ToXContentObject {

        private final String connectorId;
        private final ConnectorIngestPipeline pipeline;

        public Request(String connectorId, ConnectorIngestPipeline pipeline) {
            this.connectorId = connectorId;
            this.pipeline = pipeline;
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            this.connectorId = in.readString();
            this.pipeline = in.readOptionalWriteable(ConnectorIngestPipeline::new);
        }

        public String getConnectorId() {
            return connectorId;
        }

        public ConnectorIngestPipeline getPipeline() {
            return pipeline;
        }

        @Override
        public ActionRequestValidationException validate() {
            ActionRequestValidationException validationException = null;

            if (Strings.isNullOrEmpty(connectorId)) {
                validationException = addValidationError("[connector_id] cannot be [null] or [\"\"].", validationException);
            }

            if (Objects.isNull(pipeline)) {
                validationException = addValidationError("[pipeline] cannot be [null].", validationException);
            }

            return validationException;
        }

        private static final ConstructingObjectParser<UpdateConnectorPipelineAction.Request, String> PARSER =
            new ConstructingObjectParser<>(
                "connector_update_pipeline_request",
                false,
                ((args, connectorId) -> new UpdateConnectorPipelineAction.Request(connectorId, (ConnectorIngestPipeline) args[0]))
            );

        static {
            PARSER.declareObject(constructorArg(), (p, c) -> ConnectorIngestPipeline.fromXContent(p), Connector.PIPELINE_FIELD);
        }

        public static UpdateConnectorPipelineAction.Request fromXContent(XContentParser parser, String connectorId) throws IOException {
            return PARSER.parse(parser, connectorId);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            {
                builder.field(Connector.PIPELINE_FIELD.getPreferredName(), pipeline);
            }
            builder.endObject();
            return builder;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(connectorId);
            out.writeOptionalWriteable(pipeline);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Request request = (Request) o;
            return Objects.equals(connectorId, request.connectorId) && Objects.equals(pipeline, request.pipeline);
        }

        @Override
        public int hashCode() {
            return Objects.hash(connectorId, pipeline);
        }
    }
}
