/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.connector.action;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.application.connector.Connector;
import org.elasticsearch.xpack.application.connector.ConnectorScheduling;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;

public class UpdateConnectorSchedulingAction extends ActionType<UpdateConnectorSchedulingAction.Response> {

    public static final UpdateConnectorSchedulingAction INSTANCE = new UpdateConnectorSchedulingAction();
    public static final String NAME = "cluster:admin/xpack/connector/update_scheduling";

    public UpdateConnectorSchedulingAction() {
        super(NAME, UpdateConnectorSchedulingAction.Response::new);
    }

    public static class Request extends ActionRequest implements ToXContentObject {

        private final String connectorId;
        private final ConnectorScheduling scheduling;

        public Request(String connectorId, ConnectorScheduling scheduling) {
            this.connectorId = connectorId;
            this.scheduling = scheduling;
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            this.connectorId = in.readString();
            this.scheduling = in.readOptionalWriteable(ConnectorScheduling::new);
        }

        public String getConnectorId() {
            return connectorId;
        }

        public ConnectorScheduling getScheduling() {
            return scheduling;
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        private static final ConstructingObjectParser<UpdateConnectorSchedulingAction.Request, String> PARSER =
            new ConstructingObjectParser<>(
                "connector_update_scheduling_request",
                false,
                ((args, connectorId) -> new UpdateConnectorSchedulingAction.Request(connectorId, (ConnectorScheduling) args[0]))
            );

        static {
            PARSER.declareObject(constructorArg(), (p, c) -> ConnectorScheduling.fromXContent(p), Connector.SCHEDULING_FIELD);
        }

        public static UpdateConnectorSchedulingAction.Request fromXContentBytes(
            String connectorId,
            BytesReference source,
            XContentType xContentType
        ) {
            try (XContentParser parser = XContentHelper.createParser(XContentParserConfiguration.EMPTY, source, xContentType)) {
                return UpdateConnectorSchedulingAction.Request.fromXContent(parser, connectorId);
            } catch (IOException e) {
                throw new ElasticsearchParseException("Failed to parse: " + source.utf8ToString(), e);
            }
        }

        public static UpdateConnectorSchedulingAction.Request fromXContent(XContentParser parser, String connectorId) throws IOException {
            return PARSER.parse(parser, connectorId);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            {
                builder.field(Connector.SCHEDULING_FIELD.getPreferredName(), scheduling);
            }
            builder.endObject();
            return builder;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(connectorId);
            out.writeOptionalWriteable(scheduling);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Request request = (Request) o;
            return Objects.equals(connectorId, request.connectorId) && Objects.equals(scheduling, request.scheduling);
        }

        @Override
        public int hashCode() {
            return Objects.hash(connectorId, scheduling);
        }
    }

    public static class Response extends ActionResponse implements ToXContentObject {

        final DocWriteResponse.Result result;

        public Response(StreamInput in) throws IOException {
            super(in);
            result = DocWriteResponse.Result.readFrom(in);
        }

        public Response(DocWriteResponse.Result result) {
            this.result = result;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            this.result.writeTo(out);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("result", this.result.getLowercase());
            builder.endObject();
            return builder;
        }

        public RestStatus status() {
            return switch (result) {
                case NOT_FOUND -> RestStatus.NOT_FOUND;
                default -> RestStatus.OK;
            };
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Response that = (Response) o;
            return Objects.equals(result, that.result);
        }

        @Override
        public int hashCode() {
            return Objects.hash(result);
        }
    }
}
