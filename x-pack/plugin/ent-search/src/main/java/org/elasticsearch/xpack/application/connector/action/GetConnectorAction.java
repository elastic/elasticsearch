/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.connector.action;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.application.connector.ConnectorSearchResult;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.action.ValidateActions.addValidationError;
import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

public class GetConnectorAction {

    public static final String NAME = "cluster:admin/xpack/connector/get";
    public static final ActionType<GetConnectorAction.Response> INSTANCE = new ActionType<>(NAME);

    private GetConnectorAction() {/* no instances */}

    public static class Request extends ConnectorActionRequest implements ToXContentObject {

        private final String connectorId;
        private final Boolean includeDeleted;

        private static final ParseField CONNECTOR_ID_FIELD = new ParseField("connector_id");

        private static final ParseField INCLUDE_DELETED_FIELD = new ParseField("include_deleted");

        public Request(String connectorId, Boolean includeDeleted) {
            this.connectorId = connectorId;
            this.includeDeleted = includeDeleted;
        }

        public String getConnectorId() {
            return connectorId;
        }

        public Boolean getIncludeDeleted() {
            return includeDeleted;
        }

        @Override
        public ActionRequestValidationException validate() {
            ActionRequestValidationException validationException = null;

            if (Strings.isNullOrEmpty(connectorId)) {
                validationException = addValidationError("connector_id missing", validationException);
            }

            return validationException;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            TransportAction.localOnly();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Request request = (Request) o;
            return Objects.equals(connectorId, request.connectorId) && Objects.equals(includeDeleted, request.includeDeleted);
        }

        @Override
        public int hashCode() {
            return Objects.hash(connectorId, includeDeleted);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            {
                builder.field(CONNECTOR_ID_FIELD.getPreferredName(), connectorId);
                builder.field(INCLUDE_DELETED_FIELD.getPreferredName(), includeDeleted);
            }
            builder.endObject();
            return builder;
        }

        private static final ConstructingObjectParser<Request, Void> PARSER = new ConstructingObjectParser<>(
            "get_connector_request",
            false,
            (p) -> new Request((String) p[0], (Boolean) p[1])

        );
        static {
            PARSER.declareString(constructorArg(), CONNECTOR_ID_FIELD);
            PARSER.declareBoolean(optionalConstructorArg(), INCLUDE_DELETED_FIELD);
        }

        public static Request parse(XContentParser parser) {
            return PARSER.apply(parser, null);
        }
    }

    public static class Response extends ActionResponse implements ToXContentObject {

        private final ConnectorSearchResult connector;

        public Response(ConnectorSearchResult connector) {
            this.connector = connector;
        }

        public Response(StreamInput in) throws IOException {
            this.connector = new ConnectorSearchResult(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            connector.writeTo(out);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            return connector.toXContent(builder, params);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Response response = (Response) o;
            return Objects.equals(connector, response.connector);
        }

        @Override
        public int hashCode() {
            return Objects.hash(connector);
        }
    }
}
