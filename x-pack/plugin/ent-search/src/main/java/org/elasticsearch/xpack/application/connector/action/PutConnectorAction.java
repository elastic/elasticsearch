/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.connector.action;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.application.connector.Connector;

import java.io.IOException;

import static org.elasticsearch.action.ValidateActions.addValidationError;

public class PutConnectorAction extends ActionType<PutConnectorAction.Response> {

    public static final PutConnectorAction INSTANCE = new PutConnectorAction();
    public static final String NAME = "cluster:admin/xpack/connector/put";

    public PutConnectorAction() {
        super(NAME, PutConnectorAction.Response::new);
    }

    public static class Request extends ActionRequest implements ToXContentObject {

        private final Connector connector;

        public Request(Connector connector) {
            this.connector = connector;
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            this.connector = new Connector.Builder().setIn(in).createConnector();
        }

        public Request(String connectorId, BytesReference content, XContentType contentType) {
            this.connector = Connector.fromXContentBytes(connectorId, content, contentType);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            return connector.toXContent(builder, params);
        }

        @Override
        public ActionRequestValidationException validate() {

            ActionRequestValidationException validationException = null;

            if (Strings.isNullOrEmpty(connector.id())) {
                validationException = addValidationError("connector_id cannot be null or empty", validationException);
            }

            return validationException;
        }

        public Connector connector() {
            return connector;
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
                case CREATED -> RestStatus.CREATED;
                case NOT_FOUND -> RestStatus.NOT_FOUND;
                default -> RestStatus.OK;
            };
        }
    }
}
