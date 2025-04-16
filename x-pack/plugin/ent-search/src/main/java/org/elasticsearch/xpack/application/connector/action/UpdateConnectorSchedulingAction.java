/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.connector.action;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentHelper;
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

import static org.elasticsearch.action.ValidateActions.addValidationError;
import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;

public class UpdateConnectorSchedulingAction {

    public static final String NAME = "cluster:admin/xpack/connector/update_scheduling";
    public static final ActionType<ConnectorUpdateActionResponse> INSTANCE = new ActionType<>(NAME);

    private UpdateConnectorSchedulingAction() {/* no instances */}

    public static class Request extends ConnectorActionRequest implements ToXContentObject {

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
            ActionRequestValidationException validationException = null;

            if (Strings.isNullOrEmpty(connectorId)) {
                validationException = addValidationError("[connector_id] cannot be [null] or [\"\"].", validationException);
            }

            if (Objects.isNull(scheduling)) {
                validationException = addValidationError("[scheduling] cannot be [null].", validationException);
            }

            if (Objects.isNull(scheduling.getFull())
                && Objects.isNull(scheduling.getIncremental())
                && Objects.isNull(scheduling.getIncremental())) {
                validationException = addValidationError(
                    "[scheduling] object needs to define at least one schedule type: [full | incremental | access_control]",
                    validationException
                );
            }

            return validationException;
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
}
