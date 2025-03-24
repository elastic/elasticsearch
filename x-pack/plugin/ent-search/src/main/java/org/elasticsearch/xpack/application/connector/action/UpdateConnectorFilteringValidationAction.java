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
import org.elasticsearch.xpack.application.connector.filtering.FilteringRules;
import org.elasticsearch.xpack.application.connector.filtering.FilteringValidationInfo;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.action.ValidateActions.addValidationError;
import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;

public class UpdateConnectorFilteringValidationAction {

    public static final String NAME = "cluster:admin/xpack/connector/update_filtering/draft_validation";
    public static final ActionType<ConnectorUpdateActionResponse> INSTANCE = new ActionType<>(NAME);

    private UpdateConnectorFilteringValidationAction() {/* no instances */}

    public static class Request extends ConnectorActionRequest implements ToXContentObject {

        private final String connectorId;
        private final FilteringValidationInfo validation;

        public Request(String connectorId, FilteringValidationInfo validation) {
            this.connectorId = connectorId;
            this.validation = validation;
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            this.connectorId = in.readString();
            this.validation = new FilteringValidationInfo(in);
        }

        public String getConnectorId() {
            return connectorId;
        }

        public FilteringValidationInfo getValidation() {
            return validation;
        }

        @Override
        public ActionRequestValidationException validate() {
            ActionRequestValidationException validationException = null;

            if (Strings.isNullOrEmpty(connectorId)) {
                validationException = addValidationError("[connector_id] cannot be [null] or [\"\"].", validationException);
            }

            if (validation == null) {
                validationException = addValidationError("[validation] cannot be [null].", validationException);
            }

            return validationException;
        }

        @SuppressWarnings("unchecked")
        private static final ConstructingObjectParser<UpdateConnectorFilteringValidationAction.Request, String> PARSER =
            new ConstructingObjectParser<>(
                "connector_update_filtering_validation",
                false,
                ((args, connectorId) -> new UpdateConnectorFilteringValidationAction.Request(
                    connectorId,
                    (FilteringValidationInfo) args[0]
                ))
            );

        static {
            PARSER.declareObject(constructorArg(), (p, c) -> FilteringValidationInfo.fromXContent(p), FilteringRules.VALIDATION_FIELD);
        }

        public static UpdateConnectorFilteringValidationAction.Request fromXContent(XContentParser parser, String connectorId)
            throws IOException {
            return PARSER.parse(parser, connectorId);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            {
                builder.field(FilteringRules.VALIDATION_FIELD.getPreferredName(), validation);
            }
            builder.endObject();
            return builder;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(connectorId);
            validation.writeTo(out);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Request request = (Request) o;
            return Objects.equals(connectorId, request.connectorId) && Objects.equals(validation, request.validation);
        }

        @Override
        public int hashCode() {
            return Objects.hash(connectorId, validation);
        }
    }
}
