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
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.application.connector.Connector;
import org.elasticsearch.xpack.application.connector.ConnectorFiltering;
import org.elasticsearch.xpack.application.connector.filtering.FilteringAdvancedSnippet;
import org.elasticsearch.xpack.application.connector.filtering.FilteringRule;
import org.elasticsearch.xpack.application.connector.filtering.FilteringRules;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.action.ValidateActions.addValidationError;
import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

public class UpdateConnectorFilteringAction {

    public static final String NAME = "cluster:admin/xpack/connector/update_filtering";
    public static final ActionType<ConnectorUpdateActionResponse> INSTANCE = new ActionType<>(NAME);

    private UpdateConnectorFilteringAction() {/* no instances */}

    public static class Request extends ConnectorActionRequest implements ToXContentObject {

        private final String connectorId;
        @Nullable
        private final List<ConnectorFiltering> filtering;
        @Nullable
        private final FilteringAdvancedSnippet advancedSnippet;
        @Nullable
        private final List<FilteringRule> rules;

        public Request(
            String connectorId,
            List<ConnectorFiltering> filtering,
            FilteringAdvancedSnippet advancedSnippet,
            List<FilteringRule> rules
        ) {
            this.connectorId = connectorId;
            this.filtering = filtering;
            this.advancedSnippet = advancedSnippet;
            this.rules = rules;
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            this.connectorId = in.readString();
            this.filtering = in.readOptionalCollectionAsList(ConnectorFiltering::new);
            this.advancedSnippet = new FilteringAdvancedSnippet(in);
            this.rules = in.readCollectionAsList(FilteringRule::new);
        }

        public String getConnectorId() {
            return connectorId;
        }

        public List<ConnectorFiltering> getFiltering() {
            return filtering;
        }

        public FilteringAdvancedSnippet getAdvancedSnippet() {
            return advancedSnippet;
        }

        public List<FilteringRule> getRules() {
            return rules;
        }

        @Override
        public ActionRequestValidationException validate() {
            ActionRequestValidationException validationException = null;

            if (Strings.isNullOrEmpty(connectorId)) {
                validationException = addValidationError("[connector_id] cannot be [null] or [\"\"].", validationException);
            }

            // If [filtering] is not present in the request payload it means that the user should define [rules] and/or [advanced_snippet]
            if (filtering == null) {
                if (rules == null && advancedSnippet == null) {
                    validationException = addValidationError("[advanced_snippet] and [rules] cannot be both [null].", validationException);
                }
            }
            // If [filtering] is present we don't expect [rules] and [advances_snippet] in the request body
            else {
                if (rules != null || advancedSnippet != null) {
                    validationException = addValidationError(
                        "If [filtering] is specified, [rules] and [advanced_snippet] should not be present in the request body.",
                        validationException
                    );
                }
            }

            return validationException;
        }

        @SuppressWarnings("unchecked")
        private static final ConstructingObjectParser<UpdateConnectorFilteringAction.Request, String> PARSER =
            new ConstructingObjectParser<>(
                "connector_update_filtering_request",
                false,
                ((args, connectorId) -> new UpdateConnectorFilteringAction.Request(
                    connectorId,
                    (List<ConnectorFiltering>) args[0],
                    (FilteringAdvancedSnippet) args[1],
                    (List<FilteringRule>) args[2]
                ))
            );

        static {
            PARSER.declareObjectArray(optionalConstructorArg(), (p, c) -> ConnectorFiltering.fromXContent(p), Connector.FILTERING_FIELD);
            PARSER.declareObject(
                optionalConstructorArg(),
                (p, c) -> FilteringAdvancedSnippet.fromXContent(p),
                FilteringRules.ADVANCED_SNIPPET_FIELD
            );
            PARSER.declareObjectArray(optionalConstructorArg(), (p, c) -> FilteringRule.fromXContent(p), FilteringRules.RULES_FIELD);
        }

        public static UpdateConnectorFilteringAction.Request fromXContent(XContentParser parser, String connectorId) throws IOException {
            return PARSER.parse(parser, connectorId);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            {
                builder.field(Connector.FILTERING_FIELD.getPreferredName(), filtering);
                builder.field(FilteringRules.ADVANCED_SNIPPET_FIELD.getPreferredName(), advancedSnippet);
                builder.xContentList(FilteringRules.RULES_FIELD.getPreferredName(), rules);
            }
            builder.endObject();
            return builder;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(connectorId);
            out.writeOptionalCollection(filtering);
            advancedSnippet.writeTo(out);
            out.writeCollection(rules);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Request request = (Request) o;
            return Objects.equals(connectorId, request.connectorId)
                && Objects.equals(filtering, request.filtering)
                && Objects.equals(advancedSnippet, request.advancedSnippet)
                && Objects.equals(rules, request.rules);
        }

        @Override
        public int hashCode() {
            return Objects.hash(connectorId, filtering, advancedSnippet, rules);
        }
    }
}
