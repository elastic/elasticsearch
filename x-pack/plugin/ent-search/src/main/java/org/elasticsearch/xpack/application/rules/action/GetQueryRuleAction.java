/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.rules.action;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.LegacyActionRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.application.rules.QueryRule;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.action.ValidateActions.addValidationError;
import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;

public class GetQueryRuleAction {

    public static final String NAME = "cluster:admin/xpack/query_rules/rule/get";
    public static final ActionType<Response> INSTANCE = new ActionType<>(NAME);

    private GetQueryRuleAction() {/* no instances */}

    public static class Request extends LegacyActionRequest implements ToXContentObject {
        private final String rulesetId;
        private final String ruleId;

        private static final ParseField RULESET_ID_FIELD = new ParseField("ruleset_id");
        private static final ParseField RULE_ID_FIELD = new ParseField("rule_id");

        public Request(StreamInput in) throws IOException {
            super(in);
            this.rulesetId = in.readString();
            this.ruleId = in.readString();
        }

        public Request(String rulesetId, String ruleId) {
            this.rulesetId = rulesetId;
            this.ruleId = ruleId;
        }

        @Override
        public ActionRequestValidationException validate() {
            ActionRequestValidationException validationException = null;

            if (Strings.isNullOrEmpty(rulesetId)) {
                validationException = addValidationError("ruleset_id missing", validationException);
            }

            if (Strings.isNullOrEmpty(ruleId)) {
                validationException = addValidationError("rule_id missing", validationException);
            }

            return validationException;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(rulesetId);
            out.writeString(ruleId);
        }

        public String rulesetId() {
            return rulesetId;
        }

        public String ruleId() {
            return ruleId;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Request request = (Request) o;
            return Objects.equals(rulesetId, request.rulesetId) && Objects.equals(ruleId, request.ruleId);
        }

        @Override
        public int hashCode() {
            return Objects.hash(rulesetId, ruleId);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(RULESET_ID_FIELD.getPreferredName(), rulesetId);
            builder.field(RULE_ID_FIELD.getPreferredName(), ruleId);
            builder.endObject();
            return builder;
        }

        private static final ConstructingObjectParser<Request, String> PARSER = new ConstructingObjectParser<>(
            "get_query_rule_request",
            false,
            (p) -> new Request((String) p[0], (String) p[1])

        );

        static {
            PARSER.declareString(constructorArg(), RULESET_ID_FIELD);
            PARSER.declareString(constructorArg(), RULE_ID_FIELD);
        }

        public static Request parse(XContentParser parser, String name) {
            return PARSER.apply(parser, name);
        }

    }

    public static class Response extends ActionResponse implements ToXContentObject {

        private final QueryRule queryRule;

        public Response(StreamInput in) throws IOException {
            super(in);
            this.queryRule = new QueryRule(in);
        }

        public Response(QueryRule queryRule) {
            this.queryRule = queryRule;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            queryRule.writeTo(out);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            return queryRule.toXContent(builder, params);
        }

        public QueryRule queryRule() {
            return queryRule;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Response response = (Response) o;
            return Objects.equals(queryRule, response.queryRule);
        }

        @Override
        public int hashCode() {
            return Objects.hash(queryRule);
        }

        public static Response fromXContent(XContentParser parser) throws IOException {
            return new Response(QueryRule.fromXContent(parser));
        }
    }
}
