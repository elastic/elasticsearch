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
import org.elasticsearch.xpack.application.rules.QueryRuleset;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.action.ValidateActions.addValidationError;
import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;

public class GetQueryRulesetAction {

    public static final ActionType<Response> TYPE = new ActionType<>("cluster:admin/xpack/query_rules/get");
    public static final String NAME = TYPE.name();
    public static final ActionType<Response> INSTANCE = new ActionType<>(NAME);

    private GetQueryRulesetAction() {/* no instances */}

    public static class Request extends LegacyActionRequest implements ToXContentObject {
        private final String rulesetId;
        private static final ParseField RULESET_ID_FIELD = new ParseField("ruleset_id");

        public Request(StreamInput in) throws IOException {
            super(in);
            this.rulesetId = in.readString();
        }

        public Request(String rulesetId) {
            this.rulesetId = rulesetId;
        }

        @Override
        public ActionRequestValidationException validate() {
            ActionRequestValidationException validationException = null;

            if (Strings.isNullOrEmpty(rulesetId)) {
                validationException = addValidationError("ruleset_id missing", validationException);
            }

            return validationException;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(rulesetId);
        }

        public String rulesetId() {
            return rulesetId;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Request request = (Request) o;
            return Objects.equals(rulesetId, request.rulesetId);
        }

        @Override
        public int hashCode() {
            return Objects.hash(rulesetId);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(RULESET_ID_FIELD.getPreferredName(), rulesetId);
            builder.endObject();
            return builder;
        }

        private static final ConstructingObjectParser<Request, String> PARSER = new ConstructingObjectParser<>(
            "get_query_ruleset_request",
            false,
            (p) -> {
                return new Request((String) p[0]);
            }

        );

        static {
            PARSER.declareString(constructorArg(), RULESET_ID_FIELD);
        }

        public static Request parse(XContentParser parser, String name) {
            return PARSER.apply(parser, name);
        }

    }

    public static class Response extends ActionResponse implements ToXContentObject {

        private final QueryRuleset queryRuleset;

        public Response(StreamInput in) throws IOException {
            super(in);
            this.queryRuleset = new QueryRuleset(in);
        }

        public Response(QueryRuleset queryRuleset) {
            this.queryRuleset = queryRuleset;
        }

        public Response(String rulesetId, List<QueryRule> rules) {
            this.queryRuleset = new QueryRuleset(rulesetId, rules);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            queryRuleset.writeTo(out);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            return queryRuleset.toXContent(builder, params);
        }

        public QueryRuleset queryRuleset() {
            return queryRuleset;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Response response = (Response) o;
            return Objects.equals(queryRuleset, response.queryRuleset);
        }

        @Override
        public int hashCode() {
            return Objects.hash(queryRuleset);
        }

        public static Response fromXContent(String resourceName, XContentParser parser) throws IOException {
            return new Response(QueryRuleset.fromXContent(resourceName, parser));
        }
    }
}
