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
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.LegacyActionRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.application.rules.QueryRule;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.action.ValidateActions.addValidationError;
import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;

public class PutQueryRuleAction {

    public static final String NAME = "cluster:admin/xpack/query_rules/rule/put";
    public static final ActionType<Response> INSTANCE = new ActionType<>(NAME);

    private PutQueryRuleAction() {/* no instances */}

    public static class Request extends LegacyActionRequest implements ToXContentObject {

        private final String queryRulesetId;
        private final QueryRule queryRule;
        private static final ParseField QUERY_RULESET_ID_FIELD = new ParseField("queryRulesetId");
        private static final ParseField QUERY_RULE_FIELD = new ParseField("queryRule");

        public Request(StreamInput in) throws IOException {
            super(in);
            this.queryRulesetId = in.readString();
            this.queryRule = new QueryRule(in);
        }

        public Request(String queryRulesetId, QueryRule queryRule) {
            this.queryRulesetId = queryRulesetId;
            this.queryRule = queryRule;
        }

        public Request(String rulesetId, String ruleId, BytesReference content, XContentType contentType) {
            this.queryRulesetId = rulesetId;

            QueryRule queryRule = QueryRule.fromXContentBytes(content, contentType);
            if (queryRule.id() == null) {
                this.queryRule = new QueryRule(ruleId, queryRule);
            } else if (ruleId.equals(queryRule.id()) == false) {
                throw new IllegalArgumentException("rule_id does not match the id in the query rule");
            } else {
                this.queryRule = queryRule;
            }
        }

        @Override
        public ActionRequestValidationException validate() {
            ActionRequestValidationException validationException = null;

            if (Strings.isNullOrEmpty(queryRulesetId)) {
                validationException = addValidationError("ruleset_id cannot be null or empty", validationException);
            }

            if (Strings.isNullOrEmpty(queryRule.id())) {
                validationException = addValidationError("rule_id cannot be null or empty", validationException);
            }

            return validationException;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(queryRulesetId);
            queryRule.writeTo(out);
        }

        public String queryRulesetId() {
            return queryRulesetId;
        }

        public QueryRule queryRule() {
            return queryRule;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Request request = (Request) o;
            return Objects.equals(queryRulesetId, request.queryRulesetId) && Objects.equals(queryRule, request.queryRule);
        }

        @Override
        public int hashCode() {
            return Objects.hash(queryRulesetId, queryRule);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(QUERY_RULESET_ID_FIELD.getPreferredName(), queryRulesetId);
            builder.field(QUERY_RULE_FIELD.getPreferredName(), queryRule);
            builder.endObject();
            return builder;
        }

        private static final ConstructingObjectParser<Request, String> PARSER = new ConstructingObjectParser<>(
            "put_query_rule_request",
            p -> new Request((String) p[0], (QueryRule) p[1])
        );

        static {
            PARSER.declareString(constructorArg(), QUERY_RULESET_ID_FIELD);
            PARSER.declareObject(constructorArg(), (p, c) -> QueryRule.fromXContent(p), QUERY_RULE_FIELD);
        }

        public static Request parse(XContentParser parser, String resourceName) {
            return PARSER.apply(parser, resourceName);
        }

        public static Request fromXContent(String queryRulesetId, XContentParser parser) throws IOException {
            return new Request(queryRulesetId, QueryRule.fromXContent(parser));
        }

        @Override
        public String toString() {
            return Strings.toString(this);
        }

    }

    public static class Response extends ActionResponse implements ToXContentObject {

        final DocWriteResponse.Result result;

        public Response(StreamInput in) throws IOException {
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
