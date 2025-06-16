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
import org.elasticsearch.xpack.application.rules.QueryRuleset;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.action.ValidateActions.addValidationError;
import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;

public class PutQueryRulesetAction {

    public static final String NAME = "cluster:admin/xpack/query_rules/put";
    public static final ActionType<Response> INSTANCE = new ActionType<>(NAME);

    private PutQueryRulesetAction() {/* no instances */}

    public static class Request extends LegacyActionRequest implements ToXContentObject {

        private final QueryRuleset queryRuleset;
        private static final ParseField QUERY_RULESET_FIELD = new ParseField("queryRuleset");

        public Request(StreamInput in) throws IOException {
            super(in);
            this.queryRuleset = new QueryRuleset(in);
        }

        public Request(QueryRuleset queryRuleset) {
            this.queryRuleset = queryRuleset;
        }

        public Request(String rulesetId, BytesReference content, XContentType contentType) {
            this.queryRuleset = QueryRuleset.fromXContentBytes(rulesetId, content, contentType);
        }

        @Override
        public ActionRequestValidationException validate() {
            ActionRequestValidationException validationException = null;

            if (Strings.isNullOrEmpty(queryRuleset.id())) {
                validationException = addValidationError("ruleset_id cannot be null or empty", validationException);
            }

            List<QueryRule> rules = queryRuleset.rules();
            if (rules == null || rules.isEmpty()) {
                validationException = addValidationError("rules cannot be null or empty", validationException);
            } else {
                for (QueryRule rule : rules) {
                    if (rule.id() == null) {
                        validationException = addValidationError(
                            "rule_id cannot be null or empty. rule: [" + rule + "]",
                            validationException
                        );
                    }
                }
            }

            return validationException;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            queryRuleset.writeTo(out);
        }

        public QueryRuleset queryRuleset() {
            return queryRuleset;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Request request = (Request) o;
            return Objects.equals(queryRuleset, request.queryRuleset);
        }

        @Override
        public int hashCode() {
            return Objects.hash(queryRuleset);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            return queryRuleset.toXContent(builder, params);
        }

        private static final ConstructingObjectParser<Request, String> PARSER = new ConstructingObjectParser<>(
            "put_query_rules_request",
            p -> new Request((QueryRuleset) p[0])
        );

        static {
            PARSER.declareObject(constructorArg(), (p, c) -> QueryRuleset.fromXContent(c, p), QUERY_RULESET_FIELD);
        }

        public static Request fromXContent(String id, XContentParser parser) throws IOException {
            return new Request(QueryRuleset.fromXContent(id, parser));
        }

        @Override
        public String toString() {
            return Strings.toString(this);
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
