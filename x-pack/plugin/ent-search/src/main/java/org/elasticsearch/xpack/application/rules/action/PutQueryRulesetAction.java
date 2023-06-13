/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.rules.action;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.StatusToXContentObject;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.application.rules.QueryRuleset;

import java.io.IOException;
import java.util.Objects;

public class PutQueryRulesetAction extends ActionType<PutQueryRulesetAction.Response> {

    public static final PutQueryRulesetAction INSTANCE = new PutQueryRulesetAction();
    public static final String NAME = "cluster:admin/xpack/query_rules/put";

    public PutQueryRulesetAction() {
        super(NAME, PutQueryRulesetAction.Response::new);
    }

    public static class Request extends ActionRequest {

        private final QueryRuleset queryRuleset;
        private final boolean create;

        public Request(StreamInput in) throws IOException {
            super(in);
            this.queryRuleset = new QueryRuleset(in);
            this.create = in.readBoolean();
        }

        public Request(QueryRuleset queryRuleset, boolean create) {
            this.queryRuleset = queryRuleset;
            this.create = create;
        }

        public Request(String rulesetId, boolean create, BytesReference content, XContentType contentType) {
            this.queryRuleset = QueryRuleset.fromXContentBytes(rulesetId, content, contentType);
            this.create = create;
        }

        @Override
        public ActionRequestValidationException validate() {
            ActionRequestValidationException validationException = null;

            // TODO: validate query ruleset

            return validationException;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            queryRuleset.writeTo(out);
            out.writeBoolean(create);
        }

        public QueryRuleset queryRuleset() {
            return queryRuleset;
        }

        public boolean create() {
            return create;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Request request = (Request) o;
            return create == request.create && Objects.equals(queryRuleset, request.queryRuleset);
        }

        @Override
        public int hashCode() {
            return Objects.hash(queryRuleset, create);
        }
    }

    public static class Response extends ActionResponse implements StatusToXContentObject {

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

        @Override
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
