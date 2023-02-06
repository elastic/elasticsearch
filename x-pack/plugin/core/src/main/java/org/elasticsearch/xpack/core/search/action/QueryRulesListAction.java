/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.search.action;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;

public class QueryRulesListAction extends ActionType<QueryRulesListAction.Response> {

    public static final QueryRulesListAction INSTANCE = new QueryRulesListAction();
    public static final String NAME = "cluster:admin/query_rules/list";

    private QueryRulesListAction() {
        super(NAME, QueryRulesListAction.Response::new);
    }

    public static class Request extends ActionRequest {

        public Request(StreamInput in) throws IOException {
            super(in);
        }

        public Request() {}

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }
    }

    public static class Response extends ActionResponse implements ToXContentObject {

        private final String[] ids;

        public Response(String[] ids) {
            this.ids = ids;
        }

        Response(StreamInput in) throws IOException {
            super(in);
            this.ids = in.readStringArray();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeStringArray(this.ids);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.array("ruleset_ids", ids);
            builder.endObject();
            return builder;
        }
    }
}
