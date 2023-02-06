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
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;

public class QueryRulesListAction extends ActionType<QueryRulesListAction.Response> {

    public static final QueryRulesListAction INSTANCE = new QueryRulesListAction();
    public static final String NAME = "cluster:admin/query_rules/list";

    private QueryRulesListAction() {
        super(NAME, QueryRulesListAction.Response::new);
    }

    public static class Request extends ActionRequest {

        private final int size;
        private final int from;

        public Request(StreamInput in) throws IOException {
            super(in);
            this.size = in.readVInt();
            this.from = in.readVInt();
        }

        public Request(int size, int offset) {
            this.size = size;
            this.from = offset;
        }

        private static final ParseField SIZE_FIELD = new ParseField("size");
        private static final ParseField FROM_FIELD = new ParseField("from");

        private static final ConstructingObjectParser<Request, Void> PARSER = new ConstructingObjectParser<>("query_rules_list", a -> {
            int size = (a[0] == null ? 10 : (Integer) a[0]);
            int offset = (a[1] == null ? 0 : (Integer) a[1]);
            return new Request(size, offset);
        });

        static {
            PARSER.declareInt(ConstructingObjectParser.optionalConstructorArg(), SIZE_FIELD);
            PARSER.declareInt(ConstructingObjectParser.optionalConstructorArg(), FROM_FIELD);
        }

        public static Request parse(XContentParser parser) {
            return PARSER.apply(parser, null);
        }

        public int getSize() {
            return size;
        }

        public int getFrom() {
            return from;
        }

        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeVInt(size);
            out.writeVInt(from);
        }

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
