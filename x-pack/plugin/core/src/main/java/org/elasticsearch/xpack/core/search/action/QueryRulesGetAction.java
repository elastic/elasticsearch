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
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.index.mapper.SourceFieldMapper;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;

public class QueryRulesGetAction extends ActionType<QueryRulesGetAction.Response> {

    public static final QueryRulesGetAction INSTANCE = new QueryRulesGetAction();
    public static final String NAME = "cluster:admin/read/query_rules/get";

    private QueryRulesGetAction() {
        super(NAME, QueryRulesGetAction.Response::new);
    }

    public static class Request extends ActionRequest {

        private final String rulesetId;

        public Request(StreamInput in) throws IOException {
            super(in);
            this.rulesetId = in.readString();
        }

        public Request(String rulesetId) {
            this.rulesetId = rulesetId;
        }

        @Override
        public ActionRequestValidationException validate() {
            // TODO check id and body non null, body has required fields etc...
            return null;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(rulesetId);
        }

        public String getRuleSetId() {
            return this.rulesetId;
        }

    }

    public static class Response extends ActionResponse implements ToXContentObject {

        private final BytesReference source;
        private final boolean found;
        private final String rulesetId;

        public Response(String rulesetId, boolean found, BytesReference sourceAsBytes) {
            this.source = sourceAsBytes;
            this.found = found;
            this.rulesetId = rulesetId;
        }

        Response(StreamInput in) throws IOException {
            super(in);
            this.source = in.readBytesReference();
            this.rulesetId = in.readString();
            this.found = in.readBoolean();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeBytesReference(this.source);
            out.writeString(rulesetId);
            out.writeBoolean(found);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("ruleset_id", this.rulesetId);
            builder.field("found", this.found);
            if (found) {
                XContentHelper.writeRawField(SourceFieldMapper.NAME, source, builder, params);
            }
            builder.endObject();
            return builder;
        }
    }
}
