/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.relevancesearch.xsearch.action;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;

public class XSearchSearchAction extends ActionType<XSearchSearchAction.Response> {

    public static final XSearchSearchAction INSTANCE = new XSearchSearchAction();

    static final String NAME = "indices:admin/search_engine/get";

    private XSearchSearchAction() {
        super(NAME, Response::new);
    }

    public static class Request extends ActionRequest implements SearchRequest.Reader {

        public Request(StreamInput in) throws IOException {
            super(in);
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
        }

        @Override
        public Object read(StreamInput in) throws IOException {
            return "hello world";
        }
    }

    public static class Response extends ActionResponse implements ToXContentObject {

        public static final ParseField NAME_FIELD = new ParseField("name");

        private final String response;

        public Response(String response) {
            this.response = response;
        }

        public Response(StreamInput in) throws IOException {
            this(in.readString());
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(response);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
            builder.startObject();
            builder.field("hello world");
            builder.endObject();
            return builder;
        }
    }
}
