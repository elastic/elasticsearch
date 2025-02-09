/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.view;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

public class GetViewAction extends ActionType<GetViewAction.Response> {

    public static final GetViewAction INSTANCE = new GetViewAction();
    public static final String NAME = "cluster:admin/xpack/esql/view/get";

    private GetViewAction() {
        super(NAME);
    }

    public static class Request extends ActionRequest {
        private String name;

        public Request(String name) {
            this.name = Objects.requireNonNull(name, "name cannot be null");
        }

        public Request(StreamInput in) throws IOException {
            super(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(name);
        }

        public String name() {
            return name;
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Request request = (Request) o;
            return name.equals(request.name);
        }

        @Override
        public int hashCode() {
            return name.hashCode();
        }
    }

    public static class Response extends ActionResponse implements ToXContentObject {

        private final View view;

        public Response(final View view) {
            this.view = view;
        }

        public View getView() {
            return view;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            TransportAction.localOnly();
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            view.toXContent(builder, params);
            builder.endObject();
            return builder;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            return view.equals(((Response) o).view);
        }

        @Override
        public int hashCode() {
            return view.hashCode();
        }

        @Override
        public String toString() {
            return "GetViewAction.Response{view=" + view.toString() + '}';
        }
    }
}
