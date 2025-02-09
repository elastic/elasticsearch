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
import org.elasticsearch.common.xcontent.ChunkedToXContentHelper;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Map;

import static org.elasticsearch.xpack.esql.view.ViewMetadata.VIEWS;

public class ListViewsAction extends ActionType<ListViewsAction.Response> {

    public static final ListViewsAction INSTANCE = new ListViewsAction();
    public static final String NAME = "cluster:admin/xpack/esql/views";

    private ListViewsAction() {
        super(NAME);
    }

    public static class Request extends ActionRequest {
        public Request() {
            super();
        }

        public Request(StreamInput in) throws IOException {
            super(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            return o != null && getClass() == o.getClass();
        }

        @Override
        public int hashCode() {
            return ListViewsAction.Request.class.hashCode();
        }
    }

    public static class Response extends ActionResponse implements ToXContentObject {

        private final Map<String, View> views;

        public Response(final Map<String, View> views) {
            this.views = views;
        }

        public Map<String, View> getViews() {
            return views;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            TransportAction.localOnly();
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            var v = ChunkedToXContentHelper.xContentObjectFieldObjects(VIEWS.getPreferredName(), views);
            while (v.hasNext()) {
                v.next().toXContent(builder, params);
            }
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
            return views.equals(((Response) o).views);
        }

        @Override
        public int hashCode() {
            return views.hashCode();
        }

        @Override
        public String toString() {
            return "ListViewsAction.Response{view=" + views.toString() + '}';
        }
    }
}
