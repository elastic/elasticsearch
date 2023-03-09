/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.analytics.action;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

public class PostAnalyticsEventAction extends ActionType<PostAnalyticsEventAction.Response> {

    public static final PostAnalyticsEventAction INSTANCE = new PostAnalyticsEventAction();

    public static final String NAME = "cluster:admin/xpack/application/analytics/post_event";

    private PostAnalyticsEventAction() {
        super(NAME, Response::new);
    }

    public static class Request extends ActionRequest {

        private final String collectionName;

        public Request(String collectionName) {
            this.collectionName = collectionName;
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            this.collectionName = in.readString();
        }

        public String collectionName() {
            return collectionName;
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(collectionName);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Request that = (Request) o;
            return Objects.equals(collectionName, that.collectionName);
        }

        @Override
        public int hashCode() {
            return Objects.hash(collectionName);
        }
    }

    public static class Response extends ActionResponse implements ToXContentObject {

        public static Response ACCEPTED = new Response(true);

        private static final ParseField RESULT_FIELD = new ParseField("accepted");

        private final boolean accepted;

        public Response(boolean accepted) {
            this.accepted = accepted;
        }

        public Response(StreamInput in) throws IOException {
            this.accepted = in.readBoolean();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeBoolean(accepted);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Response that = (Response) o;
            return accepted == that.accepted;
        }

        @Override
        public int hashCode() {
            return Objects.hash(accepted);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            return builder.startObject().field(RESULT_FIELD.getPreferredName(), accepted).endObject();
        }
    }
}
