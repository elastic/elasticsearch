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
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.application.analytics.event.AnalyticsEvent;

import java.io.IOException;
import java.util.Locale;
import java.util.Objects;

import static org.elasticsearch.action.ValidateActions.addValidationError;

public class PostAnalyticsEventAction extends ActionType<PostAnalyticsEventAction.Response> {

    public static final PostAnalyticsEventAction INSTANCE = new PostAnalyticsEventAction();

    public static final String NAME = "cluster:admin/xpack/application/analytics/post_event";

    private PostAnalyticsEventAction() {
        super(NAME, Response::new);
    }

    public static class Request extends ActionRequest {

        private final String collectionName;

        private final String eventType;

        private final BytesReference payload;

        private final XContentType xContentType;

        public Request(String collectionName, String eventType, BytesReference payload, XContentType xContentType) {
            this.collectionName = collectionName;
            this.eventType = eventType;
            this.payload = payload;
            this.xContentType = xContentType;
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            this.collectionName = in.readString();
            this.eventType = in.readString();
            this.xContentType = in.readEnum(XContentType.class);
            this.payload = in.readBytesReference();
        }

        public String collectionName() {
            return collectionName;
        }

        public AnalyticsEvent.Type eventType() {
            return AnalyticsEvent.Type.valueOf(eventType.toUpperCase(Locale.ROOT));
        }

        public BytesReference payload() {
            return payload;
        }

        public XContentType xContentType() {
            return xContentType;
        }

        @Override
        public ActionRequestValidationException validate() {
            ActionRequestValidationException validationException = null;

            if (Strings.isNullOrEmpty(collectionName)) {
                validationException = addValidationError("collection name is missing", validationException);
            }

            if (Strings.isNullOrEmpty(eventType)) {
                validationException = addValidationError("event type is missing", validationException);
            }

            try {
                AnalyticsEvent.Type.valueOf(eventType.toUpperCase(Locale.ROOT));
            } catch (IllegalArgumentException e) {
                validationException = addValidationError(Strings.format("Invalid event type: %s", eventType), validationException);
            }

            return validationException;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(collectionName);
            out.writeString(eventType);
            out.writeEnum(xContentType);
            out.writeBytesReference(payload);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Request that = (Request) o;
            return Objects.equals(collectionName, that.collectionName)
                && Objects.equals(eventType, that.eventType)
                && Objects.equals(xContentType, that.xContentType)
                && payload.utf8ToString() == that.payload.utf8ToString();

        }

        @Override
        public int hashCode() {
            return Objects.hash(collectionName, eventType, xContentType, payload.utf8ToString());
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
