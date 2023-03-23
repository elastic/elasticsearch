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
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.application.analytics.event.AnalyticsEvent;
import org.elasticsearch.xpack.application.analytics.event.AnalyticsEventType;

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

        private final long eventTime;

        private final boolean debug;

        private final BytesReference payload;

        private final XContentType xContentType;

        public Request(String collectionName, String eventType, boolean debug, XContentType xContentType, BytesReference payload) {
            this(collectionName, eventType, debug, System.currentTimeMillis(), xContentType, payload);
        }

        public Request(
            String collectionName,
            String eventType,
            boolean debug,
            long eventTime,
            XContentType xContentType,
            BytesReference payload
        ) {
            this.collectionName = collectionName;
            this.eventType = eventType;
            this.debug = debug;
            this.eventTime = eventTime;
            this.xContentType = xContentType;
            this.payload = payload;
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            this.collectionName = in.readString();
            this.eventType = in.readString();
            this.debug = in.readBoolean();
            this.eventTime = in.readLong();
            this.xContentType = in.readEnum(XContentType.class);
            this.payload = in.readBytesReference();
        }

        public String collectionName() {
            return collectionName;
        }

        public AnalyticsEventType eventType() {
            return AnalyticsEventType.valueOf(eventType.toUpperCase(Locale.ROOT));
        }

        public long eventTime() {
            return eventTime;
        }

        public BytesReference payload() {
            return payload;
        }

        public boolean isDebug() {
            return debug;
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
                AnalyticsEventType.valueOf(eventType.toUpperCase(Locale.ROOT));
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
            out.writeBoolean(debug);
            out.writeLong(eventTime);
            XContentHelper.writeTo(out, xContentType);
            out.writeBytesReference(payload);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Request that = (Request) o;
            return Objects.equals(collectionName, that.collectionName)
                && debug == that.debug
                && eventTime == that.eventTime
                && Objects.equals(eventType, that.eventType)
                && Objects.equals(xContentType, that.xContentType)
                && Objects.equals(payload, that.payload);
        }

        @Override
        public int hashCode() {
            return Objects.hash(collectionName, eventType, debug, eventTime, xContentType, payload);
        }
    }

    public static class Response extends ActionResponse implements ToXContentObject {

        public static Response ACCEPTED = new Response(true);

        private static final ParseField RESULT_FIELD = new ParseField("accepted");

        private static final ParseField EVENT_FIELD = new ParseField("event");

        private final boolean accepted;

        private final boolean debug;

        private final AnalyticsEvent analyticsEvent;

        public boolean isAccepted() {
            return accepted;
        }

        public boolean isDebug() {
            return debug;
        }

        public AnalyticsEvent getAnalyticsEvent() {
            return analyticsEvent;
        }

        public Response(boolean accepted) {
            this(accepted, null);
        }

        public Response(boolean accepted, AnalyticsEvent event) {
            this.accepted = accepted;
            this.debug = Objects.nonNull(event);
            this.analyticsEvent = event;
        }

        public Response(StreamInput in) throws IOException {
            this.accepted = in.readBoolean();
            this.debug = in.readBoolean();
            this.analyticsEvent = this.debug ? new AnalyticsEvent(in) : null;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeBoolean(accepted);
            out.writeBoolean(debug);
            if (debug) analyticsEvent.writeTo(out);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Response that = (Response) o;
            return accepted == that.accepted && debug == that.debug && Objects.equals(analyticsEvent, that.analyticsEvent);
        }

        @Override
        public int hashCode() {
            return Objects.hash(accepted, debug, analyticsEvent);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            {
                builder.field(RESULT_FIELD.getPreferredName(), accepted);
                if (debug) builder.field(EVENT_FIELD.getPreferredName(), analyticsEvent);
            }

            return builder.endObject();
        }
    }
}
