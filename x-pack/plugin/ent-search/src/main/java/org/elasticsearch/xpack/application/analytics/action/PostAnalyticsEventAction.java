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
import org.elasticsearch.common.xcontent.StatusToXContentObject;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xcontent.ParseField;
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
        super(NAME, Response::readFromStreamInput);
    }

    public static class Request extends ActionRequest implements AnalyticsEvent.Context {

        private final String eventCollectionName;

        private final String eventType;

        private final long eventTime;

        private final boolean debug;

        private final BytesReference payload;

        private final XContentType xContentType;

        public Request(String eventCollectionName, String eventType, boolean debug, XContentType xContentType, BytesReference payload) {
            this(eventCollectionName, eventType, debug, System.currentTimeMillis(), xContentType, payload);
        }

        public Request(
            String eventCollectionName,
            String eventType,
            boolean debug,
            long eventTime,
            XContentType xContentType,
            BytesReference payload
        ) {
            this.eventCollectionName = eventCollectionName;
            this.eventType = eventType;
            this.debug = debug;
            this.eventTime = eventTime;
            this.xContentType = xContentType;
            this.payload = payload;
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            this.eventCollectionName = in.readString();
            this.eventType = in.readString();
            this.debug = in.readBoolean();
            this.eventTime = in.readLong();
            this.xContentType = in.readEnum(XContentType.class);
            this.payload = in.readBytesReference();
        }

        public String eventCollectionName() {
            return eventCollectionName;
        }

        public AnalyticsEvent.Type eventType() {
            return AnalyticsEvent.Type.valueOf(eventType.toUpperCase(Locale.ROOT));
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

            if (Strings.isNullOrEmpty(eventCollectionName)) {
                validationException = addValidationError("collection name is missing", validationException);
            }

            if (Strings.isNullOrEmpty(eventType)) {
                validationException = addValidationError("event type is missing", validationException);
            }

            try {
                if (eventType.toLowerCase(Locale.ROOT).equals(eventType) == false) {
                    throw new IllegalArgumentException("event type must be lowercase");
                }

                AnalyticsEvent.Type.valueOf(eventType.toUpperCase(Locale.ROOT));
            } catch (IllegalArgumentException e) {
                validationException = addValidationError(Strings.format("invalid event type: [%s]", eventType), validationException);
            }

            return validationException;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(eventCollectionName);
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
            return Objects.equals(eventCollectionName, that.eventCollectionName)
                && debug == that.debug
                && eventTime == that.eventTime
                && Objects.equals(eventType, that.eventType)
                && Objects.equals(xContentType, that.xContentType)
                && Objects.equals(payload, that.payload);
        }

        @Override
        public int hashCode() {
            return Objects.hash(eventCollectionName, eventType, debug, eventTime, xContentType, payload);
        }
    }

    public static class Response extends ActionResponse implements StatusToXContentObject {
        public static Response ACCEPTED = new Response(true);

        public static Response readFromStreamInput(StreamInput in) throws IOException {
            boolean accepted = in.readBoolean();
            boolean isDebug = in.readBoolean();

            if (isDebug) {
                return new DebugResponse(accepted, new AnalyticsEvent(in));
            }

            return new Response(accepted);
        }

        private static final ParseField RESULT_FIELD = new ParseField("accepted");

        private final boolean accepted;

        public Response(boolean accepted) {
            this.accepted = accepted;
        }

        public boolean isAccepted() {
            return accepted;
        }

        public boolean isDebug() {
            return false;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeBoolean(accepted);
            out.writeBoolean(isDebug());
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Response that = (Response) o;
            return accepted == that.accepted && isDebug() == that.isDebug();
        }

        @Override
        public int hashCode() {
            return Objects.hash(accepted, isDebug());
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            {
                builder.field(RESULT_FIELD.getPreferredName(), accepted);
                addFieldsToXContent(builder, params);
            }

            return builder.endObject();
        }

        protected void addFieldsToXContent(XContentBuilder builder, Params params) throws IOException {

        }

        @Override
        public RestStatus status() {
            return RestStatus.ACCEPTED;
        }
    }

    public static class DebugResponse extends Response {
        private static final ParseField EVENT_FIELD = new ParseField("event");

        private final AnalyticsEvent analyticsEvent;

        public DebugResponse(boolean accepted, AnalyticsEvent analyticsEvent) {
            super(accepted);
            this.analyticsEvent = Objects.requireNonNull(analyticsEvent, "analyticsEvent cannot be null");
        }

        @Override
        public boolean isDebug() {
            return true;
        }

        public AnalyticsEvent analyticsEvent() {
            return analyticsEvent;
        }

        protected void addFieldsToXContent(XContentBuilder builder, Params params) throws IOException {
            builder.field(EVENT_FIELD.getPreferredName(), analyticsEvent());
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            analyticsEvent.writeTo(out);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            DebugResponse that = (DebugResponse) o;
            return super.equals(o) && analyticsEvent.equals(that.analyticsEvent);
        }

        @Override
        public int hashCode() {
            return 31 * super.hashCode() + Objects.hashCode(analyticsEvent);
        }
    }
}
