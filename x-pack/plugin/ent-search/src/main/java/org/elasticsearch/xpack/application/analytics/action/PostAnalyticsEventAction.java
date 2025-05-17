/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.analytics.action;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.LegacyActionRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.network.NetworkAddress;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.UpdateForV10;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.application.analytics.event.AnalyticsEvent;

import java.io.IOException;
import java.net.InetAddress;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.action.ValidateActions.addValidationError;
import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;

/**
 * @deprecated in 9.0
 */
@Deprecated
@UpdateForV10(owner = UpdateForV10.Owner.ENTERPRISE_SEARCH)
public class PostAnalyticsEventAction {

    public static final String NAME = "cluster:admin/xpack/application/analytics/post_event";
    public static final ActionType<Response> INSTANCE = new ActionType<>(NAME);

    private PostAnalyticsEventAction() {/* no instances */}

    public static class Request extends LegacyActionRequest implements AnalyticsEvent.Context, ToXContentObject {

        private final String eventCollectionName;

        private final String eventType;

        private final long eventTime;

        private final boolean debug;

        private final BytesReference payload;

        private final XContentType xContentType;

        private final Map<String, List<String>> headers;

        private final String clientAddress;

        private Request(
            String eventCollectionName,
            String eventType,
            long eventTime,
            XContentType xContentType,
            BytesReference payload,
            boolean debug,
            @Nullable Map<String, List<String>> headers,
            @Nullable String clientAddress
        ) {
            this.eventCollectionName = eventCollectionName;
            this.eventType = eventType;
            this.debug = debug;
            this.eventTime = eventTime;
            this.xContentType = xContentType;
            this.payload = payload;
            this.headers = Objects.requireNonNullElse(headers, Map.of());
            this.clientAddress = clientAddress;
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            this.eventCollectionName = in.readString();
            this.eventType = in.readString();
            this.debug = in.readBoolean();
            this.eventTime = in.readLong();
            this.xContentType = in.readEnum(XContentType.class);
            this.payload = in.readBytesReference();
            this.headers = in.readMap(StreamInput::readStringCollectionAsList);
            this.clientAddress = in.readOptionalString();
        }

        public static RequestBuilder builder(
            String eventCollectionName,
            String eventType,
            XContentType xContentType,
            BytesReference payload
        ) {
            return new RequestBuilder(eventCollectionName, eventType, xContentType, payload);
        }

        @Override
        public String eventCollectionName() {
            return eventCollectionName;
        }

        @Override
        public AnalyticsEvent.Type eventType() {
            return AnalyticsEvent.Type.valueOf(eventType.toUpperCase(Locale.ROOT));
        }

        @Override
        public long eventTime() {
            return eventTime;
        }

        @Override
        public String userAgent() {
            return header("User-Agent");
        }

        @Override
        public String clientAddress() {
            return clientAddress;
        }

        public BytesReference payload() {
            return payload;
        }

        public XContentType xContentType() {
            return xContentType;
        }

        public boolean isDebug() {
            return debug;
        }

        private String header(String header) {
            final List<String> values = headers.get(header);
            if (values != null && values.isEmpty() == false) {
                return values.get(0);
            }

            return null;
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
            out.writeMap(headers, StreamOutput::writeStringCollection);
            out.writeOptionalString(clientAddress);
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
                && Objects.equals(xContentType.canonical(), that.xContentType.canonical())
                && Objects.equals(payload, that.payload)
                && Objects.equals(headers, that.headers)
                && Objects.equals(clientAddress, that.clientAddress);
        }

        @Override
        public int hashCode() {
            return Objects.hash(
                eventCollectionName,
                eventType,
                debug,
                eventTime,
                xContentType.canonical(),
                payload,
                headers,
                clientAddress
            );
        }

        @Override
        public String toString() {
            return Strings.toString(this);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("event_collection_name", eventCollectionName);
            builder.field("debug", debug);
            builder.field("event_time", eventTime);
            builder.field("event_type", eventType);
            builder.field("x_content_type", xContentType);
            builder.field("payload", payload);
            builder.field("headers", headers);
            builder.field("client_address", clientAddress);
            builder.endObject();
            return builder;
        }
    }

    public static class RequestBuilder {

        private String eventCollectionName;

        private String eventType;

        private long eventTime = System.currentTimeMillis();

        private boolean debug = false;

        private BytesReference payload;

        private XContentType xContentType;

        private Map<String, List<String>> headers;

        private String clientAddress;

        private RequestBuilder(String eventCollectionName, String eventType, XContentType xContentType, BytesReference payload) {
            this.eventCollectionName = eventCollectionName;
            this.eventType = eventType;
            this.xContentType = xContentType;
            this.payload = payload;
        }

        public Request request() {
            return new Request(eventCollectionName, eventType, eventTime, xContentType, payload, debug, headers, clientAddress);
        }

        public RequestBuilder eventCollectionName(String eventCollectionName) {
            this.eventCollectionName = eventCollectionName;
            return this;
        }

        public RequestBuilder eventType(String eventType) {
            this.eventType = eventType;
            return this;
        }

        public RequestBuilder eventTime(long eventTime) {
            this.eventTime = eventTime;
            return this;
        }

        public RequestBuilder debug(boolean debug) {
            this.debug = debug;
            return this;
        }

        public RequestBuilder payload(BytesReference payload) {
            this.payload = payload;
            return this;
        }

        public RequestBuilder xContentType(XContentType xContentType) {
            this.xContentType = xContentType;
            return this;
        }

        public RequestBuilder headers(Map<String, List<String>> headers) {
            this.headers = headers;
            return this;
        }

        public RequestBuilder clientAddress(InetAddress clientAddress) {
            return clientAddress(NetworkAddress.format(clientAddress));
        }

        public RequestBuilder clientAddress(String clientAddress) {
            this.clientAddress = clientAddress;
            return this;
        }
    }

    public static class Response extends ActionResponse implements ToXContentObject {
        public static final Response ACCEPTED = new Response(true);

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

        private static final ConstructingObjectParser<Response, String> PARSER = new ConstructingObjectParser<>(
            "post_analytics_event_response",
            false,
            (params) -> new Response((boolean) params[0])
        );

        static {
            PARSER.declareBoolean(constructorArg(), Response.RESULT_FIELD);
        }

        public static Response parse(XContentParser parser) {
            return PARSER.apply(parser, null);
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
