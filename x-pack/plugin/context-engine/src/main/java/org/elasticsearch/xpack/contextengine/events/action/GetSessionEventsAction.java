/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.contextengine.events.action;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.UntypedActionRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.contextengine.events.SessionEvent;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.action.ValidateActions.addValidationError;

public class GetSessionEventsAction {

    public static final String NAME = "cluster:admin/xpack/context_engine/session_events/get";
    public static final ActionType<Response> INSTANCE = new ActionType<>(NAME);
    public static final int DEFAULT_SIZE = 100;

    private GetSessionEventsAction() {/* no instances */}

    public static class Request extends UntypedActionRequest {

        private final String sessionId;
        @Nullable
        private final String space;
        @Nullable
        private final String aiIndexId;
        private final int size;

        public Request(String sessionId, @Nullable String space, @Nullable String aiIndexId, int size) {
            this.sessionId = sessionId;
            this.space = space;
            this.aiIndexId = aiIndexId;
            this.size = size;
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            this.sessionId = in.readString();
            this.space = in.readOptionalString();
            this.aiIndexId = in.readOptionalString();
            this.size = in.readVInt();
        }

        public String sessionId() {
            return sessionId;
        }

        @Nullable
        public String space() {
            return space;
        }

        @Nullable
        public String aiIndexId() {
            return aiIndexId;
        }

        public int size() {
            return size;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(sessionId);
            out.writeOptionalString(space);
            out.writeOptionalString(aiIndexId);
            out.writeVInt(size);
        }

        @Override
        public ActionRequestValidationException validate() {
            ActionRequestValidationException validationException = null;
            if (Strings.isNullOrEmpty(sessionId)) {
                validationException = addValidationError("[session_id] cannot be null or empty", validationException);
            }
            if (size <= 0) {
                validationException = addValidationError("[size] must be greater than 0", validationException);
            }
            return validationException;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Request request = (Request) o;
            return size == request.size
                && Objects.equals(sessionId, request.sessionId)
                && Objects.equals(space, request.space)
                && Objects.equals(aiIndexId, request.aiIndexId);
        }

        @Override
        public int hashCode() {
            return Objects.hash(sessionId, space, aiIndexId, size);
        }
    }

    public static class Response extends ActionResponse implements ToXContentObject {

        private final List<SessionEvent> events;

        public Response(List<SessionEvent> events) {
            this.events = List.copyOf(events);
        }

        public Response(StreamInput in) throws IOException {
            this.events = in.readCollectionAsImmutableList(SessionEvent::new);
        }

        public List<SessionEvent> events() {
            return events;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeCollection(events);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("count", events.size());
            builder.xContentList("results", events);
            builder.endObject();
            return builder;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Response response = (Response) o;
            return Objects.equals(events, response.events);
        }

        @Override
        public int hashCode() {
            return Objects.hash(events);
        }
    }
}
