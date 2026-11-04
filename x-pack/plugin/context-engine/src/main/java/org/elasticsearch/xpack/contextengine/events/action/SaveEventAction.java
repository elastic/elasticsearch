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
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.contextengine.events.SessionEvent;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.action.ValidateActions.addValidationError;

public class SaveEventAction {

    public static final String NAME = "cluster:admin/xpack/context_engine/session_events/save";
    public static final ActionType<Response> INSTANCE = new ActionType<>(NAME);

    private SaveEventAction() {/* no instances */}

    public static class Request extends UntypedActionRequest implements ToXContentObject {

        private final SessionEvent event;

        public Request(SessionEvent event) {
            this.event = event;
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            this.event = new SessionEvent(in);
        }

        public static Request fromXContent(XContentParser parser) throws IOException {
            return new Request(SessionEvent.fromXContent(parser));
        }

        public SessionEvent event() {
            return event;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            event.writeTo(out);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            return event.toXContent(builder, params);
        }

        @Override
        public ActionRequestValidationException validate() {
            ActionRequestValidationException validationException = null;
            if (Strings.isNullOrEmpty(event.sessionId())) {
                validationException = addValidationError("[session_id] cannot be null or empty", validationException);
            }
            if (Strings.isNullOrEmpty(event.userId())) {
                validationException = addValidationError("[user_id] cannot be null or empty", validationException);
            }
            if (Strings.isNullOrEmpty(event.transcript())) {
                validationException = addValidationError("[transcript] cannot be null or empty", validationException);
            }
            if (event.timestamp() != null) {
                validationException = addValidationError("[@timestamp] is set at write time and cannot be provided", validationException);
            }
            return validationException;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Request request = (Request) o;
            return Objects.equals(event, request.event);
        }

        @Override
        public int hashCode() {
            return Objects.hash(event);
        }
    }

    public static class Response extends ActionResponse implements ToXContentObject {

        private final SessionEvent event;

        public Response(SessionEvent event) {
            this.event = event;
        }

        public Response(StreamInput in) throws IOException {
            this.event = new SessionEvent(in);
        }

        public SessionEvent event() {
            return event;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            event.writeTo(out);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            return event.toXContent(builder, params);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Response response = (Response) o;
            return Objects.equals(event, response.event);
        }

        @Override
        public int hashCode() {
            return Objects.hash(event);
        }
    }
}
