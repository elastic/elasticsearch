/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.contextengine.memory.action;

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
import org.elasticsearch.xpack.contextengine.memory.SessionMemory;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.action.ValidateActions.addValidationError;

public class RememberAction {

    public static final String NAME = "cluster:admin/xpack/context_engine/session_memory/remember";
    public static final ActionType<Response> INSTANCE = new ActionType<>(NAME);

    private RememberAction() {/* no instances */}

    public static class Request extends UntypedActionRequest implements ToXContentObject {

        private final SessionMemory memory;

        public Request(SessionMemory memory) {
            this.memory = memory;
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            this.memory = new SessionMemory(in);
        }

        public static Request fromXContent(XContentParser parser) throws IOException {
            return new Request(SessionMemory.fromXContent(parser));
        }

        public SessionMemory memory() {
            return memory;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            memory.writeTo(out);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            return memory.toXContent(builder, params);
        }

        @Override
        public ActionRequestValidationException validate() {
            ActionRequestValidationException validationException = null;
            if (Strings.isNullOrEmpty(memory.sessionId())) {
                validationException = addValidationError("[session_id] cannot be null or empty", validationException);
            }
            if (Strings.isNullOrEmpty(memory.userId())) {
                validationException = addValidationError("[user_id] cannot be null or empty", validationException);
            }
            if (Strings.isNullOrEmpty(memory.agentId())) {
                validationException = addValidationError("[agent_id] cannot be null or empty", validationException);
            }
            if (Strings.isNullOrEmpty(memory.content())) {
                validationException = addValidationError("[content] cannot be null or empty", validationException);
            }
            if (memory.timestamp() != null) {
                validationException = addValidationError("[@timestamp] is set at write time and cannot be provided", validationException);
            }
            return validationException;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Request request = (Request) o;
            return Objects.equals(memory, request.memory);
        }

        @Override
        public int hashCode() {
            return Objects.hash(memory);
        }
    }

    public static class Response extends ActionResponse implements ToXContentObject {

        private final SessionMemory memory;

        public Response(SessionMemory memory) {
            this.memory = memory;
        }

        public Response(StreamInput in) throws IOException {
            this.memory = new SessionMemory(in);
        }

        public SessionMemory memory() {
            return memory;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            memory.writeTo(out);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            return memory.toXContent(builder, params);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Response response = (Response) o;
            return Objects.equals(memory, response.memory);
        }

        @Override
        public int hashCode() {
            return Objects.hash(memory);
        }
    }
}
