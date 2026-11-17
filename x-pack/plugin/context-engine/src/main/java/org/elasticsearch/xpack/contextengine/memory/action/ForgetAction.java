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
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.action.ValidateActions.addValidationError;

public class ForgetAction {

    public static final String NAME = "cluster:admin/xpack/context_engine/session_memory/forget";
    public static final ActionType<Response> INSTANCE = new ActionType<>(NAME);

    private ForgetAction() {/* no instances */}

    /**
     * Deletes every revision of the memory with the given id, narrowed by whichever of the filters are given.
     */
    public static class Request extends UntypedActionRequest {

        private final String id;
        @Nullable
        private final String sessionId;
        @Nullable
        private final String userId;
        @Nullable
        private final String space;

        public Request(String id, @Nullable String sessionId, @Nullable String userId, @Nullable String space) {
            this.id = id;
            this.sessionId = sessionId;
            this.userId = userId;
            this.space = space;
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            this.id = in.readString();
            this.sessionId = in.readOptionalString();
            this.userId = in.readOptionalString();
            this.space = in.readOptionalString();
        }

        public String id() {
            return id;
        }

        @Nullable
        public String sessionId() {
            return sessionId;
        }

        @Nullable
        public String userId() {
            return userId;
        }

        @Nullable
        public String space() {
            return space;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(id);
            out.writeOptionalString(sessionId);
            out.writeOptionalString(userId);
            out.writeOptionalString(space);
        }

        @Override
        public ActionRequestValidationException validate() {
            ActionRequestValidationException validationException = null;
            if (Strings.isNullOrEmpty(id)) {
                validationException = addValidationError("[id] cannot be null or empty", validationException);
            }
            return validationException;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Request request = (Request) o;
            return Objects.equals(id, request.id)
                && Objects.equals(sessionId, request.sessionId)
                && Objects.equals(userId, request.userId)
                && Objects.equals(space, request.space);
        }

        @Override
        public int hashCode() {
            return Objects.hash(id, sessionId, userId, space);
        }
    }

    public static class Response extends ActionResponse implements ToXContentObject {

        private final int deleted;

        public Response(int deleted) {
            this.deleted = deleted;
        }

        public Response(StreamInput in) throws IOException {
            this.deleted = in.readVInt();
        }

        public int deleted() {
            return deleted;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVInt(deleted);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("deleted", deleted);
            builder.endObject();
            return builder;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Response response = (Response) o;
            return deleted == response.deleted;
        }

        @Override
        public int hashCode() {
            return Objects.hash(deleted);
        }
    }
}
