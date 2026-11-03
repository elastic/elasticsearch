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
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.contextengine.memory.SessionMemory;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.action.ValidateActions.addValidationError;

public class ListSessionMemoryAction {

    public static final String NAME = "cluster:admin/xpack/context_engine/session_memory/list";
    public static final ActionType<Response> INSTANCE = new ActionType<>(NAME);
    public static final int DEFAULT_SIZE = 100;
    public static final int MAX_SIZE = 1000;

    private ListSessionMemoryAction() {/* no instances */}

    /**
     * Lists memories newest first, narrowed by whichever of the filters are given, without any inference.
     */
    public static class Request extends UntypedActionRequest {

        @Nullable
        private final String sessionId;
        @Nullable
        private final String userId;
        @Nullable
        private final String agentId;
        @Nullable
        private final String space;
        @Nullable
        private final String aiIndexId;
        private final int size;

        public Request(
            @Nullable String sessionId,
            @Nullable String userId,
            @Nullable String agentId,
            @Nullable String space,
            @Nullable String aiIndexId,
            int size
        ) {
            this.sessionId = sessionId;
            this.userId = userId;
            this.agentId = agentId;
            this.space = space;
            this.aiIndexId = aiIndexId;
            this.size = size;
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            this.sessionId = in.readOptionalString();
            this.userId = in.readOptionalString();
            this.agentId = in.readOptionalString();
            this.space = in.readOptionalString();
            this.aiIndexId = in.readOptionalString();
            this.size = in.readVInt();
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
        public String agentId() {
            return agentId;
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
            out.writeOptionalString(sessionId);
            out.writeOptionalString(userId);
            out.writeOptionalString(agentId);
            out.writeOptionalString(space);
            out.writeOptionalString(aiIndexId);
            out.writeVInt(size);
        }

        @Override
        public ActionRequestValidationException validate() {
            ActionRequestValidationException validationException = null;
            if (size <= 0 || size > MAX_SIZE) {
                validationException = addValidationError("[size] must be between 1 and " + MAX_SIZE, validationException);
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
                && Objects.equals(userId, request.userId)
                && Objects.equals(agentId, request.agentId)
                && Objects.equals(space, request.space)
                && Objects.equals(aiIndexId, request.aiIndexId);
        }

        @Override
        public int hashCode() {
            return Objects.hash(sessionId, userId, agentId, space, aiIndexId, size);
        }
    }

    public static class Response extends ActionResponse implements ToXContentObject {

        private final List<SessionMemory> memories;

        public Response(List<SessionMemory> memories) {
            this.memories = List.copyOf(memories);
        }

        public Response(StreamInput in) throws IOException {
            this.memories = in.readCollectionAsImmutableList(SessionMemory::new);
        }

        public List<SessionMemory> memories() {
            return memories;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeCollection(memories);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("count", memories.size());
            builder.xContentList("results", memories);
            builder.endObject();
            return builder;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Response response = (Response) o;
            return Objects.equals(memories, response.memories);
        }

        @Override
        public int hashCode() {
            return Objects.hash(memories);
        }
    }
}
