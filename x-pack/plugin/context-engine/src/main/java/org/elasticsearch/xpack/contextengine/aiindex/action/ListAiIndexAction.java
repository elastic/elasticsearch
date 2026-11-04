/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.contextengine.aiindex.action;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.UntypedActionRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.contextengine.aiindex.AiIndex;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.action.ValidateActions.addValidationError;

public class ListAiIndexAction {

    public static final String NAME = "cluster:admin/xpack/context_engine/ai_index/list";
    public static final ActionType<Response> INSTANCE = new ActionType<>(NAME);
    public static final int DEFAULT_SIZE = 100;
    public static final int MAX_SIZE = 1000;

    private ListAiIndexAction() {/* no instances */}

    /**
     * Lists registry records, across all spaces unless {@code space} is given, optionally only those whose backing store
     * is {@code dest}.
     */
    public static class Request extends UntypedActionRequest {

        @Nullable
        private final String space;
        @Nullable
        private final String dest;
        private final int size;

        public Request(@Nullable String space, @Nullable String dest, int size) {
            this.space = space;
            this.dest = dest;
            this.size = size;
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            this.space = in.readOptionalString();
            this.dest = in.readOptionalString();
            this.size = in.readVInt();
        }

        @Nullable
        public String space() {
            return space;
        }

        @Nullable
        public String dest() {
            return dest;
        }

        public int size() {
            return size;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeOptionalString(space);
            out.writeOptionalString(dest);
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
            return size == request.size && Objects.equals(space, request.space) && Objects.equals(dest, request.dest);
        }

        @Override
        public int hashCode() {
            return Objects.hash(space, dest, size);
        }
    }

    public static class Response extends ActionResponse implements ToXContentObject {

        private final List<AiIndex> aiIndices;

        public Response(List<AiIndex> aiIndices) {
            this.aiIndices = List.copyOf(aiIndices);
        }

        public Response(StreamInput in) throws IOException {
            this.aiIndices = in.readCollectionAsImmutableList(AiIndex::new);
        }

        public List<AiIndex> aiIndices() {
            return aiIndices;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeCollection(aiIndices);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("count", aiIndices.size());
            builder.xContentList("results", aiIndices);
            builder.endObject();
            return builder;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Response response = (Response) o;
            return Objects.equals(aiIndices, response.aiIndices);
        }

        @Override
        public int hashCode() {
            return Objects.hash(aiIndices);
        }
    }
}
