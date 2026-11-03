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
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.DataFormat;
import org.elasticsearch.inference.DataType;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.contextengine.memory.SessionMemory;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.action.ValidateActions.addValidationError;
import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

public class SearchSessionMemoryAction {

    public static final String NAME = "cluster:admin/xpack/context_engine/session_memory/search";
    public static final ActionType<Response> INSTANCE = new ActionType<>(NAME);
    public static final int DEFAULT_SIZE = 10;
    /** Every hit is reranked, so the cap bounds the rerank pool as well as the kNN candidates. */
    public static final int MAX_SIZE = 100;

    private SearchSessionMemoryAction() {/* no instances */}

    /**
     * Searches session memory by meaning. The query is text, or a non-text input object in the same shape as a
     * {@code media} item, and is matched against both {@code content.semantic} and {@code media} through the session
     * memory endpoint. The optional filters narrow the candidate set before the vector search.
     */
    public static class Request extends UntypedActionRequest implements ToXContentObject {

        public static final ParseField QUERY_FIELD = new ParseField("query");
        public static final ParseField SIZE_FIELD = new ParseField("size");

        @SuppressWarnings("unchecked")
        private static final ConstructingObjectParser<Request, Void> PARSER = new ConstructingObjectParser<>(
            "search_session_memory_request",
            args -> new Request(
                args[0],
                (String) args[1],
                (String) args[2],
                (String) args[3],
                (String) args[4],
                (String) args[5],
                args[6] == null ? DEFAULT_SIZE : (Integer) args[6]
            )
        );

        static {
            PARSER.declareField(
                constructorArg(),
                (p, c) -> p.currentToken() == XContentParser.Token.VALUE_STRING ? p.text() : p.map(),
                QUERY_FIELD,
                ObjectParser.ValueType.OBJECT_OR_STRING
            );
            PARSER.declareString(optionalConstructorArg(), SessionMemory.SESSION_ID_FIELD);
            PARSER.declareString(optionalConstructorArg(), SessionMemory.USER_ID_FIELD);
            PARSER.declareString(optionalConstructorArg(), SessionMemory.AGENT_ID_FIELD);
            PARSER.declareString(optionalConstructorArg(), SessionMemory.SPACE_FIELD);
            PARSER.declareString(optionalConstructorArg(), SessionMemory.AI_INDEX_ID_FIELD);
            PARSER.declareInt(optionalConstructorArg(), SIZE_FIELD);
        }

        public static Request fromXContent(XContentParser parser) throws IOException {
            return PARSER.parse(parser, null);
        }

        private final Object query;
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
            Object query,
            @Nullable String sessionId,
            @Nullable String userId,
            @Nullable String agentId,
            @Nullable String space,
            @Nullable String aiIndexId,
            int size
        ) {
            this.query = query;
            this.sessionId = sessionId;
            this.userId = userId;
            this.agentId = agentId;
            this.space = space;
            this.aiIndexId = aiIndexId;
            this.size = size;
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            this.query = in.readGenericValue();
            this.sessionId = in.readOptionalString();
            this.userId = in.readOptionalString();
            this.agentId = in.readOptionalString();
            this.space = in.readOptionalString();
            this.aiIndexId = in.readOptionalString();
            this.size = in.readVInt();
        }

        /**
         * The query input: a {@link String} for text, or a {@link Map} with {@code type} and {@code value} for non-text input.
         */
        public Object query() {
            return query;
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
            out.writeGenericValue(query);
            out.writeOptionalString(sessionId);
            out.writeOptionalString(userId);
            out.writeOptionalString(agentId);
            out.writeOptionalString(space);
            out.writeOptionalString(aiIndexId);
            out.writeVInt(size);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(QUERY_FIELD.getPreferredName(), query);
            if (sessionId != null) {
                builder.field(SessionMemory.SESSION_ID_FIELD.getPreferredName(), sessionId);
            }
            if (userId != null) {
                builder.field(SessionMemory.USER_ID_FIELD.getPreferredName(), userId);
            }
            if (agentId != null) {
                builder.field(SessionMemory.AGENT_ID_FIELD.getPreferredName(), agentId);
            }
            if (space != null) {
                builder.field(SessionMemory.SPACE_FIELD.getPreferredName(), space);
            }
            if (aiIndexId != null) {
                builder.field(SessionMemory.AI_INDEX_ID_FIELD.getPreferredName(), aiIndexId);
            }
            builder.field(SIZE_FIELD.getPreferredName(), size);
            builder.endObject();
            return builder;
        }

        @Override
        public ActionRequestValidationException validate() {
            ActionRequestValidationException validationException = null;
            if (query instanceof String text) {
                if (text.isEmpty()) {
                    validationException = addValidationError("[query] cannot be empty", validationException);
                }
            } else if (query instanceof Map<?, ?> input) {
                if (isNonEmptyString(input.get("type")) == false || isNonEmptyString(input.get("value")) == false) {
                    validationException = addValidationError(
                        "[query] objects must have a non-empty [type] and [value]",
                        validationException
                    );
                } else {
                    validationException = validateTypeAndFormat("[query]", input, validationException);
                }
            } else {
                validationException = addValidationError("[query] must be a string or an object", validationException);
            }
            if (size <= 0 || size > MAX_SIZE) {
                validationException = addValidationError("[size] must be between 1 and " + MAX_SIZE, validationException);
            }
            return validationException;
        }

        private static boolean isNonEmptyString(Object value) {
            return value instanceof String string && string.isEmpty() == false;
        }

        static ActionRequestValidationException validateTypeAndFormat(
            String field,
            Map<?, ?> input,
            ActionRequestValidationException validationException
        ) {
            try {
                DataType.fromString((String) input.get("type"));
            } catch (IllegalArgumentException e) {
                validationException = addValidationError(field + " " + e.getMessage(), validationException);
            }
            Object format = input.get("format");
            if (format instanceof String string) {
                try {
                    DataFormat.fromString(string);
                } catch (IllegalArgumentException e) {
                    validationException = addValidationError(field + " " + e.getMessage(), validationException);
                }
            } else if (format != null) {
                validationException = addValidationError(field + " [format] must be a string", validationException);
            }
            return validationException;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Request request = (Request) o;
            return size == request.size
                && Objects.equals(query, request.query)
                && Objects.equals(sessionId, request.sessionId)
                && Objects.equals(userId, request.userId)
                && Objects.equals(agentId, request.agentId)
                && Objects.equals(space, request.space)
                && Objects.equals(aiIndexId, request.aiIndexId);
        }

        @Override
        public int hashCode() {
            return Objects.hash(query, sessionId, userId, agentId, space, aiIndexId, size);
        }
    }

    /**
     * One matching memory and its relevance score.
     */
    public record Hit(SessionMemory memory, float score) implements Writeable, ToXContentObject {

        public Hit(StreamInput in) throws IOException {
            this(new SessionMemory(in), in.readFloat());
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            memory.writeTo(out);
            out.writeFloat(score);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("_score", score);
            builder.field("memory", memory);
            builder.endObject();
            return builder;
        }
    }

    public static class Response extends ActionResponse implements ToXContentObject {

        private final List<Hit> hits;

        public Response(List<Hit> hits) {
            this.hits = List.copyOf(hits);
        }

        public Response(StreamInput in) throws IOException {
            this.hits = in.readCollectionAsImmutableList(Hit::new);
        }

        public List<Hit> hits() {
            return hits;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeCollection(hits);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("count", hits.size());
            builder.xContentList("results", hits);
            builder.endObject();
            return builder;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Response response = (Response) o;
            return Objects.equals(hits, response.hits);
        }

        @Override
        public int hashCode() {
            return Objects.hash(hits);
        }
    }
}
