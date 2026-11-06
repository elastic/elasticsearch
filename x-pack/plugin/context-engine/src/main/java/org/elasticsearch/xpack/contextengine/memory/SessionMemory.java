/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.contextengine.memory;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

/**
 * One document in the session memory data stream: a statement or summary the agent chose to keep.
 */
public class SessionMemory implements Writeable, ToXContentObject {

    public static final ParseField TIMESTAMP_FIELD = new ParseField("@timestamp");
    public static final ParseField ID_FIELD = new ParseField("id");
    public static final ParseField SPACE_FIELD = new ParseField("space");
    public static final ParseField AI_INDEX_IDS_FIELD = new ParseField("ai_index_ids");
    public static final ParseField SESSION_ID_FIELD = new ParseField("session_id");
    public static final ParseField USER_ID_FIELD = new ParseField("user_id");
    public static final ParseField CONTENT_FIELD = new ParseField("content");
    public static final ParseField MEDIA_FIELD = new ParseField("media");
    public static final ParseField INPUTS_FIELD = new ParseField("inputs");
    public static final ParseField ATTRIBUTES_FIELD = new ParseField("attributes");

    /**
     * Serializes the stored shape, where the content and media are written together as the {@code inputs} semantic field
     * instead of a {@code media} array, so one embedding space covers every input a memory holds.
     */
    public static final ToXContent.Params STORED_PARAMS = new ToXContent.MapParams(Map.of("stored", "true"));

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<SessionMemory, Void> PARSER = new ConstructingObjectParser<>(
        "session_memory",
        args -> new SessionMemory(
            args[0] == null ? null : Instant.parse((String) args[0]),
            (String) args[1],
            (String) args[2],
            (List<String>) args[3],
            (String) args[4],
            (String) args[5],
            (String) args[6],
            args[7] != null ? (List<Map<String, Object>>) args[7] : mediaFromInputs((List<Object>) args[9]),
            (Map<String, Object>) args[8]
        )
    );

    static {
        PARSER.declareString(optionalConstructorArg(), TIMESTAMP_FIELD);
        PARSER.declareString(optionalConstructorArg(), ID_FIELD);
        PARSER.declareString(optionalConstructorArg(), SPACE_FIELD);
        PARSER.declareStringArray(optionalConstructorArg(), AI_INDEX_IDS_FIELD);
        PARSER.declareString(optionalConstructorArg(), SESSION_ID_FIELD);
        PARSER.declareString(optionalConstructorArg(), USER_ID_FIELD);
        PARSER.declareString(optionalConstructorArg(), CONTENT_FIELD);
        PARSER.declareObjectArray(optionalConstructorArg(), (p, c) -> p.map(), MEDIA_FIELD);
        PARSER.declareObject(optionalConstructorArg(), (p, c) -> p.map(), ATTRIBUTES_FIELD);
        PARSER.declareField(optionalConstructorArg(), (p, c) -> parseInputs(p), INPUTS_FIELD, ObjectParser.ValueType.OBJECT_ARRAY);
    }

    private static List<Object> parseInputs(XContentParser parser) throws IOException {
        List<Object> inputs = new ArrayList<>();
        if (parser.currentToken() == XContentParser.Token.START_OBJECT) {
            inputs.add(parser.map());
            return inputs;
        }
        while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
            inputs.add(parser.currentToken() == XContentParser.Token.START_OBJECT ? parser.map() : parser.text());
        }
        return inputs;
    }

    @SuppressWarnings("unchecked")
    private static List<Map<String, Object>> mediaFromInputs(List<Object> inputs) {
        if (inputs == null) {
            return null;
        }
        List<Map<String, Object>> media = new ArrayList<>();
        for (Object input : inputs) {
            if (input instanceof Map<?, ?> item) {
                media.add((Map<String, Object>) item);
            }
        }
        return media;
    }

    public static SessionMemory fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    @Nullable
    private final Instant timestamp;
    @Nullable
    private final String id;
    @Nullable
    private final String space;
    private final List<String> aiIndexIds;
    private final String sessionId;
    private final String userId;
    @Nullable
    private final String content;
    private final List<Map<String, Object>> media;
    @Nullable
    private final Map<String, Object> attributes;

    public SessionMemory(
        @Nullable Instant timestamp,
        @Nullable String id,
        @Nullable String space,
        @Nullable List<String> aiIndexIds,
        String sessionId,
        String userId,
        @Nullable String content,
        @Nullable List<Map<String, Object>> media,
        @Nullable Map<String, Object> attributes
    ) {
        this.timestamp = timestamp;
        this.id = id;
        this.space = space;
        this.aiIndexIds = aiIndexIds == null ? List.of() : List.copyOf(aiIndexIds);
        this.sessionId = sessionId;
        this.userId = userId;
        this.content = content;
        this.media = media == null ? List.of() : List.copyOf(media);
        this.attributes = attributes;
    }

    public SessionMemory(StreamInput in) throws IOException {
        this.timestamp = in.readOptionalInstant();
        this.id = in.readOptionalString();
        this.space = in.readOptionalString();
        this.aiIndexIds = in.readStringCollectionAsList();
        this.sessionId = in.readOptionalString();
        this.userId = in.readOptionalString();
        this.content = in.readOptionalString();
        this.media = in.readCollectionAsImmutableList(StreamInput::readGenericMap);
        this.attributes = in.readBoolean() ? in.readGenericMap() : null;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalInstant(timestamp);
        out.writeOptionalString(id);
        out.writeOptionalString(space);
        out.writeStringCollection(aiIndexIds);
        out.writeOptionalString(sessionId);
        out.writeOptionalString(userId);
        out.writeOptionalString(content);
        out.writeCollection(media, StreamOutput::writeGenericMap);
        out.writeBoolean(attributes != null);
        if (attributes != null) {
            out.writeGenericMap(attributes);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (timestamp != null) {
            builder.field(TIMESTAMP_FIELD.getPreferredName(), timestamp.toString());
        }
        if (id != null) {
            builder.field(ID_FIELD.getPreferredName(), id);
        }
        if (space != null) {
            builder.field(SPACE_FIELD.getPreferredName(), space);
        }
        if (aiIndexIds.isEmpty() == false) {
            builder.stringListField(AI_INDEX_IDS_FIELD.getPreferredName(), aiIndexIds);
        }
        builder.field(SESSION_ID_FIELD.getPreferredName(), sessionId);
        builder.field(USER_ID_FIELD.getPreferredName(), userId);
        if (content != null) {
            builder.field(CONTENT_FIELD.getPreferredName(), content);
        }
        if (params.paramAsBoolean("stored", false)) {
            List<Object> inputs = new ArrayList<>();
            if (content != null) {
                inputs.add(content);
            }
            inputs.addAll(media);
            if (inputs.isEmpty() == false) {
                builder.field(INPUTS_FIELD.getPreferredName(), inputs);
            }
        } else if (media.isEmpty() == false) {
            builder.field(MEDIA_FIELD.getPreferredName(), media);
        }
        if (attributes != null) {
            builder.field(ATTRIBUTES_FIELD.getPreferredName(), attributes);
        }
        builder.endObject();
        return builder;
    }

    /**
     * Returns a copy with the given id and write time, used when the document is first stored.
     */
    public SessionMemory withIdAndTimestamp(String id, Instant timestamp) {
        return new SessionMemory(timestamp, id, space, aiIndexIds, sessionId, userId, content, media, attributes);
    }

    @Nullable
    public Instant timestamp() {
        return timestamp;
    }

    @Nullable
    public String id() {
        return id;
    }

    @Nullable
    public String space() {
        return space;
    }

    public List<String> aiIndexIds() {
        return aiIndexIds;
    }

    public String sessionId() {
        return sessionId;
    }

    public String userId() {
        return userId;
    }

    @Nullable
    public String content() {
        return content;
    }

    /**
     * Non-text inputs, each an object with a {@code type} and a data URL {@code value}, stored next to the content in the
     * {@code inputs} field.
     */
    public List<Map<String, Object>> media() {
        return media;
    }

    @Nullable
    public Map<String, Object> attributes() {
        return attributes;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SessionMemory that = (SessionMemory) o;
        return Objects.equals(timestamp, that.timestamp)
            && Objects.equals(id, that.id)
            && Objects.equals(space, that.space)
            && Objects.equals(aiIndexIds, that.aiIndexIds)
            && Objects.equals(sessionId, that.sessionId)
            && Objects.equals(userId, that.userId)
            && Objects.equals(content, that.content)
            && Objects.equals(media, that.media)
            && Objects.equals(attributes, that.attributes);
    }

    @Override
    public int hashCode() {
        return Objects.hash(timestamp, id, space, aiIndexIds, sessionId, userId, content, media, attributes);
    }
}
