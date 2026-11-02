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
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.time.Instant;
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
    public static final ParseField AI_INDEX_ID_FIELD = new ParseField("ai_index_id");
    public static final ParseField SESSION_ID_FIELD = new ParseField("session_id");
    public static final ParseField USER_ID_FIELD = new ParseField("user_id");
    public static final ParseField AGENT_ID_FIELD = new ParseField("agent_id");
    public static final ParseField TYPE_FIELD = new ParseField("type");
    public static final ParseField CONTENT_FIELD = new ParseField("content");
    public static final ParseField MEDIA_FIELD = new ParseField("media");
    public static final ParseField ATTRIBUTES_FIELD = new ParseField("attributes");

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
            (String) args[7],
            (String) args[8],
            (List<Map<String, Object>>) args[9],
            (Map<String, Object>) args[10]
        )
    );

    static {
        PARSER.declareString(optionalConstructorArg(), TIMESTAMP_FIELD);
        PARSER.declareString(optionalConstructorArg(), ID_FIELD);
        PARSER.declareString(optionalConstructorArg(), SPACE_FIELD);
        PARSER.declareStringArray(optionalConstructorArg(), AI_INDEX_ID_FIELD);
        PARSER.declareString(optionalConstructorArg(), SESSION_ID_FIELD);
        PARSER.declareString(optionalConstructorArg(), USER_ID_FIELD);
        PARSER.declareString(optionalConstructorArg(), AGENT_ID_FIELD);
        PARSER.declareString(optionalConstructorArg(), TYPE_FIELD);
        PARSER.declareString(optionalConstructorArg(), CONTENT_FIELD);
        PARSER.declareObjectArray(optionalConstructorArg(), (p, c) -> p.map(), MEDIA_FIELD);
        PARSER.declareObject(optionalConstructorArg(), (p, c) -> p.map(), ATTRIBUTES_FIELD);
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
    private final String agentId;
    @Nullable
    private final String type;
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
        String agentId,
        @Nullable String type,
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
        this.agentId = agentId;
        this.type = type;
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
        this.agentId = in.readOptionalString();
        this.type = in.readOptionalString();
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
        out.writeOptionalString(agentId);
        out.writeOptionalString(type);
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
            builder.stringListField(AI_INDEX_ID_FIELD.getPreferredName(), aiIndexIds);
        }
        builder.field(SESSION_ID_FIELD.getPreferredName(), sessionId);
        builder.field(USER_ID_FIELD.getPreferredName(), userId);
        builder.field(AGENT_ID_FIELD.getPreferredName(), agentId);
        if (type != null) {
            builder.field(TYPE_FIELD.getPreferredName(), type);
        }
        if (content != null) {
            builder.field(CONTENT_FIELD.getPreferredName(), content);
        }
        if (media.isEmpty() == false) {
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
        return new SessionMemory(timestamp, id, space, aiIndexIds, sessionId, userId, agentId, type, content, media, attributes);
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

    public String agentId() {
        return agentId;
    }

    @Nullable
    public String type() {
        return type;
    }

    @Nullable
    public String content() {
        return content;
    }

    /**
     * Non-text inputs for the {@code media} field, each an object with a {@code type} and a data URL {@code value}.
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
            && Objects.equals(agentId, that.agentId)
            && Objects.equals(type, that.type)
            && Objects.equals(content, that.content)
            && Objects.equals(media, that.media)
            && Objects.equals(attributes, that.attributes);
    }

    @Override
    public int hashCode() {
        return Objects.hash(timestamp, id, space, aiIndexIds, sessionId, userId, agentId, type, content, media, attributes);
    }
}
