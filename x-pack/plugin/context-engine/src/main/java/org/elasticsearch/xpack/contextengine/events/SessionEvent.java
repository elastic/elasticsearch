/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.contextengine.events;

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
import java.util.Objects;

import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

/**
 * One document in the session events data stream: the raw transcript of one round of an agent session.
 */
public class SessionEvent implements Writeable, ToXContentObject {

    public static final ParseField TIMESTAMP_FIELD = new ParseField("@timestamp");
    public static final ParseField ID_FIELD = new ParseField("id");
    public static final ParseField SPACE_FIELD = new ParseField("space");
    public static final ParseField AI_INDEX_ID_FIELD = new ParseField("ai_index_id");
    public static final ParseField SESSION_ID_FIELD = new ParseField("session_id");
    public static final ParseField ROUND_ID_FIELD = new ParseField("round_id");
    public static final ParseField USER_ID_FIELD = new ParseField("user_id");
    public static final ParseField TRANSCRIPT_FIELD = new ParseField("transcript");

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<SessionEvent, Void> PARSER = new ConstructingObjectParser<>(
        "session_event",
        args -> new SessionEvent(
            args[0] == null ? null : Instant.parse((String) args[0]),
            (String) args[1],
            (String) args[2],
            (List<String>) args[3],
            (String) args[4],
            (String) args[5],
            (String) args[6],
            (String) args[7]
        )
    );

    static {
        PARSER.declareString(optionalConstructorArg(), TIMESTAMP_FIELD);
        PARSER.declareString(optionalConstructorArg(), ID_FIELD);
        PARSER.declareString(optionalConstructorArg(), SPACE_FIELD);
        PARSER.declareStringArray(optionalConstructorArg(), AI_INDEX_ID_FIELD);
        PARSER.declareString(optionalConstructorArg(), SESSION_ID_FIELD);
        PARSER.declareString(optionalConstructorArg(), ROUND_ID_FIELD);
        PARSER.declareString(optionalConstructorArg(), USER_ID_FIELD);
        PARSER.declareString(optionalConstructorArg(), TRANSCRIPT_FIELD);
    }

    public static SessionEvent fromXContent(XContentParser parser) throws IOException {
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
    @Nullable
    private final String roundId;
    private final String userId;
    private final String transcript;

    public SessionEvent(
        @Nullable Instant timestamp,
        @Nullable String id,
        @Nullable String space,
        @Nullable List<String> aiIndexIds,
        String sessionId,
        @Nullable String roundId,
        String userId,
        String transcript
    ) {
        this.timestamp = timestamp;
        this.id = id;
        this.space = space;
        this.aiIndexIds = aiIndexIds == null ? List.of() : List.copyOf(aiIndexIds);
        this.sessionId = sessionId;
        this.roundId = roundId;
        this.userId = userId;
        this.transcript = transcript;
    }

    public SessionEvent(StreamInput in) throws IOException {
        this.timestamp = in.readOptionalInstant();
        this.id = in.readOptionalString();
        this.space = in.readOptionalString();
        this.aiIndexIds = in.readStringCollectionAsList();
        this.sessionId = in.readOptionalString();
        this.roundId = in.readOptionalString();
        this.userId = in.readOptionalString();
        this.transcript = in.readOptionalString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalInstant(timestamp);
        out.writeOptionalString(id);
        out.writeOptionalString(space);
        out.writeStringCollection(aiIndexIds);
        out.writeOptionalString(sessionId);
        out.writeOptionalString(roundId);
        out.writeOptionalString(userId);
        out.writeOptionalString(transcript);
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
        if (roundId != null) {
            builder.field(ROUND_ID_FIELD.getPreferredName(), roundId);
        }
        builder.field(USER_ID_FIELD.getPreferredName(), userId);
        builder.field(TRANSCRIPT_FIELD.getPreferredName(), transcript);
        builder.endObject();
        return builder;
    }

    /**
     * Returns a copy with the given id and write time, used when the document is first stored.
     */
    public SessionEvent withIdAndTimestamp(String id, Instant timestamp) {
        return new SessionEvent(timestamp, id, space, aiIndexIds, sessionId, roundId, userId, transcript);
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

    @Nullable
    public String roundId() {
        return roundId;
    }

    public String userId() {
        return userId;
    }

    public String transcript() {
        return transcript;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SessionEvent that = (SessionEvent) o;
        return Objects.equals(timestamp, that.timestamp)
            && Objects.equals(id, that.id)
            && Objects.equals(space, that.space)
            && Objects.equals(aiIndexIds, that.aiIndexIds)
            && Objects.equals(sessionId, that.sessionId)
            && Objects.equals(roundId, that.roundId)
            && Objects.equals(userId, that.userId)
            && Objects.equals(transcript, that.transcript);
    }

    @Override
    public int hashCode() {
        return Objects.hash(timestamp, id, space, aiIndexIds, sessionId, roundId, userId, transcript);
    }
}
