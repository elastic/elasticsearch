/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.rollup.job;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.persistent.PersistentTaskState;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.indexing.IndexerState;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

/**
 * This class is essentially just a wrapper around the IndexerState and the
 * indexer's current position.  When the allocated task updates its status,
 * it is providing a new version of this.
 */
public class RollupJobStatus implements Task.Status, PersistentTaskState {
    public static final String NAME = "xpack/rollup/job";

    private final IndexerState state;

    @Nullable
    private final TreeMap<String, Object> currentPosition;

    private static final ParseField STATE = new ParseField("job_state");
    private static final ParseField CURRENT_POSITION = new ParseField("current_position");
    private static final ParseField UPGRADED_DOC_ID = new ParseField("upgraded_doc_id"); // This can be removed in 9.0

    @SuppressWarnings("unchecked")
    public static final ConstructingObjectParser<RollupJobStatus, Void> PARSER = new ConstructingObjectParser<>(
        NAME,
        args -> new RollupJobStatus((IndexerState) args[0], (HashMap<String, Object>) args[1])
    );

    static {
        PARSER.declareString(constructorArg(), IndexerState::fromString, STATE);
        PARSER.declareField(optionalConstructorArg(), p -> {
            if (p.currentToken() == XContentParser.Token.START_OBJECT) {
                return p.map();
            }
            if (p.currentToken() == XContentParser.Token.VALUE_NULL) {
                return null;
            }
            throw new IllegalArgumentException("Unsupported token [" + p.currentToken() + "]");
        }, CURRENT_POSITION, ObjectParser.ValueType.VALUE_OBJECT_ARRAY);

        // Optional to accommodate old versions of state, not used in ctor
        PARSER.declareBoolean(ConstructingObjectParser.optionalConstructorArg(), UPGRADED_DOC_ID);
    }

    public RollupJobStatus(IndexerState state, @Nullable Map<String, Object> position) {
        this.state = state;
        this.currentPosition = position == null ? null : new TreeMap<>(position);
    }

    public RollupJobStatus(StreamInput in) throws IOException {
        state = IndexerState.fromStream(in);
        currentPosition = in.readBoolean() ? new TreeMap<>(in.readMap()) : null;
        if (in.getVersion().before(Version.V_8_0_0)) {
            // 7.x nodes serialize `upgradedDocumentID` flag. We don't need it anymore, but
            // we need to pull it off the stream
            // This can go away completely in 9.0
            in.readBoolean();
        }
    }

    public IndexerState getIndexerState() {
        return state;
    }

    public Map<String, Object> getPosition() {
        return currentPosition;
    }

    public static RollupJobStatus fromXContent(XContentParser parser) {
        try {
            return PARSER.parse(parser, null);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(STATE.getPreferredName(), state.value());
        if (currentPosition != null) {
            builder.field(CURRENT_POSITION.getPreferredName(), currentPosition);
        }
        builder.endObject();
        return builder;
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        state.writeTo(out);
        out.writeBoolean(currentPosition != null);
        if (currentPosition != null) {
            out.writeGenericMap(currentPosition);
        }
        if (out.getVersion().before(Version.V_8_0_0)) {
            // 7.x nodes expect a boolean `upgradedDocumentID` flag. We don't have it anymore,
            // but we need to tell them we are upgraded in case there is a mixed cluster
            // This can go away completely in 9.0
            out.writeBoolean(true);
        }
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }

        if (other == null || getClass() != other.getClass()) {
            return false;
        }

        RollupJobStatus that = (RollupJobStatus) other;

        return Objects.equals(this.state, that.state) && Objects.equals(this.currentPosition, that.currentPosition);
    }

    @Override
    public int hashCode() {
        return Objects.hash(state, currentPosition);
    }
}
