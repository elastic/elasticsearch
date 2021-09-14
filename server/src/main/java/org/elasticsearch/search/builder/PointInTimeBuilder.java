/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.builder;

import org.elasticsearch.action.search.SearchContextId;
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

/**
 * A search request with a point in time will execute using the reader contexts associated with that point time
 * instead of the latest reader contexts.
 */
public final class PointInTimeBuilder implements Writeable, ToXContentFragment {
    private static final ParseField ID_FIELD = new ParseField("id");
    private static final ParseField KEEP_ALIVE_FIELD = new ParseField("keep_alive");
    private static final ObjectParser<XContentParams, Void> PARSER;

    static {
        PARSER = new ObjectParser<>(SearchSourceBuilder.POINT_IN_TIME.getPreferredName(), XContentParams::new);
        PARSER.declareString((params, id) -> params.encodedId = id, ID_FIELD);
        PARSER.declareField((params, keepAlive) -> params.keepAlive = keepAlive,
            (p, c) -> TimeValue.parseTimeValue(p.text(), KEEP_ALIVE_FIELD.getPreferredName()),
            KEEP_ALIVE_FIELD, ObjectParser.ValueType.STRING);
    }

    private static final class XContentParams {
        private String encodedId;
        private TimeValue keepAlive;
    }

    private final String encodedId;
    private transient SearchContextId searchContextId; // lazily decoded from the encodedId
    private TimeValue keepAlive;

    public PointInTimeBuilder(String pitID) {
        this.encodedId = Objects.requireNonNull(pitID, "Point in time ID must be provided");
    }

    public PointInTimeBuilder(StreamInput in) throws IOException {
        encodedId = in.readString();
        keepAlive = in.readOptionalTimeValue();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(encodedId);
        out.writeOptionalTimeValue(keepAlive);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field(ID_FIELD.getPreferredName(), encodedId);
        if (keepAlive != null) {
            builder.field(KEEP_ALIVE_FIELD.getPreferredName(), keepAlive.getStringRep());
        }
        return builder;
    }

    public static PointInTimeBuilder fromXContent(XContentParser parser) throws IOException {
        final XContentParams params = PARSER.parse(parser, null);
        if (params.encodedId == null) {
            throw new IllegalArgumentException("point in time id is not provided");
        }
        return new PointInTimeBuilder(params.encodedId).setKeepAlive(params.keepAlive);
    }

    /**
     * Returns the encoded id of this point in time
     */
    public String getEncodedId() {
        return encodedId;
    }

    /**
     * Returns the search context of this point in time from its encoded id.
     */
    public SearchContextId getSearchContextId(NamedWriteableRegistry namedWriteableRegistry) {
        if (searchContextId == null) {
            searchContextId = SearchContextId.decode(namedWriteableRegistry, encodedId);
        }
        return searchContextId;
    }

    /**
     * If specified, the search layer will keep this point in time around for at least the given keep-alive.
     * Otherwise, the point in time will be kept around until the original keep alive elapsed.
     */
    public PointInTimeBuilder setKeepAlive(TimeValue keepAlive) {
        this.keepAlive = keepAlive;
        return this;
    }

    /**
     * If specified, the search layer will keep this point in time around for at least the given keep-alive.
     * Otherwise, the point in time will be kept around until the original keep alive elapsed.
     */
    public PointInTimeBuilder setKeepAlive(String keepAlive) {
        return setKeepAlive(TimeValue.parseTimeValue(keepAlive, "keep_alive"));
    }

    @Nullable
    public TimeValue getKeepAlive() {
        return keepAlive;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final PointInTimeBuilder that = (PointInTimeBuilder) o;
        return Objects.equals(encodedId, that.encodedId) && Objects.equals(keepAlive, that.keepAlive);
    }

    @Override
    public int hashCode() {
        return Objects.hash(encodedId, keepAlive);
    }
}
