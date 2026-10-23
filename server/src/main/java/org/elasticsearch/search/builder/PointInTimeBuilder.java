/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.builder;

import org.elasticsearch.action.search.SearchContextId;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.SliceIndexing;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Base64;
import java.util.Objects;

/**
 * A search request with a point in time will execute using the reader contexts associated with that point time
 * instead of the latest reader contexts.
 */
public final class PointInTimeBuilder implements Writeable, ToXContentFragment {
    private static final ParseField ID_FIELD = new ParseField("id");
    private static final ParseField KEEP_ALIVE_FIELD = new ParseField("keep_alive");
    private static final ParseField SLICE_FIELD = new ParseField(SliceIndexing.FIELD_NAME);
    private static final ObjectParser<XContentParams, Void> PARSER;

    static {
        PARSER = new ObjectParser<>(SearchSourceBuilder.POINT_IN_TIME.getPreferredName(), XContentParams::new);
        PARSER.declareString((params, id) -> params.encodedId = new BytesArray(Base64.getUrlDecoder().decode(id)), ID_FIELD);
        PARSER.declareField(
            (params, keepAlive) -> params.keepAlive = keepAlive,
            (p, c) -> TimeValue.parseTimeValue(p.text(), KEEP_ALIVE_FIELD.getPreferredName()),
            KEEP_ALIVE_FIELD,
            ObjectParser.ValueType.STRING
        );
        PARSER.declareString((params, slice) -> params.searchSlice = slice, SLICE_FIELD);
    }

    private static final class XContentParams {
        private BytesReference encodedId;
        private TimeValue keepAlive;
        private String searchSlice;
    }

    private final BytesReference encodedId;
    private transient SearchContextId searchContextId; // lazily decoded from the encodedId
    private TimeValue keepAlive;
    /**
     * The optional {@code _slice} supplied inside the {@code pit} object. A point-in-time search targets no index and therefore
     * cannot carry the slice as a {@code /{index}/{_slice}/_search} path segment; it is instead provided here and applied to the
     * search request by the REST layer. It is intentionally not serialized: the coordinator translates it into the search request's
     * slice routing before dispatching to data nodes, so it does not need to cross the wire as part of the PIT.
     */
    @Nullable
    private transient String searchSlice;

    public PointInTimeBuilder(BytesReference pitID) {
        this.encodedId = Objects.requireNonNull(pitID, "Point in time ID must be provided");
    }

    public PointInTimeBuilder(StreamInput in) throws IOException {
        encodedId = in.readBytesReference();
        keepAlive = in.readOptionalTimeValue();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeBytesReference(encodedId);
        out.writeOptionalTimeValue(keepAlive);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field(ID_FIELD.getPreferredName(), Base64.getUrlEncoder().encodeToString(BytesReference.toBytes(encodedId)));
        if (keepAlive != null) {
            builder.field(KEEP_ALIVE_FIELD.getPreferredName(), keepAlive.getStringRep());
        }
        if (searchSlice != null) {
            builder.field(SLICE_FIELD.getPreferredName(), searchSlice);
        }
        return builder;
    }

    public static PointInTimeBuilder fromXContent(XContentParser parser) throws IOException {
        final XContentParams params = PARSER.parse(parser, null);
        if (params.encodedId == null) {
            throw new IllegalArgumentException("point in time id is not provided");
        }
        return new PointInTimeBuilder(params.encodedId).setKeepAlive(params.keepAlive).setSearchSlice(params.searchSlice);
    }

    /**
     * Returns the encoded id of this point in time
     */
    public BytesReference getEncodedId() {
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

    @Nullable
    public TimeValue getKeepAlive() {
        return keepAlive;
    }

    /**
     * Sets the optional {@code _slice} supplied inside the {@code pit} object. See {@link #searchSlice}.
     */
    public PointInTimeBuilder setSearchSlice(@Nullable String searchSlice) {
        this.searchSlice = searchSlice;
        return this;
    }

    /**
     * Returns the optional {@code _slice} supplied inside the {@code pit} object, or {@code null} if none was provided.
     */
    @Nullable
    public String getSearchSlice() {
        return searchSlice;
    }

    /**
     * Returns {@code true} if the point in time is explicitly released when returning the response.
     */
    public boolean singleSession() {
        return keepAlive != null && TimeValue.MINUS_ONE.equals(keepAlive);
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
