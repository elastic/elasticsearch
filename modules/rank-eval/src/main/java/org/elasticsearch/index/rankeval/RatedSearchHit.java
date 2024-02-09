/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.rankeval;

import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ChunkedToXContentHelper;
import org.elasticsearch.common.xcontent.ChunkedToXContentObject;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ObjectParser.ValueType;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Iterator;
import java.util.Objects;
import java.util.OptionalInt;

/**
 * Combines a {@link SearchHit} with a document rating.
 */
public class RatedSearchHit implements Writeable, ChunkedToXContentObject {

    private final SearchHit searchHit;
    private final OptionalInt rating;

    public RatedSearchHit(SearchHit searchHit, OptionalInt rating) {
        this.searchHit = searchHit.asUnpooled();
        this.rating = rating;
    }

    RatedSearchHit(StreamInput in) throws IOException {
        this(SearchHit.readFrom(in, false), in.readBoolean() ? OptionalInt.of(in.readVInt()) : OptionalInt.empty());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        searchHit.writeTo(out);
        out.writeBoolean(rating.isPresent());
        if (rating.isPresent()) {
            out.writeVInt(rating.getAsInt());
        }
    }

    public SearchHit getSearchHit() {
        return this.searchHit;
    }

    public OptionalInt getRating() {
        return this.rating;
    }

    @Override
    public Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params outerParams) {
        return Iterators.concat(
            ChunkedToXContentHelper.singleChunk((builder, params) -> builder.startObject().field("hit")),
            searchHit.toXContentChunked(outerParams),
            ChunkedToXContentHelper.singleChunk(
                (builder, params) -> builder.field("rating", rating.isPresent() ? rating.getAsInt() : null).endObject()
            )
        );
    }

    private static final ParseField HIT_FIELD = new ParseField("hit");
    private static final ParseField RATING_FIELD = new ParseField("rating");
    private static final ConstructingObjectParser<RatedSearchHit, Void> PARSER = new ConstructingObjectParser<>(
        "rated_hit",
        true,
        a -> new RatedSearchHit((SearchHit) a[0], (OptionalInt) a[1])
    );

    static {
        PARSER.declareObject(ConstructingObjectParser.constructorArg(), (p, c) -> SearchHit.fromXContent(p), HIT_FIELD);
        PARSER.declareField(
            ConstructingObjectParser.constructorArg(),
            (p) -> p.currentToken() == XContentParser.Token.VALUE_NULL ? OptionalInt.empty() : OptionalInt.of(p.intValue()),
            RATING_FIELD,
            ValueType.INT_OR_NULL
        );
    }

    public static RatedSearchHit parse(XContentParser parser) throws IOException {
        return PARSER.apply(parser, null);
    }

    @Override
    public final boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        RatedSearchHit other = (RatedSearchHit) obj;
        return Objects.equals(rating, other.rating) && Objects.equals(searchHit, other.searchHit);
    }

    @Override
    public final int hashCode() {
        return Objects.hash(rating, searchHit);
    }
}
