/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.rankeval;

import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser.ValueType;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.SearchHit;

import java.io.IOException;
import java.util.Objects;
import java.util.OptionalInt;

/**
 * Combines a {@link SearchHit} with a document rating.
 */
public class RatedSearchHit implements Writeable, ToXContentObject {

    private final SearchHit searchHit;
    private final OptionalInt rating;

    public RatedSearchHit(SearchHit searchHit, OptionalInt rating) {
        this.searchHit = searchHit;
        this.rating = rating;
    }

    RatedSearchHit(StreamInput in) throws IOException {
        this(new SearchHit(in), in.readBoolean() ? OptionalInt.of(in.readVInt()) : OptionalInt.empty());
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
    public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params)
            throws IOException {
        builder.startObject();
        builder.field("hit", (ToXContent) searchHit);
        builder.field("rating", rating.isPresent() ? rating.getAsInt() : null);
        builder.endObject();
        return builder;
    }

    private static final ParseField HIT_FIELD = new ParseField("hit");
    private static final ParseField RATING_FIELD = new ParseField("rating");
    private static final ConstructingObjectParser<RatedSearchHit, Void> PARSER = new ConstructingObjectParser<>("rated_hit", true,
            a -> new RatedSearchHit((SearchHit) a[0], (OptionalInt) a[1]));

    static {
        PARSER.declareObject(ConstructingObjectParser.constructorArg(), (p, c) -> SearchHit.fromXContent(p), HIT_FIELD);
        PARSER.declareField(ConstructingObjectParser.constructorArg(),
                (p) -> p.currentToken() == XContentParser.Token.VALUE_NULL ? OptionalInt.empty() : OptionalInt.of(p.intValue()),
                RATING_FIELD, ValueType.INT_OR_NULL);
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
        return Objects.equals(rating, other.rating)
                && Objects.equals(searchHit, other.searchHit);
    }

    @Override
    public final int hashCode() {
        return Objects.hash(rating, searchHit);
    }
}
