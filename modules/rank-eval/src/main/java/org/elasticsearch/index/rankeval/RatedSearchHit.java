/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.rankeval;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;
import java.util.OptionalInt;

/**
 * Combines a {@link SearchHit} with a document rating.
 */
public class RatedSearchHit implements Writeable, ToXContentObject {

    private final SearchHit searchHit;
    private final OptionalInt rating;

    /**
     * Retains a reference to a pooled {@link SearchHit} from a live search response. Callers must eventually release refs held by
     * {@link RankEvalResponse#close()}.
     */
    public RatedSearchHit(SearchHit searchHit, OptionalInt rating) {
        this(searchHit, rating, true);
    }

    /**
     * @param acquireRef if {@code true}, {@link SearchHit#mustIncRef()} is called for live search hits; if {@code false}, the caller
     *                   transfers ownership (e.g. {@link SearchHit#readFrom(StreamInput, boolean)} with {@code pooled == false}).
     */
    private RatedSearchHit(SearchHit searchHit, OptionalInt rating, boolean acquireRef) {
        if (acquireRef) {
            searchHit.mustIncRef();
        }
        this.searchHit = searchHit;
        this.rating = rating;
    }

    RatedSearchHit(StreamInput in) throws IOException {
        this(SearchHit.readFrom(in, false), in.readBoolean() ? OptionalInt.of(in.readVInt()) : OptionalInt.empty(), false);
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
    public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.startObject();
        builder.field("hit", (ToXContent) searchHit);
        builder.field("rating", rating.isPresent() ? rating.getAsInt() : null);
        builder.endObject();
        return builder;
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
