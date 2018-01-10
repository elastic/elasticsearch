/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.index.rankeval;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.SearchHit;

import java.io.IOException;
import java.util.Objects;
import java.util.Optional;

/**
 * Combines a {@link SearchHit} with a document rating.
 */
public class RatedSearchHit implements Writeable, ToXContent {

    private final SearchHit searchHit;
    private final Optional<Integer> rating;

    public RatedSearchHit(SearchHit searchHit, Optional<Integer> rating) {
        this.searchHit = searchHit;
        this.rating = rating;
    }

    RatedSearchHit(StreamInput in) throws IOException {
        this(SearchHit.readSearchHit(in),
                in.readBoolean() == true ? Optional.of(in.readVInt()) : Optional.empty());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        searchHit.writeTo(out);
        out.writeBoolean(rating.isPresent());
        if (rating.isPresent()) {
            out.writeVInt(rating.get());
        }
    }

    public SearchHit getSearchHit() {
        return this.searchHit;
    }

    public Optional<Integer> getRating() {
        return this.rating;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params)
            throws IOException {
        builder.startObject();
        builder.field("hit", (ToXContent) searchHit);
        builder.field("rating", rating.orElse(null));
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
        return Objects.equals(rating, other.rating)
                && Objects.equals(searchHit, other.searchHit);
    }

    @Override
    public final int hashCode() {
        return Objects.hash(rating, searchHit);
    }
}
