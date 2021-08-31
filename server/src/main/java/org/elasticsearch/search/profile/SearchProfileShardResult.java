/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.profile;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.search.profile.aggregation.AggregationProfileShardResult;
import org.elasticsearch.search.profile.query.QueryProfileShardResult;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

/**
 * Profile results from a particular shard for all search phases.
 */
public class SearchProfileShardResult implements Writeable, ToXContentFragment {
    private final SearchProfileQueryPhaseResult search;

    private final ProfileResult fetch;

    public SearchProfileShardResult(SearchProfileQueryPhaseResult search, @Nullable ProfileResult fetch) {
        this.search = search;
        this.fetch = fetch;
    }

    public SearchProfileShardResult(StreamInput in) throws IOException {
        search = new SearchProfileQueryPhaseResult(in);
        fetch = in.readOptionalWriteable(ProfileResult::new);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        search.writeTo(out);
        out.writeOptionalWriteable(fetch);
    }

    public SearchProfileQueryPhaseResult getSearch() {
        return search;
    }

    public ProfileResult getFetch() {
        return fetch;
    }

    public List<QueryProfileShardResult> getQueryProfileResults() {
        return search.getQueryProfileResults();
    }

    public AggregationProfileShardResult getAggregationProfileResults() {
        return search.getAggregationProfileResults();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startArray("searches");
        for (QueryProfileShardResult result : search.getQueryProfileResults()) {
            result.toXContent(builder, params);
        }
        builder.endArray();
        search.getAggregationProfileResults().toXContent(builder, params);
        if (fetch != null) {
            builder.field("fetch");
            fetch.toXContent(builder, params);
        }
        return builder;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        SearchProfileShardResult other = (SearchProfileShardResult) obj;
        return search.equals(other.search) && Objects.equals(fetch, other.fetch);
    }

    @Override
    public int hashCode() {
        return Objects.hash(search, fetch);
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }
}
