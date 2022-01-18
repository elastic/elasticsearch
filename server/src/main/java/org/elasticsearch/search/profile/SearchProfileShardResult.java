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
import org.elasticsearch.core.Nullable;
import org.elasticsearch.search.profile.aggregation.AggregationProfileShardResult;
import org.elasticsearch.search.profile.query.QueryProfileShardResult;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

/**
 * Profile results from a particular shard for all search phases.
 */
public class SearchProfileShardResult implements Writeable, ToXContentFragment {
    private final SearchProfileQueryPhaseResult queryPhase;

    private final ProfileResult fetchPhase;

    public SearchProfileShardResult(SearchProfileQueryPhaseResult queryPhase, @Nullable ProfileResult fetch) {
        this.queryPhase = queryPhase;
        this.fetchPhase = fetch;
    }

    public SearchProfileShardResult(StreamInput in) throws IOException {
        queryPhase = new SearchProfileQueryPhaseResult(in);
        fetchPhase = in.readOptionalWriteable(ProfileResult::new);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        queryPhase.writeTo(out);
        out.writeOptionalWriteable(fetchPhase);
    }

    public SearchProfileQueryPhaseResult getQueryPhase() {
        return queryPhase;
    }

    public ProfileResult getFetchPhase() {
        return fetchPhase;
    }

    public List<QueryProfileShardResult> getQueryProfileResults() {
        return queryPhase.getQueryProfileResults();
    }

    public AggregationProfileShardResult getAggregationProfileResults() {
        return queryPhase.getAggregationProfileResults();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startArray("searches");
        for (QueryProfileShardResult result : queryPhase.getQueryProfileResults()) {
            result.toXContent(builder, params);
        }
        builder.endArray();
        queryPhase.getAggregationProfileResults().toXContent(builder, params);
        if (fetchPhase != null) {
            builder.field("fetch");
            fetchPhase.toXContent(builder, params);
        }
        return builder;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        SearchProfileShardResult other = (SearchProfileShardResult) obj;
        return queryPhase.equals(other.queryPhase) && Objects.equals(fetchPhase, other.fetchPhase);
    }

    @Override
    public int hashCode() {
        return Objects.hash(queryPhase, fetchPhase);
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }
}
