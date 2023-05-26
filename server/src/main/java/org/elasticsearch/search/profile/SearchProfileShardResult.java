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

    /// MP: IDEA add SearchShardTarget as instance field - is this possible?

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

    public SearchProfileDfsPhaseResult getSearchProfileDfsPhaseResult() {
        return queryPhase.getSearchProfileDfsPhaseResult();
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
        if (getSearchProfileDfsPhaseResult() != null) {
            builder.field("dfs");
            getSearchProfileDfsPhaseResult().toXContent(builder, params);
        }
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
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SearchProfileShardResult that = (SearchProfileShardResult) o;
        return Objects.equals(queryPhase, that.queryPhase) && Objects.equals(fetchPhase, that.fetchPhase);
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
