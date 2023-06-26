/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.profile;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.search.profile.aggregation.AggregationProfileShardResult;
import org.elasticsearch.search.profile.query.QueryProfileShardResult;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Profile results from a shard for the search phase.
 */
public class SearchProfileQueryPhaseResult implements Writeable {

    private SearchProfileDfsPhaseResult searchProfileDfsPhaseResult;

    private final List<QueryProfileShardResult> queryProfileResults;

    private final AggregationProfileShardResult aggProfileShardResult;

    public SearchProfileQueryPhaseResult(
        List<QueryProfileShardResult> queryProfileResults,
        AggregationProfileShardResult aggProfileShardResult
    ) {
        this.searchProfileDfsPhaseResult = null;
        this.aggProfileShardResult = aggProfileShardResult;
        this.queryProfileResults = Collections.unmodifiableList(queryProfileResults);
    }

    public SearchProfileQueryPhaseResult(StreamInput in) throws IOException {
        if (in.getTransportVersion().onOrAfter(TransportVersion.V_8_6_0)) {
            searchProfileDfsPhaseResult = in.readOptionalWriteable(SearchProfileDfsPhaseResult::new);
        }
        int profileSize = in.readVInt();
        List<QueryProfileShardResult> queryProfileResults = new ArrayList<>(profileSize);
        for (int i = 0; i < profileSize; i++) {
            QueryProfileShardResult result = new QueryProfileShardResult(in);
            queryProfileResults.add(result);
        }
        this.queryProfileResults = Collections.unmodifiableList(queryProfileResults);
        this.aggProfileShardResult = new AggregationProfileShardResult(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (out.getTransportVersion().onOrAfter(TransportVersion.V_8_6_0)) {
            out.writeOptionalWriteable(searchProfileDfsPhaseResult);
        }
        out.writeVInt(queryProfileResults.size());
        for (QueryProfileShardResult queryShardResult : queryProfileResults) {
            queryShardResult.writeTo(out);
        }
        aggProfileShardResult.writeTo(out);
    }

    public void setSearchProfileDfsPhaseResult(SearchProfileDfsPhaseResult searchProfileDfsPhaseResult) {
        this.searchProfileDfsPhaseResult = searchProfileDfsPhaseResult;
    }

    public SearchProfileDfsPhaseResult getSearchProfileDfsPhaseResult() {
        return searchProfileDfsPhaseResult;
    }

    public List<QueryProfileShardResult> getQueryProfileResults() {
        return queryProfileResults;
    }

    public AggregationProfileShardResult getAggregationProfileResults() {
        return aggProfileShardResult;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SearchProfileQueryPhaseResult that = (SearchProfileQueryPhaseResult) o;
        return Objects.equals(searchProfileDfsPhaseResult, that.searchProfileDfsPhaseResult)
            && Objects.equals(queryProfileResults, that.queryProfileResults)
            && Objects.equals(aggProfileShardResult, that.aggProfileShardResult);
    }

    @Override
    public int hashCode() {
        return Objects.hash(searchProfileDfsPhaseResult, queryProfileResults, aggProfileShardResult);
    }
}
