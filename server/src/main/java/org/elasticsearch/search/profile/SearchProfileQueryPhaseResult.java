/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
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

    private static final TransportVersion RESCORE_PROFILE = TransportVersion.fromName("rescore_profile");

    private SearchProfileDfsPhaseResult searchProfileDfsPhaseResult;

    private final List<QueryProfileShardResult> queryProfileResults;

    private final AggregationProfileShardResult aggProfileShardResult;

    private final List<ProfileResult> rescoreProfileResults;

    public SearchProfileQueryPhaseResult(
        List<QueryProfileShardResult> queryProfileResults,
        AggregationProfileShardResult aggProfileShardResult
    ) {
        this(queryProfileResults, aggProfileShardResult, List.of());
    }

    /**
     * @param rescoreProfileResults one result per rescorer, in the order the rescorers were run
     */
    public SearchProfileQueryPhaseResult(
        List<QueryProfileShardResult> queryProfileResults,
        AggregationProfileShardResult aggProfileShardResult,
        List<ProfileResult> rescoreProfileResults
    ) {
        this.searchProfileDfsPhaseResult = null;
        this.aggProfileShardResult = aggProfileShardResult;
        this.queryProfileResults = Collections.unmodifiableList(queryProfileResults);
        this.rescoreProfileResults = Collections.unmodifiableList(rescoreProfileResults);
    }

    public SearchProfileQueryPhaseResult(StreamInput in) throws IOException {
        searchProfileDfsPhaseResult = in.readOptionalWriteable(SearchProfileDfsPhaseResult::new);
        int profileSize = in.readVInt();
        List<QueryProfileShardResult> queryProfileResults = new ArrayList<>(profileSize);
        for (int i = 0; i < profileSize; i++) {
            QueryProfileShardResult result = new QueryProfileShardResult(in);
            queryProfileResults.add(result);
        }
        this.queryProfileResults = Collections.unmodifiableList(queryProfileResults);
        this.aggProfileShardResult = new AggregationProfileShardResult(in);
        if (in.getTransportVersion().supports(RESCORE_PROFILE)) {
            this.rescoreProfileResults = in.readCollectionAsImmutableList(ProfileResult::new);
        } else {
            this.rescoreProfileResults = List.of();
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalWriteable(searchProfileDfsPhaseResult);
        out.writeVInt(queryProfileResults.size());
        for (QueryProfileShardResult queryShardResult : queryProfileResults) {
            queryShardResult.writeTo(out);
        }
        aggProfileShardResult.writeTo(out);
        if (out.getTransportVersion().supports(RESCORE_PROFILE)) {
            out.writeCollection(rescoreProfileResults);
        }
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

    /**
     * Profile results of the rescorers that ran on the shard, in execution order. Empty if there was no rescorer.
     */
    public List<ProfileResult> getRescoreProfileResults() {
        return rescoreProfileResults;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SearchProfileQueryPhaseResult that = (SearchProfileQueryPhaseResult) o;
        return Objects.equals(searchProfileDfsPhaseResult, that.searchProfileDfsPhaseResult)
            && Objects.equals(queryProfileResults, that.queryProfileResults)
            && Objects.equals(aggProfileShardResult, that.aggProfileShardResult)
            && Objects.equals(rescoreProfileResults, that.rescoreProfileResults);
    }

    @Override
    public int hashCode() {
        return Objects.hash(searchProfileDfsPhaseResult, queryProfileResults, aggProfileShardResult, rescoreProfileResults);
    }
}
