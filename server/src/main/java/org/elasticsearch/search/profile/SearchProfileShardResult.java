/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.profile;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.search.profile.aggregation.AggregationProfileShardResult;
import org.elasticsearch.search.profile.query.QueryProfileShardResult;

import java.io.IOException;
import java.util.List;

public class SearchProfileShardResult implements Writeable {
    private final SearchProfileQueryPhaseResult search;

    private final ProfileResult fetch;

    public SearchProfileShardResult(SearchProfileQueryPhaseResult search, ProfileResult fetch) {
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
}
