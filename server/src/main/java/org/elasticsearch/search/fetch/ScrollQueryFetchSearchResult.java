/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.fetch;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.search.SearchPhaseResult;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.query.QuerySearchResult;

import java.io.IOException;

public final class ScrollQueryFetchSearchResult extends SearchPhaseResult {

    private final QueryFetchSearchResult result;

    public ScrollQueryFetchSearchResult(StreamInput in) throws IOException {
        SearchShardTarget searchShardTarget = new SearchShardTarget(in);
        result = new QueryFetchSearchResult(in);
        setSearchShardTarget(searchShardTarget);
    }

    public ScrollQueryFetchSearchResult(QueryFetchSearchResult result, SearchShardTarget shardTarget) {
        this.result = result;
        setSearchShardTarget(shardTarget);
    }

    public QueryFetchSearchResult result() {
        return result;
    }

    @Override
    public void setSearchShardTarget(SearchShardTarget shardTarget) {
        super.setSearchShardTarget(shardTarget);
        result.setSearchShardTarget(shardTarget);
    }

    @Override
    public void setShardIndex(int shardIndex) {
        super.setShardIndex(shardIndex);
        result.setShardIndex(shardIndex);
    }

    @Override
    public QuerySearchResult queryResult() {
        return result.queryResult();
    }

    @Override
    public FetchSearchResult fetchResult() {
        return result.fetchResult();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        getSearchShardTarget().writeTo(out);
        result.writeTo(out);
    }

    @Override
    public void incRef() {
        result.incRef();
    }

    @Override
    public boolean tryIncRef() {
        return result.tryIncRef();
    }

    @Override
    public boolean decRef() {
        return result.decRef();
    }

    @Override
    public boolean hasReferences() {
        return result.hasReferences();
    }
}
