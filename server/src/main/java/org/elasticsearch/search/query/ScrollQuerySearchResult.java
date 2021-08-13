/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.query;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.search.SearchPhaseResult;
import org.elasticsearch.search.SearchShardTarget;

import java.io.IOException;

public final class ScrollQuerySearchResult extends SearchPhaseResult {

    private final QuerySearchResult result;

    public ScrollQuerySearchResult(StreamInput in) throws IOException {
        super(in);
        SearchShardTarget shardTarget = new SearchShardTarget(in);
        result = new QuerySearchResult(in);
        setSearchShardTarget(shardTarget);
    }

    public ScrollQuerySearchResult(QuerySearchResult result, SearchShardTarget shardTarget) {
        this.result = result;
        setSearchShardTarget(shardTarget);
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
        return result;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        getSearchShardTarget().writeTo(out);
        result.writeTo(out);
    }
}
