/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.rank.feature;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.search.SearchPhaseResult;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.internal.ShardSearchContextId;
import org.elasticsearch.search.internal.ShardSearchRequest;

import java.io.IOException;

/**
 * The result of a rank feature search phase.
 * Each instance holds a {@code RankFeatureShardResult} along with the references associated with it.
 */
public class RankFeatureResult extends SearchPhaseResult {

    private RankFeatureShardResult rankShardResult;

    public RankFeatureResult() {}

    @SuppressWarnings("this-escape")
    public RankFeatureResult(ShardSearchContextId id, SearchShardTarget shardTarget, ShardSearchRequest request) {
        this.contextId = id;
        setSearchShardTarget(shardTarget);
        setShardSearchRequest(request);
    }

    @SuppressWarnings("this-escape")
    public RankFeatureResult(StreamInput in) throws IOException {
        contextId = new ShardSearchContextId(in);
        rankShardResult = in.readOptionalWriteable(RankFeatureShardResult::new);
        setShardSearchRequest(in.readOptionalWriteable(ShardSearchRequest::new));
        setSearchShardTarget(in.readOptionalWriteable(SearchShardTarget::new));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        assert hasReferences();
        contextId.writeTo(out);
        out.writeOptionalWriteable(rankShardResult);
        out.writeOptionalWriteable(getShardSearchRequest());
        out.writeOptionalWriteable(getSearchShardTarget());
    }

    @Override
    public RankFeatureResult rankFeatureResult() {
        return this;
    }

    public void shardResult(RankFeatureShardResult shardResult) {
        this.rankShardResult = shardResult;
    }

    public RankFeatureShardResult shardResult() {
        return rankShardResult;
    }

    @Override
    public boolean hasSearchContext() {
        return rankShardResult != null;
    }
}
