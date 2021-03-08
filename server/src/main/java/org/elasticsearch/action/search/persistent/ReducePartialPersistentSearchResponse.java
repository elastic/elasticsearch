/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.search.persistent;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.search.persistent.PersistentSearchShard;
import org.elasticsearch.search.persistent.PersistentSearchShardFetchFailure;

import java.io.IOException;
import java.util.List;

public class ReducePartialPersistentSearchResponse extends ActionResponse {
    private final List<PersistentSearchShard> reducedShards;
    private final List<PersistentSearchShardFetchFailure> failedToFetchShards;

    public ReducePartialPersistentSearchResponse(List<PersistentSearchShard> reducedShards,
                                                 List<PersistentSearchShardFetchFailure> failedToFetchShards) {
        this.reducedShards = reducedShards;
        this.failedToFetchShards = failedToFetchShards;
    }

    public ReducePartialPersistentSearchResponse(StreamInput in) throws IOException {
        this.reducedShards = in.readList(PersistentSearchShard::new);
        this.failedToFetchShards = in.readList(PersistentSearchShardFetchFailure::new);
    }

    public List<PersistentSearchShard> getReducedShards() {
        return reducedShards;
    }

    public List<PersistentSearchShardFetchFailure> getFailedToFetchShards() {
        return failedToFetchShards;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeList(reducedShards);
        out.writeList(failedToFetchShards);
    }
}
