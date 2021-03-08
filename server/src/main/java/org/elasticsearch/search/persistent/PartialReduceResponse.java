/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.persistent;

import java.util.Collections;
import java.util.List;

public class PartialReduceResponse {
    private final PersistentSearchResponse searchResponse;
    private final List<PersistentSearchShard> reducedShards;
    private final List<PersistentSearchShardFetchFailure> failedToFetchShards;

    public PartialReduceResponse(PersistentSearchResponse searchResponse,
                                 List<PersistentSearchShard> reducedShards,
                                 List<PersistentSearchShardFetchFailure> failedShards) {
        this.searchResponse = searchResponse;
        this.reducedShards = Collections.unmodifiableList(reducedShards);
        this.failedToFetchShards = failedShards == null ? Collections.emptyList() : Collections.unmodifiableList(failedShards);
    }

    public PersistentSearchResponse getSearchResponse() {
        return searchResponse;
    }

    public List<PersistentSearchShard> getReducedShards() {
        return reducedShards;
    }

    public List<PersistentSearchShardFetchFailure> getFailedToFetchShards() {
        return failedToFetchShards;
    }
}
