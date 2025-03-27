/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.query;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchException;
import org.elasticsearch.search.SearchShardTarget;

import java.io.IOException;

/**
 * Specific instance of {@link SearchException} that indicates that a search timeout occurred.
 * Always returns http status 429 (Too Many Requests)
 */
public class SearchTimeoutException extends SearchException {
    public SearchTimeoutException(SearchShardTarget shardTarget, String msg) {
        super(shardTarget, msg);
    }

    public SearchTimeoutException(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public RestStatus status() {
        return RestStatus.TOO_MANY_REQUESTS;
    }

    @Override
    public boolean isTimeout() {
        return true;
    }

    /**
     * Propagate a timeout according to whether partial search results are allowed or not.
     * In case partial results are allowed, a flag will be set to the provided {@link QuerySearchResult} to indicate that there was a
     * timeout, but the execution will continue and partial results will be returned to the user.
     * When partial results are disallowed, a {@link SearchTimeoutException} will be thrown and returned to the user.
     */
    public static void handleTimeout(boolean allowPartialSearchResults, SearchShardTarget target, QuerySearchResult querySearchResult) {
        if (allowPartialSearchResults == false) {
            throw new SearchTimeoutException(target, "Time exceeded");
        }
        querySearchResult.searchTimedOut(true);
    }
}
