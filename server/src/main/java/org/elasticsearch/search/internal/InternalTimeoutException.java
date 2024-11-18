/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.internal;

import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.query.QuerySearchResult;
import org.elasticsearch.search.query.SearchTimeoutException;

/**
 * Base exception thrown whenever a search timeout occurs. It can only be subclassed and created effectively via
 * {@link ContextIndexSearcher#throwTimeExceededException()}. This more generic base exception can be used to catch timeout errors
 * in the different search phases and handle timeouts appropriately
 */
public abstract sealed class InternalTimeoutException extends RuntimeException permits ContextIndexSearcher.TimeExceededException {

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
