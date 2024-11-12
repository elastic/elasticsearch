/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.internal;

import org.elasticsearch.search.query.SearchTimeoutException;

/**
 * Base exception thrown whenever a search timeout occurs. It can only be subclassed and created effectively via
 * {@link ContextIndexSearcher#throwTimeExceededException()}. This more generic base exception can be used to catch
 * in the different search phases and handle timeouts appropriately
 */
public abstract sealed class InternalTimeoutException extends RuntimeException permits ContextIndexSearcher.TimeExceededException {

    public static void handleTimeout(SearchContext context) {
        if (context.request().allowPartialSearchResults() == false) {
            throw new SearchTimeoutException(context.shardTarget(), "Time exceeded");
        }
        context.queryResult().searchTimedOut(true);
    }
}
