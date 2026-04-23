/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.reindex;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.search.SearchContextMissingNodesException;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchContextMissingException;

/**
 * Reindex can lose a scroll or PIT that it must keep alive. When that happens, the
 * search layer typically reports a missing context as {@link RestStatus#NOT_FOUND} (4xx).
 * Since the keep-alive for pits is out of the clients control (set by the {@code REINDEX_PIT_KEEP_ALIVE_SETTING}
 * setting), the client should see a server error (5xx) instead.
 */
public final class ReindexSourceSearchContextHelper {

    private ReindexSourceSearchContextHelper() {}

    /**
     * {@code true} if {@code throwable} (including causes) is either a {@link ReindexSourceSearchContextLostException},
     * {@link SearchContextMissingException} or {@link SearchContextMissingNodesException}
     */
    public static boolean isReindexSourceContextFailure(Throwable throwable) {
        if (throwable == null) {
            return false;
        }
        if (throwable instanceof ReindexSourceSearchContextLostException) {
            return true;
        }
        if (ExceptionsHelper.unwrap(throwable, SearchContextMissingException.class) != null) {
            return true;
        }
        if (ExceptionsHelper.unwrap(throwable, SearchContextMissingNodesException.class) != null) {
            return true;
        }
        return false;
    }

    /**
     * If the search error returned is because the scroll or point-in-time search context expired,
     * then we need to wrap this 4XX error in a 5XX {@link ReindexSourceSearchContextLostException}
     * since a reindexing task failing because a keep-alive expired is an internal error. This is
     * because we removed the user ability to set the search context keep-alive when we transitioned
     * to using point-in-time search
     */
    public static Throwable maybeWrapReindexContextFailure(Throwable throwable) {
        if (isReindexSourceContextFailure(throwable) == false) {
            return throwable;
        }
        if (throwable instanceof ReindexSourceSearchContextLostException) {
            return throwable;
        }
        return new ReindexSourceSearchContextLostException(throwable);
    }
}
