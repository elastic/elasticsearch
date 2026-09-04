/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.support;

import org.apache.lucene.store.AlreadyClosedException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.NoShardAvailableActionException;
import org.elasticsearch.action.UnavailableShardsException;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.shard.IllegalIndexShardStateException;
import org.elasticsearch.index.shard.ShardNotFoundException;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.query.SearchTimeoutException;

public class TransportActions {

    public static boolean isShardNotAvailableException(final Throwable e) {
        final Throwable actual = ExceptionsHelper.unwrapCause(e);
        return (actual instanceof ShardNotFoundException
            || actual instanceof IndexNotFoundException
            || actual instanceof IllegalIndexShardStateException
            || actual instanceof NoShardAvailableActionException
            || actual instanceof UnavailableShardsException
            || actual instanceof AlreadyClosedException);
    }

    /**
     * Returns {@code false} if the exception signals a deterministic failure that will reproduce identically on every shard copy, making a
     * retry on a different replica pointless. Returns {@code true} for transient failures where a different replica may succeed.
     * <p>
     * Shard-not-available exceptions (see {@link #isShardNotAvailableException}) are always considered retriable regardless of their HTTP
     * status. All other 4xx exceptions are treated as non-retriable — a malformed query, a missing alias, or a bad aggregation path will
     * fail in exactly the same way on every replica — with two exceptions: {@link RestStatus#REQUEST_TIMEOUT} (408) and
     * {@link RestStatus#TOO_MANY_REQUESTS} (429) indicate transient conditions that may not affect a different replica. No Elasticsearch
     * exception currently maps to 408; the carve-out is reserved for future use.
     * <p>
     * {@link SearchTimeoutException} is an explicit exception to the 429 carve-out: the per-request search timeout was exceeded and
     * should be surfaced to the caller immediately rather than silently extended by retrying on another replica.
     * <p>
     * This logic reflects search retry semantics. Other code paths, including indexing, should evaluate carefully before adopting it.
     */
    public static boolean isRetriableShardLevelException(Throwable e) {
        if (isShardNotAvailableException(e)) {
            return true;
        }
        if (ExceptionsHelper.unwrapCause(e) instanceof SearchTimeoutException) {
            return false;
        }
        int status = ExceptionsHelper.status(e).getStatus();
        return status < 400
            || status >= 500
            || status == RestStatus.REQUEST_TIMEOUT.getStatus()
            || status == RestStatus.TOO_MANY_REQUESTS.getStatus();
    }

    /**
     * If a failure is already present, should this failure override it or not for read operations.
     */
    public static boolean isReadOverrideException(Exception e) {
        return isShardNotAvailableException(e) == false;
    }

}
