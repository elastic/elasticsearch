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
     * retry on a different replica pointless. Returns {@code true} for transient shard-availability failures where a different replica may
     * succeed.
     * <p>
     * Shard-not-available exceptions (see {@link #isShardNotAvailableException}) are always considered retriable regardless of their HTTP
     * status. All other {@link RestStatus#BAD_REQUEST} exceptions are treated as non-retriable: whether caused by a malformed request, a
     * canceled task, or a closed index, every shard copy will fail in the same way.
     */
    public static boolean isRetriableShardLevelException(Throwable e) {
        if (isShardNotAvailableException(e)) {
            return true;
        }
        return ExceptionsHelper.status(e) != RestStatus.BAD_REQUEST;
    }

    /**
     * If a failure is already present, should this failure override it or not for read operations.
     */
    public static boolean isReadOverrideException(Exception e) {
        return isShardNotAvailableException(e) == false;
    }

}
