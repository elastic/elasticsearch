/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.routing.allocation;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.core.Nullable;

/**
 * A class representing a failed shard.
 */
public class FailedShard {
    private final ShardRouting routingEntry;
    private final String message;
    private final Exception failure;
    private final boolean markAsStale;

    public FailedShard(ShardRouting routingEntry, String message, @Nullable Exception failure, boolean markAsStale) {
        assert routingEntry.assignedToNode() : "only assigned shards can be failed " + routingEntry;
        this.routingEntry = routingEntry;
        this.message = message;
        this.failure = failure;
        this.markAsStale = markAsStale;
    }

    @Override
    public String toString() {
        return "failed shard, shard " + routingEntry + ", message [" + message + "], markAsStale [" + markAsStale + "], failure ["
            + (failure == null ? "null" : ExceptionsHelper.stackTrace(failure)) + "]";
    }

    /**
     * The shard routing entry for the failed shard.
     */
    public ShardRouting getRoutingEntry() {
        return routingEntry;
    }

    /**
     * The failure message, if available, explaining why the shard failed.
     */
    @Nullable
    public String getMessage() {
        return message;
    }

    /**
     * The exception, if present, causing the shard to fail.
     */
    @Nullable
    public Exception getFailure() {
        return failure;
    }

    /**
     * Whether or not to mark the shard as stale (eg. removing from in-sync set) when failing the shard.
     */
    public boolean markAsStale() {
        return markAsStale;
    }
}
