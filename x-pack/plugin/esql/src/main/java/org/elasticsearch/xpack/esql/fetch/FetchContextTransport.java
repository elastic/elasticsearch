/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.fetch;

import org.elasticsearch.search.internal.ShardSearchContextId;

import java.util.List;

/**
 * Reaches the nodes that hold the fetch contexts of a query, for the {@link FetchContextLease} of the query. The lease
 * decides which contexts to free and when. The transport decides how the request gets to their node, so a route to the
 * nodes of another cluster needs no change in the lease.
 * <p>
 * It sends its requests and doesn't wait for them. A request that fails, or a node that left, leaves the contexts to the
 * reaper of their node, which frees them once their keep-alive passed. It runs in the thread context of the user who ran
 * the query, and must be safe to call from any thread.
 */
public interface FetchContextTransport {
    /**
     * Frees the contexts {@code ids} of {@code node}. The requests aren't children of the task of the query, because the
     * query frees its contexts also once its task was cancelled.
     */
    void free(FetchNode node, List<ShardSearchContextId> ids);
}
