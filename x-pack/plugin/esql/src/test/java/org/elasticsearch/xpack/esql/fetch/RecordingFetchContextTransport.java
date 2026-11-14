/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.fetch;

import org.elasticsearch.search.internal.ShardSearchContextId;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Records what a lease asks its transport to send, and reaches no node. It stands for the route to any cluster, so a test
 * sees which contexts the lease frees on which node, without contexts behind them.
 */
final class RecordingFetchContextTransport implements FetchContextTransport {
    /**
     * One free the lease asked for.
     */
    record Sent(FetchNode node, List<ShardSearchContextId> ids) {}

    final List<Sent> frees = new CopyOnWriteArrayList<>();

    @Override
    public void free(FetchNode node, List<ShardSearchContextId> ids) {
        frees.add(new Sent(node, List.copyOf(ids)));
    }

    /**
     * Every context the lease freed so far.
     */
    Set<ShardSearchContextId> freed() {
        Set<ShardSearchContextId> freed = new HashSet<>();
        frees.forEach(free -> freed.addAll(free.ids()));
        return freed;
    }
}
