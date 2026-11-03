/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.fetch;

import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.search.internal.ShardSearchContextId;
import org.elasticsearch.tasks.Task;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Records what a lease asks its transport to send, and reaches no node. It stands for the route to any cluster, so a test
 * sees which contexts the lease frees and refreshes on which node, without contexts behind them.
 */
final class RecordingFetchContextTransport implements FetchContextTransport {
    /**
     * One request the lease asked for.
     */
    record Sent(FetchNode node, OriginalIndices indices, List<ShardSearchContextId> ids) {}

    final List<Sent> frees = new CopyOnWriteArrayList<>();
    final List<Sent> keepAlives = new CopyOnWriteArrayList<>();

    @Override
    public void free(FetchNode node, OriginalIndices indices, List<ShardSearchContextId> ids) {
        frees.add(new Sent(node, indices, List.copyOf(ids)));
    }

    @Override
    public void keepAlive(FetchNode node, OriginalIndices indices, List<ShardSearchContextId> ids, TimeValue keepAlive, Task rootTask) {
        keepAlives.add(new Sent(node, indices, List.copyOf(ids)));
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
