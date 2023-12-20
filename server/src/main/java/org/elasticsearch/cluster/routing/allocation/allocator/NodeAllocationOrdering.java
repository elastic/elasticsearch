/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.routing.allocation.allocator;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Tracks the order in which nodes are used for allocation so that we can allocate shards to nodes in a round-robin fashion (all else being
 * equal).
 */
public class NodeAllocationOrdering {

    private final AtomicLong order = new AtomicLong(0);
    private final Map<String, Long> recentAllocations = new HashMap<>();
    private final Comparator<String> comparator = Comparator.comparing(nodeId -> recentAllocations.getOrDefault(nodeId, 0L));

    public void recordAllocation(String nodeId) {
        recentAllocations.put(nodeId, order.incrementAndGet());
    }

    public List<String> sort(Collection<String> nodeIds) {
        var list = new ArrayList<>(nodeIds);
        list.sort(comparator);
        return list;
    }

    public void retainNodes(Set<String> retainedNodeIds) {
        recentAllocations.keySet().retainAll(retainedNodeIds);
    }

    public void clear() {
        order.set(0L);
        recentAllocations.clear();
    }
}
