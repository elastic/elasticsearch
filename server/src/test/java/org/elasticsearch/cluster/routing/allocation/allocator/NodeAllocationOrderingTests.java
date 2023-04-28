/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.routing.allocation.allocator;

import org.elasticsearch.test.ESTestCase;

import java.util.Set;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;

public class NodeAllocationOrderingTests extends ESTestCase {

    public void testSortNodeIds() {
        var order = new NodeAllocationOrdering();
        order.recordAllocation("node-1");
        order.recordAllocation("node-2");

        var nodeIds = order.sort(Set.of("node-1", "node-2", "node-3", "node-4", "node-5"));

        assertThat(nodeIds.get(4), equalTo("node-2"));// as this node received the most recent allocation
        assertThat(nodeIds.get(3), equalTo("node-1"));
        assertThat(nodeIds.subList(0, 3), containsInAnyOrder("node-3", "node-4", "node-5"));// no recent allocations, any could be used
    }

    public void testRetainOnlyAliveNodes() {
        var order = new NodeAllocationOrdering();
        order.recordAllocation("node-1");
        order.recordAllocation("node-2");
        order.recordAllocation("node-3");

        order.retainNodes(Set.of("node-1", "node-2"));// simulate node-3 leaving the cluster

        var nodeIds = order.sort(Set.of("node-1", "node-2", "node-3"));// node-3 is back

        // node-3 should be pushed to the beginning of the list as its allocation history was cleaned when it left.
        // now we assume no recent allocations on it
        assertThat(nodeIds, contains("node-3", "node-1", "node-2"));
    }
}
