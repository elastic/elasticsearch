/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.routing.allocation.allocator;

import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.routing.allocation.RoutingExplanations;
import org.elasticsearch.cluster.routing.allocation.command.AllocationCommands;

import java.util.function.Consumer;

public record PendingAllocationCommand(AllocationCommands commands, boolean explain, Consumer<RoutingExplanations> listener) {

    public void execute(RoutingAllocation allocation) {
        listener.accept(commands.execute(allocation, explain));
    }
}
