/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.routing.allocation.allocator;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.routing.allocation.RoutingExplanations;
import org.elasticsearch.cluster.routing.allocation.command.AllocationCommands;

public record PendingAllocationCommand(
    AllocationCommands commands,
    boolean explain,
    boolean retryFailed,
    ActionListener<RoutingExplanations> listener
) {

    public void execute(RoutingAllocation allocation) {
        try {
            allocation.debugDecision(true);
            allocation.ignoreDisable(true);
            if (retryFailed) {
                allocation.resetFailedAllocationCounter();
            }
            listener.onResponse(commands.execute(allocation, explain));
        } catch (RuntimeException e) {
            listener.onFailure(e);
        } finally {
            allocation.ignoreDisable(false);
            allocation.debugDecision(false);
        }
    }
}
