/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.routing.allocation.allocator;

import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.routing.allocation.command.AllocationCommand;
import org.elasticsearch.cluster.routing.allocation.command.AllocationCommands;
import org.elasticsearch.cluster.routing.allocation.command.MoveAllocationCommand;

import java.util.ArrayList;
import java.util.List;

public class PendingAllocationCommandsService {

    // TODO inject everywhere
    public static PendingAllocationCommandsService INSTANCE = new PendingAllocationCommandsService();

    private final SynchronizedListBuffer<MoveAllocationCommand> pendingMoveCommands = new SynchronizedListBuffer<>();

    public void addCommands(AllocationCommands commands) {
        for (AllocationCommand command : commands.commands()) {
            if (command instanceof MoveAllocationCommand move) {
                pendingMoveCommands.add(move);
            }
        }
    }

    public void applyMoveCommandsToDesiredBalanceCalculation(RoutingAllocation allocation) {
        var pending = pendingMoveCommands.clearAndGetAll();
        for (MoveAllocationCommand command : pending) {
            try {
                command.execute(allocation, false);
            } catch (RuntimeException e) {
                // TODO log failures
            }
        }
    }

    public void onNoLongerMaster() {
        // TODO uncomment once service is properly injected (currently this is shared between all nodes in ITs)
        // var pendingCommands = pendingCommandsToCompute.clearAndGetAll();
        // for (var command : pendingCommands) {
        // command.listener.onFailure(new NotMasterException("no longer master"));
        // }
        // pendingCommandsToReconcile.clearAndGetAll();
    }

    private static class SynchronizedListBuffer<T> {

        private final List<T> buffer = new ArrayList<>();

        private synchronized void add(T item) {
            buffer.add(item);
        }

        private synchronized List<T> clearAndGetAll() {
            var copy = new ArrayList<T>(buffer);
            buffer.clear();
            return copy;
        }
    }
}
