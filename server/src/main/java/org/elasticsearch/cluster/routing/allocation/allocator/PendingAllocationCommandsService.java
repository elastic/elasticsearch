/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.routing.allocation.allocator;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.NotMasterException;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.routing.allocation.RoutingExplanations;
import org.elasticsearch.cluster.routing.allocation.command.AllocationCommands;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class PendingAllocationCommandsService {

    // TODO inject everywhere
    public static PendingAllocationCommandsService INSTANCE = new PendingAllocationCommandsService();

    private final SynchronizedListBuffer<PendingAllocationCommands> pendingCommandsToCompute = new SynchronizedListBuffer<>();
    private final SynchronizedListBuffer<PendingAllocationCommands> pendingCommandsToReconcile = new SynchronizedListBuffer<>();

    public void addCommands(
        AllocationCommands commands,
        boolean explain,
        boolean retryFailed,
        ActionListener<RoutingExplanations> listener
    ) {
        pendingCommandsToCompute.add(new PendingAllocationCommands(commands, explain, retryFailed, listener));
    }

    public void applyPendingCommandsOnBalanceComputation(RoutingAllocation allocation) {
        var pendingCommands = pendingCommandsToCompute.clearAndGetAll();
        for (var command : pendingCommands) {
            execute(allocation, command.commands, command.explain, command.retryFailed, command.listener);
        }
        pendingCommandsToReconcile.addAll(pendingCommands);
    }

    public void applyPendingCommandsOnBalanceReconciliation(RoutingAllocation allocation) {
        var pendingCommands = pendingCommandsToReconcile.clearAndGetAll();
        for (var command : pendingCommands) {
            execute(allocation, command.commands, command.explain, command.retryFailed, ActionListener.noop());
        }
    }

    public void onNoLongerMaster() {
        // TODO uncomment once service is properly injected (currently this is shared between all nodes in ITs)
//        var pendingCommands = pendingCommandsToCompute.clearAndGetAll();
//        for (var command : pendingCommands) {
//            command.listener.onFailure(new NotMasterException("no longer master"));
//        }
//        pendingCommandsToReconcile.clearAndGetAll();
    }

    public static void execute(
        RoutingAllocation allocation,
        AllocationCommands commands,
        boolean explain,
        boolean retryFailed,
        ActionListener<RoutingExplanations> listener
    ) {
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

    public record PendingAllocationCommands(
        AllocationCommands commands,
        boolean explain,
        boolean retryFailed,
        ActionListener<RoutingExplanations> listener
    ) {}

    private static class SynchronizedListBuffer<T> {

        private final List<T> buffer = new ArrayList<>();

        private synchronized void add(T item) {
            buffer.add(item);
        }

        private synchronized void addAll(Collection<? extends T> item) {
            buffer.addAll(item);
        }

        private synchronized List<T> clearAndGetAll() {
            var copy = new ArrayList<T>(buffer);
            buffer.clear();
            return copy;
        }
    }
}
