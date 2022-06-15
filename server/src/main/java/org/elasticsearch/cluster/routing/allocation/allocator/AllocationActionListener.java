/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.routing.allocation.allocator;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.action.ActionListener;

import java.util.concurrent.atomic.AtomicInteger;

public class AllocationActionListener<T> {

    private final ActionListener<T> delegate;
    private final SetOnce<T> response = new SetOnce<>();
    private final AtomicInteger listenersExecuted = new AtomicInteger(2);

    /**
     * This listener could be used when reroute completion (such as even balancing shards across the cluster) is not required for the
     * completion of the caller operation.
     *
     * For example, it is required to compute the desired balance to properly allocate newly created index, but it is not when deleting one.
     */
    public static ActionListener<Void> rerouteCompletionIsNotRequired() {
        return ActionListener.noop();
    }

    public AllocationActionListener(ActionListener<T> delegate) {
        this.delegate = delegate;
    }

    private void notifyListenerExecuted() {
        if (listenersExecuted.decrementAndGet() == 0) {
            delegate.onResponse(AllocationActionListener.this.response.get());
        }
    }

    public ActionListener<T> clusterStateUpdate() {
        return new ActionListener<>() {
            @Override
            public void onResponse(T response) {
                AllocationActionListener.this.response.set(response);
                notifyListenerExecuted();
            }

            @Override
            public void onFailure(Exception e) {
                delegate.onFailure(e);
            }
        };
    }

    public ActionListener<Void> reroute() {
        return new ActionListener<>() {
            @Override
            public void onResponse(Void unused) {
                notifyListenerExecuted();
            }

            @Override
            public void onFailure(Exception e) {
                delegate.onFailure(e);
            }
        };
    }
}
