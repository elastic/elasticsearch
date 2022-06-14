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
