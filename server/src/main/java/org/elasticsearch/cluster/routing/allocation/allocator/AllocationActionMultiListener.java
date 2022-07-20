/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.routing.allocation.allocator;

import org.elasticsearch.action.ActionListener;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * This event listener might be needed to delay execution of multiple distinct tasks until followup reroute is complete.
 */
public class AllocationActionMultiListener<T> {

    private final AtomicBoolean complete = new AtomicBoolean(false);
    private final List<DelayedListener<T>> delayed = new ArrayList<>();

    public ActionListener<T> delay(ActionListener<T> delegate) {
        return new ActionListener<T>() {
            @Override
            public void onResponse(T response) {
                assert complete.get() == false : "Should not complete tasks after reroute is finished";
                delayed.add(new DelayedListener<>(delegate, response));
            }

            @Override
            public void onFailure(Exception e) {
                assert complete.get() == false : "Should not complete tasks after reroute is finished";
                // there is no need to delay listener in case of failure
                delegate.onFailure(e);
            }
        };
    }

    public ActionListener<Void> reroute() {
        return new ActionListener<>() {
            @Override
            public void onResponse(Void unused) {
                assert complete.compareAndSet(false, true) : "Should only complete once";
                for (var entry : delayed) {
                    entry.listener.onResponse(entry.response);
                }
            }

            @Override
            public void onFailure(Exception e) {
                assert complete.compareAndSet(false, true) : "Should only complete once";
                for (var entry : delayed) {
                    entry.listener.onFailure(e);
                }
            }
        };
    }

    private record DelayedListener<T> (ActionListener<T> listener, T response) {}
}
