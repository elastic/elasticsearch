/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.routing.allocation.allocator;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ContextPreservingActionListener;
import org.elasticsearch.common.util.concurrent.ThreadContext;

import java.util.ArrayList;
import java.util.List;

/**
 * This event listener might be needed to delay execution of multiple distinct tasks until followup reroute is complete.
 */
public class AllocationActionMultiListener<T> {

    private volatile boolean complete = false;
    private final List<DelayedListener<T>> delayed = new ArrayList<>();
    private final ThreadContext context;

    public AllocationActionMultiListener(ThreadContext context) {
        this.context = context;
    }

    public ActionListener<T> delay(ActionListener<T> delegate) {
        final var wrappedDelegate = new ContextPreservingActionListener<>(context.newRestorableContext(false), delegate);
        return new ActionListener<T>() {
            @Override
            public void onResponse(T response) {
                if (tryDelayListener(wrappedDelegate, response) == false) {
                    wrappedDelegate.onResponse(response);
                }
            }

            @Override
            public void onFailure(Exception e) {
                // there is no need to delay listener in case of failure
                wrappedDelegate.onFailure(e);
            }
        };
    }

    public ActionListener<Void> reroute() {
        return new ActionListener<>() {
            @Override
            public void onResponse(Void unused) {
                for (var listener : completeAndGetDelayedListeners()) {
                    listener.listener.onResponse(listener.response);
                }
            }

            @Override
            public void onFailure(Exception e) {
                for (var listener : completeAndGetDelayedListeners()) {
                    listener.listener.onFailure(e);
                }
            }
        };
    }

    public void noRerouteNeeded() {
        for (var listener : completeAndGetDelayedListeners()) {
            listener.listener.onResponse(listener.response);
        }
    }

    /**
     * @return {@code true} if listener should be delayed or {@code false} if it needs to be completed immediately
     */
    private synchronized boolean tryDelayListener(ActionListener<T> listener, T response) {
        if (complete) {
            return false;
        } else {
            delayed.add(new DelayedListener<>(listener, response));
            return true;
        }
    }

    /**
     * Completes a delay and returns a list of all delayed listeners
     */
    private synchronized List<DelayedListener<T>> completeAndGetDelayedListeners() {
        assert complete == false : "Should only complete once";
        complete = true;
        var listeners = List.copyOf(delayed);
        delayed.clear();
        return listeners;
    }

    private record DelayedListener<T>(ActionListener<T> listener, T response) {}
}
