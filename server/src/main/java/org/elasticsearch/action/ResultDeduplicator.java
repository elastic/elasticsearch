/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action;

import org.elasticsearch.common.util.concurrent.ConcurrentCollections;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentMap;
import java.util.function.BiConsumer;

/**
 * Deduplicator for arbitrary keys and results that can be used to ensure a given action is only executed once at a time for a given
 * request.
 * @param <T> Request type
 */
public final class ResultDeduplicator<T, R> {

    private final ConcurrentMap<T, CompositeListener> requests = ConcurrentCollections.newConcurrentMap();

    /**
     * Ensures a given request not executed multiple times when another equal request is already in-flight.
     * If the request is not yet known to the deduplicator it will invoke the passed callback with an {@link ActionListener}
     * that must be completed by the caller when the request completes. Once that listener is completed the request will be removed from
     * the deduplicator's internal state. If the request is already known to the deduplicator it will keep
     * track of the given listener and invoke it when the listener passed to the callback on first invocation is completed.
     * @param request Request to deduplicate
     * @param listener Listener to invoke on request completion
     * @param callback Callback to be invoked with request and completion listener the first time the request is added to the deduplicator
     */
    public void executeOnce(T request, ActionListener<R> listener, BiConsumer<T, ActionListener<R>> callback) {
        ActionListener<R> completionListener = requests.computeIfAbsent(request, CompositeListener::new).addListener(listener);
        if (completionListener != null) {
            callback.accept(request, completionListener);
        }
    }

    /**
     * Remove all tracked requests from this instance so that the first time {@link #executeOnce} is invoked with any request it triggers
     * an actual request execution. Use this e.g. for requests to master that need to be sent again on master failover.
     */
    public void clear() {
        requests.clear();
    }

    public int size() {
        return requests.size();
    }

    private final class CompositeListener implements ActionListener<R> {

        private final List<ActionListener<R>> listeners = new ArrayList<>();

        private final T request;

        private boolean isNotified;
        private Exception failure;
        private R response;

        CompositeListener(T request) {
            this.request = request;
        }

        CompositeListener addListener(ActionListener<R> listener) {
            synchronized (this) {
                if (this.isNotified == false) {
                    listeners.add(listener);
                    return listeners.size() == 1 ? this : null;
                }
            }
            if (failure != null) {
                listener.onFailure(failure);
            } else {
                listener.onResponse(response);
            }
            return null;
        }

        @Override
        public void onResponse(R response) {
            synchronized (this) {
                this.response = response;
                this.isNotified = true;
            }
            try {
                ActionListener.onResponse(listeners, response);
            } finally {
                requests.remove(request);
            }
        }

        @Override
        public void onFailure(Exception failure) {
            synchronized (this) {
                this.failure = failure;
                this.isNotified = true;
            }
            try {
                ActionListener.onFailure(listeners, failure);
            } finally {
                requests.remove(request);
            }
        }
    }
}
