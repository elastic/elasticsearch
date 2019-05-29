/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.transport;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentMap;
import java.util.function.BiConsumer;

/**
 * Deduplicator for {@link TransportRequest}s that keeps track of {@link TransportRequest}s that should
 * not be sent in parallel.
 * @param <T> Transport Request Class
 */
public final class TransportRequestDeduplicator<T extends TransportRequest> {

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
    public void executeOnce(T request, ActionListener<Void> listener, BiConsumer<T, ActionListener<Void>> callback) {
        ActionListener<Void> completionListener = requests.computeIfAbsent(request, CompositeListener::new).addListener(listener);
        if (completionListener != null) {
            callback.accept(request, completionListener);
        }
    }

    public int size() {
        return requests.size();
    }

    private final class CompositeListener implements ActionListener<Void> {

        private final List<ActionListener<Void>> listeners = new ArrayList<>();

        private final T request;

        private boolean isNotified;
        private Exception failure;

        CompositeListener(T request) {
            this.request = request;
        }

        CompositeListener addListener(ActionListener<Void> listener) {
            synchronized (this) {
                if (this.isNotified == false) {
                    listeners.add(listener);
                    return listeners.size() == 1 ? this : null;
                }
            }
            if (failure != null) {
                listener.onFailure(failure);
            } else {
                listener.onResponse(null);
            }
            return null;
        }

        private void onCompleted(Exception failure) {
            synchronized (this) {
                this.failure = failure;
                this.isNotified = true;
            }
            try {
                if (failure == null) {
                    ActionListener.onResponse(listeners, null);
                } else {
                    ActionListener.onFailure(listeners, failure);
                }
            } finally {
                requests.remove(request);
            }
        }

        @Override
        public void onResponse(final Void aVoid) {
            onCompleted(null);
        }

        @Override
        public void onFailure(Exception failure) {
            onCompleted(failure);
        }
    }
}
