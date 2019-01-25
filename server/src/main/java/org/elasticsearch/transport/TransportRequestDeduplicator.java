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

/**
 * Deduplicator for {@link TransportRequest}s that keeps track of {@link TransportRequest}s that should
 * not be sent in parallel.
 * @param <T> Transport Request Class
 */
public final class TransportRequestDeduplicator<T extends TransportRequest> {

    private final ConcurrentMap<T, CompositeListener> requests = ConcurrentCollections.newConcurrentMap();

    /**
     * Register a listener for the given request with the deduplicator.
     * If the request is not yet registered with the deduplicator it will return an {@link ActionListener}
     * that must be completed by the called when the request completes. If the request is already known to
     * the deduplicator it will keep track of the given listener and invoke it when the listener returned
     * for the first invocation with the request is completed.
     * The caller of this method should therefore execute the transport request if returned an instance
     * of {@link ActionListener} and invoke that listener when completing the request and do nothing when
     * being returned {@code null}.
     * @param request Request to deduplicate
     * @param listener Listener to invoke on request completion
     * @return Listener that must be invoked by the caller or null when the request is already known
     */
    public ActionListener<Void> register(T request, ActionListener<Void> listener) {
        return requests.computeIfAbsent(request, CompositeListener::new).addListener(listener);
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
